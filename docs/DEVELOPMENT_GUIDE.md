# Transactional Notification Service: подробный план разработки

Это учебная шпаргалка по последовательной разработке `transactional-notification-service`.

Цель руководства — не просто получить работающий пример, а по ходу реализации
разобраться:

- зачем одновременно нужны Kafka, RabbitMQ и Celery;
- как провести границы луковой архитектуры;
- где валидировать внешние сообщения;
- когда подтверждать Kafka offset;
- почему дубликаты сообщений неизбежны;
- как тестировать application-слой без запущенных брокеров;
- что отличает работающий MVP от production-ready сервиса.

Руководство приводит полный код production-oriented Console MVP:

```text
Test Kafka Producer
        ↓
Kafka: notification.requested.v1
        ↓
Notification Kafka Consumer
        ↓
ProcessNotificationEvent
        ↓
CeleryTaskPublisher
        ↓
RabbitMQ
        ↓
Celery Worker
        ↓
SendNotification
        ↓
JinjaTemplateRenderer + ConsoleEmailSender
```

После прохождения основного руководства сервис ещё не будет полностью готов к
production: перед реальной отправкой email необходимо добавить идемпотентность,
PostgreSQL, SMTP, наблюдаемость и продуманную retry-политику. Эти этапы описаны
в конце документа.

---

## 1. Как проходить это руководство

Не вставляй весь код проекта одним большим блоком. Для каждого этапа используй
следующий цикл:

1. Прочитай цель этапа.
2. Попробуй самостоятельно описать ответственность нового компонента.
3. Напечатай приведённый код руками.
4. Запусти тест или команду проверки.
5. Намеренно измени код и посмотри, какой тест сломается.
6. Выполни самостоятельное упражнение.
7. Только после этого переходи дальше.

Полезная проверка понимания после каждого файла:

```text
1. Кто вызывает этот компонент?
2. От каких компонентов он зависит?
3. Почему он находится именно в этом слое?
4. Что произойдёт при исключении?
5. Можно ли протестировать его без Kafka/RabbitMQ?
```

В руководстве используются три обозначения:

- **обязательно** — без этого нарушается корректность системы;
- **архитектурное решение** — выбранный нами способ организации кода;
- **временно для MVP** — осознанное ограничение, которое будет устранено позже.

### Учебный формат каждого этапа

Дальше почти каждый этап устроен как маленький урок, а не как инструкция
«скопируй и забудь». Если в каком-то месте ты чувствуешь, что код появился
слишком резко, используй этот порядок:

1. Сначала прочитай, какую проблему решает файл.
2. Закрой полный код глазами и попробуй написать минимальный вариант сам.
3. Не пытайся угадать всё идеально. Достаточно поймать форму:
   имена классов, основные методы, входные и выходные данные.
4. Потом открой полный вариант и сравни:
   что ты назвал иначе, где забыл async, где смешал слой с инфраструктурой.
5. После сравнения прочитай разбор: он объясняет, зачем код написан именно так.
6. Только затем запускай проверку.

Важная мысль: если твой первый вариант отличается от полного кода, это не
ошибка обучения. Разница между твоим вариантом и эталоном как раз показывает,
какие архитектурные решения стоит разобрать.

### Как читать каждый фрагмент кода

Когда видишь новый файл, задавай пять вопросов:

```text
1. Это внешний контракт, application DTO, domain model или adapter?
2. Кто создаёт этот объект?
3. Кто вызывает этот метод?
4. Что случится, если зависимость упадёт с ошибкой?
5. Можно ли протестировать этот код без Kafka, RabbitMQ, Celery и Docker?
```

Если ответ на пятый вопрос «нет» для application/core-кода, значит слой
скорее всего уже загрязнился инфраструктурой.

### Мини-попытка перед полным кодом

В каждом серьёзном этапе ниже будет блок «Сначала попробуй сам». Его цель —
не заставить тебя страдать без подсказок, а дать мозгу зацепку. Когда ты потом
увидишь полный код, он будет восприниматься не как чужая магия, а как ответ на
задачу, которую ты уже успел сформулировать.

Пример мини-попытки:

```text
Нужно сделать use case, который получает DTO и публикует task.

Попробуй сам:
- придумай имя класса;
- придумай метод;
- реши, что передать в конструктор;
- реши, где создать task_id;
- реши, нужно ли ловить ошибку publisher.
```

После этого полный код будет не просто набором строк. Ты уже будешь понимать,
какие решения в нём закодированы.

### Как открыть это руководство в Fedora KDE 44

Для чтения исходного Markdown в редакторе выполни:

```bash
code docs/DEVELOPMENT_GUIDE.md
```

В VS Code/Codium Markdown Preview открывается сочетанием `Ctrl+Shift+V`.

Для просмотра как готовой HTML-страницы используй установленный `pandoc`:

```bash
pandoc docs/DEVELOPMENT_GUIDE.md \
  --standalone \
  --toc \
  --metadata title="Transactional Notification Service Development Guide" \
  --output /tmp/transactional-notification-development-guide.html
```

Затем открой результат через приложение KDE по умолчанию:

```bash
xdg-open /tmp/transactional-notification-development-guide.html
```

После изменения Markdown повтори команду `pandoc`, чтобы обновить HTML.

---

## 2. Модель системы до написания кода

### 2.1. Роли Kafka, RabbitMQ и Celery

Kafka и RabbitMQ не являются взаимозаменяемыми компонентами в этом проекте.

Kafka используется как внешняя событийная шина:

```text
Auth Service ─┐
Booking Service ──→ Kafka ──→ Notification Service
Order Service ─┘
```

Особенности Kafka:

- события некоторое время сохраняются;
- разные consumer groups независимо читают одни события;
- сообщения внутри partition имеют порядок;
- consumer хранит позицию чтения как offset;
- уже прочитанные события можно перечитать.

RabbitMQ используется как внутренняя очередь команд:

```text
Notification Consumer → RabbitMQ → Email Workers
```

Особенности RabbitMQ в этом проекте:

- задача передаётся одному worker;
- worker подтверждает выполнение задачи;
- очередь помогает регулировать скорость отправки;
- Celery предоставляет worker-процессы, маршрутизацию и retries.

Celery — не отдельный брокер. Это Python-фреймворк, который использует RabbitMQ
как broker.

### 2.2. Event и command — разные понятия

В Kafka приходит **event**:

```text
auth-service сообщает:
"было запрошено email-подтверждение"
```

Во внутреннюю очередь отправляется **command/task**:

```text
notification-service поручает worker:
"отрендери и отправь это email-сообщение"
```

Внешний event и внутренняя task не должны использовать одну модель. Они могут
развиваться и версионироваться независимо.

### 2.3. Гарантия доставки

Порядок обработки одного Kafka-сообщения:

```text
1. Получить Kafka event.
2. Провалидировать event.
3. Опубликовать Celery task в RabbitMQ.
4. Подтвердить Kafka offset.
```

Offset нельзя подтверждать до успешной публикации задачи. Иначе при падении
RabbitMQ событие потеряется.

Но существует другой сценарий:

```text
1. RabbitMQ успешно принял задачу.
2. Процесс упал до Kafka commit.
3. После перезапуска Kafka снова отдала событие.
4. В RabbitMQ появилась вторая такая же задача.
```

Следовательно, поток Kafka → RabbitMQ имеет семантику **at-least-once**:
задача будет опубликована как минимум один раз, но иногда более одного раза.

Наличие одинакового `task_id` не заставляет Celery автоматически убрать
дубликат. Перед подключением реального SMTP понадобится идемпотентность.

### 2.4. Что делать с невалидным событием

Невалидное событие нельзя бесконечно читать снова. Такое сообщение называют
poison message.

Для него используем отдельный Kafka topic:

```text
notification.requested.v1.dlq
```

Алгоритм:

```text
Невалидное событие
    ↓
Записать исходное сообщение и ошибку в DLQ
    ↓
Только после успешной записи в DLQ подтвердить исходный offset
```

Временная ошибка RabbitMQ в DLQ не отправляется. Она должна остановить
consumer, чтобы событие было прочитано повторно после восстановления сервиса.

---

## 3. Правила луковой архитектуры

### 3.1. Направление зависимостей

```text
run
 ↓
config/container
 ↓
interfaces + infrastructure
 ↓
application
 ↓
core
```

Эта схема продолжает подход из `test_psek`:

- `core` содержит независимые domain-модели;
- `application` содержит DTO, use cases и порты нужных им внешних действий;
- `infrastructure` реализует application-порты;
- `interfaces` принимает внешние Kafka/Celery-сообщения;
- `config` содержит settings и собирает зависимости;
- `run` содержит запускаемые процессы.

Внутренние слои не импортируют внешние:

- `core` не знает про Pydantic, Kafka, Celery, RabbitMQ и Jinja;
- `application` не знает про `AIOKafkaConsumer`, `Celery` и конкретный sender;
- `interfaces` переводит wire-контракты во внутренние DTO;
- `infrastructure` реализует порты из `application`;
- `config/container.py` создаёт объекты и передаёт зависимости.

`core` здесь означает всё независимое ядро сервиса, а `domain` является его
частью. Поэтому путь `src/core/domain/models.py` соответствует архитектурному
языку `test_psek`.

### 3.2. Где находятся модели

| Модель | Слой | Причина |
|---|---|---|
| `EmailRequestedV1` | `interfaces/contracts` | Внешний Kafka-контракт из другого сервиса |
| `SendEmailTaskV1` | `interfaces/contracts` | Контракт сообщения RabbitMQ |
| `ProcessNotificationDTO` | `application/dto` | Вход use case обработки event |
| `SendEmailDTO` | `application/dto` | Вход use case отправки |
| `EmailRecipient` | `core/domain` | Независимое понятие предметной области |
| `RenderedEmail` | `core/domain` | Независимый результат рендеринга |

DTO — это объекты передачи данных между adapter и use case. Как и в
`test_psek`, application DTO реализованы Pydantic-моделями.

Pydantic-модели в `interfaces/contracts` тоже переносят данные, но мы явно
называем их **wire-контрактами**, потому что они описывают JSON конкретной
версии Kafka/Celery-сообщения. Контракт и application DTO нельзя объединять:
они меняются по разным причинам.

### 3.3. Почему один use case async, а второй sync

`ProcessNotificationEvent` вызывается асинхронным Kafka consumer и публикует
задачу через async-порт:

```python
await publisher.publish(task)
```

`SendNotification` вызывается обычной синхронной Celery task. Jinja и
Console sender также синхронные, поэтому этот use case оставляем синхронным.

Не нужно превращать всё приложение в async только ради единообразия. Async
нужен там, где он действительно помогает ожидать сетевой I/O.

### 3.4. Что взято из `test_psek`, а что добавлено

Сохраняем знакомую тебе основу:

```text
core/domain       — внутренние модели
application/dto   — Pydantic DTO
application/use_case
infrastructure
interfaces
config
run
```

Для notification-service добавляются три явных элемента:

- `application/ports` — потому что use cases зависят от нескольких внешних
  действий, а не только repository;
- `interfaces/contracts/v1` — потому что Kafka и RabbitMQ сообщения являются
  версионируемыми wire-контрактами;
- `config/container.py` — потому что нужно собирать зависимости для двух
  отдельных процессов: Kafka consumer и Celery worker.

Mapper внешнего wire-контракта расположен в `interfaces/mappers.py`, а не в
`application/mappers`: иначе application пришлось бы импортировать
`interfaces/contracts`. В будущем внутренние мапперы, которые не знают о
Kafka/Celery-контрактах, можно размещать в `application/mappers`, как в
`test_psek`.

Это развитие структуры `test_psek` под новый тип сервиса, а не её замена другой
архитектурой.

---

## 4. Целевая структура проекта

После завершения Console MVP структура будет такой:

```text
transactional-notification-service/
├── compose.yaml
├── .env.example
├── pyproject.toml
├── README.md
├── docs/
│   └── DEVELOPMENT_GUIDE.md
├── email_templates/
│   └── auth/
│       ├── email_confirmation/
│       │   ├── subject.txt
│       │   ├── body.html
│       │   └── body.txt
├── run/
│   ├── __init__.py
│   ├── consumer.py
│   └── worker.py
├── scripts/
│   ├── __init__.py
│   └── publish_test_event.py
├── src/
│   ├── __init__.py
│   ├── application/
│   │   ├── __init__.py
│   │   ├── dto/
│   │   │   ├── __init__.py
│   │   │   └── notification.py
│   │   ├── exceptions.py
│   │   ├── ports/
│   │   │   ├── __init__.py
│   │   │   ├── email_sender.py
│   │   │   ├── task_publisher.py
│   │   │   └── template_renderer.py
│   │   └── use_case/
│   │       ├── __init__.py
│   │       ├── process_notification_event.py
│   │       └── send_notification.py
│   ├── config/
│   │   ├── __init__.py
│   │   ├── container.py
│   │   ├── logging.py
│   │   └── settings.py
│   ├── core/
│   │   ├── __init__.py
│   │   ├── domain/
│   │   │   ├── __init__.py
│   │   │   └── models.py
│   ├── infrastructure/
│   │   ├── __init__.py
│   │   ├── celery/
│   │   │   ├── __init__.py
│   │   │   ├── app.py
│   │   │   ├── mappers.py
│   │   │   └── publisher.py
│   │   ├── email/
│   │   │   ├── __init__.py
│   │   │   └── console_sender.py
│   │   ├── kafka/
│   │   │   ├── __init__.py
│   │   │   └── dead_letter.py
│   │   ├── templates/
│   │   │   ├── __init__.py
│   │   │   └── jinja_renderer.py
│   └── interfaces/
│       ├── __init__.py
│       ├── celery/
│       │   ├── __init__.py
│       │   └── tasks.py
│       ├── contracts/
│       │   ├── __init__.py
│       │   └── v1/
│       │       ├── __init__.py
│       │       ├── email_requested.py
│       │       └── send_email_task.py
│       ├── kafka/
│       │   ├── __init__.py
│       │   ├── consumer.py
│       │   └── handler.py
│       └── mappers.py
└── tests/
    ├── __init__.py
    ├── integration/
    │   ├── __init__.py
    │   └── test_jinja_renderer.py
    └── unit/
        ├── __init__.py
        ├── test_process_notification_event.py
        └── test_send_notification.py
```

Создавай пустые `__init__.py` в перечисленных Python-пакетах.

Существующие черновые файлы `src/core/repositories.py`,
`src/interfaces/contracts/events.py` и `src/interfaces/contracts/tasks.py` для
этого MVP не нужны. Не удаляй их автоматически: сначала сравни старую задумку
с целевой структурой, затем удали осознанно.

`src/core/repositories.py` из `test_psek` описывает repository для domain-модели
`User`. В notification-service БД пока нет, поэтому repository ещё не нужен.
Публикация задачи, рендеринг и отправка email являются зависимостями конкретных
use cases, поэтому их порты находятся в `application/ports`.

---

# Часть I. Чистое ядро без брокеров

## 5. Этап 1: настроить uv и инструменты разработки

### Цель

Подготовить воспроизводимое окружение и короткие команды проверки.

### Команды

Если `pyproject.toml` уже существует, не вызывай `uv init` повторно.

```bash
uv add aiokafka celery "pydantic[email]" pydantic-settings jinja2
uv add --dev pytest pytest-asyncio ruff mypy
```

Приведи `pyproject.toml` к следующему виду. Версии зависимостей, которые уже
добавил `uv`, можно сохранить:

```toml
[project]
name = "transactional-notification-service"
version = "0.1.0"
description = "Kafka-driven transactional notification service"
readme = "README.md"
requires-python = ">=3.13"
dependencies = [
    "aiokafka>=0.14.0",
    "celery>=5.6.3",
    "email-validator>=2.3.0",
    "jinja2>=3.1.6",
    "pydantic>=2.13.4",
    "pydantic-settings>=2.14.1",
]

[dependency-groups]
dev = [
    "mypy>=2.1.0",
    "pytest>=9.0.3",
    "pytest-asyncio>=1.4.0",
    "ruff>=0.15.16",
]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.ruff]
line-length = 100
target-version = "py313"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "ASYNC"]

[tool.mypy]
python_version = "3.13"
strict = true
```

### Проверка

```bash
uv sync
uv run python --version
uv run pytest
uv run ruff check .
```

Пустой набор тестов на этом этапе допустим.

### Что изучить

- `uv add` изменяет `pyproject.toml` и `uv.lock`;
- `uv sync` приводит `.venv` к состоянию lock-файла;
- `uv run` запускает команду внутри окружения проекта;
- `uv.lock` нужно хранить в Git для приложения.

### Самостоятельное упражнение

Добавь команду форматирования:

```bash
uv run ruff format .
```

Посмотри `git diff` и убедись, что понимаешь каждое изменение.

---

## 6. Этап 2: domain-модели

### Цель

Создать небольшие внутренние модели, которые не зависят от библиотек и
транспортов.

#### Сначала попробуй сам

До полного кода попробуй написать две модели:

```text
EmailRecipient:
- email;
- необязательное имя.

RenderedEmail:
- тема письма;
- HTML-версия;
- text-версия.
```

Не используй Pydantic. Представь, что эти модели должны импортироваться даже в
проекте, где вообще не установлены Kafka, Celery, FastAPI и Jinja.

### Файл `src/core/domain/models.py`

```python
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EmailRecipient:
    email: str
    name: str | None = None


@dataclass(frozen=True, slots=True)
class RenderedEmail:
    subject: str
    html_body: str
    text_body: str
```

### Почему так

- `frozen=True` не позволяет случайно изменить данные после создания;
- `slots=True` запрещает случайно добавлять новые атрибуты;
- модели ничего не знают про Pydantic, Kafka или Celery;
- в domain пока нет `Notification` aggregate, потому что без БД и жизненного
  цикла он не приносит пользы.

#### Подробный разбор кода

`EmailRecipient` — это не Kafka payload и не Celery task. Это внутреннее
представление получателя письма. Ему не важно, откуда пришёл email: из Kafka,
из базы, из теста или из ручного скрипта.

`RenderedEmail` — результат работы шаблонизатора. После рендера у нас уже есть
готовые строки: subject, html body и text body. Email sender не должен знать,
как устроена Jinja и какие переменные были в шаблоне. Он получает уже готовое
письмо.

Почему здесь `dataclass`, а не Pydantic:

- domain-модель не должна валидировать внешний JSON;
- domain-модель не должна знать про сериализацию;
- domain-модель должна быть максимально дешёвой и независимой;
- Pydantic полезен на границах и в DTO, но не обязан жить во всём проекте.

Почему `frozen=True` важен практически: если письмо уже отрендерено, мы не
хотим случайно поменять `subject` перед отправкой. Такие случайные изменения
тяжело искать, особенно когда код пойдёт через worker.

Почему `slots=True` полезен для обучения: если ты ошибёшься и напишешь
`recipient.emial = "..."`, Python не создаст новый случайный атрибут, а сразу
упадёт. Это неприятно на секунду, но экономит часы отладки.

### Обязательная проверка

Убедись, что этот файл можно импортировать без установленных Kafka/Celery:

```bash
uv run python -c "from src.core.domain.models import EmailRecipient; print(EmailRecipient('user@example.com'))"
```

### Самостоятельное упражнение

Попробуй изменить `recipient.email` после создания и объясни результат.

---

## 7. Этап 3: application DTO и исключения

### Цель

Описать входные данные use cases независимо от внешних контрактов.

#### Сначала попробуй сам

Не смотри на полный код и попробуй ответить:

```text
ProcessNotificationDTO:
- что должен получить use case, который обрабатывает событие из Kafka?
- нужен ли ему Kafka topic?
- должен ли он знать, что внешний JSON называл поле `type`?
- куда положить переменные шаблона, если сервис должен быть универсальным?

SendEmailDTO:
- что должен получить worker, чтобы отправить письмо?
- нужен ли ему channel, если это уже email task?
- нужен ли task_id?
```

Здесь главная идея: DTO описывает не внешний мир, а удобный вход конкретного
use case. Поэтому DTO может быть похож на внешний контракт, но не обязан быть
его копией.

### Файл `src/application/dto/notification.py`

```python
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class EmailRecipientDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    email: str
    name: str | None = None


class ProcessNotificationDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str = Field(min_length=1, max_length=128)
    notification_type: str = Field(min_length=3, max_length=200)
    channel: str
    user_id: str | None
    recipient: EmailRecipientDTO
    context: dict[str, Any]
    created_at: datetime


class SendEmailDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_id: UUID
    event_id: str
    notification_type: str
    recipient: EmailRecipientDTO
    context: dict[str, Any]
    created_at: datetime
```

#### Подробный разбор DTO

`EmailRecipientDTO` похож на domain `EmailRecipient`, но это не одна и та же
модель. DTO живёт в application-слое и используется как вход use case.
Domain-модель живёт в core/domain и используется там, где мы уже работаем с
понятием получателя внутри системы.

`ProcessNotificationDTO` — вход для сценария `ProcessNotificationEvent`.
Название специально длиннее, чем просто `NotificationDTO`: оно говорит, для
какого действия предназначены данные.

Поля:

- `event_id` нужен для трассировки и будущей идемпотентности;
- `notification_type` выбирает шаблон, например `auth.email_confirmation`;
- `channel` пока проверяется use case-ом, потому что позже могут появиться
  `sms`, `telegram`, `push`;
- `user_id` может пригодиться для логов, аудита и будущей БД;
- `recipient` отделён от `context`, потому что получатель является системным
  полем, а не произвольной переменной шаблона;
- `context` остаётся словарём, потому что сервис должен быть универсальным:
  разные проекты и разные типы писем будут передавать разные переменные;
- `created_at` берётся из исходного event и показывает, когда событие было
  создано внешним сервисом.

`SendEmailDTO` — уже команда для отправки email. В ней нет `channel`, потому
что после обработки Kafka event мы уже приняли решение: это email.

Почему `context` не заменён отдельными полями `verification_url` и
`expires_at`: текущий контракт действительно содержит эти поля, но сервис
задуман универсальным. Для другого письма это могут быть `order_id`,
`receipt_url`, `support_email`, `amount`, `currency` и так далее. Если каждый
раз менять DTO, сервис быстро станет привязанным к одному проекту.

Почему `extra="forbid"` даже во внутренних DTO: если ты случайно передал
`verificaton_url` с опечаткой, Pydantic не должен молча принять объект. Лучше
упасть сразу, чем отправить письмо без важной ссылки.

### Файл `src/application/exceptions.py`

```python
class ApplicationError(Exception):
    """Base class for expected application errors."""


class UnsupportedChannelError(ApplicationError):
    def __init__(self, channel: str) -> None:
        super().__init__(f"Unsupported notification channel: {channel}")
        self.channel = channel
```

#### Разбор исключений

`ApplicationError` — общий базовый класс для ожидаемых ошибок application-слоя.
Это не обязательно на первом MVP, но удобно: adapter-ы могут отличать
предсказуемые ошибки бизнес-сценариев от неожиданных багов.

`UnsupportedChannelError` означает: событие валидное как JSON, но приложение
пока не умеет обработать указанный канал. Это отличается от ошибки Kafka,
ошибки RabbitMQ или ошибки шаблонизатора.

Почему exception находится в `application`, а не в `interfaces`: решение о
поддерживаемых каналах принимает use case, а не Kafka adapter.

### Почему `notification_type`, а не `type`

Во внешнем JSON поле называется `type`, потому что это часть контракта. Во
внутреннем Python-коде более явное имя `notification_type` читается лучше и не
перекрывает встроенную функцию `type`.

### Почему DTO и wire-контракты не объединены

Оба вида моделей используют Pydantic, но выполняют разные роли:

- `interfaces/contracts/v1` фиксирует внешний JSON и его версию;
- `application/dto` фиксирует удобный вход конкретного use case;
- mapper между ними защищает application от изменений транспорта.

Application DTO не содержит имён Kafka topics, Celery queues или деталей
сериализации.

---

## 8. Этап 4: application-порты

### Цель

Описать, что требуется use cases от внешнего мира, не выбирая реализацию.

#### Сначала попробуй сам

Представь, что use case хочет:

```text
1. Опубликовать email task.
2. Отрендерить шаблон.
3. Отправить готовое письмо.
```

Попробуй написать только интерфейсы этих действий без Celery, RabbitMQ, Jinja
и SMTP. Если в твоём варианте появился импорт `celery` или `jinja2`, значит ты
уже ушёл из application-слоя во внешний adapter.

### Файл `src/application/ports/task_publisher.py`

```python
from typing import Protocol

from src.application.dto.notification import SendEmailDTO


class TaskPublisher(Protocol):
    async def publish(self, task: SendEmailDTO) -> None: ...
```

`TaskPublisher` отвечает только на вопрос: «как use case попросит внешний мир
поставить задачу на отправку email?». Он не знает, будет ли это Celery,
RabbitMQ, Redis Queue, HTTP-запрос или простая fake-реализация в тесте.

Метод `publish` сделан `async`, потому что реальная публикация в брокер — это
сетевой I/O. Даже если Celery внутри синхронный, adapter сможет обернуть вызов
так, чтобы внешний контракт use case оставался асинхронным.

### Файл `src/application/ports/template_renderer.py`

```python
from collections.abc import Mapping
from typing import Any, Protocol

from src.core.domain.models import RenderedEmail


class TemplateRenderer(Protocol):
    def render(
        self,
        template_code: str,
        context: Mapping[str, Any],
    ) -> RenderedEmail: ...
```

`TemplateRenderer` принимает `template_code` и `context`. Он не принимает путь
к файлу, потому что путь — деталь Jinja adapter-а. Use case говорит:
«отрендери шаблон такого типа», а не «открой файл вот тут».

`Mapping[str, Any]` выбран вместо `dict[str, Any]`, потому что renderer только
читает context. Ему не нужно право изменять словарь.

### Файл `src/application/ports/email_sender.py`

```python
from typing import Protocol

from src.core.domain.models import EmailRecipient, RenderedEmail


class EmailSender(Protocol):
    def send(self, recipient: EmailRecipient, email: RenderedEmail) -> None: ...
```

`EmailSender` получает уже готовый `RenderedEmail`. Это важно: отправитель не
должен сам выбирать шаблон, лезть в context или знать про Jinja. Его работа —
доставить готовое письмо конкретному получателю.

В MVP sender будет просто писать письмо в лог. Позже здесь появится SMTP или
провайдер вроде SendGrid/Mailgun, но use case от этого не изменится.

### Почему `Protocol`, а не `ABC`

`Protocol` использует структурную типизацию. Реализации не обязаны наследовать
порт явно: достаточно иметь метод с нужной сигнатурой.

Для этого проекта оба варианта допустимы. `Protocol` уменьшает связанность и
упрощает fake-реализации в тестах.

### Почему эти порты находятся в `application`

Они описывают внешние действия, необходимые конкретным use cases:

- `ProcessNotificationEvent` требует публикацию `SendEmailDTO`;
- `SendNotification` требует renderer и sender.

В будущем repository для полноценной domain-модели `Notification` можно
расположить в `core/repositories.py`, аналогично `test_psek`.

### Архитектурная проверка

В application-портах не должно быть импортов:

```text
celery
aiokafka
jinja2
pydantic
sqlalchemy
```

---

## 9. Этап 5: use case обработки Kafka event

### Цель

Создать и протестировать бизнес-сценарий до подключения Kafka и Celery.

#### Сначала попробуй сам

До полного кода попробуй написать черновик класса:

```text
Класс: ProcessNotificationEvent

Он должен:
- получить ProcessNotificationDTO;
- проверить, что channel == "email";
- создать SendEmailDTO;
- опубликовать SendEmailDTO через publisher;
- вернуть созданную task.

В конструктор передай:
- publisher;
- id_factory для task_id;
- clock для created_at.
```

В черновике специально подумай над двумя вопросами:

```text
1. Где должен создаваться task_id: в mapper, use case или Celery adapter?
2. Нужно ли ловить ошибку publisher внутри use case?
```

Ответы будут понятнее после полного кода.

### Файл `src/application/use_case/process_notification_event.py`

```python
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID, uuid4

from src.application.dto.notification import ProcessNotificationDTO, SendEmailDTO
from src.application.exceptions import UnsupportedChannelError
from src.application.ports.task_publisher import TaskPublisher


def utc_now() -> datetime:
    return datetime.now(UTC)


class ProcessNotificationEvent:
    def __init__(
        self,
        publisher: TaskPublisher,
        *,
        id_factory: Callable[[], UUID] = uuid4,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._publisher = publisher
        self._id_factory = id_factory
        self._clock = clock

    async def execute(self, dto: ProcessNotificationDTO) -> SendEmailDTO:
        if dto.channel != "email":
            raise UnsupportedChannelError(dto.channel)

        task = SendEmailDTO(
            task_id=self._id_factory(),
            event_id=dto.event_id,
            notification_type=dto.notification_type,
            recipient=dto.recipient,
            context=dict(dto.context),
            created_at=self._clock(),
        )

        await self._publisher.publish(task)
        return task
```

#### Что здесь выполнено

Этот use case превращает входное событие уровня приложения в команду отправки
email. Он ещё не отправляет письмо. Его ответственность уже и точнее:

```text
ProcessNotificationDTO
    ↓
проверка поддерживаемого channel
    ↓
SendEmailDTO
    ↓
TaskPublisher.publish()
```

Такой use case является мостом между входным consumer-ом и внутренними
worker-ами, но не знает ни о Kafka, ни о RabbitMQ.

#### Подробный разбор кода

`publisher: TaskPublisher` передаётся через конструктор. Это dependency
injection: use case не создаёт Celery-клиент сам. В тесте мы дадим fake
publisher, а в production — Celery adapter.

`id_factory` по умолчанию равен `uuid4`. Это маленькая, но очень полезная
деталь. Если внутри теста оставить настоящий `uuid4()`, результат каждый раз
будет разным и assert станет неудобным. Поэтому в тесте мы передаём
`lambda: TASK_ID`.

`clock` работает по той же причине. В production нужна текущая дата, а в тесте
фиксированная. Так мы убираем случайность из unit-теста.

Проверка:

```python
if dto.channel != "email":
    raise UnsupportedChannelError(dto.channel)
```

пока выглядит простой, но она важна архитектурно. Внешний event может однажды
принести `sms`, `telegram` или `push`. Kafka handler должен только разобрать
сообщение, а решение «умеет ли приложение такой канал» остаётся внутри
application-слоя.

Создание task:

```python
task = SendEmailDTO(...)
```

является переходом от «мы получили notification event» к «нужно отправить
email». Поэтому у `SendEmailDTO` уже нет поля `channel`: оно стало лишним,
потому что тип команды уже email-specific.

Строка:

```python
context=dict(dto.context)
```

создаёт копию словаря. Это маленькая защита: если кто-то снаружи держит ссылку
на исходный `context`, он не должен незаметно менять task после её создания.

Строка:

```python
await self._publisher.publish(task)
```

не обёрнута в `try/except`. Это осознанно. Если RabbitMQ недоступен, ошибка
должна подняться до Kafka adapter-а. Тогда Kafka offset не будет подтверждён,
и сообщение можно будет обработать позже.

Самая опасная ошибка в этом use case — написать так:

```python
try:
    await self._publisher.publish(task)
except ConnectionError:
    pass
```

Так код «не падает», но событие теряется: Kafka consumer может решить, что всё
успешно, подтвердить offset, а task в RabbitMQ так и не появится.

### Важное поведение

Use case не перехватывает исключение publisher:

```python
await self._publisher.publish(task)
```

Если RabbitMQ недоступен, исключение должно выйти наружу. Kafka adapter увидит
ошибку и не подтвердит offset.

Это **обязательное** поведение.

### Почему внедрены `id_factory` и `clock`

В production используются `uuid4()` и текущее время. В тестах можно передать
фиксированные значения и не писать нестабильные проверки.

### Файл `tests/unit/test_process_notification_event.py`

#### Сначала попробуй сам написать тесты

Перед полным кодом попробуй набросать три проверки:

```text
1. Для channel=email use case публикует SendEmailDTO.
2. Для channel=sms use case кидает UnsupportedChannelError.
3. Если publisher падает, use case не скрывает ошибку.
```

Тебе понадобятся две fake-реализации:

```text
FakeTaskPublisher:
- сохраняет опубликованные task в список.

FailingTaskPublisher:
- всегда кидает ConnectionError.
```

Если тяжело написать сразу весь тест, начни с первого: создай DTO, вызови
`execute`, проверь результат и список `publisher.published`.

```python
from datetime import UTC, datetime
from uuid import UUID

import pytest

from src.application.dto.notification import (
    EmailRecipientDTO,
    ProcessNotificationDTO,
    SendEmailDTO,
)
from src.application.exceptions import UnsupportedChannelError
from src.application.use_case.process_notification_event import ProcessNotificationEvent

TASK_ID = UUID("11111111-1111-1111-1111-111111111111")
CREATED_AT = datetime(2026, 6, 11, 12, 0, tzinfo=UTC)


class FakeTaskPublisher:
    def __init__(self) -> None:
        self.published: list[SendEmailDTO] = []

    async def publish(self, task: SendEmailDTO) -> None:
        self.published.append(task)


class FailingTaskPublisher:
    async def publish(self, task: SendEmailDTO) -> None:
        raise ConnectionError("RabbitMQ is unavailable")


def make_dto(channel: str = "email") -> ProcessNotificationDTO:
    return ProcessNotificationDTO(
        event_id="event-1",
        notification_type="auth.email_confirmation",
        channel=channel,
        user_id="user-1",
        recipient=EmailRecipientDTO(email="user@example.com", name="Ivan"),
        context={
            "verification_url": "https://example.com/verify",
            "expires_at": "2026-06-16T12:30:00+00:00",
        },
        created_at=datetime(2026, 6, 11, 11, 59, tzinfo=UTC),
    )


async def test_publishes_email_task() -> None:
    publisher = FakeTaskPublisher()
    use_case = ProcessNotificationEvent(
        publisher,
        id_factory=lambda: TASK_ID,
        clock=lambda: CREATED_AT,
    )

    task = await use_case.execute(make_dto())

    assert task == SendEmailDTO(
        task_id=TASK_ID,
        event_id="event-1",
        notification_type="auth.email_confirmation",
        recipient=EmailRecipientDTO(email="user@example.com", name="Ivan"),
        context={
            "verification_url": "https://example.com/verify",
            "expires_at": "2026-06-16T12:30:00+00:00",
        },
        created_at=CREATED_AT,
    )
    assert publisher.published == [task]


async def test_rejects_unsupported_channel() -> None:
    publisher = FakeTaskPublisher()
    use_case = ProcessNotificationEvent(publisher)

    with pytest.raises(UnsupportedChannelError):
        await use_case.execute(make_dto(channel="sms"))

    assert publisher.published == []


async def test_does_not_hide_publisher_error() -> None:
    use_case = ProcessNotificationEvent(FailingTaskPublisher())

    with pytest.raises(ConnectionError, match="RabbitMQ is unavailable"):
        await use_case.execute(make_dto())
```

#### Подробный разбор теста

`FakeTaskPublisher` — это не mock ради mock-а. Это маленькая контролируемая
реализация порта `TaskPublisher`. Она показывает, зачем мы вообще вводили port:
use case можно проверить без RabbitMQ и Celery.

`published: list[SendEmailDTO]` хранит всё, что use case попытался
опубликовать. После вызова `execute` мы проверяем:

```python
assert publisher.published == [task]
```

Это доказывает две вещи:

- use case вернул созданную task;
- use case действительно передал ту же task во внешний publisher.

`FailingTaskPublisher` нужен для проверки отрицательного сценария. Он имитирует
недоступный RabbitMQ. Тест:

```python
with pytest.raises(ConnectionError):
    await use_case.execute(make_dto())
```

закрепляет важное поведение: use case не должен прятать сетевую ошибку.

`make_dto()` — локальная фабрика тестовых данных. Она не часть production-кода.
Её задача — убрать шум из тестов. Вместо того чтобы в каждом тесте заново
писать большой `ProcessNotificationDTO`, мы пишем один helper и меняем только
то, что важно для конкретного сценария:

```python
make_dto(channel="sms")
```

Почему тест async: метод `execute` async, потому что публикация в broker в
реальности является I/O-операцией. Даже fake publisher должен иметь async
метод, чтобы соответствовать порту.

Частые ошибки при переписывании этого файла:

- случайная запятая после объекта: `publisher = FakeTaskPublisher(),` создаёт
  tuple, а не publisher;
- забыть вызвать clock: `self._clock` вместо `self._clock()`;
- положить `make_dto` внутрь класса из-за лишнего отступа;
- написать `Unsuported` вместо `Unsupported`;
- перехватить `ConnectionError` внутри use case и тем самым сломать гарантию
  доставки.

### Проверка

```bash
uv run pytest tests/unit/test_process_notification_event.py -vv
```

Ожидаемый результат:

```text
3 passed
```

### Самостоятельное упражнение

Временно оберни `publish()` в `try/except ConnectionError: pass`. Запусти тесты,
посмотри падение и объясни, почему такое подавление ошибки приведёт к потере
уведомления.

---

## 10. Этап 6: use case отправки email

### Цель

Описать отправку независимо от Celery, Jinja и конкретного email-провайдера.

#### Сначала попробуй сам

Напиши черновик use case:

```text
Класс: SendNotification

Он должен:
- получить SendEmailDTO;
- создать domain EmailRecipient;
- подготовить context для renderer;
- отрендерить письмо;
- передать готовое письмо sender-у.
```

Подумай, кто должен добавить `recipient` в context:

```text
1. внешний сервис в Kafka event;
2. mapper;
3. SendNotification use case.
```

В этом плане выбираем третий вариант: use case доверяет собственному DTO, а не
произвольному словарю из внешнего мира.

### Файл `src/application/use_case/send_notification.py`

```python
from src.application.dto.notification import SendEmailDTO
from src.application.ports.email_sender import EmailSender
from src.application.ports.template_renderer import TemplateRenderer
from src.core.domain.models import EmailRecipient


class SendNotification:
    def __init__(
        self,
        renderer: TemplateRenderer,
        sender: EmailSender,
    ) -> None:
        self._renderer = renderer
        self._sender = sender

    def execute(self, dto: SendEmailDTO) -> None:
        recipient = EmailRecipient(
            email=dto.recipient.email,
            name=dto.recipient.name,
        )
        context = dict(dto.context)
        context["recipient"] = {
            "email": recipient.email,
            "name": recipient.name,
        }

        email = self._renderer.render(
            dto.notification_type,
            context,
        )
        self._sender.send(recipient, email)
```

#### Что здесь выполнено

Этот use case делает вторую половину работы:

```text
SendEmailDTO
    ↓
подготовка recipient и context
    ↓
TemplateRenderer.render()
    ↓
EmailSender.send()
```

Он не знает, что task пришла из Celery. Для него это просто команда отправки
email.

#### Подробный разбор кода

Сначала use case создаёт domain-модель:

```python
recipient = EmailRecipient(...)
```

Зачем не передать `EmailRecipientDTO` сразу в sender? Потому что sender
работает с доменным понятием получателя, а не с application DTO. Это тонкая,
но полезная граница: DTO — форма входа use case, domain model — внутренний
язык системы.

Дальше создаётся:

```python
context = dict(dto.context)
```

Мы не изменяем `dto.context` напрямую. DTO объявлен frozen, но вложенный dict
всё равно остаётся изменяемым объектом. Копия защищает исходные данные.

Потом use case добавляет системное поле:

```python
context["recipient"] = {
    "email": recipient.email,
    "name": recipient.name,
}
```

Это значит: внешний producer может передать `verification_url`, `expires_at`,
`order_id` и любые другие переменные шаблона, но `recipient` контролирует сам
notification-service. Так шаблон может обращаться к `recipient.email`, а мы
уверены, что эти данные взяты из DTO, а не из произвольного `context`.

Рендер:

```python
email = self._renderer.render(dto.notification_type, context)
```

отделён от отправки. Renderer отвечает за текст письма, sender — за доставку.
Именно поэтому тест может проверить их отдельно.

### Почему use case добавляет `recipient` в context

Данные получателя уже являются частью внутренней команды. Внешний producer не
должен дублировать имя и email в произвольном `context`.

Ключ `recipient` является зарезервированным системным ключом: use case всегда
перезаписывает его достоверными данными из `dto.recipient`.

### Файл `tests/unit/test_send_notification.py`

#### Сначала попробуй сам написать тест

Перед полным кодом попробуй сделать fake renderer и fake sender:

```text
FakeTemplateRenderer:
- запоминает template_code и context;
- возвращает готовый RenderedEmail.

FakeEmailSender:
- запоминает recipient и email.
```

Тест должен доказать:

```text
1. use case передал renderer-у правильный notification_type;
2. context содержит verification_url, expires_at и recipient;
3. sender получил domain EmailRecipient;
4. sender получил RenderedEmail, который вернул renderer.
```

```python
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from src.application.dto.notification import EmailRecipientDTO, SendEmailDTO
from src.application.use_case.send_notification import SendNotification
from src.core.domain.models import EmailRecipient, RenderedEmail


class FakeTemplateRenderer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, Any]]] = []

    def render(
        self,
        template_code: str,
        context: Mapping[str, Any],
    ) -> RenderedEmail:
        self.calls.append((template_code, context))
        return RenderedEmail(
            subject="Confirm email",
            html_body="<p>Confirm</p>",
            text_body="Confirm",
        )


class FakeEmailSender:
    def __init__(self) -> None:
        self.sent: list[tuple[EmailRecipient, RenderedEmail]] = []

    def send(self, recipient: EmailRecipient, email: RenderedEmail) -> None:
        self.sent.append((recipient, email))


def test_renders_and_sends_email() -> None:
    renderer = FakeTemplateRenderer()
    sender = FakeEmailSender()
    use_case = SendNotification(renderer, sender)
    dto = SendEmailDTO(
        task_id=UUID("11111111-1111-1111-1111-111111111111"),
        event_id="event-1",
        notification_type="auth.email_confirmation",
        recipient=EmailRecipientDTO(email="user@example.com", name="Ivan"),
        context={
            "verification_url": "https://example.com/verify",
            "expires_at": "2026-06-16T12:30:00+00:00",
        },
        created_at=datetime(2026, 6, 11, 12, 0, tzinfo=UTC),
    )

    use_case.execute(dto)

    assert renderer.calls == [
        (
            "auth.email_confirmation",
            {
                "verification_url": "https://example.com/verify",
                "expires_at": "2026-06-16T12:30:00+00:00",
                "recipient": {
                    "email": "user@example.com",
                    "name": "Ivan",
                },
            },
        )
    ]
    assert sender.sent == [
        (
            EmailRecipient(email="user@example.com", name="Ivan"),
            RenderedEmail(
                subject="Confirm email",
                html_body="<p>Confirm</p>",
                text_body="Confirm",
            ),
        )
    ]
```

#### Подробный разбор теста

Этот тест не проверяет Jinja и не проверяет SMTP. Он проверяет только
координацию внутри use case:

```text
DTO → renderer → sender
```

`FakeTemplateRenderer.calls` хранит историю вызовов renderer-а. Это позволяет
проверить, какой именно context собрал use case. Здесь важно убедиться, что
`recipient` добавлен самим приложением.

`FakeEmailSender.sent` хранит отправленные письма. Мы проверяем, что sender
получил domain `EmailRecipient`, а не DTO. Это маленькая проверка чистоты
границы между application и domain.

Тест не обязан знать, как Jinja ищет файлы. Это будет отдельный integration
test для Jinja adapter-а.

### Проверка

```bash
uv run pytest tests/unit -vv
```

### Что уже достигнуто

Оба главных сценария сервиса протестированы без Kafka, RabbitMQ, Celery и Jinja.
Это главное преимущество правильно проведённых архитектурных границ.

---

# Часть II. Внешние контракты

## 11. Этап 7: Kafka-контракт v1

### Цель

Провалидировать данные на входной границе и явно версионировать контракт.

#### Сначала попробуй сам

Посмотри на контракт другого сервиса:

```go
type EmailRequested struct {
    EventID         string    `json:"event_id"`
    Type            string    `json:"type"`
    UserID          string    `json:"user_id"`
    Email           string    `json:"email"`
    VerificationURL string    `json:"verification_url"`
    ExpiresAt       time.Time `json:"expires_at"`
    CreatedAt       time.Time `json:"created_at"`
}
```

Попробуй сам описать Pydantic-модель:

```text
- запрети лишние поля;
- проверь email как email;
- проверь verification_url как URL;
- потребуй timezone у expires_at и created_at;
- не добавляй channel, recipient или context: их нет во внешнем JSON.
```

Здесь важно не «улучшить» чужой контракт, а точно принять то, что реально
приходит из Kafka.

### Файл `src/interfaces/contracts/v1/email_requested.py`

```python
from datetime import datetime

from pydantic import BaseModel, ConfigDict, EmailStr, Field, HttpUrl, field_validator


class EmailRequestedV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1, max_length=128)
    type: str = Field(
        min_length=3,
        max_length=200,
        pattern=r"^[a-z0-9_]+(?:\.[a-z0-9_]+)+$",
    )
    user_id: str = Field(min_length=1, max_length=128)
    email: EmailStr
    verification_url: HttpUrl
    expires_at: datetime
    created_at: datetime

    @field_validator("expires_at", "created_at")
    @classmethod
    def datetime_must_have_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime value must include timezone")
        return value
```

Этот контракт соответствует данным с изображения:

```go
type EmailRequested struct {
    EventID         string    `json:"event_id"`
    Type            string    `json:"type"`
    UserID          string    `json:"user_id"`
    Email           string    `json:"email"`
    VerificationURL string    `json:"verification_url"`
    ExpiresAt       time.Time `json:"expires_at"`
    CreatedAt       time.Time `json:"created_at"`
}
```

Во входящем JSON нет `channel`, `recipient` и `context`. Это нормально:
контракт уже email-specific, поэтому mapper ниже сам выставит `channel="email"`,
создаст recipient из поля `email` и соберёт context для шаблона.

#### Подробный разбор контракта

`EmailRequestedV1` живёт в `interfaces/contracts/v1`, потому что это внешний
wire-контракт. Он описывает не удобную модель приложения, а конкретный JSON,
который прилетает из Kafka.

Почему имя не `ProcessNotificationDTO`: потому что этот класс не является
входом use case. Его задача — проверить внешний payload и не пустить мусор
глубже в систему.

`EmailStr` и `HttpUrl` используются именно здесь, на границе. Если внешний
сервис отправил не email или не URL, сообщение считается невалидным ещё до
application-слоя.

Validator для дат проверяет timezone. Без timezone строка вроде
`2026-06-16T12:30:00` неоднозначна: это UTC, Москва или локальное время
контейнера? Для сообщений между сервисами такая неопределённость опасна.

Regex у `type` допускает значения вида:

```text
auth.email_confirmation
order.receipt_created
billing.payment_failed
```

и не допускает `/` или `..`. Это важно, потому что позже `type` будет
превращаться в путь к шаблону.

### Почему `extra="forbid"`

Неизвестное поле часто означает опечатку или несовпадение версий контракта.
Явный отказ помогает раньше обнаружить ошибку интеграции.

---

## 12. Этап 8: контракт Celery task v1

#### Сначала попробуй сам

Теперь представь сообщение, которое notification-service сам кладёт в
RabbitMQ/Celery:

```text
Нужно передать worker-у:
- task_id;
- event_id;
- type шаблона;
- email recipient;
- context для шаблона;
- created_at.
```

Попробуй написать модель так, чтобы worker снова валидировал вход. Не
рассчитывай на то, что «это же наше сообщение, значит оно точно правильное».

### Файл `src/interfaces/contracts/v1/send_email_task.py`

```python
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

SEND_EMAIL_TASK_NAME = "notification.send_email.v1"


class EmailTaskRecipientV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: EmailStr
    name: str | None = Field(default=None, max_length=200)


class SendEmailTaskV1(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: UUID
    event_id: str = Field(min_length=1, max_length=128)
    type: str = Field(
        min_length=3,
        max_length=200,
        pattern=r"^[a-z0-9_]+(?:\.[a-z0-9_]+)+$",
    )
    channel: Literal["email"] = "email"
    recipient: EmailTaskRecipientV1
    context: dict[str, Any]
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def created_at_must_have_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must include timezone")
        return value
```

### Почему Celery task снова валидируется worker-ом

Publisher и worker могут работать с разными версиями приложения. Сообщение в
RabbitMQ является внешним вводом для worker, даже если его создал наш же
сервис.

Нельзя считать входные данные доверенными только потому, что они пришли из
внутренней очереди.

#### Подробный разбор task-контракта

`SendEmailTaskV1` — это не application DTO, хотя он очень похож на
`SendEmailDTO`. Разница в роли:

```text
SendEmailDTO      — объект внутри application-слоя.
SendEmailTaskV1   — JSON-контракт сообщения в RabbitMQ/Celery.
```

Почему task снова содержит `context`: RabbitMQ является границей процесса. Даже
если consumer и worker лежат в одном репозитории, они могут запускаться разными
версиями кода. Поэтому worker обязан заново проверить сообщение.

Почему здесь `channel: Literal["email"]`: эта task уже предназначена только
для email worker-а. В отличие от Kafka event, где могут появиться разные
каналы, внутренняя Celery task в этом этапе конкретная.

`SEND_EMAIL_TASK_NAME` вынесен в константу, чтобы producer и worker не
расходились в строке имени task. Ошибка в одной букве приведёт к тому, что
Celery не найдёт зарегистрированную задачу.

---

## 13. Этап 9: мапперы между границами

### Цель

Не позволить внешним wire-контрактам проникнуть в application-слой.

#### Сначала попробуй сам

Напиши две функции на черновике:

```text
email_event_to_dto:
- принимает EmailRequestedV1;
- возвращает ProcessNotificationDTO;
- channel ставит равным "email";
- email превращает в EmailRecipientDTO;
- verification_url и expires_at кладёт в context.

send_email_task_to_dto:
- принимает SendEmailTaskV1;
- возвращает SendEmailDTO;
- копирует context.
```

Главное правило: mapper может знать сразу о двух слоях. Это его работа. Но
use case не должен знать о `EmailRequestedV1` или `SendEmailTaskV1`.

### Файл `src/interfaces/mappers.py`

```python
from src.application.dto.notification import (
    EmailRecipientDTO,
    ProcessNotificationDTO,
    SendEmailDTO,
)
from src.interfaces.contracts.v1.email_requested import EmailRequestedV1
from src.interfaces.contracts.v1.send_email_task import SendEmailTaskV1


def email_event_to_dto(
    event: EmailRequestedV1,
) -> ProcessNotificationDTO:
    return ProcessNotificationDTO(
        event_id=event.event_id,
        notification_type=event.type,
        channel="email",
        user_id=event.user_id,
        recipient=EmailRecipientDTO(
            email=str(event.email),
            name=None,
        ),
        context={
            "verification_url": str(event.verification_url),
            "expires_at": event.expires_at.isoformat(),
        },
        created_at=event.created_at,
    )


def send_email_task_to_dto(task: SendEmailTaskV1) -> SendEmailDTO:
    return SendEmailDTO(
        task_id=task.task_id,
        event_id=task.event_id,
        notification_type=task.type,
        recipient=EmailRecipientDTO(
            email=str(task.recipient.email),
            name=task.recipient.name,
        ),
        context=dict(task.context),
        created_at=task.created_at,
    )
```

### Архитектурная проверка

`application` ничего не знает о `EmailRequestedV1` и
`SendEmailTaskV1`. Только внешний слой знает одновременно о wire-контрактах и
application DTO.

#### Подробный разбор mapper-а

`email_event_to_dto` выполняет адаптацию конкретного входного события к
универсальной модели приложения:

```text
EmailRequestedV1.email
    ↓
EmailRecipientDTO.email

EmailRequestedV1.verification_url
EmailRequestedV1.expires_at
    ↓
ProcessNotificationDTO.context
```

Почему `verification_url` и `expires_at` уходят в `context`, а не становятся
полями DTO: текущий event относится к подтверждению email, но сервис должен
уметь обслуживать разные проекты и разные шаблоны. Для одного шаблона нужен
`verification_url`, для другого `receipt_url`, для третьего `reset_url`.
Универсальная часть сервиса не должна меняться при каждом новом шаблоне.

Почему `channel="email"` выставляется здесь: внешний контракт уже называется
`EmailRequested`, значит он email-specific. Во внешнем JSON нет поля channel,
но application use case ожидает channel, потому что позже может принимать
другие типы уведомлений из других mapper-ов.

`send_email_task_to_dto` делает обратную адаптацию на стороне worker-а:
RabbitMQ payload снова превращается в application DTO. Worker после этого
работает так, будто данные пришли из обычного вызова Python, а не из брокера.

Mapper — нормальное место для «склейки» слоёв. Плохо, если такая склейка
размазывается по use case, consumer и worker одновременно.

### Самостоятельное упражнение

Напиши unit-тест, который:

1. Создаёт `EmailRequestedV1`.
2. Преобразует его в `ProcessNotificationDTO`.
3. Проверяет, что `email` превратился в recipient, а `verification_url` и
   `expires_at` попали в context.

---

# Часть III. Jinja и Console email

## 14. Этап 10: шаблоны писем

### Цель

Хранить шаблоны отдельно от Python-кода и выбирать их по notification type.

#### Сначала попробуй сам

Создай мысленно директорию для типа:

```text
auth.email_confirmation
        ↓
email_templates/auth/email_confirmation/
```

Попробуй решить, какие три файла нужны письму:

```text
subject.txt  — тема письма;
body.html    — HTML-версия;
body.txt     — текстовая версия.
```

В шаблоне используй только значения из `context`, который mapper положил в
DTO: `verification_url` и `expires_at`. Не добавляй `app_name`, если его нет во
входном event.

### Файл `email_templates/auth/email_confirmation/subject.txt`

```jinja2
Подтвердите email
```

### Файл `email_templates/auth/email_confirmation/body.html`

```jinja2
<!doctype html>
<html lang="ru">
  <body>
    <p>Здравствуйте.</p>
    <p>Подтвердите ваш email:</p>
    <p><a href="{{ verification_url }}">Подтвердить email</a></p>
    <p>Ссылка действует до {{ expires_at }}.</p>
  </body>
</html>
```

### Файл `email_templates/auth/email_confirmation/body.txt`

```jinja2
Здравствуйте.

Подтвердите ваш email:
{{ verification_url }}

Ссылка действует до {{ expires_at }}.
```

Для MVP достаточно шаблона `auth.email_confirmation`, потому что входной
контракт с изображения содержит именно `verification_url` и `expires_at`.
Другие типы писем потребуют либо новых полей во входном контракте, либо нового
contract version.

#### Подробный разбор шаблонов

Шаблон — это не Python-код, но он тоже часть контракта. Если `body.html`
использует `{{ verification_url }}`, значит renderer должен получить
`verification_url` в context. Поэтому mapper и use case обязаны сохранить это
значение.

Почему есть и HTML, и text:

- HTML нужен для нормального вида письма;
- text нужен для клиентов, которые не показывают HTML;
- text полезен для логов, тестов и отладки;
- некоторые email-провайдеры и антиспам-системы лучше относятся к письмам,
  где есть обе версии.

Почему шаблон не получает весь DTO: Jinja не должна знать про `event_id`,
`task_id`, Kafka offset и другие служебные поля. Шаблон получает только то, что
нужно для текста письма.

### Важное соглашение

Notification type преобразуется в путь:

```text
auth.email_confirmation
        ↓
auth/email_confirmation/
```

Regex в Kafka-контракте не допускает `/` и `..`, поэтому внешний event не
сможет произвольно читать файлы вне директории шаблонов.

---

## 15. Этап 11: Jinja adapter

#### Сначала попробуй сам

До полного кода попробуй описать класс:

```text
JinjaTemplateRenderer:
- получает templates_dir в конструкторе;
- по template_code строит путь директории;
- читает subject.txt, body.html, body.txt;
- рендерит их с context;
- возвращает RenderedEmail.
```

Подумай, что должно произойти, если в шаблоне есть `{{ verification_url }}`, а
в context его нет. В этом плане ответ: рендер должен упасть, а не тихо
подставить пустую строку.

### Файл `src/infrastructure/templates/jinja_renderer.py`

```python
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined, select_autoescape

from src.core.domain.models import RenderedEmail


class JinjaTemplateRenderer:
    def __init__(self, templates_dir: Path) -> None:
        self._environment = Environment(
            loader=FileSystemLoader(templates_dir),
            autoescape=select_autoescape(enabled_extensions=("html", "xml")),
            undefined=StrictUndefined,
        )

    def render(
        self,
        template_code: str,
        context: Mapping[str, Any],
    ) -> RenderedEmail:
        directory = template_code.replace(".", "/")

        subject_template = self._environment.get_template(f"{directory}/subject.txt")
        html_template = self._environment.get_template(f"{directory}/body.html")
        text_template = self._environment.get_template(f"{directory}/body.txt")

        values = dict(context)
        return RenderedEmail(
            subject=subject_template.render(**values).strip(),
            html_body=html_template.render(**values).strip(),
            text_body=text_template.render(**values).strip(),
        )
```

#### Подробный разбор adapter-а

`JinjaTemplateRenderer` находится в `infrastructure`, потому что Jinja — это
конкретная внешняя библиотека. Application-слой знает только порт
`TemplateRenderer`, а не `jinja2.Environment`.

`templates_dir` передаётся в конструктор, а не захардкожен внутри класса. Это
позволяет в тестах использовать временную директорию или локальную папку
`email_templates`.

Строка:

```python
directory = template_code.replace(".", "/")
```

превращает `auth.email_confirmation` в `auth/email_confirmation`. Это простое
соглашение: notification type одновременно является кодом шаблона.

`StrictUndefined` — один из самых важных параметров. Без него Jinja может
молча заменить отсутствующую переменную пустой строкой. Для email это опасно:
можно «успешно» отправить письмо без ссылки подтверждения.

`select_autoescape(enabled_extensions=("html", "xml"))` включает экранирование
для HTML-шаблонов. Если в context попадёт строка с HTML-тегом, Jinja не должна
превратить её в исполняемый HTML.

`RenderedEmail` возвращается как domain-модель. После adapter-а остальная
система больше не зависит от Jinja.

### Почему `StrictUndefined` обязательно

Без него отсутствующая переменная часто превращается в пустую строку. Worker
может «успешно» отправить письмо без ссылки подтверждения.

С `StrictUndefined` рендеринг завершится ошибкой, и проблема станет видимой.

### Файл `tests/integration/test_jinja_renderer.py`

#### Сначала попробуй сам написать integration test

Этот тест уже проверяет реальную Jinja и реальные файлы шаблонов. Попробуй:

```text
1. Создать JinjaTemplateRenderer(Path("email_templates")).
2. Вызвать render("auth.email_confirmation", context).
3. Проверить subject.
4. Проверить, что ссылка попала в html_body и text_body.
5. Отдельно проверить, что без verification_url будет UndefinedError.
```

```python
from pathlib import Path

import pytest
from jinja2 import UndefinedError

from src.infrastructure.templates.jinja_renderer import JinjaTemplateRenderer


def test_renders_email_confirmation_template() -> None:
    renderer = JinjaTemplateRenderer(Path("email_templates"))

    email = renderer.render(
        "auth.email_confirmation",
        {
            "recipient": {
                "email": "user@example.com",
                "name": None,
            },
            "verification_url": "https://example.com/verify",
            "expires_at": "2026-06-16T12:30:00+00:00",
        },
    )

    assert email.subject == "Подтвердите email"
    assert "https://example.com/verify" in email.html_body
    assert "https://example.com/verify" in email.text_body


def test_fails_when_required_context_value_is_missing() -> None:
    renderer = JinjaTemplateRenderer(Path("email_templates"))

    with pytest.raises(UndefinedError):
        renderer.render(
            "auth.email_confirmation",
            {
                "recipient": {
                    "email": "user@example.com",
                    "name": None,
                },
                "expires_at": "2026-06-16T12:30:00+00:00",
            },
        )
```

#### Подробный разбор теста Jinja

Это integration test, потому что он проверяет связку:

```text
файлы шаблонов + JinjaTemplateRenderer + Jinja2
```

В отличие от unit-теста `SendNotification`, здесь мы уже хотим убедиться, что
реальные файлы лежат в правильных директориях и используют правильные имена
переменных.

Тест с `UndefinedError` защищает от тихой поломки писем. Если кто-то удалит
`verification_url` из context или переименует переменную в шаблоне, тест должен
упасть.

### Проверка

```bash
uv run pytest tests/integration/test_jinja_renderer.py -vv
```

### Самостоятельное упражнение

Добавь тест на экранирование HTML в имени:

```text
<script>alert(1)</script>
```

Посмотри разницу между `body.html` и `body.txt`.

---

## 16. Этап 12: ConsoleEmailSender

#### Сначала попробуй сам

Напиши sender, который не отправляет реальный email, а только логирует:

```text
ConsoleEmailSender.send(recipient, email)
```

Ему не нужен SMTP, пароль, TLS или внешний провайдер. Для MVP мы хотим
проверить весь pipeline без риска отправить реальные письма.

### Файл `src/infrastructure/email/console_sender.py`

```python
import logging

from src.core.domain.models import EmailRecipient, RenderedEmail

logger = logging.getLogger(__name__)


class ConsoleEmailSender:
    def send(self, recipient: EmailRecipient, email: RenderedEmail) -> None:
        logger.info(
            "Email sent to console\n"
            "recipient=%s\n"
            "name=%s\n"
            "subject=%s\n"
            "text_body=\n%s\n"
            "html_body=\n%s",
            recipient.email,
            recipient.name,
            email.subject,
            email.text_body,
            email.html_body,
        )
```

#### Подробный разбор ConsoleEmailSender

Этот adapter реализует порт `EmailSender`, но вместо реальной доставки пишет
данные в лог. Это не «игрушка», а безопасная ступень разработки.

Почему нельзя сразу подключать реальный SMTP: до идемпотентности Kafka →
RabbitMQ может породить дубликаты. Если сразу включить SMTP, повторная
доставка event может отправить пользователю два одинаковых письма.

Console sender позволяет проверить:

- Kafka consumer получил событие;
- task попала в RabbitMQ;
- Celery worker взял task;
- Jinja отрендерила письмо;
- use case дошёл до sender-а.

И всё это без реальной отправки наружу.

### Почему это adapter

Application знает только об `EmailSender`. Сегодня реализация пишет письмо в
лог, позже тот же порт будет реализован через SMTP или API провайдера.

`ConsoleEmailSender` допустим только для локальной разработки: он выводит тело
письма, которое может содержать чувствительные ссылки и токены.

---

# Часть IV. Конфигурация и локальная инфраструктура

## 17. Этап 13: настройки приложения

#### Сначала попробуй сам

Перед полным кодом выпиши, какие значения не должны быть захардкожены:

```text
- Kafka bootstrap servers;
- Kafka topic;
- Kafka group id;
- RabbitMQ/Celery broker URL;
- директория шаблонов;
- уровень логирования.
```

Настройки должны читаться из окружения, чтобы один и тот же код запускался
локально, в Docker и позже на сервере.

### Файл `.env.example`

```dotenv
LOG_LEVEL=INFO

KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_NOTIFICATION_TOPIC=notification.requested.v1
KAFKA_DEAD_LETTER_TOPIC=notification.requested.v1.dlq
KAFKA_CONSUMER_GROUP=transactional-notification-service.v1
KAFKA_AUTO_OFFSET_RESET=earliest

CELERY_BROKER_URL=amqp://notification:notification@localhost:5672//
CELERY_EMAIL_QUEUE=notification.email

EMAIL_TEMPLATES_DIR=email_templates
```

Создай локальный `.env`:

```bash
cp .env.example .env
```

Не помещай `.env` с реальными секретами в Git.

### Файл `src/config/settings.py`

```python
from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    log_level: str = "INFO"

    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_notification_topic: str = "notification.requested.v1"
    kafka_dead_letter_topic: str = "notification.requested.v1.dlq"
    kafka_consumer_group: str = "transactional-notification-service.v1"
    kafka_auto_offset_reset: Literal["earliest", "latest"] = "earliest"

    celery_broker_url: str = "amqp://notification:notification@localhost:5672//"
    celery_email_queue: str = "notification.email"

    email_templates_dir: Path = Path("email_templates")


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

#### Подробный разбор настроек

`Settings` находится в `src/config`, потому что это внешний слой приложения:
он знает про env-переменные и инфраструктуру. Domain и application не должны
читать `.env`.

`env_file=".env"` удобен локально: можно положить настройки рядом с проектом.
В production значения обычно приходят через environment variables, и код от
этого не меняется.

`extra="ignore"` в settings допустим: в окружении часто есть много переменных,
которые не относятся к приложению. Это отличается от входных контрактов, где
`extra="forbid"` полезен для строгой проверки payload.

`get_settings()` обёрнут в `lru_cache`, чтобы настройки создавались один раз.
Это удобно и для производительности, и для единообразия: разные части
приложения получают один и тот же объект настроек.

### Файл `src/config/logging.py`

```python
import logging


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
```

#### Подробный разбор logging

Логи настраиваются в entrypoint-ах, а не внутри каждого класса. Adapter-ы и use
case могут получать logger через `logging.getLogger(__name__)`, но не должны
каждый раз решать формат логов.

Для MVP достаточно `basicConfig`. Позже можно заменить формат на JSON, добавить
request/event id и отправку логов в observability stack.

### Почему настройки находятся во внешнем слое

Use cases не должны читать environment variables. Настройки нужны композиции и
конкретным adapters.

---

## 18. Этап 14: Kafka и RabbitMQ в Docker Compose

#### Сначала попробуй сам

До полного compose-файла попробуй нарисовать сервисы:

```text
kafka      — принимает внешние events;
rabbitmq   — брокер Celery task;
```

Подумай, какие порты нужно открыть наружу для локальной разработки:

```text
Kafka: 9092
RabbitMQ: 5672
RabbitMQ Management UI: 15672
```

Главная цель compose на этом этапе — дать локальную инфраструктуру, чтобы
проверять consumer, producer и worker без чужого окружения.

### Файл `compose.yaml`

```yaml
services:
  kafka:
    image: apache/kafka:4.3.0
    restart: unless-stopped
    ports:
      - "9092:9092"
    volumes:
      - kafka_data:/var/lib/kafka/data
    healthcheck:
      test:
        [
          "CMD-SHELL",
          "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1",
        ]
      interval: 10s
      timeout: 10s
      retries: 10

  rabbitmq:
    image: rabbitmq:4.3.1-management-alpine
    hostname: notification-rabbitmq
    restart: unless-stopped
    environment:
      RABBITMQ_DEFAULT_USER: notification
      RABBITMQ_DEFAULT_PASS: notification
    ports:
      - "5672:5672"
      - "15672:15672"
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 10s
      timeout: 10s
      retries: 10

volumes:
  kafka_data:
  rabbitmq_data:
```

#### Подробный разбор compose

Kafka в локальном Docker часто сложнее RabbitMQ из-за advertised listeners.
Внешний producer на твоей машине должен подключаться к адресу, который Kafka
сама отдаёт клиентам. Если этот адрес настроен неверно, контейнер будет
запущен, но Python producer/consumer не сможет нормально подключиться.

RabbitMQ открывает два порта:

- `5672` — AMQP, его использует Celery;
- `15672` — web UI для локальной отладки.

Healthcheck-и не делают сервис production-ready, но помогают локально:
контейнер может быть запущен, а брокер ещё не готов принимать соединения.

Compose-файл не должен содержать секреты реального SMTP или production пароли.
Это локальная инфраструктура для разработки.

### Запуск

```bash
docker compose up -d
docker compose ps
```

Дождись состояния `healthy`.

### Создание Kafka topics

Основной topic:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic notification.requested.v1 \
  --partitions 3 \
  --replication-factor 1
```

Dead-letter topic:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic notification.requested.v1.dlq \
  --partitions 3 \
  --replication-factor 1
```

Проверка:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

Ожидаемые topics:

```text
notification.requested.v1
notification.requested.v1.dlq
```

RabbitMQ Management UI:

```text
http://localhost:15672
login: notification
password: notification
```

### Что важно понять

- `9092` — Kafka client protocol;
- `5672` — AMQP, через него Celery общается с RabbitMQ;
- `15672` — только web-интерфейс RabbitMQ;
- replication factor `1` подходит только для локального single-node Kafka;
- volume сохраняет локальные сообщения между перезапусками контейнера.

### Самостоятельное упражнение

Останови и снова запусти контейнеры:

```bash
docker compose stop
docker compose start
```

Проверь, что topics сохранились.

---

# Часть V. Celery и RabbitMQ

## 19. Этап 15: Celery application

### Цель

Настроить Celery как внешний framework, не допуская его в application-слой.

#### Сначала попробуй сам

Попробуй описать, что должна знать Celery app:

```text
- broker URL;
- формат сериализации;
- timezone;
- имя очереди;
- нужно ли хранить result backend.
```

Не регистрируй task прямо здесь. Сначала создаём приложение Celery, а task
подключим в отдельном adapter-е.

### Файл `src/infrastructure/celery/app.py`

```python
from celery import Celery

from src.config.settings import get_settings
from src.interfaces.contracts.v1.send_email_task import SEND_EMAIL_TASK_NAME

settings = get_settings()

celery_app = Celery(
    "transactional_notification_service",
    broker=settings.celery_broker_url,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_backend=None,
    task_ignore_result=True,
    task_default_queue=settings.celery_email_queue,
    task_routes={
        SEND_EMAIL_TASK_NAME: {"queue": settings.celery_email_queue},
    },
    task_publish_retry=True,
    task_publish_retry_policy={
        "max_retries": 5,
        "interval_start": 0,
        "interval_step": 1,
        "interval_max": 5,
    },
    broker_connection_retry_on_startup=True,
    broker_transport_options={"confirm_publish": True},
    enable_utc=True,
    timezone="UTC",
)
```

#### Подробный разбор Celery app

Этот файл создаёт объект Celery как инфраструктурную зависимость. Он находится
в `infrastructure`, потому что Celery — внешний framework, а не бизнес-правило.

`broker=settings.celery_broker_url` говорит Celery использовать RabbitMQ.
Celery сам не является брокером, он только управляет задачами и worker-ами.

JSON-сериализация выбрана осознанно: сообщение должно быть обычным JSON, а не
Python pickle. Pickle опаснее и хуже подходит для контрактов между процессами.

`task_default_queue` задаёт очередь по умолчанию, а `task_routes` явно
направляет `notification.send_email.v1` в email-очередь. Это пригодится, когда
появятся другие типы worker-ов.

`task_ignore_result=True` отражает смысл email-отправки: нам не нужен result
backend, где Celery хранит результат функции. Для надёжности доставки позже
будет отдельная БД/идемпотентность, а не Celery result backend.

`broker_transport_options={"confirm_publish": True}` включает publisher
confirm для RabbitMQ. Это помогает понять, что RabbitMQ действительно принял
публикацию перед Kafka commit.

### Почему нет result backend

Сервису не требуется получать возвращаемое значение task. Хранить результаты
каждой отправки в Celery backend было бы лишней нагрузкой.

Позже статусы уведомлений будут храниться в PostgreSQL как часть предметной
модели, а не как технический результат Celery.

### Почему JSON

Не используй `pickle` для сообщений из очереди. JSON:

- безопаснее;
- читается человеком;
- не привязан к Python-классам;
- проще версионировать.

### Почему `confirm_publish`

Publisher confirm просит RabbitMQ подтвердить приём публикации. Это усиливает
гарантию перед Kafka commit, но всё равно не делает Kafka и RabbitMQ одной
атомарной транзакцией.

---

## 20. Этап 16: Celery adapter для TaskPublisher

#### Сначала попробуй сам

Нужно реализовать application port:

```text
TaskPublisher.publish(SendEmailDTO)
```

Но реальная Celery отправка принимает имя task и JSON payload. Попробуй
разделить задачу на два шага:

```text
1. SendEmailDTO -> SendEmailTaskV1.
2. SendEmailTaskV1 -> celery.send_task(...).
```

Первый шаг — mapper. Второй шаг — adapter.

### Файл `src/infrastructure/celery/mappers.py`

```python
from src.application.dto.notification import SendEmailDTO
from src.interfaces.contracts.v1.send_email_task import (
    EmailTaskRecipientV1,
    SendEmailTaskV1,
)


def send_email_dto_to_contract(dto: SendEmailDTO) -> SendEmailTaskV1:
    return SendEmailTaskV1(
        task_id=dto.task_id,
        event_id=dto.event_id,
        type=dto.notification_type,
        recipient=EmailTaskRecipientV1(
            email=dto.recipient.email,
            name=dto.recipient.name,
        ),
        context=dict(dto.context),
        created_at=dto.created_at,
    )
```

Этот mapper принадлежит исходящему Celery adapter: он преобразует application
DTO во внешний RabbitMQ/Celery wire-контракт.

#### Подробный разбор Celery mapper-а

Этот mapper похож на `interfaces/mappers.py`, но направление другое:

```text
application DTO
    ↓
wire contract для RabbitMQ/Celery
```

Почему mapper лежит в `infrastructure/celery`, а не рядом с application:
application не должна знать, что её DTO будет сериализован именно в Celery
task. Это решение внешнего слоя.

`context=dict(dto.context)` снова делает копию. Это защищает task payload от
случайного изменения исходного DTO после публикации.

### Файл `src/infrastructure/celery/publisher.py`

```python
import asyncio
from typing import Any

from celery import Celery

from src.application.dto.notification import SendEmailDTO
from src.infrastructure.celery.mappers import send_email_dto_to_contract
from src.interfaces.contracts.v1.send_email_task import SEND_EMAIL_TASK_NAME


class CeleryTaskPublisher:
    def __init__(self, app: Celery, queue: str) -> None:
        self._app = app
        self._queue = queue

    async def publish(self, task: SendEmailDTO) -> None:
        contract = send_email_dto_to_contract(task)
        payload: dict[str, Any] = contract.model_dump(mode="json")

        await asyncio.to_thread(
            self._app.send_task,
            SEND_EMAIL_TASK_NAME,
            kwargs={"payload": payload},
            task_id=str(task.task_id),
            queue=self._queue,
            serializer="json",
        )
```

#### Подробный разбор CeleryTaskPublisher

`CeleryTaskPublisher` реализует port `TaskPublisher`. Для use case он выглядит
как объект с методом `publish`, но внутри использует Celery.

Почему `send_task`, а не прямой импорт task-функции: publisher может работать
в процессе consumer-а, где worker-код не обязан быть импортирован. Ему
достаточно знать имя task и payload.

Почему `asyncio.to_thread`: Celery API синхронный. Kafka consumer будет async,
и если вызвать синхронный `send_task` напрямую, можно заблокировать event loop.
`to_thread` выносит блокирующий вызов в отдельный поток.

`task_id=str(task.task_id)` нужен для трассировки. Но это не дедупликация:
Celery всё равно может выполнить две задачи с одинаковым id, если они попали в
очередь дважды.

### Почему `asyncio.to_thread`

Celery `send_task()` является синхронным вызовом. Kafka consumer работает в
asyncio event loop. Прямой вызов может блокировать event loop во время сетевого
ожидания RabbitMQ.

`asyncio.to_thread()` переносит блокирующий вызов в отдельный thread и позволяет
consumer продолжать обслуживать фоновые задачи aiokafka.

### Почему вызывается `send_task`, а не импортируется Celery task

Kafka consumer знает имя команды, но не обязан импортировать код worker task.
Это уменьшает связанность двух процессов.

### Важное ограничение

Одинаковый `task_id` в Celery не является механизмом дедупликации. Он полезен
для трассировки, но worker всё равно может выполнить две задачи с одним ID.

---

## 21. Этап 17: композиция worker

#### Сначала попробуй сам

Попробуй собрать use case вручную на бумаге:

```text
SendNotification(
    renderer=...,
    sender=...,
)
```

Реши, какие конкретные реализации нужны для MVP:

```text
TemplateRenderer -> JinjaTemplateRenderer
EmailSender      -> ConsoleEmailSender
```

Именно этот файл отвечает на вопрос: «какие adapter-ы подключены сейчас?».

### Файл `src/config/container.py`

```python
from functools import lru_cache

from src.application.use_case.send_notification import SendNotification
from src.config.settings import get_settings
from src.infrastructure.email.console_sender import ConsoleEmailSender
from src.infrastructure.templates.jinja_renderer import JinjaTemplateRenderer


@lru_cache
def get_send_notification_use_case() -> SendNotification:
    settings = get_settings()
    renderer = JinjaTemplateRenderer(settings.email_templates_dir)
    sender = ConsoleEmailSender()
    return SendNotification(renderer, sender)
```

### Почему это composition root

Именно здесь абстрактные зависимости use case связываются с конкретными
реализациями:

```text
TemplateRenderer → JinjaTemplateRenderer
EmailSender      → ConsoleEmailSender
```

Application-слой не создаёт adapters самостоятельно.

#### Подробный разбор container

`container.py` — это composition root. Здесь разрешено знать сразу о
нескольких слоях, потому что задача файла — собрать приложение.

Почему use case не создаёт `JinjaTemplateRenderer` сам: тогда application
зависел бы от Jinja и пути к шаблонам. Это сломало бы луковую архитектуру.

Почему здесь `lru_cache`: worker может несколько раз запросить use case, но
создавать новый renderer и sender каждый раз не нужно. Для MVP это простая
форма singleton-а без глобальных переменных.

---

## 22. Этап 18: Celery task как входной adapter

#### Сначала попробуй сам

Представь, что Celery worker получил JSON payload. Что должна сделать task?

```text
1. Принять payload как dict.
2. Провалидировать SendEmailTaskV1.
3. Превратить task contract в SendEmailDTO.
4. Вызвать SendNotification use case.
```

Не рендери письмо прямо в Celery task. Task-функция — это adapter, а не место
для бизнес-сценария.

### Файл `src/interfaces/celery/tasks.py`

```python
from typing import Any

from celery import Celery

from src.application.use_case.send_notification import SendNotification
from src.interfaces.contracts.v1.send_email_task import SEND_EMAIL_TASK_NAME, SendEmailTaskV1
from src.interfaces.mappers import send_email_task_to_dto


def register_tasks(app: Celery, use_case: SendNotification) -> None:
    @app.task(
        name=SEND_EMAIL_TASK_NAME,
        ignore_result=True,
    )
    def send_email_task(payload: dict[str, Any]) -> None:
        task = SendEmailTaskV1.model_validate(payload)
        dto = send_email_task_to_dto(task)
        use_case.execute(dto)
```

### Ответственность этого файла

Celery task:

1. принимает внешний payload;
2. валидирует контракт;
3. преобразует его во внутренний application DTO;
4. вызывает use case.

Она не рендерит шаблон и не отправляет email самостоятельно.

`register_tasks()` получает use case явным аргументом. Поэтому Celery adapter
не импортирует `config/container` и не ищет зависимости самостоятельно.

#### Подробный разбор Celery task adapter-а

Celery вызывает функцию `send_email_task` сам, когда worker получает сообщение
из RabbitMQ. Поэтому эта функция является входной границей процесса worker.

`payload: dict[str, Any]` — это ещё не доверенный объект. Его обязательно нужно
пропустить через:

```python
SendEmailTaskV1.model_validate(payload)
```

После validation mapper превращает внешний контракт в `SendEmailDTO`, и только
после этого вызывается application use case.

Почему `register_tasks(app, use_case)` лучше, чем глобальный use case внутри
модуля: зависимости становятся явными. В тесте можно передать fake use case,
а в production — собранный через container.

### Файл `run/worker.py`

```python
from src.config.container import get_send_notification_use_case
from src.infrastructure.celery.app import celery_app
from src.interfaces.celery.tasks import register_tasks

register_tasks(celery_app, get_send_notification_use_case())

__all__ = ["celery_app"]
```

Как и `run/main.py` в `test_psek`, этот модуль является внешней точкой запуска.
Он не содержит бизнес-логики: получает готовый use case из container,
регистрирует входной adapter и предоставляет собранное приложение процессу
Celery.

#### Подробный разбор entrypoint worker

Команда Celery:

```bash
celery -A run.worker:celery_app worker
```

импортирует модуль `run.worker` и ищет в нём объект `celery_app`. Поэтому в
этом файле мы сначала регистрируем task, а потом экспортируем app.

`run/worker.py` не должен содержать бизнес-правил. Если внутри entrypoint-а
появляется логика «как отправить письмо», значит она попала не в тот слой.

### Первый запуск worker

```bash
uv run celery \
  -A run.worker:celery_app \
  worker \
  --loglevel=INFO \
  --queues=notification.email
```

Оставь worker запущенным в отдельном терминале.

В RabbitMQ Management UI должна появиться очередь:

```text
notification.email
```

### Самостоятельное упражнение

Найди зарегистрированную task в стартовом выводе Celery:

```text
notification.send_email.v1
```

Если её нет, проверь, что `run/worker.py` вызвал `register_tasks()` до запуска
worker.

---

# Часть VI. Kafka consumer

## 23. Этап 19: Kafka handler

### Цель

Отделить разбор конкретного Kafka payload от управления соединением и offsets.

#### Сначала попробуй сам

Напиши черновик handler-а:

```text
KafkaNotificationHandler.handle(raw_value)

Он должен:
- отклонить None;
- распарсить JSON;
- провалидировать EmailRequestedV1;
- преобразовать event в ProcessNotificationDTO;
- вызвать ProcessNotificationEvent.
```

Не добавляй сюда commit offset. Handler отвечает за обработку payload, а
consumer отвечает за управление Kafka offset.

### Файл `src/interfaces/kafka/handler.py`

```python
import json

from src.application.use_case.process_notification_event import ProcessNotificationEvent
from src.interfaces.contracts.v1.email_requested import EmailRequestedV1
from src.interfaces.mappers import email_event_to_dto


class InvalidEventPayloadError(ValueError):
    """Kafka record does not contain a usable notification payload."""


class KafkaNotificationHandler:
    def __init__(self, use_case: ProcessNotificationEvent) -> None:
        self._use_case = use_case

    async def handle(self, raw_value: bytes | None) -> None:
        if raw_value is None:
            raise InvalidEventPayloadError("Kafka event value cannot be null")

        payload = json.loads(raw_value)
        event = EmailRequestedV1.model_validate(payload)
        dto = email_event_to_dto(event)
        await self._use_case.execute(dto)
```

#### Подробный разбор Kafka handler

`raw_value` приходит из Kafka как bytes. Handler не знает про partition,
offset, consumer group и commit. Он отвечает только за смысл payload.

`raw_value is None` — это Kafka tombstone-сообщение. Для compacted topics оно
может быть нормальным явлением, но для нашего notification event value не
должен быть пустым. Поэтому считаем это невалидным payload.

`json.loads(raw_value)` может упасть с `JSONDecodeError`. Это постоянная
ошибка сообщения: повторное чтение того же bytes не сделает JSON валидным.

`EmailRequestedV1.model_validate(payload)` отделяет внешний контракт от
application-слоя. Если payload валиден, mapper создаёт DTO, понятный use case.

Handler не ловит ошибку RabbitMQ. Если `self._use_case.execute(dto)` упадёт
из-за publisher-а, ошибка должна подняться до Kafka consumer-а, чтобы offset не
был подтверждён.

### Какие ошибки считаются постоянными

Следующие ошибки повторное чтение не исправит:

- `UnicodeDecodeError`;
- `json.JSONDecodeError`;
- `pydantic.ValidationError`;
- `UnsupportedChannelError`;
- `InvalidEventPayloadError` для Kafka tombstone с `null` value.

Их отправим в DLQ.

Ошибка публикации в RabbitMQ является временной. Её не нужно ловить как
невалидное событие.

---

## 24. Этап 20: Kafka dead-letter publisher

#### Сначала попробуй сам

Представь, что пришло сообщение `not-json`. Его нельзя обработать, но нельзя и
просто потерять. Что нужно сохранить в DLQ?

```text
- исходный topic;
- partition;
- offset;
- key;
- value;
- тип ошибки;
- текст ошибки;
- время попадания в DLQ.
```

Подумай, почему key/value лучше хранить как base64, а не как обычную строку.

### Файл `src/infrastructure/kafka/dead_letter.py`

```python
import base64
import json
from datetime import UTC, datetime

from aiokafka import AIOKafkaProducer
from aiokafka.structs import ConsumerRecord


class KafkaDeadLetterPublisher:
    def __init__(self, producer: AIOKafkaProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    async def publish(
        self,
        record: ConsumerRecord[bytes, bytes],
        error: Exception,
    ) -> None:
        envelope = {
            "source": {
                "topic": record.topic,
                "partition": record.partition,
                "offset": record.offset,
                "timestamp": record.timestamp,
            },
            "key_base64": self._encode(record.key),
            "value_base64": self._encode(record.value),
            "error": {
                "type": type(error).__name__,
                "message": str(error),
            },
            "failed_at": datetime.now(UTC).isoformat(),
        }

        await self._producer.send_and_wait(
            self._topic,
            json.dumps(envelope).encode("utf-8"),
            key=record.key,
        )

    @staticmethod
    def _encode(value: bytes | None) -> str | None:
        if value is None:
            return None
        return base64.b64encode(value).decode("ascii")
```

#### Подробный разбор DLQ publisher

DLQ publisher — это отдельный adapter для записи плохих сообщений в отдельный
Kafka topic. Он не решает бизнес-задачу отправки email, но помогает системе не
застрять на poison message.

`envelope` — это обёртка вокруг исходного сообщения. Мы сохраняем не только
ошибку, но и координаты исходного record:

```text
topic + partition + offset
```

Эти три значения позволяют позже найти, откуда пришла проблема.

`key_base64` и `value_base64` нужны, потому что невалидное сообщение может быть
любыми bytes. Если попытаться всегда декодировать его как UTF-8, можно потерять
исходные данные или получить новую ошибку уже внутри DLQ-логики.

`send_and_wait` важен: мы хотим дождаться подтверждения записи в DLQ. Только
после этого consumer имеет право подтвердить исходный Kafka offset.

### Почему payload хранится в base64

Невалидное Kafka-сообщение может быть не UTF-8 и вообще не JSON. Base64
позволяет сохранить исходные bytes без потери данных.

### Почему DLQ тоже требует подтверждения

Исходный offset подтверждается только после успешного `send_and_wait()` в DLQ.
Если DLQ недоступен, consumer завершится и позже снова прочитает poison message.

Если запись в DLQ прошла, а commit исходного offset не прошёл, DLQ тоже может
получить дубликат. Это ещё одно проявление at-least-once семантики.

---

## 25. Этап 21: Kafka consumer и ручной commit

#### Сначала попробуй сам

Нарисуй алгоритм обработки одного record:

```text
try:
    handler.handle(record.value)
except permanent_error:
    dead_letter.publish(record, error)

commit offset
```

Теперь подумай, чего здесь нет:

```text
except ConnectionError:
    ...
```

Этой ветки специально нет. Временная ошибка RabbitMQ должна остановить
обработку до commit.

### Файл `src/interfaces/kafka/consumer.py`

```python
import json
import logging
from typing import Protocol

from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from src.application.exceptions import UnsupportedChannelError
from src.interfaces.kafka.handler import InvalidEventPayloadError, KafkaNotificationHandler

logger = logging.getLogger(__name__)

PERMANENT_ERRORS = (
    UnicodeDecodeError,
    json.JSONDecodeError,
    ValidationError,
    UnsupportedChannelError,
    InvalidEventPayloadError,
)


class DeadLetterPublisher(Protocol):
    async def start(self) -> None: ...

    async def stop(self) -> None: ...

    async def publish(
        self,
        record: ConsumerRecord[bytes, bytes],
        error: Exception,
    ) -> None: ...


class NotificationKafkaConsumer:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        handler: KafkaNotificationHandler,
        dead_letter: DeadLetterPublisher,
    ) -> None:
        self._consumer = consumer
        self._handler = handler
        self._dead_letter = dead_letter

    async def run(self) -> None:
        await self._dead_letter.start()
        try:
            await self._consumer.start()
            logger.info("Kafka consumer started")
            try:
                async for record in self._consumer:
                    await self._process_record(record)
            finally:
                await self._consumer.stop()
                logger.info("Kafka consumer stopped")
        finally:
            await self._dead_letter.stop()

    async def _process_record(self, record: ConsumerRecord[bytes, bytes]) -> None:
        try:
            await self._handler.handle(record.value)
        except PERMANENT_ERRORS as error:
            logger.exception(
                "Permanent event error; publishing to DLQ topic=%s partition=%s offset=%s",
                record.topic,
                record.partition,
                record.offset,
            )
            await self._dead_letter.publish(record, error)

        topic_partition = TopicPartition(record.topic, record.partition)
        await self._consumer.commit({topic_partition: record.offset + 1})

        logger.info(
            "Kafka record committed topic=%s partition=%s offset=%s",
            record.topic,
            record.partition,
            record.offset,
        )
```

`NotificationKafkaConsumer` зависит от локального `DeadLetterPublisher`
protocol, а не от конкретного `KafkaDeadLetterPublisher`. Конкретную
реализацию передаёт composition root.

#### Подробный разбор consumer

`enable_auto_commit=False` будет задан при создании `AIOKafkaConsumer` в
container-е. Поэтому этот класс сам решает, когда offset можно подтвердить.

`PERMANENT_ERRORS` — список ошибок, которые повторное чтение не исправит.
Например, если payload не JSON, он не станет JSON через секунду.

Алгоритм `_process_record` специально короткий:

```text
1. Попробовать обработать record.
2. Если ошибка постоянная — записать record в DLQ.
3. После успешной обработки или успешной DLQ-записи сделать commit.
```

Если `handler.handle` упадёт из-за RabbitMQ, ошибка не попадёт в
`PERMANENT_ERRORS`. Значит метод прервётся до строки commit. Это защищает нас
от потери уведомления.

Почему `DeadLetterPublisher` описан как локальный `Protocol`: consumer не
обязан знать, что DLQ тоже Kafka. В тесте или будущем варианте можно передать
другую реализацию с теми же методами.

### Самая важная строка consumer

```python
await self._consumer.commit({topic_partition: record.offset + 1})
```

Kafka хранит offset **следующего** сообщения, которое нужно прочитать. Поэтому
после обработки сообщения с offset `42` сохраняется `43`.

### Почему commit расположен после `except`

Есть два успешных исхода:

1. use case успешно опубликовал задачу в RabbitMQ;
2. постоянная ошибка успешно записана в DLQ.

В обоих случаях исходный Kafka event больше не нужно читать.

Если publisher RabbitMQ выбросит исключение, оно не совпадёт с
`PERMANENT_ERRORS`. Метод завершится до commit. Это именно нужное поведение.

### Почему пока обрабатываем по одному сообщению

Так проще корректно связать обработку и offset. Позже можно использовать
`getmany()` и batch commit, но тогда нужно внимательно подтверждать offset
отдельно для каждой partition и учитывать rebalance.

---

## 26. Этап 22: композиция consumer

#### Сначала попробуй сам

Попробуй собрать цепочку зависимостей без кода:

```text
AIOKafkaConsumer
KafkaNotificationHandler
ProcessNotificationEvent
CeleryTaskPublisher
KafkaDeadLetterPublisher
NotificationKafkaConsumer
```

Затем реши, где должны появиться реальные broker clients. Ответ: в composition
root, а не внутри use case.

### Расширь файл `src/config/container.py`

```python
from functools import lru_cache

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from src.application.use_case.process_notification_event import ProcessNotificationEvent
from src.application.use_case.send_notification import SendNotification
from src.config.settings import get_settings
from src.infrastructure.celery.app import celery_app
from src.infrastructure.celery.publisher import CeleryTaskPublisher
from src.infrastructure.email.console_sender import ConsoleEmailSender
from src.infrastructure.kafka.dead_letter import KafkaDeadLetterPublisher
from src.infrastructure.templates.jinja_renderer import JinjaTemplateRenderer
from src.interfaces.kafka.consumer import NotificationKafkaConsumer
from src.interfaces.kafka.handler import KafkaNotificationHandler


@lru_cache
def get_send_notification_use_case() -> SendNotification:
    settings = get_settings()
    renderer = JinjaTemplateRenderer(settings.email_templates_dir)
    sender = ConsoleEmailSender()
    return SendNotification(renderer, sender)


def build_notification_consumer() -> NotificationKafkaConsumer:
    settings = get_settings()

    task_publisher = CeleryTaskPublisher(
        app=celery_app,
        queue=settings.celery_email_queue,
    )
    use_case = ProcessNotificationEvent(task_publisher)
    handler = KafkaNotificationHandler(use_case)

    consumer = AIOKafkaConsumer(
        settings.kafka_notification_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=settings.kafka_consumer_group,
        enable_auto_commit=False,
        auto_offset_reset=settings.kafka_auto_offset_reset,
    )

    dead_letter_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        enable_idempotence=True,
    )
    dead_letter = KafkaDeadLetterPublisher(
        dead_letter_producer,
        settings.kafka_dead_letter_topic,
    )

    return NotificationKafkaConsumer(
        consumer=consumer,
        handler=handler,
        dead_letter=dead_letter,
    )
```

### Что именно собрано

```text
AIOKafkaConsumer
    ↓
KafkaNotificationHandler
    ↓
ProcessNotificationEvent
    ↓
CeleryTaskPublisher
```

Это composition root процесса consumer.

#### Подробный разбор композиции consumer

`CeleryTaskPublisher` подключается как реализация application port
`TaskPublisher`. После этого `ProcessNotificationEvent` по-прежнему ничего не
знает про Celery.

`KafkaNotificationHandler` получает use case и занимается только payload.
`NotificationKafkaConsumer` получает handler и занимается offset-ами.

`AIOKafkaConsumer(..., enable_auto_commit=False)` — ключевая настройка. Если
оставить auto commit, Kafka может подтвердить сообщение до публикации task в
RabbitMQ.

`AIOKafkaProducer` для DLQ создаётся отдельно от consumer. Это нормальная
практика: consumer читает один topic, producer пишет в другой.

`acks="all"` и `enable_idempotence=True` усиливают надёжность записи в DLQ, но
не отменяют общий принцип at-least-once.

---

## 27. Этап 23: entrypoint consumer

#### Сначала попробуй сам

Entrypoint должен сделать только четыре вещи:

```text
1. Прочитать settings.
2. Настроить logging.
3. Собрать consumer через container.
4. Запустить consumer.run().
```

Если хочется добавить сюда разбор JSON или публикацию task, остановись: это
уже ответственность других слоёв.

### Файл `run/consumer.py`

```python
import asyncio

from src.config.container import build_notification_consumer
from src.config.logging import configure_logging
from src.config.settings import get_settings


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    consumer = build_notification_consumer()
    await consumer.run()


if __name__ == "__main__":
    asyncio.run(main())
```

### Запуск

```bash
uv run python -m run.consumer
```

Пока сообщений нет, consumer просто ожидает их.

Остановить процесс можно через `Ctrl+C`. Блок `finally` закроет Kafka consumer
и producer.

#### Подробный разбор entrypoint consumer

`asyncio.run(main())` создаёт event loop и запускает async consumer. Так как
`aiokafka` работает через asyncio, entrypoint consumer тоже должен быть async.

`configure_logging(settings.log_level)` вызывается здесь, потому что именно
entrypoint решает, как выглядит процесс при запуске.

`build_notification_consumer()` скрывает сборку зависимостей. Entry point не
должен вручную создавать каждый adapter, иначе он быстро превратится в
нечитабельную смесь инфраструктуры.

---

# Часть VII. Тестовый producer и полный запуск

## 28. Этап 24: тестовый Kafka producer

### Цель

Иметь воспроизводимый способ проверить сервис без другого микросервиса.

Это не одноразовый эксперимент, а полезный development-инструмент.

#### Сначала попробуй сам

До полного кода попробуй написать producer, который:

```text
- создаёт AIOKafkaProducer;
- собирает EmailRequestedV1;
- сериализует event в JSON;
- отправляет его в notification.requested.v1;
- использует event_id как key;
- закрывает producer в finally.
```

Смысл скрипта — заменить внешний auth-service на время разработки.

### Файл `scripts/publish_test_event.py`

```python
import asyncio
from datetime import UTC, datetime
from uuid import uuid4

from aiokafka import AIOKafkaProducer

from src.config.settings import get_settings
from src.interfaces.contracts.v1.email_requested import EmailRequestedV1


async def main() -> None:
    settings = get_settings()
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",
        enable_idempotence=True,
    )
    await producer.start()

    try:
        event = EmailRequestedV1(
            event_id=str(uuid4()),
            type="auth.email_confirmation",
            user_id=str(uuid4()),
            email="user@example.com",
            verification_url="https://example.com/verify-email?token=abc123",
            expires_at=datetime(2026, 6, 16, 12, 30, tzinfo=UTC),
            created_at=datetime.now(UTC),
        )

        metadata = await producer.send_and_wait(
            settings.kafka_notification_topic,
            event.model_dump_json().encode("utf-8"),
            key=event.event_id.encode("utf-8"),
        )
        print(
            "Published",
            f"topic={metadata.topic}",
            f"partition={metadata.partition}",
            f"offset={metadata.offset}",
            f"event_id={event.event_id}",
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
```

#### Подробный разбор test producer

Этот скрипт намеренно создаёт `EmailRequestedV1`, а не внутренний DTO. Он
имитирует внешний сервис, который пишет в Kafka именно внешний контракт.

`event.model_dump_json().encode("utf-8")` превращает Pydantic-модель в bytes,
потому что Kafka value передаётся как bytes.

`key=event.event_id.encode("utf-8")` делает key связанным с event. Это помогает
трассировать запись и стабилизирует выбор partition для одинакового key.

`finally: await producer.stop()` нужен даже в учебном скрипте. Сетевые клиенты
нужно закрывать, иначе можно получить зависшие соединения или предупреждения
event loop.

### Почему key равен `event_id`

Kafka выбирает partition на основе key. Все записи с одинаковым key обычно
попадают в одну partition и сохраняют взаимный порядок.

Для некоторых типов событий полезнее использовать `user_id` как key, чтобы
сохранить порядок всех уведомлений одного пользователя. Это контрактное
решение между producer и consumer, которое нужно принять отдельно.

### Почему producer idempotent

Idempotent producer уменьшает вероятность дубликатов, созданных повторными
сетевыми отправками самим Kafka producer. Он не решает дубликаты между Kafka и
RabbitMQ и не заменяет идемпотентность notification-service.

---

## 29. Полный локальный запуск

Открой четыре терминала в корне проекта.

### Терминал 1: инфраструктура

```bash
docker compose up -d
docker compose ps
```

При первом запуске создай topics командами из этапа 14.

### Терминал 2: Celery worker

```bash
uv run celery \
  -A run.worker:celery_app \
  worker \
  --loglevel=INFO \
  --queues=notification.email
```

### Терминал 3: Kafka consumer

```bash
uv run python -m run.consumer
```

### Терминал 4: тестовый producer

```bash
uv run python -m scripts.publish_test_event
```

### Ожидаемый поток

Producer:

```text
Published topic=notification.requested.v1 partition=... offset=... event_id=...
```

Consumer:

```text
Kafka record committed topic=notification.requested.v1 partition=... offset=...
```

Worker:

```text
Email sent to console
recipient=user@example.com
name=None
subject=Подтвердите email
...
```

RabbitMQ UI:

```text
Queue notification.email
Ready: 0
Unacked: 0
```

### Полная проверка кода

```bash
uv run pytest -vv
uv run ruff check .
uv run ruff format --check .
uv run mypy src
```

На первом проходе `mypy --strict` может потребовать уточнить типы сторонних
библиотек. Не отключай проверки глобально. Разбери каждую ошибку и используй
локальные аннотации или точечный `# type: ignore[код]` только с объяснением.

---

# Часть VIII. Обязательные эксперименты со сбоями

## 30. Эксперимент 1: RabbitMQ недоступен

### Действия

1. Оставь Kafka и consumer запущенными.
2. Останови RabbitMQ:

   ```bash
   docker compose stop rabbitmq
   ```

3. Опубликуй событие:

   ```bash
   uv run python -m scripts.publish_test_event
   ```

4. Посмотри ошибку consumer.
5. Запусти RabbitMQ и consumer снова:

   ```bash
   docker compose start rabbitmq
   uv run python -m run.consumer
   ```

### Ожидаемое поведение

Kafka offset не был подтверждён, поэтому после перезапуска событие будет
прочитано снова и задача попадёт в RabbitMQ.

### Что объяснить самому себе

Почему нельзя ловить `ConnectionError`/`OperationalError` и продолжать цикл с
commit?

---

## 31. Эксперимент 2: невалидный Kafka event

Запусти console producer:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic notification.requested.v1
```

Введи:

```text
not-json
```

Затем прочитай DLQ:

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic notification.requested.v1.dlq \
  --from-beginning
```

### Ожидаемое поведение

- consumer не зациклился на poison message;
- DLQ содержит source topic, partition, offset, исходные bytes и ошибку;
- исходный offset подтверждён только после записи в DLQ.

---

## 32. Эксперимент 3: отсутствует значение шаблона

Удали `verification_url` из тестового producer и отправь событие.

### Ожидаемое поведение текущего MVP

- Kafka consumer успешно публикует задачу и подтверждает offset;
- worker получает задачу;
- Jinja `StrictUndefined` вызывает ошибку;
- worker пишет ошибку выполнения в лог;
- по умолчанию Celery подтверждает failed task и автоматически её не повторяет.

### Вывод

Kafka consumer уже не может повторить эту задачу: event был корректно передан
в RabbitMQ. Повторная обработка на этом этапе — ответственность Celery/worker.

Перед production нужно классифицировать ошибки worker:

- постоянная ошибка шаблона — не retry;
- временная ошибка SMTP/API — retry с backoff;
- после исчерпания retries — отдельная dead-letter стратегия и статус `failed`.

---

## 33. Эксперимент 4: повторная доставка

Для демонстрации дубликатов временно поставь breakpoint или добавь исключение
после `publisher.publish()`, но до Kafka commit.

### Ожидаемое поведение

После перезапуска Kafka снова отдаст event, а RabbitMQ получит ещё одну задачу.

### Вывод

Это не баг Kafka. Это следствие отсутствия общей транзакции между Kafka и
RabbitMQ.

---

# Часть IX. Как читать и отлаживать Kafka/Celery

## 34. Kafka: минимальный словарь

### Topic

Логическое имя потока событий:

```text
notification.requested.v1
```

### Partition

Упорядоченная часть topic. Порядок гарантирован только внутри одной partition.

### Offset

Позиция сообщения внутри partition:

```text
partition=1, offset=42
```

Offset уникален только внутри конкретной partition.

### Consumer group

Все consumer с одинаковым `group_id` являются одним логическим подписчиком.
Kafka распределяет partitions между ними.

Если topic имеет три partitions, одновременно полезно работают максимум три
consumer одного group. Четвёртый будет ждать.

### Rebalance

Перераспределение partitions между consumer при запуске, остановке или сбое
одного из них.

При переходе к batch-обработке нужно отдельно изучить
`ConsumerRebalanceListener`.

---

## 35. Celery/RabbitMQ: минимальный словарь

### Broker

RabbitMQ, который хранит задачи до получения worker.

### Task

Зарегистрированная Celery-функция с именем:

```text
notification.send_email.v1
```

### Queue

Очередь RabbitMQ:

```text
notification.email
```

Task name и queue name — разные вещи.

### Worker

Процесс Celery, который читает queue и выполняет task.

### Acknowledgement

Подтверждение RabbitMQ, что задача обработана worker.

Не включай `task_acks_late=True` механически. Late acknowledgement позволяет
вернуть задачу в очередь при падении worker, но также увеличивает вероятность
повторной отправки email. Сначала нужна идемпотентность.

### Retry

Повторный запуск task после ошибки. Retry должен применяться только к временным
ошибкам. Повторять ошибку отсутствующего шаблона бессмысленно.

---

# Часть X. Что улучшить после Console MVP

## 36. Этап 25: идемпотентность и PostgreSQL

Это обязательный этап перед реальным SMTP.

### Почему недостаточно проверять `event_id` в памяти

- сервис может перезапуститься;
- может работать несколько replicas;
- память replicas не синхронизирована.

### Минимальная таблица notifications

```text
notifications
-------------
id                  UUID primary key
event_id            VARCHAR unique not null
task_id             UUID unique not null
notification_type   VARCHAR not null
channel             VARCHAR not null
recipient_email     VARCHAR not null
payload             JSONB not null
status              VARCHAR not null
attempt_count       INTEGER not null default 0
last_error          TEXT null
created_at          TIMESTAMPTZ not null
sent_at             TIMESTAMPTZ null
```

### Важная проблема

Наивный алгоритм:

```text
1. Сохранить event_id в PostgreSQL.
2. Опубликовать task в RabbitMQ.
```

ломается, если процесс упал между шагами: БД считает event обработанным, но
задача не опубликована.

Обратный порядок тоже ломается:

```text
1. Опубликовать task.
2. Сохранить event_id.
```

при падении создаёт дубликат.

### Production-решение: transactional outbox

В одной PostgreSQL-транзакции:

```text
1. Создать notification с уникальным event_id.
2. Создать outbox-запись.
3. Commit.
```

Отдельный outbox publisher:

```text
1. Читает неопубликованные outbox-записи.
2. Публикует Celery task.
3. Отмечает запись опубликованной.
```

Даже outbox publisher может опубликовать task дважды при падении после
публикации, поэтому worker всё равно должен быть идемпотентным.

### Критерии готовности этапа

- `event_id` имеет unique constraint;
- повторный Kafka event не создаёт новую notification;
- worker атомарно захватывает notification для отправки;
- отправленная notification не отправляется повторно;
- статус и ошибка сохраняются;
- тесты моделируют падение между шагами.

---

## 37. Этап 26: SMTP adapter

Добавь новый adapter:

```text
src/infrastructure/email/smtp_sender.py
```

Он реализует существующий `EmailSender`, поэтому `SendNotification` менять не
нужно.

### Что учесть

- TLS и проверка сертификата;
- connect/read timeout;
- authentication;
- корректные `From`, `Reply-To`, MIME HTML/plain;
- классификация SMTP-кодов на временные и постоянные;
- запрет логирования паролей и чувствительного context;
- идемпотентность до включения retries.

### Ошибки

Введи отдельные исключения adapter:

```text
TemporaryEmailDeliveryError
PermanentEmailDeliveryError
```

Celery task повторяет только `TemporaryEmailDeliveryError`.

---

## 38. Этап 27: retry-политика Celery

После появления идемпотентности и классифицированных ошибок:

```python
@celery_app.task(
    name=SEND_EMAIL_TASK_NAME,
    autoretry_for=(TemporaryEmailDeliveryError,),
    retry_backoff=True,
    retry_backoff_max=600,
    retry_jitter=True,
    max_retries=5,
    ignore_result=True,
)
def send_email_task(payload: dict[str, Any]) -> None:
    ...
```

Не используй:

```python
autoretry_for=(Exception,)
```

Такой код бессмысленно повторяет ошибки контрактов, отсутствующие шаблоны и
ошибки программирования.

### Критерии готовности

- временная ошибка вызывает retry;
- постоянная ошибка сразу переводит notification в `failed`;
- retry увеличивает `attempt_count`;
- после исчерпания попыток сохраняется `last_error`;
- одинаковая задача не отправляет письмо дважды.

---

## 39. Этап 28: наблюдаемость

### Структурные логи

Каждый лог обработки должен содержать:

```text
event_id
task_id
notification_type
Kafka topic/partition/offset
Celery task id
notification status
```

Не логируй:

- токены подтверждения;
- reset URL целиком;
- пароли;
- SMTP credentials;
- полный произвольный context.

### Метрики

Полезные метрики:

```text
kafka_consumer_lag
notification_events_processed_total
notification_events_dlq_total
notification_tasks_published_total
notification_send_success_total
notification_send_failure_total
notification_send_duration_seconds
celery_queue_depth
```

### Health checks

Разделяй:

- liveness: процесс жив;
- readiness: процесс может подключиться к обязательным зависимостям.

FastAPI для health endpoints можно добавить позже как отдельный входной
adapter. Он не должен становиться основным способом отправки уведомлений.

---

## 40. Этап 29: contract и integration tests

### Unit tests

Без Docker:

- use cases;
- мапперы;
- правила выбора channel;
- классификация ошибок;
- renderer на временных шаблонах.

### Integration tests

С реальными брокерами:

- event попадает из Kafka в RabbitMQ;
- Kafka offset не подтверждается при недоступном RabbitMQ;
- poison message попадает в DLQ;
- Celery worker получает и валидирует task;
- повторное событие обрабатывается идемпотентно.

### Contract tests

Зафиксируй примеры JSON и проверяй:

- старый корректный event всё ещё валидируется;
- несовместимые изменения требуют нового `v2`;
- producer-сервисы используют тот же schema contract.

В более зрелой системе стоит рассмотреть Schema Registry и Avro/Protobuf/JSON
Schema. Для первого сервиса Pydantic + версионированный topic достаточно.

---

# Часть XI. Контрольные списки

## 41. Checklist Console MVP

- [ ] Все application use cases тестируются без брокеров.
- [ ] Application не импортирует Kafka/Celery/Jinja/Pydantic.
- [ ] Kafka event и Celery task имеют разные контракты.
- [ ] Контракты имеют версию `v1`.
- [ ] Kafka auto commit выключен.
- [ ] Offset подтверждается после публикации task.
- [ ] Подтверждается `record.offset + 1`.
- [ ] Невалидные события отправляются в DLQ.
- [ ] DLQ хранит исходные bytes.
- [ ] Ошибка RabbitMQ не превращается в commit.
- [ ] Celery использует JSON, а не pickle.
- [ ] Worker повторно валидирует task payload.
- [ ] Jinja использует `StrictUndefined`.
- [ ] Шаблоны находятся вне Python-кода.
- [ ] Есть тестовый Kafka producer.
- [ ] Полный поток выводит email в console.
- [ ] `pytest`, `ruff` и `mypy` запускаются через `uv`.

## 42. Checklist перед реальным SMTP

- [ ] Есть PostgreSQL и статусы notification.
- [ ] Есть unique constraint по `event_id`.
- [ ] Реализован transactional outbox или эквивалентная стратегия.
- [ ] Worker идемпотентен.
- [ ] Временные и постоянные SMTP-ошибки разделены.
- [ ] Retry применяется только к временным ошибкам.
- [ ] Есть лимит retries и статус `failed`.
- [ ] Секреты не находятся в Git.
- [ ] Чувствительный context не попадает в логи.
- [ ] Настроены метрики и alerting.
- [ ] Есть integration tests со сбоями.
- [ ] Определены retention и процесс разбора DLQ.

## 43. Вопросы для самопроверки

Ты готов двигаться дальше, если можешь своими словами ответить:

1. Почему Kafka consumer не отправляет email напрямую?
2. Почему event и task имеют разные модели?
3. Почему offset подтверждается после RabbitMQ publish?
4. Почему commit сохраняет `offset + 1`?
5. Почему всё равно возможны дубликаты?
6. Почему одинаковый Celery task ID не является дедупликацией?
7. Какие ошибки нужно отправлять в Kafka DLQ?
8. Какие ошибки должны остановить consumer без commit?
9. Почему worker повторно валидирует payload?
10. Почему `StrictUndefined` полезнее пустых значений?
11. Где связываются порты и adapters?
12. Почему application-слой не читает `.env`?
13. Почему нельзя включать retry для любого `Exception`?
14. Почему реальный SMTP нельзя безопасно подключать до идемпотентности?
15. Как transactional outbox уменьшает окно потери задач?

---

# Часть XII. Полезные команды

## 44. Проект

```bash
uv sync
uv run pytest -vv
uv run ruff check .
uv run ruff format .
uv run mypy src
```

## 45. Docker Compose

```bash
docker compose up -d
docker compose ps
docker compose logs -f kafka
docker compose logs -f rabbitmq
docker compose stop
docker compose down
```

Не используй `docker compose down -v`, если не хочешь удалить локальные данные
Kafka и RabbitMQ.

## 46. Kafka topics

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list
```

```bash
docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic notification.requested.v1
```

## 47. Запуск процессов

```bash
uv run celery \
  -A run.worker:celery_app \
  worker \
  --loglevel=INFO \
  --queues=notification.email
```

```bash
uv run python -m run.consumer
```

```bash
uv run python -m scripts.publish_test_event
```

---

# Часть XIII. Официальные источники

Документация, с которой стоит сверяться во время реализации:

- Apache Kafka Quickstart:
  <https://kafka.apache.org/quickstart/>
- aiokafka consumer и manual commit:
  <https://aiokafka.readthedocs.io/en/stable/consumer.html>
- aiokafka producer:
  <https://aiokafka.readthedocs.io/en/stable/producer.html>
- Celery с RabbitMQ:
  <https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/rabbitmq.html>
- Celery Calling Tasks:
  <https://docs.celeryq.dev/en/stable/userguide/calling.html>
- Celery Tasks и retries:
  <https://docs.celeryq.dev/en/stable/userguide/tasks.html>
- Pydantic models:
  <https://docs.pydantic.dev/latest/concepts/models/>
- Jinja API:
  <https://jinja.palletsprojects.com/en/stable/api/>
- uv projects:
  <https://docs.astral.sh/uv/guides/projects/>

---

## 48. Итоговый маршрут

Проходи этапы в этом порядке:

```text
1. Domain models
2. Application DTO and application ports
3. ProcessNotificationEvent + unit tests
4. SendNotification + unit tests
5. Versioned external contracts
6. Mappers
7. Jinja renderer + templates
8. Console sender
9. Settings
10. Kafka and RabbitMQ in Compose
11. Celery application and publisher
12. Celery worker task
13. Kafka handler
14. Kafka DLQ
15. Kafka consumer with manual commit
16. Test producer
17. End-to-end launch
18. Failure experiments
19. PostgreSQL and idempotency
20. SMTP and classified retries
21. Observability and production deployment
```

Не оценивай прогресс только по количеству написанных файлов. Главный результат
каждого этапа — способность объяснить, почему компонент существует, где
проходит его граница и что произойдёт при сбое.
