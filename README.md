# Transactional Notification Service

`Transactional Notification Service` — это переиспользуемый микросервис для обработки и отправки транзакционных уведомлений из разных проектов.

Сервис получает события уведомлений из Kafka, преобразует их во внутреннюю задачу отправки, помещает задачу в RabbitMQ, после чего отдельные worker-процессы постепенно обрабатывают очередь и отправляют сообщения пользователям.

Основная цель проекта — создать универсальный notification-сервис, который можно подключать к разным микросервисным проектам без переписывания логики отправки уведомлений.

## Назначение сервиса

Сервис предназначен для отправки системных и транзакционных сообщений:

* подтверждение регистрации;
* подтверждение email;
* восстановление пароля;
* уведомление о смене пароля;
* код авторизации;
* уведомление о входе с нового устройства;
* уведомление о подозрительной активности;
* уведомления о статусах бронирований, заявок или заказов;
* системные уведомления пользователям или администраторам.

На первом этапе сервис ориентирован на email-уведомления. В дальнейшем архитектура может быть расширена для поддержки других каналов: Telegram, SMS, push-уведомлений или внутренних уведомлений в приложении.

## Общая идея работы

Сервис работает в два этапа.

Первый этап — получение события:

```text
Other Microservice
        ↓
Kafka
        ↓
Notification Consumer
        ↓
RabbitMQ
```

Второй этап — отправка сообщения:

```text
RabbitMQ
        ↓
Notification Worker
        ↓
Template Renderer
        ↓
Email Provider
        ↓
User Email
```

Kafka используется как входная шина событий между микросервисами.

RabbitMQ используется как внутренняя очередь задач для постепенной и контролируемой отправки сообщений.

## Почему используются Kafka и RabbitMQ

В проекте Kafka и RabbitMQ выполняют разные задачи.

Kafka отвечает за получение событий от других микросервисов. Например, auth-service может отправить событие о том, что пользователь зарегистрировался и ему нужно отправить письмо подтверждения.

RabbitMQ отвечает за очередь задач отправки. После получения события notification-service не отправляет письмо сразу, а создаёт задачу отправки и помещает её в очередь. Worker-процессы постепенно забирают задачи из RabbitMQ и отправляют сообщения.

Такой подход позволяет:

* не блокировать обработку Kafka-сообщений долгой отправкой email;
* контролировать скорость отправки сообщений;
* обрабатывать временные ошибки почтового провайдера;
* добавлять retry-механику;
* запускать несколько worker-процессов;
* в будущем сохранять статусы доставки в БД.

## Основные компоненты сервиса

### Kafka Consumer

Получает события из Kafka.

Задачи Kafka Consumer:

* подключиться к Kafka;
* подписаться на нужный topic;
* получить событие;
* распарсить JSON-сообщение;
* проверить обязательные поля;
* передать событие в application-слой.

Consumer не должен содержать бизнес-логику отправки сообщений. Его задача — только получить событие и передать его дальше.

### Event Processor

Обрабатывает входящее событие уведомления.

Задачи Event Processor:

* принять нормализованное событие;
* проверить тип уведомления;
* преобразовать событие в задачу отправки;
* передать задачу в RabbitMQ.

На этом этапе письмо ещё не отправляется.

### RabbitMQ Publisher

Публикует задачу отправки в RabbitMQ.

Задачи RabbitMQ Publisher:

* принять внутреннюю задачу отправки;
* сериализовать её в JSON;
* отправить задачу в нужную очередь RabbitMQ.

### Notification Worker

Получает задачи из RabbitMQ и выполняет отправку.

Задачи worker-а:

* получить задачу из RabbitMQ;
* определить шаблон письма по `template_code`;
* подставить данные из `context`;
* собрать subject, HTML и plain-text версию письма;
* отправить email через выбранный provider;
* обработать ошибку при неудачной отправке.

### Template Renderer

Отвечает за работу с шаблонами сообщений.

Задачи Template Renderer:

* найти нужный шаблон по `template_code`;
* загрузить HTML-шаблон;
* загрузить subject-шаблон;
* подставить данные из `context`;
* вернуть готовое письмо для отправки.

HTML-шаблоны не генерируются внутри сервиса. Они подключаются извне через Docker volume. Это позволяет использовать один и тот же notification-service в разных проектах с разными шаблонами писем.

Пример подключения шаблонов:

```text
main-project/
  email_templates/
    auth/
      email_confirmation/
        subject.txt
        body.html
        body.txt

      password_reset/
        subject.txt
        body.html
        body.txt

  services/
    notification-service/
```

В Docker Compose шаблоны могут быть подключены так:

```yaml
services:
  notification-service:
    volumes:
      - ./email_templates:/app/email_templates:ro
```

### Email Provider

Отвечает за непосредственную отправку письма.

В первой версии можно реализовать:

* `ConsoleEmailProvider` — вывод письма в консоль для разработки;
* `SMTPEmailProvider` — реальная отправка через SMTP.

В будущем можно добавить:

* SendGrid;
* Mailgun;
* Amazon SES;
* Telegram;
* SMS provider;
* push provider.

### Repository

Repository нужен для хранения истории уведомлений и статусов доставки.

На первом учебном этапе БД можно не подключать. Сначала нужно добиться рабочего потока:

```text
Kafka → Consumer → RabbitMQ → Worker → Console output
```

После этого можно добавить PostgreSQL и хранить:

* входящие события;
* созданные notification-записи;
* статус доставки;
* ошибки отправки;
* количество попыток;
* дату создания;
* дату успешной отправки.

## Входящие данные из Kafka

Notification-service принимает события из Kafka в JSON-формате.

Базовое событие:

```json
{
  "event_id": "01JZ3V9ZK7M8WQZ4JX9E2F6A1B",
  "type": "auth.email_confirmation",
  "channel": "email",
  "user_id": "8b5d9d9e-0a4e-4f5e-b91e-77b4c8c2f111",
  "recipient": {
    "email": "user@example.com",
    "name": "Ivan"
  },
  "context": {
    "app_name": "Example App",
    "verification_url": "https://example.com/verify-email?token=abc123"
  },
  "created_at": "2026-06-10T12:00:00Z"
}
```

## Описание полей Kafka-события

### `event_id`

Уникальный идентификатор события.

Нужен для защиты от повторной обработки одного и того же события. Kafka может доставить сообщение повторно, поэтому сервис должен уметь понимать, обрабатывал он это событие раньше или нет.

Пример:

```json
"event_id": "01JZ3V9ZK7M8WQZ4JX9E2F6A1B"
```

### `type`

Тип уведомления. По этому полю сервис понимает, какой шаблон использовать.

Примеры:

```text
auth.email_confirmation
auth.password_reset
auth.password_changed
auth.login_code
auth.new_device_login
booking.created
booking.cancelled
booking.reminder
system.delivery_failed
```

### `channel`

Канал отправки уведомления.

В первой версии основной канал:

```text
email
```

В будущем возможны:

```text
telegram
sms
push
in_app
```

### `user_id`

Идентификатор пользователя, которому отправляется уведомление.

Поле может быть полезно для логирования, истории доставки и связи notification-записи с пользователем.

### `recipient`

Получатель уведомления.

Для email-канала используется:

```json
{
  "email": "user@example.com",
  "name": "Ivan"
}
```

### `context`

Данные, которые будут подставлены в шаблон письма.

Пример:

```json
{
  "app_name": "Example App",
  "verification_url": "https://example.com/verify-email?token=abc123"
}
```

Сервис не должен сам создавать токены подтверждения, ссылки восстановления пароля или проверять бизнес-логику auth-service. Эти данные должны быть подготовлены внешним сервисом и переданы в `context`.

### `created_at`

Дата и время создания события внешним сервисом.

Пример:

```json
"created_at": "2026-06-10T12:00:00Z"
```

## Примеры Kafka-событий

### Подтверждение email

```json
{
  "event_id": "01JZ3V9ZK7M8WQZ4JX9E2F6A1B",
  "type": "auth.email_confirmation",
  "channel": "email",
  "user_id": "8b5d9d9e-0a4e-4f5e-b91e-77b4c8c2f111",
  "recipient": {
    "email": "user@example.com",
    "name": "Ivan"
  },
  "context": {
    "app_name": "Example App",
    "verification_url": "https://example.com/verify-email?token=abc123"
  },
  "created_at": "2026-06-10T12:00:00Z"
}
```

### Восстановление пароля

```json
{
  "event_id": "01JZ3W0B9C6EH3T5E6X8P91F2C",
  "type": "auth.password_reset",
  "channel": "email",
  "user_id": "8b5d9d9e-0a4e-4f5e-b91e-77b4c8c2f111",
  "recipient": {
    "email": "user@example.com",
    "name": "Ivan"
  },
  "context": {
    "app_name": "Example App",
    "reset_url": "https://example.com/reset-password?token=abc123",
    "expires_in_minutes": 30
  },
  "created_at": "2026-06-10T12:05:00Z"
}
```

### Уведомление о смене пароля

```json
{
  "event_id": "01JZ3W3D1FQ7S4W9N5E2H7B8K9",
  "type": "auth.password_changed",
  "channel": "email",
  "user_id": "8b5d9d9e-0a4e-4f5e-b91e-77b4c8c2f111",
  "recipient": {
    "email": "user@example.com",
    "name": "Ivan"
  },
  "context": {
    "app_name": "Example App",
    "changed_at": "2026-06-10T12:10:00Z",
    "support_email": "support@example.com"
  },
  "created_at": "2026-06-10T12:10:00Z"
}
```

## Задача, которая кладётся в RabbitMQ

После получения события из Kafka сервис создаёт внутреннюю задачу отправки.

Пример задачи:

```json
{
  "task_id": "01JZ3WA9VW8P1WDGTXK2ZN6Y44",
  "event_id": "01JZ3V9ZK7M8WQZ4JX9E2F6A1B",
  "type": "auth.email_confirmation",
  "channel": "email",
  "recipient": {
    "email": "user@example.com",
    "name": "Ivan"
  },
  "context": {
    "app_name": "Example App",
    "verification_url": "https://example.com/verify-email?token=abc123"
  },
  "created_at": "2026-06-10T12:00:01Z"
}
```

На первом этапе можно передавать в RabbitMQ все данные для отправки.

В более продвинутой версии можно сохранять notification в PostgreSQL, а в RabbitMQ отправлять только `notification_id`.

Пример:

```json
{
  "notification_id": "33a97e4e-1ad3-4a62-9b9f-3c9f83c2cb62"
}
```

## Планируемые статусы уведомлений

После подключения БД сервис может хранить статусы доставки:

```text
pending   — уведомление создано, но ещё не поставлено в очередь
queued    — задача отправки помещена в RabbitMQ
sending   — worker начал отправку
sent      — сообщение успешно отправлено
retrying  — отправка временно не удалась, будет повтор
failed    — сообщение не удалось отправить после всех попыток
```

## Планируемая структура проекта

```text
src/
  contracts/
    events.py
    tasks.py

  domain/
    notification.py

  application/
    use_cases/
      process_notification_event.py
      send_notification.py
    ports/
      task_publisher.py
      template_renderer.py
      email_provider.py
      notification_repository.py

  infrastructure/
    kafka/
      consumer.py
      handlers.py

    rabbitmq/
      publisher.py
      worker.py

    templates/
      jinja_renderer.py

    email/
      console_provider.py
      smtp_provider.py

    db/
      models.py
      repositories.py
      session.py

  config.py
```

## Основные сценарии

### 1. Обработка события из Kafka

Сценарий:

```text
ProcessNotificationEvent
```

Вход:

```text
NotificationRequestedEvent
```

Действия:

```text
1. Получить событие из Kafka.
2. Проверить обязательные поля.
3. Проверить тип уведомления.
4. Преобразовать событие во внутреннюю задачу отправки.
5. Отправить задачу в RabbitMQ.
```

На первом этапе этот сценарий не работает с БД.

В будущей версии он также сможет:

```text
1. Проверять дубликаты по event_id.
2. Сохранять notification-запись в PostgreSQL.
3. Менять статус на queued.
```

### 2. Отправка уведомления worker-ом

Сценарий:

```text
SendNotification
```

Вход:

```text
SendNotificationTask
```

Действия:

```text
1. Получить задачу из RabbitMQ.
2. Определить шаблон по type/template_code.
3. Загрузить subject.txt.
4. Загрузить body.html.
5. Подставить context в шаблон.
6. Отправить email через email provider.
7. Обработать результат отправки.
```

На первом этапе вместо реальной отправки можно использовать `ConsoleEmailProvider`, который выводит письмо в консоль.

## MVP

Первая версия проекта должна реализовать минимальный поток:

```text
Kafka → Notification Consumer → RabbitMQ → Notification Worker → Console output
```

В MVP нужно сделать:

* docker-compose с Kafka и RabbitMQ;
* Kafka topic для входящих notification-событий;
* Python consumer, который читает сообщения из Kafka;
* DTO для входящего события;
* преобразование Kafka event в RabbitMQ task;
* publisher, который отправляет задачу в RabbitMQ;
* worker, который читает задачу из RabbitMQ;
* console email provider, который выводит сообщение в консоль;
* простую структуру шаблонов;
* базовый рендеринг subject/html через Jinja2.

В MVP не обязательно делать:

* PostgreSQL;
* Alembic;
* полноценный repository;
* SMTP;
* FastAPI;
* retry-механику;
* админку;
* хранение истории отправки.

## Следующие этапы после MVP

### Этап 1. Kafka → RabbitMQ → Console

Цель — понять поток событий.

```text
Kafka event
    ↓
Consumer
    ↓
RabbitMQ task
    ↓
Worker
    ↓
Console output
```

### Этап 2. Шаблоны

Добавить внешние HTML-шаблоны:

```text
email_templates/
  auth/
    email_confirmation/
      subject.txt
      body.html
      body.txt
```

Worker должен уметь находить шаблон по `type`.

### Этап 3. SMTP

Добавить реальную отправку email через SMTP.

### Этап 4. PostgreSQL

Добавить хранение истории уведомлений:

* event_id;
* recipient_email;
* type/template_code;
* channel;
* status;
* error_text;
* retry_count;
* created_at;
* sent_at.

### Этап 5. Retry и обработка ошибок

Добавить:

* retry при временной ошибке отправки;
* dead-letter очередь;
* ограничение количества попыток;
* сохранение ошибок.

### Этап 6. FastAPI

Опционально добавить API для разработки и диагностики:

* `GET /health`;
* `GET /notifications/{id}`;
* `POST /templates/preview`;
* `POST /notifications/test`.

FastAPI не является основным способом отправки уведомлений. Основной вход в сервис — Kafka.

## Главные правила архитектуры

1. Kafka consumer не должен отправлять email напрямую.
2. Kafka consumer только получает событие и передаёт его дальше.
3. RabbitMQ worker отвечает за фактическую отправку сообщений.
4. HTML-шаблоны не генерируются внутри сервиса, а подключаются извне.
5. Auth-service или другой внешний сервис сам создаёт токены и ссылки.
6. Notification-service не должен знать бизнес-логику авторизации, бронирований или заказов.
7. Notification-service отвечает только за доставку уведомлений.
8. Repository появляется только тогда, когда появляется необходимость хранить статусы и историю.
9. На первом этапе можно обойтись без БД.
10. Основная цель первой версии — рабочий поток Kafka → RabbitMQ → Worker.

## Пример полного потока

```text
1. User registers in Auth Service.
2. Auth Service creates verification token.
3. Auth Service creates verification URL.
4. Auth Service publishes event to Kafka.
5. Notification Service consumes event from Kafka.
6. Notification Service validates event.
7. Notification Service creates RabbitMQ task.
8. Notification Worker consumes task from RabbitMQ.
9. Worker loads email template.
10. Worker renders subject and HTML.
11. Worker sends email.
12. Later: Worker saves delivery status in PostgreSQL.
```

## Текущий фокус разработки

Сейчас основная задача — не начинать с БД и repository, а сначала реализовать поток сообщений:

```text
Kafka → RabbitMQ → Worker
```

После того как этот поток заработает, можно добавлять:

```text
templates → SMTP → PostgreSQL → retries → FastAPI
```

Такой порядок позволит сначала разобраться с Kafka и RabbitMQ, а не смешивать изучение брокеров сообщений с SQLAlchemy, Alembic и проектированием БД.
