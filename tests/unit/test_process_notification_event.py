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
