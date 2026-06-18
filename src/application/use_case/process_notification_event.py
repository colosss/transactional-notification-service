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
