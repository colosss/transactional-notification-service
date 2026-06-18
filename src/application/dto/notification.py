from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class EmailRecipientDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    email: str
    name: str | None = None


class ProcessNotificationDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    event_id: str
    notification_type: str
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
