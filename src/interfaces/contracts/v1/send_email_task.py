from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

SEND_EMAIL_TASK_NAME="notification.send_email.v1"

class EmailTaskRecipientV1(BaseModel):
    model_config=ConfigDict(extra="forbid")

    email: EmailStr
    name: str|None = Field(default=None, max_length=200)

class SendEmailTaskV1(BaseModel):
    model_config=ConfigDict(extra="forbid")

    task_id: UUID
    event_id: str=Field(min_length=1, max_length=128)
    type: str=Field(
        min_length=2,
        max_length=200,
        pattern=r"^[a-z0-9_]+(?:\.[a-z0-9_]+)+$",
    )
    channel:Literal["email"]="email"
    recipient: EmailTaskRecipientV1
    context: dict[str, Any]
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def created_at_must_have_timezone(cls, value: datetime)->datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("created_at must include timezone")
        return value
    