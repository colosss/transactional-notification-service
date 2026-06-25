from datetime import datetime
from pydantic import BaseModel, ConfigDict, EmailStr, Field, HttpUrl, field_validator

class EmailRequestedv1(BaseModel):
    model_config=ConfigDict(extra="forbid")

    event_id: str=Field(min_length=1, max_length=128)
    type: str=Field(
        min_length=3,
        max_length=200,
        pattern=r"^[a-z0-9_]+(?:\.[a-z0-9_]+)+$",
    )
    user_id: str=Field(min_length=1, max_length=128)
    email: EmailStr
    verification_url: HttpUrl
    expires_at: datetime
    created_at: datetime

    @field_validator("expires_at", "created_at")
    @classmethod
    def datetime_must_have_timezone(cls, value: datetime)->datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("datetime value must include timezone")
        return value