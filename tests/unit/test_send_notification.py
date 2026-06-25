from src.application.use_case.send_notification import SendNotification
from collections.abc import Mapping
from datetime import datetime, UTC
from typing import Any
from uuid import UUID
from src.core.domain.models import RenderedEmail, EmailRecipient
from src.application.dto.notification import EmailRecipientDTO, SendEmailDTO

class FakeTemplateRenderer:
    def __init__(self)->None:
        self.calls: list[tuple[str, Mapping[str, Any]]]=[]
        
    def render(
        self,
        template_code: str,
        context: Mapping[str, Any]
    )->RenderedEmail:
        self.calls.append((template_code, context))
        return RenderedEmail(
            subject="Confirm email",
            html_body="<p>Confirm</p>",
            text_body="Confirm",
        )
    
class FakeEmailSender:
    def __init__(self)->None:
        self.sent:list[tuple[EmailRecipient, RenderedEmail]]=[]

    def send(self, recipient: EmailRecipient, email: RenderedEmail)->None:
        self.sent.append((recipient, email))

def test_renders_and_sends_email()->None:
    renderer=FakeTemplateRenderer()
    sender=FakeEmailSender()
    use_case=SendNotification(renderer, sender)
    dto=SendEmailDTO(
        task_id=UUID("11111111-1111-1111-1111-111111111111"),
        event_id="event-1",
        notification_type="auth.email_confirmation",
        recipient=EmailRecipientDTO(email="user@example.com", name="user"),
        context={
            "verification_url": "https://example.com/verify",
            "expires_at": "2026-06-16T12:30:00+00:00",
        },
        created_at=datetime(2026, 6, 11, 12, 0, tzinfo=UTC)
    )
    use_case.execute(dto)

    assert renderer.calls==[
        (
            "auth.email_confirmation",
            {
                "verification_url": "https://example.com/verify",
                "expires_at": "2026-06-16T12:30:00+00:00",
                "recipient": {
                    "email": "user@example.com",
                    "name": "user",
                },
            },
        )
    ]
    
    assert sender.sent==[
        (
            EmailRecipient(email="user@example.com", name="user"),
            RenderedEmail(
                subject="Confirm email",
                html_body="<p>Confirm</p>",
                text_body="Confirm",
            ),
        )
    ]