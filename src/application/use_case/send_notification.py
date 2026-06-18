from src.application.dto.notification import SendEmailDTO
from src.application.ports.email_sender import EmailSender
from src.application.ports.template_renderer import TemplateRenderer
from src.core.domain.models import EmailRecipient

class SendNotification:
    def __init__(
        self,
        renderer: TemplateRenderer,
        sender: EmailSender,
    )->None:
        self._renderer=renderer
        self._sender=sender

    def execute(self, dto: SendEmailDTO)->None:
        recipient=EmailRecipient(
            email=dto.recipient.email,
            name=dto.recipient.name,
        )
        context=dict(dto.context)
        context["recipient"]={
            "email": recipient.email,
            "name": recipient.name,
        }

        email=self._renderer.render(
            dto.notification_type,
            context,
        )
        self._sender.send(recipient, email)