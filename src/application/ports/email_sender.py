from typing import Protocol

from src.core.domain.models import EmailRecipient, RenderedEmail

class EmailSender(Protocol):
    def send(self, recipient: EmailRecipient, email: RenderedEmail)->None: ...
    