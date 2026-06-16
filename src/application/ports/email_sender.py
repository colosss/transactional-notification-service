from typing import Protocol

from src.core.domain.models import EmailRecipient, RenderedEmail

class EmailSender(Protocol):
    def send(self, recipient: EmailRecipient, emal: RenderedEmail)->None: ...
    