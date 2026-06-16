from typing import Protocol
from src.application.dto.notification import SendEmailDTO

class TaskPublisher(Protocol):
    async def publish(self, task: SendEmailDTO)->None: ...