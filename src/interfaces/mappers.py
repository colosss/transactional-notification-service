from src.interfaces.contracts.v1.email_requested import EmailRequestedv1
from src.interfaces.contracts.v1.send_email_task import SendEmailTaskV1
from src.application.dto.notification import (
    ProcessNotificationDTO,
    EmailRecipientDTO,
    SendEmailDTO
)

def email_event_to_dto(event: EmailRequestedv1)->ProcessNotificationDTO:
    return ProcessNotificationDTO(
        event_id=event.event_id,
        notification_type=event.type,
        channel="email",
        user_id=event.user_id,
        recipient=EmailRecipientDTO(
            email=str(event.email),
            name=None,
        ),
        context={
            "verification_url": str(event.verification_url),
            "expires_at": event.expires_at.isoformat(),

        },
        created_at=event.created_at,
    )
    

def send_email_task_to_dto(task: SendEmailTaskV1)->SendEmailDTO:
    return SendEmailDTO(
        task_id=task.task_id,
        event_id=task.event_id,
        notification_type=task.type,
        recipient=EmailRecipientDTO(
            email=str(task.recipient.email),
            name=task.recipient.name,
        ),
        context=dict(task.context),
        created_at=task.created_at,
    )