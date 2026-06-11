from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class EmailRecipient:
    email: str
    name: str|None = None

@dataclass(frozen=True, slots=True)
class RenderedEmail:
    subject: str
    html_body: str
    text_body: str