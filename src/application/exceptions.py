class ApplicationError(Exception):
    """Base class for expected application errors."""


class UnsupportedChannelError(ApplicationError):
    def __init__(self, channel: str) -> None:
        super().__init__(f"Unsupported notification channel: {channel}")
        self.channel = channel
