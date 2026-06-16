class ApplicationError(Exception): 
    pass

class UnsuportedChannelError(ApplicationError):
    def __init__(self, channel: str)->None:
        super().__init__(f"Unsupported notification channel: {channel}")
        self.channel=channel

        