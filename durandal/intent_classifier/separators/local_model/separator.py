from ..base import IntentSeparator

class LocalModelIntentSeparator(IntentSeparator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def check_condition(self, content: str) -> bool:
        """
        Return if the content contains more than 7 words.
        """
        return len(content.strip().split(" ")) > 7
    
    async def create_segments(self, content: str) -> list[str]:
        return [content]
