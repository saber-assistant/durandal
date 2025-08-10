class IntentLayer:
    """
    Base class for all intent layers.
    """
    def __init__(self, **kwargs):
        self.other_kwargs = kwargs
    
    async def on_startup(self):
        pass

    async def check_condition(self, previous_segments: list[str], segment: str, is_partial: bool = False) -> bool:
        """
        Check if the layer should be applied to the given text segment.
        Can be overridden by subclasses. Default implementation always returns True for non-blank segments.
        """
        return len(segment.strip()) > 0


    async def classify(self, previous_segments: list[str], segment: str, is_partial: bool = False) -> dict:
        """
        Classify the given text and return the result(s).
        Should be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement this method.")


    async def on_complete(self, result: dict):
        """
        Hook called after classification attempt (regardless of success or failure), before on_success or on_failure.
        Can be overridden by subclasses.
        """
        pass
    
    async def on_success(self, result: dict):
        """
        Hook called when classification is successful.
        Can be overridden by subclasses.
        """
        pass

    async def on_failure(self, result: dict, reason: str):
        """
        Hook called when classification fails.
        Can be overridden by subclasses.
        """
        pass
