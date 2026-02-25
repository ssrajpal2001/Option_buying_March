from abc import ABC, abstractmethod

class DataFeed(ABC):
    """
    Abstract base class for data feeds.
    Defines the interface for both live and historical data sources.
    """
    @abstractmethod
    def register_message_handler(self, handler):
        """Registers a handler function for incoming market data."""
        pass

    @abstractmethod
    async def close(self):
        """Closes the connection to the data source."""
        pass

    @abstractmethod
    def subscribe(self, symbols, mode='full'):
        """Subscribes to a list of instrument symbols."""
        pass

    @abstractmethod
    def unsubscribe(self, symbols):
        """Unsubscribes from a list of instrument symbols."""
        pass
