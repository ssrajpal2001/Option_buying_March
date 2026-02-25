import asyncio
from collections import defaultdict
from utils.logger import logger

class EventBus:
    def __init__(self):
        self._listeners = defaultdict(list)
        self._events = {}

    async def wait_for(self, event_type):
        """Wait for a specific event to be published."""
        if event_type not in self._events:
            self._events[event_type] = asyncio.Event()
        await self._events[event_type].wait()

    def subscribe(self, event_type, listener):
        """Subscribe to an event."""
        self._listeners[event_type].append(listener)
        logger.debug(f"Listener {getattr(listener, '__name__', 'Unknown')} subscribed to {event_type}")

    def unsubscribe(self, event_type, listener):
        """Unsubscribe from an event."""
        if event_type in self._listeners:
            try:
                self._listeners[event_type].remove(listener)
                logger.info(f"Listener {getattr(listener, '__name__', 'Unknown')} unsubscribed from {event_type}")
            except ValueError:
                logger.warning(f"Attempted to unsubscribe a non-existent listener from {event_type}")

    async def publish(self, event_type, *args, **kwargs):
        """Publish an event to all subscribers."""
        if event_type in self._events:
            self._events[event_type].set()
        if event_type in self._listeners:
            # Create a copy of the list to prevent issues if a listener unsubscribes itself
            listeners = self._listeners[event_type][:]
            tasks = [asyncio.create_task(listener(*args, **kwargs)) for listener in listeners]
            if tasks:
                await asyncio.gather(*tasks)

event_bus = EventBus()
