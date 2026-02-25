import asyncio
import json
from utils.logger import logger
from utils import MarketDataFeedV3_pb2 as pb
from hub.data_feed_base import DataFeed

class RedisDataFeed(DataFeed):
    def __init__(self, redis_manager):
        self.redis_manager = redis_manager
        self.message_handlers = []
        self._running = False
        self._listener_task = None

    def register_message_handler(self, handler):
        if handler not in self.message_handlers:
            self.message_handlers.append(handler)
            logger.info(f"Redis Message handler {getattr(handler, '__name__', 'unnamed')} registered.")

    def subscribe(self, symbols, mode='full'):
        """Sends a subscription request to the Price Feed Server via Redis."""
        logger.info(f"Requesting subscription for {len(symbols)} symbols via Redis.")
        asyncio.create_task(
            self.redis_manager.client.publish("market:sub_requests", json.dumps({
                'action': 'sub',
                'keys': symbols,
                'mode': mode
            }))
        )

    def unsubscribe(self, symbols):
        """Sends an unsubscription request to the Price Feed Server via Redis."""
        logger.info(f"Requesting unsubscription for {len(symbols)} symbols via Redis.")
        asyncio.create_task(
            self.redis_manager.client.publish("market:sub_requests", json.dumps({
                'action': 'unsub',
                'keys': symbols
            }))
        )

    async def connect_and_listen(self):
        self._running = True
        pubsub = self.redis_manager.client.pubsub()
        await pubsub.subscribe("market:ticks")
        logger.info("Listening for ticks from Redis channel 'market:ticks'...")

        while self._running:
            try:
                # Use a timeout to allow checking self._running
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message['type'] == 'message':
                    data_bytes = message['data']

                    # Parse protobuf
                    feed_response = pb.FeedResponse()
                    feed_response.ParseFromString(data_bytes)

                    # Call handlers
                    for handler in self.message_handlers:
                        try:
                            # We assuming handlers are async or fast
                            if asyncio.iscoroutinefunction(handler):
                                asyncio.create_task(handler(feed_response))
                            elif hasattr(handler, 'handle_message') and asyncio.iscoroutinefunction(handler.handle_message):
                                asyncio.create_task(handler.handle_message(feed_response))
                            else:
                                # Fallback to sync call if needed, but discouraged
                                if hasattr(handler, 'handle_message'):
                                    handler.handle_message(feed_response)
                                else:
                                    handler(feed_response)
                        except Exception as e:
                            logger.error(f"Error in Redis tick handler: {e}")

            except Exception as e:
                logger.error(f"Error in Redis data feed listener: {e}")
                await asyncio.sleep(1)

    def start(self):
        if not self._listener_task:
            self._listener_task = asyncio.create_task(self.connect_and_listen())
        return self._listener_task

    async def close(self):
        self._running = False
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
        logger.info("Redis data feed closed.")
