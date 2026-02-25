import asyncio
import os
import sys

# Add the project root to sys.path to allow imports from hub and utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import logger, configure_logger
from utils.config_manager import ConfigManager
from utils.api_client_manager import ApiClientManager
from utils.websocket_manager import WebSocketManager
from hub.state_redis import RedisStateManager
import redis.asyncio as redis

class PriceFeedServer:
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.api_client_manager = ApiClientManager(config_manager)
        self.redis_manager = None
        self.ws_manager = None
        self.is_running = False

    async def initialize(self):
        await self.api_client_manager.async_init()

        # Redis Setup
        self.redis_manager = RedisStateManager(
            host=self.config_manager.get('redis', 'host', fallback='localhost'),
            port=self.config_manager.get_int('redis', 'port', fallback=6379),
            db=self.config_manager.get_int('redis', 'db', fallback=0)
        )
        await self.redis_manager.connect()

        # WebSocket Setup
        self.ws_manager = WebSocketManager(self.api_client_manager)
        self.ws_manager.register_message_handler(self.handle_market_tick)

    async def handle_market_tick(self, feed_response):
        """
        Relays the market tick to Redis Pub/Sub.
        We serialize the entire FeedResponse protobuf message to bytes.
        """
        try:
            # Serializing protobuf to bytes for efficient transport
            data_bytes = feed_response.SerializeToString()

            # Publish to a global market data channel
            # In a more granular setup, we could publish to per-instrument channels
            await self.redis_manager.client.publish("market:ticks", data_bytes)

            # Also store the latest tick in a hash for quick polling if needed
            # (Optional: might be redundant if Pub/Sub is used)
            # await self.redis_manager.client.hset("market:latest_ticks", ...)

        except Exception as e:
            logger.error(f"Error relaying tick to Redis: {e}")

    async def run(self):
        self.is_running = True
        logger.info("Starting Price Feed Server...")

        # In a real scenario, we would need to know which instruments to subscribe to.
        # For this server, we might poll Redis for "desired_subscriptions"
        # or just subscribe to everything the bot(s) need.

        # For now, let's implement a listener for subscription requests from the bots
        asyncio.create_task(self.listen_for_subscription_requests())

        await self.ws_manager.connect_and_listen()

    async def listen_for_subscription_requests(self):
        """Listens for subscription requests from bots and updates the WS subscriptions."""
        pubsub = self.redis_manager.client.pubsub()
        await pubsub.subscribe("market:sub_requests")
        logger.info("Listening for subscription requests on 'market:sub_requests'...")

        async for message in pubsub.listen():
            if message['type'] == 'message':
                try:
                    raw_data = message['data']
                    if isinstance(raw_data, bytes):
                        raw_data = raw_data.decode('utf-8')
                    data = json.loads(raw_data)
                    action = data.get('action') # 'sub' or 'unsub'
                    keys = data.get('keys', [])

                    if action == 'sub':
                        logger.info(f"Received subscription request for {len(keys)} instruments.")
                        self.ws_manager.subscribe(keys)
                    elif action == 'unsub':
                        logger.info(f"Received unsubscription request for {len(keys)} instruments.")
                        self.ws_manager.unsubscribe(keys)
                except Exception as e:
                    logger.error(f"Error processing subscription request: {e}")

    async def stop(self):
        self.is_running = False
        if self.ws_manager:
            await self.ws_manager.close()
        if self.redis_manager:
            await self.redis_manager.close()
        logger.info("Price Feed Server stopped.")

async def main():
    config_path = 'config/config_trader.ini'
    config_manager = ConfigManager(config_file=config_path)
    configure_logger(config_manager)

    server = PriceFeedServer(config_manager)
    try:
        await server.initialize()
        await server.run()
    except Exception as e:
        logger.critical(f"Server crash: {e}", exc_info=True)
    finally:
        await server.stop()

if __name__ == "__main__":
    import json # Import inside for script usage
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
