import json
try:
    import redis.asyncio as redis
except ImportError:
    redis = None
from utils.logger import logger

class RedisStateManager:
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.client = None

    async def connect(self):
        if redis is None:
            logger.error("Redis module not found. Please install it with 'pip install redis hiredis'")
            return

        try:
            # IMPORTANT: decode_responses=False to allow binary Protobuf data
            self.client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                password=self.password,
                decode_responses=False
            )
            await self.client.ping()
            logger.info(f"Connected to Redis at {self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise

    async def set_value(self, key, value):
        if not self.client: return
        await self.client.set(key, value)

    async def get_value(self, key):
        if not self.client: return None
        val = await self.client.get(key)
        return val.decode('utf-8') if val else None

    async def hset(self, name, key=None, value=None, mapping=None):
        if not self.client: return
        if mapping:
            await self.client.hset(name, mapping=mapping)
        else:
            await self.client.hset(name, key, value)

    async def hget(self, name, key):
        if not self.client: return None
        val = await self.client.hget(name, key)
        return val.decode('utf-8') if val else None

    async def hgetall(self, name):
        if not self.client: return {}
        data = await self.client.hgetall(name)
        # Decode keys and values
        return {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}

    async def publish(self, channel, message):
        if not self.client: return
        await self.client.publish(channel, message)

    async def close(self):
        if self.client:
            await self.client.aclose()
