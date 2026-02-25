import asyncpg
import asyncio
from utils.logger import logger
import json

class DatabaseManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, db_config=None):
        if self._initialized:
            return
        self.config = db_config or {}
        self.pool = None
        self._initialized = True

    async def connect(self, config_manager=None):
        """Initializes the PostgreSQL connection pool using values from ConfigManager."""
        try:
            if self.pool:
                return

            # Use ConfigManager if provided, otherwise fallback to dict or env
            if config_manager:
                user = config_manager.get('database', 'user', fallback='postgres')
                password = config_manager.get('database', 'password', fallback='postgres')
                host = config_manager.get('database', 'host', fallback='127.0.0.1')
                port = config_manager.get('database', 'port', fallback='5432')
                database = config_manager.get('database', 'database', fallback='trading_bot')
            else:
                user = self.config.get('user', 'postgres')
                password = self.config.get('password', 'postgres')
                host = self.config.get('host', 'localhost')
                port = self.config.get('port', '5432')
                database = self.config.get('database', 'trading_bot')

            self.pool = await asyncpg.create_pool(
                user=user,
                password=password,
                host=host,
                port=port,
                database=database,
                min_size=5,
                max_size=20
            )
            logger.info(f"Database connection pool established at {host}:{port}/{database}")

            # Run schema initialization
            await self._init_schema()

        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    async def _init_schema(self):
        """Executes the schema.sql script to ensure tables exist."""
        import os
        schema_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'schema.sql')
        if os.path.exists(schema_path):
            with open(schema_path, 'r') as f:
                schema_sql = f.read()

            async with self.pool.acquire() as conn:
                await conn.execute(schema_sql)
                logger.info("Database schema validated/initialized.")

    async def get_active_users_and_brokers(self):
        """Fetches all active users and their active broker configurations."""
        query = """
            SELECT
                u.id as user_id,
                u.email,
                b.id as broker_id,
                b.broker_name,
                b.instance_name,
                b.api_key,
                b.api_secret_encrypted,
                b.broker_settings
            FROM users u
            JOIN user_brokers b ON u.id = b.user_id
            WHERE u.is_active = TRUE AND b.is_active = TRUE
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [dict(r) for r in rows]

    async def get_user_strategy(self, user_id, instrument_name):
        """Fetches strategy settings for a specific user and instrument."""
        query = """
            SELECT * FROM user_strategies
            WHERE user_id = $1 AND instrument_name = $2 AND is_active = TRUE
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(query, user_id, instrument_name)
            return dict(row) if row else None

    async def log_trade_entry(self, user_id, broker_id, strategy_id, symbol, side, price, timestamp, strategy_log=""):
        """Records a new trade entry in the ledger."""
        query = """
            INSERT INTO trades (user_id, broker_id, strategy_id, instrument_symbol, side, entry_price, entry_timestamp, status, strategy_log)
            VALUES ($1, $2, $3, $4, $5, $6, $7, 'OPEN', $8)
            RETURNING id
        """
        async with self.pool.acquire() as conn:
            trade_id = await conn.fetchval(query, user_id, broker_id, strategy_id, symbol, side, price, timestamp, strategy_log)
            return trade_id

    async def log_trade_exit(self, trade_id, exit_price, exit_timestamp, pnl, reason, strategy_log=""):
        """Updates an existing trade with exit details."""
        query = """
            UPDATE trades
            SET exit_price = $2, exit_timestamp = $3, pnl = $4, exit_reason = $5, status = 'CLOSED',
                strategy_log = strategy_log || '\n' || $6
            WHERE id = $1
        """
        async with self.pool.acquire() as conn:
            await conn.execute(query, trade_id, exit_price, exit_timestamp, pnl, reason, strategy_log)

    async def close(self):
        """Gracefully shuts down the pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed.")
