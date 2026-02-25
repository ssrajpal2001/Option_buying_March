import asyncio
from utils.database_manager import DatabaseManager
from utils.encryption_manager import EncryptionManager
import getpass

async def seed():
    # Use ConfigManager to get DB settings
    from utils.config_manager import ConfigManager
    import os
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config_trader.ini')
    config_manager = ConfigManager(config_file=config_path)

    db = DatabaseManager()
    try:
        await db.connect(config_manager=config_manager)
    except Exception as e:
        print(f"CRITICAL: Could not connect to database. {e}")
        print("Please check your config/config_trader.ini [database] settings.")
        return

    enc = EncryptionManager()

    print("--- Commercial Phase 1: DB Seeding ---")
    email = input("User Email: ")
    broker = input("Broker Name (ZERODHA/ANGELONE/UPSTOX): ").upper()
    instance = input("Account Label (e.g. My Zerodha): ")
    api_key = input("API Key: ")
    api_secret = getpass.getpass("API Secret: ")

    secret_enc = enc.encrypt(api_secret)

    async with db.pool.acquire() as conn:
        # Create user
        user_id = await conn.fetchval(
            "INSERT INTO users (email) VALUES ($1) ON CONFLICT (email) DO UPDATE SET email=EXCLUDED.email RETURNING id",
            email
        )

        # Create broker
        await conn.execute(
            """INSERT INTO user_brokers (user_id, broker_name, instance_name, api_key, api_secret_encrypted, broker_settings)
               VALUES ($1, $2, $3, $4, $5, $6)
               ON CONFLICT (user_id, instance_name) DO UPDATE
               SET api_key=EXCLUDED.api_key, api_secret_encrypted=EXCLUDED.api_secret_encrypted""",
            user_id, broker, instance, api_key, secret_enc, '{"instruments_to_trade": "NIFTY"}'
        )

        # Create strategy
        await conn.execute(
            """INSERT INTO user_strategies (user_id, instrument_name, entry_tf_minutes, exit_tf_minutes, exit_logic_type)
               VALUES ($1, $2, 1, 5, 'S1_LOW')
               ON CONFLICT (user_id, instrument_name) DO NOTHING""",
            user_id, "NIFTY"
        )

    print(f"\nSUCCESS: User {email} and broker {instance} seeded into database.")
    await db.close()

if __name__ == "__main__":
    asyncio.run(seed())
