import asyncio
import datetime
import json
import sys
import os
import time
from hub.provider_factory import ProviderFactory
from hub.broker_manager import BrokerManager
from utils.logger import logger, configure_logger
from utils.config_manager import ConfigManager
from utils.config_validator import ConfigValidator
from utils.api_client_manager import ApiClientManager
from hub.data_manager import DataManager
from hub.display_manager import DisplayManager
from hub.lifecycle_manager import LifecycleManager
from hub.engine_manager import EngineManager
from hub.portfolio_manager import PortfolioManager
from hub.backtest_orchestrator import BacktestOrchestrator
from hub.strike_manager import StrikeManager
from utils.trade_logger import TradeLogger
from hub.live_orchestrator import LiveOrchestrator
from hub.event_bus import event_bus

import argparse

async def main():
    """
    Main entry point for the multi-broker, multi-instrument trading application.
    Initializes and orchestrates all the major components.
    """
    # --- 1. Configuration and Logging ---
    parser = argparse.ArgumentParser(description="AlgoSoft Trading Bot")
    parser.add_argument('--config', type=str, default='config/config_trader.ini', help='Path to the configuration file.')
    parser.add_argument('--entry_tf', type=int, help='Override entry timeframe (minutes)')
    parser.add_argument('--exit_tf', type=int, help='Override exit timeframe (minutes)')
    parser.add_argument('--provider', type=str, help='Specific data provider credential section to use')
    parser.add_argument('--client_mode', action='store_true', help='Run in multi-tenant client mode (reads config from env vars)')
    args = parser.parse_args()

    if args.client_mode:
        from hub.client_config import load_client_config
        client_cfg = load_client_config()
        logger.info(f"[CLIENT MODE] Starting bot for {client_cfg}")
        _status_path = os.path.join("config", f"bot_status_client_{client_cfg.client_id}.json")
        os.makedirs("config", exist_ok=True)
        with open(_status_path, "w") as _sf:
            json.dump({"bot_running": True, "mode": client_cfg.trading_mode, "instrument": client_cfg.instrument, "broker": client_cfg.broker, "heartbeat": time.time()}, _sf)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, args.config)
    config_manager = ConfigManager(config_file=config_path)

    # Apply runtime overrides for parallel variant testing
    if args.entry_tf:
        config_manager.set_override('settings', 'entry_timeframe_minutes', args.entry_tf)
        config_manager.set_override('settings', 'entry_pattern_tf', args.entry_tf)
    if args.exit_tf:
        config_manager.set_override('settings', 'exit_timeframe_minutes', args.exit_tf)
        config_manager.set_override('settings', 'exit_pattern_tf', args.exit_tf)
    if args.provider: config_manager.set_override('data_providers', 'provider_list', args.provider)

    # Automatically generate a unique log file if overrides are used to prevent file locking issues
    if args.entry_tf or args.exit_tf:
        etf = config_manager.get('settings', 'entry_timeframe_minutes')
        xtf = config_manager.get('settings', 'exit_timeframe_minutes')
        config_manager.set_override('app', 'log_file', f"app_{etf}m_{xtf}m.log")

    configure_logger(config_manager)

    # Log the active strategy parameters for visual confirmation
    entry_p_tf = config_manager.get('settings', 'entry_pattern_tf', fallback=config_manager.get('settings', 'entry_timeframe_minutes', fallback='1'))
    exit_p_tf = config_manager.get('settings', 'exit_pattern_tf', fallback=config_manager.get('settings', 'exit_timeframe_minutes', fallback='1'))
    logger.info(f"ACTIVE STRATEGY: Entry TF: {entry_p_tf}m | Exit TF: {exit_p_tf}m")

    # Initialize TradeLogger with config for dynamic filename generation
    TradeLogger().setup(config_manager)
    logger.info("Starting the multi-broker trading application...")
    logger.info(f"Using configuration file: {args.config}")

    validator = ConfigValidator(config_manager)
    if not validator.validate(context='trader'):
        logger.critical("Invalid configuration. Exiting.")
        sys.exit(1)

    # --- 2. API and Core Component Setup ---
    if args.client_mode:
        upstox_token = os.environ.get('UPSTOX_ACCESS_TOKEN', '')
        upstox_key = os.environ.get('UPSTOX_API_KEY', '')
        if upstox_token:
            for p in config_manager.get_data_providers():
                section = p['name']
                config_manager.set_override(section, 'access_token', upstox_token)
                if upstox_key:
                    config_manager.set_override(section, 'api_key', upstox_key)
                logger.info(f"[CLIENT MODE] Injected Upstox credentials from DB into [{section}]")

    is_backtest = config_manager.get_boolean('settings', 'backtest_enabled', fallback=False)
    api_client_manager = ApiClientManager(config_manager)
    await api_client_manager.async_init()

    # Redis initialization
    redis_manager = None
    if config_manager.get_boolean('redis', 'enabled', fallback=False):
        from hub.state_redis import RedisStateManager
        redis_manager = RedisStateManager(
            host=config_manager.get('redis', 'host', fallback='localhost'),
            port=config_manager.get_int('redis', 'port', fallback=6379),
            db=config_manager.get_int('redis', 'db', fallback=0)
        )
        await redis_manager.connect()

    from utils.database_manager import DatabaseManager
    db_manager = DatabaseManager()

    db_connected = False
    try:
        await db_manager.connect(config_manager=config_manager)
        db_connected = True
    except Exception as e:
        db_required = config_manager.get_boolean('database', 'required', fallback=True)
        if db_required:
            logger.critical(f"DATABASE ERROR: Could not connect to PostgreSQL. {e}")
            logger.info("Setup Help: Ensure PostgreSQL is running on the host/port specified in config_trader.ini.")
            logger.info("If you are running in Cloud9, you may need to start the service: 'sudo service postgresql start'")
            sys.exit(1)
        else:
            logger.warning(f"DATABASE WARNING: Connection failed ({e}). Proceeding in legacy mode (INI only).")

    broker_manager = BrokerManager(config_manager, db_manager=db_manager if db_connected else None)

    # --- 3. Engine Management ---
    engine_manager = EngineManager(config_manager, api_client_manager, broker_manager,
                                   db_manager if db_connected else None, redis_manager,
                                   DisplayManager({}, config_manager))

    try:
        if is_backtest:
            await broker_manager.load_brokers()
            instruments = [i.strip() for i in config_manager.get('settings', 'instrument_to_trade', fallback='NIFTY').upper().split(',')]

            for inst in instruments:
                rest, ws = await ProviderFactory.create_data_provider(api_client_manager, config_manager, True, redis_manager)
                orch = await engine_manager.create_orchestrator(inst, rest, ws, True)
                await orch.add_user_session("backtest_user", "backtest_user@local")

                lm = LifecycleManager(orch, ws, broker_manager, DisplayManager(orch, config_manager), True)

                # --- Multi-day Backtest Date Parsing ---
                backtest_date_raw = config_manager.get('settings', 'backtest_date', fallback='')
                backtest_dates = []

                if " to " in backtest_date_raw:
                    try:
                        start_str, end_str = backtest_date_raw.split(" to ")
                        start_dt = datetime.datetime.strptime(start_str.strip(), '%Y-%m-%d').date()
                        end_dt = datetime.datetime.strptime(end_str.strip(), '%Y-%m-%d').date()
                        current = start_dt
                        while current <= end_dt:
                            backtest_dates.append(current)
                            current += datetime.timedelta(days=1)
                    except Exception as e:
                        logger.error(f"Failed to parse backtest date range: {e}")
                        backtest_dates = [backtest_date_raw]
                elif "," in backtest_date_raw:
                    backtest_dates = [d.strip() for d in backtest_date_raw.split(',')]
                else:
                    backtest_dates = [backtest_date_raw]

                lm.backtest_dates = backtest_dates
                await lm.start()
        else:
            event_bus.subscribe('EXECUTE_TRADE_REQUEST', broker_manager.handle_execute_trade_request)
            event_bus.subscribe('EXIT_TRADE_REQUEST', broker_manager.handle_exit_request)
            await engine_manager.run_engines()

    except asyncio.CancelledError:
        logger.info("Main task cancelled, shutting down.")
    except Exception as e:
        logger.critical(f"An unhandled error occurred in main: {e}", exc_info=True)
    finally:
        logger.info("Shutting down all application components...")
        for instrument, manager in engine_manager.lifecycle_managers.items():
            logger.info(f"Stopping lifecycle manager for {instrument}...")
            await manager.stop()

        if api_client_manager:
            await api_client_manager.close()

        if redis_manager:
            await redis_manager.close()

        # Ensure all trade logs are closed gracefully
        TradeLogger().shutdown()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application interrupted by user. Shutting down.")
