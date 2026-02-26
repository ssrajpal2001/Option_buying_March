from utils.logger import logger
from abc import ABC, abstractmethod
import asyncio

from .state_manager import StateManager
from .atm_manager import AtmManager
from .data_manager import DataManager
from .price_feed_handler import PriceFeedHandler
from .trade_execution_manager import TradeExecutionManager
from .event_bus import event_bus
from .strike_manager import StrikeManager
from .backtest_pnl_tracker import BacktestPnLTracker
from .orchestrator_state import OrchestratorState
from .tick_processor import TickProcessor
from .signal_monitor import SignalMonitor
from .trade_executor import TradeExecutor
from .position_manager import PositionManager
from .user_session import UserSession
from .data_recorder import DataRecorder
from .indicator_manager import IndicatorManager
from utils.ohlc_aggregator import OHLCAggregator
from utils.common_models import AppConfig, MarketData
from utils.json_config_manager import JsonConfigManager

class BaseOrchestrator(ABC):
    def __init__(self, instrument_name, rest_client, websocket_manager, broker_manager, config_manager, data_manager=None, contract_map=None, redis_manager=None, is_backtest=None):
        self.instrument_name = instrument_name
        self.rest_client = rest_client
        self.websocket = websocket_manager
        self.config_manager = config_manager
        self.json_config = JsonConfigManager()
        self.contract_map = contract_map or {}
        self.redis_manager = redis_manager

        # Priority: Explicit argument > Config setting
        if is_backtest is not None:
            self.is_backtest = is_backtest
        else:
            self.is_backtest = self.config_manager.get_boolean('settings', 'backtest_enabled', fallback=False)

        # 1. SHARED STATE (One per instrument)
        # StateManager must be created first as AtmManager depends on it.
        self.state_manager = StateManager(self.config_manager, instrument_name=self.instrument_name, redis_manager=self.redis_manager, is_backtest=self.is_backtest)
        self.atm_manager = AtmManager(self.config_manager, websocket_manager, self.state_manager, rest_client, instrument_name=self.instrument_name, orchestrator=self)

        self.entry_timeframe = self.config_manager.get_int('settings', 'entry_timeframe_minutes', 1)
        self.exit_timeframe = self.config_manager.get_int('settings', 'exit_timeframe_minutes', 5)

        # Configurable S1LOW Timeframes
        # Priority: JSON config (indicators/s1_confirm) > INI instrument section > INI settings > Defaults
        json_fast = self.json_config.get_value(f"{self.instrument_name}.buy.exit_indicators.s1_confirm.fast_tf")
        json_slow = self.json_config.get_value(f"{self.instrument_name}.buy.exit_indicators.s1_confirm.slow_tf")

        self.s1_low_fast_tf = int(json_fast) if json_fast is not None else \
                             self.config_manager.get_int(self.instrument_name, 's1_low_fast_tf',
                                                         self.config_manager.get_int('settings', 's1_low_fast_tf', 1))
        self.s1_low_slow_tf = int(json_slow) if json_slow is not None else \
                             self.config_manager.get_int(self.instrument_name, 's1_low_slow_tf',
                                                         self.config_manager.get_int('settings', 's1_low_slow_tf', 5))

        # Increased history limit to 2000 to support multi-day S&R/Pattern analysis (approx 5 days of 1-min data)
        self.entry_aggregator = OHLCAggregator(interval_minutes=1, history_limit=2000, name="Pattern_Entry") # Fixed 1-min for Pattern Recognition
        self.exit_aggregator = OHLCAggregator(interval_minutes=1, history_limit=2000, name="Pattern_Exit")  # Fixed 1-min for Pattern Recognition
        self.one_min_aggregator = OHLCAggregator(interval_minutes=self.s1_low_fast_tf, history_limit=2000, name="S1LOW_Fast") # S1LOW Fast TF
        self.five_min_aggregator = OHLCAggregator(interval_minutes=self.s1_low_slow_tf, history_limit=2000, name="S1LOW_Slow") # S1LOW Slow TF

        # 2. MULTI-TENANT USER SESSIONS
        self.user_sessions = {} # user_id -> UserSession

        self.broker_manager = broker_manager
        if self.broker_manager:
            self.broker_manager.set_state_manager(self.state_manager)

        # --- Instrument Configuration ---
        # The primary instrument is now passed in and set directly.
        self.primary_instrument = self.instrument_name

        # Read the futures_instrument_key from the specific instrument's section.
        self.futures_instrument_key = self.config_manager.get(self.primary_instrument, 'futures_instrument_key')
        if not self.futures_instrument_key:
            logger.warning(f"futures_instrument_key not defined in config for {self.primary_instrument}. Relying on auto-discovery.")

        self.index_instrument_key = self.config_manager.get(self.primary_instrument, 'instrument_symbol')

        # If a DataManager is not passed in, it's created using the correct instrument symbol.
        self.data_manager = data_manager or DataManager(self.rest_client, self.index_instrument_key, self.config_manager)

        self.atm_manager.spot_instrument_key = self.futures_instrument_key

        self.strike_manager = StrikeManager(self.state_manager, self.atm_manager, self.config_manager, instrument_name=self.instrument_name)
        self.price_feed_handler = PriceFeedHandler(self.state_manager, self.atm_manager, self, self.entry_aggregator, self.exit_aggregator, self.one_min_aggregator, self.five_min_aggregator)
        self.trade_execution_manager = TradeExecutionManager(self.broker_manager, self.state_manager, self.atm_manager, self.config_manager)

        self.pnl_tracker = BacktestPnLTracker(self.instrument_name, self.config_manager) if self.is_backtest else None

        # Initialize DataRecorder for live/paper V2 recording (Default: Disabled in Bot)
        # Recording is now handled by a standalone script scripts/run_recorder.py
        should_record = self.config_manager.get_boolean('settings', 'record_data', fallback=False)
        self.data_recorder = DataRecorder(self.instrument_name) if (not self.is_backtest and should_record) else None

        self.orchestrator_state = OrchestratorState()
        self.is_active = False

        self.websocket.register_message_handler(self.price_feed_handler.handle_message)

        # --- V2 Logic Component Initializations ---
        logger.debug("Initializing V2 Logic Components...")
        self.indicator_manager = IndicatorManager(self)
        logger.info(f"[{self.instrument_name}] IndicatorManager initialized on orchestrator.")
        self.signal_monitor = SignalMonitor(self)
        self.trade_executor = TradeExecutor(self)
        self.position_manager = PositionManager(self)

        # The TickProcessor is initialized separately after all other components are set up.
        self.tick_processor = None

    def finalize_initialization(self):
        """
        Finalizes the initialization by creating components that depend on
        all other modules being in place.
        """
        self.tick_processor = TickProcessor(self)
        self.atm_manager.set_ready()
        logger.debug("Orchestrator initialization finalized.")

    @abstractmethod
    def _get_timestamp(self):
        """Returns the current timestamp (live or backtest)."""
        pass

    @abstractmethod
    def _is_trade_active(self):
        """Checks if a trade is currently active."""
        raise NotImplementedError

    @abstractmethod
    async def _check_backtest_exit_conditions(self, timestamp):
        """Checks for exit conditions during a backtest."""
        pass

    async def add_user_session(self, user_id, email, strategy_config=None):
        """Adds a new isolated user session to this orchestrator."""
        if user_id not in self.user_sessions:
            session = UserSession(user_id, email, self.instrument_name, self, strategy_config)
            await session.state_manager.load_state()
            self.user_sessions[user_id] = session
            logger.debug(f"[{self.instrument_name}] User {email} added to monitoring.")

    async def broadcast_signal(self, direction, instrument_key, signal_ltp, strike_price, timestamp, strategy_log, entry_type='BUY'):
        """Broadcasts a validated market signal to all isolated user sessions."""
        quantity_multiplier = 1
        signal_strike = strike_price

        if hasattr(self, 'sell_manager') and self.sell_manager.strangle_placed:
            hedge_strike, hedge_key = self.sell_manager.get_buy_strike(direction)
            if hedge_strike and hedge_key:
                logger.info(f"[{self.instrument_name}] Hedge override: {strike_price} -> {hedge_strike} (sell_hedge_{direction}) | qty x1")
                strike_price = hedge_strike
                instrument_key = hedge_key
                quantity_multiplier = 1

        logger.debug(f"[{self.instrument_name}] Broadcasting {direction} signal ({entry_type}) to {len(self.user_sessions)} users.")
        tasks = []
        for session in self.user_sessions.values():
            tasks.append(session.evaluate_signal(
                direction=direction,
                instrument_key=instrument_key,
                signal_ltp=signal_ltp,
                strike_price=strike_price,
                timestamp=timestamp,
                signal_strike=signal_strike,
                strategy_log=strategy_log,
                entry_type=entry_type,
                quantity_multiplier=quantity_multiplier
            ))

        if tasks:
            await asyncio.gather(*tasks)

    def get_current_tick_data(self, ce_strike, pe_strike, is_backtest, backtest_current_tick=None):
        """
        Delegates the fetching of the current tick data to the TickProcessor,
        which is now the central authority for this logic.
        """
        return self.tick_processor.get_tick_data(ce_strike, pe_strike, is_backtest, backtest_current_tick)

    def get_initial_subscriptions(self):
        """
        Returns a list of instrument keys that this orchestrator needs for its initial data feed.
        """
        if self.is_backtest:
            return []

        instrument_symbol = self.config_manager.get(self.instrument_name, 'instrument_symbol')
        instruments = [self.futures_instrument_key, instrument_symbol]
        logger.debug(f"[{self.instrument_name}] Requesting initial subscriptions for: {instruments}")
        return instruments

    async def process_tick(self, backtest_previous_tick=None, backtest_current_tick=None):
        if self.orchestrator_state.done_for_day:
            return
        await self.tick_processor.process_tick(backtest_previous_tick, backtest_current_tick)
        await self.check_overall_exit()

    def stop(self):
        logger.info("Stopping Orchestrator's components...")
        # No action needed here anymore as the scheduler is removed.
        pass

    async def check_overall_exit(self):
        """Checks if the overall point target for the day has been reached."""
        config_path = f"{self.instrument_name}.overall_exit"
        enabled = self.json_config.get_value(f"{config_path}.enabled")
        if not enabled:
            return

        target_points = self.json_config.get_value(f"{config_path}.point_target")
        if target_points is None:
            return

        total_points = await self.calculate_overall_points()
        if total_points >= float(target_points):
            logger.info(f"[{self.instrument_name}] OVERALL TARGET REACHED: {total_points:.2f} points >= {target_points}. Done for the day.")
            self.orchestrator_state.done_for_day = True
            await self.exit_all_trades(reason=f"Overall Target Reached ({total_points:.2f} pts)")

    async def calculate_overall_points(self):
        """Calculates total points PnL from all sources (User sessions and SellManager)."""
        total_pnl_rs = 0.0

        if self.is_backtest:
            # Realized + Unrealized from pnl_tracker (Hedges and closed Strangle legs)
            total_pnl_rs = self.pnl_tracker.get_total_pnl()
            # Add Unrealized from active Strangle legs
            if hasattr(self, 'sell_manager'):
                total_pnl_rs += self.sell_manager.get_total_pnl()
        else:
            # 1. Sum from User Sessions (BUY trades)
            for session in self.user_sessions.values():
                # Realized
                total_pnl_rs += session.state_manager.total_pnl
                # Unrealized
                for pos in [session.state_manager.call_position, session.state_manager.put_position]:
                    if pos:
                        total_pnl_rs += pos.get('pnl', 0)

            # 2. Sum from SellManager (SELL legs)
            if hasattr(self, 'sell_manager'):
                total_pnl_rs += self.sell_manager.get_total_pnl()

        # Convert to points relative to standard unit (Base Qty * Lot Size)
        base_qty = self.state_manager.quantity
        lot_size = self.state_manager.lot_size

        if base_qty > 0 and lot_size > 0:
            return total_pnl_rs / (base_qty * lot_size)
        return 0.0

    async def exit_all_trades(self, reason):
        """Exits all active trades in all sessions and SellManager."""
        timestamp = self._get_timestamp()

        # 1. Exit session trades
        for session in self.user_sessions.values():
            for side in ['CALL', 'PUT']:
                if session.is_in_trade(side):
                    pos = session.state_manager.get_position(side)
                    ltp = session.state_manager.get_ltp(pos.get('instrument_key')) or pos.get('ltp', 0)
                    await session.position_manager._exit_trade(side, ltp, timestamp, reason)

        # 2. Exit SellManager legs
        if hasattr(self, 'sell_manager') and self.sell_manager.strangle_placed and not self.sell_manager.strangle_closed:
            await self.sell_manager.close_all(timestamp)

    def clear_for_new_run(self):
        """Clears all state and aggregators for a fresh run (e.g. multi-day backtest)."""
        # Ensure fresh strategy logic from JSON
        self.json_config.load()
        logger.debug(f"[{self.instrument_name}] Clearing orchestrator state for a fresh run.")
        self.state_manager.clear_all_state()
        for session in self.user_sessions.values():
            session.state_manager.clear_all_state()

        self.entry_aggregator.clear()
        self.exit_aggregator.clear()
        self.one_min_aggregator.clear()
        self.five_min_aggregator.clear()

        if self.pnl_tracker:
            self.pnl_tracker.trade_history = []
            self.pnl_tracker.active_call_trade = None
            self.pnl_tracker.active_put_trade = None

        if hasattr(self.data_manager, 'clear_caches'):
            self.data_manager.clear_caches()

        if hasattr(self, 'sell_manager'):
            from hub.sell_manager import SellManager
            self.sell_manager = SellManager(self)
        if hasattr(self, '_backtest_strangle_triggered'):
            self._backtest_strangle_triggered = False
        self.orchestrator_state.done_for_day = False
