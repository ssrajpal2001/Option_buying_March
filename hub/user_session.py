from .state_manager import StateManager
from .position_manager import PositionManager
from .portfolio_manager import PortfolioManager
from .signal_monitor import SignalMonitor
from utils.logger import logger

class UserSession:
    """
    Encapsulates all user-specific state and logic for a specific instrument.
    This ensures that one user's trades and risk settings do not bleed into another's.
    """
    def __init__(self, user_id, email, instrument_name, orchestrator, strategy_config=None):
        self.user_id = user_id
        self.email = email
        self.instrument_name = instrument_name
        self.orchestrator = orchestrator
        self.strategy_config = strategy_config or {}

        # 1. Isolated StateManager
        self.state_manager = StateManager(
            orchestrator.config_manager,
            instrument_name=instrument_name,
            user_id=user_id,
            redis_manager=orchestrator.redis_manager,
            is_backtest=orchestrator.is_backtest
        )

        # 2. Isolated PositionManager
        self.position_manager = PositionManager(self)

        # 3. Isolated SignalMonitor (Monitors entries)
        self.signal_monitor = SignalMonitor(self)

        # 4. Isolated PortfolioManager (Handles broker feedback)
        self.portfolio_manager = PortfolioManager(self.state_manager, instrument_name, user_id=user_id)

        logger.info(f"UserSession Initialized: User={email} | Instrument={instrument_name}")

    def is_in_trade(self, side=None):
        """Checks if this specific user is in a trade."""
        if side:
            return self.state_manager.is_in_trade(side)
        return self.state_manager.is_in_any_trade()

    async def manage_active_trades(self, timestamp, current_ticks, current_atm):
        """Monitors exits for this specific user."""
        if self.is_in_trade():
            await self.position_manager.manage_active_trades_v2(timestamp, current_ticks, current_atm)

    async def evaluate_signal(self, direction, instrument_key, signal_ltp, strike_price, timestamp, strategy_log, entry_type='BUY', quantity_multiplier=1):
        """Evaluates a market signal against user-specific state and settings."""
        if self.is_in_trade(direction):
            logger.debug(f"User {self.email} already in {direction} trade. Skipping signal.")
            return

        # SIMULTANEOUS TRADE CHECK
        allow_simultaneous = self.signal_monitor._get_user_setting('allow_simultaneous_trades', bool, fallback=False)

        # Check reversal enabled from both global and JSON-specific settings
        mode = 'buy' if entry_type == 'BUY' else 'sell'
        exit_use_reversal_global = self.signal_monitor._get_user_setting('exit_use_reversal', bool, fallback=False)
        exit_use_reversal_json = self.signal_monitor._get_user_setting('enabled', bool, fallback=False, mode=mode, category='exit_indicators/reversal')
        exit_use_reversal = exit_use_reversal_global or exit_use_reversal_json

        if not allow_simultaneous and self.is_in_trade():
            # If reversal is ON, we allow it ONLY if the active trade's formula supports it.
            # Otherwise, TradeExecutor would just skip the reversal but still be blocked by simultaneous check.
            can_reverse = False
            if exit_use_reversal:
                opposite_side = 'PUT' if direction == 'CALL' else 'CALL'
                pos_opp = self.state_manager.get_position(opposite_side)
                if pos_opp:
                    opp_mode = 'buy' if pos_opp.get('entry_type') == 'BUY' else 'sell'
                    opp_exit_formula = self.signal_monitor._get_user_setting('exit_formula', str, fallback='', mode=opp_mode)
                    if 'reversal' in opp_exit_formula.lower():
                        can_reverse = True

            if not can_reverse:
                logger.info(f"User {self.email} already in trade and simultaneous/reversal trades are disabled. Skipping {direction} signal.")
                return

        # Trigger execution for this specific user
        await self.orchestrator.trade_executor.execute_trade_v2(
            direction=direction,
            signal_instrument_key=instrument_key,
            signal_ltp=signal_ltp,
            strike_price=strike_price,
            timestamp=timestamp,
            signal_strike=strike_price,
            strategy_log=strategy_log,
            user_id=self.user_id,
            entry_type=entry_type,
            quantity_multiplier=quantity_multiplier
        )
