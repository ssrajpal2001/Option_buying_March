from .base_orchestrator import BaseOrchestrator
import datetime
import pytz
from utils.logger import logger
from .event_bus import event_bus
import asyncio

class LiveOrchestrator(BaseOrchestrator):
    def __init__(self, *args, **kwargs):
        kwargs['is_backtest'] = False
        super().__init__(*args, **kwargs)
        self.subscribed_instruments = set()

        # Dynamic watchlist management for traded ITM contracts
        event_bus.subscribe('ADD_TO_WATCHLIST', self.handle_add_to_watchlist)
        event_bus.subscribe('REMOVE_FROM_WATCHLIST', self.handle_remove_from_watchlist)

    async def handle_add_to_watchlist(self, data):
        instrument_key = data.get('instrument_key')
        if instrument_key and instrument_key not in self.subscribed_instruments:
            logger.debug(f"Dynamically adding {instrument_key} to watchlist for live P&L display.")
            self.subscribed_instruments.add(instrument_key)
            self.websocket.subscribe([instrument_key])

    async def handle_remove_from_watchlist(self, data):
        instrument_key = data.get('instrument_key')
        if instrument_key and instrument_key in self.subscribed_instruments:
            logger.debug(f"Dynamically removing {instrument_key} from watchlist after trade exit.")
            self.subscribed_instruments.discard(instrument_key)
            self.websocket.unsubscribe([instrument_key])

    def _get_timestamp(self):
        """Returns the latest exchange timestamp if available, otherwise falls back to local time."""
        if hasattr(self, 'state_manager') and self.state_manager.last_exchange_time:
            # We use the exchange timestamp if it's not too stale (e.g. within 5 seconds)
            # This ensures bot logic stays in sync with market data even with network jitter.
            now_system = datetime.datetime.now(pytz.timezone('Asia/Kolkata'))
            if (now_system - self.state_manager.last_exchange_time).total_seconds() < 5:
                return self.state_manager.last_exchange_time

        return datetime.datetime.now(pytz.timezone('Asia/Kolkata'))

    def _is_trade_active(self, user_id=None):
        """Checks if trades are active globally or for a specific user."""
        if user_id and user_id in self.user_sessions:
            return self.user_sessions[user_id].is_in_trade()

        # Return True if ANY user session has an active trade
        return any(session.is_in_trade() for session in self.user_sessions.values())

    async def _check_backtest_exit_conditions(self, timestamp):
        # This method is required by the abstract base class, but does nothing in live mode.
        pass
