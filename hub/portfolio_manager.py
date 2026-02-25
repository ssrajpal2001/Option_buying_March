from utils.logger import logger
from hub.event_bus import event_bus

class PortfolioManager:
    def __init__(self, state_manager, instrument_name, user_id=None):
        self.state_manager = state_manager
        self.instrument_name = instrument_name
        self.user_id = user_id
        event_bus.subscribe('TRADE_CONFIRMED', self.handle_trade_confirmed)
        event_bus.subscribe('TRADE_CLOSED', self.handle_trade_closed)

    async def handle_trade_closed(self, data):
        """Callback for when a trade is closed."""
        # Ensure event is for THIS user and THIS instrument
        if self.user_id and data.get('user_id') != self.user_id:
            return

        if data.get('instrument_name') != self.instrument_name:
            return

        direction = data.get('direction')
        await self.state_manager.reset_trade_state(direction)
        # Signal that the trade is closed (margin released)
        self.state_manager.trade_closed_event.set()

    async def handle_trade_confirmed(self, data):
        """Callback for when a trade is confirmed by the broker."""
        # Ensure event is for THIS user and THIS instrument
        if self.user_id and data.get('user_id') != self.user_id:
            return

        if data.get('instrument_name') != self.instrument_name:
            return

        direction = data.get('direction')
        trade_contract = data.get('trade_contract')
        ltp = data.get('ltp')
        symbol = data.get('instrument_symbol', trade_contract.instrument_key)
        entry_type = data.get('entry_type', 'BUY')

        pos = await self.state_manager.enter_trade(
            direction=direction,
            instrument_key=trade_contract.instrument_key,
            instrument_symbol=symbol,
            ltp=ltp,
            strike_price=trade_contract.strike_price,
            entry_type=entry_type
        )
        if pos:
            pos['entry_confirmed_by_broker'] = True

        # Signal that the trade is confirmed
        self.state_manager.trade_confirmed_event.set()
        logger.info(f"[{self.instrument_name}] Trade confirmed for {direction} on {symbol} (User: {self.user_id})")
