from .base_broker import BaseBroker
from utils.logger import logger
from utils.trade_logger import TradeLogger
from hub.event_bus import event_bus

class PaperTradeClient(BaseBroker):
    def __init__(self, broker_instance_name, config_manager, **kwargs):
        super().__init__(broker_instance_name, config_manager, **kwargs)
        logger.info(f"{self.instance_name} initialized for instruments: {list(self.instruments)}")

    def connect(self):
        self.is_connected = True
        logger.info(f"PaperTradeClient '{self.instance_name}' is ready.")

    async def handle_entry_signal(self, **kwargs):
        instrument_name = kwargs.get('instrument_name')
        instrument_symbol = kwargs.get('instrument_symbol')
        direction = kwargs.get('direction')
        price = kwargs.get('ltp', 0)
        entry_type = kwargs.get('entry_type', 'BUY')

        logger.info(f"--- [{self.instance_name}] PAPER TRADE ENTRY ({direction} - {entry_type}) for {instrument_name} ---")
        logger.info(f"Instrument: {instrument_symbol} | Entry Price: {price}")

        # Use the new instrument-aware logger
        self.trade_logger.log_entry(
            broker=self.instance_name,
            instrument_name=instrument_name,
            instrument_symbol=instrument_symbol,
            trade_type=direction,
            price=price,
            strategy_log=kwargs.get('strategy_log', ""),
            user_id=self.user_id
        )

        # Feedback to StateManager (Paper trade confirms immediately)
        await event_bus.publish('TRADE_CONFIRMED', {
            'user_id': self.user_id,
            'instrument_name': instrument_name,
            'direction': direction,
            'trade_contract': kwargs.get('contract'),
            'ltp': price,
            'entry_type': entry_type
        })

    async def handle_close_signal(self, **kwargs):
        instrument_name = kwargs.get('instrument_name')
        direction = kwargs.get('direction')
        price = kwargs.get('ltp')
        reason = kwargs.get('reason', 'UNKNOWN')

        if not self.state_manager.is_in_trade(direction):
            logger.warning(f"[{self.instance_name}] Received close signal for {direction} but StateManager reports no active trade. Ignoring.")
            return

        position = self.state_manager.get_position(direction)
        instrument_symbol = position.get('instrument_symbol')
        entry_price = position.get('entry_price', 0)
        entry_type = position.get('entry_type', 'BUY')

        if entry_type == 'SELL':
            pnl = (entry_price - price) if entry_price > 0 else 0
        else:
            pnl = (price - entry_price) if entry_price > 0 else 0

        logger.info(f"--- [{self.instance_name}] PAPER TRADE EXIT ({direction} - {entry_type}) for {instrument_name} ---")
        logger.info(f"Instrument: {instrument_symbol} | Exit Price: {price} | PNL: {pnl:.2f} | Reason: {reason}")

        # Use the new instrument-aware logger
        self.trade_logger.log_exit(
            broker=self.instance_name,
            instrument_name=instrument_name,
            instrument_symbol=instrument_symbol,
            trade_type=f"EXIT_{direction}",
            price=price,
            pnl=pnl,
            reason=reason,
            strategy_log=kwargs.get('strategy_log', ""),
            user_id=self.user_id
        )

        # Feedback to StateManager
        await event_bus.publish('TRADE_CLOSED', {
            'user_id': self.user_id,
            'instrument_name': instrument_name,
            'direction': direction
        })

    # --- Abstract Method Implementations ---

    def place_order(self, contract, transaction_type, quantity, signal_expiry_date=None):
        return "PAPER_ORDER_ID"

    def construct_zerodha_symbol(self, contract, signal_expiry_date=None):
        """Returns a readable symbol for reporting purposes."""
        if not contract:
            return "N/A"
        name = getattr(contract, 'name', 'NIFTY').upper()
        if "NIFTY 50" in name: name = "NIFTY"
        elif "NIFTY BANK" in name: name = "BANKNIFTY"

        strike = int(contract.strike_price)
        opt_type = contract.instrument_type.upper()
        expiry = contract.expiry
        expiry_str = expiry.strftime('%d%b') if hasattr(expiry, 'strftime') else str(expiry)

        return f"{name} {expiry_str} {strike} {opt_type}"

    def close_all_positions(self):
        logger.info(f"[{self.instance_name}] End-of-day closure: Closing all open paper trade positions.")
        if self.state_manager:
            if self.state_manager.is_in_trade('CALL'):
                self.handle_close_signal(direction='CALL', reason="EOD")
            if self.state_manager.is_in_trade('PUT'):
                self.handle_close_signal(direction='PUT', reason="EOD")

    def translate_symbol(self, standard_symbol):
        # Paper trading uses the trading symbol directly.
        return standard_symbol

    def execute_trade(self, trade_type, instrument_symbol, stop_loss_price):
        # Placeholder to satisfy the abstract base class.
        pass

    def _place_live_order(self, broker_symbol):
        # Not applicable for a paper trading client.
        pass

    def _log_paper_trade(self, broker_symbol):
        # This is the core function of this class, but the logic is handled
        # in handle_entry_signal and handle_close_signal for clarity.
        pass
