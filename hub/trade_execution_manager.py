from utils.logger import logger
from hub.event_bus import event_bus
import asyncio
from utils.trade_logger import TradeLogger
import datetime

trade_logger = TradeLogger()

class TradeExecutionManager:
    def __init__(self, broker_manager, state_manager, atm_manager, config_manager, rest_client=None, pnl_tracker=None):
        self.broker_manager = broker_manager
        self.state_manager = state_manager
        self.atm_manager = atm_manager
        self.config_manager = config_manager
        self.rest_client = rest_client
        self.pnl_tracker = pnl_tracker
        self.pending_trades = {}
        self.backtest_enabled = self.config_manager.get_boolean('settings', 'backtest_enabled', fallback=False)

    async def handle_price_update(self, data):
        """Callback for when an option price is updated."""
        instrument_key = data.get('instrument_key')
        ltp = data.get('ltp')
        if not instrument_key or ltp is None:
            return

        if instrument_key in self.pending_trades:
            trade_info = self.pending_trades.pop(instrument_key)
            logger.info(f"Received first price tick for {instrument_key}. Confirming trade.")
            await event_bus.publish('TRADE_CONFIRMED', {
                'direction': trade_info['direction'],
                'trade_contract': trade_info['trade_contract'],
                'ltp': ltp,
                'instrument_name': self.state_manager.instrument_name,
                'entry_type': trade_info.get('entry_type', 'BUY')
            })

    async def handle_trade_confirmation(self, **kwargs):
        """Callback for when a trade is confirmed by the broker."""
        pass

    async def handle_exit_request(self, exit_data):
        """Routes exit requests to the correct handler based on mode."""
        if self.backtest_enabled:
            await self._handle_backtest_exit(exit_data)
        else:
            await self._handle_live_exit(exit_data)

    async def _handle_live_exit(self, exit_data):
        """Handles the logic for exiting a trade in live mode."""
        direction = exit_data.get("direction")
        position = self.state_manager.call_position if direction == 'CALL' else self.state_manager.put_position
        
        if not position:
            logger.warning(f"Received an exit request for {direction}, but no active position was found.")
            return

        instrument_symbol = position.get('instrument_symbol')
        exit_price = exit_data.get('ltp')
        reason = exit_data.get('reason', 'N/A')

        logger.info(f"Logging live trade exit for {instrument_symbol} at {exit_price}. Reason: {reason}")
        trade_logger.log_exit(
            instrument=instrument_symbol,
            exit_price=exit_price,
            reason=reason
        )

        await self.state_manager.reset_trade_state(direction, exit_ltp=exit_price)

    async def _handle_backtest_exit(self, exit_data):
        """Callback for when a trade exit is requested during a backtest."""
        if not self.pnl_tracker: return

        instrument_key = exit_data.get("instrument_key")
        direction = exit_data.get("direction")
        
        entry_price = self.state_manager.current_trade.get('entry_price')
        if not entry_price:
            logger.error(f"Could not find entry price for {instrument_key}. Cannot record trade.")
            return

        timestamp = self.state_manager.get_last_tick_timestamp()
        exit_price = self.get_historical_ltp(instrument_key, timestamp) if timestamp else None
        
        if exit_price:
            self.pnl_tracker.record_trade(
                instrument_symbol=exit_data.get("instrument_symbol"),
                direction=direction,
                entry_price=entry_price,
                exit_price=exit_price
            )
            if instrument_key in self.pending_trades:
                del self.pending_trades[instrument_key]

    async def handle_trade_request(self, trade_data):
        """Callback for when a trade is requested from the TradeOrchestrator."""
        logger.info(f"DIAGNOSTIC: handle_trade_request received: {trade_data}")

        instrument_key = trade_data.get("instrument_key")
        direction = trade_data["direction"]
        
        logger.info(f"DIAGNOSTIC: Processing trade for {direction} on {instrument_key}")

        trade_contract = next((c for c in self.atm_manager.all_contracts if c.instrument_key == instrument_key), None)
        if not trade_contract:
            logger.error(f"DIAGNOSTIC: Could not find contract details for {instrument_key}. Cannot execute trade.")
            return

        logger.info(f"DIAGNOSTIC: Found trade contract: {trade_contract.instrument_key}")

        ltp = None
        if self.backtest_enabled:
            timestamp = self.state_manager.get_last_tick_timestamp()
            if timestamp:
                ltp = self.get_historical_ltp(instrument_key, timestamp)
        else:
            ltp = self.state_manager.option_prices.get(instrument_key)

        if not ltp:
            logger.error(f"DIAGNOSTIC: Could not get LTP for {instrument_key}. Cannot execute trade.")
            return

        logger.info(f"DIAGNOSTIC: Found LTP: {ltp}")

        logger.info(f"Executing S/R-confirmed trade for {direction} on {trade_contract.instrument_key} at LTP {ltp}")
        
        logger.info("DIAGNOSTIC: Calling trade_logger.log_entry...")
        trade_logger.log_entry(
            broker="PaperTrade",
            instrument=trade_contract.instrument_key,
            trade_type=direction,
            entry_price=ltp,
            trade_data={}
        )
        logger.info("DIAGNOSTIC: trade_logger.log_entry call complete.")

        self.pending_trades[instrument_key] = {
            "direction": direction,
            "trade_contract": trade_contract,
            "ltp": ltp,
            "confirmation_event": asyncio.Event()
        }

        logger.info("DIAGNOSTIC: Calling broker_manager.broadcast_entry_signal...")
        self.broker_manager.broadcast_entry_signal(
            direction=direction,
            instrument_key=instrument_key,
            instrument_symbol=trade_contract.instrument_key,
            ltp=ltp,
            strike_price=trade_contract.strike_price
        )
        logger.info("DIAGNOSTIC: broker_manager.broadcast_entry_signal call complete.")

    async def get_historical_ltp(self, instrument_key, timestamp):
        """Fetches the historical LTP for a given instrument and timestamp."""
        if not self.rest_client:
            logger.error("REST client is not available for historical LTP fetching.")
            return None
        
        to_date = timestamp.date()
        from_date = to_date # For intraday, from_date is the same as to_date
        
        try:
            # Use asyncio.to_thread to run the synchronous SDK call in the event loop
            historical_data = await asyncio.to_thread(
                self.rest_client.get_historical_candle_data,
                instrument_key, '1minute', to_date, from_date
            )
            if historical_data is not None and not historical_data.empty:
                # Find the candle closest to the exit timestamp
                closest_time_index = historical_data.index.get_indexer([timestamp], method='nearest')[0]
                return historical_data.iloc[closest_time_index]['close']
        except Exception as e:
            logger.error(f"Error fetching historical LTP for {instrument_key}: {e}", exc_info=True)
        
        return None
