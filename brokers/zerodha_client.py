import sys
from kiteconnect import KiteConnect
from .base_broker import BaseBroker
from utils.logger import logger
from utils.auth_manager_zerodha import handle_zerodha_login
from hub.event_bus import event_bus
import os

class ZerodhaClient(BaseBroker):
    def __init__(self, broker_instance_name, config_manager, login_required=True):
        super().__init__(broker_instance_name, config_manager)
        self.kite = None

        if login_required:
            # The credentials section is needed by the auth manager
            credentials_section = self.config_manager.get(broker_instance_name, 'credentials')
            try:
                # Use the centralized login handler which manages the entire auth flow
                self.kite = handle_zerodha_login(credentials_section, self.config_manager)
                if self.kite:
                    profile = self.kite.profile()
                    logger.info(f"Zerodha authentication successful for user: {profile.get('user_id')} [{self.instance_name}].")
                else:
                    # If handle_zerodha_login returns None, it means authentication failed
                    raise Exception("The authentication process failed and did not return a client.")
            except Exception as e:
                logger.critical(f"AUTHENTICATION FAILED for Zerodha account [{self.instance_name}]. Reason: {e}", exc_info=True)
                sys.exit(1)

    def connect(self):
        # The connection is already established in the __init__ method via handle_zerodha_login
        # This method is here to satisfy the abstract base class requirements.
        pass

    async def handle_entry_signal(self, **kwargs):
        """Handles an entry signal by placing an order."""
        if not self.kite:
            logger.error(f"Kite connect object not initialized for '{self.instance_name}'. Cannot place order.")
            return

        contract = kwargs.get('contract')
        instrument_name = kwargs.get('instrument_name')
        direction = kwargs.get('direction')
        signal_expiry_date = kwargs.get('signal_expiry_date') # Extract the expiry date

        if not all([contract, instrument_name, direction, signal_expiry_date]):
            logger.error(f"handle_entry_signal missing required arguments. Data: {kwargs}")
            return

        broker_base_qty = self.config_manager.get_int(self.instance_name, 'quantity', 1)
        instrument_lot_size = contract.lot_size
        final_qty = broker_base_qty * instrument_lot_size
        logger.info(f"Calculated trade quantity for {self.instance_name} on {instrument_name}: Broker Qty ({broker_base_qty}) * Lot Size ({instrument_lot_size}) = {final_qty}")

        try:
            order_id = None
            instrument_symbol = self.construct_zerodha_symbol(contract, signal_expiry_date)

            entry_type = kwargs.get('entry_type', 'BUY')
            transaction_type = "BUY" if entry_type == 'BUY' else "SELL"

            if self.paper_trade:
                logger.info(f"--- ZERODHA [PAPER TRADE] ENTRY ({direction} - {entry_type}) ---")
                logger.info(f"  Instrument: {instrument_symbol}")
                logger.info(f"  Quantity: {final_qty} | Price: {kwargs.get('ltp', 0)}")
                order_id = "PAPER_ZERODHA_ORDER"
            else:
                product_type = kwargs.get('product_type', 'NRML')
                order_id = self.place_order(contract, transaction_type, final_qty, signal_expiry_date, product_type=product_type)

            if order_id:
                if not self.paper_trade:
                    logger.info(f"Successfully placed {transaction_type} ({direction}) order for {instrument_symbol} with order_id: {order_id}")

                # Feedback to StateManager (Multi-Tenant aware)
                await event_bus.publish('TRADE_CONFIRMED', {
                    'user_id': self.user_id,
                    'instrument_name': instrument_name,
                    'direction': direction,
                    'trade_contract': contract,
                    'instrument_symbol': instrument_symbol,
                    'ltp': kwargs.get('ltp', 0)
                })

                self.trade_logger.log_entry(
                    broker=self.instance_name,
                    instrument_name=instrument_name,
                    instrument_symbol=instrument_symbol,
                    trade_type=direction,
                    price=kwargs.get('ltp', 0),
                    strategy_log=kwargs.get('strategy_log', ""),
                    user_id=self.user_id
                )
                return order_id
            else:
                logger.error(f"Failed to place BUY ({direction}) order for {contract.instrument_key}.")
                return None
        except Exception as e:
            logger.error(f"Exception placing order for {contract.instrument_key}: {e}", exc_info=True)
            return None

    async def handle_close_signal(self, **kwargs):
        """Handles a close signal by closing the corresponding position."""
        if not self.kite:
            logger.error(f"Kite connect object not initialized for '{self.instance_name}'. Cannot close position.")
            return

        side = kwargs.get('side')
        instrument_name = kwargs.get('instrument_name')

        # Prioritize contract/position from payload, fallback to StateManager
        contract = kwargs.get('contract')
        position = self.state_manager.get_position(side)

        if not contract and position:
            contract = position.get('contract')

        if not contract:
            logger.warning(f"No active {side} position or contract found to close.")
            return

        # Calculate quantity
        broker_base_qty = self.config_manager.get_int(self.instance_name, 'quantity', 1)
        instrument_lot_size = contract.lot_size
        final_qty = broker_base_qty * instrument_lot_size

        try:
            order_id = None
            signal_expiry_date = kwargs.get('signal_expiry_date')
            instrument_symbol = self.construct_zerodha_symbol(contract, signal_expiry_date)

            entry_type = position.get('entry_type', 'BUY') if position else 'BUY'
            exit_transaction_type = "SELL" if entry_type == 'BUY' else "BUY"

            if self.paper_trade:
                logger.info(f"--- ZERODHA [PAPER TRADE] EXIT ({side} - {exit_transaction_type}) ---")
                logger.info(f"  Instrument: {instrument_symbol}")
                logger.info(f"  Quantity: {final_qty} | Price: {kwargs.get('ltp', 0)}")
                order_id = "PAPER_ZERODHA_EXIT"
            else:
                order_id = self.place_order(contract, exit_transaction_type, final_qty, signal_expiry_date)

            if order_id:
                if not self.paper_trade:
                    logger.info(f"Successfully placed {exit_transaction_type} order to close {side} position for {instrument_symbol} with order_id: {order_id}")

                # Feedback to StateManager
                await event_bus.publish('TRADE_CLOSED', {
                    'user_id': self.user_id,
                    'instrument_name': instrument_name,
                    'direction': side
                })

                # Log the live trade exit to the unified logger
                price = kwargs.get('ltp', 0)
                reason = kwargs.get('reason', 'UNKNOWN')
                entry_price = position.get('entry_price', 0) if position else 0
                pnl = (price - entry_price) if entry_price > 0 else 0

                self.trade_logger.log_exit(
                    broker=self.instance_name,
                    instrument_name=instrument_name,
                    instrument_symbol=instrument_symbol,
                    trade_type=f"EXIT_{side}",
                    price=price,
                    pnl=pnl,
                    reason=reason,
                    strategy_log=kwargs.get('strategy_log', ""),
                    user_id=self.user_id
                )
            else:
                logger.error(f"Failed to close {side} position for {contract.instrument_key}.")
        except Exception as e:
            logger.error(f"Exception closing position for {contract.instrument_key}: {e}", exc_info=True)

    def place_order(self, contract, transaction_type, quantity, signal_expiry_date, product_type='NRML'):
        """
        Places an order with Zerodha.
        Generates the Zerodha-specific symbol from the contract object.
        product_type: 'NRML' for carry-forward (sell strangle legs), 'MIS' for intraday (buy hedge legs).
        """
        symbol = self.construct_zerodha_symbol(contract, signal_expiry_date)
        if not symbol:
            logger.error(f"Could not construct a valid symbol for contract: {contract.__dict__}")
            return None

        if transaction_type == "BUY":
            zerodha_transaction_type = self.kite.TRANSACTION_TYPE_BUY
        else:
            zerodha_transaction_type = self.kite.TRANSACTION_TYPE_SELL

        # Resolve correct Zerodha exchange (NFO for NSE, BFO for BSE)
        exchange = self.kite.EXCHANGE_NFO
        if hasattr(contract, 'exchange') and contract.exchange == 'BSE':
            exchange = self.kite.EXCHANGE_BFO

        zerodha_product = self.kite.PRODUCT_NRML if product_type == 'NRML' else self.kite.PRODUCT_MIS

        try:
            order_id = self.kite.place_order(
                tradingsymbol=symbol,
                exchange=exchange,
                transaction_type=zerodha_transaction_type,
                quantity=quantity,
                variety=self.kite.VARIETY_REGULAR,
                order_type=self.kite.ORDER_TYPE_MARKET,
                product=zerodha_product
            )
            return order_id
        except Exception as e:
            logger.error(f"Error placing order with Zerodha. Symbol: {symbol}, Exchange: {exchange}, Type: {transaction_type}, Qty: {quantity}. API Error: {e}", exc_info=True)
            return None

    def construct_zerodha_symbol(self, contract, signal_expiry_date=None):
        """
        Constructs a Zerodha-compatible trading symbol for a given contract.
        Determines monthly vs weekly by checking if the contract's expiry falls
        on the last Thursday of its month (NSE monthly options always expire on last Thursday).
        - Monthly format: NIFTY<YY><MON><STRIKE>CE (e.g., NIFTY26MAR25650PE)
        - Weekly format:  NIFTY<YY><M><DD><STRIKE>CE (e.g., NIFTY262625650PE for Feb 26th)
        """
        import datetime, calendar

        # Dynamic prefix detection: BANKNIFTY, NIFTY, SENSEX, etc.
        instrument_name = getattr(contract, 'name', 'NIFTY').upper()
        if instrument_name == "NIFTY 50": instrument_name = "NIFTY"
        elif instrument_name == "NIFTY BANK": instrument_name = "BANKNIFTY"

        expiry = contract.expiry
        strike = int(contract.strike_price)
        option_type = contract.instrument_type.upper()
        year_str = expiry.strftime('%y')

        # Normalize to date object (expiry can be datetime or date)
        expiry_date_obj = expiry.date() if isinstance(expiry, datetime.datetime) else expiry

        # Detect monthly expiry: NSE monthly options always expire on the last Thursday of the month
        last_day_of_month = calendar.monthrange(expiry_date_obj.year, expiry_date_obj.month)[1]
        last_thursday = None
        for day in range(last_day_of_month, last_day_of_month - 7, -1):
            candidate = datetime.date(expiry_date_obj.year, expiry_date_obj.month, day)
            if candidate.weekday() == 3:  # Thursday
                last_thursday = candidate
                break
        is_monthly_expiry = (expiry_date_obj == last_thursday)

        logger.debug(f"construct_zerodha_symbol: expiry={expiry_date_obj}, last_thursday={last_thursday}, is_monthly={is_monthly_expiry}")

        if is_monthly_expiry:
            # Monthly format: NIFTY<YY><MON><STRIKE>CE
            month_str = expiry.strftime('%b').upper()
            return f"{instrument_name}{year_str}{month_str}{strike}{option_type}"
        else:
            # Weekly format: NIFTY<YY><M><D><STRIKE>CE
            # Month mapping for weekly: 1-9, O, N, D
            month_val = expiry.month
            if month_val == 10: month_char = 'O'
            elif month_val == 11: month_char = 'N'
            elif month_val == 12: month_char = 'D'
            else: month_char = str(month_val)

            day_str = expiry.strftime('%d') # Day with leading zero
            return f"{instrument_name}{year_str}{month_char}{day_str}{strike}{option_type}"

    def get_ltp(self, symbol, exchange=None):
        """Gets the Last Traded Price for a symbol."""
        if not self.kite:
            return 0.0

        # Auto-detect exchange if not provided
        if exchange is None:
            exchange = 'BFO' if 'SENSEX' in symbol.upper() else 'NFO'

        try:
            # Format for LTP call: "EXCHANGE:TRADINGSYMBOL"
            instrument = f"{exchange}:{symbol}"
            quote = self.kite.ltp(instrument)
            return quote[instrument]['last_price']
        except Exception as e:
            logger.error(f"Error fetching LTP from Zerodha for {symbol} ({exchange}): {e}")
            return 0.0

    def close_all_positions(self):
        """Closes all open positions."""
        logger.info(f"[{self.instance_name}] Closing all open Zerodha positions (logic to be implemented).")
        # NOTE: Zerodha API requires fetching all open positions and then iterating
        # through them to place opposite orders. This is a placeholder.
        pass
