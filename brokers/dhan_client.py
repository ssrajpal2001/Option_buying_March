import sys
import datetime
import pandas as pd
from dhanhq import dhanhq
from .base_broker import BaseBroker
from utils.logger import logger
from utils.auth_manager_dhan import handle_dhan_login
from hub.event_bus import event_bus

class DhanClient(BaseBroker):
    def __init__(self, broker_instance_name, config_manager, login_required=True, user_id=None, db_config=None):
        super().__init__(broker_instance_name, config_manager, user_id=user_id, db_config=db_config)
        self.dhan = None
        self._security_id_cache = {} # (name, expiry, strike, type) -> security_id
        self._security_list_df = None
        self._last_security_list_load = None

        if login_required:
            if self.db_config:
                # Multi-tenant DB path
                try:
                    # User said only access_token is required.
                    # Dhan library constructor: (client_id, access_token)
                    # We'll use api_key as client_id and api_secret as access_token from DB
                    client_id = self.db_config.get('api_key', '')
                    access_token = self.db_config.get('api_secret', '')
                    self.dhan = dhanhq(client_id, access_token)
                    logger.info(f"Dhan client initialized from DB for User ID: {self.user_id}.")
                except Exception as e:
                    logger.error(f"Failed to initialize Dhan client for user {self.user_id}: {e}")
            else:
                # Legacy INI path
                credentials_section = self.config_manager.get(broker_instance_name, 'credentials')
                try:
                    self.dhan = handle_dhan_login(credentials_section, self.config_manager)
                    logger.info(f"Dhan authentication successful for {credentials_section} [{self.instance_name}].")
                except Exception as e:
                    logger.critical(f"AUTHENTICATION FAILED for Dhan account [{self.instance_name}]. Reason: {e}", exc_info=True)
                    sys.exit(1)

    def connect(self):
        # Established in __init__
        pass

    async def _load_security_list(self):
        """Downloads and loads the Dhan security list into a DataFrame."""
        now = datetime.datetime.now()
        # Reload once per day
        if self._security_list_df is not None and self._last_security_list_load and self._last_security_list_load.date() == now.date():
            return

        try:
            logger.info("Dhan: Downloading security list...")
            # mode='compact' should be enough for basic info
            self._security_list_df = self.dhan.fetch_security_list(mode='compact')
            self._last_security_list_load = now
            logger.info(f"Dhan: Security list loaded. Total records: {len(self._security_list_df)}")

            # Print columns for debugging if it fails
            # logger.debug(f"Dhan Security List Columns: {self._security_list_df.columns.tolist()}")
        except Exception as e:
            logger.error(f"Failed to load Dhan security list: {e}")
            self._security_list_df = None

    async def get_security_id(self, contract):
        """Finds the security_id for a given contract."""
        # Use our own logic to map contract info to Dhan's security list
        strike = float(contract.strike_price)
        expiry = contract.expiry.date() if isinstance(contract.expiry, datetime.datetime) else contract.expiry
        opt_type = contract.instrument_type.upper() # 'CE' or 'PE'
        name = contract.name.upper()

        cache_key = (name, expiry, strike, opt_type)
        if cache_key in self._security_id_cache:
            return self._security_id_cache[cache_key]

        await self._load_security_list()

        if self._security_list_df is None:
            return None

        try:
            df = self._security_list_df

            # Dhan Expiry is usually in 'YYYY-MM-DD HH:MM:SS' format in the CSV
            # Let's convert SEM_EXPIRY_DATE to date
            if 'SEM_EXPIRY_DATE' in df.columns:
                # Use errors='coerce' to handle '0001-01-01' and other malformed dates
                df['expiry_date'] = pd.to_datetime(df['SEM_EXPIRY_DATE'], errors='coerce').dt.date
            else:
                logger.error("Dhan: SEM_EXPIRY_DATE column missing in security list.")
                return None

            # Filter conditions
            # For Options (OPTIDX), Dhan uses SEM_TRADING_SYMBOL which starts with Name
            # e.g. NIFTY-Feb2026-25000-CE

            # Strategy 1: Trading Symbol Match (e.g. NIFTY-Feb2026-25000-CE)
            mask1 = (
                (df['SEM_TRADING_SYMBOL'].str.startswith(name + "-")) &
                (df['expiry_date'] == expiry) &
                (df['SEM_OPTION_TYPE'].str.upper() == opt_type) &
                (df['SEM_STRIKE_PRICE'].astype(float) == strike)
            )

            # Strategy 2: Custom Symbol Match (e.g. NIFTY 24 FEB 25000 CALL)
            # We don't know the exact format of SEM_CUSTOM_SYMBOL but it usually contains these parts
            mask2 = (
                (df['SEM_CUSTOM_SYMBOL'].str.contains(name, case=False, na=False)) &
                (df['SEM_CUSTOM_SYMBOL'].str.contains(str(int(strike)), na=False)) &
                (df['expiry_date'] == expiry) &
                (df['SEM_OPTION_TYPE'].str.upper() == opt_type)
            )

            result = df[mask1]
            if result.empty:
                logger.debug(f"Dhan: Strategy 1 (Trading Symbol) failed for {name}. Trying Strategy 2 (Custom Symbol)...")
                result = df[mask2]

            if result.empty:
                # Strategy 3: Fuzzy Expiry Match (Same strike, type, symbol name, but +/- 2 days on expiry)
                logger.debug(f"Dhan: Strategy 2 failed. Trying Strategy 3 (Fuzzy Expiry)...")
                mask3 = (
                    (df['SEM_TRADING_SYMBOL'].str.startswith(name + "-")) &
                    (pd.to_datetime(df['expiry_date']).dt.date >= expiry - datetime.timedelta(days=2)) &
                    (pd.to_datetime(df['expiry_date']).dt.date <= expiry + datetime.timedelta(days=2)) &
                    (df['SEM_OPTION_TYPE'].str.upper() == opt_type) &
                    (df['SEM_STRIKE_PRICE'].astype(float) == strike)
                )
                result = df[mask3]

            if not result.empty:
                security_id = str(result.iloc[0]['SEM_SMST_SECURITY_ID'])
                self._security_id_cache[cache_key] = security_id
                logger.info(f"Dhan: Found SecurityID {security_id} for {name} {strike} {opt_type} {expiry}")
                return security_id
            else:
                logger.warning(f"Dhan: No match found in security list for {name} {strike} {opt_type} {expiry}")
                return None

        except Exception as e:
            logger.error(f"Error searching Dhan security list: {e}", exc_info=True)
            return None

    async def handle_entry_signal(self, **kwargs):
        if not self.dhan:
            logger.error(f"Dhan client not initialized for '{self.instance_name}'.")
            return

        contract = kwargs.get('contract')
        instrument_name = kwargs.get('instrument_name')
        direction = kwargs.get('direction')

        if not all([contract, instrument_name, direction]):
            logger.error("Dhan: handle_entry_signal missing required arguments.")
            return

        broker_base_qty = self.config_manager.get_int(self.instance_name, 'quantity', 1)
        instrument_lot_size = contract.lot_size
        final_qty = broker_base_qty * instrument_lot_size

        try:
            security_id = await self.get_security_id(contract)
            if not security_id:
                logger.error(f"Dhan: Could not resolve security_id for {contract.instrument_key}")
                return None

            order_id = None
            if self.paper_trade:
                logger.info(f"--- DHAN [PAPER TRADE] ENTRY ({direction}) ---")
                logger.info(f"  Instrument: {contract.instrument_key} | SecurityID: {security_id}")
                logger.info(f"  Quantity: {final_qty} | Price: {kwargs.get('ltp', 0)}")
                order_id = "PAPER_DHAN_ORDER"
            else:
                order_id = self.place_order(security_id, "BUY", final_qty, contract)

            if order_id:
                instrument_symbol = self.construct_dhan_symbol(contract)
                if not self.paper_trade:
                    logger.info(f"Successfully placed Dhan BUY ({direction}) order for {instrument_symbol}, ID: {order_id}")

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
        except Exception as e:
            logger.error(f"Exception placing Dhan entry order: {e}", exc_info=True)
            return None

    async def handle_close_signal(self, **kwargs):
        if not self.dhan:
            logger.error(f"Dhan client not initialized for '{self.instance_name}'.")
            return

        side = kwargs.get('side')
        instrument_name = kwargs.get('instrument_name')

        # Prioritize contract/position from payload, fallback to StateManager
        contract = kwargs.get('contract')
        position = self.state_manager.get_position(side)

        if not contract and position:
            contract = position.get('contract')

        if not contract:
            logger.warning(f"No active {side} position found on Dhan.")
            return
        broker_base_qty = self.config_manager.get_int(self.instance_name, 'quantity', 1)
        final_qty = broker_base_qty * contract.lot_size

        try:
            security_id = await self.get_security_id(contract)
            if not security_id:
                logger.error(f"Dhan: Could not resolve security_id for exit of {contract.instrument_key}")
                return

            order_id = None
            if self.paper_trade:
                logger.info(f"--- DHAN [PAPER TRADE] EXIT ({side}) ---")
                logger.info(f"  Instrument: {contract.instrument_key}")
                logger.info(f"  Quantity: {final_qty} | Price: {kwargs.get('ltp', 0)}")
                order_id = "PAPER_DHAN_EXIT"
            else:
                order_id = self.place_order(security_id, "SELL", final_qty, contract)

            if order_id:
                if not self.paper_trade:
                    logger.info(f"Successfully placed Dhan SELL order to close {side} position, ID: {order_id}")

                await event_bus.publish('TRADE_CLOSED', {
                    'user_id': self.user_id,
                    'instrument_name': instrument_name,
                    'direction': side
                })

                price = kwargs.get('ltp', 0)
                reason = kwargs.get('reason', 'UNKNOWN')
                entry_price = position.get('entry_price', 0) if position else 0
                pnl = (price - entry_price) if entry_price > 0 else 0

                instrument_symbol = self.construct_dhan_symbol(contract)
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
        except Exception as e:
            logger.error(f"Exception closing Dhan position: {e}", exc_info=True)

    def place_order(self, security_id, transaction_type, quantity, contract):
        if transaction_type == "BUY":
            dhan_transaction_type = self.dhan.BUY
        else:
            dhan_transaction_type = self.dhan.SELL

        # Map Exchange
        exchange = contract.exchange.upper() if hasattr(contract, 'exchange') else 'NSE'
        if 'NSE' in exchange:
            segment = self.dhan.NSE_FNO
        elif 'BSE' in exchange:
            segment = self.dhan.BSE_FNO
        else:
            segment = self.dhan.NSE_FNO

        try:
            # Dhan API expects string security_id
            # Price is required even for MARKET orders (use 0)
            response = self.dhan.place_order(
                security_id=str(security_id),
                exchange_segment=segment,
                transaction_type=dhan_transaction_type,
                quantity=int(quantity),
                order_type=self.dhan.MARKET,
                product_type=self.dhan.MARGIN, # Carry Forward for Options
                price=0.0,
                validity='DAY'
            )

            if response.get('status') == 'success':
                return response.get('data', {}).get('orderId')
            else:
                logger.error(f"Dhan order failed. Resp: {response}")
                return None
        except Exception as e:
            logger.error(f"Error in Dhan place_order: {e}", exc_info=True)
            return None

    def get_ltp(self, symbol):
        return 0.0

    def construct_dhan_symbol(self, contract):
        """Constructs a readable symbol for Dhan contracts."""
        strike = int(contract.strike_price)
        expiry = contract.expiry.date() if isinstance(contract.expiry, datetime.datetime) else contract.expiry
        opt_type = contract.instrument_type.upper()
        name = contract.name.upper()
        # Format: NIFTY 24 FEB 25000 CE
        return f"{name} {expiry.strftime('%d %b').upper()} {strike} {opt_type}"

    def close_all_positions(self):
        logger.info(f"[{self.instance_name}] Closing all Dhan positions (Placeholder).")
        pass
