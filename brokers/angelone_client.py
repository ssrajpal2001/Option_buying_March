import sys
import datetime
import pandas as pd
import requests
from .base_broker import BaseBroker
from utils.logger import logger
from utils.auth_manager_angelone import handle_angelone_login
from hub.event_bus import event_bus

class AngelOneClient(BaseBroker):
    def __init__(self, broker_instance_name, config_manager, login_required=True, user_id=None, db_config=None, **kwargs):
        super().__init__(broker_instance_name, config_manager, user_id=user_id, db_config=db_config, **kwargs)
        self.smart_api = None
        self._token_map = None # symbol_key -> {token, tradingsymbol, lot_size}
        self._last_token_load = None

        if login_required:
            if self.db_config:
                try:
                    self.smart_api = handle_angelone_login(self.db_config)
                    if self.smart_api:
                        logger.info(f"AngelOne client initialized from DB for User ID: {self.user_id}.")
                except Exception as e:
                    logger.error(f"Failed to initialize AngelOne client for user {self.user_id}: {e}")
            else:
                credentials_section = self.config_manager.get(broker_instance_name, 'credentials', fallback=broker_instance_name)
                try:
                    self.smart_api = handle_angelone_login(credentials_section, self.config_manager)
                    if self.smart_api:
                        logger.info(f"AngelOne authentication successful for {credentials_section} [{self.instance_name}].")
                except Exception as e:
                    logger.critical(f"AUTHENTICATION FAILED for AngelOne account [{self.instance_name}]. Reason: {e}", exc_info=True)
                    sys.exit(1)

    def connect(self):
        pass

    async def _load_token_map(self):
        """Downloads and processes the Angel One token master file."""
        now = datetime.datetime.now()
        if self._token_map is not None and self._last_token_load and self._last_token_load.date() == now.date():
            return

        url = "https://margincalculator.angelbroking.com/OpenAPI_Standard_MSil.php?Exchange=NFO"
        try:
            logger.info("AngelOne: Downloading token master...")
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=30) as response:
                    data = await response.json()

            # Create a lookup map for faster access
            # Key: (name, expiry, strike, type)
            self._token_map = {}
            for item in data:
                # Angel One expiry format: 29JAN2026
                # tradingsymbol: NIFTY29JAN2625000CE
                # symbol: NIFTY29JAN2625000CE
                # name: NIFTY
                # strike: 2500000 (Note: strike is often multiplied by 100 in some brokers, check this)
                # item['strike'] is usually a string, e.g. "25000.000000"

                name = item['name'].upper()
                try:
                    expiry_date = datetime.datetime.strptime(item['expiry'], '%d%b%Y').date()
                except:
                    continue

                # Robust strike handling: Some strikes are scaled by 100, others aren't.
                # Index options are usually absolute. Stock options might be scaled.
                raw_strike = float(item['strike'])
                strike = raw_strike / 100.0 if raw_strike > 100000 else raw_strike

                option_type = 'CE' if item['symbol'].endswith('CE') else 'PE' if item['symbol'].endswith('PE') else 'XX'

                key = (name, expiry_date, strike, option_type)
                self._token_map[key] = {
                    'token': item['token'],
                    'tradingsymbol': item['symbol'],
                    'lotsize': int(item['lotsize'])
                }

            self._last_token_load = now
            logger.info(f"AngelOne: Token master loaded. Total records: {len(self._token_map)}")
        except Exception as e:
            logger.error(f"Failed to load AngelOne token master: {e}")

    async def get_instrument_info(self, contract):
        """Resolves the token and tradingsymbol for a given contract."""
        await self._load_token_map()
        if not self._token_map:
            return None

        name = contract.name.upper()
        expiry = contract.expiry.date() if isinstance(contract.expiry, datetime.datetime) else contract.expiry
        strike = float(contract.strike_price)
        opt_type = contract.instrument_type.upper()

        key = (name, expiry, strike, opt_type)
        info = self._token_map.get(key)

        if not info:
            logger.warning(f"AngelOne: No instrument info found for {key}")
            # Try fuzzy match if exact expiry fails (some brokers have slightly different expiry dates in master)
            for k, v in self._token_map.items():
                if k[0] == name and abs((k[1] - expiry).days) <= 1 and k[2] == strike and k[3] == opt_type:
                    logger.info(f"AngelOne: Found fuzzy match for {key} -> {k}")
                    return v
        return info

    async def handle_entry_signal(self, **kwargs):
        if not self.smart_api:
            logger.error(f"AngelOne client not initialized for '{self.instance_name}'.")
            return

        contract = kwargs.get('contract')
        instrument_name = kwargs.get('instrument_name')
        direction = kwargs.get('direction')

        if not all([contract, instrument_name, direction]):
            logger.error("AngelOne: handle_entry_signal missing required arguments.")
            return

        broker_base_qty = self.config_manager.get_int(self.instance_name, 'quantity', 1)
        instrument_lot_size = contract.lot_size
        final_qty = broker_base_qty * instrument_lot_size

        try:
            info = await self.get_instrument_info(contract)
            if not info:
                logger.error(f"AngelOne: Could not resolve token for {contract.instrument_key}")
                return None

            order_id = None
            if self.paper_trade:
                logger.info(f"--- ANGELONE [PAPER TRADE] ENTRY ({direction}) ---")
                logger.info(f"  Instrument: {info['tradingsymbol']} | Token: {info['token']}")
                logger.info(f"  Quantity: {final_qty} | Price: {kwargs.get('ltp', 0)}")
                order_id = "PAPER_ANGELONE_ORDER"
            else:
                order_id = self.place_order(info, "BUY", final_qty)

            if order_id:
                instrument_symbol = info['tradingsymbol']
                if not self.paper_trade:
                    logger.info(f"Successfully placed AngelOne BUY ({direction}) order for {instrument_symbol}, ID: {order_id}")

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
            logger.error(f"Exception placing AngelOne entry order: {e}", exc_info=True)
            return None

    async def handle_close_signal(self, **kwargs):
        if not self.smart_api:
            logger.error(f"AngelOne client not initialized for '{self.instance_name}'.")
            return

        side = kwargs.get('side')
        instrument_name = kwargs.get('instrument_name')

        contract = kwargs.get('contract')
        position = self.state_manager.get_position(side)
        if not contract and position:
            contract = position.get('contract')

        if not contract:
            logger.warning(f"No active {side} position found on AngelOne.")
            return

        broker_base_qty = self.config_manager.get_int(self.instance_name, 'quantity', 1)
        final_qty = broker_base_qty * contract.lot_size

        try:
            info = await self.get_instrument_info(contract)
            if not info:
                logger.error(f"AngelOne: Could not resolve token for exit of {contract.instrument_key}")
                return

            order_id = None
            if self.paper_trade:
                logger.info(f"--- ANGELONE [PAPER TRADE] EXIT ({side}) ---")
                logger.info(f"  Instrument: {info['tradingsymbol']}")
                logger.info(f"  Quantity: {final_qty} | Price: {kwargs.get('ltp', 0)}")
                order_id = "PAPER_ANGELONE_EXIT"
            else:
                order_id = self.place_order(info, "SELL", final_qty)

            if order_id:
                if not self.paper_trade:
                    logger.info(f"Successfully placed AngelOne SELL order to close {side} position, ID: {order_id}")

                await event_bus.publish('TRADE_CLOSED', {
                    'user_id': self.user_id,
                    'instrument_name': instrument_name,
                    'direction': side
                })

                price = kwargs.get('ltp', 0)
                reason = kwargs.get('reason', 'UNKNOWN')
                entry_price = position.get('entry_price', 0) if position else 0
                pnl = (price - entry_price) if entry_price > 0 else 0

                instrument_symbol = info['tradingsymbol']
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
            logger.error(f"Exception closing AngelOne position: {e}", exc_info=True)

    def place_order(self, info, transaction_type, quantity):
        try:
            order_params = {
                "variety": "NORMAL",
                "tradingsymbol": info['tradingsymbol'],
                "symboltoken": info['token'],
                "transactiontype": transaction_type,
                "exchange": "NFO",
                "ordertype": "MARKET",
                "producttype": "CARRYFORWARD",
                "duration": "DAY",
                "quantity": str(int(quantity))
            }

            order_id = self.smart_api.placeOrder(order_params)
            return order_id
        except Exception as e:
            logger.error(f"Error in AngelOne place_order: {e}", exc_info=True)
            return None

    def get_ltp(self, symbol):
        return 0.0

    def close_all_positions(self):
        pass
