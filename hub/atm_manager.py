import asyncio
import datetime
from utils.logger import logger
from hub.event_bus import event_bus
from hub.expiry_manager import ExpiryManager
from hub.subscription_manager import SubscriptionManager

class AtmManager:
    def __init__(self, config_manager, websocket_manager, state_manager, rest_client=None, instrument_name="NIFTY", orchestrator=None):
        self.config = config_manager
        self.websocket = websocket_manager
        self.state_manager = state_manager
        self.orchestrator = orchestrator
        self.rest_client = rest_client
        self.instrument_name = instrument_name
        self.data_manager = None

        self.current_spot_price = 0
        self.strikes = {}
        self.contracts = {}
        self.all_contracts = []
        self.contract_lookup = {}
        self.mode_expiries = {}

        self.signal_expiry_date = None
        self.trade_expiry_date = None
        self.target_expiry = None
        self.near_expiry_date = None
        self.monthly_expiries = []

        event_bus.subscribe('SPOT_PRICE_UPDATE', self._handle_spot_price_update)
        self.initial_spot_price_received = asyncio.Event()
        self.initial_data_received = asyncio.Event()
        self._is_ready = False
        self.re_strike_in_progress = False

        self.expiry_manager = None
        self.sub_manager = None

    def set_ready(self):
        self.expiry_manager = ExpiryManager(self.all_contracts, self.near_expiry_date, self.monthly_expiries)
        self.sub_manager = SubscriptionManager(self.websocket, self.config, self.data_manager, self.orchestrator)
        self._is_ready = True
        logger.debug("AtmManager initialized with Expiry and Subscription managers.")

    async def _handle_spot_price_update(self, data):
        if isinstance(data, dict):
            if data.get('instrument') != self.instrument_name: return
            spot_price = data.get('ltp')
        else: spot_price = data

        self.state_manager.spot_price = spot_price
        if not self.initial_spot_price_received.is_set(): self.initial_spot_price_received.set()
        if not self._is_ready: return

        strike_interval = self.config.get_int(self.instrument_name, 'strike_interval')
        current_atm = self.strikes.get('atm')

        if current_atm is None or self._is_atm_breached(spot_price, current_atm, strike_interval):
            await self.update_strikes_and_subscribe(spot_price)

    def _is_atm_breached(self, spot_price, current_atm, strike_interval):
        buffer = self.config.get_int('settings', 'atm_breach_buffer_points', 0)
        half = strike_interval / 2
        return not (current_atm - half - buffer <= spot_price < current_atm + half + buffer)

    async def update_strikes_and_subscribe(self, spot_price, is_initial_setup=False):
        if self.re_strike_in_progress: return
        self.current_spot_price = spot_price
        strike_interval = self.config.get_int(self.instrument_name, 'strike_interval')
        new_atm = int(round(spot_price / strike_interval) * strike_interval)

        if is_initial_setup or new_atm != self.strikes.get('atm'):
            try:
                self.re_strike_in_progress = True
                protected = self._get_protected_keys()
                keys = await self.sub_manager.perform_resubscription(new_atm, protected, self.signal_expiry_date, self.contracts, self.find_contracts_for_strike)
                self.strikes['atm'] = new_atm
                if is_initial_setup: self.state_manager.initial_instrument_keys = keys
            finally: self.re_strike_in_progress = False

    def _get_protected_keys(self):
        protected = set()
        if self.orchestrator:
            for session in self.orchestrator.user_sessions.values():
                for pos in [session.state_manager.call_position, session.state_manager.put_position]:
                    if pos:
                        if 'instrument_key' in pos: protected.add(pos['instrument_key'])
                        sl_strike = pos.get('s1_monitoring_strike')
                        if sl_strike:
                            for side in ['CALL', 'PUT']:
                                key = self.find_instrument_key_by_strike(sl_strike, side, self.signal_expiry_date)
                                if key: protected.add(key)

                mon_data = session.state_manager.dual_sr_monitoring_data
                if mon_data:
                    for side in ['ce_data', 'pe_data']:
                        side_d = mon_data.get(side)
                        if side_d:
                            k = self.find_instrument_key_by_strike(side_d.get('strike_price'), 'CALL' if side == 'ce_data' else 'PUT', self.signal_expiry_date)
                            if k: protected.add(k)

        for strike_info in self.contracts.values():
            for side in ['CE', 'PE']:
                k = strike_info.get(side, {}).get('key')
                if k: protected.add(k)
        return protected

    def find_contracts_for_strike(self, strike_price, expiry_date=None):
        target = expiry_date.date() if hasattr(expiry_date, 'date') else expiry_date
        if not target: return None, None
        expiry_strikes = self.contract_lookup.get(target, {})
        data = expiry_strikes.get(float(strike_price), {})
        return data.get('CE'), data.get('PE')

    def find_instrument_key_by_strike(self, strike_price, option_type, expiry_date):
        api_type = 'CE' if option_type.upper() in ['CALL', 'CE'] else 'PE'
        target = expiry_date.date() if hasattr(expiry_date, 'date') else expiry_date
        if not target: return None
        expiry_strikes = self.contract_lookup.get(target, {})
        contract = expiry_strikes.get(float(strike_price), {}).get(api_type)
        return contract.instrument_key if contract else None

    def get_contract_by_instrument_key(self, instrument_key):
        for expiry_strikes in self.contract_lookup.values():
            for strike_data in expiry_strikes.values():
                for contract in strike_data.values():
                    if contract.instrument_key == instrument_key: return contract
        return None

    def get_expiry_by_mode(self, mode, expiry_type='signal'):
        if mode and mode.lower() in self.mode_expiries:
            expiry = self.mode_expiries[mode.lower()].get(expiry_type)
            if expiry: return expiry
        return self.signal_expiry_date if expiry_type == 'signal' else self.trade_expiry_date

    def _determine_expiries(self, context_date=None):
        if not self.expiry_manager:
            if self.all_contracts:
                self.set_ready()
            else:
                logger.error(f"[{self.instrument_name}] _determine_expiries called but expiry_manager and contracts are missing.")
                return
        self._determine_default_expiries(context_date)
        if self.orchestrator and hasattr(self.orchestrator, 'json_config'):
            for mode in ['buy', 'sell']:
                s_m = self.orchestrator.json_config.get_value(f"{self.instrument_name}.{mode}.signal_expiry")
                t_m = self.orchestrator.json_config.get_value(f"{self.instrument_name}.{mode}.trade_expiry")
                if s_m or t_m:
                    s_d = self.expiry_manager.calculate_expiry_date(s_m or 'MONTHLY', context_date)
                    t_d = self.expiry_manager.get_trade_expiry_date(context_date, mode=t_m or 'MONTHLY')
                    self.mode_expiries[mode] = {'signal': s_d, 'trade': t_d}

    def _determine_default_expiries(self, context_date):
        mode = self.config.get('settings', 'signal_expiry', fallback='CURRENT_WEEK').upper()
        self.signal_expiry_date = self.expiry_manager.calculate_expiry_date(mode, context_date)
        self.trade_expiry_date = self.expiry_manager.get_trade_expiry_date(context_date, mode=self.config.get('settings', 'trade_expiry_type', fallback='WEEKLY'))
        self.target_expiry = self.trade_expiry_date

    def _build_contract_lookup_table(self):
        self.contract_lookup.clear()
        for contract in self.all_contracts:
            exp = contract.expiry.date()
            strike = float(contract.strike_price)
            ctype = contract.instrument_type
            if exp not in self.contract_lookup: self.contract_lookup[exp] = {}
            if strike not in self.contract_lookup[exp]: self.contract_lookup[exp][strike] = {}
            self.contract_lookup[exp][strike][ctype] = contract
