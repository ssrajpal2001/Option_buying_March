import os
import datetime
import pandas as pd
from utils.logger import logger

class OptionContract:
    """A simple class to hold option contract data and parse the API response."""
    def __init__(self, data):
        self.instrument_key = data.get('instrument_key')
        self.exchange = data.get('exchange')
        self.name = data.get('name')
        self.instrument_type = data.get('instrument_type')
        self.strike_price = data.get('strike_price')
        self.lot_size = data.get('lot_size')
        self.expiry = self._parse_expiry(data.get('expiry'))

    def _parse_expiry(self, expiry_str):
        if not expiry_str:
            return None
        return datetime.datetime.strptime(expiry_str, '%Y-%m-%d')

class ContractManager:
    def __init__(self, rest_client, config_manager, atm_manager=None):
        self.rest_client = rest_client
        self.config_manager = config_manager
        self.atm_manager = atm_manager
        self.all_options = []
        self.near_expiry_date = None
        self.monthly_expiries = []

    def _get_project_root(self):
        return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    async def load_contracts(self, instrument_key, discover_futures_func):
        is_backtest = self.config_manager.get_boolean('settings', 'backtest_enabled', fallback=False)
        has_api = self.rest_client and not hasattr(self.rest_client, 'called')

        if has_api:
            await discover_futures_func()

        if is_backtest:
            res = await self._load_contracts_from_csv()
            csv_success = res is not None
            if csv_success and self.all_options:
                return True, res
            if has_api:
                return await self._load_contracts_from_api(instrument_key), None
            return len(self.all_options) > 0, None
        else:
            return await self._load_contracts_from_api(instrument_key), None

    async def _load_contracts_from_csv(self):
        try:
            csv_filename = self.config_manager.get('settings', 'backtest_csv_path', fallback='tick_data_log.csv')
            csv_path = os.path.join(self._get_project_root(), csv_filename)
            if not os.path.isfile(csv_path): return None

            df = pd.read_csv(csv_path, parse_dates=['timestamp'], on_bad_lines='warn', engine='python')
            if df.empty: return None

            df['timestamp'] = df['timestamp'].dt.tz_localize('Asia/Kolkata')
            df.set_index('timestamp', inplace=True)
            df.sort_index(inplace=True)
            fallback_date = df.index[0] if not df.empty else datetime.datetime.now()

            # Hybrid Model logic simplified here
            logger.debug("Backtest Hybrid Model: Fetching live option chain to ensure all expiries are available.")
            live_contracts = await self.get_live_option_contracts(self.config_manager.get_instrument_key_by_symbol(self.config_manager.get('settings', 'instrument_to_trade')))
            if live_contracts:
                self.all_options = live_contracts

            if not self.all_options: return None
            self._determine_near_expiry_date()
            self._identify_monthly_expiries()
            return df
        except Exception as e:
            logger.error(f"Failed to load contracts from CSV: {e}", exc_info=True)
            return None

    async def _supplement_expiry_day_contracts(self, instrument_key):
        """On NSE expiry day, Upstox excludes today's expiring contracts from the standard
        /option/contract endpoint.  This method detects that and supplements all_options by
        fetching the /expired-instruments/option/contract endpoint."""
        today = datetime.date.today()
        loaded_expiries = {c.expiry.date() for c in self.all_options if c.instrument_type in ['CE', 'PE']}
        if today in loaded_expiries:
            return  # today's contracts already present — nothing to do

        logger.info(f"ContractManager: Expiry day detected ({today}) — today's contracts absent from standard endpoint. Fetching from expired-instruments API...")
        if not hasattr(self.rest_client, 'get_expiring_option_contracts'):
            logger.warning("ContractManager: rest_client does not support get_expiring_option_contracts — cannot supplement.")
            return

        expiring_raw = await self.rest_client.get_expiring_option_contracts(instrument_key, today)
        if not expiring_raw:
            logger.warning("ContractManager: expired-instruments API returned no contracts. Bot may be unable to trade today's expiry.")
            return

        today_contracts = [OptionContract(c) for c in expiring_raw
                           if c.get('expiry') == today.strftime('%Y-%m-%d')]
        if today_contracts:
            self.all_options.extend(today_contracts)
            logger.info(f"ContractManager: Supplemented with {len(today_contracts)} expiry-day contracts for {today}. Total option contracts: {len(self.all_options)}")
        else:
            logger.warning(f"ContractManager: expired-instruments API returned data but none matched today ({today}). Bot may be unable to trade today's expiry.")

    async def _load_contracts_from_api(self, instrument_key):
        try:
            if not instrument_key: return False
            raw_contracts = await self.rest_client.get_option_contracts(instrument_key)
            if not raw_contracts:
                self.all_options = []
                return True
            self.all_options = [OptionContract(c) for c in raw_contracts]
            await self._supplement_expiry_day_contracts(instrument_key)
            self._determine_near_expiry_date()
            self._identify_monthly_expiries()
            return True
        except Exception as e:
            logger.error(f"Error loading contracts from API: {e}", exc_info=True)
            return False

    def _determine_near_expiry_date(self):
        today = datetime.date.today()
        unique_expiries = sorted(list(set(c.expiry.date() for c in self.all_options if c.instrument_type in ['CE', 'PE'])))
        if not unique_expiries: return

        trade_expiry_type = self.config_manager.get('settings', 'trade_expiry_type', fallback='WEEKLY').upper()
        if trade_expiry_type == 'WEEKLY':
            # Use the first available expiry >= today from the API-provided list.
            # This works regardless of which weekday NSE chooses as expiry day.
            for expiry_date in unique_expiries:
                if expiry_date >= today:
                    self.near_expiry_date = datetime.datetime.combine(expiry_date, datetime.time.min)
                    logger.info(f"ContractManager: Near weekly expiry resolved to {expiry_date} (day={expiry_date.strftime('%A')})")
                    return
        elif trade_expiry_type == 'MONTHLY':
            for expiry_date in self.monthly_expiries:
                if expiry_date >= today:
                    self.near_expiry_date = datetime.datetime.combine(expiry_date, datetime.time.min)
                    return

        if not self.near_expiry_date and unique_expiries:
            closest = min((d for d in unique_expiries if d >= today), default=unique_expiries[-1])
            self.near_expiry_date = datetime.datetime.combine(closest, datetime.time.min)

    def _identify_monthly_expiries(self):
        expiries_by_month = {}
        for contract in self.all_options:
            if contract.instrument_type not in ['CE', 'PE']: continue
            expiry_date = contract.expiry.date()
            month_key = (expiry_date.year, expiry_date.month)
            if month_key not in expiries_by_month or expiry_date > expiries_by_month[month_key]:
                expiries_by_month[month_key] = expiry_date
        self.monthly_expiries = sorted(list(expiries_by_month.values()))

    def get_contract_by_instrument_key(self, instrument_key):
        for c in self.all_options:
            if c.instrument_key == instrument_key: return c
        return None

    def find_instrument_key_by_strike(self, strike_price, option_type, expiry_date):
        api_type = 'CE' if option_type.upper() in ['CALL', 'CE'] else 'PE'
        target_expiry = expiry_date.date() if isinstance(expiry_date, datetime.datetime) else expiry_date

        matches = [c for c in self.all_options if c.instrument_type == api_type and c.expiry.date() == target_expiry]
        if not matches: return None

        # Exact
        exact = next((c for c in matches if float(c.strike_price) == float(strike_price)), None)
        if exact: return exact.instrument_key

        # Closest
        closest = min(matches, key=lambda x: abs(float(x.strike_price) - float(strike_price)))
        if abs(float(closest.strike_price) - float(strike_price)) <= 250:
             return closest.instrument_key
        return None

    async def get_live_option_contracts(self, instrument_key):
        try:
            raw_contracts = await self.rest_client.get_option_contracts(instrument_key)
            return [OptionContract(c) for c in raw_contracts] if raw_contracts else []
        except Exception as e:
            logger.error(f"Error fetching live options: {e}")
            return []
