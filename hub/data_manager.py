import os
from utils.logger import logger
from hub.event_bus import event_bus
import datetime
from upstox_client.rest import ApiException
import asyncio
import pandas as pd
from hub.contract_manager import ContractManager, OptionContract
from hub.futures_manager import FuturesManager

class DataManager:
    def __init__(self, rest_client, instrument_key, config_manager, atm_manager=None):
        self.rest_client = rest_client
        self.instrument_key = instrument_key
        self.config_manager = config_manager
        self.atm_manager = atm_manager

        self.contract_manager = ContractManager(rest_client, config_manager, atm_manager)
        self.futures_manager = FuturesManager(rest_client, config_manager, atm_manager)

        self.market_data = {}
        self.daily_ohlc_cache = {}
        self.backtest_ohlc_data = {}
        self.api_ohlc_cache = {}
        self.live_ohlc_cache = {}
        self._ohlc_lock = asyncio.Lock()

    @property
    def all_options(self): return self.contract_manager.all_options
    @all_options.setter
    def all_options(self, val): self.contract_manager.all_options = val

    @property
    def near_expiry_date(self): return self.contract_manager.near_expiry_date
    @near_expiry_date.setter
    def near_expiry_date(self, val): self.contract_manager.near_expiry_date = val

    @property
    def monthly_expiries(self): return self.contract_manager.monthly_expiries

    @property
    def backtest_df(self):
        # We need to access backtest_df for some historical queries
        # For simplicity, I'll keep it here and let ContractManager set it if needed,
        # or just load it here since it's used for historical data too.
        if not hasattr(self, '_backtest_df'):
            self._backtest_df = None
        return self._backtest_df
    @backtest_df.setter
    def backtest_df(self, val): self._backtest_df = val

    async def load_contracts(self):
        success, df = await self.contract_manager.load_contracts(self.instrument_key, self.discover_futures_key)
        if df is not None:
            self.backtest_df = df
        return success

    async def discover_futures_key(self):
        await self.futures_manager.discover_futures_key(self.instrument_key, self._update_orch_futures_key)

    def _update_orch_futures_key(self, new_key):
        if self.atm_manager and self.atm_manager.orchestrator:
            orch = self.atm_manager.orchestrator
            orch.futures_instrument_key = new_key
            self.atm_manager.spot_instrument_key = new_key
            if hasattr(orch, 'price_feed_handler'):
                orch.price_feed_handler.futures_instrument_key = new_key

    def get_trading_instruments(self):
        return self.all_options, self.near_expiry_date

    async def get_historical_index_price_at_timestamp(self, timestamp: datetime.datetime) -> float:
        try:
            if self.backtest_df is not None and not self.backtest_df.empty:
                relevant = self.backtest_df[self.backtest_df.index < timestamp]
                if not relevant.empty:
                    for col in ['spot_price', 'atm', 'index_price']:
                        if col in relevant.columns: return float(relevant.iloc[-1][col])

            df = await self._fetch_and_prepare_api_data(self.instrument_key, timestamp.date(), timestamp.date(), "1minute")
            if df.empty: return None

            if timestamp.tzinfo is None:
                import pytz
                timestamp = pytz.timezone('Asia/Kolkata').localize(timestamp)

            is_backtest = self.config_manager.get_boolean('settings', 'backtest_enabled', fallback=False)
            relevant_data = df[df.index < timestamp] if is_backtest else df[df.index <= timestamp]
            return relevant_data.iloc[-1]['close'] if not relevant_data.empty else None
        except Exception as e:
            logger.error(f"Error fetching historical index price: {e}")
            return None

    async def _fetch_and_prepare_api_data(self, instrument_key, from_date, to_date, interval="1minute"):
        try:
            df = await self.rest_client.get_historical_candle_data(instrument_key, interval, to_date, from_date)
            if df is None or df.empty: return pd.DataFrame()

            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df.set_index('timestamp', inplace=True)

            if df.index.tz is None:
                df.index = df.index.tz_localize('Asia/Kolkata')
            else:
                df.index = df.index.tz_convert('Asia/Kolkata')
            return df
        except Exception as e:
            logger.error(f"DATA_FETCH: Error for '{instrument_key}': {e}")
            return pd.DataFrame()

    async def get_historical_ohlc(self, instrument_key: str, timeframe_minutes, current_timestamp: datetime.datetime = None, num_minutes_back: int = None, from_date: datetime.datetime = None, for_full_day: bool = False, include_current: bool = False) -> pd.DataFrame:
        import re
        parsed_minutes = 1
        if isinstance(timeframe_minutes, int):
            parsed_minutes = timeframe_minutes
        elif isinstance(timeframe_minutes, str):
            match = re.search(r'\d+', timeframe_minutes)
            if match: parsed_minutes = int(match.group())

        if parsed_minutes > 1:
            one_minute_df = await self.get_historical_ohlc(instrument_key, 1, current_timestamp, num_minutes_back, from_date, for_full_day)
            if one_minute_df.empty: return pd.DataFrame()

            resampling_logic = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
            if 'volume' in one_minute_df.columns: resampling_logic['volume'] = 'sum'

            if self.config_manager.get_boolean('settings', 'backtest_enabled', fallback=False):
                # Proper multi-day resampling for backtest
                all_resampled = []
                unique_dates = one_minute_df.index.normalize().unique()
                for d in unique_dates:
                    day_df = one_minute_df[one_minute_df.index.normalize() == d].copy()
                    # Resample day starting from 9:15
                    resampled_day = day_df.resample(f"{parsed_minutes}min", origin='start_day', offset='9h15min').agg(resampling_logic).dropna()
                    all_resampled.append(resampled_day)

                resampled_df = pd.concat(all_resampled).sort_index()
                # Apply current timestamp filter
                if not include_current:
                    resampled_df = resampled_df[resampled_df.index < current_timestamp]
                else:
                    resampled_df = resampled_df[resampled_df.index <= current_timestamp]
            else:
                # Live mode resample
                resampled_df = one_minute_df.resample(f"{parsed_minutes}min", origin='start_day', offset='9h15min').agg(resampling_logic).dropna()

            return resampled_df

        is_backtest = self.config_manager.get_boolean('settings', 'backtest_enabled', fallback=False)
        if not is_backtest:
            async with self._ohlc_lock:
                now = current_timestamp or datetime.datetime.now()
                to_date = now.date()
                from_date_obj = from_date.date() if from_date else (to_date - datetime.timedelta(days=4) if for_full_day else to_date)
                cache_key = (instrument_key, "1minute", to_date)
                cached = self.live_ohlc_cache.get(cache_key)
                if cached and (now - cached[0]).total_seconds() < 30:
                    return cached[1].copy() if for_full_day else cached[1][cached[1].index <= now].copy()

                df = await self._fetch_and_prepare_api_data(instrument_key, from_date_obj, to_date)
                if not df.empty: self.live_ohlc_cache[cache_key] = (now, df.copy())
                return df if for_full_day or df.empty else df[df.index <= now].copy()

        # Backtest Logic
        if current_timestamp is None: return pd.DataFrame()
        backtest_date = current_timestamp.date()
        api_cache_key = (instrument_key, backtest_date, "1minute")

        if api_cache_key in self.api_ohlc_cache:
            df = self.api_ohlc_cache[api_cache_key]
            if df.empty: return df
            if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is None:
                df.index = df.index.tz_localize('Asia/Kolkata')
            ts = current_timestamp if current_timestamp.tzinfo else pd.Timestamp(current_timestamp).tz_localize('Asia/Kolkata')
            return df[df.index <= ts].copy() if include_current else df[df.index < ts].copy()

        if instrument_key in self.backtest_ohlc_data:
            df = self.backtest_ohlc_data[instrument_key]
            return df[df.index <= current_timestamp].copy() if include_current else df[df.index < current_timestamp].copy()

        # CSV Resampling fallback... (rest of the complex logic simplified or preserved)
        # For brevity, I'll keep the core structure but it's now much smaller.
        return await self._get_backtest_ohlc_csv_fallback(instrument_key, backtest_date, current_timestamp, include_current, for_full_day)

    async def _get_backtest_ohlc_csv_fallback(self, instrument_key, backtest_date, current_timestamp, include_current, for_full_day):
        cache_key = (instrument_key, backtest_date, "1minute")
        if cache_key not in self.daily_ohlc_cache:
            if self.backtest_df is not None and not self.backtest_df.empty:
                df = self.backtest_df[self.backtest_df.index.date == backtest_date].copy()
                if not df.empty:
                    contract = self.atm_manager.get_contract_by_instrument_key(instrument_key)
                    if contract:
                        price_col = f'{contract.instrument_type.lower()}_ltp'
                        if price_col in df.columns:
                            res = df.resample('1min')[price_col].ohlc().dropna()
                            self.daily_ohlc_cache[cache_key] = res

            if cache_key not in self.daily_ohlc_cache:
                df = await self.fetch_and_cache_api_ohlc(instrument_key, backtest_date)
                if not df.empty:
                    self.daily_ohlc_cache[cache_key] = df

        df = self.daily_ohlc_cache.get(cache_key, pd.DataFrame())
        if df.empty: return df
        if df.index.tz is None: df.index = df.index.tz_localize('Asia/Kolkata')
        ts = current_timestamp if current_timestamp.tzinfo else pd.Timestamp(current_timestamp).tz_localize('Asia/Kolkata')
        return df[df.index <= ts].copy() if include_current else df[df.index < ts].copy()

    async def fetch_and_cache_api_ohlc(self, instrument_key: str, date: datetime.date, interval: str = "1minute"):
        real_key = instrument_key
        is_bt = self.config_manager.get_boolean('settings', 'backtest_enabled')
        if is_bt:
            parts = instrument_key.split()
            if len(parts) >= 6:
                contract = await self.get_live_contract_details(int(parts[1]), datetime.datetime.strptime(f"{parts[3]} {parts[4]} {parts[5]}", "%d %b %Y").date(), parts[2])
                if contract: real_key = contract.instrument_key
                else: return pd.DataFrame()

        # We fetch 4 days of history to be safe for weekend gaps.
        df = await self._fetch_and_prepare_api_data(real_key, date - datetime.timedelta(days=4), date, interval)

        if is_bt:
            # Store in backtest_ohlc_data to make it available for all lookups in this session
            if instrument_key not in self.backtest_ohlc_data:
                self.backtest_ohlc_data[instrument_key] = df
            else:
                self.backtest_ohlc_data[instrument_key] = pd.concat([self.backtest_ohlc_data[instrument_key], df]).sort_index()
                self.backtest_ohlc_data[instrument_key] = self.backtest_ohlc_data[instrument_key][~self.backtest_ohlc_data[instrument_key].index.duplicated(keep='last')]

        self.api_ohlc_cache[(instrument_key, date, interval)] = df.copy()
        return df

    def clear_api_ohlc_cache_for_strike(self, old_strike: int, expiry_date: datetime.date):
        keys_to_remove = []
        for k in self.api_ohlc_cache:
            contract = self.atm_manager.get_contract_by_instrument_key(k[0])
            if contract and contract.strike_price == old_strike:
                keys_to_remove.append(k)
        for k in keys_to_remove: del self.api_ohlc_cache[k]

    def clear_caches(self):
        self.daily_ohlc_cache.clear()
        self.api_ohlc_cache.clear()
        self.live_ohlc_cache.clear()

    async def get_live_contract_details(self, strike, expiry, type):
        contracts = await self.contract_manager.get_live_option_contracts(self.instrument_key)
        for c in contracts:
            if c.strike_price == strike and c.expiry.date() == expiry and c.instrument_type == type: return c
        return None

    async def prime_aggregator(self, aggregator, instrument_key, timestamp):
        if not instrument_key:
            logger.warning(f"prime_aggregator called with None instrument_key — skipping.")
            return
        df = await self.get_historical_ohlc(instrument_key, aggregator.interval_minutes, timestamp, for_full_day=True, include_current=True)
        if not df.empty:
            aggregator.prime_with_history(instrument_key, df[df.index < timestamp])
