import pandas as pd
from utils.logger import logger
import os

class BacktestDataManager:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.data_manager = orchestrator.data_manager
        self._index_ohlc_cache = None
        self._futures_ohlc_cache = None
        self._atp_cache = {} # instrument_key -> {timestamp: atp}

    async def pre_fetch_underlying_data(self, date_str):
        idx_key = self.orchestrator.index_instrument_key
        fut_key = self.orchestrator.futures_instrument_key
        from_date = pd.to_datetime(date_str) - pd.Timedelta(days=4)
        from_date_str = from_date.strftime('%Y-%m-%d')

        if idx_key:
            self._index_ohlc_cache = await self.data_manager._fetch_and_prepare_api_data(idx_key, from_date_str, date_str, '1minute')
        if fut_key:
            self._futures_ohlc_cache = await self.data_manager._fetch_and_prepare_api_data(fut_key, from_date_str, date_str, '1minute')

        # Load ATP data from separate file if it exists
        await self._load_external_atp_data(date_str)

    async def _load_external_atp_data(self, date_str):
        """Loads ATP data from atp_data_{instrument}_{date}.csv if it exists."""
        inst_name = self.orchestrator.instrument_name
        atp_filename = f"atp_data_{inst_name}_{date_str}.csv"
        atp_path = os.path.join(os.getcwd(), atp_filename)

        if os.path.exists(atp_path):
            try:
                df = pd.read_csv(atp_path, parse_dates=['minute_ts'])
                if df.empty: return

                import pytz
                kolkata = pytz.timezone('Asia/Kolkata')
                if df['minute_ts'].dt.tz is None:
                    df['minute_ts'] = df['minute_ts'].dt.tz_localize(kolkata)
                else:
                    df['minute_ts'] = df['minute_ts'].dt.tz_convert(kolkata)

                for inst_key, group in df.groupby('instrument_key'):
                    if inst_key not in self._atp_cache:
                        self._atp_cache[inst_key] = {}

                    for _, row in group.iterrows():
                        self._atp_cache[inst_key][row['minute_ts']] = row['atp']

                logger.info(f"V2: Loaded ATP data for {len(self._atp_cache)} instruments from {atp_filename}")
            except Exception as e:
                logger.error(f"Failed to load external ATP data from {atp_filename}: {e}")

    def get_atp(self, instrument_key, timestamp):
        """Returns the ATP for a given instrument and timestamp from the cache."""
        if not instrument_key or instrument_key not in self._atp_cache:
            return None

        # Exact match for the minute
        bucket_ts = timestamp.replace(second=0, microsecond=0)
        if bucket_ts in self._atp_cache[instrument_key]:
            return self._atp_cache[instrument_key][bucket_ts]

        # Fallback to the most recent known ATP
        relevant = {ts: val for ts, val in self._atp_cache[instrument_key].items() if ts <= timestamp}
        if relevant:
            return relevant[max(relevant.keys())]

        return None

    def get_index_price(self, timestamp):
        if self._index_ohlc_cache is not None:
            relevant = self._index_ohlc_cache[self._index_ohlc_cache.index <= timestamp]
            if not relevant.empty: return relevant.iloc[-1]['close']
            if not self._index_ohlc_cache.empty: return self._index_ohlc_cache.iloc[0]['open']
        return None

    def get_futures_price(self, timestamp):
        if self._futures_ohlc_cache is not None:
            relevant = self._futures_ohlc_cache[self._futures_ohlc_cache.index <= timestamp]
            if not relevant.empty: return relevant.iloc[-1]['close']
            if not self._futures_ohlc_cache.empty: return self._futures_ohlc_cache.iloc[0]['open']
        return None
