import pandas as pd
from utils.logger import logger

class BacktestDataManager:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.data_manager = orchestrator.data_manager
        self._index_ohlc_cache = None
        self._futures_ohlc_cache = None

    async def pre_fetch_underlying_data(self, date_str):
        idx_key = self.orchestrator.index_instrument_key
        fut_key = self.orchestrator.futures_instrument_key
        from_date = pd.to_datetime(date_str) - pd.Timedelta(days=4)
        from_date_str = from_date.strftime('%Y-%m-%d')

        if idx_key:
            self._index_ohlc_cache = await self.data_manager._fetch_and_prepare_api_data(idx_key, from_date_str, date_str, '1minute')
        if fut_key:
            self._futures_ohlc_cache = await self.data_manager._fetch_and_prepare_api_data(fut_key, from_date_str, date_str, '1minute')

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
