import pandas as pd
from utils.logger import logger

class PatternMatcher:
    def __init__(self, orchestrator, indicator_manager):
        self.orchestrator = orchestrator
        self.indicator_manager = indicator_manager
        self.data_manager = orchestrator.data_manager

    async def get_resampled_history(self, inst_key, timeframe_minutes, count, end_ts):
        # Increased fetch_count to ensure we have enough candles
        fetch_count = count * timeframe_minutes + timeframe_minutes
        ohlc_1m = await self._get_last_n_candles(inst_key, fetch_count, end_ts)
        if ohlc_1m is None or ohlc_1m.empty: return None
        if timeframe_minutes == 1: return ohlc_1m.tail(count)

        resampling_logic = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
        if 'volume' in ohlc_1m.columns: resampling_logic['volume'] = 'sum'

        resampled = ohlc_1m.resample(f"{timeframe_minutes}min", closed='left', label='left').agg(resampling_logic).dropna()
        if not resampled.empty:
            last_bucket_start = resampled.index[-1]
            actual_count = len(ohlc_1m[ohlc_1m.index >= last_bucket_start])
            if actual_count < timeframe_minutes:
                resampled = resampled.iloc[:-1]
        return resampled.tail(count)

    async def _get_last_n_candles(self, inst_key, count, end_ts):
        if self.orchestrator.is_backtest:
            ohlc_1m = await self.data_manager.get_historical_ohlc(inst_key, 1, end_ts + pd.Timedelta(minutes=1), for_full_day=True)
        else:
            ohlc_1m = self.orchestrator.entry_aggregator.get_historical_ohlc(inst_key)
            local_relevant = ohlc_1m[ohlc_1m.index <= end_ts] if ohlc_1m is not None and not ohlc_1m.empty else None
            if local_relevant is None or len(local_relevant) < count:
                ohlc_1m = await self.data_manager.get_historical_ohlc(inst_key, 1, end_ts + pd.Timedelta(minutes=1), for_full_day=True)

        if ohlc_1m is None or ohlc_1m.empty: return None
        return ohlc_1m[ohlc_1m.index <= end_ts].tail(count)

    def identify_crossover(self, ce_hist, pe_hist):
        if ce_hist is None or pe_hist is None or ce_hist.empty or pe_hist.empty: return []
        common = ce_hist.index.intersection(pe_hist.index)
        return ['CE' if ce_hist.loc[ts, 'close'] > pe_hist.loc[ts, 'close'] else 'PE' for ts in common]
