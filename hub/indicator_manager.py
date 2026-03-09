from utils.logger import logger
from utils.support_resistance import SupportResistanceCalculator
import pandas as pd
import asyncio
import pytz
import datetime as dt
from datetime import datetime, time, timedelta

class IndicatorManager:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.state_manager = orchestrator.state_manager
        self.data_manager = orchestrator.data_manager
        self.config_manager = orchestrator.config_manager
        self.atm_manager = orchestrator.atm_manager

        self._vwap_state = {}
        self._vwap_slope_cache = {}
        self._sr_cache = {}
        self._r1_profit_cache = {}
        self._index_915_range = {} # (index_key, date) -> (high, low)

    async def get_robust_ohlc(self, inst_key, timeframe_minutes, timestamp, include_current=True, for_full_day=True):
        """
        Returns OHLC data for the given instrument and timeframe.
        Attempts to use aggregators and local caches before hitting REST API.
        """
        import re
        parsed_minutes = 1
        if isinstance(timeframe_minutes, int):
            parsed_minutes = timeframe_minutes
        elif isinstance(timeframe_minutes, str):
            match = re.search(r'\d+', timeframe_minutes)
            if match: parsed_minutes = int(match.group())

        if self.orchestrator.is_backtest:
            return await self.data_manager.get_historical_ohlc(
                instrument_key=inst_key,
                timeframe_minutes=parsed_minutes,
                current_timestamp=timestamp,
                for_full_day=for_full_day,
                include_current=include_current
            )

        aggregator = None
        if parsed_minutes == self.orchestrator.entry_aggregator.interval_minutes:
            aggregator = self.orchestrator.entry_aggregator
        elif parsed_minutes == self.orchestrator.one_min_aggregator.interval_minutes:
            aggregator = self.orchestrator.one_min_aggregator
        elif parsed_minutes == self.orchestrator.five_min_aggregator.interval_minutes:
            aggregator = self.orchestrator.five_min_aggregator

        ohlc = None
        if aggregator:
            ohlc = aggregator.get_historical_ohlc(inst_key)
            if aggregator.interval_minutes == 1:
                current = aggregator.get_all_current_ohlc().get(inst_key)
                if current:
                    curr_df = pd.DataFrame([current]).set_index('timestamp')
                    if curr_df.index.tz is None:
                        curr_df.index = curr_df.index.tz_localize(pytz.timezone('Asia/Kolkata'))
                    if ohlc is None or ohlc.empty:
                        ohlc = curr_df
                    else:
                        ohlc = pd.concat([ohlc, curr_df])
                        ohlc = ohlc[~ohlc.index.duplicated(keep='last')]
                    ohlc = ohlc.sort_index()

        # [V3 Strategy] "Historical Stitching":
        # Always attempt to complement aggregator data with API history to ensure
        # indicators like RSI (14-period) are available immediately for new strikes.
        if not self.orchestrator.is_backtest:
            # Determine how many candles we currently have
            current_len = len(ohlc) if ohlc is not None else 0
            # Target: at least 20 candles (for a 14-period RSI buffer)
            if current_len < 20:
                logger.debug(f"[IndicatorManager] Stitching history for {inst_key} ({current_len} candles in memory)")
                hist_api = await self.data_manager.get_historical_ohlc(
                    instrument_key=inst_key,
                    timeframe_minutes=parsed_minutes,
                    current_timestamp=timestamp,
                    for_full_day=True,
                    include_current=False # Don't duplicate the 'live' running candle
                )
                if hist_api is not None and not hist_api.empty:
                    if ohlc is None or ohlc.empty:
                        ohlc = hist_api
                    else:
                        # Merge API history (older) with Aggregator data (newer)
                        ohlc = pd.concat([hist_api, ohlc])
                        ohlc = ohlc[~ohlc.index.duplicated(keep='last')]
                        ohlc = ohlc.sort_index()

        if (ohlc is None or ohlc.empty) and parsed_minutes > 1:
            one_min_ohlc = self.orchestrator.entry_aggregator.get_historical_ohlc(inst_key)
            if one_min_ohlc is not None and len(one_min_ohlc) >= parsed_minutes:
                resampling_logic = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'}
                if 'volume' in one_min_ohlc.columns: resampling_logic['volume'] = 'sum'
                resample_freq = f"{parsed_minutes}min"
                ohlc = one_min_ohlc.resample(resample_freq).agg(resampling_logic).dropna()

        if ohlc is None or ohlc.empty:
            ohlc = await self.data_manager.get_historical_ohlc(
                instrument_key=inst_key,
                timeframe_minutes=parsed_minutes,
                current_timestamp=timestamp,
                for_full_day=for_full_day,
                include_current=include_current
            )

        if ohlc is not None and not ohlc.empty:
            if ohlc.index.tz is None:
                ohlc.index = ohlc.index.tz_localize('Asia/Kolkata')
            else:
                ohlc.index = ohlc.index.tz_convert('Asia/Kolkata')

        return ohlc

    async def calculate_vwap(self, inst_key, timestamp):
        if not inst_key:
            return None

        # 1. Check ATP history first (allows lookups for previous candles in both live and backtest)
        atp_hist = getattr(self.state_manager, 'atp_history', {}).get(inst_key, {})
        if atp_hist:
            # We want the VWAP (ATP) from the candle that just finalized.
            # If timestamp is 10:30:00, we want the state as of 10:29:59 (the 10:25-10:30 candle's final ATP).
            search_ts = timestamp - timedelta(seconds=1)
            if search_ts.tzinfo is None:
                search_ts = pytz.timezone('Asia/Kolkata').localize(search_ts)

            # Filter for timestamp-like keys and find latest available up to search_ts
            candidates = [ts for ts in atp_hist.keys() if hasattr(ts, 'year') and ts <= search_ts]
            if candidates:
                return float(atp_hist[max(candidates)])

        # 2. Fallback to current live ATP if history is missing and we are looking for 'now'
        if not self.orchestrator.is_backtest:
            now = self.orchestrator._get_timestamp()
            if (now - timestamp).total_seconds() < 60:
                atps = getattr(self.state_manager, 'option_atps', {})
                live_atp = atps.get(inst_key)
                if live_atp and live_atp > 0:
                    return float(live_atp)

        current_day = timestamp.date()
        state_key = (inst_key, current_day)
        state = self._vwap_state.get(state_key)
        last_final_minute = timestamp.replace(second=0, microsecond=0) - pd.Timedelta(minutes=1)

        if not state or state['last_final_minute'] < last_final_minute:
            ohlc_1m = await self.data_manager.get_historical_ohlc(inst_key, 1, timestamp, for_full_day=True)
            if ohlc_1m is not None and not ohlc_1m.empty:
                df = ohlc_1m[(ohlc_1m.index.date == current_day) & (ohlc_1m.index <= last_final_minute)]
                if not df.empty:
                    tp = (df['high'] + df['low'] + df['close']) / 3
                    vol = df['volume']
                    state = {
                        'cum_pv': (tp * vol).sum(),
                        'cum_vol': vol.sum(),
                        'last_final_minute': last_final_minute
                    }
                    self._vwap_state[state_key] = state

        cum_pv = state['cum_pv'] if state else 0.0
        cum_vol = state['cum_vol'] if state else 0.0

        if self.orchestrator.is_backtest:
            all_candles = await self.data_manager.get_historical_ohlc(inst_key, 1, timestamp, for_full_day=True, include_current=True)
            if all_candles is not None and not all_candles.empty:
                current_minute = timestamp.replace(second=0, microsecond=0)
                live_matches = all_candles[all_candles.index == current_minute]
                if not live_matches.empty:
                    live_candle = live_matches.iloc[0]
                    tp = (live_candle['high'] + live_candle['low'] + live_candle['close']) / 3
                    cum_pv += tp * live_candle['volume']
                    cum_vol += live_candle['volume']

        if cum_vol > 0:
            return cum_pv / cum_vol

        # Fallback to latest Close price if no volume data is available (common in backtests)
        ohlc = await self.get_robust_ohlc(inst_key, 1, timestamp, include_current=True)
        if ohlc is not None and not ohlc.empty:
            return float(ohlc.iloc[-1]['close'])

        return None

    async def get_vwap_slope_status(self, inst_key, timestamp, timeframe_minutes, count=1, live_vwap=None):
        if not inst_key:
            return None, None, None, None, 0, 0
        cache_key = (inst_key, timeframe_minutes, count, live_vwap, timestamp.date(), timestamp.hour, timestamp.minute)
        cached = self._vwap_slope_cache.get(cache_key)
        if cached and (timestamp - cached['ts']).total_seconds() < 5.0:
            return cached['val']

        if live_vwap is not None:
            atp_hist = getattr(self.state_manager, 'atp_history', {}).get(inst_key, {})
            if atp_hist:
                current_interval_start = timestamp.replace(
                    minute=(timestamp.minute // timeframe_minutes) * timeframe_minutes,
                    second=0, microsecond=0)
                prev_boundary = current_interval_start - pd.Timedelta(minutes=timeframe_minutes)
                candidates = {ts: v for ts, v in atp_hist.items() if isinstance(ts, type(prev_boundary)) and ts <= prev_boundary}
                if candidates:
                    v0 = candidates[max(candidates.keys())]
                    v1 = live_vwap
                    is_rising = v1 > v0
                    is_falling = v1 < v0
                    cons_r = 1 if is_rising else 0
                    cons_f = 1 if is_falling else 0
                    res = (is_rising, is_falling, v1, v0, cons_r, cons_f)
                    self._vwap_slope_cache[cache_key] = {'ts': timestamp, 'val': res}
                    return res

        ohlc_1m = await self.get_robust_ohlc(inst_key, 1, timestamp)
        if ohlc_1m is None or ohlc_1m.empty:
            return None, None, None, None, 0, 0

        current_interval_start = timestamp.replace(minute=(timestamp.minute // timeframe_minutes) * timeframe_minutes, second=0, microsecond=0)
        t0 = current_interval_start - pd.Timedelta(minutes=timeframe_minutes)
        df = ohlc_1m[ohlc_1m.index.date == timestamp.date()].copy()

        if live_vwap is None and (len(df) < 2 or (ohlc_1m.index.empty or ohlc_1m.index[-1] < timestamp.replace(second=0, microsecond=0))):
            return None, None, None, None, 0, 0
        if live_vwap is not None and df.empty:
            return None, None, None, None, 0, 0

        if not df.empty:
            df = df.sort_index()
            anchor_ts = timestamp.replace(hour=9, minute=15, second=0, microsecond=0)
            boundary_ts = timestamp.replace(second=0, microsecond=0)
            if boundary_ts > anchor_ts:
                expected_range = pd.date_range(start=anchor_ts, end=boundary_ts, freq='1min')
                df_vol = df['volume'] if 'volume' in df.columns else pd.Series(0, index=df.index)
                df = df.reindex(expected_range).ffill()
                df['volume'] = df_vol.reindex(expected_range).fillna(0)

        if not df.empty and 'volume' not in df.columns:
            df['volume'] = 0.0

        def get_vwap_at(ts):
            d = df[df.index <= ts]
            if d.empty: return None
            tp = (d['high'] + d['low'] + d['close']) / 3
            vol = d.get('volume', pd.Series(0, index=d.index))
            return (tp * vol).sum() / vol.sum() if vol.sum() > 0 else tp.mean()

        finalized_vwaps = []
        for i in range(count):
            boundary_ts = t0 - pd.Timedelta(minutes=i * timeframe_minutes)
            val = get_vwap_at(boundary_ts)
            if val is None: break
            finalized_vwaps.append((boundary_ts, val))

        if not finalized_vwaps:
            return False, False, live_vwap, None, 0, 0

        if live_vwap is None:
            live_vwap = get_vwap_at(timestamp)

        last_final_val = finalized_vwaps[0][1]
        is_rising_now = (live_vwap > last_final_val) if live_vwap is not None and last_final_val is not None else False
        is_falling_now = (live_vwap < last_final_val) if live_vwap is not None and last_final_val is not None else False

        cons_rising = 0
        for i in range(len(finalized_vwaps) - 1):
            if finalized_vwaps[i][1] > finalized_vwaps[i+1][1]: cons_rising += 1
            else: break
        cons_falling = 0
        for i in range(len(finalized_vwaps) - 1):
            if finalized_vwaps[i][1] < finalized_vwaps[i+1][1]: cons_falling += 1
            else: break

        res = (is_rising_now and (1 + cons_rising) >= count, is_falling_now and (1 + cons_falling) >= count, live_vwap, last_final_val, (1 + cons_rising) if is_rising_now else 0, (1 + cons_falling) if is_falling_now else 0)
        self._vwap_slope_cache[cache_key] = {'ts': timestamp, 'val': res}
        return res

    async def get_index_915_range(self, index_key, timestamp):
        """Fetches and caches the 9:15 AM candle high/low for the given index."""
        current_date = timestamp.date()
        cache_key = (index_key, current_date)
        if cache_key in self._index_915_range:
            return self._index_915_range[cache_key]

        # Fetch 1m data for the index
        ohlc = await self.data_manager.get_historical_ohlc(index_key, 1, timestamp, for_full_day=True)
        if ohlc is not None and not ohlc.empty:
            day_data = ohlc[ohlc.index.date == current_date]
            target_time = time(9, 15)
            anchor = day_data[day_data.index.time == target_time]
            if not anchor.empty:
                res = (float(anchor.iloc[0]['high']), float(anchor.iloc[0]['low']))
                self._index_915_range[cache_key] = res
                logger.info(f"V2: 9:15 Range Captured for {index_key}: High={res[0]:.2f}, Low={res[1]:.2f}")
                return res

        return None, None

    async def get_sr_status(self, inst_key, timeframe_minutes, timestamp):
        cache_key = (inst_key, timeframe_minutes, timestamp.date(), timestamp.hour, timestamp.minute)
        if cache_key in self._sr_cache:
            return self._sr_cache[cache_key]

        ohlc_1m = await self.get_robust_ohlc(inst_key, 1, timestamp)
        res = await SupportResistanceCalculator.get_sr_status_shared(
            self.data_manager, inst_key, timeframe_minutes, timestamp, ohlc_1m
        )
        self._sr_cache[cache_key] = res
        return res

    async def get_nuanced_barrier(self, inst_key, indicator_type, tf, timestamp):
        s1_v, r1_v, s1_est, r1_est, phase, s1_bh, r1_bl, b_lvl = await self.get_sr_status(inst_key, tf, timestamp)
        is_r1 = (indicator_type == 'r1_high')
        is_tracking = (phase == 'R1_TRACKING' if is_r1 else phase == 'S1_TRACKING')

        if is_tracking:
            current_min_ts = timestamp.replace(second=0, microsecond=0)
            anchor_ts = current_min_ts.replace(hour=9, minute=15, second=0, microsecond=0)
            if current_min_ts >= anchor_ts:
                mins_since_anchor = int((current_min_ts - anchor_ts).total_seconds() / 60)
                last_end_ts = anchor_ts + pd.Timedelta(minutes=(mins_since_anchor // tf) * tf)
                hist = await self.data_manager.get_historical_ohlc(inst_key, tf, last_end_ts + pd.Timedelta(seconds=1), for_full_day=True)
                if hist is not None and not hist.empty:
                    relevant = hist[hist.index <= last_end_ts]
                    if not relevant.empty:
                        prev_candle = relevant.iloc[-1]
                        val = prev_candle['high'] if is_r1 else prev_candle['low']
                        return float(val), ('PrevHigh' if is_r1 else 'PrevLow'), s1_v, r1_v, phase, s1_bh, r1_bl, b_lvl

        val = r1_v if is_r1 else s1_v
        return float(val) if val is not None else None, ('R1' if is_r1 else 'S1'), s1_v, r1_v, phase, s1_bh, r1_bl, b_lvl

    async def get_monotonic_barrier(self, strike, inst_key, tf, indicator_type, timestamp, direction, tracker):
        """
        Retrieves a nuanced barrier and ensures it moves monotonically.
        Returns (mono_val, b_val, label, s1_v, r1_v, phase, s1_bh, r1_bl, b_lvl, prev_val)
        """
        res = await self.get_nuanced_barrier(inst_key, indicator_type, tf, timestamp)
        b_val, label, s1_v, r1_v, phase, s1_bh, r1_bl, b_lvl = res

        if b_val is None:
            return None, None, label, s1_v, r1_v, phase, s1_bh, r1_bl, b_lvl, None

        is_r1 = (indicator_type == 'r1_high')
        if phase == ('R1_TRACKING' if is_r1 else 'S1_TRACKING'):
            return b_val, b_val, label, s1_v, r1_v, phase, s1_bh, r1_bl, b_lvl, b_val

        inst_side = 'CE' if direction == 'CALL' else 'PE'
        strike_key = f"{float(strike):.1f}_{inst_side}_{tf}m_{indicator_type}"

        if indicator_type == 's1_low':
            prev_val = tracker.get(strike_key, 0.0)
            mono_val = max(b_val, prev_val)
            tracker[strike_key] = mono_val
        else:
            prev_val = tracker.get(strike_key, 999999.0)
            mono_val = min(b_val, prev_val)
            tracker[strike_key] = mono_val

        return mono_val, b_val, label, s1_v, r1_v, phase, s1_bh, r1_bl, b_lvl, prev_val

    async def get_r1_profit_status(self, inst_key, timeframe_minutes, timestamp):
        """
        Retrieves R1 status for profit taking for a given timeframe.
        Returns (r1_val, is_established, candle_low).
        """
        tf = timeframe_minutes
        current_boundary = timestamp.replace(second=0, microsecond=0)
        if current_boundary.tzinfo is None:
            current_boundary = pd.Timestamp(current_boundary).tz_localize('Asia/Kolkata')

        if not hasattr(self, '_r1_profit_cache'): self._r1_profit_cache = {}
        cache_key = (inst_key, tf)
        cached = self._r1_profit_cache.get(cache_key)
        if cached and cached['ts'] == current_boundary:
            return cached['val']

        ohlc = await self.get_robust_ohlc(inst_key, tf, timestamp)
        if ohlc is None or ohlc.empty:
            return None, False, None

        today = timestamp.date()
        finalized_ohlc = ohlc[(ohlc.index < current_boundary) & (ohlc.index.date == today)]

        if finalized_ohlc.empty:
            return None, False, None

        calc = SupportResistanceCalculator(None, None)
        for ts, row in finalized_ohlc.iterrows():
            candle_data = {'timestamp': ts, 'open': row['open'], 'high': row['high'], 'low': row['low'], 'close': row['close']}
            calc.process_straddle_candle(inst_key, candle_data)

        state = calc.get_calculated_sr_state(inst_key)
        sr_levels = state.get('sr_levels', {})
        r1 = sr_levels.get('R1')
        r1_val = float(r1['high']) if r1 else None
        is_established = r1.get('is_established', False) if r1 else False
        r1_low = float(r1['low']) if r1 else None

        res = (r1_val, is_established, r1_low)
        self._r1_profit_cache[cache_key] = {'ts': current_boundary, 'val': res}
        return res

    async def calculate_atr(self, inst_key, timeframe_minutes, length, timestamp, current_ltp=None):
        """Calculates the Average True Range (ATR) for an instrument, optionally including current LTP."""
        # Include current candle to use live LTP if requested
        ohlc = await self.get_robust_ohlc(inst_key, timeframe_minutes, timestamp, include_current=True)
        if ohlc is None or len(ohlc) < length + 1:
            ohlc = await self.data_manager.get_historical_ohlc(inst_key, timeframe_minutes, timestamp, for_full_day=True, include_current=True)
            if ohlc is None or len(ohlc) < length + 1:
                return None

        df = ohlc.sort_index().copy()

        # If current_ltp is provided, inject it into the last candle to match user requirement
        if current_ltp is not None and not df.empty:
            last_idx = df.index[-1]
            df.at[last_idx, 'close'] = float(current_ltp)
            df.at[last_idx, 'high'] = max(df.at[last_idx, 'high'], float(current_ltp))
            df.at[last_idx, 'low'] = min(df.at[last_idx, 'low'], float(current_ltp))

        df['prev_close'] = df['close'].shift(1)

        # True Range calculation
        df['tr'] = pd.concat([
            df['high'] - df['low'],
            (df['high'] - df['prev_close']).abs(),
            (df['low'] - df['prev_close']).abs()
        ], axis=1).max(axis=1)

        # ATR as a simple moving average of True Range
        atr_series = df['tr'].rolling(window=length).mean()
        atr = atr_series.iloc[-1]

        return float(atr) if pd.notna(atr) else None

    async def calculate_combined_rsi(self, key1, key2, timeframe_minutes, period, timestamp):
        """
        Calculates RSI on the sum of close prices of two instruments.
        Uses Wilder's smoothing (standard RSI).
        """
        if not key1 or not key2:
            return None

        # Ensure we request enough history to cover the RSI period across day boundaries.
        # We need at least 'period + 1' candles.
        # Fetch historical data (exclude current running candle as requested)
        ohlc1 = await self.get_robust_ohlc(key1, timeframe_minutes, timestamp, include_current=False, for_full_day=True)
        ohlc2 = await self.get_robust_ohlc(key2, timeframe_minutes, timestamp, include_current=False, for_full_day=True)

        if ohlc1 is None or len(ohlc1) < period + 1 or ohlc2 is None or len(ohlc2) < period + 1:
            logger.debug(f"[IndicatorManager] Combined RSI: Missing or insufficient OHLC for {key1} or {key2}. Fetching from API...")
            # Try to fetch history manually from API
            for k in [key1, key2]:
                # This call handles both live and backtest by using the rest client
                await self.data_manager.fetch_and_cache_api_ohlc(k, timestamp.date())

            ohlc1 = await self.get_robust_ohlc(key1, timeframe_minutes, timestamp, include_current=False, for_full_day=True)
            ohlc2 = await self.get_robust_ohlc(key2, timeframe_minutes, timestamp, include_current=False, for_full_day=True)

            if ohlc1 is None or ohlc2 is None or len(ohlc1) < period + 1 or len(ohlc2) < period + 1:
                return None

        # Align by index (timestamp) and sum the close prices
        combined = pd.DataFrame({'close1': ohlc1['close'], 'close2': ohlc2['close']}).dropna()

        if len(combined) < period + 1:
            logger.debug(f"[IndicatorManager] Combined RSI: Insufficient data points ({len(combined)} < {period+1})")
            return None

        combined['sum_close'] = combined['close1'] + combined['close2']

        # Standard Wilder's RSI calculation
        delta = combined['sum_close'].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        # Wilder's smoothing is equivalent to EWM with alpha = 1/period
        avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        val = rsi.iloc[-1]
        return float(val) if pd.notna(val) else None
