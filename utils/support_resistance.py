from utils.logger import logger, log_sr_details
import pandas as pd
import copy

class SupportResistanceCalculator:
    def __init__(self, config_manager, data_manager):
        self.config_manager = config_manager
        self.data_manager = data_manager
        # self.states stores the S&R state for each instrument
        # Format: { inst_key: { 'current_phase': str, 'sr_levels': { 'S1': {...}, 'R1': {...}, ... } } }
        self.states = {}

    def get_calculated_sr_state(self, inst_key):
        state = self.states.get(inst_key, {'current_phase': 'UNKNOWN', 'sr_levels': {}})
        sr_levels = state.get('sr_levels', {})
        s1 = sr_levels.get('S1')
        r1 = sr_levels.get('R1')

        state['s1_established'] = s1.get('is_established', False) if s1 else False
        state['r1_established'] = r1.get('is_established', False) if r1 else False
        return state

    def reset_and_process_sequence(self, inst_key, candles):
        """Clears existing state for an instrument and replays a sequence of candles."""
        if inst_key in self.states:
            del self.states[inst_key]

        for candle in candles:
            self.process_straddle_candle(inst_key, candle)

        return self.get_calculated_sr_state(inst_key)

    @staticmethod
    async def get_sr_status_shared(data_manager, inst_key, timeframe_minutes, timestamp, ohlc_1m):
        """
        Calculates S&R status by replaying 1-minute history since 09:15 AM.
        Standardizes grouping and resampling logic for both SignalMonitor and PositionManager.
        Returns: (s1_low, r1_high, s1_est, r1_est, phase, s1_high, r1_low, breakout_level)
        """
        # 1. Ensure ohlc_1m is valid
        
        if ohlc_1m is None or ohlc_1m.empty:
            return None, None, False, False, None, None, None, None

        today = timestamp.date()
        from datetime import time
        boundary_ts = timestamp.replace(second=0, microsecond=0)
        if boundary_ts.tzinfo is None:
            boundary_ts = pd.Timestamp(boundary_ts).tz_localize('Asia/Kolkata')

        df_1m = ohlc_1m[
            (ohlc_1m.index.date == today) &
            (ohlc_1m.index.time >= time(9, 15)) &
            (ohlc_1m.index < boundary_ts)
        ].copy()

        # Rule #0: Ensure ascending order and robustness against missing candles
        df_1m = df_1m.sort_index()

        if not df_1m.empty:
            # Reindex to fill any gaps in 1-minute data between 09:15 and boundary_ts-1m
            # This ensures bucket calculations (iloc) remain synchronized with the clock.
            anchor_ts = boundary_ts.replace(hour=9, minute=15)
            expected_range = pd.date_range(start=anchor_ts, end=boundary_ts - pd.Timedelta(minutes=1), freq='1min')
            df_1m = df_1m.reindex(expected_range).ffill()

        if df_1m.empty or pd.isna(df_1m.iloc[0]['high']):
            # FALLBACK: If history doesn't start at 09:15, calculation will fail.
            # However, we've already tried fetching from API in _get_robust_ohlc.
            # If we are here, it means even API doesn't have 09:15 for this strike.
            # This can happen if the strike wasn't active.
            return None, None, False, False, None, None, None, None

        # Rule #0.1: Force anchor data if missing
        if df_1m.index[0].time() > time(9, 15):
             logger.debug(f"V2 S&R: History for {inst_key} starts late at {df_1m.index[0].time()}. S&R might be unstable.")

        # 2. Replay Logic
        # We use a fresh calculator for every status check to ensure state persistence
        # is derived strictly from historical price action (History Replay).
        calc = SupportResistanceCalculator(None, None)

        # Rule #1: Anchor at 09:15 AM (Mandatory start of day)

        # 3. Process Candles with Option A Precision (1m for Phase 0, TF for Tracking)
        last_processed_idx = -1

        # Phase 0: Always process 1-minute candles until breakout OR until first TF boundary
        # This ensures the Rule #1 anchor establishes support/resistance at the most precise 1m level.
        for i in range(len(df_1m)):
            row = df_1m.iloc[i]
            ts = row.name
            
            # Use silent=True for all historical candles to avoid log spam during replay
            is_final_candle = (i == len(df_1m) - 1)
            calc.process_straddle_candle(inst_key, {
                'timestamp': ts,
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'duration': 1
            }, silent=not is_final_candle)
            last_processed_idx = i

            # Strategy Requirement: At the first TF boundary (e.g. 09:18 for 3m), 
            # if no breakout has happened, we stop 1-minute processing and let 
            # the bucket-based logic take over to establish the "base candle".
            is_at_boundary = False
            if timeframe_minutes > 1:
                total_mins = ts.hour * 60 + ts.minute
                anchor_mins = 9 * 60 + 15
                if total_mins > anchor_mins and (total_mins - anchor_mins) % timeframe_minutes == 0:
                    is_at_boundary = True

            # Transition Logic: If Phase 0 is finished OR we reached a boundary, 
            # align to the NEXT clock-aligned TF boundary.
            if timeframe_minutes > 1 and (calc.states[inst_key]['current_phase'] != 'INITIAL_TREND_ESTABLISHMENT' or is_at_boundary):
                # Strategy Requirement: TF buckets must be clock-aligned to 09:15 AM anchor.
                def is_at_boundary(ts):
                    total_mins = ts.hour * 60 + ts.minute
                    anchor_mins = 9 * 60 + 15
                    return (total_mins - anchor_mins) % timeframe_minutes == 0

                # Continue 1m processing until we reach the start of a new TF bucket
                while (last_processed_idx + 1) < len(df_1m):
                    next_ts = df_1m.index[last_processed_idx + 1]
                    if is_at_boundary(next_ts):
                        break

                    last_processed_idx += 1
                    row = df_1m.iloc[last_processed_idx]
                    is_final_candle = (last_processed_idx == len(df_1m) - 1)
                    calc.process_straddle_candle(inst_key, {
                        'timestamp': row.name,
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'duration': 1
                    }, silent=not is_final_candle)
                break

        # Phase 1 & 2: Process remaining history using Strategy Timeframe buckets
        if timeframe_minutes > 1:
            # Start from the next natural boundary after Phase 0 alignment
            start_bucket_idx = last_processed_idx + 1

            # Strategy Requirement: R1/S1 should only update on full timeframe completions.
            # We skip the final partial bucket to ensure levels remain constant within the TF.
            for i in range(start_bucket_idx, len(df_1m), timeframe_minutes):
                group = df_1m.iloc[i:i + timeframe_minutes]
                if len(group) < timeframe_minutes:
                    continue

                bucket = {
                    'timestamp': group.index[0],
                    'high': float(group['high'].max()),
                    'low': float(group['low'].min()),
                    'close': float(group.iloc[-1]['close']),
                    'duration': len(group)
                }
                is_final_candle = (i + timeframe_minutes >= len(df_1m))
                calc.process_straddle_candle(inst_key, bucket, silent=not is_final_candle)

        # 4. Extract Final State
        state = calc.get_calculated_sr_state(inst_key)
        sr_levels = state.get('sr_levels', {})
        phase = state.get('current_phase')
        s1 = sr_levels.get('S1')
        r1 = sr_levels.get('R1')

        s1_val = float(s1['low']) if s1 else None
        r1_val = float(r1['high']) if r1 else None
        s1_est = state.get('s1_established', False)
        r1_est = state.get('r1_established', False)

        s1_high = float(s1['high']) if s1 and 'high' in s1 else s1_val
        r1_low = float(r1['low']) if r1 and 'low' in r1 else r1_val
        
        # Phase-aware protective breach levels
        active_breach_low = None
        active_breach_high = None

        r2 = sr_levels.get('R2')
        s2 = sr_levels.get('S2')

        # Active Breach Low (Support for CALLs)
        if phase in ['R1_TRACKING', 'S2_TRACKING']:
            active_breach_low = r1.get('breakout_level') if r1 else None
        elif phase == 'R2_TRACKING':
            active_breach_low = r2.get('breakout_level') if (r2 and r2.get('breakout_level')) else (r1.get('breakout_level') if r1 else None)

        # Active Breach High (Resistance for PUTs)
        if phase in ['S1_TRACKING', 'R2_TRACKING']:
            active_breach_high = s1.get('breakout_level') if s1 else None
        elif phase == 'S2_TRACKING':
            active_breach_high = s2.get('breakout_level') if (s2 and s2.get('breakout_level')) else (s1.get('breakout_level') if s1 else None)

        # breakout_val is kept for backward compatibility (Legacy entry signal)
        breakout_val = active_breach_low if phase in ['R1_TRACKING', 'S2_TRACKING', 'R2_TRACKING'] else (active_breach_high if phase != 'INITIAL_TREND_ESTABLISHMENT' else None)

        # Log details for verification (Final state for this replay)
        log_sr_details(f"FINAL_STATE | {timestamp} | {inst_key} | TF:{timeframe_minutes}m | {phase} | S1: {s1_val} | R1: {r1_val} | B_Low: {active_breach_low} | B_High: {active_breach_high}")

        return (s1_val, r1_val, s1_est, r1_est, phase, active_breach_high, active_breach_low, breakout_val)

    def process_straddle_candle(self, inst_key, candle_data, silent=False):
        """
        Support & Resistance logic using a granular state machine.
        Follows Ping-Pong logic with Rule #1 (09:15) as anchor.
        """
        if inst_key not in self.states:
            self._initialize_instrument(inst_key, candle_data)
            return

        ts = candle_data['timestamp']
        high = candle_data['high']
        low = candle_data['low']
        duration = candle_data.get('duration', 1)

        state = self.states[inst_key]
        last_candle = state['last_candle']

        # Skip if older
        if ts < last_candle['timestamp']:
            return
        # Skip if same time and NOT a longer duration (prevents double processing in history replay)
        if ts == last_candle['timestamp'] and duration <= last_candle.get('duration', 1):
            return

        phase = state['current_phase']
        sr_levels = state['sr_levels']
        s1 = sr_levels['S1']
        r1 = sr_levels['R1']

        # --- Phase 0: Base Range Breakout ---
        prev_high = last_candle['high']
        prev_low = last_candle['low']

        if phase == 'INITIAL_TREND_ESTABLISHMENT':
            # 1. OUTSIDE CANDLE: EXPAND BASE
            if high > prev_high and low < prev_low:
                s1['low'] = low
                r1['high'] = high
                s1['timestamp'] = ts
                r1['timestamp'] = ts
                if not silent: logger.debug(f"V2 S&R: Phase 0 Base Expansion (Outside Candle) | Strike: {inst_key} | at {ts.time()} | Range: {low:.2f} - {high:.2f}")

            # 2. BREAKOUT HIGH (BOUNCE): S1 Established at PREVIOUS Low
            # User Requirement: Both High and Low must be higher for establishment
            elif high > prev_high and low > prev_low:
                s1['low'] = prev_low
                s1['is_established'] = True
                s1['timestamp'] = last_candle['timestamp']

                r1['high'] = high
                r1['breakout_level'] = low
                r1['is_established'] = False
                r1['timestamp'] = ts

                state['current_phase'] = 'R1_TRACKING'
                msg = f"V2 S&R: Phase 0 -> R1_TRACKING (High Breach at {ts.time()}). S1 established at {s1['low']:.2f} | Hurdle: {prev_high:.2f}"
                if not silent: 
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

            # 4. BREAKOUT LOW (PULLBACK): R1 Established at PREVIOUS High
            # User Requirement: Both High and Low must be lower for establishment
            elif low < prev_low and high < prev_high:
                r1['high'] = prev_high
                r1['is_established'] = True
                r1['timestamp'] = last_candle['timestamp']

                s1['low'] = low
                s1['breakout_level'] = high
                s1['is_established'] = False
                s1['timestamp'] = ts

                state['current_phase'] = 'S1_TRACKING'
                msg = f"V2 S&R: Phase 0 -> S1_TRACKING (Low Breach at {ts.time()}). R1 established at {r1['high']:.2f} | Hurdle: {prev_low:.2f}"
                if not silent: 
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

            state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
            return

        # --- Phase 1: Primary Trend Tracking ---
        if phase == 'R1_TRACKING':
            # 1. Directional Flip: Breach established primary S1 -> Go back to tracking S1
            if low < s1['low']:
                # Transition: s1 becomes tracking s1
                s1_base = s1['low']
                s1['low'] = low # Update to new trough immediately
                s1['breakout_level'] = s1_base
                s1['timestamp'] = ts
                s1['is_established'] = False
                sr_levels['S2'] = None
                state['current_phase'] = 'S1_TRACKING'
                msg = f"V2 S&R: Directional Flip -> S1_TRACKING (S1 Breached) | Strike: {inst_key} | at {ts.time()}"
                if not silent: logger.debug(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            if high > r1['high']:
                # Keep moving peak up
                r1['high'] = high
                # Keep original breakout hurdle r1['low'] unchanged
                r1['timestamp'] = ts
                r1['is_established'] = False

            # Pullback Confirmation (Two-way): R1 Established
            if high < prev_high and low < prev_low:
                r1['is_established'] = True
                state['current_phase'] = 'S2_TRACKING'
                # Start tracking pullback trough S2
                sr_levels['S2'] = {'low': low, 'high': high, 'breakout_level': high, 'timestamp': ts, 'is_established': False}
                msg = f"V2 S&R: R1 Established at {r1['high']:.2f} | Confirmed at {ts.time()} (Phase: S2_TRACKING)"
                if not silent: 
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

        elif phase == 'S1_TRACKING':
            # 1. Directional Flip: Breach established primary R1 -> Go back to tracking R1
            if high > r1['high']:
                r1_base = r1['high']
                r1['high'] = high # Update to new peak immediately
                r1['breakout_level'] = r1_base
                r1['timestamp'] = ts
                r1['is_established'] = False
                sr_levels['R2'] = None
                state['current_phase'] = 'R1_TRACKING'
                msg = f"V2 S&R: Directional Flip -> R1_TRACKING (R1 Breached) | Strike: {inst_key} | at {ts.time()}"
                if not silent: logger.debug(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            if low < s1['low']:
                # Keep moving trough down
                s1['low'] = low
                # Keep original breakout hurdle s1['high'] unchanged
                s1['timestamp'] = ts
                s1['is_established'] = False

            # Bounce Confirmation (Two-way): S1 Established
            if low > prev_low and high > prev_high:
                s1['is_established'] = True
                state['current_phase'] = 'R2_TRACKING'
                # Start tracking bounce peak R2
                sr_levels['R2'] = {'high': high, 'low': low, 'breakout_level': low, 'timestamp': ts, 'is_established': False}
                msg = f"V2 S&R: S1 Established at {s1['low']:.2f} | Confirmed at {ts.time()} (Phase: R2_TRACKING)"
                if not silent: 
                    logger.debug(msg)
                    log_sr_details(f"STRIKE: {inst_key} | {msg}")

        # --- Phase 2: Secondary Tracking (Ping-Pong / Promotion) ---
        elif phase == 'S2_TRACKING':
            s2 = sr_levels['S2']

            # 1. Structural Reset: Breach established primary R1 -> Go back to tracking R1
            if high > r1['high']:
                r1_base = r1['high']
                # PROMOTE S2 to S1 (Trail Support Up) before resetting R1
                if sr_levels.get('S2'):
                    old_s1 = s1['low']
                    sr_levels['S1'] = sr_levels['S2'].copy()
                    sr_levels['S1']['is_established'] = True
                    s1 = sr_levels['S1'] # Re-reference
                    msg = f"V2 S&R: Scenario A (R1 Breach) + S2->S1 Promotion | Strike: {inst_key} | S1Low: {old_s1:.2f} -> {s1['low']:.2f} | Resetting R1"
                    if not silent: 
                        logger.debug(msg)
                        log_sr_details(msg)
                else:
                    msg = f"V2 S&R: Scenario A (R1 Breach) -> R1_TRACKING | Strike: {inst_key} | at {ts.time()}"
                    if not silent: 
                        logger.debug(msg)
                        log_sr_details(msg)

                r1['high'] = high # Update to new peak
                r1['breakout_level'] = low
                r1['timestamp'] = ts
                sr_levels['S2'] = None
                state['current_phase'] = 'R1_TRACKING'
                r1['is_established'] = False
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            # 2. Directional Flip: Breach established primary S1 -> Go back to tracking S1
            if low < s1['low']:
                s1_base = s1['low']
                s1['low'] = low # Update to new trough immediately
                s1['breakout_level'] = s1_base
                s1['timestamp'] = ts
                sr_levels['S2'] = None
                state['current_phase'] = 'S1_TRACKING'
                s1['is_established'] = False
                msg = f"V2 S&R: Directional Flip -> S1_TRACKING (S1 Breached) | Strike: {inst_key} | at {ts.time()}"
                if not silent: 
                    logger.debug(msg)
                    log_sr_details(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            elif low < s2['low']:
                # Continuing pullback to find new trough
                s2['low'] = low
                s2['timestamp'] = ts

            elif low > prev_low and high > prev_high:
                # Bounce confirmed! (Two-way confirmation)
                # Scenario B: Bounces but doesn't break R1
                old_s1 = s1['low']

                # PT1 Logic: Promote S2 to S1 (Trail Support Up)
                sr_levels['S1'] = sr_levels['S2'].copy()
                sr_levels['S1']['is_established'] = True
                s1 = sr_levels['S1'] # Re-reference for logging
                sr_levels['S2'] = None # VOID

                # Start tracking next peak R2
                state['current_phase'] = 'R2_TRACKING'
                sr_levels['R2'] = {'high': high, 'low': low, 'breakout_level': low, 'timestamp': ts, 'is_established': False}
                msg = f"V2 S&R: Scenario B (Bounce No Breach) + S2->S1 Promotion | Strike: {inst_key} | S1Low: {old_s1:.2f} -> {s1['low']:.2f} | Tracking R2"
                if not silent: 
                    logger.debug(msg)
                    log_sr_details(msg)

        elif phase == 'R2_TRACKING':
            r2 = sr_levels['R2']

            # 1. Structural Reset: Breach established primary R1 -> Go back to tracking R1
            if high > r1['high']:
                r1_base = r1['high']
                # Scenario A: R1 Breach -> R1 takes R2 values and tracking restarts
                sr_levels['R1'] = sr_levels['R2'].copy()
                r1 = sr_levels['R1']
                r1['high'] = high # Update to new peak immediately
                r1['breakout_level'] = r1_base
                r1['timestamp'] = ts
                r1['is_established'] = False
                sr_levels['R2'] = None
                state['current_phase'] = 'R1_TRACKING'
                msg = f"V2 S&R: Scenario A (R1 Breach) -> R1_TRACKING | Strike: {inst_key} | at {ts.time()} | R1 took R2 values"
                if not silent: 
                    logger.debug(msg)
                    log_sr_details(msg)
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            # 2. Directional Flip: Breach established primary S1 -> Go back to tracking S1
            if low < s1['low']:
                s1_base = s1['low']
                # PT2 Logic: Promote R2 to R1 (Trail Resistance Down) before resetting S1
                if sr_levels.get('R2'):
                    old_r1 = r1['high']
                    sr_levels['R1'] = sr_levels['R2'].copy()
                    sr_levels['R1']['is_established'] = True
                    r1 = sr_levels['R1'] # Re-reference
                    msg = f"V2 S&R: Directional Flip (S1 Breach) + R2->R1 Promotion | Strike: {inst_key} | R1High: {old_r1:.2f} -> {r1['high']:.2f} | Resetting S1"
                    if not silent: 
                        logger.debug(msg)
                        log_sr_details(msg)
                else:
                    msg = f"V2 S&R: Directional Flip -> S1_TRACKING (S1 Breached) | Strike: {inst_key} | at {ts.time()}"
                    if not silent: 
                        logger.debug(msg)
                        log_sr_details(msg)

                s1['low'] = low # Update to new trough
                s1['breakout_level'] = high
                s1['timestamp'] = ts
                sr_levels['R2'] = None
                state['current_phase'] = 'S1_TRACKING'
                s1['is_established'] = False
                state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}
                return

            elif high > r2['high']:
                # Continuing bounce to find new peak
                r2['high'] = high
                r2['timestamp'] = ts

            elif high < prev_high and low < prev_low:
                # Pullback confirmed! (Two-way confirmation)
                # Scenario B: New Pullback without breaking R1
                old_r1 = r1['high']

                # PT2 Logic: Promote R2 to R1 (Trail Resistance Down)
                sr_levels['R1'] = sr_levels['R2'].copy()
                sr_levels['R1']['is_established'] = True
                r1 = sr_levels['R1'] # Re-reference for logging
                sr_levels['R2'] = None # VOID

                # Start tracking next trough S2
                state['current_phase'] = 'S2_TRACKING'
                sr_levels['S2'] = {'low': low, 'high': high, 'breakout_level': high, 'timestamp': ts, 'is_established': False}
                msg = f"V2 S&R: Scenario B (Pullback No Breach) + R2->R1 Promotion | Strike: {inst_key} | R1High: {old_r1:.2f} -> {r1['high']:.2f} | Tracking S2"
                if not silent: 
                    logger.debug(msg)
                    log_sr_details(msg)

        # Always update last candle at the end
        state['last_candle'] = {'high': high, 'low': low, 'timestamp': ts, 'duration': duration}

    def _initialize_instrument(self, inst_key, candle_data):
        ts = candle_data['timestamp']
        high = candle_data['high']
        low = candle_data['low']
        duration = candle_data.get('duration', 1)

        self.states[inst_key] = {
            'current_phase': 'INITIAL_TREND_ESTABLISHMENT',
            'last_candle': {'high': high, 'low': low, 'timestamp': ts, 'duration': duration},
            'sr_levels': {
                'S1': {'low': low, 'high': high, 'timestamp': ts, 'is_established': False},
                'R1': {'high': high, 'low': low, 'timestamp': ts, 'is_established': False},
                'S2': None,
                'R2': None
            }
        }
