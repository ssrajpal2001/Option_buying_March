import sys
import os
import copy
import pandas as pd
from datetime import datetime, time

# Add root to sys.path to allow imports from utils and hub
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.support_resistance import SupportResistanceCalculator

def simulate_v2_monitor_trace():
    """
    Simulates the SignalMonitor._get_r1_high_status logic using 1-minute data.
    """
    timeframe_minutes = 3
    inst_key = "NIFTY26000CE"

    # User provided 1-minute data from NSE_FO|64872
    base_time = datetime(2026, 2, 10, 9, 15)

    # Trace values extracted from logs
    raw_1m_buckets = [
        (0, 192.95, 195.00, 181.80, 192.65), # 09:15
        (3, 192.95, 202.00, 181.80, 200.00), # 09:15 bucket
        (6, 200.00, 203.20, 194.50, 195.30), # 09:18
        (9, 195.75, 196.25, 186.35, 191.05), # 09:21
        (12, 191.20, 197.50, 191.00, 195.65),# 09:24
        (15, 195.35, 198.40, 193.35, 195.65),# 09:27
        (18, 196.45, 198.95, 192.70, 197.40),# 09:30
        (21, 197.40, 211.90, 197.30, 211.20),# 09:33
        (24, 211.20, 212.45, 207.95, 210.50),# 09:36
        (27, 210.55, 212.90, 207.55, 211.50),# 09:39
        (30, 212.15, 214.40, 210.00, 212.95),# 09:42
        (33, 213.65, 217.00, 212.00, 217.00),# 09:45
        (36, 216.70, 218.80, 206.50, 208.95),# 09:48
        (39, 208.35, 214.65, 205.65, 206.55),# 09:51
        (42, 206.75, 210.70, 202.00, 210.00),# 09:54
        (45, 210.30, 211.65, 208.00, 211.55),# 09:57
        (48, 211.70, 214.70, 210.00, 213.25),# 10:00
        (51, 214.10, 215.40, 213.25, 213.25),# 10:03
        (54, 213.75, 217.50, 212.75, 213.25),# 10:06
        (57, 213.70, 214.00, 208.75, 210.00),# 10:09
        (60, 210.00, 214.30, 207.25, 209.25),# 10:12
    ]

    # Expand buckets to 1m data
    raw_1m = []
    for m, o, h, l, c in raw_1m_buckets:
        for i in range(3):
            raw_1m.append((m + i, o, h, l, c))

    df_1m = pd.DataFrame([
        {'timestamp': base_time + pd.Timedelta(minutes=m), 'open': o, 'high': h, 'low': l, 'close': c}
        for m, o, h, l, c in raw_1m
    ])
    df_1m.set_index('timestamp', inplace=True)

    print(f"\n" + "="*180)
    print(f" S&R TRACE: {inst_key} | {timeframe_minutes}m timeframe from 1m ticks")
    print("="*180)
    header = f"{'TIME':<8} | {'OPEN':<8} | {'HIGH':<8} | {'LOW':<8} | {'CLOSE':<8} | {'PHASE':<25} | {'S1LOW':<14} | {'R1HIGH':<14} | {'MONO_S1':<12} | {'S2LOW':<12} | {'R2HIGH':<12} | {'EVENT'}"
    print(header)
    print("-" * 180)

    last_state = None
    mono_tracker = 0

    for i in range(len(df_1m)):
        current_ts = df_1m.index[i] + pd.Timedelta(seconds=5)

        df_completed = df_1m[
            (df_1m.index.time >= time(9, 15)) &
            (df_1m.index < current_ts.replace(second=0, microsecond=0))
        ].copy()

        if df_completed.empty: continue

        # Resampling
        resampled_buckets = []
        anchor_1m = df_completed[df_completed.index.time == time(9, 15)]

        for j in range(0, len(df_completed), timeframe_minutes):
            group = df_completed.iloc[j:j + timeframe_minutes]
            if len(group) == timeframe_minutes:
                bucket = {
                    'timestamp': group.index[0],
                    'high': group['high'].max(),
                    'low': group['low'].min(),
                    'close': group.iloc[-1]['close'],
                    'duration': timeframe_minutes
                }
                resampled_buckets.append(bucket)

        calc = SupportResistanceCalculator(None, None)

        # Rule #1 Anchor
        if not anchor_1m.empty:
            row = anchor_1m.iloc[0]
            calc.process_straddle_candle(inst_key, {
                'timestamp': anchor_1m.index[0],
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'duration': 1
            })

        # Buckets
        for bucket in resampled_buckets:
            calc.process_straddle_candle(inst_key, bucket)

        state = calc.get_calculated_sr_state(inst_key)
        curr_phase = state['current_phase']
        levels = state['sr_levels']

        s1 = levels.get('S1')
        r1 = levels.get('R1')
        s2 = levels.get('S2')
        r2 = levels.get('R2')

        s1_str = f"{s1['low']:.2f}{'(E)' if s1.get('is_established') else ''}" if s1 else "---"
        r1_str = f"{r1['high']:.2f}{'(E)' if r1.get('is_established') else ''}" if r1 else "---"
        s2_str = f"{s2['low']:.2f}" if s2 else "---"
        r2_str = f"{r2['high']:.2f}" if r2 else "---"

        event_list = []
        if last_state:
            if curr_phase != last_state['phase']:
                event_list.append(f"PHASE -> {curr_phase}")

            curr_r1_est = r1.get('is_established', False) if r1 else False
            if curr_r1_est and not last_state['r1_est']:
                event_list.append("R1 ESTABLISHED")

            curr_s1_est = s1.get('is_established', False) if s1 else False
            if curr_s1_est and not last_state['s1_est']:
                event_list.append("S1 ESTABLISHED")

            if r1 and last_state['r1_val'] and abs(r1['high'] - last_state['r1_val']) > 0.01:
                event_list.append("R1 TRAIL")
            if s1 and last_state['s1_val'] and abs(s1['low'] - last_state['s1_val']) > 0.01:
                event_list.append("S1 TRAIL")

        last_state = {
            'phase': curr_phase,
            'r1_val': float(r1['high']) if r1 else None,
            's1_val': float(s1['low']) if s1 else None,
            'r1_est': r1.get('is_established', False) if r1 else False,
            's1_est': s1.get('is_established', False) if s1 else False
        }

        raw_s1_val = float(s1['low']) if s1 else 0
        if raw_s1_val > mono_tracker:
            mono_tracker = raw_s1_val
        mono_s1_str = f"{mono_tracker:.2f}" if mono_tracker > 0 else "---"

        row_1m = df_1m.iloc[i]
        event = ", ".join(event_list)

        print(f"{df_1m.index[i].strftime('%H:%M'):<8} | "
              f"{row_1m['open']:<8.2f} | {row_1m['high']:<8.2f} | {row_1m['low']:<8.2f} | {row_1m['close']:<8.2f} | "
              f"{curr_phase:<25} | {s1_str:<14} | {r1_str:<14} | {mono_s1_str:<12} | {s2_str:<12} | {r2_str:<12} | {event}")

if __name__ == "__main__":
    simulate_v2_monitor_trace()
