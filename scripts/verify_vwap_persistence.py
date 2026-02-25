import sys
import os
import asyncio
import pandas as pd
from datetime import datetime, time
from unittest.mock import MagicMock, AsyncMock

# Add root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from hub.signal_monitor import SignalMonitor
from hub.position_manager import PositionManager

async def test_vwap_trend_persistence():
    print("\n--- Testing VWAP Trend Persistence ---")
    orchestrator = MagicMock()
    orchestrator.orchestrator_state = MagicMock()
    orchestrator.orchestrator_state.v2_target_strike_pair = {'strike': 26000}
    orchestrator.is_backtest = True
    orchestrator.entry_aggregator = MagicMock()

    # Mock config
    config = {
        'entry_use_pattern': False, # Bypass pattern for easier trend testing
        'entry_use_vwap': False,
        'entry_use_vwap_trend': True,
        'entry_vwap_trend_tf': 1,
        'entry_vwap_trend_occurrences': 3,
        'entry_r1_tf': 3,
        'entry_pattern_tf': 1,
        'exit_pattern_tf': 1,
        'entry_use_r1_breach': False
    }
    orchestrator.config_manager.get_boolean.side_effect = lambda s, k, fallback=None: config.get(k, fallback)
    orchestrator.config_manager.get_int.side_effect = lambda s, k, fallback=None: config.get(k, fallback)

    monitor = SignalMonitor(orchestrator)
    monitor.data_manager.prime_aggregator = AsyncMock()
    monitor._calculate_vwap = AsyncMock(return_value=100.0)
    monitor._get_r1_high_status = AsyncMock(return_value=(90.0, 110.0, True, True, 'ESTABLISHED', 95.0, 105.0))

    # Mock history for _get_resampled_history (returning CE as higher always)
    base_ts = pd.Timestamp('2026-02-09 09:15:00', tz='Asia/Kolkata')
    history = pd.DataFrame([{'close': 100 + i} for i in range(10)],
                           index=[base_ts + pd.Timedelta(minutes=i) for i in range(10)])
    monitor._get_resampled_history = AsyncMock(return_value=history)

    # Sequence of VWAP trend results
    # (is_rising, curr, prev)
    trend_sequence = [
        (True, 101, 100), # Tick 1: counter -> 1
        (True, 102, 101), # Tick 2: counter -> 2
        (False, 101, 102),# Tick 3: counter -> 0 (Reset!)
        (True, 102, 101), # Tick 4: counter -> 1
        (True, 103, 102), # Tick 5: counter -> 2
        (True, 104, 103)  # Tick 6: counter -> 3 (TRIGGER!)
    ]
    monitor._get_vwap_trend = AsyncMock()
    monitor._get_vwap_trend.side_effect = trend_sequence

    monitor._confirm_entry = AsyncMock()

    watchlist = {26000: {'ce_ltp': 150.0, 'pe_ltp': 50.0}}

    for i, (is_rising, curr, prev) in enumerate(trend_sequence):
        ts = pd.Timestamp('2026-02-09 09:25:00', tz='Asia/Kolkata') + pd.Timedelta(seconds=i)
        await monitor.check_crossover_breach(ts, watchlist, 26000)

        counter = monitor.state_manager.dual_sr_monitoring_data['ce_data']['vwap_trend_counter']
        print(f"Check {i+1}: Rising={is_rising}, Counter={counter}, Triggered={monitor._confirm_entry.called}")

        if i < 5:
            if monitor._confirm_entry.called:
                print("FAILED: Triggered too early!")
                return
        else:
            if not monitor._confirm_entry.called:
                print("FAILED: Did not trigger at tick 6!")
                return

    print("SUCCESS: VWAP Trend Persistence works correctly.")

if __name__ == "__main__":
    asyncio.run(test_vwap_trend_persistence())
