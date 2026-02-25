import pandas as pd
from datetime import datetime

def simulate_sticky_cooldown():
    # Simulate an exit at 10:13:00
    exit_timestamp = pd.Timestamp("2026-02-10 10:13:00")

    # Monitoring data after SL exit
    monitoring_data = {
        'pending_vwap_side': 'CE',
        'awaiting_fresh_vwap_breach': True,
        'sticky_dip_confirmed': False,
        'last_exit_time': exit_timestamp
    }

    # Current Tick (same second or same minute)
    current_timestamp = pd.Timestamp("2026-02-10 10:13:05")

    # Logic from SignalMonitor
    is_above_vwap = True
    is_r1_breached = False # Price is below R1High (DIP condition)

    is_sticky_reentry = monitoring_data.get('awaiting_fresh_vwap_breach', False)
    is_dip_pending = False

    if is_sticky_reentry:
        is_below_filters = not is_above_vwap or not is_r1_breached
        if is_below_filters:
            if not monitoring_data.get('sticky_dip_confirmed'):
                # COOLDOWN CHECK
                last_exit = monitoring_data.get('last_exit_time')
                if last_exit and last_exit.minute == current_timestamp.minute and last_exit.hour == current_timestamp.hour:
                    is_dip_pending = True
                    print(f"DEBUG: DIP blocked due to same-minute cooldown at {current_timestamp}")
                else:
                    monitoring_data['sticky_dip_confirmed'] = True
                    print(f"DEBUG: DIP confirmed at {current_timestamp}")

    # Execution check
    if is_above_vwap and not is_r1_breached and not is_dip_pending:
         # In reality, entry triggers when is_r1_breached becomes True
         pass

    print(f"Result: is_dip_pending={is_dip_pending}, sticky_dip_confirmed={monitoring_data['sticky_dip_confirmed']}")

    # Simulate next minute
    next_minute_ts = pd.Timestamp("2026-02-10 10:14:00")
    is_dip_pending = False
    if is_sticky_reentry:
        is_below_filters = not is_above_vwap or not is_r1_breached
        if is_below_filters:
            if not monitoring_data.get('sticky_dip_confirmed'):
                last_exit = monitoring_data.get('last_exit_time')
                if last_exit and last_exit.minute == next_minute_ts.minute and last_exit.hour == next_minute_ts.hour:
                    is_dip_pending = True
                else:
                    monitoring_data['sticky_dip_confirmed'] = True
                    print(f"DEBUG: DIP confirmed at {next_minute_ts}")

    print(f"Result Next Min: is_dip_pending={is_dip_pending}, sticky_dip_confirmed={monitoring_data['sticky_dip_confirmed']}")

if __name__ == "__main__":
    simulate_sticky_cooldown()
