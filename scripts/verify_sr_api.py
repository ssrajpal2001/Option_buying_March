import asyncio
import os
import sys
import copy
import pandas as pd
from datetime import datetime, time, timedelta

# Add root to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.config_manager import ConfigManager
from utils.api_client_manager import ApiClientManager
from hub.data_manager import DataManager
from utils.support_resistance import SupportResistanceCalculator
from utils.logger import configure_logger, logger

async def verify_sr_api(target_instrument, target_strike, target_type, target_expiry, target_date_str, timeframe_minutes, direct_key=None):
    """
    Fetches real historical data from Upstox API and verifies S&R logic.
    """
    # 1. Configuration - Robust path detection
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, '..'))
    config_path = os.path.join(project_root, 'config/config_trader.ini')

    if not os.path.exists(config_path):
        print(f"Error: Configuration file not found at {config_path}")
        return

    config_manager = ConfigManager(config_file=config_path)
    configure_logger(config_manager)

    # 2. Parameters Parsing
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d').date()

    print(f"\n" + "="*80)
    if direct_key:
        print(f" S&R API VERIFICATION: DIRECT KEY: {direct_key}")
    else:
        print(f" S&R API VERIFICATION: {target_instrument} {target_strike} {target_type} | EXPIRY: {target_expiry}")
    print(f" Target Date: {target_date} | Timeframe: {timeframe_minutes}m")
    print("="*80)

    # 3. Initialize API and Data Manager
    api_manager = ApiClientManager(config_manager)
    await api_manager.async_init()

    # Use active client
    rest_client = api_manager.get_active_client()
    if not rest_client:
        print("Error: Could not initialize API client. check credentials.ini")
        await api_manager.close()
        return

    index_symbol = config_manager.get(target_instrument, 'instrument_symbol') or 'NSE_INDEX|Nifty 50'
    data_manager = DataManager(rest_client, index_symbol, config_manager)

    # 4. Find the specific instrument key
    if direct_key:
        inst_key = direct_key
        print(f"Using Direct Instrument Key: {inst_key}")
    else:
        print(f"Finding instrument key for {target_strike} {target_type} expiring {target_expiry}...")
        expiry_dt = datetime.strptime(target_expiry, '%Y-%m-%d')
        contract = await data_manager.get_live_contract_details(target_strike, expiry_dt.date(), target_type)

        if not contract:
            print(f"Error: Could not find contract for {target_strike} {target_type} {target_expiry}")
            await api_manager.close()
            return

        inst_key = contract.instrument_key
        print(f"Found Instrument Key: {inst_key}")

    # 5. Fetch 1-minute historical data
    print(f"Fetching 1m historical data for {target_date}...")
    # Fetch from market open to end of session
    ohlc_1m = await data_manager._fetch_and_prepare_api_data(inst_key, target_date, target_date, "1minute")

    if ohlc_1m.empty:
        print(f"Error: No data returned from API for {inst_key} on {target_date}")
        await api_manager.close()
        return

    # Filter strictly from 09:15 today
    df_1m = ohlc_1m[
        (ohlc_1m.index.date == target_date) &
        (ohlc_1m.index.time >= time(9, 15))
    ].copy()

    if df_1m.empty:
        print(f"Error: No data available after 09:15 on {target_date}")
        await api_manager.close()
        return

    # 6. Group and process
    calc = SupportResistanceCalculator(None, None)

    header = f"{'TIME':<10} | {'OPEN':<8} | {'HIGH':<8} | {'LOW':<8} | {'CLOSE':<8} | {'PHASE':<25} | {'S1LOW':<12} | {'R1HIGH':<12} | {'S2LOW':<12} | {'R2HIGH':<12} | {'EVENT'}"
    print(f"\n{header}")
    print("-" * 145)

    def format_level(levels, level_key):
        lvl = levels.get(level_key)
        if not lvl: return "---"
        val = lvl.get('low') if 'S' in level_key else lvl.get('high')
        est = "(E)" if lvl.get('is_established') else ""
        return f"{val:.2f}{est}"

    # Rule #1: 09:15 1m candle
    anchor_1m = df_1m[df_1m.index.time == time(9, 15)]
    if not anchor_1m.empty:
        row = anchor_1m.iloc[0]
        candle_0915 = {
            'timestamp': anchor_1m.index[0],
            'open': float(row['open']),
            'high': float(row['high']),
            'low': float(row['low']),
            'close': float(row['close']),
            'duration': 1
        }
        calc.process_straddle_candle(inst_key, candle_0915)
        state = calc.get_calculated_sr_state(inst_key)
        levels = state.get('sr_levels', {})

        s1_str = format_level(levels, 'S1')
        r1_str = format_level(levels, 'R1')

        print(f"{candle_0915['timestamp'].strftime('%H:%M'):<10} | "
              f"{candle_0915['open']:<8.2f} | {candle_0915['high']:<8.2f} | {candle_0915['low']:<8.2f} | {candle_0915['close']:<8.2f} | "
              f"{state['current_phase']:<25} | {s1_str:<12} | {r1_str:<12} | {'---':<12} | {'---':<12} | Rule #1")

    # Group into buckets
    for i in range(0, len(df_1m), timeframe_minutes):
        group = df_1m.iloc[i:i + timeframe_minutes]
        if not group.empty:
            bucket = {
                'timestamp': group.index[0],
                'open': float(group.iloc[0]['open']),
                'high': float(group['high'].max()),
                'low': float(group['low'].min()),
                'close': float(group.iloc[-1]['close']),
                'duration': timeframe_minutes
            }

            prev_state = copy.deepcopy(calc.get_calculated_sr_state(inst_key))
            calc.process_straddle_candle(inst_key, bucket)
            state = calc.get_calculated_sr_state(inst_key)

            levels = state.get('sr_levels', {})
            s1_str = format_level(levels, 'S1')
            r1_str = format_level(levels, 'R1')
            s2_str = format_level(levels, 'S2')
            r2_str = format_level(levels, 'R2')

            event_list = []
            if state['current_phase'] != prev_state.get('current_phase'):
                event_list.append(f"PHASE -> {state['current_phase']}")

            # Establishment Checks
            for lvl_key in ['S1', 'R1', 'S2', 'R2']:
                curr_lvl = levels.get(lvl_key)
                prev_lvl = prev_state.get('sr_levels', {}).get(lvl_key)
                if curr_lvl and curr_lvl.get('is_established') and (not prev_lvl or not prev_lvl.get('is_established')):
                    event_list.append(f"{lvl_key} ESTABLISHED")

            # Trail Checks
            if levels.get('S1') and prev_state.get('sr_levels', {}).get('S1') and \
                 levels['S1']['low'] != prev_state['sr_levels']['S1']['low']:
                 event_list.append("S1 TRAIL")
            if levels.get('R1') and prev_state.get('sr_levels', {}).get('R1') and \
                 levels['R1']['high'] != prev_state['sr_levels']['R1']['high']:
                 event_list.append("R1 TRAIL")

            event = ", ".join(list(dict.fromkeys(event_list))) # Unique list

            print(f"{bucket['timestamp'].strftime('%H:%M'):<10} | "
                  f"{bucket['open']:<8.2f} | {bucket['high']:<8.2f} | {bucket['low']:<8.2f} | {bucket['close']:<8.2f} | "
                  f"{state['current_phase']:<25} | {s1_str:<12} | {r1_str:<12} | {s2_str:<12} | {r2_str:<12} | {event}")

    await api_manager.close()
    print("\nVerification Complete.")

def parse_instrument_string(instr_str):
    """
    Parses a string like "NIFTY 26000 CE 24 FEB 2026" or "NIFTY 26000 CE 24-02-2026"
    """
    parts = instr_str.split()
    if len(parts) < 3:
        return None

    # Simple extraction
    name = parts[0]
    strike = int(parts[1])
    opt_type = parts[2]

    # Try to find date parts
    date_str = " ".join(parts[3:])
    # Replace common suffixes
    date_str = date_str.replace("TH", "").replace("RD", "").replace("ST", "").replace("ND", "").replace("EXPIRY", "").strip()

    formats = ["%d %b %Y", "%d %b %y", "%d-%m-%Y", "%Y-%m-%d", "%d %B %Y", "%d %b"]
    parsed_date = None
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            if parsed_date.year == 1900: # handled default year
                parsed_date = parsed_date.replace(year=datetime.now().year)
            break
        except ValueError:
            continue

    if parsed_date:
        return name, strike, opt_type, parsed_date.strftime('%Y-%m-%d')
    return name, strike, opt_type, None

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Verify S&R logic using real Upstox API data.")
    parser.add_argument('--key', type=str, help='Direct instrument key (e.g. "NSE_FO|64872")')
    parser.add_argument('--option', type=str, help='Full option name (e.g. "NIFTY 26000 CE 24 FEB 2026")')
    parser.add_argument('--instrument', type=str, default='NIFTY', help='Instrument name (e.g., NIFTY, BANKNIFTY)')
    parser.add_argument('--strike', type=int, default=26000, help='Strike price')
    parser.add_argument('--type', type=str, default='CE', help='Option type (CE/PE)')
    parser.add_argument('--expiry', type=str, default='2026-02-24', help='Expiry date (YYYY-MM-DD)')
    parser.add_argument('--date', type=str, default='2026-02-09', help='Date to fetch history for (YYYY-MM-DD)')
    parser.add_argument('--tf', type=int, default=3, help='Timeframe in minutes')

    args = parser.parse_args()

    inst, strike, otype, expiry = args.instrument, args.strike, args.type, args.expiry

    if args.option:
        parsed = parse_instrument_string(args.option)
        if parsed:
            inst, strike, otype, p_expiry = parsed
            if p_expiry: expiry = p_expiry
            print(f"Parsed from string: {inst} {strike} {otype} {expiry}")

    asyncio.run(verify_sr_api(
        inst, strike, otype, expiry, args.date, args.tf, direct_key=args.key
    ))
