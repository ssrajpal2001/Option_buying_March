import re
import pandas as pd
import sys
import os

def parse_log_file(filepath):
    data = []

    # Regex patterns
    summary_pattern = re.compile(r"\[TRADE_SUMMARY\] Timestamp: ([\d\- :+]+) \| ATM: (\d+)")
    blocked_pattern = re.compile(r"V2: (CE|PE) Buy signal PASSED Pattern, but BLOCKED by TICK filters: .* \(LTP: ([\d\.]+), Slope: Live:([\d\.]+) [>=<] Prev:([\d\.]+) \(Cons:(\d+)/(\d+)\)\)")
    triggered_pattern = re.compile(r"V2 TRIGGERED (CE|PE) BUY at ([\d\.]+). Indicators: VWAP:([\d\.]+), R1:([\d\.]+), Slope: Live:([\d\.]+) [>=<] Prev:([\d\.]+) \(Cons:(\d+)/(\d+)\)")
    monitoring_pattern = re.compile(r"Monitoring (CE|PE) Strike (\d+) at LTP ([\d\.]+). Indicators: VWAP:([\d\.]+), R1:([\d\.]+), Slope: Live:([\d\.]+) [>=<] Prev:([\d\.]+) \(Cons:(\d+)/(\d+)\)")

    pending_data = []

    with open(filepath, 'r') as f:
        for line in f:
            # 1. Check for data logs first
            found = False
            for pattern in [blocked_pattern, triggered_pattern, monitoring_pattern]:
                m = pattern.search(line)
                if m:
                    side = m.group(1)
                    if pattern == monitoring_pattern:
                        ltp = float(m.group(3))
                        vwap = float(m.group(4))
                        r1 = float(m.group(5))
                        live_v = float(m.group(6))
                        prev_v = float(m.group(7))
                        cons = int(m.group(8))
                    elif pattern == triggered_pattern:
                        ltp = float(m.group(2))
                        vwap = float(m.group(3))
                        r1 = float(m.group(4))
                        live_v = float(m.group(5))
                        prev_v = float(m.group(6))
                        cons = int(m.group(7))
                    else: # Blocked
                        ltp = float(m.group(2))
                        live_v = float(m.group(3))
                        prev_v = float(m.group(4))
                        cons = int(m.group(5))
                        vwap = None
                        r1 = None

                    pending_data.append({
                        'side': side,
                        'ltp': ltp,
                        'vwap': vwap,
                        'r1': r1,
                        'live_vwap': live_v,
                        'prev_vwap': prev_v,
                        'consecutive': cons
                    })
                    found = True
                    break

            if found:
                continue

            # 2. Extract Timestamp and ATM from Trade Summary (Finalizes pending data)
            match = summary_pattern.search(line)
            if match:
                current_timestamp = pd.to_datetime(match.group(1))
                current_atm = int(match.group(2))

                for item in pending_data:
                    item['timestamp'] = current_timestamp
                    item['atm'] = current_atm
                    data.append(item)

                pending_data = []

    df = pd.DataFrame(data)
    if df.empty:
        return df

    # Pivot the data to have CE and PE side-by-side
    ce_df = df[df['side'] == 'CE'].drop(columns='side').add_prefix('ce_')
    pe_df = df[df['side'] == 'PE'].drop(columns='side').add_prefix('pe_')

    ce_df.rename(columns={'ce_timestamp': 'timestamp', 'ce_atm': 'atm'}, inplace=True)
    pe_df.rename(columns={'pe_timestamp': 'timestamp', 'pe_atm': 'atm'}, inplace=True)

    # Merge on timestamp and atm
    merged = pd.merge(ce_df, pe_df, on=['timestamp', 'atm'], how='outer')
    merged.sort_values('timestamp', inplace=True)

    # Add dummy columns required by DataManager
    merged['ce_symbol'] = 'NIFTY CE DUMMY'
    merged['pe_symbol'] = 'NIFTY PE DUMMY'
    merged['ce_strike'] = merged['atm']
    merged['pe_strike'] = merged['atm']
    merged['strike_price'] = merged['atm']
    merged['spot_price'] = merged['atm'] # Use ATM as proxy for futures if missing

    return merged

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python extract_data_from_logs.py <log_file>")
        sys.exit(1)

    log_file = sys.argv[1]
    if not os.path.exists(log_file):
        print(f"File not found: {log_file}")
        sys.exit(1)

    df = parse_log_file(log_file)
    output_file = "market_data_extracted.csv"
    df.to_csv(output_file, index=False)
    print(f"Extracted {len(df)} rows to {output_file}")
