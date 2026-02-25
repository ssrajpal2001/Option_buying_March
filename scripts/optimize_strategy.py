import subprocess
import itertools
import pandas as pd
import os
import re

def run_backtest(entry_occurrences, exit_occurrences):
    """Runs the bot with specific parameters and returns the total PnL."""
    config_file = 'config/config_trader.ini'

    # Create a temporary config for this run
    temp_config = f'config/config_opt_{entry_occurrences}_{exit_occurrences}.ini'
    with open(config_file, 'r') as f:
        content = f.read()

    # Replace parameters
    content = re.sub(r'entry_vwap_slope_occurrences\s*=\s*\d+', f'entry_vwap_slope_occurrences = {entry_occurrences}', content)
    content = re.sub(r'exit_vwap_slope_occurrences\s*=\s*\d+', f'exit_vwap_slope_occurrences = {exit_occurrences}', content)

    # Ensure backtest is enabled and using the extracted CSV
    content = re.sub(r'backtest_enabled\s*=\s*\w+', 'backtest_enabled = true', content)
    content = re.sub(r'backtest_csv_path\s*=\s*[\w\.\-]+', 'backtest_csv_path = market_data_extracted.csv', content)

    with open(temp_config, 'w') as f:
        f.write(content)

    print(f"Running backtest: EntryOcc={entry_occurrences}, ExitOcc={exit_occurrences}...")

    # Run the bot
    try:
        # Redirect output to a log file
        log_file = f"logs/opt_{entry_occurrences}_{exit_occurrences}.log"
        os.makedirs("logs", exist_ok=True)
        with open(log_file, "w") as f:
            subprocess.run(["python", "main.py", "--config", temp_config], stdout=f, stderr=subprocess.STDOUT, timeout=300)
    except Exception as e:
        print(f"Error running backtest: {e}")
        return None

    # Parse the log for total PnL
    total_pnl = 0
    trade_count = 0
    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            lines = f.readlines()
            for line in reversed(lines):
                if "TOTAL SESSION PNL:" in line:
                    match = re.search(r"TOTAL SESSION PNL: ([\-\d\.]+)", line)
                    if match:
                        total_pnl = float(match.group(1))
                        break
            for line in lines:
                if "Closed" in line and "trade" in line:
                    trade_count += 1

    return total_pnl, trade_count

def main():
    entry_range = [1, 2]
    exit_range = [1, 2, 3]

    results = []

    for entry_occ, exit_occ in itertools.product(entry_range, exit_range):
        pnl, count = run_backtest(entry_occ, exit_occ)
        results.append({
            'entry_vwap_slope_occurrences': entry_occ,
            'exit_vwap_slope_occurrences': exit_occ,
            'total_pnl': pnl,
            'trade_count': count
        })

    df = pd.DataFrame(results)
    df.sort_values('total_pnl', ascending=False, inplace=True)
    print("\n--- Optimization Results ---")
    print(df.to_string(index=False))
    df.to_csv("optimization_results.csv", index=False)

if __name__ == "__main__":
    main()
