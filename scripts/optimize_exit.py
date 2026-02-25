import subprocess
import pandas as pd
import os
import re
import json

def run_backtest(exit_formula):
    """Runs the bot with a specific exit formula and returns the total PnL."""
    logic_file = 'config/strategy_logic.json'

    # Backup original logic
    with open(logic_file, 'r') as f:
        original_logic = json.load(f)

    # Update logic with the formula to test
    test_logic = original_logic.copy()
    test_logic['NIFTY']['buy']['exit_formula'] = exit_formula

    with open(logic_file, 'w') as f:
        json.dump(test_logic, f, indent=2)

    print(f"Running backtest with Exit Formula: {exit_formula}...")

    # Run the bot
    try:
        # Redirect output to a log file
        safe_name = exit_formula.replace(" ", "_").replace("(", "").replace(")", "").replace("|", "OR").replace("&", "AND")
        log_file = f"logs/opt_exit_{safe_name}.log"
        os.makedirs("logs", exist_ok=True)
        with open(log_file, "w") as f:
            # Note: Using python3
            subprocess.run(["python3", "main.py"], stdout=f, stderr=subprocess.STDOUT, timeout=300)
    except Exception as e:
        print(f"Error running backtest: {e}")
        # Restore original logic
        with open(logic_file, 'w') as f:
            json.dump(original_logic, f, indent=2)
        return None

    # Restore original logic
    with open(logic_file, 'w') as f:
        json.dump(original_logic, f, indent=2)

    # Parse the log for total PnL
    total_pnl = 0
    trade_count = 0
    exit_reason = "N/A"

    if os.path.exists(log_file):
        with open(log_file, "r") as f:
            lines = f.readlines()
            for line in reversed(lines):
                if "Total PnL:" in line:
                    match = re.search(r"Total PnL: ([\-\d\.]+)", line)
                    if match:
                        total_pnl = float(match.group(1))
                if "Reason:" in line and "Exited" in line:
                    exit_reason = line.split("Reason:")[-1].strip()
                    break
            for line in lines:
                if "Entered CALL trade" in line or "Entered PUT trade" in line:
                    trade_count += 1

    return total_pnl, trade_count, exit_reason

def main():
    formulas = [
        "s1_low",
        "s1_double_drop",
        "atr_tsl",
        "vwap_slope",
        "tsl",
        "s1_confirm",
        "r1_stagnation",
        "r1_low_breach",
        "s1_double_drop OR atr_tsl",
        "s1_low OR vwap_slope",
        "s1_double_drop OR vwap_slope OR r1_stagnation"
    ]

    results = []

    for formula in formulas:
        res = run_backtest(formula)
        if res:
            pnl, count, reason = res
            results.append({
                'exit_formula': formula,
                'total_pnl': pnl,
                'trade_count': count,
                'last_exit_reason': reason
            })

    df = pd.DataFrame(results)
    df.sort_values('total_pnl', ascending=False, inplace=True)
    print("\n--- Exit Optimization Results ---")
    print(df.to_string(index=False))
    df.to_csv("exit_optimization_results.csv", index=False)

if __name__ == "__main__":
    main()
