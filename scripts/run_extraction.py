import sys
import os
import pandas as pd
sys.path.append(os.getcwd())
from tools.extract_data_from_logs import parse_log_file

def run():
    # Use app.log by default, or take from args
    log_file = sys.argv[1] if len(sys.argv) > 1 else 'app.log'

    if not os.path.exists(log_file):
        print(f"Log file {log_file} not found.")
        return

    print(f"Extracting data from {log_file}...")
    df = parse_log_file(log_file)

    if not df.empty:
        output_file = 'market_data_extracted.csv'
        df.to_csv(output_file, index=False)
        print(f"Successfully extracted {len(df)} records to {output_file}")
    else:
        print("No data found in logs matching the extraction patterns.")

if __name__ == "__main__":
    run()
