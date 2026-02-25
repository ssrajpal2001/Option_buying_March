
import pandas as pd
import requests
import io

def analyze_ltp_differences():
    """
    Downloads, processes a CSV file from a URL, and prints timestamps
    where the difference between CE and PE LTPs is less than 2% of their mean.

    Args:
        csv_url (str): The URL of the CSV file to analyze.
    """
    
    try:
        CSV_FILE_PATH = "/home/ec2-user/environment/WORKNG/Buying-with-backtest/market_data_2026-01-05_expiry_2026-01-27.csv"
        df = pd.read_csv(CSV_FILE_PATH, parse_dates=['timestamp'], on_bad_lines='warn', engine='python')
            # print(f"Successfully loaded {len(df)} rows.")
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    # Ensure required columns exist
    required_columns = ['timestamp', 'ce_ltp', 'pe_ltp','spot_price','ce_strike']
    if not all(col in df.columns for col in required_columns):
        print(f"CSV is missing one of the required columns: {required_columns}")
        return

        # print("\\n--- Timestamps where CE/PE LTP difference is < 2% of mean ---")
    found_count = 0
    for index, row in df.iterrows():
        timestamp = row['timestamp']
        ce_ltp = row['ce_ltp']
        pe_ltp = row['pe_ltp']
        spot_price=row['spot_price']
        ce_strike=row['ce_strike']

        # Ensure LTPs are numeric and not NaN
        if pd.isna(ce_ltp) or pd.isna(pe_ltp):
            continue

        # Calculate the mean of the two LTPs
        mean_ltp = (ce_ltp + pe_ltp) / 2

        # Avoid division by zero
        if mean_ltp == 0:
            continue

        # Calculate the absolute difference and then the percentage of the mean
        abs_diff = abs(ce_ltp - pe_ltp)
        diff_percent_of_mean = abs_diff / mean_ltp

        # Check if the difference is less than 2%
        if diff_percent_of_mean < 0.02:
            found_count += 1
                # print(f"Event found at timestamp: {timestamp} (CE: {ce_ltp:.2f}, PE: {pe_ltp:.2f},Spot price {spot_price:.2f},ce strike price {ce_strike:.2f})")
            

    if found_count == 0:
        print("No events found that match the criteria.")
    else:
        print(f"\\nAnalysis complete. Found {found_count} matching events.")


if __name__ == "__main__":
    # URL provided by the user
    analyze_ltp_differences()
