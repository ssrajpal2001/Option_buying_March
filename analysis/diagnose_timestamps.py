import pandas as pd

def diagnose_csv_timestamps(file_path, start_time, end_time):
    """
    Loads a CSV, parses timestamps, and prints unique timestamps within a given range.
    """
    try:
        # print(f"--- Running Timestamp Diagnostic ---")
        # print(f"Loading data from: {file_path}")

        # Load the CSV data, specifically parsing the 'timestamp' column
        df = pd.read_csv(file_path, parse_dates=['timestamp'], on_bad_lines='warn', engine='python')
        # print(f"Successfully loaded {len(df)} rows.")

        # Ensure the timestamp column is timezone-aware (Asia/Kolkata)
        if df['timestamp'].dt.tz is not None:
            df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Kolkata')
        else:
            df['timestamp'] = df['timestamp'].dt.tz_localize('Asia/Kolkata')
        
        # print(f"Converted timestamps to 'Asia/Kolkata' timezone.")

        # Define the time range to investigate
        start = pd.to_datetime(start_time).tz_localize('Asia/Kolkata')
        end = pd.to_datetime(end_time).tz_localize('Asia/Kolkata')
        
        # print(f"Filtering for timestamps between {start_time} and {end_time}...")

        # Filter the DataFrame to the specified time range
        mask = (df['timestamp'] >= start) & (df['timestamp'] <= end)
        filtered_df = df.loc[mask]

        if filtered_df.empty:
            print("\n[RESULT] No timestamps found in the specified range.")
            print("This suggests the data is missing from the CSV file itself.")
        else:
            unique_timestamps = filtered_df['timestamp'].unique()
            print(f"\n[RESULT] Found {len(unique_timestamps)} unique timestamps in the range:")
            # for ts in unique_timestamps:
            #     print(ts)
            print("\nThis confirms the data IS PRESENT in the CSV. The issue is likely in the bot's processing loop.")

    except FileNotFoundError:
        print(f"\n[ERROR] The file was not found at the specified path: {file_path}")
    except Exception as e:
        print(f"\n[ERROR] An unexpected error occurred: {e}")

if __name__ == "__main__":
    # The path from the user's log file
    CSV_FILE_PATH = "/home/ec2-user/environment/WORKNG/Buying-with-backtest/market_data_2026-01-05_expiry_2026-01-27.csv"
    
    # The time range where the script was observed to skip
    START_TIME_STR = "2026-01-05 13:53:00"
    END_TIME_STR = "2026-01-05 14:07:00"
    
    diagnose_csv_timestamps(CSV_FILE_PATH, START_TIME_STR, END_TIME_STR)
