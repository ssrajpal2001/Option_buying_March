import asyncio
import pandas as pd
from utils.logger import logger
import os
import datetime

class CSVDataFeeder:
    def __init__(self, file_path='tick_data_log.csv', contract_map=None, config_manager=None):
        csv_path = os.path.join(self._get_project_root(), file_path)
        self.file_path = csv_path
        self.data = None
        self.backtest_date = None
        self.current_index = 0
        self.contract_map = contract_map or {}
        self.trade_orchestrator = None
        self.state_manager = None
        self.atm_manager = None
        self.config_manager = config_manager

    def _get_project_root(self):
        """Helper to get the project's root directory."""
        return os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

    def load_data(self, target_date=None):
        """Loads and reshapes the backtest data. Generates synthetic timestamps if CSV is missing."""
        try:
            if target_date:
                self.backtest_date = target_date if isinstance(target_date, datetime.date) else datetime.datetime.strptime(target_date, '%Y-%m-%d').date()

            if not os.path.isfile(self.file_path):
                logger.warning(f"CSV file NOT FOUND or is a directory at {self.file_path}. Generating synthetic 1-minute timestamps for API-only backtest.")

                # 1. Try to get date from config (if not provided as argument)
                if not self.backtest_date:
                    config_date = self.config_manager.get('settings', 'backtest_date', fallback=None)
                    if config_date:
                        try:
                            self.backtest_date = datetime.datetime.strptime(config_date, '%Y-%m-%d').date()
                            logger.info(f"Using backtest date from config: {self.backtest_date}")
                        except ValueError:
                            logger.error(f"Invalid backtest_date format in config: {config_date}. Expected YYYY-MM-DD.")

                # 2. Attempt to extract date from filename (e.g. market_data_2026-01-20.csv)
                if not self.backtest_date:
                    import re
                    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', self.file_path)
                    if date_match:
                        self.backtest_date = datetime.datetime.strptime(date_match.group(1), '%Y-%m-%d').date()
                    else:
                        # 3. Fallback to yesterday
                        self.backtest_date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()

                start_ts = datetime.datetime.combine(self.backtest_date, datetime.time(9, 15))
                end_ts = datetime.datetime.combine(self.backtest_date, datetime.time(15, 30))

                import pytz
                kolkata = pytz.timezone('Asia/Kolkata')
                start_ts = kolkata.localize(start_ts)
                end_ts = kolkata.localize(end_ts)

                timestamps = pd.date_range(start=start_ts, end=end_ts, freq='1min')
                self.data = pd.DataFrame({'timestamp': timestamps})
                # Add dummy columns to satisfy existing logic
                self.data['strike_price'] = 0
                self.data['spot_price'] = 0
                logger.info(f"Generated {len(self.data)} synthetic timestamps for {self.backtest_date}")
                return

            long_df = pd.read_csv(self.file_path, parse_dates=['timestamp'], on_bad_lines='warn', engine='python')
            self.data = long_df

            # Robust timezone handling
            if self.data['timestamp'].dt.tz is not None:
                self.data['timestamp'] = self.data['timestamp'].dt.tz_convert('Asia/Kolkata')
            else:
                self.data['timestamp'] = self.data['timestamp'].dt.tz_localize('Asia/Kolkata')

            self.data.sort_values(by='timestamp', inplace=True)
            
            # --- Filter data for target_date if provided ---
            if target_date:
                target_dt_obj = target_date if isinstance(target_date, datetime.date) else pd.to_datetime(target_date).date()
                self.data = self.data[self.data['timestamp'].dt.date == target_dt_obj]
                self.backtest_date = target_dt_obj
                logger.info(f"Filtered CSV data to {len(self.data)} rows for {target_dt_obj}")

            # --- Set the backtest_date from the first row if not already set ---
            if not self.backtest_date and not self.data.empty:
                self.backtest_date = self.data['timestamp'].iloc[0].date()

            if 'ce_strike' in self.data.columns:
                self.data['strike_price'] = self.data['ce_strike']
            elif 'strike_price' not in self.data.columns:
                # If neither ce_strike nor strike_price exist, it's likely synthetic or minimal
                self.data['strike_price'] = 0

            logger.info(f"Successfully loaded and processed {len(self.data)} rows from {self.file_path}")

        except Exception as e:
            logger.error(f"Failed to load or process backtest CSV: {e}")
            raise

    async def start_feed(self): pass
    async def process_data_for_timestamp(self, timestamp, row): pass
    async def close(self): pass
    def subscribe(self, *args, **kwargs): pass
    def unsubscribe(self, *args, **kwargs): pass
    def register_message_handler(self, handler): pass
