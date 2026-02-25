import csv
from datetime import datetime
import os
from threading import Lock

class TradeLogger:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(TradeLogger, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
        with self._lock:
            if self._initialized:
                return
            self.log_files = {}  # Stores file handles and writers for each instrument
            self.lock = Lock()
            self.config_manager = None
            self._initialized = True

    def setup(self, config_manager):
        """Initializes the logger with configuration for dynamic filename generation."""
        with self.lock:
            self.config_manager = config_manager

    def _get_writer_for_instrument(self, instrument_name, user_id=None):
        """
        Retrieves or creates the CSV writer and file handle for a specific instrument and user.
        Manages a dictionary of file handles to avoid re-opening files.
        """
        instrument_name = instrument_name.upper()
        # Key includes user_id for multi-tenant isolation
        log_key = f"{instrument_name}_{user_id}" if user_id else instrument_name

        if log_key not in self.log_files:
            # Dynamic filename based on strategy settings
            entry_tf = "1"
            exit_tf = "1"
            exit_logic = "CROSSOVER_FLIP"

            if self.config_manager:
                entry_tf = self.config_manager.get('settings', 'entry_timeframe_minutes', fallback='1')
                exit_tf = self.config_manager.get('settings', 'exit_timeframe_minutes', fallback='1')
                exit_logic = self.config_manager.get('settings', 'exit_logic_type', fallback='CROSSOVER_FLIP')

            user_suffix = f"_user_{user_id}" if user_id else ""
            filename = f"trades_{instrument_name}{user_suffix}_{entry_tf}m_{exit_tf}m_{exit_logic}.csv"
            file_exists = os.path.exists(filename) and os.path.getsize(filename) > 0

            # Open the file in append mode and keep it open.
            file_handle = open(filename, 'a', newline='')
            writer = csv.writer(file_handle)

            if not file_exists:
                writer.writerow([
                    "Timestamp", "Broker", "InstrumentSymbol", "TradeType",
                    "Price", "PNL", "Reason", "StrategyLog"
                ])
                file_handle.flush()

            self.log_files[log_key] = {'handle': file_handle, 'writer': writer}

        return self.log_files[log_key]['writer'], self.log_files[log_key]['handle']

    def log_entry(self, broker, instrument_name, instrument_symbol, trade_type, price, strategy_log="", user_id=None):
        """Logs a trade entry into the correct instrument-specific CSV file."""
        with self.lock:
            writer, handle = self._get_writer_for_instrument(instrument_name, user_id)
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                broker,
                instrument_symbol,
                trade_type,  # e.g., 'CALL' or 'PUT'
                price,
                "",  # PNL is not applicable on entry
                "",   # Reason is not applicable on entry
                strategy_log
            ])
            handle.flush()

    def log_exit(self, broker, instrument_name, instrument_symbol, trade_type, price, pnl, reason, strategy_log="", user_id=None):
        """Logs a trade exit into the correct instrument-specific CSV file."""
        with self.lock:
            writer, handle = self._get_writer_for_instrument(instrument_name, user_id)
            writer.writerow([
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                broker,
                instrument_symbol,
                trade_type, # e.g., 'EXIT_CALL'
                price,
                f"{pnl:.2f}",
                reason,
                strategy_log
            ])
            handle.flush()

    def shutdown(self):
        """Closes all open file handles to ensure all data is written to disk."""
        with self.lock:
            for instrument, data in self.log_files.items():
                if data.get('handle'):
                    data['handle'].close()
            self.log_files.clear()
