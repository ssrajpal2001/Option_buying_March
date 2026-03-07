import csv
import os
import datetime
from utils.logger import logger

class DataRecorder:
    """
    Records market data for ATM +/- 10 strikes to a date-named CSV file.
    Format: timestamp,spot_price,atm_strike,ce_strike,ce_symbol,ce_ltp,ce_delta,ce_vega,ce_theta,ce_gamma,ce_open,ce_high,ce_low,ce_close,pe_strike,pe_symbol,pe_ltp,pe_delta,pe_vega,pe_theta,pe_gamma,pe_open,pe_high,pe_low,pe_close
    """
    def __init__(self, instrument_name):
        self.instrument_name = instrument_name
        # Format: market_data_INSTRUMENT_YYYY-MM-DD.csv
        # Including instrument name to prevent data mixing in multi-instrument mode
        self.filename = f"market_data_{instrument_name}_{datetime.date.today().isoformat()}.csv"
        self.filepath = os.path.join(os.getcwd(), self.filename)
        self.headers = [
            'timestamp', 'spot_price', 'index_price', 'atm_strike',
            'ce_strike', 'ce_symbol', 'ce_ltp', 'ce_delta', 'ce_vega', 'ce_theta', 'ce_gamma', 'ce_open', 'ce_high', 'ce_low', 'ce_close', 'ce_oi',
            'pe_strike', 'pe_symbol', 'pe_ltp', 'pe_delta', 'pe_vega', 'pe_theta', 'pe_gamma', 'pe_open', 'pe_high', 'pe_low', 'pe_close', 'pe_oi'
        ]
        self._initialize_csv()

    def _initialize_csv(self):
        if not os.path.exists(self.filepath):
            with open(self.filepath, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(self.headers)
            logger.info(f"Initialized data recorder: {self.filepath}")

    def record_ticks(self, timestamp, spot_price, index_price, atm_strike, watchlist_data):
        """
        Records a batch of strikes for a single timestamp.
        watchlist_data: dict of strike -> {ce_ltp, ce_delta, ce_symbol, pe_ltp, pe_delta, pe_symbol, ...}
        """
        try:
            with open(self.filepath, 'a', newline='') as f:
                writer = csv.writer(f)
                for strike, data in watchlist_data.items():
                    row = [
                        timestamp.isoformat(),
                        spot_price,
                        index_price,
                        atm_strike,
                        # CE side
                        strike,
                        data.get('ce_symbol', ''),
                        data.get('ce_ltp', ''),
                        data.get('ce_delta', ''),
                        data.get('ce_vega', ''),
                        data.get('ce_theta', ''),
                        data.get('ce_gamma', ''),
                        data.get('ce_open', ''),
                        data.get('ce_high', ''),
                        data.get('ce_low', ''),
                        data.get('ce_close', ''),
                        data.get('ce_oi', ''),
                        # PE side
                        strike,
                        data.get('pe_symbol', ''),
                        data.get('pe_ltp', ''),
                        data.get('pe_delta', ''),
                        data.get('pe_vega', ''),
                        data.get('pe_theta', ''),
                        data.get('pe_gamma', ''),
                        data.get('pe_open', ''),
                        data.get('pe_high', ''),
                        data.get('pe_low', ''),
                        data.get('pe_close', ''),
                        data.get('pe_oi', '')
                    ]
                    writer.writerow(row)
        except Exception as e:
            logger.error(f"Failed to record ticks to CSV: {e}")

    def record_atp_snapshot(self, minute_ts, instrument_key, strike, side, atp, ltp, spot_price, futures_price, oi=None):
        """
        Records the closed-minute ATP snapshot for a single instrument.
        Called at minute rollover so values are frozen (not mid-candle).
        File: atp_data_{instrument}_{date}.csv
        """
        try:
            if not hasattr(self, '_atp_filepath'):
                atp_filename = f"atp_data_{self.instrument_name}_{datetime.date.today().isoformat()}.csv"
                self._atp_filepath = os.path.join(os.getcwd(), atp_filename)
                if not os.path.exists(self._atp_filepath):
                    with open(self._atp_filepath, 'w', newline='') as f:
                        csv.writer(f).writerow([
                            'minute_ts', 'instrument_key', 'strike', 'side',
                            'atp', 'ltp', 'spot_price', 'futures_price', 'oi'
                        ])
                    logger.info(f"Initialized ATP recorder: {self._atp_filepath}")

            with open(self._atp_filepath, 'a', newline='') as f:
                csv.writer(f).writerow([
                    minute_ts.isoformat() if hasattr(minute_ts, 'isoformat') else str(minute_ts),
                    instrument_key, strike, side, atp, ltp, spot_price, futures_price, oi
                ])
        except Exception as e:
            logger.error(f"Failed to record ATP snapshot to CSV: {e}")
