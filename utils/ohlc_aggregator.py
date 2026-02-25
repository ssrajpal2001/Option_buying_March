from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, Optional, List
from .logger import logger

class OHLC:
    """Represents an OHLC candle for a specific instrument."""
    def __init__(self, instrument_key: str, timestamp: datetime, price: float, interval_minutes: int = 1):
        self.instrument_key = instrument_key

        # Ensure timestamp is aware before processing
        if timestamp.tzinfo is None:
            import pytz
            timestamp = pytz.timezone('Asia/Kolkata').localize(timestamp)

        # Normalize the timestamp to the start of the interval
        minute_floor = (timestamp.minute // interval_minutes) * interval_minutes
        self.open_time = timestamp.replace(minute=minute_floor, second=0, microsecond=0)
        self.open = price
        self.high = price
        self.low = price
        self.close = price

    def update(self, price: float):
        """Updates the candle with a new price."""
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        self.close = price

    def get_values(self) -> Dict[str, float]:
        """Returns the OHLC values as a dictionary."""
        return {
            'timestamp': self.open_time,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close
        }

class OHLCAggregator:
    """
    Aggregates real-time price ticks into configurable OHLC candles for multiple instruments
    and maintains a history of the last N completed candles for each.
    """
    def __init__(self, interval_minutes: int = 1, history_limit: int = 20, name: str = "Generic"):
        if not isinstance(interval_minutes, int) or interval_minutes <= 0:
            raise ValueError("interval_minutes must be a positive integer.")
        self.interval_minutes = interval_minutes
        self.name = name
        self.current_candles: Dict[str, OHLC] = {}
        self.last_completed_candles: Dict[str, OHLC] = {}
        self.history: Dict[str, List[Dict]] = {}
        self.history_limit = history_limit
        self.current_interval_start: Optional[datetime] = None

    def _get_interval_start(self, timestamp: datetime) -> datetime:
        """Normalizes a timestamp to the start of its interval."""
        minute_floor = (timestamp.minute // self.interval_minutes) * self.interval_minutes
        return timestamp.replace(minute=minute_floor, second=0, microsecond=0)

    def add_tick(self, instrument_key: str, price: float, timestamp: datetime):
        """
        Adds a new price tick for a given instrument.
        If a new interval has started, it finalizes and archives the previous interval's candles.
        """
        # Ensure timestamp is consistent aware datetime to avoid mixed-tz history errors
        if hasattr(timestamp, 'to_pydatetime'):
            timestamp = timestamp.to_pydatetime()

        import pytz
        kolkata = pytz.timezone('Asia/Kolkata')
        if timestamp.tzinfo is None:
            timestamp = kolkata.localize(timestamp)
        else:
            timestamp = timestamp.astimezone(kolkata)

        tick_interval_start = self._get_interval_start(timestamp)

        if self.current_interval_start is None:
            self.current_interval_start = tick_interval_start
            logger.debug(f"OHLC Aggregator [{self.name}|{self.interval_minutes}m]: Initialized first interval start at {tick_interval_start.time()} (First tick at {timestamp.time()})")

        # --- New Interval Transition ---
        if tick_interval_start > self.current_interval_start:
            logger.debug(f"OHLC Aggregator [{self.name}|{self.interval_minutes}m]: Transitioning interval {self.current_interval_start.time()} -> {tick_interval_start.time()} at {timestamp.time()}")
            self._archive_completed_candles()
            self.last_completed_candles = self.current_candles.copy()
            self.current_interval_start = tick_interval_start
            self.current_candles.clear()

        # --- Out-of-order check ---
        if tick_interval_start < self.current_interval_start:
            # Ignore ticks that belong to a previously closed interval
            return

        # --- Update or Create Candle ---
        if instrument_key not in self.current_candles:
            self.current_candles[instrument_key] = OHLC(instrument_key, timestamp, price, self.interval_minutes)
        else:
            self.current_candles[instrument_key].update(price)

    def _archive_completed_candles(self):
        """Moves the completed candles from the 'current' state to the history."""
        for key, candle in self.current_candles.items():
            if key not in self.history:
                self.history[key] = []

            self.history[key].append(candle.get_values())

            if len(self.history[key]) > self.history_limit:
                self.history[key].pop(0)

    def get_historical_ohlc(self, instrument_key: str) -> pd.DataFrame:
        """
        Returns a DataFrame of the historical OHLC data for a specific instrument.
        """
        history_data = self.history.get(instrument_key, [])
        if not history_data:
            # Removed redundant warning as callers typically handle fallback to REST API
            return pd.DataFrame()

        # Create DataFrame from list of dicts
        df = pd.DataFrame(history_data)
        
        if not df.empty:
            # FORCE all values to UTC first to handle mixed awareness gracefully
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)
            df.set_index('timestamp', inplace=True)
            # Then convert to local timezone
            import pytz
            df.index = df.index.tz_convert(pytz.timezone('Asia/Kolkata'))
            
            # Ensure index is unique to avoid 'TypeError: slice vs int' in downstream consumers
            df = df[~df.index.duplicated(keep='last')]

        return df

    def get_last_completed_ohlc(self) -> Dict[str, Dict[str, float]]:
        """
        Returns a dictionary of all last completed OHLC data, typically for logging.
        """
        if not self.last_completed_candles:
            return None
        return {key: candle.get_values() for key, candle in self.last_completed_candles.items()}

    def get_last_completed_ohlc_for_instrument(self, instrument_key: str) -> Optional[Dict[str, float]]:
        """
        Returns the last completed OHLC data for a single, specific instrument.
        """
        candle = self.last_completed_candles.get(instrument_key)
        return candle.get_values() if candle else None

    def get_all_current_ohlc(self) -> Dict[str, Dict[str, float]]:
        """
        Returns a dictionary of all current (incomplete) OHLC data.
        """
        return {key: candle.get_values() for key, candle in self.current_candles.items()}

    def prime_with_history(self, instrument_key: str, historical_df: pd.DataFrame):
        """
        Pre-populates the history for a given instrument with historical OHLC data.
        The DataFrame should have a 'timestamp' index and 'open', 'high', 'low', 'close' columns.
        """
        self.history[instrument_key] = []

        # Ensure the index is localized to Asia/Kolkata to maintain consistency with live ticks
        if historical_df.index.tz is None:
            historical_df.index = historical_df.index.tz_localize('Asia/Kolkata')
        else:
            historical_df.index = historical_df.index.tz_convert('Asia/Kolkata')

        for timestamp, row in historical_df.iterrows():
            # CONVERT pandas Timestamp to aware Python datetime for consistency in history list
            if hasattr(timestamp, 'to_pydatetime'):
                timestamp = timestamp.to_pydatetime()
            candle_data = {
                'timestamp': timestamp,
                'open': row['open'],
                'high': row['high'],
                'low': row['low'],
                'close': row['close']
            }
            self.history[instrument_key].append(candle_data)

            if len(self.history[instrument_key]) > self.history_limit:
                self.history[instrument_key].pop(0)

        # Also update the last_completed_candles attribute
        if not historical_df.empty:
            last_candle = historical_df.iloc[-1]
            self.last_completed_candles[instrument_key] = OHLC(
                instrument_key,
                historical_df.index[-1],
                last_candle['open'],
                self.interval_minutes
            )
            self.last_completed_candles[instrument_key].high = last_candle['high']
            self.last_completed_candles[instrument_key].low = last_candle['low']
            self.last_completed_candles[instrument_key].close = last_candle['close']

    def clear(self):
        """Clears all state and history for a fresh start."""
        self.current_candles.clear()
        self.last_completed_candles.clear()
        self.history.clear()
        self.current_interval_start = None
