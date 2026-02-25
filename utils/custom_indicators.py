import pandas as pd
import numpy as np

def calculate_atr(high: pd.Series, low: pd.Series, close: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculates the Average True Range (ATR) using the standard Wilder's Smoothing method.

    Args:
        high (pd.Series): Series of high prices.
        low (pd.Series): Series of low prices.
        close (pd.Series): Series of close prices.
        period (int): The period over which to calculate the ATR.

    Returns:
        pd.Series: A series containing the ATR values.
    """
    # Ensure inputs are numeric, coercing errors to NaN
    high = pd.to_numeric(high, errors='coerce')
    low = pd.to_numeric(low, errors='coerce')
    close = pd.to_numeric(close, errors='coerce')

    # Calculate True Range components
    high_low = high - low
    high_close = abs(high - close.shift(1))
    low_close = abs(low - close.shift(1))

    # Combine into a DataFrame to find the max for each row, ignoring NaNs
    tr_df = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = tr_df.max(axis=1, skipna=True)

    # Calculate ATR using Wilder's Smoothing (which is an EMA with alpha = 1 / period)
    atr = true_range.ewm(alpha=1/period, min_periods=period, adjust=False).mean()

    return atr
