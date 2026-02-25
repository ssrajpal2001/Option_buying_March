# VWAP Peak Calculation & Hot Handover Logic

This document explains how the trading bot calculates and maintains the **VWAP Peak** (for BUY) or **VWAP Trough** (for SELL) used in exit conditions, especially during strike switches.

## 1. Real-Time Tracking
While a trade is active, the `PositionManager` continuously monitors the VWAP of the strike being monitored (the `s1_monitoring_strike`).

- **For BUY (CALL) trades**: The system maintains a `vwap_peak`. Every tick, it calculates the current daily VWAP. If `Current_VWAP > vwap_peak`, the peak is updated.
- **For SELL (PUT) trades**: The system maintains a `vwap_trough`. Every tick, it calculates the current daily VWAP. If `Current_VWAP < vwap_trough`, the trough is updated.

## 2. Hot Handover (Strike Switch Logic)
When the target strike changes (e.g., from 25700 to 25750), the bot must preserve the "momentum context" of the trade. Since the new strike has a different price history, the bot performs a **Hot Handover**:

1.  **Historical Backfill**: The bot fetches 1-minute historical OHLC data for the **NEW** strike, starting from the exact time the trade was entered (`entry_timestamp`).
2.  **Daily VWAP Reconstruction**: For every 1-minute candle between the entry time and the current time, the bot calculates the **Cumulative Daily VWAP**. This ensures the VWAP calculation includes all volume from 09:15 AM of the current session. The bot explicitly filters for today's data starting from 09:15 AM to ensure multi-day historical data (if present in the cache) does not pollute the intraday peak.
3.  **Peak/Trough Identification**:
    -   **VWAP Peak**: The bot looks at these reconstructed VWAP values from **Entry Time** up to **Current Time - 1 Minute**. It selects the maximum (for BUY) or minimum (for SELL) value from this historical window.
    -   *Note: We exclude the current minute for the VWAP reference to ensure we are anchoring against finalized momentum rather than a live, fluctuating candle.*
    -   **LTP Peak**: The bot also updates the `peak_price` (Highest High) or `min_price` (Lowest Low) by looking at the actual prices reached during the full trade duration (including the current minute).

## 3. Exit Evaluation (Regression Analysis)
The `ExitEvaluator` uses the established peak as a stable reference point for the `vwap_slope` exit.

-   **Formula**: `Drawdown % = (Current_VWAP - Peak_VWAP) / Peak_VWAP`
-   **Execution**: If the `Drawdown %` crosses the configured threshold (e.g., `-0.02` for a 2% drop from the peak), the trade is exited immediately.

This approach prevents "momentum erosion" where a strike might stay above its entry price but lose its relative strength compared to its own best performance during the trade.

## 4. Logging and Transparency
During backtests and live management, the bot logs the following to aid in verification:
-   `V2: Hot Handover [CALL] VWAP Peak updated to 394.18 (Calculated from 5 finalized candles between ...)`
-   `V2 MGMT: [CALL] VWAP EXIT CHECK: Current 390.10 vs Peak 394.18 | Drawdown: -1.03% | Threshold: -2.00%`
