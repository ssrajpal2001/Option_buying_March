# AlgoSoft Trading Bot: Start-to-Finish Workflow

This document describes the complete operational flow of the trading bot, implementing the **Premium Crossover Strategy** with **Trade Reversal (Flip)**.

## 1. Initialization Phase
*   **Configuration**: The bot reads `config/config_trader.ini` to determine enabled instruments (NIFTY, BANKNIFTY, SENSEX), credentials, and strategy parameters.
*   **Authentication**: It authenticates with the **Upstox API** (for market data) and the **Zerodha API** (for trade execution).
*   **Orchestrator Setup**: A dedicated `Orchestrator` is created for each index. Each orchestrator manages its own state, data feed, and logic.
*   **Futures Discovery**: The `DataManager` identifies the current month's Futures instrument key via CSV master files or smart symbol probing. This key is used for strike selection.
*   **Option Chain Loading**: The `AtmManager` fetches all available option contracts for the index and identifies the **Monthly/Weekly Expiry** dates based on configuration.
*   **Initial Subscription**: The bot subscribes to the **Index**, the **Futures**, and **6 Option Contracts** (CE and PE for ATM, ATM+1, and ATM-1 strikes).

## 2. Real-Time Monitoring Phase
*   **WebSocket Feed**: The bot receives live price ticks (LTP) and Greeks for all subscribed instruments.
*   **ATM Tracking**: The At-The-Money (ATM) strike is continuously recalculated using the latest **Futures Price**.
*   **Target Strike Selection**: Every second, the bot evaluates the monitored strikes (ATM +/- 1) and identifies the **Target Strike**—the one where the absolute difference between Call (CE) and Put (PE) premiums is the **smallest**.
*   **1-Minute Candle Aggregation**: The `OHLCAggregator` builds 1-minute OHLC candles for the options of the Target Strike. Crossover detection only proceeds once a full minute of data is finalized.

## 3. Entry Strategy (The Crossover)
*   **Baseline Establishment**: When monitoring begins for a Target Strike, the bot determines which side is currently stronger (e.g., `CE > PE`). This establishes the starting state.
*   **Crossover Detection**: The bot waits for a finalized **1-minute candle close** where the premiums reverse (cross over).
    *   If starting state was `CE > PE`, a **PUT** entry is triggered when `PE_Close > CE_Close`.
    *   If starting state was `PE > CE`, a **CALL** entry is triggered when `CE_Close > PE_Close`.
*   **Execution**: A market order is placed on the **Target Strike** in the direction of the crossover.

## 4. Trade Management & Reversal (The Flip)
*   **Position Tracking**: Once in a trade, the bot tracks live P&L and displays it on the console.
*   **Exit Monitoring**: The bot continues to monitor the premiums of the **current Target Strike** (which updates if the ATM shifts).
*   **The Flip**: If a 1-minute candle close confirms an **opposite crossover** (e.g., you are in a CALL and PE closes higher than CE on the target strike):
    1.  The bot immediately sends an **Exit Request** to close the active trade.
    2.  The bot immediately sends an **Entry Request** for the new direction.
*   **Exit Techniques**: The bot uses one primary exit technique:
    1.  **Opposite Side Crossover**: Triggers a flip when the premium strength shifts to the other side on a finalized 1-minute candle.
*   **No R1/S1**: Legacy Support/Resistance breakout logic (R1 High) is disabled.

## 5. Backtesting (REST-Only)
*   **Synthetic Timeline**: If no local CSV is provided, the bot generates a synthetic 1-minute timeline for the configured `backtest_date`.
*   **Dynamic History**: For every tick in the backtest, the bot dynamically fetches historical OHLC data from the Upstox REST API.
*   **Aggregator Priming**: Aggregators are proactively primed with daily history to ensure the "3-candle confirmation" logic has immediate access to finalized candles.
*   **Analysis Logging**: Detailed CE/PE premium analysis and target strike identification are logged at the `INFO` level to assist in strategy verification.

## 6. Shutdown & Logging
*   **Trade Logging**: Every entry, exit, and reversal is logged into **index-segregated CSV files** (e.g., `trades_NIFTY_user_default_1m_1m_CROSSOVER_FLIP.csv`).
*   **PnL Summary**: The bot maintains a global session summary of total trades and Net P&L across all instruments.
*   **Graceful Exit**: On closing, the bot attempts to close all open positions and save the current state to disk/Redis.
