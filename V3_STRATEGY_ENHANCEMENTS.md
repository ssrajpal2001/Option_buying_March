# V3 Strategy Enhancement Proposals (Updated)

This document outlines the proposed logic updates for Version 3 (V3) of the trading bot. All features will be configurable via the JSON configuration file and the Web UI.

## 1. VWAP Slope Exit (Trailing Exit)
This logic protects profits by monitoring the "lowest point" reached by the Combined VWAP during an active trade.

*   **Logic:** The bot tracks the minimum value of the **Combined VWAP** (CE VWAP + PE VWAP) from the moment the trade is entered.
*   **Trigger:** If the current Combined VWAP rises by **1% or more** from its recorded lowest point, the bot will exit the position.
*   **Purpose:** To capture reversals in the premium decay trend.

## 2. Initial Entry Logic (09:16:05 AM)
The very first trade of the day bypasses the scanner and crossover filters. It uses direct ATM/ITM selection for rapid execution.

*   **Strike Selection:**
    1.  Identifies the **ATM** based on Spot.
    2.  Finds the side (CE or PE) with the **Lower LTP**.
    3.  **Threshold Rule:** If that Lower LTP is **< 50**, the bot moves **1 strike ITM** to ensure sufficient premium.
*   **Execution:**
    1.  The bot sells the Lower LTP side first.
    2.  **Strict Balancing:** For the other side, it selects the strike that is **nearest to, but strictly less than**, the price of the first leg sold.
*   **Transition:** Once this initial position is closed, the bot switches to the "Multi-Strike Scanner" for all subsequent re-entries.

## 3. Multi-Strike Re-entry Scan (ATM ± Range)
Instead of only monitoring the ATM, the bot will now scan a range of straddles to find the best re-entry opportunity.

*   **Range:** Configurable offset from ATM (e.g., ATM ± 2 or ATM ± 4).
*   **Entry Criteria (on 5-minute timeframe):**
    *   **VWAP Crossover:** The combined 5m candle must have (**Open > VWAP** OR **High > VWAP**) AND (**Close <= VWAP**). This signifies a "rejection" or "pullback" below the VWAP.
    *   **RSI Filter:** Combined RSI must be below 50.
*   **Selection:** If multiple strike combinations pass the criteria simultaneously, the bot will enter the one with the strike **nearest to the current ATM** (Spot price).
*   **Execution Behavior:**
    *   **Indicator & Slope Exits:** Result in a **Full Exit** and a mandatory **60-second cooldown** before the scanner resumes.
    *   **Target/Ratio/LTP Exits:** Support **Smart Rolling** (keeping overlapping strikes) to save brokerage if re-entry conditions are met immediately.

## 4. Advanced Profit Locking (Scalable TSL)
A more granular trailing stop-loss based on realized/floating profit.

*   **Base Trigger:** For every 1 lot, if profit reaches **₹1,000**, the bot "locks" **₹200** as guaranteed profit.
*   **Trailing Step:** For every subsequent increase of **₹250** in profit, the lock value increases by **₹200**.
*   **Scalability:** These values are automatically multiplied by the number of lots traded.
    *   *Example (10 lots):* Lock ₹2,000 when profit reaches ₹10,000. For every ₹2,500 more profit, increase lock by ₹2,000.

## 5. UI & Configuration
*   All new logics will have **On/Off switches** in the `strategy_logic.json`.
*   The **Web Dashboard** will be updated to include toggles and input fields for:
    *   VWAP Slope Exit %
    *   Re-entry Strike Range (Offset)
    *   TSL Profit Thresholds and Lock Values
    *   Enable/Disable the new LTP Balanced Logic
