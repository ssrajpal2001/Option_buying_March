# V3 Strategy Enhancement Proposals (Updated)

This document outlines the proposed logic updates for Version 3 (V3) of the trading bot. All features will be configurable via the JSON configuration file and the Web UI.

## 1. VWAP Slope Exit (Trailing Exit)
This logic protects profits by monitoring the "lowest point" reached by the Combined VWAP during an active trade.

*   **Logic:** The bot tracks the minimum value of the **Combined VWAP** (CE VWAP + PE VWAP) from the moment the trade is entered.
*   **Trigger:** If the current Combined VWAP rises by **1% or more** from its recorded lowest point, the bot will exit the position.
*   **Purpose:** To capture reversals in the premium decay trend.

## 2. Updated LTP Balanced Entry Logic
While balancing logic currently exists, it is being updated to follow a "strictly lower" price rule for the second leg to ensure a conservative entry.

*   **Updated Logic:**
    1.  The bot identifies the initial candidates for CE and PE.
    2.  It identifies which side has the **lower LTP**.
    3.  It executes the sell order for the lower-priced leg first.
    4.  For the opposite side, it selects the strike that is **nearest to, but strictly less than**, the price of the first leg.
*   **Comparison:**
    *   *Old Logic:* Picked the closest strike where LTP >= first leg.
    *   *New Logic:* Picks the closest strike where LTP < first leg.
*   **Example:** If CE is ₹60 and the closest PE strikes are ₹58 and ₹62, the bot will now select the **₹58** strike.

## 3. Multi-Strike Re-entry Scan (ATM ± Range)
Instead of only monitoring the ATM, the bot will now scan a range of straddles to find the best re-entry opportunity.

*   **Range:** Configurable offset from ATM (e.g., ATM ± 2 or ATM ± 4).
*   **Entry Criteria (on 5-minute timeframe):**
    *   **VWAP Crossover:** The combined 5m candle must have (**Open > VWAP** OR **High > VWAP**) AND (**Close <= VWAP**). This signifies a "rejection" or "pullback" below the VWAP.
    *   **RSI Filter:** Combined RSI must be below 50.
*   **Selection:** If multiple strike combinations pass the criteria simultaneously, the bot will enter the one with the **lowest total premium** (least risk).

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
