# Crossover Strategy Logic Explanation

This document explains the new crossover-based trading strategy implemented in the bot.

## 1. Instrument Selection & Subscriptions
The bot monitors the following instruments:
*   **Futures Instrument**: Used to determine the current ATM strike.
*   **Options (6 Instruments)**: CE and PE options for 3 strikes (ATM, ATM+1, ATM-1).

Subscriptions are automatically updated as the ATM strike moves.

## 2. Target Strike Selection
Every minute, the bot evaluates the 3 monitored strikes (ATM, ATM+1, ATM-1).
The **Target Strike** is selected as the one where the absolute difference between the Call (CE) and Put (PE) prices is the **smallest**. This is typically where price parity is strongest.

## 3. Entry Logic (CE/PE Crossover)
The strategy focuses on identifying crossovers in the premiums of the monitored Target Strike.

### Initial State Detection:
When monitoring starts for a strike:
*   The bot identifies which side is currently higher (e.g., `CE > PE` or `PE > CE`).
*   This establishes the "baseline" state.

### Crossover Trigger:
The bot waits for a specific sequence of finalized **1-minute candle closes** to trigger an entry. A simple crossover is no longer sufficient; the premiums must follow one of these patterns:

**FOR CALL (CE) SIDE BUY:**
1.  **PE-CE-CE**: `PE > CE`, then `CE > PE`, then `CE > PE`.

**FOR PUT (PE) SIDE BUY:**
1.  **CE-PE-PE**: `CE > PE`, then `PE > CE`, then `PE > CE`.

**Note:** The trigger requires finalized 1-minute candle close confirmations for the entire sequence. Spikes or wicks are ignored.

### Exit Patterns:
The bot also monitors for specific sequences to trigger an early exit before an opposite entry is confirmed:

**FOR CALL (CE) SIDE EXIT:**
- **PE-CE-PE**: `PE > CE`, then `CE > PE`, then `PE > CE`.

**FOR PUT (PE) SIDE EXIT:**
- **CE-PE-CE**: `CE > PE`, then `PE > CE`, then `CE > PE`.

## 4. Trade Execution
*   When a trigger occurs, the bot executes a trade on the **same Target Strike** where the crossover was detected.
*   This ensures that the entry is taken on the most liquid and price-balanced strike.

## 5. Exit & Reversal Logic (Flip)
The bot uses a single, robust exit and reversal technique based on the **Opposite Side Crossover**.

### Reversal Mechanism:
*   While in a trade, the bot continues to monitor the CE/PE prices of the current **Target Strike**.
*   **Early Exit**: If an **Exit Pattern** (3-candle sequence) is completed for the active side, the trade is exited immediately, and the bot stays flat until a new entry pattern is confirmed.
*   **Direct Reversal (Flip)**: If an **opposite side entry pattern** (4 or 5-candle sequence) is completed while still in a trade, the bot exits the current trade and enters the opposite side in one step.
*   This "Flip" logic ensures the bot is always positioned in the direction of the latest premium crossover.

## 6. Exit Techniques
The bot utilizes one primary exit mechanism:
*   **Opposite Crossover (Flip)**: As described in section 5, the bot will exit and flip the position if a crossover occurs on the opposite side.

## 7. Continuous Monitoring
The bot constantly evaluates the ATM +/- 1 strikes to update the **Target Strike**. Even if the ATM moves, the bot tracks the crossover on the most current Target Strike to decide when to flip the position.

---
**Strategy Summary:** No R1 High or S1 Low levels are used. The entire logic is driven by the relative price action (crossover) of Call and Put premiums at the most balanced strike.
