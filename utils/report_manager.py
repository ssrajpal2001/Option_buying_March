import pandas as pd
from .logger import logger
import os
import glob

class ReportManager:
    def __init__(self):
        # The manager will now find all relevant trade logs automatically
        pass

    def generate_report(self):
        """
        Finds all 'paper_trades_*.csv' files, consolidates them, calculates 
        performance statistics, and prints a formatted report.
        """
        try:
            # Find all trade log files matching the pattern
            trade_log_files = glob.glob("paper_trades_*.csv")
            
            if not trade_log_files:
                logger.info("No trade log files found. No report to generate.")
                return

            # Read and concatenate all found CSVs into a single DataFrame
            df_list = [pd.read_csv(file) for file in trade_log_files]
            df = pd.concat(df_list, ignore_index=True)

            if df.empty or 'PNL' not in df.columns or 'Instrument' not in df.columns:
                logger.info("No trades logged across all accounts. No report to generate.")
                return

            # --- Calculations ---
            total_trades = len(df)
            profitable_trades = df[df['PNL'] > 0]
            loss_making_trades = df[df['PNL'] <= 0]
            
            num_wins = len(profitable_trades)
            num_losses = len(loss_making_trades)
            
            total_pnl = df['PNL'].sum()
            win_rate = (num_wins / total_trades) * 100 if total_trades > 0 else 0
            
            call_trades = df[df['Instrument'].str.contains("CE", na=False)]
            put_trades = df[df['Instrument'].str.contains("PE", na=False)]
            
            num_call_trades = len(call_trades)
            num_put_trades = len(put_trades)

            # --- Report Formatting ---
            report = f"""
            ==================================================
            |         Consolidated Daily Trading Report        |
            ==================================================
            |
            |   Overall Performance:
            |   --------------------
            |   Total Trades Executed: {total_trades}
            |   Profitable Trades:    {num_wins}
            |   Loss-Making Trades:   {num_losses}
            |   Win Rate:             {win_rate:.2f}%
            |
            |   Profit and Loss (PNL):
            |   ----------------------
            |   Total PNL:            {total_pnl:,.2f}
            |
            |   Trade Type Breakdown:
            |   ---------------------
            |   Call Trades:          {num_call_trades}
            |   Put Trades:           {num_put_trades}
            |
            ==================================================
            """

            logger.info(report)

        except Exception as e:
            logger.error(f"Failed to generate consolidated trading report: {e}", exc_info=True)
