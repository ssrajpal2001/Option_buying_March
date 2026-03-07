from utils.logger import logger
from utils.trade_logger import TradeLogger
import pandas as pd

class BacktestPnLTracker:
    def __init__(self, instrument_name, config_manager):
        self.instrument_name = instrument_name
        self.config_manager = config_manager
        self.active_trades = [] # List of active trade dicts
        self.trade_history = []
        self.trade_logger = TradeLogger()

    def is_trade_active(self, side=None, instrument_key=None):
        for trade in self.active_trades:
            side_match = (side is None or trade['side'] == side)
            key_match = (instrument_key is None or trade['instrument_key'] == instrument_key)
            if side_match and key_match:
                return True
        return False

    def get_active_trade(self, side, instrument_key=None):
        for trade in self.active_trades:
            if trade['side'] == side:
                if instrument_key is None or trade['instrument_key'] == instrument_key:
                    return trade
        return None

    def enter_trade(self, side, instrument_key, entry_price, timestamp, strike_price, contract, strategy_log="", user_id=None, entry_type='BUY', quantity=1):
        # Allow simultaneous trades as long as the instrument_key is different
        if self.is_trade_active(side, instrument_key):
            logger.warning(f"[{self.instrument_name}] Cannot enter duplicate {side} trade for {instrument_key}.")
            return False

        lot_size = contract.lot_size if contract and hasattr(contract, 'lot_size') else 1

        trade = {
            'instrument_key': instrument_key,
            'entry_price': entry_price,
            'exit_price': None,
            'entry_timestamp': timestamp,
            'exit_timestamp': None,
            'pnl': 0,
            'status': 'ACTIVE',
            'side': side,
            'strike_price': strike_price,
            'contract': contract,
            'strategy_log': strategy_log,
            'entry_type': entry_type,
            'lot_size': lot_size,
            'quantity': quantity,
        }

        self.active_trades.append(trade)

        logger.info(f"[{self.instrument_name}] Entered {side} ({entry_type}) trade on {instrument_key} at {entry_price:.2f} (lot={lot_size} qty={quantity})")
        self.trade_logger.log_entry("Backtest", self.instrument_name, instrument_key, side, entry_price, strategy_log, user_id=user_id)
        return True

    def update_pnl(self, side, current_ltp, instrument_key=None):
        trade = self.get_active_trade(side, instrument_key)
        if not trade:
            return

        mult = trade.get('lot_size', 1) * trade.get('quantity', 1)
        entry_type = trade.get('entry_type', 'BUY')
        if entry_type == 'SELL':
            trade['pnl'] = (trade['entry_price'] - current_ltp) * mult
        else:
            trade['pnl'] = (current_ltp - trade['entry_price']) * mult
        trade['current_ltp'] = current_ltp

    def exit_trade(self, side, exit_price, timestamp, reason="", strategy_log="", user_id=None, instrument_key=None):
        trade = self.get_active_trade(side, instrument_key)
        if not trade:
            logger.warning(f"[{self.instrument_name}] Cannot exit {side} trade; no trade is active for {instrument_key or 'any key'}.")
            return

        trade['exit_price'] = exit_price
        trade['exit_timestamp'] = timestamp

        mult = trade.get('lot_size', 1) * trade.get('quantity', 1)
        entry_type = trade.get('entry_type', 'BUY')
        if entry_type == 'SELL':
            trade['pnl'] = (trade['entry_price'] - exit_price) * mult
        else:
            trade['pnl'] = (exit_price - trade['entry_price']) * mult

        trade['status'] = 'CLOSED'
        trade['exit_reason'] = reason
        trade['exit_strategy_log'] = strategy_log

        self.trade_history.append(trade)
        self.active_trades.remove(trade)

        pnl_per_share = trade['entry_price'] - exit_price if entry_type == 'SELL' else exit_price - trade['entry_price']
        logger.info(
            f"[{self.instrument_name}] Exited {side} trade on {trade['instrument_key']} at {exit_price:.2f}. "
            f"PnL/share={pnl_per_share:.2f} × lot={trade.get('lot_size',1)} × qty={trade.get('quantity',1)} "
            f"= Total PnL: {trade['pnl']:.2f}. Reason: {reason}"
        )
        self.trade_logger.log_exit("Backtest", self.instrument_name, trade['instrument_key'], f"EXIT_{side}", exit_price, trade['pnl'], reason, strategy_log, user_id=user_id)

    def get_real_time_pnl(self):
        """Returns floating PnL for active trades."""
        return sum(trade.get('pnl', 0) for trade in self.active_trades)

    def get_total_pnl(self):
        """Returns Realized + Floating PnL."""
        realized = sum(t.get('pnl', 0) for t in self.trade_history)
        return realized + self.get_real_time_pnl()

    def generate_summary_report(self):
        if not self.trade_history:
            logger.info(f"[{self.instrument_name}] No trades were executed during the backtest.")
            return

        report_df = pd.DataFrame(self.trade_history)
        total_trades = len(report_df)
        winning_trades = report_df[report_df['pnl'] > 0]
        losing_trades = report_df[report_df['pnl'] <= 0]

        total_pnl = report_df['pnl'].sum()
        win_rate = (len(winning_trades) / total_trades) * 100 if total_trades > 0 else 0
        avg_profit = winning_trades['pnl'].mean() if not winning_trades.empty else 0
        avg_loss = losing_trades['pnl'].mean() if not losing_trades.empty else 0

        logger.info(f"\n--- Backtest Summary Report for {self.instrument_name} ---")
        logger.info(f"Total Trades: {total_trades}")
        logger.info(f"Total PnL (₹): {total_pnl:.2f}")
        logger.info(f"Win Rate: {win_rate:.2f}%")
        logger.info(f"Average Profit per Winning Trade (₹): {avg_profit:.2f}")
        logger.info(f"Average Loss per Losing Trade (₹): {avg_loss:.2f}")
        logger.info("--- End of Report ---")
