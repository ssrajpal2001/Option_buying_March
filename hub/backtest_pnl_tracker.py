from utils.logger import logger
from utils.trade_logger import TradeLogger
import pandas as pd

class BacktestPnLTracker:
    def __init__(self, instrument_name, config_manager):
        self.instrument_name = instrument_name
        self.config_manager = config_manager
        self.active_call_trade = None
        self.active_put_trade = None
        self.trade_history = []
        self.trade_logger = TradeLogger()

    def is_trade_active(self, side=None):
        if side == 'CALL':
            return self.active_call_trade is not None
        if side == 'PUT':
            return self.active_put_trade is not None
        return self.active_call_trade is not None or self.active_put_trade is not None

    def get_active_trade(self, side):
        return self.active_call_trade if side == 'CALL' else self.active_put_trade

    def enter_trade(self, side, instrument_key, entry_price, timestamp, strike_price, contract, strategy_log="", user_id=None, entry_type='BUY', quantity=1):
        if self.is_trade_active(side):
            logger.warning(f"[{self.instrument_name}] Cannot enter new {side} trade; a trade is already active.")
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

        if side == 'CALL':
            self.active_call_trade = trade
        else:
            self.active_put_trade = trade

        logger.info(f"[{self.instrument_name}] Entered {side} trade on {instrument_key} at {entry_price:.2f} (lot={lot_size} qty={quantity})")
        self.trade_logger.log_entry("Backtest", self.instrument_name, instrument_key, side, entry_price, strategy_log, user_id=user_id)
        return True

    def update_pnl(self, side, current_ltp, timestamp):
        trade = self.get_active_trade(side)
        if not trade:
            return

        mult = trade.get('lot_size', 1) * trade.get('quantity', 1)
        entry_type = trade.get('entry_type', 'BUY')
        if entry_type == 'SELL':
            trade['pnl'] = (trade['entry_price'] - current_ltp) * mult
        else:
            trade['pnl'] = (current_ltp - trade['entry_price']) * mult
        trade['current_ltp'] = current_ltp

    def exit_trade(self, side, exit_price, timestamp, reason="", strategy_log="", user_id=None):
        trade = self.get_active_trade(side)
        if not trade:
            logger.warning(f"[{self.instrument_name}] Cannot exit {side} trade; no trade is active.")
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

        if side == 'CALL':
            self.active_call_trade = None
        else:
            self.active_put_trade = None

        pnl_per_share = trade['entry_price'] - exit_price if entry_type == 'SELL' else exit_price - trade['entry_price']
        logger.info(
            f"[{self.instrument_name}] Exited {side} trade on {trade['instrument_key']} at {exit_price:.2f}. "
            f"PnL/share={pnl_per_share:.2f} × lot={trade.get('lot_size',1)} × qty={trade.get('quantity',1)} "
            f"= Total PnL: {trade['pnl']:.2f}. Reason: {reason}"
        )
        self.trade_logger.log_exit("Backtest", self.instrument_name, trade['instrument_key'], f"EXIT_{side}", exit_price, trade['pnl'], reason, strategy_log, user_id=user_id)

    def get_real_time_pnl(self):
        total_pnl = 0
        if self.active_call_trade:
            total_pnl += self.active_call_trade.get('pnl', 0)
        if self.active_put_trade:
            total_pnl += self.active_put_trade.get('pnl', 0)
        return total_pnl

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
