from datetime import datetime
from utils.logger import logger


class LiveTradeLog:
    MAX_ENTRIES = 200

    def __init__(self):
        self.trades = []

    def add(self, trade_dict):
        logger.debug(f"[TradeLog] Adding entry: {trade_dict.get('type')} {trade_dict.get('direction')} strike={trade_dict.get('strike')} PnL={trade_dict.get('pnl_rs')}")
        self.trades.insert(0, trade_dict)
        if len(self.trades) > self.MAX_ENTRIES:
            self.trades = self.trades[:self.MAX_ENTRIES]

    def to_list(self, limit=50):
        return self.trades[:limit]

    @staticmethod
    def make_entry(trade_type, direction, strike, entry_price, exit_price,
                   pnl_pts, pnl_rs, reason, order_id='', timestamp=None):
        ts = timestamp or datetime.now()
        return {
            'time': ts.strftime('%H:%M:%S') if hasattr(ts, 'strftime') else str(ts)[-8:],
            'date': ts.strftime('%Y-%m-%d') if hasattr(ts, 'strftime') else str(ts)[:10],
            'type': trade_type,
            'direction': direction,
            'strike': int(strike) if strike else 0,
            'entry_price': round(float(entry_price), 2) if entry_price else 0,
            'exit_price': round(float(exit_price), 2) if exit_price else 0,
            'pnl_pts': round(float(pnl_pts), 2) if pnl_pts is not None else 0,
            'pnl_rs': round(float(pnl_rs), 2) if pnl_rs is not None else 0,
            'reason': str(reason or ''),
            'order_id': str(order_id or ''),
        }
