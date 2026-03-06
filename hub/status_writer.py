import json
import os
import time
from pathlib import Path
from utils.logger import logger


class StatusWriter:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.last_write_ts = 0
        self.write_interval = 5
        self.status_path = Path('config/bot_status.json')
        self.last_error = None

    def record_error(self, error_msg):
        self.last_error = {"message": str(error_msg), "ts": time.time()}

    def maybe_write(self, timestamp, current_atm):
        now = time.monotonic()
        if (now - self.last_write_ts) < self.write_interval:
            return
        self.last_write_ts = now
        try:
            self._write(timestamp, current_atm)
        except Exception as e:
            logger.warning(f"[StatusWriter] Failed to write status: {e}")

    def _write(self, timestamp, current_atm):
        orch = self.orchestrator
        sm = orch.state_manager
        sell_mgr = getattr(orch, 'sell_manager', None)
        oi_mon = getattr(orch, 'oi_exit_monitor', None)

        buy_data = {}
        for side in ['CALL', 'PUT']:
            pos_data = {"status": "IDLE"}
            for session in orch.user_sessions.values():
                pos = (session.state_manager.call_position if side == 'CALL'
                       else session.state_manager.put_position)
                if pos:
                    inst_key = pos.get('instrument_key')
                    ltp = (session.state_manager.option_prices.get(inst_key, 0)
                           if inst_key else 0)
                    entry = pos.get('entry_price', 0) or 0
                    qty = pos.get('quantity', 1) or 1
                    qty_mult = pos.get('quantity_multiplier', 1) or 1
                    lot_size = getattr(sm, 'lot_size', 1) or 1
                    total_qty = qty * qty_mult * lot_size
                    entry_type = pos.get('entry_type', 'BUY')
                    if entry_type == 'SELL':
                        pnl = (entry - ltp) * total_qty
                    else:
                        pnl = (ltp - entry) * total_qty
                    pos_data = {
                        "status": "ACTIVE",
                        "strike": pos.get('strike_price'),
                        "entry": round(float(entry), 2),
                        "ltp": round(float(ltp), 2),
                        "pnl": round(float(pnl), 2),
                        "symbol": pos.get('instrument_symbol', ''),
                    }
                    break
            buy_data[side] = pos_data

        sell_lot_size = getattr(sm, 'lot_size', 1) or 1
        try:
            ref_broker = next(iter(orch.broker_manager.brokers), None)
            sell_broker_qty = (ref_broker.config_manager.get_int(
                ref_broker.instance_name, 'quantity', 1) if ref_broker else 1)
        except Exception:
            sell_broker_qty = 1
        sell_total_qty = sell_lot_size * sell_broker_qty

        sell_data = {}
        for side in ['CE', 'PE']:
            placed = getattr(sell_mgr, f"{'ce' if side == 'CE' else 'pe'}_placed", False)
            if sell_mgr and placed:
                inst_key = getattr(sell_mgr, f'sell_{side.lower()}_key', None)
                entry = getattr(sell_mgr, f'sell_{side.lower()}_entry_ltp', None) or 0
                ltp = sm.option_prices.get(inst_key, 0) if inst_key else 0
                strike = getattr(sell_mgr, f'sell_{side.lower()}_strike', None)
                sell_data[side] = {
                    "placed": True,
                    "strike": strike,
                    "entry": round(float(entry), 2),
                    "ltp": round(float(ltp), 2),
                    "pnl": round((float(entry) - float(ltp)) * sell_total_qty, 2),
                }
            else:
                sell_data[side] = {"placed": False}

        oi_snap = {}
        if oi_mon:
            oi_snap = {
                "call_oi": getattr(oi_mon, 'prev_call_oi', 0) or 0,
                "put_oi": getattr(oi_mon, 'prev_put_oi', 0) or 0,
            }

        session_pnl = 0.0
        trade_count = 0
        for session in orch.user_sessions.values():
            session_pnl += float(getattr(session.state_manager, 'total_pnl', 0) or 0)
            trade_count += int(getattr(session.state_manager, 'trade_count', 0) or 0)

        for side, pos_info in buy_data.items():
            if pos_info.get('status') == 'ACTIVE':
                session_pnl += float(pos_info.get('pnl', 0) or 0)

        for side, s_info in sell_data.items():
            if s_info.get('placed'):
                session_pnl += float(s_info.get('pnl', 0) or 0)

        if getattr(orch, 'pnl_tracker', None):
            for t in orch.pnl_tracker.trade_history:
                if t.get('entry_type') == 'SELL' and t.get('status') == 'CLOSED':
                    session_pnl += float(t.get('pnl', 0) or 0)

        cfg = getattr(orch, 'config_manager', None)
        mode = cfg.get('settings', 'trading_mode', fallback='live') if cfg else 'live'

        log_lines = []
        try:
            log_file = cfg.get('app', 'log_file', fallback='app.log') if cfg else 'app.log'
            if os.path.exists(log_file):
                with open(log_file, 'r') as f:
                    all_lines = f.readlines()
                    log_lines = [l.rstrip() for l in all_lines[-50:]]
        except Exception:
            pass

        trade_history = []
        if getattr(orch, 'trade_log', None):
            trade_history = orch.trade_log.to_list(50)

        status = {
            "updated_at": (timestamp.isoformat()
                           if hasattr(timestamp, 'isoformat') else str(timestamp)),
            "heartbeat": time.time(),
            "bot_running": True,
            "instrument": orch.instrument_name,
            "atm": current_atm,
            "spot_price": getattr(sm, 'spot_price', None),
            "index_price": getattr(sm, 'index_price', None),
            "mode": mode.upper(),
            "buy": buy_data,
            "sell": sell_data,
            "oi_snapshot": oi_snap,
            "session_pnl": round(session_pnl, 2),
            "trade_count": trade_count,
            "last_error": self.last_error,
            "log_tail": log_lines,
            "trade_history": trade_history,
        }

        tmp = self.status_path.with_suffix('.tmp')
        with open(tmp, 'w') as f:
            json.dump(status, f, default=str, indent=2)
        os.replace(tmp, self.status_path)
