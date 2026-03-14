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

    async def maybe_write(self, timestamp, current_atm):
        now = time.monotonic()
        if (now - self.last_write_ts) < self.write_interval:
            return
        self.last_write_ts = now
        try:
            await self._write(timestamp, current_atm)
        except Exception as e:
            logger.warning(f"[StatusWriter] Failed to write status: {e}")

    async def _write(self, timestamp, current_atm):
        orch = self.orchestrator
        sm = orch.state_manager
        sell_mgr = getattr(orch, 'sell_manager', None)
        v3_mgr = getattr(orch, 'sell_manager_v3', None)
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
                    entry_ts = pos.get('entry_timestamp')
                    pos_data = {
                        "status": "ACTIVE",
                        "strike": pos.get('strike_price'),
                        "entry": round(float(entry), 2),
                        "ltp": round(float(ltp), 2),
                        "pnl": round(float(pnl), 2),
                        "symbol": pos.get('instrument_symbol', ''),
                        "entry_time": entry_ts.strftime('%H:%M:%S') if hasattr(entry_ts, 'strftime') else str(entry_ts)
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
        v3_active = v3_mgr and v3_mgr.active
        # Prioritize V3 status for web display if active
        if v3_active:
            for side in ['CE', 'PE']:
                leg = v3_mgr.ce_leg if side == 'CE' else v3_mgr.pe_leg
                if leg:
                    inst_key = leg.get('key')
                    entry = leg.get('entry_ltp') or 0
                    ltp = sm.option_prices.get(inst_key, 0) if inst_key else 0
                    strike = leg.get('strike')
                    entry_ts = v3_mgr.entry_timestamp
                    sell_data[side] = {
                        "placed": True,
                        "strike": strike,
                        "entry": round(float(entry), 2),
                        "ltp": round(float(ltp), 2),
                        "pnl": round((float(entry) - float(ltp)) * sell_total_qty, 2),
                        "strategy": "V3",
                        "entry_time": entry_ts.strftime('%H:%M:%S') if hasattr(entry_ts, 'strftime') else str(entry_ts)
                    }
                else:
                    sell_data[side] = {"placed": False}

            # Add Synthetic Indicators for V3
            try:
                rsi_cfg = v3_mgr._cfg('rsi', {})
                ce_key = v3_mgr.ce_leg['key'] if v3_mgr.ce_leg else None
                pe_key = v3_mgr.pe_leg['key'] if v3_mgr.pe_leg else None

                v3_rsi = None
                if ce_key and pe_key:
                    v3_rsi = await orch.indicator_manager.calculate_combined_rsi(
                        ce_key, pe_key,
                        rsi_cfg.get('tf', 5), rsi_cfg.get('period', 14), timestamp,
                        include_current=True, skip_api=True
                    )

                ce_atp = await orch.indicator_manager.calculate_vwap(ce_key, timestamp) if ce_key else None
                pe_atp = await orch.indicator_manager.calculate_vwap(pe_key, timestamp) if pe_key else None

                ce_ltp = sm.option_prices.get(ce_key, 0) if ce_key else 0
                pe_ltp = sm.option_prices.get(pe_key, 0) if pe_key else 0
                combined_ltp = ce_ltp + pe_ltp

                lowest_vwap = v3_mgr.lowest_combined_vwap
                current_combined_vwap = (ce_atp + pe_atp) if (ce_atp and pe_atp) else None
                rise_pct = None
                if lowest_vwap and current_combined_vwap:
                    rise_pct = ((current_combined_vwap - lowest_vwap) / lowest_vwap) * 100

                # V-Slope Status (Falling vs Rising)
                v_slope_status = "IDLE"
                if current_combined_vwap:
                    prev_ts = timestamp - timedelta(minutes=1)
                    ce_atp_prev = await orch.indicator_manager.calculate_vwap(ce_key, prev_ts)
                    pe_atp_prev = await orch.indicator_manager.calculate_vwap(pe_key, prev_ts)
                    if ce_atp_prev and pe_atp_prev:
                        v_slope_status = "FALLING" if current_combined_vwap < (ce_atp_prev + pe_atp_prev) else "RISING"

                sell_data['v3_indicators'] = {
                    "rsi": round(v3_rsi, 2) if v3_rsi is not None else None,
                    "vwap": round(current_combined_vwap, 2) if current_combined_vwap else None,
                    "combined_ltp": round(combined_ltp, 2),
                    "lowest_vwap": round(lowest_vwap, 2) if lowest_vwap else None,
                    "rise_pct": round(rise_pct, 2) if rise_pct is not None else None,
                    "v_slope": v_slope_status,
                    "active": True
                }
            except Exception:
                pass
        elif v3_mgr and v3_mgr._cfg('enabled', False):
            # Show indicators for current ATM even if not active
            try:
                expiry = orch.atm_manager.signal_expiry_date
                ce_key = orch.atm_manager.find_instrument_key_by_strike(current_atm, 'CALL', expiry)
                pe_key = orch.atm_manager.find_instrument_key_by_strike(current_atm, 'PUT', expiry)
                if ce_key and pe_key:
                    rsi_cfg = v3_mgr._cfg('rsi', {})
                    v3_rsi = await orch.indicator_manager.calculate_combined_rsi(
                        ce_key, pe_key, rsi_cfg.get('tf', 5), rsi_cfg.get('period', 14), timestamp,
                        include_current=True, skip_api=True
                    )
                    ce_atp = await orch.indicator_manager.calculate_vwap(ce_key, timestamp)
                    pe_atp = await orch.indicator_manager.calculate_vwap(pe_key, timestamp)

                    current_vwap = (ce_atp + pe_atp) if (ce_atp and pe_atp) else None
                    v_slope_status = "IDLE"
                    if current_vwap:
                        prev_ts = timestamp - timedelta(minutes=1)
                        ce_atp_prev = await orch.indicator_manager.calculate_vwap(ce_key, prev_ts)
                        pe_atp_prev = await orch.indicator_manager.calculate_vwap(pe_key, prev_ts)
                        if ce_atp_prev and pe_atp_prev:
                            v_slope_status = "FALLING" if current_vwap < (ce_atp_prev + pe_atp_prev) else "RISING"

                    sell_data['v3_indicators'] = {
                        "rsi": round(v3_rsi, 2) if v3_rsi is not None else None,
                        "vwap": round(current_vwap, 2) if current_vwap else None,
                        "combined_ltp": round(sm.option_prices.get(ce_key, 0) + sm.option_prices.get(pe_key, 0), 2),
                        "v_slope": v_slope_status
                    }
            except Exception:
                pass
        else:
            for side in ['CE', 'PE']:
                placed = getattr(sell_mgr, f"{'ce' if side == 'CE' else 'pe'}_placed", False)
                if sell_mgr and placed:
                    inst_key = getattr(sell_mgr, f'sell_{side.lower()}_key', None)
                    entry = getattr(sell_mgr, f'sell_{side.lower()}_entry_ltp', None) or 0
                    ltp = sm.option_prices.get(inst_key, 0) if inst_key else 0
                    strike = getattr(sell_mgr, f'sell_{side.lower()}_strike', None)
                    entry_ts = getattr(sell_mgr, f"{side.lower()}_entry_timestamp", None)
                    sell_data[side] = {
                        "placed": True,
                        "strike": strike,
                        "entry": round(float(entry), 2),
                        "ltp": round(float(ltp), 2),
                        "pnl": round((float(entry) - float(ltp)) * sell_total_qty, 2),
                        "strategy": "V2",
                        "entry_time": entry_ts.strftime('%H:%M:%S') if hasattr(entry_ts, 'strftime') else str(entry_ts)
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

        # Realized P&L and counts from Buy Side (User Sessions)
        for session in orch.user_sessions.values():
            session_pnl += float(getattr(session.state_manager, 'total_pnl', 0) or 0)
            trade_count += int(getattr(session.state_manager, 'trade_count', 0) or 0)

        # Realized P&L and counts from Sell Side V3
        # V3 trades are logged with date strings. Filter for today to get "Session" P&L.
        today_str = timestamp.strftime('%Y-%m-%d') if hasattr(timestamp, 'strftime') else ""

        if v3_mgr:
            v3_today_trades = [t for t in v3_mgr.closed_trades if t.get('date') == today_str]
            for trade in v3_today_trades:
                session_pnl += float(trade.get('pnl_rs', 0) or 0)

            # Count each completed pair (Strangle) as one 'trade' for the header count
            trade_count += len(v3_today_trades) // 2

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
