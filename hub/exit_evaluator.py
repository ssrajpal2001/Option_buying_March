import time
import pandas as pd
from utils.logger import logger

class ExitEvaluator:
    def __init__(self, orchestrator, indicator_manager):
        self.orchestrator = orchestrator
        self.indicator_manager = indicator_manager
        self.config_manager = orchestrator.config_manager

    def _get_user_setting(self, key, mode, category=None, type_func=str, fallback=None):
        if category: path = f"{self.orchestrator.instrument_name}.{mode}.{category}.{key}"
        else: path = f"{self.orchestrator.instrument_name}.{mode}.{key}"
        val = self.orchestrator.json_config.get_value(path)

        if val is None:
            path = f"{self.orchestrator.instrument_name}.{key}"
            val = self.orchestrator.json_config.get_value(path)

        return type_func(val) if val is not None else fallback

    async def evaluate_structural_exit_group(self, direction, position_data, timestamp, s1_breached, r1_breached, r1_target_hit, signal_inst_key, global_target_strike, primary_tf, current_ltp, entry_price, peak_price):
        entry_type = position_data.get('entry_type', 'BUY')
        mode = 'buy' if entry_type == 'BUY' else 'sell'
        exit_formula = self._get_user_setting('exit_formula', mode, fallback='s1_low')
        reasons, is_sl_exit = [], False
        f_lower = exit_formula.lower()

        eval_results = {
            'pattern': bool(position_data.get('pattern_exit_triggered')),
            's1_low': s1_breached, 'r1_high': r1_breached, 'r1_target': r1_target_hit,
            'vwap_slope': False, 's1_confirm': False, 'tsl': False, 'atr_tsl': False,
            'r1_falling': False, 'r1_stagnation': False, 'r1_low_breach': False,
            's1_double_drop': bool(position_data.get('s1_double_drop_triggered')),
            'oi_gate': False
        }

        if 'r1_stagnation' in f_lower:
            stag_mins = self._get_user_setting('minutes', mode, 'exit_indicators/r1_stagnation', type_func=int, fallback=15)
            last_move = position_data.get('last_sr_move_time')
            if last_move and (timestamp - last_move).total_seconds() / 60 >= stag_mins:
                eval_results['r1_stagnation'] = True
                reasons.append(f"SR Stagnation ({stag_mins}m)")

        if 'r1_low_breach' in f_lower:
            mon_ltp = position_data.get('monitoring_ltp', current_ltp)
            if mode == 'buy':
                b_lvl = position_data.get('r1_breach_low')
                if b_lvl and mon_ltp < b_lvl:
                    eval_results['r1_low_breach'] = True
                    reasons.append(f"R1 Breach Low ({b_lvl:.2f})")
            else:
                b_lvl = position_data.get('s1_breach_high')
                if b_lvl and mon_ltp > b_lvl:
                    eval_results['r1_low_breach'] = True
                    reasons.append(f"S1 Breach High ({b_lvl:.2f})")

        if eval_results['pattern'] and 'pattern' in f_lower: reasons.append(position_data.get('pattern_exit_reason', 'Pattern Reversed'))
        if eval_results['r1_target'] and 'r1_target' in f_lower: reasons.append(f"R1 Target Hit ({global_target_strike})")
        if eval_results['s1_double_drop'] and 's1_double_drop' in f_lower:
            reasons.append(position_data.get('s1_double_drop_reason', 'Double Drop'))
        if eval_results['s1_low'] and 's1_low' in f_lower: reasons.append(f"S1 Breached ({primary_tf}m)")
        if eval_results['r1_high'] and 'r1_high' in f_lower: reasons.append(f"R1 Breached ({primary_tf}m)")

        if 'vwap_slope' in f_lower:
            try:
                s_mode = self._get_user_setting('check_mode', mode, 'exit_indicators/vwap_slope', fallback='TICK').upper()
                s_tf = self._get_user_setting('tf', mode, 'exit_indicators/vwap_slope', type_func=int, fallback=1)
                s_occ = self._get_user_setting('occurrences', mode, 'exit_indicators/vwap_slope', type_func=int, fallback=1)
                s_op = self._get_user_setting('operator', mode, 'exit_indicators/vwap_slope', fallback='<')
                s_thr = self._get_user_setting('threshold', mode, 'exit_indicators/vwap_slope', type_func=float, fallback=0.0)

                can_eval, eval_ts, live_v = True, timestamp, None
                if s_mode == 'CLOSE':
                    at_candle_boundary = (timestamp.second >= 5 or self.orchestrator.is_backtest) and (timestamp.minute % s_tf == 0)
                    if not at_candle_boundary:
                        can_eval = False
                    else:
                        aligned_min = (timestamp.minute // s_tf) * s_tf
                        candle_ref = timestamp.replace(minute=aligned_min, second=0, microsecond=0)
                        last_checked = position_data.get('_vwap_close_last_candle')
                        if last_checked == candle_ref:
                            can_eval = False
                        else:
                            position_data['_vwap_close_last_candle'] = candle_ref
                            eval_ts = timestamp.replace(second=0, microsecond=0) - pd.Timedelta(minutes=1)
                            if not self.orchestrator.is_backtest:
                                live_v = await self.indicator_manager.calculate_vwap(signal_inst_key, timestamp)
                else:
                    live_v = position_data.get('monitoring_vwap')
                    if live_v is None:
                        live_v = await self.indicator_manager.calculate_vwap(signal_inst_key, timestamp)

                if can_eval:
                    res = await self.indicator_manager.get_vwap_slope_status(signal_inst_key, eval_ts, s_tf, s_occ, live_vwap=live_v)
                    v_curr, v_prev = res[2], res[3]

                    if s_mode == 'CLOSE':
                        # CLOSE mode: track OHLC-based peak so v_curr and v_ref come from the same data source.
                        # Live-tick vwap_peak diverges from OHLC cumulative VWAP — comparing them creates
                        # artificial drawdown. Instead, we maintain an OHLC peak updated each candle boundary.
                        ohlc_peak_key = 'vwap_peak_ohlc' if mode == 'buy' else 'vwap_trough_ohlc'
                        if v_curr is not None:
                            stored = position_data.get(ohlc_peak_key)
                            if stored is None:
                                position_data[ohlc_peak_key] = v_curr
                            elif mode == 'buy' and v_curr > stored:
                                position_data[ohlc_peak_key] = v_curr
                            elif mode == 'sell' and v_curr < stored:
                                position_data[ohlc_peak_key] = v_curr
                        v_ref = position_data.get(ohlc_peak_key) or v_prev
                        is_peak_ref = True
                    else:
                        v_ref = position_data.get('vwap_peak' if mode == 'buy' else 'vwap_trough')
                        if v_ref is None or v_ref == 0: v_ref = v_prev
                        is_peak_ref = (v_ref == position_data.get('vwap_peak' if mode == 'buy' else 'vwap_trough'))

                    if v_curr is not None and v_ref:
                        diff = (v_curr - v_ref) / v_ref
                        passed = False
                        if s_op == '>': passed = diff > s_thr
                        elif s_op == '<': passed = diff < s_thr
                        elif s_op == '>=': passed = diff >= s_thr
                        elif s_op == '<=': passed = diff <= s_thr

                        if is_peak_ref:
                            eval_results['vwap_slope'] = passed
                        else:
                            eval_results['vwap_slope'] = passed and (res[4] if s_op in ('>', '>=') else res[5]) >= s_occ

                        position_data['slope_info'] = f"V1:{v_curr:.2f} {s_op} Ref:{v_ref:.2f} (Pct:{diff*100:.2f}%)"
                        if eval_results['vwap_slope']:
                            reasons.append(f"VWAP Slope {s_op} ({s_mode})")
                            logger.info(f"V2 MGMT: [{direction}] EXIT CRITERIA MET (VWAP Slope): Current VWAP {v_curr:.2f} vs Ref {v_ref:.2f} (Drawdown: {diff*100:.2f}%, Threshold: {s_thr*100:.2f}%)")
                        else:
                            logger.info(f"V2 MGMT: [{direction}] VWAP EXIT CHECK: Current {v_curr:.2f} vs Peak {v_ref:.2f} | Drawdown: {diff*100:.2f}% | Threshold: {s_thr*100:.2f}% → HOLD")
                    else:
                        position_data['slope_info'] = f"Waiting for data (V:{v_curr}, Ref:{v_ref})"
            except Exception as e:
                position_data['slope_info'] = f"Error: {str(e)}"
                eval_results['vwap_slope'] = False
                logger.warning(f"V2 MGMT: [{direction}] Exit slope evaluation error: {e}", exc_info=True)

        if 's1_confirm' in f_lower:
            mon_ltp = position_data.get('monitoring_ltp', current_ltp)
            if entry_type == 'BUY' and position_data.get('sl_confirm_pending_low') and mon_ltp < position_data['sl_confirm_pending_low']:
                eval_results['s1_confirm'] = True
                reasons.append(f"S1 Confirmation (LTP < Low)")
            elif entry_type == 'SELL' and position_data.get('sl_confirm_pending_high') and mon_ltp > position_data['sl_confirm_pending_high']:
                eval_results['s1_confirm'] = True
                reasons.append(f"R1 Confirmation (LTP > High)")

        if 'tsl' in f_lower:
            offset = self._get_user_setting('offset', mode, 'exit_indicators/tsl', type_func=float, fallback=30.0)
            trailing_sl = (peak_price + offset) if entry_type == 'SELL' else (peak_price - offset)
            if (entry_type == 'SELL' and current_ltp > trailing_sl) or (entry_type == 'BUY' and current_ltp < trailing_sl):
                eval_results['tsl'] = True
                reasons.append(f"TSL Hit")
            position_data['trailing_sl'] = trailing_sl

        if 'atr_tsl' in f_lower:
            atr_val = position_data.get('entry_atr')
            if atr_val:
                multiplier = self._get_user_setting('multiplier', mode, 'exit_indicators/atr_tsl', type_func=float, fallback=2.0)
                check_mode = self._get_user_setting('check_mode', mode, 'exit_indicators/atr_tsl', fallback='TICK').upper()
                atr_offset = atr_val * multiplier
                atr_sl = (peak_price + atr_offset) if entry_type == 'SELL' else (peak_price - atr_offset)

                is_breached = False
                if check_mode == 'CLOSE':
                    if (timestamp.second >= 5) or (self.orchestrator.is_backtest and timestamp.second == 0):
                        if (entry_type == 'SELL' and current_ltp > atr_sl) or (entry_type == 'BUY' and current_ltp < atr_sl):
                            is_breached = True
                else:
                    if (entry_type == 'SELL' and current_ltp > atr_sl) or (entry_type == 'BUY' and current_ltp < atr_sl):
                        is_breached = True

                if is_breached:
                    eval_results['atr_tsl'] = True
                    reasons.append(f"ATR TSL Hit ({check_mode})")
                position_data['atr_trailing_sl'] = atr_sl

        if 'r1_falling' in f_lower:
            r1_tf = self._get_user_setting('tf', mode, 'exit_indicators/r1_falling', type_func=int, fallback=3)
            _, r1_curr, _, _, _, _, _, _ = await self.indicator_manager.get_sr_status(signal_inst_key, r1_tf, timestamp)
            if r1_curr and position_data.get('prev_r1_high') and r1_curr < position_data['prev_r1_high']:
                eval_results['r1_falling'] = True
                reasons.append("Falling R1")
            if r1_curr: position_data['prev_r1_high'] = r1_curr

        if 'oi_gate' in f_lower:
            oi_gate_enabled = self._get_user_setting(
                'enabled', mode, 'exit_indicators/oi_gate',
                type_func=lambda x: str(x).lower() != 'false', fallback=True)
            if oi_gate_enabled:
                min_hold = self._get_user_setting(
                    'min_hold_seconds', mode, 'exit_indicators/oi_gate',
                    type_func=int, fallback=120)
                entry_ts = position_data.get('entry_timestamp')
                if entry_ts is not None:
                    try:
                        held_seconds = (pd.Timestamp.now(tz='Asia/Kolkata') - pd.Timestamp(entry_ts)).total_seconds()
                    except Exception:
                        held_seconds = min_hold
                else:
                    held_seconds = min_hold
                if held_seconds < min_hold:
                    logger.info(
                        f"[ExitEval] OI Gate exit skipped — min hold {min_hold}s not reached "
                        f"(held {int(held_seconds)}s)")
                else:
                    oi_mon = getattr(self.orchestrator, 'oi_exit_monitor', None)
                    current_atm = getattr(self.orchestrator.state_manager, 'index_price', None)
                    if oi_mon and current_atm:
                        oi_dir = oi_mon.get_oi_direction(current_atm)
                        if oi_dir:
                            direction = position_data.get('direction', 'CALL')
                            if direction == 'CALL':
                                oi_exit = oi_dir['call_oi_increasing'] or oi_dir['put_oi_decreasing']
                            else:
                                oi_exit = oi_dir['put_oi_increasing'] or oi_dir['call_oi_decreasing']
                            if oi_exit:
                                eval_results['oi_gate'] = True
                                reasons.append(
                                    f"OI Gate Exit ("
                                    f"CE_inc={oi_dir['call_oi_increasing']} "
                                    f"PE_dec={oi_dir['put_oi_decreasing']} "
                                    f"PE_inc={oi_dir['put_oi_increasing']} "
                                    f"CE_dec={oi_dir['call_oi_decreasing']})")

        if self.orchestrator.json_config.evaluate_formula(exit_formula, eval_results):
            sl_inds = ['s1_low', 'r1_high', 'tsl', 'atr_tsl', 's1_confirm', 'r1_falling', 'r1_stagnation', 'r1_low_breach', 's1_double_drop']
            is_sl_exit = any(eval_results.get(ind) and ind in f_lower for ind in sl_inds)
            return True, f"Structural Exit: {', '.join(reasons)}", is_sl_exit
        return False, "", False
