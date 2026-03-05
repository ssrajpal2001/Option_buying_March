import pandas as pd
import asyncio
import re
from utils.logger import logger
from datetime import datetime, time


class SignalEvaluator:

    def __init__(self, monitor):
        self.monitor = monitor
        self.orchestrator = monitor.orchestrator
        self.state_manager = monitor.state_manager
        self.indicator_manager = monitor.indicator_manager
        self.pattern_matcher = monitor.pattern_matcher
        self.atm_manager = monitor.atm_manager
        self.data_manager = monitor.data_manager

    def _is_in_formula(self, indicator_name, formula):
        return bool(re.search(rf'\b{indicator_name}\b', formula.lower()))

    async def evaluate_tick_criteria(self, timestamp, active_modes):
        """Updates criteria_state for TICK indicators and evaluates formulas for both sides."""
        monitoring_data = self.state_manager.dual_sr_monitoring_data
        if not monitoring_data: return
        triggered = False

        for mode in active_modes:
            entry_formula = self.monitor._get_user_setting('entry_formula',
                                                           str,
                                                           fallback='pattern',
                                                           mode=mode)
            sig_expiry = self.atm_manager.get_expiry_by_mode(mode, 'signal')
            f_lower = entry_formula.lower()

            needs_vwap = self._is_in_formula(
                'vwap', entry_formula) or self._is_in_formula(
                    'vwap_slope', entry_formula)
            needs_slope = self._is_in_formula('vwap_slope', entry_formula)
            needs_r1 = self._is_in_formula('r1_high', entry_formula)
            needs_s1 = self._is_in_formula('s1_low', entry_formula)

            for side in ['CE', 'PE']:
                side_key = 'ce_data' if side == 'CE' else 'pe_data'
                data = monitoring_data.get(side_key)
                if not data: continue

                strike = data['strike_price']
                inst_key = self.atm_manager.find_instrument_key_by_strike(
                    strike, side, sig_expiry)
                if not inst_key: continue

                # --- COMPUTE VWAP & SLOPE FIRST (independent of gate/LTP) ---
                vwap_val = None
                if needs_vwap or needs_slope:
                    vwap_val = await self.indicator_manager.calculate_vwap(inst_key, timestamp)
                    data['vwap'] = vwap_val

                if needs_slope:
                    try:
                        slope_mode = self.monitor._get_user_setting(
                            'check_mode',
                            str,
                            fallback='CLOSE',
                            mode=mode,
                            category='indicators/vwap_slope').upper()
                        slope_tf = self.monitor._get_user_setting(
                            'tf',
                            int,
                            fallback=1,
                            mode=mode,
                            category='indicators/vwap_slope')
                        slope_occ = self.monitor._get_user_setting(
                            'occurrences',
                            int,
                            fallback=1,
                            mode=mode,
                            category='indicators/vwap_slope')
                        slope_operator = self.monitor._get_user_setting(
                            'operator',
                            str,
                            fallback='>',
                            mode=mode,
                            category='indicators/vwap_slope')
                        slope_threshold = self.monitor._get_user_setting(
                            'threshold',
                            float,
                            fallback=0.0,
                            mode=mode,
                            category='indicators/vwap_slope')

                        can_eval_slope = True
                        if slope_mode == 'CLOSE':
                            # Only evaluate in the first 10 seconds after a candle closes.
                            # With tf=1, minute%1==0 is always True so we must bound by second<10.
                            can_eval_slope = (
                                (timestamp.second < 10 or self.orchestrator.is_backtest) and
                                (timestamp.minute % slope_tf == 0)
                            )

                        if can_eval_slope:
                            s_ts = timestamp if slope_mode == 'TICK' else (
                                timestamp.replace(second=0, microsecond=0) -
                                pd.Timedelta(minutes=1))
                            # CLOSE mode must use closed-candle VWAP, not live price.
                            # Passing live_vwap in CLOSE mode made it behave like TICK mode.
                            s_v = vwap_val if slope_mode == 'TICK' else None

                            is_r, is_f, v_curr, v_prev, c_r, c_f = await self.indicator_manager.get_vwap_slope_status(
                                inst_key, s_ts, slope_tf, slope_occ, live_vwap=s_v)
                            curr_cons = c_r if slope_operator in ('>', '>=') else c_f
                            slope_passed = False
                            if v_curr is not None and v_prev is not None and v_prev != 0:
                                diff = (v_curr - v_prev) / v_prev
                                if slope_operator == '>':
                                    slope_passed = (diff > slope_threshold and curr_cons >= slope_occ)
                                elif slope_operator == '<':
                                    slope_passed = (diff < slope_threshold and curr_cons >= slope_occ)
                                elif slope_operator == '>=':
                                    slope_passed = (diff >= slope_threshold and curr_cons >= slope_occ)
                                elif slope_operator == '<=':
                                    slope_passed = (diff <= slope_threshold and curr_cons >= slope_occ)
                                data['slope_info'] = f"V1:{float(v_curr):.2f} {slope_operator} V0:{float(v_prev):.2f} (Pct:{diff*100:.2f}%, Cons:{curr_cons}/{slope_occ})"
                            else:
                                data['slope_info'] = f"Waiting for data (V1:{v_curr}, V0:{v_prev})"
                            data['criteria_state']['vwap_slope'] = slope_passed
                    except Exception as e:
                        data['slope_info'] = f"Error: {str(e)}"
                        data['criteria_state']['vwap_slope'] = False
                        logger.warning(
                            f"V2: [{side}] Slope evaluation error: {e}",
                            exc_info=True)

                # --- GATE CHECK (entry blocked if gated, but slope above is already computed) ---
                gate_enabled = self.monitor._get_user_setting(
                    'range_915_gate_enabled', bool, fallback=True, mode=mode)

                range_passed = True
                if gate_enabled:
                    idx_ltp = self.state_manager.index_price or 0.0
                    idx_high, idx_low = await self.indicator_manager.get_index_915_range(
                        self.orchestrator.index_instrument_key, timestamp)
                    range_passed = self.monitor.gate_manager.is_side_permitted(
                        side, idx_ltp, idx_high, idx_low)

                if not range_passed:
                    last_range_log = getattr(
                        self.monitor, f'_last_range_wait_{side}_{mode}', 0)
                    if timestamp.timestamp() - last_range_log >= 60:
                        wait_msg = ""
                        breach_happened = getattr(
                            self.state_manager, 'range_915_breached_up' if side
                            == 'CE' else 'range_915_breached_down', False)
                        if not breach_happened:
                            wait_msg = f"Waiting for 9:15 {'High' if side == 'CE' else 'Low'} Breach"
                        else:
                            wait_msg = f"Index {idx_ltp:.2f} is not {'above' if side == 'CE' else 'below'} 9:15 Low ({idx_low:.2f})"

                        logger.info(
                            f"V2: [{side}] Gated. {wait_msg}. Range: {idx_low:.2f}-{idx_high:.2f}"
                        )
                        setattr(self.monitor,
                                f'_last_range_wait_{side}_{mode}',
                                timestamp.timestamp())
                    continue

                if self.state_manager.is_in_trade('CALL' if side ==
                                                  'CE' else 'PUT'):
                    continue
                if data.get('entry_confirmed'): continue

                # Re-entry cooldown: block same-side entry for 60s after any exit (live mode only)
                if not self.orchestrator.is_backtest:
                    cooldown_attr = 'call_cooldown_until' if side == 'CE' else 'put_cooldown_until'
                    cooldown_until = getattr(self.state_manager, cooldown_attr, None)
                    if cooldown_until is not None and timestamp < cooldown_until:
                        remaining = (cooldown_until - timestamp).total_seconds()
                        logger.info(f"V2: [{side}] Re-entry blocked by cooldown. {remaining:.0f}s remaining (until {cooldown_until.strftime('%H:%M:%S')})")
                        continue

                current_ltp = self.state_manager.get_ltp(inst_key)
                if current_ltp is None or current_ltp <= 0: continue

                # Update VWAP criteria state now that we have LTP
                if needs_vwap and self._is_in_formula('vwap', entry_formula):
                    vwap_mode = self.monitor._get_user_setting('check_mode', str, fallback='TICK', mode=mode, category='indicators/vwap').upper()
                    if vwap_mode == 'TICK':
                        data['criteria_state']['vwap'] = (vwap_val is None
                                                          or current_ltp
                                                          >= vwap_val)

                if needs_r1:
                    r1_mode = self.monitor._get_user_setting(
                        'check_mode',
                        str,
                        fallback='TICK',
                        mode=mode,
                        category='indicators/r1_high').upper()
                    r1_tf = self.monitor._get_user_setting(
                        'tf',
                        int,
                        fallback=3,
                        mode=mode,
                        category='indicators/r1_high')

                    if r1_mode == 'TICK':
                        is_br, _, b_val, s1_r, r1_r, ph, s1_h, r1_l, b_lvl = await self.monitor._check_barrier_breach(
                            inst_key,
                            current_ltp,
                            'r1_high',
                            r1_tf,
                            timestamp,
                            mode=mode)
                        data['r1_high'] = b_val
                        data['r1_phase'] = ph
                        data[
                            'r1_label'] = 'PrevHigh' if ph == 'R1_TRACKING' else 'R1'
                        data['criteria_state']['r1_high'] = is_br

                if needs_s1:
                    s1_mode = self.monitor._get_user_setting(
                        'check_mode',
                        str,
                        fallback='TICK',
                        mode=mode,
                        category='indicators/s1_low').upper()
                    s1_tf = self.monitor._get_user_setting(
                        'tf',
                        int,
                        fallback=3,
                        mode=mode,
                        category='indicators/s1_low')

                    if s1_mode == 'TICK':
                        is_br, _, b_val, s1_r, r1_r, ph, s1_h, r1_l, b_lvl = await self.monitor._check_barrier_breach(
                            inst_key,
                            current_ltp,
                            's1_low',
                            s1_tf,
                            timestamp,
                            mode=mode)
                        data['s1_low'] = b_val
                        data['s1_phase'] = ph
                        data[
                            's1_label'] = 'PrevLow' if ph == 'S1_TRACKING' else 'S1'
                        data['criteria_state']['s1_low'] = is_br

                is_sticky_reentry = monitoring_data.get(
                    'awaiting_fresh_vwap_breach', False)
                is_dip_pending = False
                if is_sticky_reentry:
                    if mode == 'buy':
                        if needs_r1:
                            r1_tf_val = self.monitor._get_user_setting(
                                'tf', int, fallback=3, mode=mode,
                                category='indicators/r1_high')
                            b_val, _, _, _, phase, _, _, _ = await self.indicator_manager.get_nuanced_barrier(
                                data['instrument_key'], 'r1_high', r1_tf_val, timestamp)
                            is_below = (vwap_val is not None and current_ltp < vwap_val) or (
                                b_val is not None and current_ltp < b_val and phase != 'R1_TRACKING')
                        else:
                            is_below = vwap_val is not None and current_ltp < vwap_val
                    else:  # sell
                        if needs_s1:
                            s1_tf_val = self.monitor._get_user_setting(
                                'tf', int, fallback=3, mode=mode,
                                category='indicators/s1_low')
                            b_val, _, _, _, phase, _, _, _ = await self.indicator_manager.get_nuanced_barrier(
                                data['instrument_key'], 's1_low', s1_tf_val, timestamp)
                            is_below = (vwap_val is not None and current_ltp > vwap_val) or (
                                b_val is not None and current_ltp > b_val and phase != 'S1_TRACKING')
                        else:
                            is_below = vwap_val is not None and current_ltp > vwap_val

                    if is_below:
                        if not monitoring_data.get('sticky_dip_confirmed'):
                            monitoring_data['sticky_dip_confirmed'] = True
                    if not monitoring_data.get('sticky_dip_confirmed'):
                        is_dip_pending = True

                eval_results = {
                    k: v
                    for k, v in data['criteria_state'].items()
                }

                last_wait_log = getattr(self.monitor,
                                        f'_last_wait_log_{side}_{mode}', 0)
                if timestamp.timestamp() - last_wait_log >= 60:
                    wait_parts = []
                    for k, v in eval_results.items():
                        if not self._is_in_formula(k, entry_formula): continue
                        if k == 'r1_high':
                            label = data.get('r1_label', 'R1')
                            val = data.get('r1_high')
                            wait_parts.append(
                                f"{label}({float(val or 0):.1f})={v}")
                        elif k == 's1_low':
                            label = data.get('s1_label', 'S1')
                            val = data.get('s1_low')
                            wait_parts.append(
                                f"{label}({float(val or 0):.1f})={v}")
                        elif k == 'vwap_slope':
                            wait_parts.append(
                                f"slope({data.get('slope_info', 'N/A')})={v}")
                        else:
                            wait_parts.append(f"{k}={v}")
                    logger.info(
                        f"V2: [{side}] [{mode.upper()}] Status [{data['strike_price']}]: {', '.join(wait_parts)} | LTP: {current_ltp:.2f} | Formula: {entry_formula}"
                    )
                    setattr(self.monitor, f'_last_wait_log_{side}_{mode}',
                            timestamp.timestamp())

                if self.monitor._evaluate_formula(entry_formula, eval_results):
                    if not range_passed:
                        last_range_log = getattr(
                            self.monitor, f'_last_range_wait_{side}_{mode}', 0)
                        if timestamp.timestamp() - last_range_log >= 60:
                            wait_msg = "Waiting for 9:15 High Breach" if side == 'CE' else "Waiting for 9:15 Low Breach"
                            logger.info(
                                f"V2: [{side}] Entry BLOCKED. {wait_msg}. Index: {idx_ltp:.2f} (9:15 Range: {idx_low:.2f} - {idx_high:.2f})"
                            )
                            setattr(self.monitor,
                                    f'_last_range_wait_{side}_{mode}',
                                    timestamp.timestamp())
                        continue

                    if is_dip_pending:
                        last_dip_log = getattr(
                            self.monitor, f'_last_dip_wait_{side}_{mode}', 0)
                        if timestamp.timestamp() - last_dip_log >= 60:
                            logger.info(
                                f"V2: [{side}] [{mode.upper()}] Entry BLOCKED. Formula passed but waiting for DIP."
                            )
                            setattr(self.monitor,
                                    f'_last_dip_wait_{side}_{mode}',
                                    timestamp.timestamp())
                        continue

                    trigger_reason = f"{side} {mode.upper()} confirmed via Formula: {entry_formula} (Tick)"
                    entry_type = self.monitor._get_user_setting('entry_type',
                                                                str,
                                                                fallback='BUY',
                                                                mode=mode)

                    if not await self.monitor._handle_potential_reversal(
                            side, mode, data, timestamp):
                        continue

                    await self.monitor._confirm_entry(side, data, inst_key,
                                                      data['strike_price'],
                                                      current_ltp,
                                                      trigger_reason,
                                                      timestamp, entry_type)
                    triggered = True
        return triggered

    async def evaluate_close_criteria(self, timestamp, active_modes):
        """Evaluates entry formulas based on finalized 1-minute candles."""
        monitoring_data = self.state_manager.dual_sr_monitoring_data
        if not monitoring_data: return
        triggered = False

        for mode in active_modes:
            sig_expiry = self.atm_manager.get_expiry_by_mode(mode, 'signal')
            entry_formula = self.monitor._get_user_setting('entry_formula',
                                                           str,
                                                           fallback='pattern',
                                                           mode=mode)

            needs_vwap = self._is_in_formula(
                'vwap', entry_formula) or self._is_in_formula(
                    'vwap_slope', entry_formula)
            needs_slope = self._is_in_formula('vwap_slope', entry_formula)
            needs_r1 = self._is_in_formula('r1_high', entry_formula)
            needs_s1 = self._is_in_formula('s1_low', entry_formula)

            for side in ['CE', 'PE']:
                side_key = 'ce_data' if side == 'CE' else 'pe_data'
                data = monitoring_data[side_key]

                if data.get('entry_confirmed'):
                    continue

                gate_enabled = self.monitor._get_user_setting(
                    'range_915_gate_enabled', bool, fallback=True, mode=mode)

                range_passed = True
                if gate_enabled:
                    idx_ltp = self.state_manager.index_price or 0.0
                    idx_high, idx_low = await self.indicator_manager.get_index_915_range(
                        self.orchestrator.index_instrument_key, timestamp)
                    range_passed = self.monitor.gate_manager.is_side_permitted(
                        side, idx_ltp, idx_high, idx_low)

                if not range_passed:
                    last_range_log = getattr(
                        self.monitor, f'_last_range_wait_close_{side}_{mode}',
                        0)
                    if timestamp.timestamp() - last_range_log >= 60:
                        breach_happened = getattr(
                            self.state_manager, 'range_915_breached_up' if side
                            == 'CE' else 'range_915_breached_down', False)
                        wait_msg = f"Gated. {'WaitBreach' if not breach_happened else 'Offside'}"
                        logger.info(
                            f"V2: [{side}] {wait_msg} (Close Mode). Index: {idx_ltp:.2f}"
                        )
                        setattr(self.monitor,
                                f'_last_range_wait_close_{side}_{mode}',
                                timestamp.timestamp())
                    continue

                if self.state_manager.is_in_trade('CALL' if side ==
                                                  'CE' else 'PUT'):
                    continue

                inst_key = self.atm_manager.find_instrument_key_by_strike(
                    data['strike_price'], side, sig_expiry)
                if not inst_key: continue

                vwap_val = None
                if needs_vwap or needs_slope:
                    vwap_val = await self.indicator_manager.calculate_vwap(
                        inst_key, timestamp)
                last_close = data['last_close']

                eval_results = {
                    'pattern': data['criteria_state'].get('pattern', False),
                    'vwap': data['criteria_state'].get('vwap', False),
                    'vwap_slope':
                    data['criteria_state'].get('vwap_slope', False),
                    'r1_high': data['criteria_state'].get('r1_high', False),
                    's1_low': data['criteria_state'].get('s1_low', False)
                }

                if needs_vwap and self._is_in_formula('vwap', entry_formula):
                    vwap_mode = self.monitor._get_user_setting(
                        'check_mode',
                        str,
                        fallback='TICK',
                        mode=mode,
                        category='indicators/vwap').upper()
                    if vwap_mode == 'CLOSE':
                        eval_results['vwap'] = (vwap_val is None
                                                or last_close >= vwap_val)

                if needs_slope:
                    slope_mode = self.monitor._get_user_setting(
                        'check_mode',
                        str,
                        fallback='CLOSE',
                        mode=mode,
                        category='indicators/vwap_slope').upper()
                    if slope_mode == 'CLOSE':
                        try:
                            slope_tf = self.monitor._get_user_setting(
                                'tf',
                                int,
                                fallback=1,
                                mode=mode,
                                category='indicators/vwap_slope')
                            slope_occ = self.monitor._get_user_setting(
                                'occurrences',
                                int,
                                fallback=1,
                                mode=mode,
                                category='indicators/vwap_slope')
                            slope_operator = self.monitor._get_user_setting(
                                'operator',
                                str,
                                fallback='>',
                                mode=mode,
                                category='indicators/vwap_slope')
                            slope_threshold = self.monitor._get_user_setting(
                                'threshold',
                                float,
                                fallback=0.0,
                                mode=mode,
                                category='indicators/vwap_slope')

                            last_1m_ts = timestamp.replace(
                                second=0,
                                microsecond=0) - pd.Timedelta(minutes=1)
                            is_r, is_f, v_curr, v_prev, c_r, c_f = await self.indicator_manager.get_vwap_slope_status(
                                inst_key, last_1m_ts, slope_tf, slope_occ)
                            curr_cons = c_r if slope_operator in (
                                '>', '>=') else c_f
                            slope_passed = False
                            if v_curr is not None and v_prev is not None and v_prev != 0:
                                diff = (v_curr - v_prev) / v_prev
                                if slope_operator == '>':
                                    slope_passed = (diff > slope_threshold
                                                    and curr_cons >= slope_occ)
                                elif slope_operator == '<':
                                    slope_passed = (diff < slope_threshold
                                                    and curr_cons >= slope_occ)
                                elif slope_operator == '>=':
                                    slope_passed = (diff >= slope_threshold
                                                    and curr_cons >= slope_occ)
                                elif slope_operator == '<=':
                                    slope_passed = (diff <= slope_threshold
                                                    and curr_cons >= slope_occ)
                                data[
                                    'slope_info'] = f"V1:{float(v_curr):.2f} {slope_operator} V0:{float(v_prev):.2f} (Pct:{diff*100:.2f}%, Cons:{curr_cons}/{slope_occ})"
                            else:
                                data[
                                    'slope_info'] = f"Waiting for data (V1:{v_curr}, V0:{v_prev})"
                            eval_results['vwap_slope'] = slope_passed
                        except Exception as e:
                            data['slope_info'] = f"Error: {str(e)}"
                            eval_results['vwap_slope'] = False
                            logger.warning(
                                f"V2: [{side}] Close slope evaluation error: {e}",
                                exc_info=True)

                if needs_r1:
                    r1_mode = self.monitor._get_user_setting(
                        'check_mode',
                        str,
                        fallback='TICK',
                        mode=mode,
                        category='indicators/r1_high').upper()
                    if r1_mode == 'CLOSE':
                        r1_tf_val = self.monitor._get_user_setting(
                            'tf',
                            int,
                            fallback=3,
                            mode=mode,
                            category='indicators/r1_high')
                        is_r1_p, _, b_val, _, _, ph, _, _, _ = await self.monitor._check_barrier_breach(
                            inst_key,
                            last_close,
                            'r1_high',
                            r1_tf_val,
                            timestamp,
                            mode=mode)
                        eval_results['r1_high'] = is_r1_p

                if needs_s1:
                    s1_mode = self.monitor._get_user_setting(
                        'check_mode',
                        str,
                        fallback='TICK',
                        mode=mode,
                        category='indicators/s1_low').upper()
                    if s1_mode == 'CLOSE':
                        s1_tf_val = self.monitor._get_user_setting(
                            'tf',
                            int,
                            fallback=3,
                            mode=mode,
                            category='indicators/s1_low')
                        is_s1_p, _, b_val, _, _, ph, _, _, _ = await self.monitor._check_barrier_breach(
                            inst_key,
                            last_close,
                            's1_low',
                            s1_tf_val,
                            timestamp,
                            mode=mode)
                        eval_results['s1_low'] = is_s1_p

                for k in ['vwap', 'vwap_slope', 'r1_high', 's1_low']:
                    if k in eval_results:
                        data['criteria_state'][k] = eval_results[k]

                if self.monitor._evaluate_formula(entry_formula, eval_results):
                    signal_ltp = self.state_manager.get_ltp(
                        inst_key) or last_close
                    trigger_reason = f"{side} {mode.upper()} confirmed via Formula: {entry_formula} (Close). Slope: {data.get('slope_info', 'N/A')}"
                    entry_type = self.monitor._get_user_setting('entry_type',
                                                                str,
                                                                fallback='BUY',
                                                                mode=mode)

                    if not await self.monitor._handle_potential_reversal(
                            side, mode, data, timestamp):
                        continue

                    monitoring_data['baseline_side'] = side
                    monitoring_data['crossover_state'] = 1
                    await self.monitor._confirm_entry(side, data, inst_key,
                                                      data['strike_price'],
                                                      signal_ltp,
                                                      trigger_reason,
                                                      timestamp, entry_type)
                    triggered = True
        return triggered
