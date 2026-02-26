from utils.logger import logger
from hub.event_bus import event_bus
from datetime import datetime, time
import pandas as pd
import asyncio
from hub.indicator_manager import IndicatorManager
from hub.exit_evaluator import ExitEvaluator

class PositionManager:
    def __init__(self, session_or_orchestrator):
        if hasattr(session_or_orchestrator, 'orchestrator_state'):
            self.session = None
            self.orchestrator = session_or_orchestrator
            self.state_manager = self.orchestrator.state_manager
            self.user_id = None
        else:
            self.session = session_or_orchestrator
            self.orchestrator = self.session.orchestrator
            self.state_manager = self.session.state_manager
            self.user_id = getattr(self.session, 'user_id', None)

        self.state = self.orchestrator.orchestrator_state
        self.config_manager = self.orchestrator.config_manager
        self.atm_manager = self.orchestrator.atm_manager
        self.data_manager = self.orchestrator.data_manager
        self.indicator_manager = self.orchestrator.indicator_manager
        self.exit_evaluator = ExitEvaluator(self.orchestrator, self.indicator_manager)
        self.pnl_tracker = self.orchestrator.pnl_tracker
        self._exit_lock = asyncio.Lock()
        self._resumed_priming = False
        self._target_strike_consensus_count = 0
        self._pending_target_strike = None

    async def manage_active_trades_v2(self, timestamp, current_ticks_for_watchlist, current_atm):
        async with self._exit_lock:
            if hasattr(self, '_last_mgmt_ts') and self._last_mgmt_ts == timestamp: return
            self._last_mgmt_ts = timestamp

            if not self._resumed_priming:
                await self._prime_active_positions(timestamp)
                self._resumed_priming = True

            trade_call = self.pnl_tracker.active_call_trade if self.pnl_tracker else None
            trade_put = self.pnl_tracker.active_put_trade if self.pnl_tracker else None

            if self.state_manager.is_in_trade('CALL'):
                await self._manage_single_position('CALL', self.state_manager.call_position, trade_call, timestamp, current_ticks_for_watchlist)
            if self.state_manager.is_in_trade('PUT'):
                await self._manage_single_position('PUT', self.state_manager.put_position, trade_put, timestamp, current_ticks_for_watchlist)

    async def _prime_active_positions(self, timestamp):
        for side in ['CALL', 'PUT']:
            pos = self.state_manager.call_position if side == 'CALL' else self.state_manager.put_position
            if pos and pos.get('exit_monitoring_strike'):
                exit_key = self.atm_manager.find_instrument_key_by_strike(pos['exit_monitoring_strike'], side, self.atm_manager.signal_expiry_date)
                if exit_key:
                    try:
                        await asyncio.wait_for(asyncio.gather(
                            self.data_manager.prime_aggregator(self.orchestrator.entry_aggregator, exit_key, timestamp),
                            self.data_manager.prime_aggregator(self.orchestrator.exit_aggregator, exit_key, timestamp),
                            self.data_manager.prime_aggregator(self.orchestrator.one_min_aggregator, exit_key, timestamp)
                        ), timeout=15.0)
                    except: pass

    async def _manage_single_position(self, direction, position_data, trade, timestamp, ticks):
        if position_data.get('exit_sent'): return

        entry_type = position_data.get('entry_type', 'BUY')
        mode = 'buy' if entry_type == 'BUY' else 'sell'

        # 1. Update LTP & Profit (Traded Strike)
        traded_ltp = await self._get_current_ltp_by_strike(direction, position_data.get('strike_price'), position_data, timestamp, ticks)
        position_data['ltp'] = traded_ltp
        entry_price = position_data.get('entry_price', 0)

        if entry_type == 'SELL':
            current_profit = entry_price - traded_ltp
            min_p = min(traded_ltp, position_data.get('min_price', entry_price))
            position_data['min_price'] = min_p
            peak_price = min_p
        else:
            current_profit = traded_ltp - entry_price
            peak_p = max(traded_ltp, position_data.get('peak_price', entry_price))
            position_data['peak_price'] = peak_p
            peak_price = peak_p

        # 2. Refresh Monitoring Strike Reference
        mon_strike = position_data.get('s1_monitoring_strike') or position_data.get('signal_strike')

        # 3. Strike Transition consensus
        global_target = self.state.v2_target_strike_pair.get('strike') if self.state.v2_target_strike_pair else None

        # 3. Timeframe Resolution
        category = 'exit_indicators/s1_low' if mode == 'buy' else 'exit_indicators/r1_high'
        is_dynamic = self._get_user_setting('dynamic_tf', bool, fallback=False, mode=mode, category=category)

        if is_dynamic:
            is_underwater = current_profit < 0
            if is_underwater:
                primary_tf = self._get_user_setting('fast_tf', int, 1, mode=mode, category=category)
            else:
                primary_tf = self._get_user_setting('tf', int, 5, mode=mode, category=category)
        else:
            primary_tf = self._get_user_setting('tf', int, 3, mode=mode, category=category)

        # 2. Strike Transition consensus
        force_sr_update = False
        if global_target and global_target != mon_strike:
            if global_target != self._pending_target_strike:
                self._pending_target_strike = global_target
                self._target_strike_consensus_count = 1
            else:
                self._target_strike_consensus_count += 1

            if self._target_strike_consensus_count >= self._get_user_setting('strike_switch_consensus', int, 2, mode=mode):
                await self.handle_target_strike_switch(global_target, direction, timestamp)
                position_data['last_sr_move_time'] = timestamp # Use current timestamp for stagnation

                # Recalculate target if enabled
                target_exit_en = self._get_user_setting('enabled', bool, False, mode=mode, category='exit_indicators/target_exit')
                if target_exit_en:
                    entry_ts = pd.to_datetime(position_data.get('entry_timestamp'))
                    if entry_ts.tzinfo is None: entry_ts = entry_ts.tz_localize('Asia/Kolkata')
                    t_ref, _ = await self._calculate_target_on_switch(direction, global_target, entry_ts)
                    if t_ref:
                        # Fetch new S1 immediately for target calculation
                        inst_side = 'CE' if direction == 'CALL' else 'PE'
                        sig_expiry = position_data.get('signal_expiry_date') or self.atm_manager.signal_expiry_date
                        new_key = self.atm_manager.find_instrument_key_by_strike(global_target, inst_side, sig_expiry)
                        if new_key:
                            new_s1, _, _, _, _, _, _, _, _, _ = await self.indicator_manager.get_monotonic_barrier(global_target, new_key, primary_tf, 's1_low', timestamp, direction, self.state_manager.s1_monotonic_tracker)
                            if new_s1: position_data['current_target'] = float(t_ref) + 2 * (float(t_ref) - float(new_s1))

                mon_strike = global_target
                force_sr_update = True
                self._pending_target_strike = None; self._target_strike_consensus_count = 0
        else:
            self._pending_target_strike = None; self._target_strike_consensus_count = 0

        # REFRESH Monitoring LTP & VWAP (using updated mon_strike if switch happened)
        inst_side = 'CE' if direction == 'CALL' else 'PE'
        sig_expiry = position_data.get('signal_expiry_date') or self.atm_manager.signal_expiry_date
        mon_key = self.atm_manager.find_instrument_key_by_strike(mon_strike, inst_side, sig_expiry)
        
        monitoring_ltp = await self._get_current_ltp_by_strike(direction, mon_strike, position_data, timestamp, ticks)
        position_data['monitoring_ltp'] = monitoring_ltp

        if mon_key:
            curr_vwap = await self.indicator_manager.calculate_vwap(mon_key, timestamp)
            if curr_vwap:
                position_data['monitoring_vwap'] = curr_vwap
                if entry_type == 'SELL':
                    v_trough = position_data.get('vwap_trough')
                    position_data['vwap_trough'] = min(curr_vwap, v_trough) if (v_trough is not None and v_trough != 0) else curr_vwap
                else:
                    v_peak = position_data.get('vwap_peak')
                    position_data['vwap_peak'] = max(curr_vwap, v_peak) if (v_peak is not None and v_peak != 0) else curr_vwap

        signal_inst_key = mon_key # Alignment for Evaluation

        s1_breached, r1_breached, r1_target_hit = await self._evaluate_barriers(direction, position_data, signal_inst_key, timestamp, mode, primary_tf, current_profit, global_target, force_sr_update)

        # 4. Exit Evaluator
        exit_triggered, exit_reason, is_sl = await self.exit_evaluator.evaluate_structural_exit_group(
            direction, position_data, timestamp, s1_breached, r1_breached, r1_target_hit,
            signal_inst_key, global_target, primary_tf, traded_ltp, entry_price, peak_price
        )

        # Periodic log: Only show indicators present in the active exit formula
        if (timestamp.second == 0) or self.orchestrator.is_backtest:
            exit_formula = self._get_user_setting('exit_formula', str, fallback='', mode=mode)
            f_lower = exit_formula.lower()

            parts = [f"PnL: {float(position_data.get('pnl', 0)):.2f}"]

            # 1. Structural Barriers (S1/R1)
            show_sr = any(ind in f_lower for ind in ['s1_low', 'r1_high', 's1_double_drop', 'r1_falling', 'r1_low_breach', 's1_confirm'])
            if show_sr:
                lbl = position_data.get('s1_label' if mode == 'buy' else 'r1_label', 'SR')
                val = position_data.get('active_s1' if mode == 'buy' else 'active_r1', 0)
                parts.append(f"Barrier {lbl}: {float(val or 0):.2f}")

            parts.append(f"LTP: {position_data['ltp']:.2f}")

            # 2. VWAP & Slope
            if 'vwap_slope' in f_lower:
                v_curr = position_data.get('monitoring_vwap') or 0.0
                v_peak = position_data.get('vwap_peak' if mode == 'buy' else 'vwap_trough')
                slope_info = position_data.get('slope_info', 'N/A')
                parts.append(f"VWAP: {float(v_curr or 0):.2f} (Peak: {float(v_peak or 0):.2f})")
                parts.append(slope_info)

            logger.info(f"V2 MGMT: [{direction}] {' | '.join(parts)}")

        if exit_triggered:
            position_data.update({'exit_sent': True, 'exit_narrative': exit_reason})
            logger.info(f"V2 MGMT: [{direction}] EXIT TRIGGERED: {exit_reason}")
            await self._exit_trade(direction, traded_ltp, timestamp, exit_reason, is_sl_exit=is_sl)

    async def _get_current_ltp_by_strike(self, direction, strike, position_data, timestamp, ticks):
        if strike in ticks:
            val = ticks[strike].get('ce_ltp' if direction == 'CALL' else 'pe_ltp')
            if val: return float(val)
        
        # Fallback for backtest or missing real-time tick
        sig_expiry = position_data.get('signal_expiry_date') or self.atm_manager.signal_expiry_date
        inst_side = 'CE' if direction == 'CALL' else 'PE'
        inst_key = self.atm_manager.find_instrument_key_by_strike(strike, inst_side, sig_expiry)
        
        if self.orchestrator.is_backtest and inst_key:
            return await self.orchestrator._get_ltp_for_backtest_instrument(inst_key, timestamp) or position_data.get('ltp', 0)
        
        if inst_key:
            live_val = self.state_manager.get_ltp(inst_key)
            if live_val: return float(live_val)
            
        return position_data.get('ltp', 0)

    async def _evaluate_barriers(self, direction, pos_data, key, timestamp, mode, primary_tf, profit, global_target, force_update=False):
        s1_b, r1_b, r1_t = False, False, False

        exit_formula = self._get_user_setting('exit_formula', str, fallback='', mode=mode)
        f_lower = exit_formula.lower()
        sr_indicators = ['s1_low', 'r1_high', 's1_double_drop', 'r1_falling', 'r1_low_breach', 's1_confirm', 'r1_stagnation', 'r1_target']
        needs_sr = any(ind in f_lower for ind in sr_indicators)

        if not needs_sr:
            return s1_b, r1_b, r1_t

        target_exit_en = self._get_user_setting('enabled', bool, False, mode=mode, category='exit_indicators/target_exit')

        # Monotonic update
        anchor = timestamp.replace(hour=9, minute=15, second=0, microsecond=0)
        is_sr_time = int((timestamp - anchor).total_seconds() / 60) % primary_tf == 0
        if is_sr_time or force_update or pos_data.get('active_s1') is None:
            await self._update_monotonic_barriers(direction, pos_data, key, timestamp, mode, primary_tf, target_exit_en)

        # 1m Close breach logic
        if (timestamp.second >= 5) or (self.orchestrator.is_backtest and timestamp.second == 0):
            ohlc_1m = await self.indicator_manager.get_robust_ohlc(key, 1, timestamp)
            if ohlc_1m is not None and not ohlc_1m.empty:
                relevant = ohlc_1m[ohlc_1m.index < timestamp.replace(second=0, microsecond=0)]
                if not relevant.empty and pos_data.get('last_s1_check_1m') != relevant.index[-1]:
                    last_c = relevant.iloc[-1]
                    c_close = float(last_c['close'])
                    if mode == 'buy' and pos_data.get('active_s1') and c_close < pos_data['active_s1']:
                        s1_b = True
                        if not pos_data.get('sl_confirm_pending_low'):
                            pos_data['sl_confirm_pending_low'] = float(last_c['low'])
                    elif mode == 'sell' and pos_data.get('active_r1') and c_close > pos_data['active_r1']:
                        r1_b = True
                        if not pos_data.get('sl_confirm_pending_high'):
                            pos_data['sl_confirm_pending_high'] = float(last_c['high'])

                    # Double Drop logic: Both Primary and Confirmation barriers are breached
                    # Requires 1m candle close to be below both levels.
                    if mode == 'buy':
                        a_s1 = pos_data.get('active_s1')
                        a_s1_c = pos_data.get('active_s1_c')
                        if s1_b and a_s1_c is not None and c_close < a_s1_c:
                            pos_data['s1_double_drop_triggered'] = True
                            pos_data['s1_double_drop_reason'] = f"Double Drop (Close:{c_close:.2f} < S1_{primary_tf}m:{a_s1:.2f} AND S1_confirm:{a_s1_c:.2f})"
                        else:
                            pos_data['s1_double_drop_triggered'] = False
                    else:
                        a_r1 = pos_data.get('active_r1')
                        a_r1_c = pos_data.get('active_r1_c')
                        if r1_b and a_r1_c is not None and c_close > a_r1_c:
                            pos_data['s1_double_drop_triggered'] = True
                            pos_data['s1_double_drop_reason'] = f"Double Rise (Close:{c_close:.2f} > R1_{primary_tf}m:{a_r1:.2f} AND R1_confirm:{a_r1_c:.2f})"
                        else:
                            pos_data['s1_double_drop_triggered'] = False

                    pos_data['last_s1_check_1m'] = relevant.index[-1]

        # R1 Profit
        if self._get_user_setting('exit_use_r1_target', bool, True, mode=mode) and global_target:
            inst_side = 'CE' if direction == 'CALL' else 'PE'
            profit_tf = self._get_user_setting('tf', int, 1, mode=mode, category='exit_indicators/r1_target')
            p_key = self.atm_manager.find_instrument_key_by_strike(global_target, inst_side, self.atm_manager.get_expiry_by_mode(mode, 'signal'))
            if profit >= self._get_user_setting('profit_take_threshold', float, 20.0):
                r1_v, is_est, c_low = await self.indicator_manager.get_r1_profit_status(p_key, profit_tf, timestamp)
                if is_est:
                    mon_ltp = pos_data.get('monitoring_ltp', pos_data['ltp'])
                    if not pos_data.get('r1_profit_threatened'): pos_data.update({'r1_profit_threatened': True, 'r1_profit_breach_candle_low': c_low})
                    elif mon_ltp < pos_data.get('r1_profit_breach_candle_low', 0): r1_t = True

        return s1_b, r1_b, r1_t

    async def _calculate_target_on_switch(self, direction, new_strike, entry_timestamp):
        expiry = self.atm_manager.signal_expiry_date
        inst_side = 'CE' if direction == 'CALL' else 'PE'
        inst_key = self.atm_manager.find_instrument_key_by_strike(new_strike, inst_side, expiry)
        if not inst_key: return None, None
        ohlc_df = await self.data_manager.get_historical_ohlc(inst_key, 1, current_timestamp=entry_timestamp + pd.Timedelta(minutes=1), for_full_day=True)
        if ohlc_df is None or ohlc_df.empty: return None, None
        if ohlc_df.index.tz is None: ohlc_df.index = ohlc_df.index.tz_localize('Asia/Kolkata')
        try: e_hist = ohlc_df.asof(entry_timestamp)['close']
        except: return None, None
        s_hist, _, _, _, _, _, _, _ = await self.indicator_manager.get_sr_status(inst_key, self.orchestrator.s1_low_fast_tf, entry_timestamp)
        if not s_hist: return None, None
        t_ref = e_hist + 2 * (e_hist - s_hist)
        return t_ref, e_hist

    async def _update_monotonic_barriers(self, direction, pos_data, key, timestamp, mode, primary_tf, target_exit_en):
        tracker = self.state_manager.s1_monotonic_tracker
        confirm_tf = self._get_user_setting('tf', int, 1, mode=mode, category='exit_indicators/s1_double_drop')

        async def get_and_adj(tf, ind_type):
            # Unpack all 10 values from get_monotonic_barrier
            mon_s = pos_data.get('s1_monitoring_strike', pos_data.get('signal_strike'))
            res = await self.indicator_manager.get_monotonic_barrier(mon_s, key, tf, ind_type, timestamp, direction, tracker)
            mono, b_raw, lbl, s1_v, r1_v, ph, s1_bh, r1_bl, b_lvl, prev = res

            if target_exit_en and ph != ('R1_TRACKING' if ind_type == 'r1_high' else 'S1_TRACKING') and pos_data.get('current_target'):
                if ind_type == 's1_low' and mode == 'buy' and mono > prev and prev > 0:
                    pos_data['current_target'] = float(pos_data['current_target']) + 2 * (float(pos_data['current_target']) - float(mono))
                elif ind_type == 'r1_high' and mode == 'sell' and mono < prev and prev < 999999:
                    pos_data['current_target'] = float(pos_data['current_target']) - 2 * (float(prev) - float(pos_data['current_target']))

            return mono, b_raw, lbl, ph, s1_bh, r1_bl

        s1_p, s1_p_raw, s1_p_lbl, ph_p, s1_bh, r1_bl = await get_and_adj(primary_tf, 's1_low')
        r1_p, r1_p_raw, r1_p_lbl, ph_r, _, _ = await get_and_adj(primary_tf, 'r1_high')
        s1_c, s1_c_raw, s1_c_lbl, _, _, _ = await get_and_adj(confirm_tf, 's1_low')
        r1_c, r1_c_raw, r1_c_lbl, _, _, _ = await get_and_adj(confirm_tf, 'r1_high')

        # Track SR movement for stagnation exit
        if pos_data.get('active_s1') != s1_p or pos_data.get('active_r1') != r1_p:
            pos_data['last_sr_move_time'] = timestamp

        pos_data.update({
            'active_s1': s1_p,
            'active_r1': r1_p,
            'active_s1_c': s1_c,
            'active_r1_c': r1_c,
            's1_label': s1_p_lbl,
            'r1_label': r1_p_lbl,
            's1_p_raw': s1_p_raw,
            'r1_p_raw': r1_p_raw,
            's1_c_raw': s1_c_raw,
            'r1_c_raw': r1_c_raw,
            'sr_phase': ph_p,
            'active_s1_tf': primary_tf,
            'r1_breach_low': r1_bl,
            's1_breach_high': s1_bh
        })

    async def handle_target_strike_switch(self, new_strike, side, timestamp):
        """Resets structural exit triggers and updates monitoring strike with Hot Handover of peaks."""
        pos_data = self.state_manager.call_position if side == 'CALL' else self.state_manager.put_position
        if not pos_data: return

        entry_type = pos_data.get('entry_type', 'BUY')
        mode = 'buy' if entry_type == 'BUY' else 'sell'
        logger.info(f"V2 MGMT: [{side}] Target strike switch: {pos_data.get('s1_monitoring_strike')} -> {new_strike}. Performing Hot Handover.")

        # Reset monotonic tracker for the NEW strike to avoid morning session context leak
        inst_side = 'CE' if side == 'CALL' else 'PE'
        prefix = f"{float(new_strike):.1f}_{inst_side}_"
        keys_to_del = [k for k in self.state_manager.s1_monotonic_tracker.keys() if k.startswith(prefix)]
        for k in keys_to_del:
            del self.state_manager.s1_monotonic_tracker[k]
            logger.debug(f"V2: Reset monotonic tracker for {k} on strike switch.")

        # CRITICAL: Immediately invalidate the CLOSE-mode OHLC peak/trough for the OLD instrument.
        # The CLOSE mode exit check uses 'vwap_peak_ohlc' (a separate key from 'vwap_peak').
        # Without this reset, the stale OHLC peak from the old strike is compared against
        # the new strike's VWAP, causing a false cross-instrument drawdown and premature exit.
        # Resetting to None puts the exit check into "Initializing" mode until the Hot Handover
        # recalculates the correct peak for the new instrument below.
        ohlc_peak_key = 'vwap_peak_ohlc' if mode == 'buy' else 'vwap_trough_ohlc'
        pos_data[ohlc_peak_key] = None
        pos_data['_vwap_close_last_candle'] = None  # Force re-evaluation at next candle boundary
        logger.debug(f"V2: Hot Handover [{side}] Reset {ohlc_peak_key} and _vwap_close_last_candle to prevent cross-instrument false exit.")

        # --- Hot Handover: Fetch historical peaks for the new strike ---
        sig_expiry = pos_data.get('signal_expiry_date') or self.atm_manager.signal_expiry_date
        new_key = self.atm_manager.find_instrument_key_by_strike(new_strike, inst_side, sig_expiry)
        
        entry_ts = pos_data.get('entry_timestamp')
        if entry_ts:
            # Fetch 1m OHLC for the new strike from entry until now (include current candle for Hot Handover)
            ohlc_df = await self.data_manager.get_historical_ohlc(new_key, 1, timestamp, for_full_day=True, include_current=True)
            if ohlc_df is not None and not ohlc_df.empty:
                # Ensure DatetimeIndex and localization
                try:
                    ohlc_df.index = pd.to_datetime(ohlc_df.index, utc=True).tz_convert('Asia/Kolkata')
                except:
                    # Fallback for already localized or other types
                    if getattr(ohlc_df.index, 'tz', None) is None:
                        ohlc_df.index = pd.to_datetime(ohlc_df.index).tz_localize('Asia/Kolkata')
                
                # Normalize timestamp and entry_ts to match ohlc_df.index tz
                def to_aware(ts):
                    t = pd.Timestamp(ts)
                    if t.tzinfo is None: return t.tz_localize('Asia/Kolkata')
                    return t.tz_convert('Asia/Kolkata')

                entry_ts_aware = to_aware(entry_ts)
                timestamp_aware = to_aware(timestamp)
                # Use floor of entry time to include the candle where trade started
                entry_floor = entry_ts_aware.replace(second=0, microsecond=0)

                # Hot Handover: Look at data from entry till CURRENT TIME - 1 minute (as requested)
                # Exclusion of current minute ensures we only use finalized historical VWAPs for the peak reference
                relevant_past = ohlc_df[(ohlc_df.index >= entry_floor) & (ohlc_df.index < timestamp_aware.replace(second=0, microsecond=0))]
                
                # Also consider the current minute for LTP peaks if available
                relevant_full = ohlc_df[(ohlc_df.index >= entry_floor) & (ohlc_df.index <= timestamp_aware)]

                if not relevant_full.empty:
                    # Update LTP peaks using full trade duration
                    if entry_type == 'SELL':
                        h_min = float(relevant_full['low'].min())
                        pos_data['min_price'] = h_min
                        logger.info(f"V2: Hot Handover [{side}] Min Price updated to {h_min:.2f} (Duration: {entry_floor.time()} - {timestamp_aware.time()})")
                    else:
                        h_max = float(relevant_full['high'].max())
                        pos_data['peak_price'] = h_max
                        logger.info(f"V2: Hot Handover [{side}] Peak Price updated to {h_max:.2f} (Duration: {entry_floor.time()} - {timestamp_aware.time()})")

                if not relevant_past.empty:
                    # Update VWAP peaks using historical data till Current Time - 1
                    # CRITICAL: We must filter for TODAY'S session data ONLY to calculate DAILY VWAP.
                    today = timestamp_aware.date()
                    # Start from 9:15 AM of the current day
                    session_start = timestamp_aware.replace(hour=9, minute=15, second=0, microsecond=0)
                    full_day_relevant = ohlc_df[(ohlc_df.index >= session_start) & (ohlc_df.index <= timestamp_aware)]
                    
                    if full_day_relevant.empty:
                         logger.warning(f"V2: Hot Handover [{side}] FAILED: No data found for today's session starting {session_start}")
                         return

                    full_day_tp = (full_day_relevant['high'] + full_day_relevant['low'] + full_day_relevant['close']) / 3
                    full_day_vol = full_day_relevant['volume']
                    cum_pv = (full_day_tp * full_day_vol).cumsum()
                    cum_vol = full_day_vol.cumsum()
                    vwaps = cum_pv / cum_vol
                    
                    # Filter for the past trade duration (excluding current minute for handover peak reference)
                    trade_vwaps = vwaps[(vwaps.index >= entry_floor) & (vwaps.index < timestamp_aware.replace(second=0, microsecond=0))]
                    if not trade_vwaps.empty:
                        if entry_type == 'SELL':
                            h_v_trough = float(trade_vwaps.min())
                            pos_data['vwap_trough'] = h_v_trough
                            logger.info(f"V2: Hot Handover [{side}] VWAP Trough updated to {h_v_trough:.2f} (Calculated from {len(trade_vwaps)} finalized candles between {trade_vwaps.index[0]} and {trade_vwaps.index[-1]})")
                        else:
                            h_v_peak = float(trade_vwaps.max())
                            pos_data['vwap_peak'] = h_v_peak
                            logger.info(f"V2: Hot Handover [{side}] VWAP Peak updated to {h_v_peak:.2f} (Calculated from {len(trade_vwaps)} finalized candles between {trade_vwaps.index[0]} and {trade_vwaps.index[-1]})")

        pos_data.update({
            's1_monitoring_strike': new_strike,
            's1_double_drop_triggered': False,
            'sl_confirm_pending_low': None,
            'sl_confirm_pending_high': None,
            'r1_breach_low': None,
            's1_breach_high': None,
            'last_sr_move_time': None,
            'signal_expiry_date': self.atm_manager.get_expiry_by_mode(mode, 'signal')
        })

    def _get_user_setting(self, key, type_func=str, fallback=None, section='settings', mode=None, category=None):
        val = None
        if mode:
            # A. Mode specific path
            path = f"{self.orchestrator.instrument_name}.{mode}.{category}.{key}" if category else f"{self.orchestrator.instrument_name}.{mode}.{key}"
            val = self.orchestrator.json_config.get_value(path)

            # B. Instrument level path fallback
            if val is None:
                path = f"{self.orchestrator.instrument_name}.{key}"
                val = self.orchestrator.json_config.get_value(path)

            if val is not None: return type_func(val)
        if self.session and self.session.strategy_config:
            db_key = key
            if key == 'exit_timeframe_minutes': db_key = 'exit_tf_minutes'
            elif key == 'entry_timeframe_minutes': db_key = 'entry_tf_minutes'
            val = self.session.strategy_config.get(db_key)
        if val is None:
            raw = self.config_manager.get(section, key, fallback=None)
            if raw is not None:
                raw = str(raw).split('#')[0].strip()
                if type_func == bool: val = raw.lower() in ('true', '1', 'yes', 'on')
                else: val = type_func(raw)
            else: val = fallback
        return val if val is not None else fallback

    async def _exit_trade(self, side, ltp, timestamp, reason='Stop Loss', is_sl_exit=False):
        import datetime
        logger.info(f"V2: Requesting EXIT for {side} at {ltp:.2f}. Reason: {reason}")

        # Reset 9:15 Breach Gate ONLY on hard SL (not TSL) as per user distinction
        # User: "if ltrade is runnign and sl is hit .it will again wait for 9.15 breach"
        # User: "if tsl is hit tehn it will chekc that the current valeu is above 9.15 it will always take tarde in ce"
        is_hard_sl = ("Stop Loss" in reason or is_sl_exit) and "Trailing" not in reason

        gate_enabled = self._get_user_setting('range_915_gate_enabled', bool, fallback=True)

        if is_hard_sl:
            if gate_enabled:
                logger.info(f"V2 MGMT: [{side}] Stop Loss detected. Resetting 9:15 Range Breach flags for re-evaluation.")
                self.state_manager.range_915_breached_up = False
                self.state_manager.range_915_breached_down = False
        elif gate_enabled:
            logger.info(f"V2 MGMT: [{side}] Exit ({reason}). Preserving 9:15 Breach flags for potential re-entry.")

        pos_data = self.state_manager.call_position if side == 'CALL' else self.state_manager.put_position
        if pos_data: pos_data['exit_sent'] = True

        # Set 60-second re-entry cooldown on the exited side (live mode only)
        if not self.orchestrator.is_backtest:
            cooldown_attr = 'call_cooldown_until' if side == 'CALL' else 'put_cooldown_until'
            cooldown_ts = timestamp + pd.Timedelta(seconds=60)
            setattr(self.state_manager, cooldown_attr, cooldown_ts)
            logger.info(f"V2 MGMT: [{side}] Re-entry cooldown active until {cooldown_ts.strftime('%H:%M:%S')} (60s)")

        if self.orchestrator.is_backtest:
            strategy_log = pos_data.get('exit_narrative', "") if pos_data else ""
            await self.state_manager.reset_trade_state(side, is_backtest=True)
            if self.pnl_tracker.is_trade_active(side): self.pnl_tracker.exit_trade(side, ltp, timestamp, reason, strategy_log=strategy_log)
            self.state_manager.trade_closed_event.set()
        else:
            instrument_key = pos_data.get('instrument_key') if pos_data else None
            if instrument_key:
                await event_bus.publish('REMOVE_FROM_WATCHLIST', {'instrument_key': instrument_key})
                contract = self.atm_manager.get_contract_by_instrument_key(instrument_key)
                await event_bus.publish('EXIT_TRADE_REQUEST', {'user_id': self.user_id, 'instrument_name': self.orchestrator.primary_instrument, 'side': side, 'ltp': ltp, 'reason': reason, 'contract': contract, 'signal_expiry_date': self.atm_manager.signal_expiry_date, 'strategy_log': pos_data.get('exit_narrative', "")})
