from utils.logger import logger
from utils.profiler import profile_microseconds
from datetime import datetime
import pandas as pd
import asyncio
import re
import pytz
from hub.indicator_manager import IndicatorManager
from hub.pattern_matcher import PatternMatcher
from hub.signal_evaluator import SignalEvaluator
from hub.breach_gate_manager import BreachGateManager

class SignalMonitor:
    def __init__(self, session_or_orchestrator):
        # Multi-tenant: Accepts either a UserSession (commercial) or BaseOrchestrator (legacy)
        if hasattr(session_or_orchestrator, 'orchestrator_state'):
            # This is the BaseOrchestrator
            self.session = None
            self.orchestrator = session_or_orchestrator
            self.state_manager = self.orchestrator.state_manager
            self.user_id = None
        else:
            # This is a UserSession
            self.session = session_or_orchestrator
            self.orchestrator = self.session.orchestrator
            self.state_manager = self.session.state_manager
            self.user_id = getattr(self.session, 'user_id', None)

        self.state = self.orchestrator.orchestrator_state
        self.config_manager = self.orchestrator.config_manager
        self.atm_manager = self.orchestrator.atm_manager
        self.data_manager = self.orchestrator.data_manager
        self.indicator_manager = self.orchestrator.indicator_manager
        self.pattern_matcher = PatternMatcher(self.orchestrator, self.indicator_manager)
        self.gate_manager = BreachGateManager(self.orchestrator)
        self.evaluator = SignalEvaluator(self)
        self.last_target_strike = None
        self._r1_cache = {}
        self._last_api_call_time = None
        self._breach_lock = asyncio.Lock()
        self._resumed_priming = False
        self._target_strike_consensus_count = 0
        self._pending_target_strike = None
        self._last_strike_switch_time = None

    def is_monitoring(self):
        return bool(self.state_manager.dual_sr_monitoring_data)

    def _get_user_setting(self, key, type_func=str, fallback=None, section='settings', mode=None, category=None):
        """Helper to get strategy settings with JSON priority, then user-specific DB overrides, then INI."""
        val = None

        # 1. Try JSON config first if mode is provided
        if mode:
            # A. Mode specific path
            if category: path = f"{self.orchestrator.instrument_name}.{mode}.{category}.{key}"
            else: path = f"{self.orchestrator.instrument_name}.{mode}.{key}"

            val = self.orchestrator.json_config.get_value(path)

            # B. Instrument level path fallback
            if val is None:
                path = f"{self.orchestrator.instrument_name}.{key}"
                val = self.orchestrator.json_config.get_value(path)

            if val is not None:
                return type_func(val)

        # 2. Try DB overrides
        if self.session and self.session.strategy_config:
            db_key = key
            if key == 'entry_timeframe_minutes': db_key = 'entry_tf_minutes'
            val = self.session.strategy_config.get(db_key)

        # 3. Try INI settings
        if val is None:
            raw_val = self.config_manager.get(section, key, fallback=None)
            if raw_val is not None:
                # Strip inline comments
                val = str(raw_val).split('#')[0].strip()

                # Manual type conversion to handle comments and booleans correctly
                if type_func == bool:
                    val = val.lower() in ('true', '1', 'yes', 'on')
                elif type_func == int:
                    val = int(val)
                elif type_func == float:
                    val = float(val)
            else:
                val = fallback

        return type_func(val) if val is not None else fallback

    def _evaluate_formula(self, formula, results_dict):
        """Delegates formula evaluation to JsonConfigManager."""
        return self.orchestrator.json_config.evaluate_formula(formula, results_dict)

    @profile_microseconds
    async def monitor_crossover(self, timestamp, target_strike_pair, current_atm):
        async with self._breach_lock:
            # DOUBLE CHECK: Has monitoring already been started for this strike?
            currently_monitored_strike = self.state_manager.dual_sr_monitoring_data.get('target_strike') if self.state_manager.dual_sr_monitoring_data else None
            if target_strike_pair['strike'] == currently_monitored_strike:
                return

            # DOUBLE CHECK: Are we already fully in trades (both sides)?
            # If at least one side is not in trade, we should continue monitoring/re-initiating.
            if self.state_manager.is_in_trade('CALL') and self.state_manager.is_in_trade('PUT'):
                return

            await self._monitor_crossover_unlocked(timestamp, target_strike_pair, current_atm)

    async def _monitor_crossover_unlocked(self, timestamp, target_strike_pair, current_atm):
        task_id = id(asyncio.current_task())
        user_display = self.user_id if self.user_id else "Global"
        logger.debug(f"[Task {task_id}] V2: Initiating Crossover Monitoring for strike pair {target_strike_pair['strike']} (User: {user_display}).")

        self._resumed_priming = True # Mark as primed

        # PRESERVE STICKY STATE ACROSS STRIKE CHANGES
        old_data = self.state_manager.dual_sr_monitoring_data or {}

        # Capture flags for Sticky Re-entry after SL
        awaiting_fresh_vwap_breach = old_data.get('awaiting_fresh_vwap_breach', False)
        sticky_dip_confirmed = old_data.get('sticky_dip_confirmed', False)

        # Capture confirmation from side level
        ce_confirmed = old_data.get('ce_data', {}).get('entry_confirmed', False)
        pe_confirmed = old_data.get('pe_data', {}).get('entry_confirmed', False)

        ce_reason = old_data.get('ce_data', {}).get('confirmed_pattern_reason')
        pe_reason = old_data.get('pe_data', {}).get('confirmed_pattern_reason')

        # PRE-INITIALIZE state to signal monitoring is in progress
        self.state_manager.dual_sr_monitoring_data = {
            'target_strike': target_strike_pair['strike'],
            'status': 'starting',
            'monitoring_start_time': timestamp,
            'awaiting_fresh_vwap_breach': awaiting_fresh_vwap_breach,
            'sticky_dip_confirmed': sticky_dip_confirmed
        }

        self.state_manager.v2_monitoring_atm_strike = current_atm

        signal_expiry = self.atm_manager.signal_expiry_date
        if not signal_expiry:
            logger.warning("V2: Cannot generate trade signal, no valid signal expiry date found.")
            return

        target_strike = target_strike_pair['strike']

        ce_instrument_key = self.atm_manager.find_instrument_key_by_strike(target_strike, 'CALL', signal_expiry)
        pe_instrument_key = self.atm_manager.find_instrument_key_by_strike(target_strike, 'PUT', signal_expiry)
        if not ce_instrument_key or not pe_instrument_key:
            logger.warning(f"V2: Could not find instrument keys for CE/PE at {target_strike} for expiry {signal_expiry}")
            return

        if self.state.current_target_strike != target_strike:
            if self.state.current_target_strike is not None:
                logger.info(f"V2: Switching monitoring strike {self.state.current_target_strike} -> {target_strike}")
                self.data_manager.clear_api_ohlc_cache_for_strike(self.state.current_target_strike, signal_expiry)
            self.state.current_target_strike = target_strike

        # PROACTIVELY PRIME AGGREGATORS
        # This ensures we have historical data for pattern calculation and immediate slope/S&R
        # calculation on every strike switch.
        try:
            # Concurrent priming for CE and PE to save time
            await asyncio.wait_for(asyncio.gather(
                self.data_manager.prime_aggregator(self.orchestrator.entry_aggregator, ce_instrument_key, timestamp),
                self.data_manager.prime_aggregator(self.orchestrator.entry_aggregator, pe_instrument_key, timestamp),
                self.data_manager.prime_aggregator(self.orchestrator.exit_aggregator, ce_instrument_key, timestamp),
                self.data_manager.prime_aggregator(self.orchestrator.exit_aggregator, pe_instrument_key, timestamp),
                self.data_manager.prime_aggregator(self.orchestrator.one_min_aggregator, ce_instrument_key, timestamp),
                self.data_manager.prime_aggregator(self.orchestrator.one_min_aggregator, pe_instrument_key, timestamp),
                self.data_manager.prime_aggregator(self.orchestrator.five_min_aggregator, ce_instrument_key, timestamp),
                self.data_manager.prime_aggregator(self.orchestrator.five_min_aggregator, pe_instrument_key, timestamp)
            ), timeout=25.0)
        except asyncio.TimeoutError:
            logger.error(f"V2: TIMEOUT during proactively priming for strike {target_strike}. Signals might be delayed.")
        except Exception as e:
            logger.error(f"V2: Error during proactively priming: {e}")

        self.last_target_strike = target_strike

        # INITIAL STATE: Determine which side is currently higher to wait for crossover
        ce_ltp = target_strike_pair.get('ce_ltp')
        pe_ltp = target_strike_pair.get('pe_ltp')

        initial_higher_side = None
        if ce_ltp and pe_ltp:
            initial_higher_side = 'CE' if ce_ltp > pe_ltp else 'PE'

        if initial_higher_side:
            ce_ltp_val = float(ce_ltp) if ce_ltp is not None else 0.0
            pe_ltp_val = float(pe_ltp) if pe_ltp is not None else 0.0
            logger.debug(f"V2: Initial state for strike {target_strike}: {initial_higher_side} is higher (Current LTPs - CE: {ce_ltp_val:.2f}, PE: {pe_ltp_val:.2f})")

        def get_fresh_criteria(side_key):
            old_side_data = old_data.get(side_key, {})
            old_criteria = old_side_data.get('criteria_state', {})
            # PRESERVE only pattern across strike changes as requested.
            # Reset others as they are instrument-specific.
            # ALSO RESET confirmed state for indicators to avoid ghost entries on new strike.
            return {
                'pattern': old_criteria.get('pattern', False),
                'vwap': False,
                'vwap_slope': False,
                'r1_high': False,
                's1_low': False
            }

        ce_monitoring_data = {
            'instrument_key': ce_instrument_key,
            'last_close': None, 'direction': 'CALL', 'strike_price': target_strike,
            'entry_confirmed': ce_confirmed,
            'confirmed_pattern_reason': ce_reason,
            'vwap_trend_counter': 0,
            'criteria_state': get_fresh_criteria('ce_data'),
            'vwap': None, 'r1_high': None, 's1_low': None,
            'r1_label': 'R1', 's1_label': 'S1', 'r1_phase': '', 's1_phase': '',
            'slope_info': 'Initializing...'
        }
        pe_monitoring_data = {
            'instrument_key': pe_instrument_key,
            'last_close': None, 'direction': 'PUT', 'strike_price': target_strike,
            'entry_confirmed': pe_confirmed,
            'confirmed_pattern_reason': pe_reason,
            'vwap_trend_counter': 0,
            'criteria_state': get_fresh_criteria('pe_data'),
            'vwap': None, 'r1_high': None, 's1_low': None,
            'r1_label': 'R1', 's1_label': 'S1', 'r1_phase': '', 's1_phase': '',
            'slope_info': 'Initializing...'
        }

        # FINAL INITIALIZATION
        self.state_manager.dual_sr_monitoring_data.update({
            'status': 'active',
            'trigger_time': timestamp,
            'strike_switch_time': timestamp,
            'initial_higher_side': initial_higher_side,
            'ce_data': ce_monitoring_data,
            'pe_data': pe_monitoring_data,
            'baseline_side': None,
            'crossover_state': 0
        })

        # Clear S&R Cache to force recalculation for the new strike/keys
        if hasattr(self, '_sr_cache'):
            self._sr_cache.clear()

        if not self.orchestrator.is_backtest:
            await self._subscribe_to_monitoring_instruments(target_strike)

        logger.debug(f"[Task {task_id}] V2: Dual Crossover Monitoring is ACTIVE for strike {target_strike}.")


    async def _check_barrier_breach(self, inst_key, ltp, indicator_type, tf, timestamp, mode='buy'):
        """
        Checks if R1 or S1 barrier is breached based on 4 criteria.
        Returns (is_breached, status_msg, barrier_val, s1_v, r1_v, phase, s1_h, r1_l, breakout_level)
        """
        b_val, label, s1_v, r1_v, phase, s1_h, r1_l, b_lvl = await self.indicator_manager.get_nuanced_barrier(inst_key, indicator_type, tf, timestamp)

        if b_val is None:
            return False, f"{indicator_type.upper()} N/A", None, s1_v, r1_v, phase, s1_h, r1_l, b_lvl

        f_ltp = float(ltp)
        is_r1 = (indicator_type == 'r1_high')
        
        # --- THE 4 BREACH CRITERIA ---
        is_breached = False
        
        if is_r1:
            # Established breach
            if r1_v and f_ltp >= r1_v: is_breached = True
            # Tracking breach (PrevHigh)
            elif phase == 'R1_TRACKING':
                if b_val and f_ltp >= b_val: is_breached = True # PrevHigh breach
        else: # S1_LOW
            if s1_v and f_ltp <= s1_v: is_breached = True
            elif phase == 'S1_TRACKING':
                if b_val and f_ltp <= b_val: is_breached = True # PrevLow breach

        status_msg = f"{label} Breach {'OK' if is_breached else 'Wait'} (LTP:{f_ltp:.1f}, {label}:{b_val:.1f}, Phase:{phase})"
        return is_breached, status_msg, b_val, s1_v, r1_v, phase, s1_h, r1_l, b_lvl

    async def _subscribe_to_monitoring_instruments(self, target_strike):
        """
        Subscribes to Signal Expiry instruments for monitoring.
        ITM Trade instruments are ONLY subscribed at the moment of breach
        using the real-time Index price.
        """
        keys_to_subscribe = set()
        signal_expiry = self.atm_manager.signal_expiry_date

        ce_signal_key = self.atm_manager.find_instrument_key_by_strike(target_strike, 'CALL', signal_expiry)
        pe_signal_key = self.atm_manager.find_instrument_key_by_strike(target_strike, 'PUT', signal_expiry)

        if ce_signal_key: keys_to_subscribe.add(ce_signal_key)
        if pe_signal_key: keys_to_subscribe.add(pe_signal_key)

        if keys_to_subscribe:
            logger.debug(f"V2: Subscribing to Signal instruments for target strike {target_strike}: {list(keys_to_subscribe)}")
            self.orchestrator.websocket.subscribe(list(keys_to_subscribe))


    async def _check_915_range_breach(self, timestamp):
        await self.gate_manager.check_915_range_breach(timestamp)

    @profile_microseconds
    async def check_crossover_breach(self, timestamp, current_atm):
        """
        The main entry point for the TickProcessor. It checks for new signals, manages
        monitoring state, and triggers crossover checks.
        """
        async with self._breach_lock:
            # Check JSON active modes
            active_modes = self.orchestrator.json_config.get_active_modes(self.orchestrator.instrument_name)
            if not active_modes: active_modes = ['buy']

            # 0. Global 9:15 Breach Gate Check (if enabled)
            ref_mode = active_modes[0]
            gate_enabled = self._get_user_setting('range_915_gate_enabled', bool, fallback=True, mode=ref_mode)
            if gate_enabled:
                await self._check_915_range_breach(timestamp)
            else:
                # Bypass: Ensure flags are True if gate is disabled
                self.state_manager.range_915_breached_up = True
                self.state_manager.range_915_breached_down = True

            currently_monitored_strike = self.state_manager.dual_sr_monitoring_data.get('target_strike') if self.state_manager.dual_sr_monitoring_data else None

            # 1. TARGET STRIKE SEARCH & SWITCH (Perform first so logs/eval use the latest strike)
            # Use first active mode's signal expiry for common crossover monitoring
            ref_mode = active_modes[0]
            sig_expiry = self.atm_manager.get_expiry_by_mode(ref_mode, 'signal')
            v2_target_pair = self.orchestrator.strike_manager.find_and_get_target_strike_pair(sig_expiry)

            is_switching = False
            if v2_target_pair:
                new_target_strike = v2_target_pair['strike']

                # STRIKE SWITCH CONSENSUS: Prevent ATM jitter
                # Requires BOTH: tick consensus AND 60-second cooldown since last switch
                if new_target_strike != currently_monitored_strike:
                    if new_target_strike != self._pending_target_strike:
                        self._pending_target_strike = new_target_strike
                        self._target_strike_consensus_count = 1
                    else:
                        self._target_strike_consensus_count += 1

                    consensus_threshold = self._get_user_setting('strike_switch_consensus', int, fallback=2, mode=ref_mode)
                    ticks_ok = self._target_strike_consensus_count >= consensus_threshold

                    elapsed_since_switch = (
                        (timestamp - self._last_strike_switch_time).total_seconds()
                        if self._last_strike_switch_time is not None else 9999
                    )
                    time_ok = self.orchestrator.is_backtest or elapsed_since_switch >= 60

                    if ticks_ok and time_ok:
                        user_display = self.user_id if self.user_id else "Global"
                        logger.info(f"V2: Target strike change confirmed ({self._target_strike_consensus_count} ticks, {elapsed_since_switch:.0f}s since last switch): {currently_monitored_strike} -> {new_target_strike} (User: {user_display})")
                        await self._monitor_crossover_unlocked(timestamp, v2_target_pair, current_atm)
                        currently_monitored_strike = new_target_strike
                        is_switching = True
                        self._last_strike_switch_time = timestamp
                        self._pending_target_strike = None
                        self._target_strike_consensus_count = 0
                    elif ticks_ok and not time_ok:
                        logger.debug(f"V2: Strike switch {currently_monitored_strike} -> {new_target_strike} ticks confirmed but cooldown active ({60 - elapsed_since_switch:.0f}s remaining). Holding.")
                else:
                    self._pending_target_strike = None
                    self._target_strike_consensus_count = 0

            # 2. EVALUATION & STATUS LOG
            if self.state_manager.dual_sr_monitoring_data:
                # REAL-TIME TICK EVALUATION: Update indicator states for TICK-mode components
                await self._update_tick_criteria_and_evaluate(timestamp, active_modes)

            # 3. PERIODIC STATUS LOG
            r1_tf = self._get_user_setting('tf', int, fallback=3, mode=ref_mode, category='indicators/r1_high')
            is_log_minute = (timestamp.minute % r1_tf == 0) and (timestamp.second == 0)
            last_status_log_ts = getattr(self, '_last_status_log_ts', None)

            if (is_log_minute or is_switching) and last_status_log_ts != timestamp:
                self._last_status_log_ts = timestamp
                monitor_data = self.state_manager.dual_sr_monitoring_data
                if monitor_data:
                    ce_data = monitor_data.get('ce_data')
                    pe_data = monitor_data.get('pe_data')
                    if ce_data and pe_data:
                        user_display = self.user_id if self.user_id else "Global"

                        entry_formula = self._get_user_setting('entry_formula', str, fallback='pattern', mode=ref_mode)
                        ce_state = await self.format_state(ce_data, 'CALL', entry_formula, timestamp)
                        pe_state = await self.format_state(pe_data, 'PUT', entry_formula, timestamp)
                        sw_label = " (SWITCHED)" if is_switching else ""

                        # 9:15 Range Info
                        idx_ltp = self.state_manager.index_price or 0.0
                        gate_enabled = self._get_user_setting('range_915_gate_enabled', bool, fallback=True, mode=ref_mode)
                        if gate_enabled:
                            idx_high, idx_low = await self.indicator_manager.get_index_915_range(self.orchestrator.index_instrument_key, timestamp)
                            range_str = f"9:15R({idx_low:.1f}-{idx_high:.1f})" if idx_high else "9:15R(Wait)"
                            status = 'NONE'
                            if getattr(self.state_manager, 'range_915_breached_up', False): status = 'UP'
                            elif getattr(self.state_manager, 'range_915_breached_down', False): status = 'DOWN'
                            gate_str = f" | {range_str} | Breach:{status}"
                        else:
                            gate_str = ""

                        logger.info(f"V2 MONITORING Status [{user_display}]: Strike {ce_data['strike_price']}{sw_label} | Index: {idx_ltp:.2f}{gate_str} | "
                                     f"CE: {ce_state} (LTP: {float(self.state_manager.get_ltp(ce_data['instrument_key']) or 0):.2f}) | "
                                     f"PE: {pe_state} (LTP: {float(self.state_manager.get_ltp(pe_data['instrument_key']) or 0):.2f})")

            # 4. RESUME MONITORING AFTER RESTART
            if currently_monitored_strike and not self._resumed_priming:
                ce_data = self.state_manager.dual_sr_monitoring_data.get('ce_data')
                pe_data = self.state_manager.dual_sr_monitoring_data.get('pe_data')

                for side_data in [ce_data, pe_data]:
                    if side_data:
                        inst_key = side_data.get('instrument_key')
                        if not inst_key:
                            logger.warning(f"V2: Resume-priming skipped for side — instrument_key is None in saved monitoring data.")
                            continue
                        try:
                            await asyncio.wait_for(asyncio.gather(
                                self.data_manager.prime_aggregator(self.orchestrator.entry_aggregator, inst_key, timestamp),
                                self.data_manager.prime_aggregator(self.orchestrator.exit_aggregator, inst_key, timestamp),
                                self.data_manager.prime_aggregator(self.orchestrator.one_min_aggregator, inst_key, timestamp)
                            ), timeout=15.0)
                        except: pass

                self._resumed_priming = True
                await self._subscribe_to_monitoring_instruments(currently_monitored_strike)

            if self.state_manager.dual_sr_monitoring_data:
                # 4. PATTERN MONITORING: 1-minute candle based
                await self._check_crossover_for_all_sides(timestamp, is_switching, active_modes)

    async def _update_tick_criteria_and_evaluate(self, timestamp, active_modes=['buy']):
        """Delegates tick evaluation to SignalEvaluator."""
        await self.evaluator.evaluate_tick_criteria(timestamp, active_modes)

    async def format_state(self, side_data, direction, formula, timestamp):
        """Helper to format the side state for logging, respecting the 9:15 gate."""
        mode = 'buy' if direction == 'CALL' else 'sell' # Approximate for logging

        if self.state_manager.is_in_trade(direction):
            pos = self.state_manager.call_position if direction == 'CALL' else self.state_manager.put_position
            ex_formula = self._get_user_setting('exit_formula', str, fallback='', mode=mode).lower()
            
            telemetry = []
            if 'vwap_slope' in ex_formula:
                v_curr = pos.get('monitoring_vwap')
                if not v_curr or v_curr == 0:
                    # Fallback for very first tick of trade or strike switch
                    mon_strike = pos.get('s1_monitoring_strike') or pos.get('signal_strike')
                    inst_side = 'CE' if direction == 'CALL' else 'PE'
                    sig_expiry = pos.get('signal_expiry_date') or self.atm_manager.signal_expiry_date
                    mon_key = self.atm_manager.find_instrument_key_by_strike(mon_strike, inst_side, sig_expiry)
                    v_curr = await self.indicator_manager.calculate_vwap(mon_key, timestamp) or 0.0
                
                entry_type = pos.get('entry_type', 'BUY')
                v_ref = pos.get('vwap_peak') if entry_type == 'BUY' else pos.get('vwap_trough')
                v_ref = v_ref or 0.0
                telemetry.append(f"V:{v_curr:.1f}, VP:{v_ref:.1f}")
            
            if 'tsl' in ex_formula or 'atr_tsl' in ex_formula:
                p_ref = pos.get('peak_price') if direction == 'CALL' else pos.get('min_price')
                p_ref = p_ref or 0.0
                telemetry.append(f"LP:{p_ref:.1f}")

            t_str = f" [{', '.join(telemetry)}]" if telemetry else ""
            return f"(In Trade){t_str}"
        gate_enabled = self._get_user_setting('range_915_gate_enabled', bool, fallback=True, mode=mode)

        if gate_enabled:
            side_short = 'CE' if direction == 'CALL' else 'PE'
            idx_ltp = self.state_manager.index_price or 0.0
            idx_high, idx_low = await self.indicator_manager.get_index_915_range(self.orchestrator.index_instrument_key, timestamp)

            if not self.gate_manager.is_side_permitted(side_short, idx_ltp, idx_high, idx_low):
                if not (getattr(self.state_manager, 'range_915_breached_up', False) or getattr(self.state_manager, 'range_915_breached_down', False)):
                    return "(GATED:WaitBreach)"
                return "(GATED:Offside)"

        parts = []
        f_lower = formula.lower()
        for k, v in side_data.get('criteria_state', {}).items():
            if not re.search(rf'\b{k}\b', f_lower):
                continue

            if k == 'r1_high':
                label = side_data.get('r1_label', 'R1')
                val = side_data.get('r1_high')
                phase = side_data.get('r1_phase', '')
                ph_str = f"|{phase}" if phase else ""
                f_val = float(val) if val is not None else 0.0
                parts.append(f"{label}({f_val:.1f}){ph_str}:{v}")
            elif k == 's1_low':
                label = side_data.get('s1_label', 'S1')
                val = side_data.get('s1_low')
                phase = side_data.get('s1_phase', '')
                ph_str = f"|{phase}" if phase else ""
                f_val = float(val) if val is not None else 0.0
                parts.append(f"{label}({f_val:.1f}){ph_str}:{v}")
            elif k == 'vwap_slope':
                info = side_data.get('slope_info', 'N/A')
                parts.append(f"slope({info}):{v}")
            else:
                parts.append(f"{k}:{v}")
        return ",".join(parts)

    async def _handle_potential_reversal(self, side, mode, data, timestamp):
        """
        Handles closing the opposite side trade if reversal is enabled and in formula.
        Returns True if entry on this side is permitted (either no conflict, or reversal happened).
        Returns False if entry is blocked by simultaneous trade rules.
        """
        monitoring_data = self.state_manager.dual_sr_monitoring_data
        opposite_side = 'PUT' if side == 'CE' else 'CALL'
        
        if not self.state_manager.is_in_trade(opposite_side):
            return True # No conflict

        # Conflict exists! Check simultaneous/reversal rules.
        allow_simultaneous = self._get_user_setting('allow_simultaneous_trades', bool, fallback=False, mode=mode)
        if allow_simultaneous:
            return True # Both can run

        # Simultaneous NOT allowed. Must be a reversal to proceed.
        exit_use_reversal = self._get_user_setting('enabled', bool, fallback=False, mode=mode, category='exit_indicators/reversal')
        if not exit_use_reversal:
            logger.info(f"V2: {side} {mode.upper()} triggered, but BLOCKED by simultaneous trade limit (Reversal disabled).")
            return False

        # Reversal setting is ON. Check if the active trade's formula supports it.
        pos_opp = self.state_manager.call_position if opposite_side == 'CALL' else self.state_manager.put_position
        opp_mode = 'buy' if pos_opp.get('entry_type') == 'BUY' else 'sell'
        opp_exit_formula = self._get_user_setting('exit_formula', str, fallback='', mode=opp_mode)

        if 'reversal' not in opp_exit_formula.lower():
            logger.info(f"V2: {side} {mode.upper()} triggered, but BLOCKED because 'reversal' is not in the active {opposite_side} exit formula.")
            return False

        # All rules pass! Trigger the reversal exit for the opposite side.
        exit_reason = f"REVERSAL ({side} {mode}) Formula passed. Closing {opposite_side}."
        exit_inst_side = 'PE' if opposite_side == 'PUT' else 'CE'
        exit_data = monitoring_data[f"{exit_inst_side.lower()}_data"]

        ex_mode = 'sell' if mode == 'buy' else 'buy'
        ex_expiry = self.atm_manager.get_expiry_by_mode(ex_mode, 'signal')
        ex_inst_key = self.atm_manager.find_instrument_key_by_strike(data['strike_price'], exit_inst_side, ex_expiry)

        exit_ltp = self.state_manager.get_ltp(ex_inst_key) or exit_data.get('last_close')
        await self._trigger_exit(opposite_side, exit_ltp, timestamp, exit_reason)
        return True

    async def _trigger_pending_entry(self, side, data, current_ltp, vwap_val, timestamp, reason_override=None, mode='buy'):
        monitoring_data = self.state_manager.dual_sr_monitoring_data
        reason = reason_override or monitoring_data.get('pending_vwap_reason', f"{side} {mode.upper()} pattern confirmed")

        entry_type = self._get_user_setting('entry_type', str, fallback='BUY', mode=mode)

        log_parts = [f"LTP: {float(current_ltp):.2f}"]
        log_parts.append(f"VWAP: {float(vwap_val or 0):.2f}")
        log_parts.append(f"Slope: {data.get('slope_info', 'N/A')}")
        log_parts.append(f"R1High: {float(data.get('r1_high') or 0):.2f}")

        logger.info(f"V2: {side} {mode.upper()} ({entry_type}) trade EXECUTED. Reason: {reason}. ({', '.join(log_parts)})")

        # Clear pending state
        monitoring_data['pending_vwap_side'] = None
        monitoring_data['pending_vwap_reason'] = None
        monitoring_data['pending_mode'] = None
        monitoring_data['awaiting_fresh_vwap_breach'] = False
        monitoring_data['sticky_dip_confirmed'] = False

        # Update baseline
        monitoring_data['baseline_side'] = side
        monitoring_data['crossover_state'] = 1

        await self._confirm_entry(side, data, data['instrument_key'], data['strike_price'], current_ltp, reason, timestamp, entry_type)

    async def _check_crossover_for_all_sides(self, timestamp, force_indicator_refresh=False, active_modes=['buy']):
        """
        Checks for CE/PE crossovers or other patterns using finalized candles of the configured timeframe.
        Evaluates for all active_modes (buy, sell).
        """
        monitoring_data = self.state_manager.dual_sr_monitoring_data
        if not monitoring_data:
            return

        ce_data = monitoring_data.get('ce_data')
        pe_data = monitoring_data.get('pe_data')
        if not ce_data or not pe_data:
            return

        # Get active modes to determine timeframe
        active_modes = self.orchestrator.json_config.get_active_modes(self.orchestrator.instrument_name)
        ref_mode = active_modes[0] if active_modes else 'buy'

        # Fetch Pattern timeframes from JSON configuration
        entry_pattern_tf = self._get_user_setting('tf', int, fallback=1, mode=ref_mode, category='indicators/pattern')
        exit_pattern_tf = self._get_user_setting('tf', int, fallback=1, mode=ref_mode, category='exit_indicators/pattern')

        # 1. SETTLING WINDOW & THROTTLE:
        # User Requirement: Wait X seconds after a minute ends to ensure candle is finalized.
        # In backtest mode, we skip this check as synthetic timestamps often have 00 seconds.
        settling_window = self._get_user_setting('settling_window_seconds', int, fallback=5, mode=ref_mode)
        if not self.orchestrator.is_backtest and timestamp.second < settling_window:
            return

        current_minute_start = timestamp.replace(second=0, microsecond=0)
        last_1m_completed_ts = current_minute_start - pd.Timedelta(minutes=1)

        # START OF DAY LOGIC:
        # Trade will start ONLY AFTER 09:15 candle is formed (at 09:16:05).
        if current_minute_start.hour == 9 and current_minute_start.minute <= 15:
            # It's before or exactly 09:15:xx. The 09:15 candle hasn't finalized yet.
            return

        # ITERATION THROTTLE: Wait for tf iterations (minutes/candles)
        # In Live: Align to clock boundaries (minute % tf == 0)
        # In Backtest: Iterations since start or candle boundary
        pattern_tf = self._get_user_setting('tf', int, fallback=1, mode=ref_mode, category='indicators/pattern')
        if last_1m_completed_ts.minute % pattern_tf != 0:
            return

        if ce_data.get('last_checked_candle') == last_1m_completed_ts and \
           pe_data.get('last_checked_candle') == last_1m_completed_ts:
            return

        # 2. CROSS-DAY SAFETY: Avoid processing future timestamps.
        # Historical pattern checking is ALWAYS allowed to support 09:15 AM analysis.
        if current_minute_start > timestamp:
            return

        # 3. GET RESAMPLED HISTORY for Pattern Check
        # We fetch history for both entry and exit timeframes to allow independent evaluation
        ce_hist_entry = await self.pattern_matcher.get_resampled_history(ce_data['instrument_key'], entry_pattern_tf, 5, last_1m_completed_ts)
        pe_hist_entry = await self.pattern_matcher.get_resampled_history(pe_data['instrument_key'], entry_pattern_tf, 5, last_1m_completed_ts)

        ce_hist_exit = ce_hist_entry
        pe_hist_exit = pe_hist_entry
        if exit_pattern_tf != entry_pattern_tf:
            ce_hist_exit = await self.pattern_matcher.get_resampled_history(ce_data['instrument_key'], exit_pattern_tf, 5, last_1m_completed_ts)
            pe_hist_exit = await self.pattern_matcher.get_resampled_history(pe_data['instrument_key'], exit_pattern_tf, 5, last_1m_completed_ts)

        if ce_hist_entry is None or pe_hist_entry is None or ce_hist_entry.empty or pe_hist_entry.empty:
            return

        # DATA SYNC RULE: Ensure the history actually contains data for the master anchor minute.
        # This prevents analyzing stale patterns or indicators (e.g. comparing 11:05 vs 11:05)
        # when the 11:06 candle hasn't arrived yet from the API.
        def get_expected_last_bucket_start(tf, anchor_ts):
            mins_since = int((last_1m_completed_ts - anchor_ts).total_seconds() / 60)
            last_end_mins = (mins_since // tf) * tf
            return anchor_ts + pd.Timedelta(minutes=last_end_mins)

        anchor_ts = last_1m_completed_ts.replace(hour=9, minute=15, second=0, microsecond=0)
        expected_ce_start = get_expected_last_bucket_start(entry_pattern_tf, anchor_ts)
        expected_pe_start = get_expected_last_bucket_start(entry_pattern_tf, anchor_ts)

        if ce_hist_entry.index[-1] < expected_ce_start or pe_hist_entry.index[-1] < expected_pe_start:
            # logger.debug(f"V2: Waiting for {last_1m_completed_ts.time()} candle to arrive in API (CE_Last: {ce_hist_entry.index[-1].time()}, PE_Last: {pe_hist_entry.index[-1].time()})")
            return

        # Ensure we only run analysis if we actually found a NEW candle.
        # We use the 1m candle completion as the master trigger for analysis.
        if ce_data.get('last_analyzed_1m') == last_1m_completed_ts:
            return

        # Update throttle state
        ce_data['last_checked_candle'] = last_1m_completed_ts
        pe_data['last_checked_candle'] = last_1m_completed_ts
        ce_data['last_analyzed_1m'] = last_1m_completed_ts
        ce_data['last_close'] = ce_hist_entry.iloc[-1]['close'] if ce_hist_entry is not None else 0
        pe_data['last_close'] = pe_hist_entry.iloc[-1]['close'] if pe_hist_entry is not None else 0

        # Get current LTP for logging context
        ce_ltp = self.state_manager.get_ltp(ce_data['instrument_key']) or 0.0
        pe_ltp = self.state_manager.get_ltp(pe_data['instrument_key']) or 0.0

        # 4. CROSSOVER LOGIC (New Pattern Matching)
        # We now process Entry and Exit separately due to different TFs.

        sides_entry = self.pattern_matcher.identify_crossover(ce_hist_entry, pe_hist_entry)
        sides_exit = self.pattern_matcher.identify_crossover(ce_hist_exit, pe_hist_exit)

        # PRE-EVALUATE Pattern State (Sticky & Fresh-Only)
        # Logic: We only set pattern to True when a FRESH crossover occurs.
        # This prevents immediate re-entry after a Stop Loss (churn reduction).
        current_side = sides_entry[-1] if sides_entry else None
        prev_side = monitoring_data.get('baseline_side')
        initial_side = monitoring_data.get('initial_higher_side')

        if current_side:
            if not prev_side:
                # First time establishing baseline after strike switch
                # A crossover is confirmed if the current higher side is different from the initial higher side
                if initial_side and current_side != initial_side:
                    logger.info(f"V2: Initial crossover detected: {initial_side} -> {current_side}")
                    if current_side == 'CE':
                        ce_data['criteria_state']['pattern'] = True
                        pe_data['criteria_state']['pattern'] = False
                    else:
                        pe_data['criteria_state']['pattern'] = True
                        ce_data['criteria_state']['pattern'] = False
                monitoring_data['baseline_side'] = current_side
            elif current_side != prev_side:
                # Fresh crossover detected!
                logger.info(f"V2: Crossover detected: {prev_side} -> {current_side}")
                if current_side == 'CE':
                    ce_data['criteria_state']['pattern'] = True
                    pe_data['criteria_state']['pattern'] = False
                else:
                    pe_data['criteria_state']['pattern'] = True
                    ce_data['criteria_state']['pattern'] = False
                monitoring_data['baseline_side'] = current_side

        # 4. PRE-EVALUATE INDICATORS FOR CLOSE MODE
        await self.evaluator.evaluate_close_criteria(timestamp, active_modes)

        # Update monitoring data with entry sequence for display
        monitoring_data['current_sequence'] = sides_entry

        # Log analysis for verification (only if pattern is used in formula)
        entry_formula = self._get_user_setting('entry_formula', str, fallback='pattern', mode=ref_mode)
        exit_formula = self._get_user_setting('exit_formula', str, fallback='s1_low', mode=ref_mode)

        if 'pattern' in entry_formula.lower() and len(sides_entry) >= 2:
            logger.info(f"V2 Entry Analysis [{ce_data['strike_price']}] {entry_pattern_tf}m: {','.join(sides_entry)}")
        if 'pattern' in exit_formula.lower() and len(sides_exit) >= 2:
            logger.info(f"V2 Exit Analysis [{ce_data['strike_price']}] {exit_pattern_tf}m: {','.join(sides_exit)}")

        history_sides = sides_entry # Alias for backward compatibility in logs

        # UPDATE Indicators in state for UI/MGMT transparency
        # USER REQUIREMENT: Update every r1_high_tf minutes to match strategy timeframe.
        # Exception: Force update at 09:16 to capture Rule #1 initialization.
        entry_r1_tf = self._get_user_setting('entry_r1_tf', int, fallback=3)
        entry_vwap_tf = self._get_user_setting('entry_vwap_tf', int, fallback=1)

        is_r1_tf_close = (last_1m_completed_ts.minute + 1) % entry_r1_tf == 0
        is_vwap_tf_close = (last_1m_completed_ts.minute + 1) % entry_vwap_tf == 0
        is_rule_1 = (last_1m_completed_ts.hour == 9 and last_1m_completed_ts.minute == 15)

        sr_indicators = ['s1_low', 'r1_high', 's1_double_drop', 'r1_falling', 'r1_low_breach', 's1_confirm']
        needs_sr = any(ind in entry_formula.lower() or ind in exit_formula.lower() for ind in sr_indicators)

        if (is_r1_tf_close or is_vwap_tf_close or is_rule_1 or force_indicator_refresh) and ce_data.get('last_indicator_update') != last_1m_completed_ts:
            # Use JSON timeframe if available
            r1_tf_val = self._get_user_setting('tf', int, fallback=3, mode=ref_mode, category='indicators/r1_high')

            # CE side indicators
            ce_vwap = await self.indicator_manager.calculate_vwap(ce_data['instrument_key'], timestamp)
            ce_data.update({'vwap': ce_vwap, 'last_indicator_update': last_1m_completed_ts})

            if needs_sr:
                ce_b_s1, ce_l_s1, _, _, ce_ph_s1, _, _, _ = await self.indicator_manager.get_nuanced_barrier(ce_data['instrument_key'], 's1_low', r1_tf_val, timestamp)
                ce_b_r1, ce_l_r1, _, _, ce_ph_r1, _, _, _ = await self.indicator_manager.get_nuanced_barrier(ce_data['instrument_key'], 'r1_high', r1_tf_val, timestamp)
                ce_data.update({
                    'r1_high': ce_b_r1, 's1_low': ce_b_s1,
                    'r1_label': ce_l_r1, 's1_label': ce_l_s1,
                    'r1_phase': ce_ph_r1, 's1_phase': ce_ph_s1,
                })

            # PE side indicators
            pe_vwap = await self.indicator_manager.calculate_vwap(pe_data['instrument_key'], timestamp)
            pe_data.update({'vwap': pe_vwap, 'last_indicator_update': last_1m_completed_ts})

            if needs_sr:
                pe_b_s1, pe_l_s1, _, _, pe_ph_s1, _, _, _ = await self.indicator_manager.get_nuanced_barrier(pe_data['instrument_key'], 's1_low', r1_tf_val, timestamp)
                pe_b_r1, pe_l_r1, _, _, pe_ph_r1, _, _, _ = await self.indicator_manager.get_nuanced_barrier(pe_data['instrument_key'], 'r1_high', r1_tf_val, timestamp)
                pe_data.update({
                    'r1_high': pe_b_r1, 's1_low': pe_b_s1,
                    'r1_label': pe_l_r1, 's1_label': pe_l_s1,
                    'r1_phase': pe_ph_r1, 's1_phase': pe_ph_s1,
                })

        trade_triggered = False
        side_to_trigger = None
        trigger_reason = ""

        # 1. EXIT LOGIC (Crossover & VWAP Slope)
        exit_use_pattern = self._get_user_setting('exit_use_pattern', bool, fallback=True)
        exit_use_vwap_slope = self._get_user_setting('exit_use_vwap_slope', bool, fallback=False)

        last_2_exit = sides_exit[-2:] if len(sides_exit) >= 2 else []

        # --- CALL EXIT STATUS ---
        if self.state_manager.is_in_trade('CALL'):
            pos = self.state_manager.call_position

            # A. Put/Call Crossover
            is_cross = (last_2_exit == ['CE', 'PE'])
            pos['pattern_exit_triggered'] = is_cross
            if is_cross: pos['pattern_exit_reason'] = "Put/Call Crossover (CE-PE)"

            # B. VWAP Slope < 0
            is_slope_down = False
            if exit_use_vwap_slope:
                slope_tf = self._get_user_setting('exit_vwap_slope_tf', int, fallback=1)
                slope_occ = self._get_user_setting('exit_vwap_slope_occurrences', int, fallback=1)
                _, is_falling, _, _, _, _ = await self.indicator_manager.get_vwap_slope_status(ce_data['instrument_key'], last_1m_completed_ts, slope_tf, slope_occ)
                is_slope_down = bool(is_falling)
            pos['vwap_slope_exit_triggered'] = is_slope_down

        # --- PUT EXIT STATUS ---
        if self.state_manager.is_in_trade('PUT'):
            pos = self.state_manager.put_position

            # A. Put/Call Crossover
            is_cross = (last_2_exit == ['PE', 'CE'])
            pos['pattern_exit_triggered'] = is_cross
            if is_cross: pos['pattern_exit_reason'] = "Put/Call Crossover (PE-CE)"

            # B. VWAP Slope < 0
            is_slope_down = False
            if exit_use_vwap_slope:
                slope_tf = self._get_user_setting('exit_vwap_slope_tf', int, fallback=1)
                slope_occ = self._get_user_setting('exit_vwap_slope_occurrences', int, fallback=1)
                _, is_falling, _, _, _, _ = await self.indicator_manager.get_vwap_slope_status(pe_data['instrument_key'], last_1m_completed_ts, slope_tf, slope_occ)
                is_slope_down = bool(is_falling)
            pos['vwap_slope_exit_triggered'] = is_slope_down

        else:
            if sides_entry:
                logger.debug(f"V2: No pattern confirmed for strike {ce_data['strike_price']}. History: {' -> '.join(sides_entry)}")
                # Even if no pattern, we update baseline for display/state purposes
                monitoring_data['baseline_side'] = sides_entry[-1]
                monitoring_data['crossover_state'] = 1

    async def _confirm_entry(self, side, data, instrument_key, strike_price, signal_ltp, entry_reason, timestamp, entry_type='BUY'):
        """Finalizes entry once crossover is confirmed."""
        task_id = id(asyncio.current_task())
        trade_direction = data['direction']

        logger.info(f"[Task {task_id}] V2: SETTING entry_confirmed=True for {side} ({entry_type})")
        data['entry_confirmed'] = True
        data['waiting_for_buffer'] = False

        # Broadcast the signal to all user sessions for this instrument
        await self.orchestrator.broadcast_signal(
            direction=trade_direction,
            instrument_key=instrument_key,
            signal_ltp=signal_ltp,
            strike_price=strike_price,
            timestamp=timestamp,
            strategy_log=entry_reason,
            entry_type=entry_type
        )

    async def _trigger_exit(self, side, ltp, timestamp, reason):
        """Triggers an exit for a specific trade side."""
        if self.session:
            await self.session.position_manager._exit_trade(side, ltp, timestamp, reason)
        elif hasattr(self.orchestrator, 'position_manager'):
            await self.orchestrator.position_manager._exit_trade(side, ltp, timestamp, reason)
