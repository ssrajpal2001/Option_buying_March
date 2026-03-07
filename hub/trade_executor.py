from utils.logger import logger
from hub.event_bus import event_bus
from hub.data_manager import OptionContract
import asyncio
import math

class TradeExecutor:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.state = orchestrator.orchestrator_state
        self.config_manager = orchestrator.config_manager
        self.atm_manager = orchestrator.atm_manager
        self.data_manager = orchestrator.data_manager
        self._finalize_lock = asyncio.Lock()

    def _get_state_manager(self, user_id=None):
        """Helper to get the correct state manager (Global or User-specific)."""
        if user_id in self.orchestrator.user_sessions:
            return self.orchestrator.user_sessions[user_id].state_manager
        return self.orchestrator.state_manager

    def _get_position_manager(self, user_id=None):
        """Helper to get the correct position manager."""
        if user_id in self.orchestrator.user_sessions:
            return self.orchestrator.user_sessions[user_id].position_manager
        return self.orchestrator.position_manager

    async def execute_trade_v2(self, direction, signal_instrument_key, signal_ltp, strike_price, timestamp, signal_strike, strategy_log="", user_id=None, entry_type='BUY', quantity_multiplier=1):
        state_manager = self._get_state_manager(user_id)

        # All trades: recalculate trade strike from SPOT INDEX price.
        # Signal/monitoring strike (futures-based) is kept unchanged for exit monitoring.
        strike_interval = self.config_manager.get_int(self.orchestrator.instrument_name, 'strike_interval', 50)
        index_price = self.orchestrator.state_manager.index_price
        if index_price and index_price > 0:
            trade_strike = int(round(index_price / strike_interval) * strike_interval)
            logger.info(f"V2: {entry_type} trade strike from INDEX ({index_price:.2f}) → {trade_strike} [Futures/signal strike was: {strike_price}]")
        else:
            trade_strike = strike_price
            logger.warning(f"V2: INDEX price unavailable, falling back to futures signal strike {strike_price}")

        # Get trade expiry based on entry_type (buy/sell) from JSON if available
        mode = 'buy' if entry_type == 'BUY' else 'sell'
        mode_expiries = self.atm_manager.mode_expiries.get(mode)
        trade_expiry = mode_expiries['trade'] if mode_expiries else self.atm_manager.trade_expiry_date
        signal_expiry_check = mode_expiries['signal'] if mode_expiries else self.atm_manager.signal_expiry_date

        if signal_expiry_check != trade_expiry:
            logger.warning(f"TRADE EXPIRY MISMATCH at execution: signal={signal_expiry_check}, trade={trade_expiry}. Will monitor {direction} on signal contract but TRADE will be on DIFFERENT expiry!")
        else:
            logger.debug(f"Trade expiry confirmed: {trade_expiry} (matches signal expiry)")

        # DETERMINISTIC STRIKE RULE: All trades (Buy and Sell) are executed on the INDEX-based ATM strike.
        trade_instrument_key = self.atm_manager.find_instrument_key_by_strike(trade_strike, direction, trade_expiry)

        if not trade_instrument_key:
            logger.error(f"Could not find instrument key for strike {trade_strike}. Aborting trade.")
            return

        # SIMULTANEOUS TRADE CHECK (Robustness)
        position_manager = self._get_position_manager(user_id)
        allow_simultaneous = position_manager._get_user_setting('allow_simultaneous_trades', bool, fallback=False)

        # Check reversal enabled from both global and JSON-specific settings
        exit_use_reversal_global = position_manager._get_user_setting('exit_use_reversal', bool, fallback=False)
        exit_use_reversal_json = position_manager._get_user_setting('enabled', bool, fallback=False, mode=mode, category='exit_indicators/reversal')
        exit_use_reversal = exit_use_reversal_global or exit_use_reversal_json

        if not allow_simultaneous and state_manager.is_in_any_trade() and not state_manager.is_in_trade(direction):
            if not exit_use_reversal:
                logger.warning(f"V2: Skipping {direction} entry. User already in another trade and simultaneous trades are disabled.")
                return

        # TRADE REVERSAL: Close the opposite side if reversal exit is enabled AND in formula.
        opposite_side = 'PUT' if direction == 'CALL' else 'CALL'

        if exit_use_reversal and state_manager.is_in_trade(opposite_side):
            # NEW: Only exit if 'reversal' is explicitly in the active trade's exit formula
            pos_opp = state_manager.get_position(opposite_side)
            opp_mode = 'buy' if pos_opp.get('entry_type') == 'BUY' else 'sell'
            opp_exit_formula = position_manager._get_user_setting('exit_formula', str, fallback='', mode=opp_mode)

            if 'reversal' in opp_exit_formula.lower():
                logger.info(f"V2: Reversal signal for {direction} received while in {opposite_side}. Closing {opposite_side} first.")
                state_manager.trade_closed_event.clear()

                if pos_opp and not pos_opp.get('exit_sent'):
                    logger.info(f"V2: Triggering immediate exit for opposite trade {opposite_side} for reversal.")
                    opp_ltp = state_manager.get_ltp(pos_opp.get('instrument_key')) or 0
                    await position_manager._exit_trade(opposite_side, opp_ltp, timestamp, f"REVERSAL SIGNAL ({direction})")

                if state_manager.is_in_trade(opposite_side):
                    if self.orchestrator.is_backtest:
                        logger.debug(f"V2: Reversal - Fast-tracking reversal in backtest mode.")
                    else:
                        try:
                            # Wait for closure confirmation before entering the new side
                            await asyncio.wait_for(state_manager.trade_closed_event.wait(), timeout=5.0)
                            logger.info(f"V2: Opposite trade {opposite_side} closure confirmed. Proceeding with {direction} reversal entry.")
                        except asyncio.TimeoutError:
                            logger.warning(f"V2: Timeout waiting for {opposite_side} closure. Proceeding with reversal anyway.")
            else:
                logger.info(f"V2: {direction} entry triggered, but NOT closing {opposite_side} because 'reversal' is not in its exit formula.")
                if not allow_simultaneous:
                    return

        # In backtesting, ensure the contract is loaded.
        if self.orchestrator.is_backtest and not self.atm_manager.get_contract_by_instrument_key(trade_instrument_key):
            logger.warning(f"Backtest: Target key {trade_instrument_key} not found in existing contracts. Attempting to fetch.")
            all_contracts = await self.data_manager.rest_client.get_option_contracts(self.data_manager.instrument_key)
            contract_details = next((c for c in all_contracts if c.get('instrument_key') == trade_instrument_key), None)
            if contract_details:
                new_contract = OptionContract(contract_details)
                self.atm_manager.add_contracts([new_contract])
                logger.info(f"Backtest: Successfully loaded missing contract for {trade_instrument_key}.")
            else:
                logger.error(f"FATAL: Could not fetch contract details for instrument {trade_instrument_key}. Aborting trade.")
                return

        if self.orchestrator.is_backtest:
            # HYBRID BACKTEST: Fetch execution price using orchestrator helper to ensure consistency
            logger.info(f"V2 Hybrid [Backtest]: Fetching execution price for {trade_instrument_key} @ {timestamp}")
            trade_ltp = await self.orchestrator._get_ltp_for_backtest_instrument(trade_instrument_key, timestamp)

            if trade_ltp is None:
                logger.error(f"[Backtest] Could not find LTP for instrument {trade_instrument_key} at entry {timestamp}. Aborting trade.")
                return

            logger.info(f"V2 Hybrid [Backtest]: Discovered execution price: {trade_ltp:.2f}")
            await self._finalize_trade_entry(direction, trade_instrument_key, trade_ltp, trade_strike, timestamp, signal_strike, strategy_log, user_id, entry_type, quantity_multiplier)
        else:
            # HYBRID APPROACH
            # 1. Start dynamic WebSocket subscription for the contract
            await event_bus.publish('ADD_TO_WATCHLIST', {'instrument_key': trade_instrument_key})

            # 2. Discovery accurate price for entry (to prevent P&L mismatch)
            logger.debug(f"V2: Discovery price for {trade_instrument_key}...")
            trade_ltp = await state_manager.get_ltp_for_instrument(trade_instrument_key, timeout=1)
            is_fallback = False

            if trade_ltp:
                logger.debug(f"V2: Using price from WebSocket tick: {trade_ltp:.2f}")
            else:
                # Priority 2: REST Discovery
                try:
                    fetched_ltp = await self.data_manager.rest_client.get_ltp(trade_instrument_key)
                    if fetched_ltp and fetched_ltp > 0:
                        trade_ltp = fetched_ltp
                        logger.info(f"V2: Using price from REST discovery: {trade_ltp:.2f}")
                except Exception as e:
                    logger.warning(f"V2: REST discovery failed: {e}")

            # Priority 3: Final Wait
            if not trade_ltp:
                logger.warning(f"V2: Price discovery still failing for {trade_instrument_key}. Final wait...")
                trade_ltp = await state_manager.get_ltp_for_instrument(trade_instrument_key, timeout=3)

            # Priority 4: Forced Discovery
            if not trade_ltp:
                logger.warning(f"V2: Still no price for {trade_strike}. Forcing wait for first market tick...")
                trade_ltp = await state_manager.get_ltp_for_instrument(trade_instrument_key, timeout=5)

            if not trade_ltp:
                logger.warning(f"V2: NO PRICE DISCOVERED for STRIKE {trade_strike} after initial checks. Falling back to signal price.")
                trade_ltp = signal_ltp
                is_fallback = True

            # FATAL CHECK: If price is still zero or None, we MUST NOT enter.
            if trade_ltp is None or trade_ltp <= 0:
                logger.critical(f"V2: FATAL - Discovered price for {trade_strike} is {trade_ltp}. Forcing wait for real tick...")
                trade_ltp = await state_manager.get_ltp_for_instrument(trade_instrument_key, timeout=10)

                if trade_ltp is None or trade_ltp <= 0:
                    logger.error(f"V2: ABORTING trade for {direction} on strike {trade_strike}. No valid price discovered after 20s.")
                    return

            # 3. Finalize entry using discovered LTP.
            existing_pos = await self._finalize_trade_entry(direction, trade_instrument_key, trade_ltp, trade_strike, timestamp, signal_strike, strategy_log, user_id, entry_type, quantity_multiplier)

            if is_fallback and existing_pos:
                existing_pos['awaiting_real_price'] = True

            # Start background update task to ensure accurate baseline once the first tick arrives
            asyncio.create_task(self._update_entry_price_on_first_tick(user_id, trade_instrument_key, direction, is_fallback))

    async def _update_entry_price_on_first_tick(self, user_id, instrument_key, side, is_fallback_price):
        """
        Background task to update the entry price of a position once the first
        real WebSocket tick arrives. This ensures accurate P&L baselining.
        """
        state_manager = self._get_state_manager(user_id)

        # Wait for a real tick from WebSocket (timeout 60s)
        ltp = await state_manager.get_ltp_for_instrument(instrument_key, timeout=60)

        if ltp and ltp > 0:
            pos = state_manager.call_position if side == 'CALL' else state_manager.put_position
            if pos and pos.get('instrument_key') == instrument_key:
                # Update if we used a fallback price OR if it was a paper trade (which confirms immediately)
                if is_fallback_price or not pos.get('entry_confirmed_by_broker'):
                    pos['entry_price'] = ltp
                    pos['ltp'] = ltp
                    # CRITICAL: Reset peak_price if we were using a fallback placeholder (like signal price)
                    # to prevent massive "fake" profit calculation if price discovery was delayed.
                    if is_fallback_price:
                        pos['peak_price'] = ltp
                        pos['awaiting_real_price'] = False # Clear the flag
                        logger.info(f"V2: Updated entry price and RESET peak_price for {side} from first real tick: {ltp:.2f}")
                    else:
                        logger.info(f"V2: Updated entry price for {side} from first real tick: {ltp:.2f}")
                    await state_manager.save_state()

    async def _finalize_trade_entry(self, direction, trade_instrument_key, trade_ltp, trade_strike, timestamp, signal_strike, strategy_log="", user_id=None, entry_type='BUY', quantity_multiplier=1):
        async with self._finalize_lock:
            return await self._finalize_trade_entry_unlocked(direction, trade_instrument_key, trade_ltp, trade_strike, timestamp, signal_strike, strategy_log, user_id, entry_type, quantity_multiplier)

    async def _finalize_trade_entry_unlocked(self, direction, trade_instrument_key, trade_ltp, trade_strike, timestamp, signal_strike, strategy_log="", user_id=None, entry_type='BUY', quantity_multiplier=1):
        task_id = id(asyncio.current_task())
        state_manager = self._get_state_manager(user_id)

        # IDEMPOTENCY CHECK: Are we already in a trade?
        if state_manager.is_in_trade(direction):
            logger.warning(f"[Task {task_id}] V2: User {user_id} already in {direction} trade. Skipping duplicate entry finalization.")
            return

        contract = self.atm_manager.get_contract_by_instrument_key(trade_instrument_key)
        if not contract:
            contract = next((c for c in self.atm_manager.all_contracts if c.instrument_key == trade_instrument_key), None)
            if contract:
                logger.warning(f"[Task {task_id}] V2: contract_lookup miss for {trade_instrument_key} — recovered from all_contracts (re-strike race).")
        if not contract:
            logger.error(f"[Task {task_id}] V2: Could not find contract for key {trade_instrument_key}. Not in lookup or all_contracts. Trade aborted.")
            return

        # Resolve correct signal_expiry for the mode
        mode = 'buy' if entry_type == 'BUY' else 'sell'
        signal_expiry = self.atm_manager.get_expiry_by_mode(mode, 'signal')

        # Mark as in-trade in state manager so SL monitor starts.
        # This is CRITICAL for both Live and Backtest.
        existing_pos = await state_manager.enter_trade(
            direction=direction,
            instrument_key=trade_instrument_key,
            instrument_symbol=trade_instrument_key,
            ltp=trade_ltp,
            strike_price=trade_strike,
            timestamp=timestamp,
            signal_strike=signal_strike,
            s1_fast_tf=self.orchestrator.s1_low_fast_tf,
            s1_slow_tf=self.orchestrator.s1_low_slow_tf,
            entry_type=entry_type,
            signal_expiry_date=signal_expiry,
            quantity_multiplier=quantity_multiplier
        )

        # "STICKY PATTERN" LOGIC: We no longer reset entry_confirmed here.
        # Confirmation remains active until a Target Strike change, Reversal Pattern, or Exit Pattern occurs.
        if state_manager.dual_sr_monitoring_data:
            side_key = 'ce_data' if direction == 'CALL' else 'pe_data'
            side_data = state_manager.dual_sr_monitoring_data.get(side_key)
            if side_data:
                # We still reset waiting_for_buffer as the buffer is only for the initial crossover
                side_data['waiting_for_buffer'] = False

        trade_entered = True
        if self.orchestrator.is_backtest:
            trade_entered = self.orchestrator.pnl_tracker.enter_trade(
                side=direction,
                instrument_key=trade_instrument_key,
                entry_price=trade_ltp,
                timestamp=timestamp,
                strike_price=trade_strike,
                contract=contract,
                strategy_log=strategy_log,
                quantity=quantity_multiplier
            )

        if not trade_entered:
            logger.warning(f"Trade entry condition not met for {direction} at {timestamp}. Not finalizing.")
            return

        if not self.orchestrator.is_backtest:
            instrument_name = self.orchestrator.primary_instrument
            # Resolve correct signal_expiry for the mode
            mode = 'buy' if entry_type == 'BUY' else 'sell'
            signal_expiry = self.atm_manager.get_expiry_by_mode(mode, 'signal')

            trade_payload = {
                "user_id": user_id,
                "instrument_name": instrument_name,
                "contract": contract,
                "direction": direction,
                "ltp": trade_ltp,
                "strike_price": trade_strike,
                "signal_strike": signal_strike,
                "signal_expiry_date": signal_expiry,
                "strategy_log": strategy_log,
                "entry_type": entry_type,
                "product_type": "MIS",
                "quantity_multiplier": quantity_multiplier
            }

            # IDEMPOTENCY CHECK based on returned position or re-fetched position
            is_initial_entry = not (existing_pos and existing_pos.get('order_sent'))

            if is_initial_entry:
                # Mark as sent EARLY to prevent duplicate orders from concurrent tasks
                if existing_pos:
                    existing_pos['order_sent'] = True
                    logger.debug(f"[Task {task_id}] V2: Marked order_sent=True for {direction}")

                logger.debug(f"[Task {task_id}] V2: Sending EXECUTE_TRADE_REQUEST for {direction}")
                await event_bus.publish('EXECUTE_TRADE_REQUEST', trade_data=trade_payload)
            else:
                logger.warning(f"[Task {task_id}] V2: Order already sent for {direction}. Skipping duplicate request.")

        if self.orchestrator.is_backtest:
            await self.data_manager.fetch_and_cache_api_ohlc(trade_instrument_key, timestamp.date(), interval="1minute")

        # Resolve correct signal_expiry for the mode
        mode = 'buy' if entry_type == 'BUY' else 'sell'
        signal_expiry = self.atm_manager.get_expiry_by_mode(mode, 'signal')

        exit_monitoring_key = self.atm_manager.find_instrument_key_by_strike(signal_strike, direction, signal_expiry)
        if not exit_monitoring_key:
            logger.error(f"CRITICAL: Could not find instrument key for signal strike {signal_strike} for exit monitoring.")
            return

        # Prime aggregators for the signal strike to maintain monitoring continuity
        try:
            await asyncio.wait_for(asyncio.gather(
                self.data_manager.prime_aggregator(self.orchestrator.entry_aggregator, exit_monitoring_key, timestamp),
                self.data_manager.prime_aggregator(self.orchestrator.exit_aggregator, exit_monitoring_key, timestamp),
                self.data_manager.prime_aggregator(self.orchestrator.one_min_aggregator, exit_monitoring_key, timestamp),
                self.data_manager.prime_aggregator(self.orchestrator.five_min_aggregator, exit_monitoring_key, timestamp)
            ), timeout=30.0)
        except asyncio.TimeoutError:
            logger.error(f"V2: TIMEOUT priming exit monitoring strike {signal_strike}. Continuing...")

        position = state_manager.call_position if direction == 'CALL' else state_manager.put_position
        if position is not None:
            position['exit_monitoring_strike'] = signal_strike

            # Pre-seed the CLOSE-mode candle dedup to prevent immediate exit on entry candle.
            # If entry happens inside a candle boundary window (minute%tf==0 AND second>=5),
            # _vwap_close_last_candle=None would cause the exit evaluator to fire on the
            # just-closed candle milliseconds after entry. Pre-seeding to current minute
            # prevents this — the next candle boundary will be a clean evaluation.
            position['_vwap_close_last_candle'] = timestamp.replace(second=0, microsecond=0)
            logger.debug(f"V2: Pre-seeded _vwap_close_last_candle={position['_vwap_close_last_candle']} to block same-candle exit on entry.")

            # --- DYNAMIC TARGET INITIALIZATION ---
            # USER REQUIREMENT: Only calculate if target_exit is enabled
            position_manager = self._get_position_manager(user_id)
            target_exit_enabled = position_manager._get_user_setting('target_exit', bool, fallback=False)

            if target_exit_enabled:
                # Initial Target = E0 + 2 * (E0 - S0)
                # E0: entry_price, S0: S1LOW of signal strike at entry time
                # Resolve correct signal_expiry for the mode
                mode = 'buy' if entry_type == 'BUY' else 'sell'
                expiry = self.atm_manager.get_expiry_by_mode(mode, 'signal')

                inst_side = 'CE' if direction == 'CALL' else 'PE'
                signal_inst_key = self.atm_manager.find_instrument_key_by_strike(signal_strike, inst_side, expiry)

                if signal_inst_key:
                    active_tf = self.orchestrator.s1_low_fast_tf # Use fast TF for initial S1
                    # Use indicator_manager directly and unpack 8 values
                    s0_val, r0_val, _, _, _, _, _, _ = await self.orchestrator.indicator_manager.get_sr_status(signal_inst_key, active_tf, timestamp)
                    if s0_val is not None:
                        e0 = float(trade_ltp)
                        s0 = float(s0_val)
                        target = float(e0 + 2 * (e0 - s0))
                        position['s1_at_entry'] = s0
                        position['initial_target'] = target
                        position['current_target'] = target
                        logger.info(f"V2: Initial Target for {direction} set to {target:.2f} (E0: {e0:.2f}, S0: {s0:.2f})")

            # --- ATR TSL INITIALIZATION ---
            atr_tsl_enabled = position_manager._get_user_setting('enabled', bool, fallback=False, mode=mode, category='exit_indicators/atr_tsl')
            if atr_tsl_enabled:
                atr_len = position_manager._get_user_setting('length', int, fallback=10, mode=mode, category='exit_indicators/atr_tsl')
                atr_tf = position_manager._get_user_setting('tf', int, fallback=1, mode=mode, category='exit_indicators/atr_tsl')

                # Defensive check for indicator_manager availability
                ind_mgr = getattr(self.orchestrator, 'indicator_manager', None) or position_manager.indicator_manager

                # Calculate ATR for the traded instrument using current LTP
                atr_val = await ind_mgr.calculate_atr(trade_instrument_key, atr_tf, atr_len, timestamp, current_ltp=trade_ltp)
                if atr_val:
                    position['entry_atr'] = atr_val
                    logger.debug(f"V2: Entry ATR for {direction} set to {atr_val:.2f} (Len: {atr_len}, TF: {atr_tf}m)")

            logger.info(f"V2: Trade entered on {trade_instrument_key}. Monitoring strike {signal_strike} for crossover exit.")
