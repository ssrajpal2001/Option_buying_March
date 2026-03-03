import json
import os
import datetime
from utils.logger import logger


class SellManager:
    """
    Manages the short-strangle sell legs.

    New design (individual, slope-driven):
    - CE and PE are searched and entered INDEPENDENTLY.
    - Entry: pick strike from ATM + N ITM candidates whose LTP >= ltp_min
      and is closest to ltp_target; enter only if VWAP slope is decreasing.
    - Per tick (untraded side): re-select candidate if current LTP < ltp_min.
    - Exit: (a) individual LTP < ltp_exit_min, or (b) combined VWAP sum slope
      rising + individual VWAP slope rising above threshold → exit that leg
      → restart search on that side.
    - EOD: close_all() closes any open legs.
    """

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.state_file = f'config/sell_state_{orchestrator.instrument_name}.json'

        # ── Per-side trade state ──────────────────────────────────────────
        self.ce_placed = False
        self.pe_placed = False
        self.ce_strike = None
        self.pe_strike = None
        self.ce_key = None
        self.pe_key = None
        self.ce_entry_ltp = None
        self.pe_entry_ltp = None
        self.ce_entry_timestamp = None
        self.pe_entry_timestamp = None
        self.ce_contract = None
        self.pe_contract = None

        # ── Search state ──────────────────────────────────────────────────
        self.ce_searching = False   # True after candidates built
        self.pe_searching = False
        self.ce_candidates = []     # list of (strike, inst_key)
        self.pe_candidates = []

        # ── Combined VWAP tracking ────────────────────────────────────────
        self.prev_combined_vwap = None

        # ── EOD / misc state ─────────────────────────────────────────────
        self.strangle_closed = False
        self.expiry = None
        self.hedge_offset = None    # kept for potential future use

        # Backward-compat aliases (engine_manager uses buy_ce_key / buy_pe_key)
        self.buy_ce_key = None
        self.buy_pe_key = None

    # ─────────────────────────────────────────────────────────────────────
    # Backward-compat properties (engine_manager checks .strangle_placed)
    # ─────────────────────────────────────────────────────────────────────

    @property
    def strangle_placed(self):
        return self.ce_placed or self.pe_placed

    @property
    def sell_ce_strike(self):
        return self.ce_strike

    @property
    def sell_pe_strike(self):
        return self.pe_strike

    @property
    def sell_ce_key(self):
        return self.ce_key

    @property
    def sell_pe_key(self):
        return self.pe_key

    @property
    def sell_ce_entry_ltp(self):
        return self.ce_entry_ltp

    @property
    def sell_pe_entry_ltp(self):
        return self.pe_entry_ltp

    # ─────────────────────────────────────────────────────────────────────
    # JSON config helpers
    # ─────────────────────────────────────────────────────────────────────

    def _cfg(self, key, type_func=float, fallback=None):
        """Read a value from NIFTY.sell.<key> in strategy_logic.json."""
        val = self.orchestrator.json_config.get_value(
            f"{self.orchestrator.instrument_name}.sell.{key}")
        return type_func(val) if val is not None else fallback

    def _exit_cfg(self, key, type_func=float, fallback=None):
        """Read a value from NIFTY.sell.exit_indicators.<key>."""
        val = self.orchestrator.json_config.get_value(
            f"{self.orchestrator.instrument_name}.sell.exit_indicators.{key}")
        return type_func(val) if val is not None else fallback

    # ─────────────────────────────────────────────────────────────────────
    # Candidate building  (called once at sell start time)
    # ─────────────────────────────────────────────────────────────────────

    def _build_candidate_list(self, chain, side, atm, interval, itm_count):
        """
        Return [(strike, inst_key)] for ATM + `itm_count` ITM strikes.
        CE ITM = strikes BELOW spot: ATM, ATM-interval, ATM-2*interval, …
        PE ITM = strikes ABOVE spot: ATM, ATM+interval, ATM+2*interval, …
        List is sorted ATM-first.
        """
        options_key = 'call_options' if side == 'CE' else 'put_options'
        if side == 'CE':
            target_strikes = {float(atm + i * interval)
                              for i in range(0, -(itm_count + 1), -1)}
        else:
            target_strikes = {float(atm + i * interval)
                              for i in range(0, itm_count + 1)}

        candidates = []
        for entry in chain:
            strike = entry.get('strike_price')
            if strike is None or float(strike) not in target_strikes:
                continue
            side_data = entry.get(options_key) or {}
            inst_key = side_data.get('instrument_key')
            if inst_key:
                candidates.append((float(strike), inst_key))

        candidates.sort(key=lambda x: abs(x[0] - atm))
        logger.info(
            f"[SellManager] {side} candidates (ATM={atm}, ITM={itm_count}): "
            f"{[(int(s), k[:25]) for s, k in candidates]}")
        return candidates

    async def build_candidates_for_all_sides(self, timestamp):
        """
        Fetch option chain and build ATM+ITM candidate lists for CE and PE.
        Called once when the clock reaches sell.start_time.
        """
        expiry = self.orchestrator.atm_manager.signal_expiry_date
        if not expiry:
            logger.error("[SellManager] Cannot build candidates — signal_expiry_date not set.")
            return

        self.expiry = expiry
        index_key = self.orchestrator.index_instrument_key
        atm = self.orchestrator.atm_manager.strikes.get('atm')
        interval = self.orchestrator.config_manager.get_int(
            self.orchestrator.instrument_name, 'strike_interval', 50)
        itm_count = self._cfg('itm_count', int, 2)

        if not atm or not interval:
            logger.error("[SellManager] ATM or strike interval unavailable — cannot build candidates.")
            return

        logger.info(
            f"[SellManager] Fetching option chain: {index_key} expiry={expiry} ATM={atm}")
        chain = await self.orchestrator.rest_client.get_option_chain(index_key, expiry)
        if not chain:
            logger.error("[SellManager] Empty option chain — aborting candidate build.")
            return

        self.ce_candidates = self._build_candidate_list(chain, 'CE', atm, interval, itm_count)
        self.pe_candidates = self._build_candidate_list(chain, 'PE', atm, interval, itm_count)

        if not self.ce_placed:
            self.ce_searching = True
        if not self.pe_placed:
            self.pe_searching = True

        # Subscribe all candidate keys to websocket so LTPs arrive in ticks
        all_keys = [k for _, k in self.ce_candidates + self.pe_candidates]
        if all_keys and hasattr(self.orchestrator, 'websocket') and self.orchestrator.websocket:
            self.orchestrator.websocket.subscribe(all_keys)
            logger.info(f"[SellManager] Subscribed {len(all_keys)} candidate keys to WS.")

        logger.info(
            f"[SellManager] Ready — CE searching={self.ce_searching} "
            f"PE searching={self.pe_searching}")

    # ─────────────────────────────────────────────────────────────────────
    # LTP candidate selection helpers
    # ─────────────────────────────────────────────────────────────────────

    def _get_best_ltp_candidate(self, candidates, ticks, ltp_min, ltp_target):
        """
        From live tick data pick the candidate with LTP >= ltp_min and
        minimum |ltp - ltp_target|.
        Returns (strike, inst_key, ltp) or (None, None, None).
        """
        best_strike = best_key = best_ltp = None
        best_diff = float('inf')
        for strike, inst_key in candidates:
            tick = ticks.get(inst_key) or {}
            ltp = tick.get('ltp', 0) or 0
            if ltp >= ltp_min:
                diff = abs(ltp - ltp_target)
                if diff < best_diff:
                    best_diff = diff
                    best_strike, best_key, best_ltp = strike, inst_key, ltp
        return best_strike, best_key, best_ltp

    # ─────────────────────────────────────────────────────────────────────
    # VWAP slope helpers
    # ─────────────────────────────────────────────────────────────────────

    async def _check_slope_decreasing(self, inst_key, timestamp):
        """
        Returns True if the VWAP slope for inst_key is currently falling
        (diff_pct < threshold, where threshold defaults to 0.0).
        """
        tf = self._cfg('indicators.vwap_slope.tf', int, 1)
        threshold = self._cfg('indicators.vwap_slope.threshold', float, 0.0)
        live_vwap = await self.orchestrator.indicator_manager.calculate_vwap(
            inst_key, timestamp)
        if live_vwap is None:
            return False
        result = await self.orchestrator.indicator_manager.get_vwap_slope_status(
            inst_key, timestamp, tf, count=1, live_vwap=live_vwap)
        if result is None or result[0] is None:
            return False
        _, _, v_curr, v_prev, _, _ = result
        if v_prev and v_prev > 0:
            return (v_curr - v_prev) / v_prev < threshold
        return False

    async def _get_vwap(self, inst_key, timestamp):
        return await self.orchestrator.indicator_manager.calculate_vwap(
            inst_key, timestamp)

    # ─────────────────────────────────────────────────────────────────────
    # Main per-tick method  (called from tick_processor every tick)
    # ─────────────────────────────────────────────────────────────────────

    async def on_tick(self, ticks, timestamp):
        """
        ticks: dict {inst_key: {'ltp': float, ...}} for live mode.
        Called every processed tick from tick_processor.
        """
        if self.strangle_closed:
            return
        if not self.ce_candidates and not self.pe_candidates:
            return   # Candidates not built yet (before 9:20)

        ltp_min = self._cfg('ltp_min', float, 50.0)
        ltp_target = self._cfg('ltp_target', float, 50.0)
        ltp_exit_min = self._cfg('ltp_exit_min', float, 20.0)

        # ── 1. LTP exit check for each placed leg ──────────────────────
        for side in ['CE', 'PE']:
            placed = self.ce_placed if side == 'CE' else self.pe_placed
            if not placed:
                continue
            key = self.ce_key if side == 'CE' else self.pe_key
            tick = ticks.get(key) or {}
            ltp = tick.get('ltp', 0) or 0
            if ltp > 0 and ltp < ltp_exit_min:
                logger.info(
                    f"[SellManager] {side} LTP {ltp:.2f} < exit_min {ltp_exit_min:.0f} — exiting.")
                await self.exit_side(side, timestamp,
                                     reason=f"LTP decayed below {ltp_exit_min}")

        # ── 2. Attempt entry for each searching side ───────────────────
        for side in ['CE', 'PE']:
            searching = self.ce_searching if side == 'CE' else self.pe_searching
            if not searching:
                continue
            candidates = self.ce_candidates if side == 'CE' else self.pe_candidates
            if not candidates:
                continue
            await self._try_enter_side(side, timestamp, ticks, candidates,
                                       ltp_min, ltp_target)

        # ── 3. Combined VWAP slope exit (when both legs active) ─────────
        if self.ce_placed and self.pe_placed:
            await self._check_combined_vwap_slope(timestamp)

    # ─────────────────────────────────────────────────────────────────────
    # Entry attempt
    # ─────────────────────────────────────────────────────────────────────

    async def _try_enter_side(self, side, timestamp, ticks, candidates,
                               ltp_min, ltp_target):
        strike, inst_key, ltp = self._get_best_ltp_candidate(
            candidates, ticks, ltp_min, ltp_target)
        if strike is None:
            logger.debug(
                f"[SellManager] {side}: no candidate with LTP >= {ltp_min}. Still scanning.")
            return

        slope_ok = await self._check_slope_decreasing(inst_key, timestamp)
        if not slope_ok:
            logger.debug(
                f"[SellManager] {side} {int(strike)}: LTP={ltp:.2f} OK "
                f"but slope not yet decreasing.")
            return

        # Resolve contract object for lot-size and order placement
        expiry_strikes = self.orchestrator.atm_manager.contract_lookup.get(
            self.expiry, {})
        contract = expiry_strikes.get(float(strike), {}).get(side)
        if not contract:
            logger.error(
                f"[SellManager] {side} strike {strike} not in contract_lookup — skipping.")
            return

        product_type = self._cfg('product_type', str, 'NRML')
        brokers = self.orchestrator.broker_manager.brokers
        for broker in brokers:
            if not broker.is_configured_for_instrument(self.orchestrator.instrument_name):
                continue
            qty = (broker.config_manager.get_int(broker.instance_name, 'quantity', 1)
                   * contract.lot_size)
            if self.orchestrator.is_backtest or getattr(broker, 'paper_trade', False):
                logger.info(
                    f"[SellManager][PAPER SELL {product_type}] "
                    f"{side}: strike={int(strike)} LTP={ltp:.2f} qty={qty}")
            else:
                order_id = broker.place_order(
                    contract, 'SELL', qty, self.expiry, product_type=product_type)
                logger.info(
                    f"[SellManager] Sold {side} {int(strike)} "
                    f"order_id={order_id} LTP={ltp:.2f}")

        # Persist state
        if side == 'CE':
            self.ce_placed = True
            self.ce_searching = False
            self.ce_strike = strike
            self.ce_key = inst_key
            self.ce_entry_ltp = ltp
            self.ce_entry_timestamp = timestamp
            self.ce_contract = contract
        else:
            self.pe_placed = True
            self.pe_searching = False
            self.pe_strike = strike
            self.pe_key = inst_key
            self.pe_entry_ltp = ltp
            self.pe_entry_timestamp = timestamp
            self.pe_contract = contract

        # Subscribe placed key so LTP arrives immediately
        if not self.orchestrator.is_backtest:
            ws = getattr(self.orchestrator, 'websocket', None)
            if ws:
                ws.subscribe([inst_key])

        self.save_state()
        logger.info(
            f"[SellManager] ✔ {side} leg entered: strike={int(strike)} "
            f"LTP={ltp:.2f} key={inst_key} product={product_type}")

    # ─────────────────────────────────────────────────────────────────────
    # Combined VWAP slope exit
    # ─────────────────────────────────────────────────────────────────────

    async def _check_combined_vwap_slope(self, timestamp):
        """
        Monitor CE_VWAP + PE_VWAP.
        If combined slope rises above threshold, find which individual leg
        is also rising above threshold and exit it → restart search.
        """
        ce_vwap = await self._get_vwap(self.ce_key, timestamp)
        pe_vwap = await self._get_vwap(self.pe_key, timestamp)
        if ce_vwap is None or pe_vwap is None:
            return

        combined = ce_vwap + pe_vwap
        if self.prev_combined_vwap is None:
            self.prev_combined_vwap = combined
            return

        if self.prev_combined_vwap > 0:
            comb_threshold = self._exit_cfg(
                'combined_vwap_slope.threshold', float, 0.0)
            comb_slope = (combined - self.prev_combined_vwap) / self.prev_combined_vwap

            if comb_slope > comb_threshold:
                logger.info(
                    f"[SellManager] Combined VWAP slope rising: "
                    f"{self.prev_combined_vwap:.2f} → {combined:.2f} "
                    f"({comb_slope*100:+.3f}%). Checking individual legs.")

                ind_threshold = self._exit_cfg(
                    'individual_vwap_slope.threshold', float, 0.0)
                ind_tf = self._exit_cfg('individual_vwap_slope.tf', int, 1)

                for side in ['CE', 'PE']:
                    placed = self.ce_placed if side == 'CE' else self.pe_placed
                    if not placed:
                        continue
                    key = self.ce_key if side == 'CE' else self.pe_key
                    live_vwap = ce_vwap if side == 'CE' else pe_vwap
                    result = await self.orchestrator.indicator_manager.get_vwap_slope_status(
                        key, timestamp, int(ind_tf), count=1, live_vwap=live_vwap)
                    if result and result[2] is not None and result[3] is not None:
                        _, _, v_curr, v_prev, _, _ = result
                        if v_prev and v_prev > 0:
                            ind_slope = (v_curr - v_prev) / v_prev
                            if ind_slope > ind_threshold:
                                logger.info(
                                    f"[SellManager] {side} individual VWAP "
                                    f"slope rising ({ind_slope*100:+.3f}%) — exiting.")
                                await self.exit_side(
                                    side, timestamp,
                                    reason="Individual VWAP slope rising above threshold")

        self.prev_combined_vwap = combined

    # ─────────────────────────────────────────────────────────────────────
    # Exit one leg
    # ─────────────────────────────────────────────────────────────────────

    async def exit_side(self, side, timestamp, reason='Exit'):
        """
        Buy back one sell leg and restart the search for that side.
        """
        strike = self.ce_strike if side == 'CE' else self.pe_strike
        key = self.ce_key if side == 'CE' else self.pe_key
        entry_ltp = self.ce_entry_ltp if side == 'CE' else self.pe_entry_ltp
        entry_ts = self.ce_entry_timestamp if side == 'CE' else self.pe_entry_timestamp
        contract = self.ce_contract if side == 'CE' else self.pe_contract

        if strike is None or contract is None:
            logger.warning(
                f"[SellManager] exit_side({side}): no active position — skipping.")
            return

        exit_ltp = None
        product_type = self._cfg('product_type', str, 'NRML')
        brokers = self.orchestrator.broker_manager.brokers

        for broker in brokers:
            if not broker.is_configured_for_instrument(self.orchestrator.instrument_name):
                continue
            qty = (broker.config_manager.get_int(broker.instance_name, 'quantity', 1)
                   * contract.lot_size)
            if self.orchestrator.is_backtest or getattr(broker, 'paper_trade', False):
                if self.orchestrator.is_backtest and key:
                    exit_ltp = await self.orchestrator._get_ltp_for_backtest_instrument(
                        key, timestamp)
                logger.info(
                    f"[SellManager][PAPER BUY {product_type}] {side}: "
                    f"strike={int(strike)} qty={qty} reason={reason}")
            else:
                order_id = broker.place_order(
                    contract, 'BUY', qty, self.expiry, product_type=product_type)
                logger.info(
                    f"[SellManager] Closed {side} {int(strike)} "
                    f"order_id={order_id} reason={reason}")

        # Record PnL
        if entry_ltp and exit_ltp and self.orchestrator.pnl_tracker:
            ref_broker = next(
                (b for b in brokers
                 if b.is_configured_for_instrument(self.orchestrator.instrument_name)), None)
            broker_qty = (ref_broker.config_manager.get_int(
                ref_broker.instance_name, 'quantity', 1) if ref_broker else 1)
            pnl = (entry_ltp - exit_ltp) * contract.lot_size * broker_qty
            pnl_side = 'CALL' if side == 'CE' else 'PUT'
            logger.info(
                f"[SellManager] {side} PnL: entry={entry_ltp:.2f} "
                f"exit={exit_ltp:.2f} pnl=₹{pnl:+.2f}")
            self.orchestrator.pnl_tracker.trade_history.append({
                'instrument_key': key,
                'entry_price': entry_ltp,
                'exit_price': exit_ltp,
                'entry_timestamp': entry_ts,
                'exit_timestamp': timestamp,
                'pnl': pnl,
                'lot_size': contract.lot_size,
                'quantity': broker_qty,
                'status': 'CLOSED',
                'side': pnl_side,
                'strike_price': strike,
                'contract': contract,
                'strategy_log': f'SELL {product_type} leg — {reason}',
                'entry_type': 'SELL',
            })

        # Reset this side → restart search
        if side == 'CE':
            self.ce_placed = False
            self.ce_strike = None
            self.ce_key = None
            self.ce_entry_ltp = None
            self.ce_entry_timestamp = None
            self.ce_contract = None
            self.ce_searching = True
        else:
            self.pe_placed = False
            self.pe_strike = None
            self.pe_key = None
            self.pe_entry_ltp = None
            self.pe_entry_timestamp = None
            self.pe_contract = None
            self.pe_searching = True

        self.prev_combined_vwap = None   # reset combined tracking
        self.save_state()
        logger.info(f"[SellManager] {side} exited ({reason}). Fresh search started.")

    # ─────────────────────────────────────────────────────────────────────
    # EOD close — close whatever is still open
    # ─────────────────────────────────────────────────────────────────────

    async def close_all(self, timestamp):
        """Called at end-of-day to buy back any open sell legs."""
        if self.strangle_closed:
            return

        closed_any = False
        for side in ['CE', 'PE']:
            placed = self.ce_placed if side == 'CE' else self.pe_placed
            if placed:
                await self.exit_side(side, timestamp, reason='EOD Close')
                closed_any = True

        self.strangle_closed = True
        self.save_state()
        if not closed_any:
            logger.info("[SellManager] close_all: no open legs to close.")
        else:
            logger.info("[SellManager] EOD close completed.")

    # ─────────────────────────────────────────────────────────────────────
    # Backward-compat stub used by base_orchestrator.broadcast_signal
    # ─────────────────────────────────────────────────────────────────────

    def get_buy_strike(self, direction):
        """
        No longer drives hedge selection — buy strike is always ATM
        (handled by signal_monitor / trade_executor).
        Returning (None, None) allows broadcast_signal to proceed normally.
        """
        return None, None

    # ─────────────────────────────────────────────────────────────────────
    # Backtest support
    # ─────────────────────────────────────────────────────────────────────

    async def _find_best_backtest_candidate(self, candidates, timestamp, ltp_min, ltp_target):
        """Backtest: fetch real historical LTPs and apply the same selection logic."""
        best_diff = float('inf')
        best_strike = best_key = best_ltp = None
        for strike, inst_key in candidates:
            hist_ltp = await self.orchestrator._get_ltp_for_backtest_instrument(
                inst_key, timestamp)
            if hist_ltp and hist_ltp >= ltp_min:
                diff = abs(hist_ltp - ltp_target)
                if diff < best_diff:
                    best_diff = diff
                    best_strike, best_key, best_ltp = strike, inst_key, hist_ltp
        return best_strike, best_key, best_ltp

    async def execute_short_strangle_backtest(self, timestamp):
        """
        Backtest entry point — evaluate candidates at `timestamp`, pick best by
        LTP, check slope, enter each side that qualifies.
        Called from backtest orchestrator when the sell start time is reached.
        """
        if self.strangle_closed or (self.ce_placed and self.pe_placed):
            return
        if not self.ce_candidates and not self.pe_candidates:
            logger.warning("[SellManager][Backtest] No candidates built — skipping entry.")
            return

        ltp_min = self._cfg('ltp_min', float, 50.0)
        ltp_target = self._cfg('ltp_target', float, 50.0)
        product_type = self._cfg('product_type', str, 'NRML')

        for side in ['CE', 'PE']:
            placed = self.ce_placed if side == 'CE' else self.pe_placed
            if placed:
                continue
            candidates = self.ce_candidates if side == 'CE' else self.pe_candidates
            strike, inst_key, ltp = await self._find_best_backtest_candidate(
                candidates, timestamp, ltp_min, ltp_target)
            if strike is None:
                logger.info(
                    f"[SellManager][Backtest] {side}: no candidate with "
                    f"hist LTP >= {ltp_min} at {timestamp}.")
                continue

            slope_ok = await self._check_slope_decreasing(inst_key, timestamp)
            if not slope_ok:
                logger.info(
                    f"[SellManager][Backtest] {side} {int(strike)}: "
                    f"LTP={ltp:.2f} OK but slope not decreasing at {timestamp}.")
                continue

            expiry_strikes = self.orchestrator.atm_manager.contract_lookup.get(
                self.expiry, {})
            contract = expiry_strikes.get(float(strike), {}).get(side)
            if not contract:
                logger.error(
                    f"[SellManager][Backtest] {side} strike {strike} "
                    f"not in contract_lookup.")
                continue

            logger.info(
                f"[SellManager][Backtest PAPER SELL {product_type}] "
                f"{side}: strike={int(strike)} histLTP={ltp:.2f}")

            if side == 'CE':
                self.ce_placed = True
                self.ce_searching = False
                self.ce_strike = strike
                self.ce_key = inst_key
                self.ce_entry_ltp = ltp
                self.ce_entry_timestamp = timestamp
                self.ce_contract = contract
            else:
                self.pe_placed = True
                self.pe_searching = False
                self.pe_strike = strike
                self.pe_key = inst_key
                self.pe_entry_ltp = ltp
                self.pe_entry_timestamp = timestamp
                self.pe_contract = contract

        self.save_state()

    # ─────────────────────────────────────────────────────────────────────
    # State persistence
    # ─────────────────────────────────────────────────────────────────────

    def save_state(self):
        state = {
            'ce_placed': self.ce_placed,
            'pe_placed': self.pe_placed,
            'ce_strike': self.ce_strike,
            'pe_strike': self.pe_strike,
            'ce_key': self.ce_key,
            'pe_key': self.pe_key,
            'ce_entry_ltp': self.ce_entry_ltp,
            'pe_entry_ltp': self.pe_entry_ltp,
            'strangle_closed': self.strangle_closed,
            'expiry': self.expiry.isoformat() if self.expiry else None,
        }
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"[SellManager] Failed to save state: {e}")

    def load_state(self):
        if not os.path.exists(self.state_file):
            return
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            expiry_str = state.get('expiry')
            expiry = datetime.date.fromisoformat(expiry_str) if expiry_str else None
            if expiry and expiry < datetime.date.today():
                logger.info(f"[SellManager] Stale state (expiry {expiry}), ignoring.")
                return
            self.ce_placed = state.get('ce_placed', False)
            self.pe_placed = state.get('pe_placed', False)
            self.ce_strike = state.get('ce_strike')
            self.pe_strike = state.get('pe_strike')
            self.ce_key = state.get('ce_key')
            self.pe_key = state.get('pe_key')
            self.ce_entry_ltp = state.get('ce_entry_ltp')
            self.pe_entry_ltp = state.get('pe_entry_ltp')
            self.strangle_closed = state.get('strangle_closed', False)
            self.expiry = expiry
            logger.info(
                f"[SellManager] State loaded: CE={self.ce_placed} PE={self.pe_placed} "
                f"expiry={expiry}")
        except Exception as e:
            logger.error(f"[SellManager] Failed to load state: {e}")
