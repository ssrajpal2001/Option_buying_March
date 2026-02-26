import json
import os
import datetime
from utils.logger import logger


class SellManager:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.state_file = f'config/sell_state_{orchestrator.instrument_name}.json'

        self.strangle_placed = False
        self.strangle_closed = False
        self.sell_ce_strike = None
        self.sell_pe_strike = None
        self.sell_ce_key = None
        self.sell_pe_key = None
        self.buy_ce_key = None   # instrument_key of sell_ce_strike + 100
        self.buy_pe_key = None   # instrument_key of sell_pe_strike - 100
        self.sell_ce_entry_ltp = None
        self.sell_pe_entry_ltp = None
        self.buy_ce_entry_ltp = None   # hedge CE entry LTP at strangle placement
        self.buy_pe_entry_ltp = None   # hedge PE entry LTP at strangle placement
        self.sell_entry_timestamp = None
        self.sell_ce_contract = None
        self.sell_pe_contract = None
        self.expiry = None

    # ------------------------------------------------------------------
    # Internal helpers — work directly on a pre-fetched chain list
    # ------------------------------------------------------------------

    def _find_from_chain(self, chain, side):
        """
        Scans option chain data for the strike with LTP closest to ₹100
        on the given side ('CE' or 'PE').

        Returns (float(strike_price), contract, instrument_key, ltp) or (None, None, None, None).
        The contract object is looked up from contract_lookup for its lot_size.
        """
        options_key = 'call_options' if side == 'CE' else 'put_options'

        candidates = []
        for entry in chain:
            side_data = entry.get(options_key) or {}
            market = side_data.get('market_data') or {}
            ltp = market.get('ltp', 0) or 0
            strike_price = entry.get('strike_price')
            inst_key = side_data.get('instrument_key')
            # User requirement: LTP must be > 100
            if ltp > 100 and strike_price is not None and inst_key:
                candidates.append((ltp, float(strike_price), inst_key))

        if not candidates:
            logger.warning(f"[SellManager] No {side} strikes found with LTP > 100. Falling back to closest available.")
            for entry in chain:
                side_data = entry.get(options_key) or {}
                market = side_data.get('market_data') or {}
                ltp = market.get('ltp', 0) or 0
                strike_price = entry.get('strike_price')
                inst_key = side_data.get('instrument_key')
                if ltp > 0 and strike_price is not None and inst_key:
                    candidates.append((ltp, float(strike_price), inst_key))

            if not candidates:
                logger.error(f"[SellManager] No {side} strikes found in option chain")
                return None, None, None, None

            candidates.sort(key=lambda x: abs(x[0] - 100))
        else:
            # Pick the strike closest to 100 among those that are > 100
            candidates.sort(key=lambda x: x[0])

        ltp, strike, inst_key = candidates[0]
        logger.info(f"[SellManager] Selected {side} sell strike: {strike} (LTP: {ltp:.2f}, key: {inst_key})")

        expiry_strikes = self.orchestrator.atm_manager.contract_lookup.get(self.expiry, {})
        contract = expiry_strikes.get(float(strike), {}).get(side)
        if not contract:
            logger.error(f"[SellManager] {side} strike {strike} not found in contract_lookup — cannot determine lot_size")
            return None, None, None, None

        return strike, contract, inst_key, ltp

    async def _find_from_chain_backtest(self, chain, side, timestamp):
        """
        Backtest version of _find_from_chain. Fetches the real historical LTP at
        `timestamp` for each candidate strike via the Upstox REST OHLC API, then
        applies the same 'closest to ₹100' selection logic on those historical prices.

        Expects `chain` to already be filtered to the target ATM-anchored strikes
        (typically 12 candidates) to minimise API calls.
        """
        options_key = 'call_options' if side == 'CE' else 'put_options'

        candidates_raw = []
        for entry in chain:
            side_data = entry.get(options_key) or {}
            inst_key = side_data.get('instrument_key')
            strike_price = entry.get('strike_price')
            if inst_key and strike_price is not None:
                candidates_raw.append((float(strike_price), inst_key))

        logger.info(f"[SellManager][Backtest] Fetching historical LTPs for {len(candidates_raw)} {side} candidates at {timestamp}...")

        candidates = []
        for strike, inst_key in candidates_raw:
            hist_ltp = await self.orchestrator._get_ltp_for_backtest_instrument(inst_key, timestamp)
            # User requirement: LTP must be > 100
            if hist_ltp is not None and hist_ltp > 100:
                candidates.append((hist_ltp, strike, inst_key))

        if not candidates:
            logger.warning(f"[SellManager][Backtest] No {side} strikes with historical LTP > 100 found at {timestamp}. Falling back to closest available.")
            for strike, inst_key in candidates_raw:
                hist_ltp = await self.orchestrator._get_ltp_for_backtest_instrument(inst_key, timestamp)
                if hist_ltp is not None and hist_ltp > 0:
                    candidates.append((hist_ltp, strike, inst_key))

            if not candidates:
                logger.error(f"[SellManager][Backtest] No {side} strikes with historical LTP found at {timestamp}")
                return None, None, None, None

            candidates.sort(key=lambda x: abs(x[0] - 100))
        else:
            # Pick the strike closest to 100 among those that are > 100
            candidates.sort(key=lambda x: x[0])

        hist_ltp, strike, inst_key = candidates[0]
        logger.info(f"[SellManager][Backtest] Selected {side} sell strike: {strike} (Historical LTP at {timestamp}: {hist_ltp:.2f}, key: {inst_key})")

        expiry_strikes = self.orchestrator.atm_manager.contract_lookup.get(self.expiry, {})
        contract = expiry_strikes.get(float(strike), {}).get(side)
        if not contract:
            logger.error(f"[SellManager][Backtest] {side} strike {strike} not found in contract_lookup — cannot determine lot_size")
            return None, None, None, None

        return strike, contract, inst_key, hist_ltp

    def _find_hedge_key_in_chain(self, chain, hedge_strike, side):
        """
        Finds the instrument_key for hedge_strike on side ('CE' or 'PE') in the chain data.
        Returns instrument_key string or None.
        """
        options_key = 'call_options' if side == 'CE' else 'put_options'
        for entry in chain:
            if float(entry.get('strike_price', -1)) == float(hedge_strike):
                inst_key = (entry.get(options_key) or {}).get('instrument_key')
                if inst_key:
                    return inst_key
        logger.warning(f"[SellManager] Hedge strike {hedge_strike} {side} not found in option chain")
        return None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def execute_short_strangle(self, timestamp):
        """Places SELL NRML orders for both CE and PE legs at market open."""
        if self.strangle_placed:
            logger.info("[SellManager] Strangle already placed, skipping.")
            return

        expiry = self.orchestrator.atm_manager.signal_expiry_date
        if not expiry:
            logger.error("[SellManager] Cannot place strangle — signal_expiry_date not set.")
            return

        self.expiry = expiry
        index_key = self.orchestrator.index_instrument_key

        # Fetch the full option chain once for hedge key lookup
        logger.info(f"[SellManager] Fetching option chain for {index_key} expiry {expiry}")
        chain = await self.orchestrator.rest_client.get_option_chain(index_key, expiry)
        if not chain:
            logger.error(f"[SellManager] Option chain returned empty for {index_key} expiry {expiry}. Aborting strangle.")
            return

        # --- T001: ATM-anchored strike filtering ---
        # CE search: 8 strikes above ATM to 3 strikes below ATM.
        # PE search: 8 strikes below ATM to 3 strikes above ATM.
        # This reduces backtest historical LTP API calls from 63+ to 24.
        atm = self.orchestrator.atm_manager.strikes.get('atm')
        interval = self.orchestrator.config_manager.get_int(
            self.orchestrator.instrument_name, 'strike_interval', 50)

        if atm and interval:
            # CE: range(-3, 9) covers 3 strikes below to 8 strikes above ATM
            ce_targets = {float(atm + i * interval) for i in range(-3, 9)}
            # PE: range(-8, 4) covers 8 strikes below to 3 strikes above ATM
            pe_targets = {float(atm + i * interval) for i in range(-8, 4)}
            logger.info(
                f"[SellManager] ATM={atm} interval={interval} — "
                f"CE search: {int(atm - 3*interval)}–{int(atm + 8*interval)} | "
                f"PE search: {int(atm - 8*interval)}–{int(atm + 3*interval)}"
            )
            ce_chain = [e for e in chain if float(e.get('strike_price', -1)) in ce_targets]
            pe_chain = [e for e in chain if float(e.get('strike_price', -1)) in pe_targets]
        else:
            logger.warning("[SellManager] ATM or interval not available — falling back to full chain scan")
            ce_chain = chain
            pe_chain = chain

        if self.orchestrator.is_backtest:
            ce_strike, ce_contract, ce_sell_key, ce_entry_ltp = await self._find_from_chain_backtest(ce_chain, 'CE', timestamp)
            pe_strike, pe_contract, pe_sell_key, pe_entry_ltp = await self._find_from_chain_backtest(pe_chain, 'PE', timestamp)
        else:
            ce_strike, ce_contract, ce_sell_key, ce_entry_ltp = self._find_from_chain(ce_chain, 'CE')
            pe_strike, pe_contract, pe_sell_key, pe_entry_ltp = self._find_from_chain(pe_chain, 'PE')

        if ce_strike is None or pe_strike is None:
            logger.error("[SellManager] Could not find valid strikes for strangle. Aborting.")
            return

        # Resolve hedge keys from the full chain (hedge may be outside the 5-strike window)
        buy_ce_key = self._find_hedge_key_in_chain(chain, ce_strike + 150, 'CE')
        buy_pe_key = self._find_hedge_key_in_chain(chain, pe_strike - 150, 'PE')

        if not buy_ce_key:
            logger.warning(f"[SellManager] CE hedge strike {ce_strike + 150} not found in chain — hedge BUY may fail")
        if not buy_pe_key:
            logger.warning(f"[SellManager] PE hedge strike {pe_strike - 150} not found in chain — hedge BUY may fail")

        # --- T002: Fetch hedge entry LTPs at strangle placement (backtest only) ---
        if self.orchestrator.is_backtest:
            self.buy_ce_entry_ltp = (
                await self.orchestrator._get_ltp_for_backtest_instrument(buy_ce_key, timestamp)
                if buy_ce_key else None
            )
            self.buy_pe_entry_ltp = (
                await self.orchestrator._get_ltp_for_backtest_instrument(buy_pe_key, timestamp)
                if buy_pe_key else None
            )
            logger.info(
                f"[SellManager][Backtest] Hedge entry LTPs recorded: "
                f"CE hedge {int(ce_strike + 150)}={self.buy_ce_entry_ltp} | "
                f"PE hedge {int(pe_strike - 150)}={self.buy_pe_entry_ltp}"
            )

        brokers = self.orchestrator.broker_manager.brokers
        for broker in brokers:
            if not broker.is_configured_for_instrument(self.orchestrator.instrument_name):
                continue

            ce_qty = broker.config_manager.get_int(broker.instance_name, 'quantity', 1) * ce_contract.lot_size
            pe_qty = broker.config_manager.get_int(broker.instance_name, 'quantity', 1) * pe_contract.lot_size

            if self.orchestrator.is_backtest or getattr(broker, 'paper_trade', False):
                logger.info(f"[SellManager][PAPER SELL NRML] CE: {ce_strike} qty={ce_qty} | PE: {pe_strike} qty={pe_qty}")
            else:
                ce_order_id = broker.place_order(ce_contract, 'SELL', ce_qty, expiry, product_type='NRML')
                pe_order_id = broker.place_order(pe_contract, 'SELL', pe_qty, expiry, product_type='NRML')
                logger.info(f"[SellManager] Sold CE {ce_strike} order_id={ce_order_id} | PE {pe_strike} order_id={pe_order_id}")

        self.sell_ce_strike = ce_strike
        self.sell_pe_strike = pe_strike
        self.sell_ce_key = ce_sell_key
        self.sell_pe_key = pe_sell_key
        self.buy_ce_key = buy_ce_key
        self.buy_pe_key = buy_pe_key
        self.sell_ce_entry_ltp = ce_entry_ltp
        self.sell_pe_entry_ltp = pe_entry_ltp
        self.sell_entry_timestamp = timestamp
        self.sell_ce_contract = ce_contract
        self.sell_pe_contract = pe_contract
        self.strangle_placed = True
        all_strangle_keys = [k for k in [self.sell_ce_key, self.sell_pe_key, self.buy_ce_key, self.buy_pe_key] if k]
        if all_strangle_keys:
            self.orchestrator.websocket.subscribe(all_strangle_keys)
            logger.info(f"[SellManager] Subscribed all 4 strangle keys to websocket: {all_strangle_keys}")
        self.save_state()
        logger.info(
            f"[SellManager] Short strangle placed: "
            f"SELL CE {ce_strike} (key:{ce_sell_key}) | SELL PE {pe_strike} (key:{pe_sell_key}) | "
            f"Hedge CE key:{buy_ce_key} | Hedge PE key:{buy_pe_key} | Expiry: {expiry}"
        )

    async def close_all(self, timestamp):
        """Buys back both CE and PE legs to close the short strangle (EOD close)."""
        if self.strangle_closed:
            logger.info("[SellManager] Strangle already closed, skipping.")
            return
        if not self.strangle_placed:
            logger.info("[SellManager] No strangle to close.")
            return

        expiry = self.expiry
        expiry_strikes = self.orchestrator.atm_manager.contract_lookup.get(expiry, {})

        ce_contract = expiry_strikes.get(float(self.sell_ce_strike), {}).get('CE')
        pe_contract = expiry_strikes.get(float(self.sell_pe_strike), {}).get('PE')

        if not ce_contract or not pe_contract:
            logger.error(f"[SellManager] Could not find CE/PE contracts to close. CE:{self.sell_ce_strike} PE:{self.sell_pe_strike}")
            return

        brokers = self.orchestrator.broker_manager.brokers
        for broker in brokers:
            if not broker.is_configured_for_instrument(self.orchestrator.instrument_name):
                continue

            ce_qty = broker.config_manager.get_int(broker.instance_name, 'quantity', 1) * ce_contract.lot_size
            pe_qty = broker.config_manager.get_int(broker.instance_name, 'quantity', 1) * pe_contract.lot_size

            if self.orchestrator.is_backtest or getattr(broker, 'paper_trade', False):
                logger.info(f"[SellManager][PAPER BUY NRML CLOSE] CE: {self.sell_ce_strike} qty={ce_qty} | PE: {self.sell_pe_strike} qty={pe_qty}")
            else:
                ce_order_id = broker.place_order(ce_contract, 'BUY', ce_qty, expiry, product_type='NRML')
                pe_order_id = broker.place_order(pe_contract, 'BUY', pe_qty, expiry, product_type='NRML')
                logger.info(f"[SellManager] Closed CE {self.sell_ce_strike} order_id={ce_order_id} | PE {self.sell_pe_strike} order_id={pe_order_id}")

        self.strangle_closed = True
        self.save_state()
        logger.info(f"[SellManager] Short strangle closed: CE {self.sell_ce_strike} | PE {self.sell_pe_strike}")

        if self.orchestrator.is_backtest and self.sell_ce_entry_ltp and self.sell_pe_entry_ltp:
            ce_exit_ltp = await self.orchestrator._get_ltp_for_backtest_instrument(self.sell_ce_key, timestamp)
            pe_exit_ltp = await self.orchestrator._get_ltp_for_backtest_instrument(self.sell_pe_key, timestamp)
            if ce_exit_ltp and pe_exit_ltp:
                _ref_broker = next(
                    (b for b in self.orchestrator.broker_manager.brokers
                     if b.is_configured_for_instrument(self.orchestrator.instrument_name)), None
                )
                _broker_qty = _ref_broker.config_manager.get_int(_ref_broker.instance_name, 'quantity', 1) if _ref_broker else 1
                ce_lot = ce_contract.lot_size if ce_contract else 1
                pe_lot = pe_contract.lot_size if pe_contract else 1

                ce_pnl_per = self.sell_ce_entry_ltp - ce_exit_ltp
                pe_pnl_per = self.sell_pe_entry_ltp - pe_exit_ltp
                ce_pnl_total = ce_pnl_per * ce_lot * _broker_qty
                pe_pnl_total = pe_pnl_per * pe_lot * _broker_qty
                logger.info(
                    f"[SellManager] SELL STRANGLE PnL at close: "
                    f"CE {int(self.sell_ce_strike)} entry={self.sell_ce_entry_ltp:.2f} exit={ce_exit_ltp:.2f} "
                    f"PnL/share={ce_pnl_per:+.2f} × lot={ce_lot} × qty={_broker_qty} = ₹{ce_pnl_total:+.2f} | "
                    f"PE {int(self.sell_pe_strike)} entry={self.sell_pe_entry_ltp:.2f} exit={pe_exit_ltp:.2f} "
                    f"PnL/share={pe_pnl_per:+.2f} × lot={pe_lot} × qty={_broker_qty} = ₹{pe_pnl_total:+.2f} | "
                    f"Combined Total PnL=₹{ce_pnl_total + pe_pnl_total:+.2f}"
                )
                if self.orchestrator.pnl_tracker:
                    for side, key, strike, entry_ltp, exit_ltp, pnl_total, lot, contract in [
                        ('CALL', self.sell_ce_key, self.sell_ce_strike, self.sell_ce_entry_ltp, ce_exit_ltp, ce_pnl_total, ce_lot, self.sell_ce_contract),
                        ('PUT',  self.sell_pe_key, self.sell_pe_strike, self.sell_pe_entry_ltp, pe_exit_ltp, pe_pnl_total, pe_lot, self.sell_pe_contract),
                    ]:
                        self.orchestrator.pnl_tracker.trade_history.append({
                            'instrument_key': key,
                            'entry_price': entry_ltp,
                            'exit_price': exit_ltp,
                            'entry_timestamp': self.sell_entry_timestamp,
                            'exit_timestamp': timestamp,
                            'pnl': pnl_total,
                            'lot_size': lot,
                            'quantity': _broker_qty,
                            'status': 'CLOSED',
                            'side': side,
                            'strike_price': strike,
                            'contract': contract,
                            'strategy_log': 'SELL NRML strangle leg',
                            'entry_type': 'SELL',
                        })

                # --- T003: Hedge EOD P&L log (informational only — no double-count) ---
                # BUY hedge positions are already tracked via the normal pnl_tracker lifecycle:
                #   enter_trade at signal entry → exit_trade at SL hit → re-enter on new trade
                #   → force-exit at EOD via close_open_backtest_positions.
                # This block provides a consolidated log at EOD close for visibility.
                if self.buy_ce_key and self.buy_pe_key and self.buy_ce_entry_ltp and self.buy_pe_entry_ltp:
                    buy_ce_exit = await self.orchestrator._get_ltp_for_backtest_instrument(
                        self.buy_ce_key, timestamp)
                    buy_pe_exit = await self.orchestrator._get_ltp_for_backtest_instrument(
                        self.buy_pe_key, timestamp)
                    if buy_ce_exit and buy_pe_exit:
                        hedge_lot = ce_lot
                        hedge_qty = _broker_qty * 1
                        buy_ce_pnl = (buy_ce_exit - self.buy_ce_entry_ltp) * hedge_lot * hedge_qty
                        buy_pe_pnl = (buy_pe_exit - self.buy_pe_entry_ltp) * hedge_lot * hedge_qty
                        logger.info(
                            f"[SellManager] HEDGE EOD P&L (informational): "
                            f"CE hedge {int(self.sell_ce_strike + 150)} "
                            f"entry={self.buy_ce_entry_ltp:.2f} exit={buy_ce_exit:.2f} "
                            f"PnL/share={buy_ce_exit - self.buy_ce_entry_ltp:+.2f} × "
                            f"lot={hedge_lot} × qty={hedge_qty} = ₹{buy_ce_pnl:+.2f} | "
                            f"PE hedge {int(self.sell_pe_strike - 150)} "
                            f"entry={self.buy_pe_entry_ltp:.2f} exit={buy_pe_exit:.2f} "
                            f"PnL/share={buy_pe_exit - self.buy_pe_entry_ltp:+.2f} × "
                            f"lot={hedge_lot} × qty={hedge_qty} = ₹{buy_pe_pnl:+.2f} | "
                            f"Hedge Combined=₹{buy_ce_pnl + buy_pe_pnl:+.2f}"
                        )
                    else:
                        logger.warning("[SellManager][Backtest] Could not fetch EOD exit LTP for hedge legs.")
            else:
                logger.warning(f"[SellManager][Backtest] Could not fetch exit LTP for sell legs at {timestamp}. PnL not calculated.")

    def get_buy_strike(self, direction):
        """
        Returns the hedge (BUY) strike and instrument_key for the given signal direction.
          CALL signal → buy CE at sell_ce_strike + 150  (key stored as buy_ce_key)
          PUT  signal → buy PE at sell_pe_strike - 150  (key stored as buy_pe_key)
        Returns (None, None) if strangle not placed or hedge key was not resolved.
        """
        if not self.strangle_placed:
            return None, None

        if direction == 'CALL':
            hedge_strike = self.sell_ce_strike + 150
            hedge_key = self.buy_ce_key
        elif direction == 'PUT':
            hedge_strike = self.sell_pe_strike - 150
            hedge_key = self.buy_pe_key
        else:
            return None, None

        if not hedge_key:
            logger.warning(f"[SellManager] No stored hedge key for {direction} (hedge strike {hedge_strike})")
            return None, None

        return hedge_strike, hedge_key

    def save_state(self):
        """Persists strangle state to JSON so a mid-day restart doesn't re-place orders."""
        state = {
            'strangle_placed': self.strangle_placed,
            'strangle_closed': self.strangle_closed,
            'sell_ce_strike': self.sell_ce_strike,
            'sell_pe_strike': self.sell_pe_strike,
            'sell_ce_key': self.sell_ce_key,
            'sell_pe_key': self.sell_pe_key,
            'buy_ce_key': self.buy_ce_key,
            'buy_pe_key': self.buy_pe_key,
            'sell_ce_entry_ltp': self.sell_ce_entry_ltp,
            'sell_pe_entry_ltp': self.sell_pe_entry_ltp,
            'buy_ce_entry_ltp': self.buy_ce_entry_ltp,
            'buy_pe_entry_ltp': self.buy_pe_entry_ltp,
            'expiry': self.expiry.isoformat() if self.expiry else None
        }
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.debug(f"[SellManager] State saved to {self.state_file}")
        except Exception as e:
            logger.error(f"[SellManager] Failed to save state: {e}")

    def get_total_pnl(self):
        """Calculates total PnL (realized + unrealized) for the strangle legs."""
        realized = 0.0
        unrealized = 0.0

        # Unpack info for calculation
        _ref_broker = next(
            (b for b in self.orchestrator.broker_manager.brokers
             if b.is_configured_for_instrument(self.orchestrator.instrument_name)), None
        )
        _broker_qty = _ref_broker.config_manager.get_int(_ref_broker.instance_name, 'quantity', 1) if _ref_broker else 1

        # 1. Unrealized for active legs
        if self.strangle_placed and not self.strangle_closed:
            for key, entry_ltp, strike, side in [
                (self.sell_ce_key, self.sell_ce_entry_ltp, self.sell_ce_strike, 'CE'),
                (self.sell_pe_key, self.sell_pe_entry_ltp, self.sell_pe_strike, 'PE')
            ]:
                if key and entry_ltp:
                    curr_ltp = self.orchestrator.state_manager.get_ltp(key)
                    if curr_ltp:
                        # SELL leg: Entry - Current
                        expiry_strikes = self.orchestrator.atm_manager.contract_lookup.get(self.expiry, {})
                        contract = expiry_strikes.get(float(strike), {}).get(side)
                        lot = contract.lot_size if contract else 1
                        unrealized += (entry_ltp - curr_ltp) * lot * _broker_qty

        # 2. Realized from closed legs (already added to trade_history in close_all if backtest)
        # In live mode, we don't have a realized points tracker for SellManager yet.
        # But for "overall points", we usually care about the current session.
        return realized + unrealized

    def load_state(self):
        """Restores strangle state from JSON file if it exists and is from today's expiry."""
        if not os.path.exists(self.state_file):
            return

        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)

            expiry_str = state.get('expiry')
            expiry = datetime.date.fromisoformat(expiry_str) if expiry_str else None

            if expiry and expiry < datetime.date.today():
                logger.info(f"[SellManager] Stale state file (expiry {expiry}), ignoring.")
                return

            self.strangle_placed = state.get('strangle_placed', False)
            self.strangle_closed = state.get('strangle_closed', False)
            self.sell_ce_strike = state.get('sell_ce_strike')
            self.sell_pe_strike = state.get('sell_pe_strike')
            self.sell_ce_key = state.get('sell_ce_key')
            self.sell_pe_key = state.get('sell_pe_key')
            self.buy_ce_key = state.get('buy_ce_key')
            self.buy_pe_key = state.get('buy_pe_key')
            self.sell_ce_entry_ltp = state.get('sell_ce_entry_ltp')
            self.sell_pe_entry_ltp = state.get('sell_pe_entry_ltp')
            self.buy_ce_entry_ltp = state.get('buy_ce_entry_ltp')
            self.buy_pe_entry_ltp = state.get('buy_pe_entry_ltp')
            self.expiry = expiry

            if self.strangle_placed:
                logger.info(
                    f"[SellManager] Restored state: "
                    f"CE {self.sell_ce_strike} | PE {self.sell_pe_strike} | "
                    f"Hedge CE key:{self.buy_ce_key} | Hedge PE key:{self.buy_pe_key} | "
                    f"Expiry: {expiry} | Closed: {self.strangle_closed}"
                )
        except Exception as e:
            logger.error(f"[SellManager] Failed to load state: {e}")
