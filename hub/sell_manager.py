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
        self.expiry = None

    # ------------------------------------------------------------------
    # Internal helpers — work directly on a pre-fetched chain list
    # ------------------------------------------------------------------

    def _find_from_chain(self, chain, side):
        """
        Scans option chain data for the strike with LTP > 100 closest to ₹100
        on the given side ('CE' or 'PE').

        Returns (float(strike_price), contract, instrument_key) or (None, None, None).
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
            if ltp > 100 and strike_price is not None and inst_key:
                candidates.append((abs(ltp - 100), ltp, float(strike_price), inst_key))

        if not candidates:
            logger.error(f"[SellManager] No {side} strikes with LTP > 100 found in option chain")
            return None, None, None

        candidates.sort(key=lambda x: x[0])
        _, ltp, strike, inst_key = candidates[0]
        logger.info(f"[SellManager] Selected {side} sell strike: {strike} (LTP: {ltp:.2f}, key: {inst_key})")

        # Look up contract for lot_size
        expiry_strikes = self.orchestrator.atm_manager.contract_lookup.get(self.expiry, {})
        contract = expiry_strikes.get(float(strike), {}).get(side)
        if not contract:
            logger.error(f"[SellManager] {side} strike {strike} not found in contract_lookup — cannot determine lot_size")
            return None, None, None

        return strike, contract, inst_key

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

        # Fetch the full option chain once for all lookups
        logger.info(f"[SellManager] Fetching option chain for {index_key} expiry {expiry}")
        chain = await self.orchestrator.rest_client.get_option_chain(index_key, expiry)
        if not chain:
            logger.error(f"[SellManager] Option chain returned empty for {index_key} expiry {expiry}. Aborting strangle.")
            return

        ce_strike, ce_contract, ce_sell_key = self._find_from_chain(chain, 'CE')
        pe_strike, pe_contract, pe_sell_key = self._find_from_chain(chain, 'PE')

        if ce_strike is None or pe_strike is None:
            logger.error("[SellManager] Could not find valid strikes for strangle. Aborting.")
            return

        # Resolve hedge keys from the same chain data
        buy_ce_key = self._find_hedge_key_in_chain(chain, ce_strike + 100, 'CE')
        buy_pe_key = self._find_hedge_key_in_chain(chain, pe_strike - 100, 'PE')

        if not buy_ce_key:
            logger.warning(f"[SellManager] CE hedge strike {ce_strike + 100} not found in chain — hedge BUY may fail")
        if not buy_pe_key:
            logger.warning(f"[SellManager] PE hedge strike {pe_strike - 100} not found in chain — hedge BUY may fail")

        brokers = self.orchestrator.broker_manager.brokers
        for broker in brokers:
            if not broker.is_configured_for_instrument(self.orchestrator.instrument_name):
                continue

            ce_qty = broker.config_manager.get_int(broker.instance_name, 'quantity', 1) * ce_contract.lot_size
            pe_qty = broker.config_manager.get_int(broker.instance_name, 'quantity', 1) * pe_contract.lot_size

            if getattr(broker, 'paper_trade', False):
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
        self.strangle_placed = True
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

            if getattr(broker, 'paper_trade', False):
                logger.info(f"[SellManager][PAPER BUY NRML CLOSE] CE: {self.sell_ce_strike} qty={ce_qty} | PE: {self.sell_pe_strike} qty={pe_qty}")
            else:
                ce_order_id = broker.place_order(ce_contract, 'BUY', ce_qty, expiry, product_type='NRML')
                pe_order_id = broker.place_order(pe_contract, 'BUY', pe_qty, expiry, product_type='NRML')
                logger.info(f"[SellManager] Closed CE {self.sell_ce_strike} order_id={ce_order_id} | PE {self.sell_pe_strike} order_id={pe_order_id}")

        self.strangle_closed = True
        self.save_state()
        logger.info(f"[SellManager] Short strangle closed: CE {self.sell_ce_strike} | PE {self.sell_pe_strike}")

    def get_buy_strike(self, direction):
        """
        Returns the hedge (BUY) strike and instrument_key for the given signal direction.
          CALL signal → buy CE at sell_ce_strike + 100  (key stored as buy_ce_key)
          PUT  signal → buy PE at sell_pe_strike - 100  (key stored as buy_pe_key)
        Returns (None, None) if strangle not placed or hedge key was not resolved.
        """
        if not self.strangle_placed:
            return None, None

        if direction == 'CALL':
            hedge_strike = self.sell_ce_strike + 100
            hedge_key = self.buy_ce_key
        elif direction == 'PUT':
            hedge_strike = self.sell_pe_strike - 100
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
            'expiry': self.expiry.isoformat() if self.expiry else None
        }
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.debug(f"[SellManager] State saved to {self.state_file}")
        except Exception as e:
            logger.error(f"[SellManager] Failed to save state: {e}")

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
