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
        self.expiry = None

    async def find_100_nearest_strike(self, expiry, side):
        """
        Finds the strike with LTP > 100 that is closest to ₹100 for the given side (CE or PE).
        Returns (float(strike_price), contract) or (None, None) if not found.
        """
        expiry_strikes = self.orchestrator.atm_manager.contract_lookup.get(expiry, {})
        if not expiry_strikes:
            logger.error(f"[SellManager] No contracts found for expiry {expiry}")
            return None, None

        key_to_contract = {}
        for strike, sides in expiry_strikes.items():
            contract = sides.get(side)
            if contract:
                key_to_contract[contract.instrument_key] = (strike, contract)

        if not key_to_contract:
            logger.error(f"[SellManager] No {side} contracts found for expiry {expiry}")
            return None, None

        ltp_map = await self.orchestrator.rest_client.get_ltps(list(key_to_contract.keys()))

        candidates = []
        for inst_key, (strike, contract) in key_to_contract.items():
            ltp = ltp_map.get(inst_key, 0.0)
            if ltp > 100:
                candidates.append((abs(ltp - 100), ltp, float(strike), contract))

        if not candidates:
            logger.error(f"[SellManager] No {side} strikes with LTP > 100 found for expiry {expiry}")
            return None, None

        candidates.sort(key=lambda x: x[0])
        _, ltp, strike, contract = candidates[0]
        logger.info(f"[SellManager] Selected {side} sell strike: {strike} (LTP: {ltp:.2f})")
        return strike, contract

    async def execute_short_strangle(self, timestamp):
        """Places SELL NRML orders for both CE and PE legs at market open."""
        if self.strangle_placed:
            logger.info("[SellManager] Strangle already placed, skipping.")
            return

        expiry = self.orchestrator.atm_manager.signal_expiry_date
        if not expiry:
            logger.error("[SellManager] Cannot place strangle — signal_expiry_date not set.")
            return

        ce_strike, ce_contract = await self.find_100_nearest_strike(expiry, 'CE')
        pe_strike, pe_contract = await self.find_100_nearest_strike(expiry, 'PE')

        if ce_strike is None or pe_strike is None:
            logger.error("[SellManager] Could not find valid strikes for strangle. Aborting.")
            return

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
        self.sell_ce_key = ce_contract.instrument_key
        self.sell_pe_key = pe_contract.instrument_key
        self.expiry = expiry
        self.strangle_placed = True
        self.save_state()
        logger.info(f"[SellManager] Short strangle placed: SELL CE {ce_strike} | SELL PE {pe_strike} | Expiry: {expiry}")

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
        CALL signal → buy CE at sell_ce_strike + 100
        PUT  signal → buy PE at sell_pe_strike - 100
        Returns (None, None) if strangle not placed or hedge strike not found.
        """
        if not self.strangle_placed:
            return None, None

        expiry_strikes = self.orchestrator.atm_manager.contract_lookup.get(self.expiry, {})

        if direction == 'CALL':
            hedge_strike = self.sell_ce_strike + 100
            side = 'CE'
        elif direction == 'PUT':
            hedge_strike = self.sell_pe_strike - 100
            side = 'PE'
        else:
            return None, None

        contract = expiry_strikes.get(float(hedge_strike), {}).get(side)
        if not contract:
            logger.warning(f"[SellManager] Hedge strike {hedge_strike}{side} not found in contract_lookup")
            return None, None

        return hedge_strike, contract.instrument_key

    def save_state(self):
        """Persists strangle state to JSON so a mid-day restart doesn't re-place orders."""
        state = {
            'strangle_placed': self.strangle_placed,
            'strangle_closed': self.strangle_closed,
            'sell_ce_strike': self.sell_ce_strike,
            'sell_pe_strike': self.sell_pe_strike,
            'sell_ce_key': self.sell_ce_key,
            'sell_pe_key': self.sell_pe_key,
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
        """Restores strangle state from JSON file if it exists and is from today."""
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
            self.expiry = expiry

            if self.strangle_placed:
                logger.info(f"[SellManager] Restored state: CE {self.sell_ce_strike} | PE {self.sell_pe_strike} | Expiry: {expiry} | Closed: {self.strangle_closed}")
        except Exception as e:
            logger.error(f"[SellManager] Failed to load state: {e}")
