"""
Order State Machine — Atomic CE+PE entry with retry and leg rollback.

States:
  PENDING → CE_PLACED → CE_FILLED → PE_PLACED → OPEN
  On CE fill + PE fail → rollback CE (place market exit)
  On PE fail after 2 retries → mark FAILED, log order_failure
"""

import time
import logging
from enum import Enum
from typing import Optional

logger = logging.getLogger("algosoft")


class OrderState(str, Enum):
    PENDING = "PENDING"
    CE_PLACED = "CE_PLACED"
    CE_FILLED = "CE_FILLED"
    PE_PLACED = "PE_PLACED"
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    FAILED = "FAILED"
    ROLLED_BACK = "ROLLED_BACK"


RETRY_DELAYS = [5, 15]  # seconds between retries


class AtomicLegOrder:
    """
    Manages a two-legged (CE + PE) option entry atomically.
    If one leg fills and the other fails → immediately exit the filled leg.
    """

    def __init__(self, broker_client, instance_id: int, client_id: int, db_logger=None):
        self.broker = broker_client
        self.instance_id = instance_id
        self.client_id = client_id
        self.db_logger = db_logger

        self.state = OrderState.PENDING
        self.ce_order_id: Optional[str] = None
        self.pe_order_id: Optional[str] = None
        self.ce_fill_price: Optional[float] = None
        self.pe_fill_price: Optional[float] = None
        self.failure_reason: Optional[str] = None

    def place_entry(
        self,
        ce_symbol: str,
        pe_symbol: str,
        quantity: int,
        trading_mode: str = "paper",
    ) -> dict:
        """
        Place CE and PE legs atomically.
        Returns dict with success, state, ce/pe fill prices.
        """
        # ── Place CE leg ────────────────────────────────────────────────────
        logger.info(f"[OSM] Placing CE leg: {ce_symbol} qty={quantity}")
        ce_result = self._place_with_retry("CE", ce_symbol, quantity, trading_mode)

        if not ce_result["success"]:
            self.state = OrderState.FAILED
            self.failure_reason = f"CE placement failed: {ce_result['error']}"
            self._log_failure("CE", ce_result["error"], "CE_PLACE_FAILED", 0, paired_closed=False)
            return self._result()

        self.state = OrderState.CE_FILLED
        self.ce_order_id = ce_result["order_id"]
        self.ce_fill_price = ce_result["fill_price"]

        # ── Place PE leg ────────────────────────────────────────────────────
        logger.info(f"[OSM] Placing PE leg: {pe_symbol} qty={quantity}")
        pe_result = self._place_with_retry("PE", pe_symbol, quantity, trading_mode)

        if not pe_result["success"]:
            logger.warning(f"[OSM] PE leg failed after retries — rolling back CE")
            self.state = OrderState.ROLLED_BACK
            self.failure_reason = f"PE placement failed: {pe_result['error']} — CE rolled back"
            self._rollback_ce(ce_symbol, quantity, trading_mode)
            self._log_failure("PE", pe_result["error"], "PE_PLACE_FAILED_CE_ROLLED_BACK",
                              pe_result.get("retry_count", 0), paired_closed=True)
            return self._result()

        self.state = OrderState.OPEN
        self.pe_order_id = pe_result["order_id"]
        self.pe_fill_price = pe_result["fill_price"]

        logger.info(
            f"[OSM] Both legs filled — CE={self.ce_fill_price} PE={self.pe_fill_price} "
            f"total_premium={self.total_premium:.2f}"
        )
        return self._result()

    def _place_with_retry(self, leg: str, symbol: str, quantity: int, trading_mode: str) -> dict:
        last_error = ""
        for attempt in range(len(RETRY_DELAYS) + 1):
            if attempt > 0:
                delay = RETRY_DELAYS[attempt - 1]
                logger.info(f"[OSM] Retrying {leg} leg (attempt {attempt+1}) in {delay}s...")
                time.sleep(delay)

            try:
                if trading_mode == "paper":
                    result = self._paper_order(leg, symbol, quantity)
                else:
                    result = self.broker.place_order(
                        tradingsymbol=symbol,
                        transaction_type="BUY",
                        quantity=quantity,
                        order_type="MARKET",
                        product="MIS",
                    )

                if result and result.get("order_id"):
                    fill_price = result.get("fill_price") or result.get("average_price", 0.0)
                    return {"success": True, "order_id": result["order_id"],
                            "fill_price": float(fill_price), "retry_count": attempt}

                last_error = str(result)
            except Exception as e:
                last_error = str(e)
                logger.warning(f"[OSM] {leg} attempt {attempt+1} exception: {e}")

        return {"success": False, "error": last_error, "retry_count": len(RETRY_DELAYS)}

    def _rollback_ce(self, ce_symbol: str, quantity: int, trading_mode: str):
        """Exit the filled CE leg to prevent naked position."""
        try:
            if trading_mode == "paper":
                logger.info(f"[OSM] PAPER rollback CE {ce_symbol}")
                return
            self.broker.place_order(
                tradingsymbol=ce_symbol,
                transaction_type="SELL",
                quantity=quantity,
                order_type="MARKET",
                product="MIS",
            )
            logger.info(f"[OSM] CE rollback successful for {ce_symbol}")
        except Exception as e:
            logger.error(f"[OSM] CE rollback FAILED for {ce_symbol}: {e}")

    def _paper_order(self, leg: str, symbol: str, quantity: int) -> dict:
        import random
        simulated_price = round(random.uniform(40, 200), 2)
        return {"order_id": f"PAPER-{leg}-{int(time.time())}", "fill_price": simulated_price, "average_price": simulated_price}

    def _log_failure(self, side: str, error: str, reason: str, retry_count: int, paired_closed: bool):
        if not self.db_logger:
            return
        try:
            self.db_logger(
                instance_id=self.instance_id,
                client_id=self.client_id,
                order_side=side,
                broker_error=error,
                failure_reason=reason,
                retry_attempt=retry_count,
                paired_leg_closed=paired_closed,
            )
        except Exception as e:
            logger.error(f"[OSM] Failed to log order failure: {e}")

    @property
    def total_premium(self) -> float:
        return (self.ce_fill_price or 0.0) + (self.pe_fill_price or 0.0)

    def _result(self) -> dict:
        return {
            "success": self.state == OrderState.OPEN,
            "state": self.state,
            "ce_order_id": self.ce_order_id,
            "pe_order_id": self.pe_order_id,
            "ce_fill_price": self.ce_fill_price,
            "pe_fill_price": self.pe_fill_price,
            "total_premium": self.total_premium,
            "failure_reason": self.failure_reason,
        }
