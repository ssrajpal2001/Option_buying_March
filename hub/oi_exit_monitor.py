import time
from utils.logger import logger


class OIExitMonitor:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.prev_call_oi = None
        self.prev_put_oi = None
        self.last_check_ts = 0

    def _cfg(self, key, type_func=float, fallback=None):
        try:
            val = self.orchestrator.json_config.get_value(
                f"{self.orchestrator.instrument_name}.oi_exit.{key}"
            )
            return type_func(val) if val is not None else fallback
        except Exception:
            return fallback

    def _get_oi_snapshot(self, atm, interval, strikes_range):
        state = self.orchestrator.state_manager
        atm_mgr = self.orchestrator.atm_manager
        expiry = atm_mgr.signal_expiry_date

        total_call_oi = 0
        total_put_oi = 0
        atm = int(round(atm / interval) * interval)

        for n in range(-strikes_range, strikes_range + 1):
            strike = atm + n * interval
            ck = atm_mgr.find_instrument_key_by_strike(strike, 'CALL', expiry)
            pk = atm_mgr.find_instrument_key_by_strike(strike, 'PUT', expiry)
            if ck:
                total_call_oi += state.option_oi.get(ck, 0)
            if pk:
                total_put_oi += state.option_oi.get(pk, 0)

        return total_call_oi, total_put_oi

    async def check(self, timestamp, current_atm):
        try:
            enabled = self._cfg('enabled', lambda x: str(x).lower() == 'true', False)
            strikes_range = self._cfg('strikes_range', int, 2)
            call_oi_increase_pct = self._cfg('call_oi_increase_pct', float, 10.0)
            put_oi_increase_pct = self._cfg('put_oi_increase_pct', float, 10.0)
            check_interval = self._cfg('check_interval_seconds', float, 60.0)

            now = time.monotonic()
            if (now - self.last_check_ts) < check_interval:
                return
            self.last_check_ts = now

            interval = self.orchestrator.config_manager.get_int(
                self.orchestrator.instrument_name, 'strike_interval', 50
            )

            call_oi, put_oi = self._get_oi_snapshot(current_atm, interval, strikes_range)

            if call_oi == 0 and put_oi == 0:
                return

            logger.debug(
                f"[OI_EXIT] Snapshot — CALL OI: {call_oi:,.0f}  PUT OI: {put_oi:,.0f}"
            )

            if self.prev_call_oi is None:
                self.prev_call_oi = call_oi
                self.prev_put_oi = put_oi
                return

            if enabled:
                if self.prev_call_oi > 0:
                    call_chg = (call_oi - self.prev_call_oi) / self.prev_call_oi * 100
                    if call_chg >= call_oi_increase_pct:
                        logger.info(
                            f"[OI_EXIT] CALL OI rose {call_chg:.1f}% "
                            f"({self.prev_call_oi:,.0f} → {call_oi:,.0f}) — "
                            f"exiting sell-PE and buy-CE"
                        )
                        await self._trigger_oi_exit('CALL_OI_RISE', timestamp)

                if self.prev_put_oi > 0:
                    put_chg = (put_oi - self.prev_put_oi) / self.prev_put_oi * 100
                    if put_chg >= put_oi_increase_pct:
                        logger.info(
                            f"[OI_EXIT] PUT OI rose {put_chg:.1f}% "
                            f"({self.prev_put_oi:,.0f} → {put_oi:,.0f}) — "
                            f"exiting sell-CE and buy-PE"
                        )
                        await self._trigger_oi_exit('PUT_OI_RISE', timestamp)

            self.prev_call_oi = call_oi
            self.prev_put_oi = put_oi

        except Exception as e:
            logger.error(f"[OI_EXIT] Error in check: {e}", exc_info=True)

    async def _trigger_oi_exit(self, signal, timestamp):
        reason = f"OI Exit: {signal}"

        sm = getattr(self.orchestrator, 'sell_manager', None)
        if sm:
            try:
                if signal == 'CALL_OI_RISE' and sm.pe_placed and not sm.strangle_closed:
                    logger.info(f"[OI_EXIT] Exiting sell-PE leg. Reason: {reason}")
                    await sm.exit_side('PE', timestamp, reason)
                if signal == 'PUT_OI_RISE' and sm.ce_placed and not sm.strangle_closed:
                    logger.info(f"[OI_EXIT] Exiting sell-CE leg. Reason: {reason}")
                    await sm.exit_side('CE', timestamp, reason)
            except Exception as e:
                logger.error(f"[OI_EXIT] Error exiting sell side: {e}", exc_info=True)

        for session in self.orchestrator.user_sessions.values():
            try:
                pm = getattr(session, 'position_manager', None)
                usr_sm = getattr(session, 'state_manager', None)
                if not pm or not usr_sm:
                    continue

                if signal == 'CALL_OI_RISE' and usr_sm.is_in_trade('CALL'):
                    pos = usr_sm.call_position
                    inst_key = pos.get('instrument_key')
                    ltp = (
                        usr_sm.option_prices.get(inst_key)
                        or pos.get('ltp')
                        or pos.get('entry_price', 0)
                    )
                    logger.info(f"[OI_EXIT] Exiting buy-CE for session {session}. Reason: {reason}")
                    await pm._exit_trade('CALL', ltp, timestamp, reason)

                if signal == 'PUT_OI_RISE' and usr_sm.is_in_trade('PUT'):
                    pos = usr_sm.put_position
                    inst_key = pos.get('instrument_key')
                    ltp = (
                        usr_sm.option_prices.get(inst_key)
                        or pos.get('ltp')
                        or pos.get('entry_price', 0)
                    )
                    logger.info(f"[OI_EXIT] Exiting buy-PE for session {session}. Reason: {reason}")
                    await pm._exit_trade('PUT', ltp, timestamp, reason)

            except Exception as e:
                logger.error(f"[OI_EXIT] Error exiting buy side for session: {e}", exc_info=True)
