import asyncio
import datetime
import pytz
from utils.logger import logger
from utils.profiler import profile_microseconds

class TickProcessor:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.state = orchestrator.orchestrator_state
        self.config_manager = orchestrator.config_manager
        self.atm_manager = orchestrator.atm_manager
        self.state_manager = orchestrator.state_manager
        self.strike_manager = orchestrator.strike_manager
        self.last_target_strike_check_time = 0
        self.last_crossover_check_time = 0
        self.last_mgmt_check_time = 0
        self.last_heartbeat_time = 0

    def get_tick_data(self, ce_strike, pe_strike, is_backtest, backtest_current_tick=None):
        """
        Constructs a dictionary with all relevant data for the current tick for signal calculation.
        This method is now the single source of truth for assembling tick data.
        """
        # spot_price = Futures (for Target Selection)
        # index_price = Spot (for ITM Selection)
        tick_data = {
            'spot_price': self.state_manager.spot_price,
            'index_price': self.state_manager.index_price
        }

        # Fallback to ATM if target strikes are missing
        if not ce_strike or not pe_strike:
            atm_strike = self.atm_manager.strikes.get('atm')
            if not atm_strike:
                return {'spot_price': self.state_manager.spot_price}
            logger.debug(f"Target strike not found, falling back to ATM {atm_strike} for tick data.")
            ce_strike = pe_strike = atm_strike

        if is_backtest:
            if backtest_current_tick:
                ce_data = backtest_current_tick.get(ce_strike, {})
                pe_data = backtest_current_tick.get(pe_strike, {})
                tick_data.update({
                    'ce_ltp': ce_data.get('ce_ltp'),
                    'ce_delta': ce_data.get('ce_delta'),
                    'pe_ltp': pe_data.get('pe_ltp'),
                    'pe_delta': pe_data.get('pe_delta'),
                })
        else: # Live Mode
            ce_contract, pe_contract = self.atm_manager.find_contracts_for_strike(ce_strike, self.atm_manager.signal_expiry_date)

            ce_key = ce_contract.instrument_key if ce_contract else None
            pe_key = pe_contract.instrument_key if pe_contract else None

            if not ce_key or not pe_key:
                logger.debug(f"V2: [{self.orchestrator.instrument_name}] Strike {ce_strike} - Missing Contract Keys: CE={ce_key}, PE={pe_key}")
                pass

            ce_ltp = self.state_manager.option_prices.get(ce_key)
            pe_ltp = self.state_manager.option_prices.get(pe_key)

            # if ce_ltp is None or pe_ltp is None:
            #     logger.debug(f"V2: [{self.orchestrator.instrument_name}] Strike {ce_strike} - Missing LTPs: CE={ce_ltp} (Key: {ce_key}), PE={pe_ltp} (Key: {pe_key})")

            tick_data.update({
                'ce_ltp': ce_ltp,
                'ce_delta': self.state_manager.option_deltas.get(ce_key),
                'ce_symbol': ce_key,
                'pe_ltp': self.state_manager.option_prices.get(pe_key),
                'pe_delta': self.state_manager.option_deltas.get(pe_key),
                'pe_symbol': pe_key,
            })

        return tick_data

    @profile_microseconds
    async def process_tick(self, backtest_previous_tick=None, backtest_current_tick=None):
        """
        The main processing entry point. Exclusively uses V2 logic.
        """
        if not self.orchestrator.is_active and not self.orchestrator.is_backtest:
            return

        await self.process_tick_v2(backtest_previous_tick, backtest_current_tick)


    async def process_tick_v2(self, backtest_previous_tick=None, backtest_current_tick=None):
        now_ts = asyncio.get_event_loop().time()
        timestamp = self.orchestrator._get_timestamp()

        # HEARTBEAT (every 5 minutes)
        if not self.orchestrator.is_backtest:
            if now_ts - self.last_heartbeat_time >= 300.0:
                self.last_heartbeat_time = now_ts
                session_info = []
                for uid, session in self.orchestrator.user_sessions.items():
                    status = "In Trade" if session.is_in_trade() else ("Monitoring" if session.signal_monitor.is_monitoring() else "Idle")
                    session_info.append(f"{uid or 'Global'}:{status}")

                # Diagnostic: Time since last exchange update
                lag_str = "N/A"
                if self.state_manager.last_exchange_time:
                    let = self.state_manager.last_exchange_time
                    if let.tzinfo is None: let = pytz.timezone('Asia/Kolkata').localize(let)
                    lag = (datetime.datetime.now(pytz.timezone('Asia/Kolkata')) - let).total_seconds()
                    lag_str = f"{lag:.1f}s"

                # Health Check: Ensure tick worker task is still alive
                worker_status = "ALIVE"
                if hasattr(self.orchestrator, 'price_feed_handler'):
                    worker = self.orchestrator.price_feed_handler._worker_task
                    if worker.done():
                        worker_status = "DEAD"
                        try:
                            # Trigger exception if any occurred in the task
                            if worker.exception():
                                worker_status = f"CRASHED({worker.exception()})"
                        except: pass

                logger.info(f"V2 HEARTBEAT: [{self.orchestrator.instrument_name}] Loop active. Feed Lag: {lag_str} | Worker: {worker_status} | Exchange: {self.state_manager.last_exchange_time} | Sessions: {', '.join(session_info)}")

        # The target strike calculation is now performed on every tick (throttled to 1s).

        # IN V2: spot_price (Futures) is used for Target Selection
        #        index_price (Spot) is used for ITM trade execution
        futures_price = None
        index_price = None

        if backtest_current_tick:
            first_strike = next(iter(backtest_current_tick))
            futures_price = backtest_current_tick[first_strike].get('spot_price')
            index_price = backtest_current_tick[first_strike].get('index_price')

        if not futures_price:
            futures_price = self.state_manager.spot_price
        if not index_price:
            index_price = self.state_manager.index_price

        if not futures_price:
            logger.debug("V2: Futures price not available. Skipping tick.")
            return

        strike_interval = self.config_manager.get_int(self.atm_manager.instrument_name, 'strike_interval')
        # Ensure ATM is a float to match the strike price data type from contracts
        current_atm = float(round(futures_price / strike_interval) * strike_interval)
        self.state_manager.atm_strike = current_atm

        if not current_atm:
            logger.debug("V2: ATM could not be calculated. Skipping tick.")
            return

        current_ticks_for_watchlist = {}
        watchlist_strikes = self.strike_manager.get_strike_watchlist(current_atm)
        for strike in watchlist_strikes:
            tick_data = self.orchestrator.get_current_tick_data(strike, strike, self.orchestrator.is_backtest, backtest_current_tick)
            if tick_data:
                current_ticks_for_watchlist[strike] = tick_data

        # DATA RECORDING (ATM +/- 10)
        if not self.orchestrator.is_backtest and self.orchestrator.data_recorder:
            if now_ts - getattr(self, '_last_record_time', 0) >= 1.0:
                self._last_record_time = now_ts
                recording_strikes = self.strike_manager.get_recording_watchlist(current_atm)
                recording_data = {}
                for strike in recording_strikes:
                    # Reuse from watchlist if possible
                    if strike in current_ticks_for_watchlist:
                        recording_data[strike] = current_ticks_for_watchlist[strike]
                    else:
                        t_data = self.orchestrator.get_current_tick_data(strike, strike, False)
                        if t_data: recording_data[strike] = t_data

                if recording_data:
                    self.orchestrator.data_recorder.record_ticks(timestamp, futures_price, index_price, current_atm, recording_data)

        # 1. THROTTLE Target Strike Check to 60 seconds (1 minute)
        # Strategy logic: We only need to re-evaluate the target strike every minute once one is found.
        # If no target strike is active, we check every 2 seconds to ensure fast startup/recovery.
        target_throttle = 60.0 if self.state.v2_target_strike_pair else 2.0
        if self.orchestrator.is_backtest or (now_ts - self.last_target_strike_check_time >= target_throttle):
            self.last_target_strike_check_time = now_ts

            # Use StrikeManager to find the best strike pair (using default signal expiry)
            self.state.v2_target_strike_pair = self.orchestrator.strike_manager.find_and_get_target_strike_pair(
                expiry=self.atm_manager.signal_expiry_date
            )

            v2_signal_found = self.state.v2_target_strike_pair is not None
            if v2_signal_found:
                target_strike = float(self.state.v2_target_strike_pair['strike'])
                self.state_manager.target_strike = target_strike
                # Sync target strike to all active user sessions for display
                for session in self.orchestrator.user_sessions.values():
                    session.state_manager.target_strike = target_strike
            elif self.state_manager.target_strike is not None:
                # Fallback: if signal lost temporarily, keep last target for display stability
                for session in self.orchestrator.user_sessions.values():
                    session.state_manager.target_strike = self.state_manager.target_strike
            else:
                # If no pair found this tick, DO NOT clear immediately if we were already monitoring.
                # This prevents "TARGET: N/A" flickering if one tick is missing premiums.
                pass

            # Monitoring management is now handled inside each UserSession's SignalMonitor
            # during the breach check loop below.

        # 2. Crossover Breach Check (Per User)
        any_monitoring = False
        for user_id, session in self.orchestrator.user_sessions.items():
            if self.orchestrator.is_backtest or (now_ts - self.last_crossover_check_time >= 1.0):
                await session.signal_monitor.check_crossover_breach(
                    timestamp=timestamp,
                    current_atm=current_atm
                )

            if session.signal_monitor.is_monitoring():
                any_monitoring = True

        if self.orchestrator.is_backtest or (now_ts - self.last_crossover_check_time >= 1.0):
             self.last_crossover_check_time = now_ts

        if not any_monitoring:
            # Periodic status log while scanning for a target strike
            if now_ts - self.last_crossover_check_time >= 60.0:
                logger.debug(f"V2: Scanning watchlist around ATM {current_atm} for target strike...")

        # Condition 3: Manage any active trades for ALL isolated users.
        # Throttled to avoid excessive processing on every tick.
        if self.orchestrator.is_backtest or (now_ts - self.last_mgmt_check_time >= 1.0):
            self.last_mgmt_check_time = now_ts
            for user_id, session in self.orchestrator.user_sessions.items():
                if session.is_in_trade():
                    await session.manage_active_trades(
                        timestamp=timestamp,
                        current_ticks=current_ticks_for_watchlist,
                        current_atm=current_atm
                    )

        for strike, tick_data in current_ticks_for_watchlist.items():
            # In backtest, we need to manually push ticks to aggregators
            if self.orchestrator.is_backtest:
                ce_ltp = tick_data.get('ce_ltp')
                pe_ltp = tick_data.get('pe_ltp')
                # Find instrument keys for these strikes
                signal_expiry = self.atm_manager.signal_expiry_date
                ce_key = self.atm_manager.find_instrument_key_by_strike(strike, 'CALL', signal_expiry)
                pe_key = self.atm_manager.find_instrument_key_by_strike(strike, 'PUT', signal_expiry)

                if ce_key and ce_ltp:
                    self.orchestrator.entry_aggregator.add_tick(ce_key, ce_ltp, timestamp)
                    self.orchestrator.exit_aggregator.add_tick(ce_key, ce_ltp, timestamp)
                    self.orchestrator.one_min_aggregator.add_tick(ce_key, ce_ltp, timestamp)
                    self.orchestrator.five_min_aggregator.add_tick(ce_key, ce_ltp, timestamp)
                if pe_key and pe_ltp:
                    self.orchestrator.entry_aggregator.add_tick(pe_key, pe_ltp, timestamp)
                    self.orchestrator.exit_aggregator.add_tick(pe_key, pe_ltp, timestamp)
                    self.orchestrator.one_min_aggregator.add_tick(pe_key, pe_ltp, timestamp)
                    self.orchestrator.five_min_aggregator.add_tick(pe_key, pe_ltp, timestamp)

            self.state.tick_history[strike].append(tick_data)

