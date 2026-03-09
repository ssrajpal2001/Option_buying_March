import json
import os
import datetime as dt
import pytz
import asyncio
from datetime import datetime, time, timedelta
from utils.logger import logger
from .event_bus import event_bus

class SellManagerV3:
    """
    V3 Sell Strategy:
    - Entry: 9:16:05 AM, ATM based on Spot.
    - Strike selection: First ITM/ATM >= 50 LTP.
    - Lowest LTP leg sold first, then matching leg for other side.
    - Combined RSI (5m) and Combined VWAP (ATP) exit.
    - Target Profit (12%), LTP < 20, and Dynamic TSL exits.
    - Auto-restart before 2:00 PM.
    """

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.instrument_name = orchestrator.instrument_name
        self.state_file = f'config/sell_v3_state_{self.instrument_name}.json'

        # Trade State
        self.active = False
        self.ce_leg = None  # {strike, key, entry_ltp, entry_atp}
        self.pe_leg = None
        self.total_premium_points = 0.0
        self.entry_timestamp = None
        self.last_config_check_time = 0
        self.last_v3_log_time = 0
        self.last_indicator_check_minute = -1

        # TSL State
        self.tsl_wait_high_hit = False

        self.load_state()

    def _cfg(self, path, fallback=None):
        return self.orchestrator.json_config.get_value(f"{self.instrument_name}.sell_v3.{path}", fallback)

    def save_state(self):
        state = {
            'active': self.active,
            'ce_leg': self.ce_leg,
            'pe_leg': self.pe_leg,
            'total_premium_points': self.total_premium_points,
            'entry_timestamp': self.entry_timestamp.isoformat() if self.entry_timestamp else None,
            'tsl_wait_high_hit': self.tsl_wait_high_hit
        }
        try:
            os.makedirs('config', exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
        except Exception as e:
            logger.error(f"[SellV3] Failed to save state: {e}")

    def load_state(self):
        if not os.path.exists(self.state_file):
            return
        try:
            with open(self.state_file, 'r') as f:
                state = json.load(f)
            self.active = state.get('active', False)
            self.ce_leg = state.get('ce_leg')
            self.pe_leg = state.get('pe_leg')
            self.total_premium_points = state.get('total_premium_points', 0.0)
            ts = state.get('entry_timestamp')
            self.entry_timestamp = datetime.fromisoformat(ts).replace(tzinfo=pytz.timezone('Asia/Kolkata')) if ts else None
            self.tsl_wait_high_hit = state.get('tsl_wait_high_hit', False)

            if self.active:
                logger.info(f"[SellV3] Recovered active trade from {self.entry_timestamp}")
        except Exception as e:
            logger.error(f"[SellV3] Failed to load state: {e}")

    async def on_tick(self, timestamp):
        if not self._cfg('enabled', False):
            return

        if getattr(self.orchestrator, 'profit_target_hit', False):
            if self.active:
                await self._exit_all(timestamp, "Daily Profit Target Hit")
            return

        # End of day close
        end_time_str = self.orchestrator.config_manager.get('settings', 'end_time', fallback='15:25:00')
        end_time = datetime.strptime(end_time_str, '%H:%M:%S').time()
        if timestamp.time() >= end_time:
            if self.active:
                await self._exit_all(timestamp, "EOD Close")
            return

        # Check for config updates
        await self._check_config_updates(timestamp)

        if not self.active:
            await self._check_entry(timestamp)

        # We allow immediate management on the same tick if entry just happened
        if self.active:
            await self._check_exit(timestamp)

    async def _check_config_updates(self, timestamp):
        # Check for changes in the JSON file every 5 seconds
        last_dt = datetime.fromtimestamp(self.last_config_check_time, tz=pytz.timezone('Asia/Kolkata'))
        if (timestamp - last_dt).total_seconds() < 5:
            return

        mtime = os.path.getmtime(self.orchestrator.json_config.json_path)
        if mtime > self.last_config_check_time:
            # Detect changes in values we care about
            old_pct = self._cfg('profit_target_pct')
            old_tsl_val = self._cfg('tsl.value')
            old_tsl_en = self._cfg('tsl.enabled')

            self.orchestrator.json_config.load()

            new_pct = self._cfg('profit_target_pct')
            new_tsl_val = self._cfg('tsl.value')
            new_tsl_en = self._cfg('tsl.enabled')

            if old_pct != new_pct:
                logger.info(f"[SellV3] Strategy Update: Profit Target changed from {old_pct}% to {new_pct}%")
            if old_tsl_val != new_tsl_val:
                logger.info(f"[SellV3] Strategy Update: TSL Value changed from {old_tsl_val} to {new_tsl_val}")
                # Reset TSL status to allow re-locking at the new value
                self.tsl_wait_high_hit = False

            if old_tsl_en != new_tsl_en:
                logger.info(f"[SellV3] Strategy Update: TSL Enabled changed from {old_tsl_en} to {new_tsl_en}")
                # Reset TSL status when toggling
                self.tsl_wait_high_hit = False

            self.last_config_check_time = mtime

    async def _check_entry(self, timestamp):
        if timestamp.time() < time(9, 16, 5):
            return
        if timestamp.time() >= time(14, 0, 0):
            return

        # 1. ATM based on Spot
        index_price = self.orchestrator.state_manager.index_price
        interval = self.orchestrator.config_manager.get_int(self.instrument_name, 'strike_interval', 50)
        if not index_price:
            if (timestamp.second % 10) == 0:
                logger.debug(f"[SellV3] Waiting for Index Spot price to begin entry...")
            return

        atm = int(round(index_price / interval) * interval)

        # Log entry attempt at most every 5 seconds to reduce noise
        now_ts = (timestamp.hour * 3600) + (timestamp.minute * 60) + timestamp.second
        if now_ts - getattr(self, '_last_entry_attempt_log', 0) >= 5:
            self._last_entry_attempt_log = now_ts
            logger.info(f"[SellV3] Attempting entry at {timestamp} (Spot: {index_price}, ATM: {atm})")

        # 2. Strike Selection
        expiry = self.orchestrator.atm_manager.signal_expiry_date
        ltp_threshold = self._cfg('ltp_threshold', 50.0)

        ce_strike, ce_key, ce_ltp = await self._find_strike_ge(atm, 'CALL', expiry, ltp_threshold, direction='ITM')
        pe_strike, pe_key, pe_ltp = await self._find_strike_ge(atm, 'PUT', expiry, ltp_threshold, direction='ITM')

        if not ce_key or not pe_key:
            logger.warning(f"[SellV3] Could not find suitable strikes for entry.")
            return

        # 4. Execution Order (Lowest LTP first)
        success = False
        if ce_ltp < pe_ltp:
            # Sell CE first
            logger.info(f"[SellV3] Side Selection: CE({ce_ltp:.2f}) < PE({pe_ltp:.2f}). Selling CE first.")
            if await self._execute_sell('CE', ce_strike, ce_key, ce_ltp, timestamp):
                # Match PE
                pe_strike, pe_key, pe_ltp = await self._find_matching_strike(ce_ltp, 'PUT', expiry)
                if pe_key:
                    logger.info(f"[SellV3] Matching PE: Found {pe_strike} at {pe_ltp:.2f} (Target >= {ce_ltp:.2f})")
                    if await self._execute_sell('PE', pe_strike, pe_key, pe_ltp, timestamp):
                        success = True
        else:
            # Sell PE first
            logger.info(f"[SellV3] Side Selection: PE({pe_ltp:.2f}) <= CE({ce_ltp:.2f}). Selling PE first.")
            if await self._execute_sell('PE', pe_strike, pe_key, pe_ltp, timestamp):
                # Match CE
                ce_strike, ce_key, ce_ltp = await self._find_matching_strike(pe_ltp, 'CALL', expiry)
                if ce_key:
                    logger.info(f"[SellV3] Matching CE: Found {ce_strike} at {ce_ltp:.2f} (Target >= {pe_ltp:.2f})")
                    if await self._execute_sell('CE', ce_strike, ce_key, ce_ltp, timestamp):
                        success = True

        if not success:
            logger.error("[SellV3] Entry failed: One or more legs could not be executed.")
            # Partial cleanup if one leg succeeded but the other failed
            return

        self.active = True
        self.entry_timestamp = timestamp
        self.total_premium_points = self.ce_leg['entry_ltp'] + self.pe_leg['entry_ltp']
        self.tsl_wait_high_hit = False
        # Reset indicator timer to allow immediate exit check if entry happens on boundary
        self.last_indicator_check_minute = -1
        self.save_state()
        logger.info(f"[SellV3] Strangle Entered. Total Premium: {self.total_premium_points:.2f}")

    async def _find_strike_ge(self, start_strike, side, expiry, threshold, direction='ITM'):
        interval = self.orchestrator.config_manager.get_int(self.instrument_name, 'strike_interval', 50)
        curr_strike = start_strike
        for i in range(10): # Max 10 strikes away
            key = self.orchestrator.atm_manager.find_instrument_key_by_strike(curr_strike, side, expiry)
            if not key:
                logger.debug(f"[SellV3] {side} {curr_strike}: Key not found in lookup.")
                break

            ltp = await self._get_ltp(key)
            if ltp is None:
                # User fix: DO NOT break. Continue searching other ITM strikes.
                if (datetime.now().second % 10) == 0:
                    logger.debug(f"[SellV3] {side} {curr_strike}: LTP is None. Ensuring feed started and continuing search ITM...")
                # We should trigger a feed subscription just in case
                await event_bus.publish('ADD_TO_WATCHLIST', {'instrument_key': key})

                # Move ITM and continue
                if side == 'CALL': curr_strike -= interval
                else: curr_strike += interval
                continue

            if ltp >= threshold:
                logger.info(f"[SellV3] Found {side} strike {curr_strike} with LTP {ltp:.2f} (Threshold: {threshold})")
                return curr_strike, key, ltp

            logger.debug(f"[SellV3] {side} {curr_strike}: LTP {ltp:.2f} < {threshold}. Moving ITM.")

            # Move ITM
            if side == 'CALL':
                curr_strike -= interval
            else:
                curr_strike += interval

        logger.warning(f"[SellV3] No {side} strike found >= {threshold} within 10 ITM strikes of ATM {start_strike}. (Checked up to {curr_strike})")
        return None, None, None

    async def _find_matching_strike(self, target_ltp, side, expiry):
        """Find strike >= target_ltp and closest to it."""
        interval = self.orchestrator.config_manager.get_int(self.instrument_name, 'strike_interval', 50)
        index_price = self.orchestrator.state_manager.index_price
        atm = int(round(index_price / interval) * interval)

        best_strike = None
        best_key = None
        best_ltp = None
        min_diff = float('inf')

        logger.debug(f"[SellV3] Finding matching {side} strike for target LTP: {target_ltp:.2f}")

        # Check a range of strikes (ATM +/- 15 to be safer)
        for i in range(-15, 16):
            strike = atm + (i * interval)
            key = self.orchestrator.atm_manager.find_instrument_key_by_strike(strike, side, expiry)
            if not key: continue
            ltp = await self._get_ltp(key)
            if ltp:
                logger.debug(f"[SellV3] Candidate {side} {strike}: LTP {ltp:.2f}")
                if ltp >= target_ltp:
                    diff = ltp - target_ltp
                    if diff < min_diff:
                        min_diff = diff
                        best_strike = strike
                        best_key = key
                        best_ltp = ltp

        if best_strike:
            logger.info(f"[SellV3] Selected matching {side} {best_strike} at {best_ltp:.2f} (diff: {min_diff:.2f})")
        else:
            logger.warning(f"[SellV3] No matching {side} strike found >= {target_ltp:.2f}")

        return best_strike, best_key, best_ltp

    async def _get_ltp(self, key):
        if self.orchestrator.is_backtest:
            return await self.orchestrator._get_ltp_for_backtest_instrument(key, self.orchestrator._get_timestamp())
        return self.orchestrator.state_manager.get_ltp(key)

    async def _execute_sell(self, side, strike, key, ltp, timestamp):
        expiry = self.orchestrator.atm_manager.signal_expiry_date
        lookup = self.orchestrator.atm_manager.contract_lookup.get(expiry, {})
        contract = lookup.get(float(strike), {}).get(side)

        if not contract:
            logger.error(f"[SellV3] Contract not found for {side} {strike}")
            return False

        atp = self.orchestrator.state_manager.option_atps.get(key) or ltp

        leg = {
            'strike': strike,
            'key': key,
            'entry_ltp': ltp,
            'entry_atp': atp,
            'side': side
        }

        if side == 'CE': self.ce_leg = leg
        else: self.pe_leg = leg

        # Ensure live data feed starts for this contract immediately
        await event_bus.publish('ADD_TO_WATCHLIST', {'instrument_key': key})

        # Place Order
        product_type = self._cfg('product_type', 'NRML')
        order_success = True
        for broker in self.orchestrator.broker_manager.brokers:
            if not broker.is_configured_for_instrument(self.instrument_name): continue
            qty = broker.config_manager.get_int(broker.instance_name, 'quantity', 1) * contract.lot_size
            if not self.orchestrator.is_backtest and not getattr(broker, 'paper_trade', False):
                try:
                    broker.place_order(contract, 'SELL', qty, expiry, product_type=product_type)
                except Exception as e:
                    logger.error(f"[SellV3] Order failed on broker {broker.instance_name}: {e}")
                    order_success = False

            # Log for backtest PnL tracking
            if self.orchestrator.is_backtest and self.orchestrator.pnl_tracker:
                self.orchestrator.pnl_tracker.enter_trade(
                    side=side, instrument_key=key, entry_price=ltp, timestamp=timestamp,
                    strike_price=strike, contract=contract, strategy_log=f"SellV3 {side}", entry_type='SELL', quantity=qty // contract.lot_size
                )

        return order_success

    async def _check_exit(self, timestamp):
        # 1. LTP < 20 Exit (Tick by Tick)
        ce_ltp = await self._get_ltp(self.ce_leg['key'])
        pe_ltp = await self._get_ltp(self.pe_leg['key'])

        if not ce_ltp or not pe_ltp: return

        ltp_exit_min = self._cfg('ltp_exit_min', 20.0)
        if ce_ltp < ltp_exit_min or pe_ltp < ltp_exit_min:
            await self._exit_all(timestamp, f"LTP below {ltp_exit_min}")
            return

        # 2. Target Profit 12% Exit (Tick by Tick)
        current_premium = ce_ltp + pe_ltp
        profit_points = self.total_premium_points - current_premium
        target_pct = self._cfg('profit_target_pct', 12.0)
        target_points = (target_pct / 100.0) * self.total_premium_points

        if profit_points >= target_points:
            await self._exit_all(timestamp, f"Target Profit {target_pct}% hit")
            return

        # 3. Dynamic TSL (Tick by Tick)
        if self._cfg('tsl.enabled', False):
            tsl_value_rupees = self._cfg('tsl.value', 0.0)
            # Calculate current PnL in Rupees
            total_pnl_rupees = 0
            for broker in self.orchestrator.broker_manager.brokers:
                if not broker.is_configured_for_instrument(self.instrument_name): continue
                qty = broker.config_manager.get_int(broker.instance_name, 'quantity', 1)
                lot_size = self.orchestrator.config_manager.get_int(self.instrument_name, 'lot_size', 50)
                total_pnl_rupees += profit_points * qty * lot_size

            if not self.tsl_wait_high_hit and total_pnl_rupees > tsl_value_rupees:
                self.tsl_wait_high_hit = True
                logger.info(f"[SellV3] TSL High Hit! Now locking at {tsl_value_rupees} Rupees.")

            if self.tsl_wait_high_hit and total_pnl_rupees <= tsl_value_rupees:
                await self._exit_all(timestamp, f"TSL Locked Profit hit at {tsl_value_rupees} Rupees")
                return

        # 4. Indicators Exit (5-min Candle Close)
        # User fix: Trigger 10 seconds into the new 5-minute interval.
        # This allows the API time to finalize and provide data for the closed candle.
        if timestamp.minute % 5 == 0 and timestamp.second >= 10 and timestamp.minute != self.last_indicator_check_minute:
            # This logic fires slightly after the boundary (e.g. 09:20:10, 09:25:10)
            self.last_indicator_check_minute = timestamp.minute

            # Note: 14-candle wait is handled by IndicatorManager history buffer.
            await self._check_indicator_exit(timestamp)

    async def _check_indicator_exit(self, timestamp):
        # Combined RSI > 50
        rsi_cfg = self._cfg('rsi', {})
        rsi_val = await self.orchestrator.indicator_manager.calculate_combined_rsi(
            self.ce_leg['key'], self.pe_leg['key'],
            rsi_cfg.get('tf', 5), rsi_cfg.get('period', 14), timestamp
        )

        # For VWAP we use the current cumulative VWAP (ATP) at the 5-minute boundary.
        # We calculate Combined VWAP = CE ATP + PE ATP.
        ce_atp = await self.orchestrator.indicator_manager.calculate_vwap(self.ce_leg['key'], timestamp)
        pe_atp = await self.orchestrator.indicator_manager.calculate_vwap(self.pe_leg['key'], timestamp)

        combined_ltp = None
        if self.ce_leg and self.pe_leg:
            c_ltp = await self._get_ltp(self.ce_leg['key'])
            p_ltp = await self._get_ltp(self.pe_leg['key'])
            if c_ltp and p_ltp:
                combined_ltp = c_ltp + p_ltp

        if ce_atp and pe_atp and combined_ltp is not None:
            combined_vwap = ce_atp + pe_atp

            # Log current state regardless of RSI status for visibility
            rsi_str = f"{rsi_val:.2f}" if rsi_val is not None else "WAIT"
            logger.info(f"[SellV3] Indicator Check ({self.ce_leg['strike']}CE + {self.pe_leg['strike']}PE): "
                        f"RSI={rsi_str}, Combined Price={combined_ltp:.2f}, Combined VWAP={combined_vwap:.2f} "
                        f"(CE_VWAP:{ce_atp:.2f}, PE_VWAP:{pe_atp:.2f})")

            if rsi_val is not None:
                rsi_threshold = rsi_cfg.get('threshold', 50)
                if combined_ltp > combined_vwap and rsi_val > rsi_threshold:
                    await self._exit_all(timestamp, f"Indicator Exit: Price({combined_ltp:.2f}) > VWAP({combined_vwap:.2f}) and RSI({rsi_val:.2f}) > {rsi_threshold}")
        else:
            reasons = []
            if rsi_val is None: reasons.append("Missing RSI (History)")
            if not ce_atp: reasons.append(f"Missing CE VWAP({self.ce_leg['strike']})")
            if not pe_atp: reasons.append(f"Missing PE VWAP({self.pe_leg['strike']})")
            if combined_ltp is None: reasons.append("Missing Current LTP")
            logger.warning(f"[SellV3] Indicator Check Skipped: {', '.join(reasons)}")

    async def _exit_all(self, timestamp, reason):
        logger.info(f"[SellV3] EXIT ALL: {reason}")

        for leg in [self.ce_leg, self.pe_leg]:
            if not leg: continue
            expiry = self.orchestrator.atm_manager.signal_expiry_date
            lookup = self.orchestrator.atm_manager.contract_lookup.get(expiry, {})
            contract = lookup.get(float(leg['strike']), {}).get(leg['side'])

            ltp = await self._get_ltp(leg['key'])

            # Stop live data feed for this contract
            await event_bus.publish('REMOVE_FROM_WATCHLIST', {'instrument_key': leg['key']})

            product_type = self._cfg('product_type', 'NRML')
            for broker in self.orchestrator.broker_manager.brokers:
                if not broker.is_configured_for_instrument(self.instrument_name): continue
                qty = broker.config_manager.get_int(broker.instance_name, 'quantity', 1) * contract.lot_size
                if not self.orchestrator.is_backtest and not getattr(broker, 'paper_trade', False):
                    broker.place_order(contract, 'BUY', qty, expiry, product_type=product_type)

                if self.orchestrator.is_backtest and self.orchestrator.pnl_tracker:
                    self.orchestrator.pnl_tracker.exit_trade(side=leg['side'], exit_price=ltp, timestamp=timestamp, reason=reason, instrument_key=leg['key'])

        self.active = False
        self.ce_leg = None
        self.pe_leg = None
        self.save_state()

        # Check for restart
        if timestamp.time() < time(14, 0, 0):
            logger.info("[SellV3] Restarting strategy...")
            # Restart happens on next tick because active is False
        else:
            logger.info("[SellV3] After 2:00 PM. No more trades today.")

    async def close_all(self, timestamp):
        """Forced exit of all V3 positions."""
        if self.active:
            await self._exit_all(timestamp, "Forced Close")

    def reconnect_positions(self):
        """Restores live data subscriptions for active positions after bot restart."""
        if not self.active:
            return

        logger.info(f"[SellV3] Reconnecting live data for active Strangle: {self.ce_leg['strike']}CE + {self.pe_leg['strike']}PE")

        # We must use asyncio.create_task because this is called from sync finalize_initialization
        for leg in [self.ce_leg, self.pe_leg]:
            if leg and leg.get('key'):
                asyncio.create_task(event_bus.publish('ADD_TO_WATCHLIST', {'instrument_key': leg['key']}))
