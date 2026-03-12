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
    - Initial Entry (09:16:05): Direct ATM selection, Lower LTP side first.
    - If Lower LTP < threshold, move 1 strike ITM.
    - Match other side strictly lower premium.
    - Re-entry scanner: After first trade, scan range for Straddles with crossover and RSI filters.
    - Selection: Strike nearest to ATM among those passing filters.
    - Combined RSI (5m) and Combined VWAP (ATP) exit.
    - Target Profit (12%), LTP < 20, and Dynamic TSL exits.
    - VWAP Slope Exit: 1% rise from lowest point since entry.
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
        self.last_exit_timestamp = None
        self.closed_trades = [] # Persistent history of closed trades

        # Strategy State
        self.lowest_combined_vwap = None

        # TSL State
        self.tsl_wait_high_hit = False
        self.tsl_scalable_lock_points = 0.0 # Trailing lock in points

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
            'lowest_combined_vwap': self.lowest_combined_vwap,
            'tsl_wait_high_hit': self.tsl_wait_high_hit,
            'tsl_scalable_lock_points': self.tsl_scalable_lock_points,
            'closed_trades': self.closed_trades
        }
        try:
            os.makedirs('config', exist_ok=True)
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.debug(f"[SellV3] State saved to {self.state_file} (Active: {self.active}, Closed: {len(self.closed_trades)})")
        except Exception as e:
            logger.error(f"[SellV3] Failed to save state to {self.state_file}: {e}")

    def load_state(self):
        if not os.path.exists(self.state_file):
            logger.debug(f"[SellV3] No state file found at {self.state_file}")
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
            self.lowest_combined_vwap = state.get('lowest_combined_vwap')
            self.tsl_wait_high_hit = state.get('tsl_wait_high_hit', False)
            self.tsl_scalable_lock_points = state.get('tsl_scalable_lock_points', 0.0)
            self.closed_trades = state.get('closed_trades', [])

            logger.info(f"[SellV3] State loaded from {self.state_file}. History: {len(self.closed_trades)} trades.")

            # Sync to orchestrator trade log for dashboard persistence
            trade_log = getattr(self.orchestrator, 'trade_log', None)
            if trade_log:
                # Deduplicate by order_id or timestamp/strike if order_id is missing (backtest)
                existing_fingerprints = set()
                for t in trade_log.trades:
                    if t.get('order_id'): existing_fingerprints.add(t['order_id'])
                    else: existing_fingerprints.add(f"{t.get('time')}_{t.get('strike')}_{t.get('direction')}")

                # We add them in reverse order because LiveTradeLog.add inserts at index 0
                count = 0
                for trade in reversed(self.closed_trades):
                    fp = trade.get('order_id') if trade.get('order_id') else f"{trade.get('time')}_{trade.get('strike')}_{trade.get('direction')}"
                    if fp not in existing_fingerprints:
                        trade_log.add(trade)
                        count += 1
                logger.debug(f"[SellV3] Synchronized {count} new trades to dashboard log (Total: {len(trade_log.trades)}).")

            if self.active:
                logger.info(f"[SellV3] Recovered active trade from {self.entry_timestamp}")
        except Exception as e:
            logger.error(f"[SellV3] Failed to load state from {self.state_file}: {e}")

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

    async def _get_balanced_candidate(self, timestamp):
        """Logic for Balanced selection: ATM side-selection with LTP >= threshold rule, match strictly lower."""
        index_price = self.orchestrator.state_manager.index_price
        interval = self.orchestrator.config_manager.get_int(self.instrument_name, 'strike_interval', 50)
        if not index_price: return None

        atm = int(round(index_price / interval) * interval)
        expiry = self.orchestrator.atm_manager.signal_expiry_date

        ce_key = self.orchestrator.atm_manager.find_instrument_key_by_strike(atm, 'CALL', expiry)
        pe_key = self.orchestrator.atm_manager.find_instrument_key_by_strike(atm, 'PUT', expiry)

        if not ce_key or not pe_key: return None
        ce_ltp, pe_ltp = await self._get_ltp(ce_key), await self._get_ltp(pe_key)
        if ce_ltp is None or pe_ltp is None: return None

        # side with lower ltp
        if ce_ltp < pe_ltp:
            low_side, low_strike, low_key, low_ltp, other_side = 'CE', atm, ce_key, ce_ltp, 'PE'
        else:
            low_side, low_strike, low_key, low_ltp, other_side = 'PE', atm, pe_key, pe_ltp, 'CE'

        # if lower < threshold, go 1 ITM
        ltp_threshold = self._cfg('ltp_threshold', 50.0)
        if low_ltp < ltp_threshold:
            logger.info(f"[SellV3] Balanced Mode: Lower LTP {low_ltp:.2f} < {ltp_threshold}. Moving 1 strike ITM.")
            if low_side == 'CE': low_strike -= interval
            else: low_strike += interval
            low_key = self.orchestrator.atm_manager.find_instrument_key_by_strike(low_strike, 'CALL' if low_side == 'CE' else 'PUT', expiry)
            if low_key:
                new_ltp = await self._get_ltp(low_key)
                if new_ltp: low_ltp = new_ltp

        # Match other side strictly lower
        match_strike, match_key, match_ltp = await self._find_matching_strike(low_ltp, other_side, expiry)
        if not match_key:
            logger.error(f"[SellV3] Balanced Mode: No matching {other_side} strictly lower than {low_ltp:.2f}.")
            return None

        ce_info = {'strike': low_strike, 'key': low_key, 'ltp': low_ltp} if low_side == 'CE' else {'strike': match_strike, 'key': match_key, 'ltp': match_ltp}
        pe_info = {'strike': low_strike, 'key': low_key, 'ltp': low_ltp} if low_side == 'PE' else {'strike': match_strike, 'key': match_key, 'ltp': match_ltp}

        return {
            'ce': ce_info,
            'pe': pe_info,
            'total_premium': ce_info['ltp'] + pe_info['ltp'],
            'atm_dist': 0,
            'is_strangle': True
        }

    def _finalize_entry(self, timestamp):
        self.active = True
        self.entry_timestamp = timestamp
        self.total_premium_points = self.ce_leg['entry_ltp'] + self.pe_leg['entry_ltp']
        self.tsl_wait_high_hit = False
        self.lowest_combined_vwap = None
        self.tsl_scalable_lock_points = 0.0
        self.last_exit_timestamp = None
        self.last_indicator_check_minute = -1
        self.save_state()
        logger.info(f"[SellV3] V3 Position Entered. Total Premium: {self.total_premium_points:.2f}")

    async def _scan_for_best_candidate(self, timestamp):
        """Scans multi-strike range for the best straddle candidate passing RSI/VWAP filters (Re-entry only)."""
        index_price = self.orchestrator.state_manager.index_price
        interval = self.orchestrator.config_manager.get_int(self.instrument_name, 'strike_interval', 50)
        if not index_price: return None

        atm = int(round(index_price / interval) * interval)
        expiry = self.orchestrator.atm_manager.signal_expiry_date
        ltp_threshold = self._cfg('ltp_threshold', 50.0)
        offset = self._cfg('reentry_strike_offset', 2)
        tf = self._cfg('rsi.tf', 5)
        rsi_cfg = self._cfg('rsi', {})
        rsi_threshold = rsi_cfg.get('threshold', 50)

        candidates = [] # list of {ce_leg, pe_leg, total_premium}

        # User Requirement: re-entry strictly evaluates Straddle combinations
        scan_range = range(-offset, offset + 1) if self._cfg('multi_strike_scan_enabled', True) else range(0, 1)
        for i in scan_range:
            base_strike = atm + (i * interval)
            ce_key = self.orchestrator.atm_manager.find_instrument_key_by_strike(base_strike, 'CALL', expiry)
            pe_key = self.orchestrator.atm_manager.find_instrument_key_by_strike(base_strike, 'PUT', expiry)

            if not ce_key or not pe_key: continue
            ce_ltp = await self._get_ltp(ce_key)
            pe_ltp = await self._get_ltp(pe_key)

            if ce_ltp is None or pe_ltp is None: continue
            if ce_ltp < ltp_threshold and pe_ltp < ltp_threshold: continue

            # Entry Criteria (Indicator Check)
            rsi_val = await self.orchestrator.indicator_manager.calculate_combined_rsi(
                ce_key, pe_key, tf, rsi_cfg.get('period', 14), timestamp, include_current=False
            )
            if rsi_val is None and self.orchestrator.is_backtest: rsi_val = 0.0
            if rsi_val is None or rsi_val > rsi_threshold: continue

            # VWAP Crossover Check
            ce_vwap = await self.orchestrator.indicator_manager.calculate_vwap(ce_key, timestamp)
            pe_vwap = await self.orchestrator.indicator_manager.calculate_vwap(pe_key, timestamp)
            if ce_vwap is None or pe_vwap is None:
                if self.orchestrator.is_backtest: ce_vwap, pe_vwap = ce_ltp, pe_ltp
                else: continue

            combined_vwap = ce_vwap + pe_vwap
            ohlc1 = await self.orchestrator.indicator_manager.get_robust_ohlc(ce_key, tf, timestamp, include_current=False)
            ohlc2 = await self.orchestrator.indicator_manager.get_robust_ohlc(pe_key, tf, timestamp, include_current=False)
            if ohlc1 is None or ohlc1.empty or ohlc2 is None or ohlc2.empty: continue

            c1, c2 = ohlc1.iloc[-1], ohlc2.iloc[-1]
            comb_open, comb_high, comb_close = c1['open'] + c2['open'], c1['high'] + c2['high'], c1['close'] + c2['close']
            is_crossover = (comb_open > combined_vwap or comb_high > combined_vwap) and (comb_close <= combined_vwap)

            logger.info(f"[SellV3][Scan Re-entry] {int(base_strike)}CE+{int(base_strike)}PE (Base: {int(base_strike)}): "
                         f"RSI:{rsi_val:.2f}, O:{comb_open:.2f}, H:{comb_high:.2f}, C:{comb_close:.2f}, VWAP:{combined_vwap:.2f} "
                         f"| RSI_OK:{rsi_val <= rsi_threshold}, CROSS_OK:{is_crossover}")

            if rsi_val <= rsi_threshold and is_crossover:
                candidates.append({
                    'ce': {'strike': base_strike, 'key': ce_key, 'ltp': ce_ltp},
                    'pe': {'strike': base_strike, 'key': pe_key, 'ltp': pe_ltp},
                    'total_premium': ce_ltp + pe_ltp,
                    'atm_dist': abs(base_strike - index_price)
                })

        if not candidates: return None
        # User requirement: trade on that strike which is near to ATM when all entry criteria are fulfilled
        return min(candidates, key=lambda x: x['atm_dist'])

    async def _check_entry(self, timestamp):
        # Time Constraints
        start_time_str = self._cfg('start_time', '09:16:05')
        start_time_obj = datetime.strptime(start_time_str, '%H:%M:%S').time()
        end_time_str = self._cfg('entry_end_time', '14:00:00')
        end_time_obj = datetime.strptime(end_time_str, '%H:%M:%S').time()

        if timestamp.time() < start_time_obj or timestamp.time() >= end_time_obj: return

        # User Fix: Restrict entry to TF-settled boundaries
        tf = self._cfg('rsi.tf', 5)
        is_start_of_day = (timestamp.time() >= start_time_obj and timestamp.time() < (datetime.combine(timestamp.date(), start_time_obj) + timedelta(minutes=1)).time())
        is_settled_boundary = (timestamp.minute % tf == 0 and timestamp.second >= 10)
        was_recently_closed = self.last_exit_timestamp is not None

        if not is_start_of_day and not is_settled_boundary and not was_recently_closed:
            if self.orchestrator.is_backtest and (timestamp.minute % tf == 0): pass
            else: return

        # Re-entry Cooldown: Wait at least 60 seconds after an exit
        if self.last_exit_timestamp:
            elapsed = (timestamp - self.last_exit_timestamp).total_seconds()
            if elapsed < 60: return

        # Log entry attempt at most every 5 seconds
        now_ts = (timestamp.hour * 3600) + (timestamp.minute * 60) + timestamp.second
        if now_ts - getattr(self, '_last_entry_attempt_log', 0) >= 5:
            self._last_entry_attempt_log = now_ts
            logger.info(f"[SellV3] Attempting entry at {timestamp} (Spot: {self.orchestrator.state_manager.index_price})")

        # 2. Strategy Phase (Initial vs Re-entry)
        today_str = timestamp.strftime('%Y-%m-%d')
        trades_today = [t for t in self.closed_trades if t.get('date') == today_str and "Cleanup" not in t.get('reason', '')]
        is_first_trade_of_day = len(trades_today) == 0

        workflow_phase = self._cfg('workflow_phase', 'BEGINNING')
        reentry_mode = self._cfg('reentry_mode', 'SCANNER')

        best = None

        # Logic Selection based on Phase Toggle
        # 1. AT BEGINNING: First trade is always Balanced. Subsequent trades follow reentry_mode.
        # 2. CONTINUE: Every trade (including first) follows reentry_mode.

        use_balanced_logic = False
        if workflow_phase == "BEGINNING":
            if is_first_trade_of_day:
                use_balanced_logic = True
            else:
                use_balanced_logic = (reentry_mode == "BALANCED")
        else: # CONTINUE
            use_balanced_logic = (reentry_mode == "BALANCED")

        if use_balanced_logic:
            # Balanced Mode (No filters)
            best = await self._get_balanced_candidate(timestamp)
        else:
            # Technical Scanner Mode
            best = await self._scan_for_best_candidate(timestamp)

        if not best: return

        final_ce_strike, final_ce_key, final_ce_ltp = best['ce']['strike'], best['ce']['key'], best['ce']['ltp']
        final_pe_strike, final_pe_key, final_pe_ltp = best['pe']['strike'], best['pe']['key'], best['pe']['ltp']

        # Execute legs
        results = await asyncio.gather(
            self._execute_sell('CE', final_ce_strike, final_ce_key, final_ce_ltp, timestamp),
            self._execute_sell('PE', final_pe_strike, final_pe_key, final_pe_ltp, timestamp),
            return_exceptions=True
        )
        success = all(r is True for r in results)

        if not success:
            logger.error("[SellV3] Entry failed: One or more legs failed. Cleaning up orphans.")
            if self.ce_leg or self.pe_leg:
                for side, leg in [('CE', self.ce_leg), ('PE', self.pe_leg)]:
                    if leg: await self._execute_leg_exit(side, leg, timestamp, "Cleanup Orphaned Leg")
                self.ce_leg, self.pe_leg = None, None
            return

        self._finalize_entry(timestamp)

    async def _find_matching_strike(self, target_ltp, side, expiry):
        """Find strike nearest to target_ltp. If balancing enabled, must be strictly LESS than target_ltp."""
        interval = self.orchestrator.config_manager.get_int(self.instrument_name, 'strike_interval', 50)
        index_price = self.orchestrator.state_manager.index_price
        atm = int(round(index_price / interval) * interval)

        best_strike = None
        best_key = None
        best_ltp = None
        min_diff = float('inf')
        balance_mode = self._cfg('ltp_balance_enabled', True)

        logger.debug(f"[SellV3] Finding matching {side} strike for target LTP: {target_ltp:.2f} (Balance: {balance_mode})")

        # Check a range of strikes (ATM +/- 15 to be safer)
        for i in range(-15, 16):
            strike = atm + (i * interval)
            key = self.orchestrator.atm_manager.find_instrument_key_by_strike(strike, side, expiry)
            if not key: continue
            ltp = await self._get_ltp(key)
            if ltp:
                logger.debug(f"[SellV3] Candidate {side} {strike}: LTP {ltp:.2f}")
                if balance_mode:
                    # User requirement: NEAREST AND LESS THEN target_ltp
                    if ltp < target_ltp:
                        diff = target_ltp - ltp
                        if diff < min_diff:
                            min_diff = diff
                            best_strike = strike
                            best_key = key
                            best_ltp = ltp
                else:
                    # Legacy matching (nearest)
                    diff = abs(ltp - target_ltp)
                    if diff < min_diff:
                        min_diff = diff
                        best_strike = strike
                        best_key = key
                        best_ltp = ltp

        if best_strike:
            logger.info(f"[SellV3] Selected matching {side} {best_strike} at {best_ltp:.2f}")
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
            'side': side,
            'lot_size': contract.lot_size
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
        # Time Constraints for Smart Rolling
        end_time_str = self._cfg('entry_end_time', '14:00:00')
        end_time_obj = datetime.strptime(end_time_str, '%H:%M:%S').time()
        can_roll = timestamp.time() < end_time_obj

        # 1. LTP < 20 Exit (Tick by Tick)
        ce_ltp = await self._get_ltp(self.ce_leg['key'])
        pe_ltp = await self._get_ltp(self.pe_leg['key'])

        if not ce_ltp or not pe_ltp: return

        ltp_exit_min = self._cfg('ltp_exit_min', 20.0)
        if ce_ltp < ltp_exit_min or pe_ltp < ltp_exit_min:
            if can_roll:
                await self._perform_smart_roll(timestamp, f"LTP below {ltp_exit_min}")
            else:
                await self._exit_all(timestamp, f"LTP below {ltp_exit_min} (Post-EntryEnd)")
            return

        # 2. Target Profit 12% Exit (Tick by Tick)
        current_premium = ce_ltp + pe_ltp
        profit_points = self.total_premium_points - current_premium
        target_pct = self._cfg('profit_target_pct', 12.0)
        target_points = (target_pct / 100.0) * self.total_premium_points

        if profit_points >= target_points:
            # Smart Rolling Check
            if can_roll and self._cfg('smart_rolling_enabled', True):
                await self._perform_smart_roll(timestamp, f"Target Profit {target_pct}% hit")
            else:
                await self._exit_all(timestamp, f"Target Profit {target_pct}% hit")
            return

        # 2b. Ratio Exit (Tick by Tick)
        # Hits if Highest_LTP / Lowest_LTP >= Threshold
        if self._cfg('ratio_exit.enabled', True):
            ratio_threshold = self._cfg('ratio_exit.threshold', 3.0)
            high_ltp = max(ce_ltp, pe_ltp)
            low_ltp = min(ce_ltp, pe_ltp)
            if low_ltp > 0:
                current_ratio = high_ltp / low_ltp
                if current_ratio >= ratio_threshold:
                    if can_roll:
                        await self._perform_smart_roll(timestamp, f"Ratio {current_ratio:.2f} >= {ratio_threshold}")
                    else:
                        await self._exit_all(timestamp, f"Ratio {current_ratio:.2f} hit (Post-EntryEnd)")
                    return

        # 3. Scalable TSL (Lock Profit in Points)
        # User Requirement: For 1 lot profit lock 200 if profit reaches 1000
        # and for every increase in profit by 250 trail Profit by 200
        if self._cfg('tsl_scalable.enabled', False):
            base_profit = self._cfg('tsl_scalable.base_profit', 1000)
            base_lock = self._cfg('tsl_scalable.base_lock', 200)
            step_profit = self._cfg('tsl_scalable.step_profit', 250)
            step_lock = self._cfg('tsl_scalable.step_lock', 200)

            # Points-based calculation for scalability
            lot_size = self.orchestrator.config_manager.get_int(self.instrument_name, 'lot_size', 50)
            base_profit_pts = base_profit / lot_size
            base_lock_pts = base_lock / lot_size
            step_profit_pts = step_profit / lot_size
            step_lock_pts = step_lock / lot_size

            # Initial lock trigger
            if self.tsl_scalable_lock_points == 0 and profit_points >= base_profit_pts:
                self.tsl_scalable_lock_points = base_lock_pts
                logger.info(f"[SellV3] Scalable TSL: First profit lock hit at {profit_points:.2f} pts. Locking {self.tsl_scalable_lock_points:.2f} pts.")

            # Trailing step
            if self.tsl_scalable_lock_points > 0:
                # Calculate how many steps we've moved beyond the base
                extra_profit = profit_points - base_profit_pts
                if extra_profit >= step_profit_pts:
                    steps = int(extra_profit // step_profit_pts)
                    new_lock = base_lock_pts + (steps * step_lock_pts)
                    if new_lock > self.tsl_scalable_lock_points:
                        self.tsl_scalable_lock_points = new_lock
                        logger.info(f"[SellV3] Scalable TSL: Profit increased. New lock: {self.tsl_scalable_lock_points:.2f} pts.")

            if self.tsl_scalable_lock_points > 0 and profit_points <= self.tsl_scalable_lock_points:
                await self._exit_all(timestamp, f"Scalable TSL Lock hit at {profit_points:.2f} pts (Lock: {self.tsl_scalable_lock_points:.2f})")
                return

        # 3b. Legacy Dynamic TSL (Tick by Tick)
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

        # 4. VWAP Slope Exit (Tracking Lowest Point)
        # User Requirement: if the VWAP slope goes up by 1% from its lowest point, we're out.
        if self._cfg('vwap_slope_exit.enabled', False):
            ce_vwap = await self.orchestrator.indicator_manager.calculate_vwap(self.ce_leg['key'], timestamp)
            pe_vwap = await self.orchestrator.indicator_manager.calculate_vwap(self.pe_leg['key'], timestamp)

            if ce_vwap and pe_vwap:
                comb_vwap = ce_vwap + pe_vwap
                if self.lowest_combined_vwap is None or comb_vwap < self.lowest_combined_vwap:
                    self.lowest_combined_vwap = comb_vwap

                threshold_pct = self._cfg('vwap_slope_exit.threshold_pct', 1.0)
                if comb_vwap >= self.lowest_combined_vwap * (1 + threshold_pct / 100.0):
                    await self._exit_all(timestamp, f"VWAP Slope Exit: Current {comb_vwap:.2f} rose {threshold_pct}% from low {self.lowest_combined_vwap:.2f}")
                    return

        # 5. Indicators Exit (TF-min Candle Close)
        # User fix: Trigger 10 seconds into the new TF-minute interval (LIVE only).
        # In BACKTEST, we trigger on any tick within the boundary minute.
        tf = self._cfg('rsi.tf', 5)
        is_boundary = (timestamp.minute % tf == 0)
        trigger_now = False
        if self.orchestrator.is_backtest:
            trigger_now = is_boundary
        else:
            trigger_now = is_boundary and (timestamp.second >= 10)

        if trigger_now and timestamp.minute != self.last_indicator_check_minute:
            # This logic fires slightly after the boundary (e.g. 09:20:10, 09:25:10)
            self.last_indicator_check_minute = timestamp.minute

            # Note: 14-candle wait is handled by IndicatorManager history buffer.
            await self._check_indicator_exit(timestamp)

    async def _check_indicator_exit(self, timestamp):
        # User Requirement: Balanced Mode should ignore Technical Exits (RSI/VWAP)
        workflow_phase = self._cfg('workflow_phase', 'BEGINNING')
        reentry_mode = self._cfg('reentry_mode', 'SCANNER')

        today_str = timestamp.strftime('%Y-%m-%d')
        trades_today = [t for t in self.closed_trades if t.get('date') == today_str and "Cleanup" not in t.get('reason', '')]
        is_first_trade_of_day = len(trades_today) == 0 # Note: Active trade is NOT in history yet

        # Determine current mode
        current_mode_is_balanced = False
        if workflow_phase == "BEGINNING":
            # In BEGINNING phase, the CURRENT active trade is balanced if it was the first of the day
            # Since we haven't closed it yet, trades_today will be empty for the first trade
            current_mode_is_balanced = is_first_trade_of_day or (reentry_mode == "BALANCED")
        else:
            current_mode_is_balanced = (reentry_mode == "BALANCED")

        if current_mode_is_balanced:
            # Skip Technical Exits for Balanced trades
            return

        # Combined RSI > 50
        # For exit, we use the completed candles (include_current=False) as requested
        tf = self._cfg('rsi.tf', 5)
        rsi_cfg = self._cfg('rsi', {})
        rsi_val = await self.orchestrator.indicator_manager.calculate_combined_rsi(
            self.ce_leg['key'], self.pe_leg['key'],
            tf, rsi_cfg.get('period', 14), timestamp,
            include_current=False
        )

        # For VWAP and Price, we use the last finalized candle's data
        ce_atp = await self.orchestrator.indicator_manager.calculate_vwap(self.ce_leg['key'], timestamp)
        pe_atp = await self.orchestrator.indicator_manager.calculate_vwap(self.pe_leg['key'], timestamp)

        combined_ltp = None
        ohlc1 = await self.orchestrator.indicator_manager.get_robust_ohlc(self.ce_leg['key'], tf, timestamp, include_current=False)
        ohlc2 = await self.orchestrator.indicator_manager.get_robust_ohlc(self.pe_leg['key'], tf, timestamp, include_current=False)

        if ohlc1 is not None and not ohlc1.empty and ohlc2 is not None and not ohlc2.empty:
            combined_ltp = float(ohlc1.iloc[-1]['close'] + ohlc2.iloc[-1]['close'])

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

    async def _perform_smart_roll(self, timestamp, reason):
        """
        Brokerage Optimization: If new target strike matches current strike,
        skip broker orders and perform a virtual roll (internal price reset).
        """
        logger.info(f"[SellV3] SMART ROLL INITIATED: {reason}")
        # Save old legs in case of failure
        old_ce, old_pe = self.ce_leg, self.pe_leg

        # 1. Determine New Target strikes based on configured re-entry mode
        # Note: Smart Roll ONLY happens for re-entries, so we just check reentry_mode
        reentry_mode = self._cfg('reentry_mode', 'SCANNER')
        if reentry_mode == "BALANCED":
            best = await self._get_balanced_candidate(timestamp)
        else:
            best = await self._scan_for_best_candidate(timestamp)

        if not best:
            logger.warning("[SellV3] Smart Roll failed: No candidates passed filters. Falling back to Full Exit.")
            await self._exit_all(timestamp, f"{reason} (No Roll Candidate)")
            return

        final_ce_strike, final_ce_key, final_ce_ltp = best['ce']['strike'], best['ce']['key'], best['ce']['ltp']
        final_pe_strike, final_pe_key, final_pe_ltp = best['pe']['strike'], best['pe']['key'], best['pe']['ltp']

        # 2. Compare with current legs
        legs_to_close = []
        if float(final_ce_strike) != float(self.ce_leg['strike']):
            legs_to_close.append(('CE', self.ce_leg))
        if float(final_pe_strike) != float(self.pe_leg['strike']):
            legs_to_close.append(('PE', self.pe_leg))

        logger.info(f"[SellV3] Roll Comparison: CE({self.ce_leg['strike']}->{final_ce_strike}), PE({self.pe_leg['strike']}->{final_pe_strike})")

        # 3. Execute necessary broker exits
        for side, leg in legs_to_close:
            await self._execute_leg_exit(side, leg, timestamp, f"Roll Exit ({reason})")

        # 4. Record Virtual Exits for retained legs (for Order Book P&L)
        retained_sides = [s for s in ['CE', 'PE'] if s not in [l[0] for l in legs_to_close]]
        for side in retained_sides:
            leg = self.ce_leg if side == 'CE' else self.pe_leg
            roll_ltp = final_ce_ltp if side == 'CE' else final_pe_ltp
            await self._record_trade_log(side, leg, roll_ltp, timestamp, f"Smart Roll ({reason})")

        # 5. Execute necessary broker entries
        success = True
        if ('CE', old_ce) in legs_to_close:
            if not await self._execute_sell('CE', final_ce_strike, final_ce_key, final_ce_ltp, timestamp):
                success = False
        else:
            # Update retained CE leg state
            self.ce_leg['entry_ltp'] = final_ce_ltp
            self.ce_leg['entry_atp'] = self.orchestrator.state_manager.option_atps.get(final_ce_key) or final_ce_ltp

        if ('PE', old_pe) in legs_to_close:
            if not await self._execute_sell('PE', final_pe_strike, final_pe_key, final_pe_ltp, timestamp):
                success = False
        else:
            # Update retained PE leg state
            self.pe_leg['entry_ltp'] = final_pe_ltp
            self.pe_leg['entry_atp'] = self.orchestrator.state_manager.option_atps.get(final_pe_key) or final_pe_ltp

        if not success:
            logger.error("[SellV3] Smart Roll failed: One or more entry legs could not be executed. Forced Full Exit.")
            await self._exit_all(timestamp, f"Smart Roll Failure ({reason})")
            return

        # 6. Finalize State
        self.entry_timestamp = timestamp
        self.total_premium_points = self.ce_leg['entry_ltp'] + self.pe_leg['entry_ltp']
        self.tsl_wait_high_hit = False
        self.save_state()
        logger.info(f"[SellV3] Smart Roll Complete. New Combined Premium: {self.total_premium_points:.2f}")

    async def _execute_leg_exit(self, side, leg, timestamp, reason):
        expiry = self.orchestrator.atm_manager.signal_expiry_date
        lookup = self.orchestrator.atm_manager.contract_lookup.get(expiry, {})
        contract = lookup.get(float(leg['strike']), {}).get(leg['side'])

        ltp = await self._get_ltp(leg['key'])

        # Stop live data feed for this contract if it's being replaced
        await event_bus.publish('REMOVE_FROM_WATCHLIST', {'instrument_key': leg['key']})

        product_type = self._cfg('product_type', 'NRML')
        order_id = ""
        for broker in self.orchestrator.broker_manager.brokers:
            if not broker.is_configured_for_instrument(self.instrument_name): continue
            qty = broker.config_manager.get_int(broker.instance_name, 'quantity', 1) * contract.lot_size
            if not self.orchestrator.is_backtest and not getattr(broker, 'paper_trade', False):
                order_id = broker.place_order(contract, 'BUY', qty, expiry, product_type=product_type)

            if self.orchestrator.is_backtest and self.orchestrator.pnl_tracker:
                self.orchestrator.pnl_tracker.exit_trade(side=leg['side'], exit_price=ltp, timestamp=timestamp, reason=reason, instrument_key=leg['key'])

        await self._record_trade_log(side, leg, ltp, timestamp, reason, order_id)

    async def _record_trade_log(self, side, leg, ltp, timestamp, reason, order_id=""):
        try:
            from .live_trade_log import LiveTradeLog
            trade_log = getattr(self.orchestrator, 'trade_log', None)
            entry_ltp = leg.get('entry_ltp', 0)
            pnl_pts = entry_ltp - ltp

            # Use stored lot_size or fallback to avoid None crashes during lookup latency
            lot_size = leg.get('lot_size')
            if lot_size is None:
                lot_size = self.orchestrator.config_manager.get_int(self.instrument_name, 'lot_size', 50)
                logger.debug(f"[SellV3] Lot size not found in leg state, using fallback: {lot_size}")

            # Calculate total Rupee PnL across all configured brokers
            total_pnl_rs = 0
            for broker in self.orchestrator.broker_manager.brokers:
                if not broker.is_configured_for_instrument(self.instrument_name): continue
                b_qty = broker.config_manager.get_int(broker.instance_name, 'quantity', 1)
                total_pnl_rs += pnl_pts * b_qty * lot_size

            entry = LiveTradeLog.make_entry(
                trade_type='SELL',
                direction=leg['side'],
                strike=leg['strike'],
                entry_price=entry_ltp,
                exit_price=ltp,
                pnl_pts=pnl_pts,
                pnl_rs=total_pnl_rs,
                reason=reason,
                order_id=str(order_id) if order_id else '',
                timestamp=timestamp,
            )

            # Persistence: Add to persistent list
            self.closed_trades.insert(0, entry)
            if len(self.closed_trades) > 50: self.closed_trades = self.closed_trades[:50]

            if trade_log:
                trade_log.add(entry)
        except Exception as e:
            logger.error(f"[SellV3] Failed to log trade: {e}")

    async def _exit_all(self, timestamp, reason):
        logger.info(f"[SellV3] EXIT ALL: {reason}")
        self.last_exit_timestamp = timestamp

        # We process legs sequentially to avoid race conditions on state
        for side, leg in [('CE', self.ce_leg), ('PE', self.pe_leg)]:
            if not leg: continue
            await self._execute_leg_exit(side, leg, timestamp, reason)

        self.active = False
        self.ce_leg = None
        self.pe_leg = None
        self.save_state()

        # Check for restart
        end_time_str = self._cfg('entry_end_time', '14:00:00')
        end_time_obj = datetime.strptime(end_time_str, '%H:%M:%S').time()

        if timestamp.time() < end_time_obj:
            logger.info("[SellV3] Waiting for next entry opportunity...")
            # Restart happens on next tick because active is False
        else:
            logger.info(f"[SellV3] After {end_time_str}. No more trades today.")

    async def close_all(self, timestamp):
        """Forced exit of all V3 positions."""
        if self.active:
            await self._exit_all(timestamp, "Forced Close")

    def reconnect_positions(self):
        """Restores live data subscriptions and PnL tracking for active positions after bot restart."""
        if not self.active:
            return

        logger.info(f"[SellV3] Reconnecting active Strangle: {self.ce_leg['strike']}CE + {self.pe_leg['strike']}PE")

        # Register with backtest PnL tracker to ensure continuous report generation
        if self.orchestrator.is_backtest and self.orchestrator.pnl_tracker:
            expiry = self.orchestrator.atm_manager.signal_expiry_date
            lookup = self.orchestrator.atm_manager.contract_lookup.get(expiry, {})
            for leg in [self.ce_leg, self.pe_leg]:
                if not leg: continue
                contract = lookup.get(float(leg['strike']), {}).get(leg['side'])
                if contract:
                    # We use standard 1x quantity for recovered legs
                    self.orchestrator.pnl_tracker.enter_trade(
                        side=leg['side'], instrument_key=leg['key'], entry_price=leg['entry_ltp'],
                        timestamp=self.entry_timestamp, strike_price=leg['strike'], contract=contract,
                        strategy_log=f"SellV3 {leg['side']} (Recovered)", entry_type='SELL', quantity=1
                    )

        # We must use asyncio.create_task because this is called from sync finalize_initialization
        for leg in [self.ce_leg, self.pe_leg]:
            if leg and leg.get('key'):
                asyncio.create_task(event_bus.publish('ADD_TO_WATCHLIST', {'instrument_key': leg['key']}))
