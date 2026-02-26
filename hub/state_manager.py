import pandas as pd
import datetime
from utils.logger import logger
import json
import os
import asyncio
from hub.state_redis import RedisStateManager

class StateManager:
    def __init__(self, config_manager, instrument_name, user_id=None, redis_manager=None, is_backtest=False):
        # Configuration
        self.config_manager = config_manager
        self.instrument_name = instrument_name
        self.user_id = user_id
        self.is_backtest = is_backtest

        # In multi-tenant mode, state files are user-specific
        suffix = f"_{user_id}" if user_id else ""
        if self.is_backtest:
            suffix += "_backtest"

        # Dynamic state path based on strategy parameters to allow parallel execution of different timeframes
        entry_tf = self.config_manager.get('settings', 'entry_timeframe_minutes', fallback='1')
        exit_tf = self.config_manager.get('settings', 'exit_timeframe_minutes', fallback='1')

        self.state_file_path = f"config/state_{instrument_name}{suffix}_{entry_tf}m_{exit_tf}m_CROSSOVER.json"
        self.redis_manager = redis_manager
        self.redis_key_prefix = f"bot:{instrument_name}:{user_id if user_id else 'default'}"

        # --- PNL Calculation settings ---
        self.quantity = self.config_manager.get_int('settings', 'default_qty', 1)
        self.lot_size = self.config_manager.get_int(self.instrument_name, 'lot_size', 1)

        # Live Market Data
        self.option_prices = {}
        self.option_atps = {}
        self.option_deltas = {}
        self.option_data = {}
        self.option_gammas = {}
        self.option_thetas = {}
        self.option_ivs = {}
        self.option_vegas = {}
        self.option_volumes = {}
        self.option_bid_ask = {}
        self.last_tick_times = {}
        self.last_exchange_time = None # Latest timestamp from the exchange (currentTs)
        self.spot_price = None # This will now store the FUTURES price
        self.index_price = None # This will store the INDEX price
        self.spot_ohlc = None
        self.last_calculation_time = None
        self.timestamp = None
        self.target_strike = None

        # Core State
        self.call_position = {}
        self.put_position = {}
        self.current_trade = {}
        self.dual_sr_monitoring_data = {}
        self.v2_monitoring_atm_strike = None
        self.s1_monotonic_tracker = {} # Persistent S1 levels per strike/side/tf
        
        # --- Session Statistics ---
        self.total_pnl = 0.0
        self.trade_count = 0
        self.last_trade_pnl = 0.0 # PNL of the last closed trade

        self.call_cooldown_until = None
        self.put_cooldown_until = None

        # Signal State & Counters
        self.exit_signals = {'CALL': [], 'PUT': []}

        # --- Async & Data Sync Helpers ---
        self.waiting_for_resubscription_keys = set()
        self.initial_instrument_keys = []
        self.live_contracts_fetched = False
        self.cache = {}

        # Synchronization events
        self.trade_confirmed_event = asyncio.Event()
        self.trade_closed_event = asyncio.Event()

        # 9:15 Breach Gate Flags
        self.range_915_breached_up = False
        self.range_915_breached_down = False

    async def reset_trade_state(self, direction, is_backtest=False):
        """
        Resets state for a specific trade direction.
        In live mode, it also updates session stats and applies a cooldown.
        In backtest mode, it simply clears the position to allow new trades.
        """
        position_to_close = None

        if direction == "CALL" and self.call_position:
            position_to_close = self.call_position
            self.call_position = {}
            self.current_trade = {}
            # Clear monotonic S1 tracker for CALL side to ensure fresh SL on next trade
            ce_keys = [k for k in self.s1_monotonic_tracker.keys() if '_CE_' in k]
            for k in ce_keys:
                del self.s1_monotonic_tracker[k]
                logger.debug(f"V2: Cleared monotonic S1 tracker for {k} on trade reset.")
        elif direction == "PUT" and self.put_position:
            position_to_close = self.put_position
            self.put_position = {}
            self.current_trade = {}
            # Clear monotonic S1 tracker for PUT side to ensure fresh SL on next trade
            pe_keys = [k for k in self.s1_monotonic_tracker.keys() if '_PE_' in k]
            for k in pe_keys:
                del self.s1_monotonic_tracker[k]
                logger.debug(f"V2: Cleared monotonic S1 tracker for {k} on trade reset.")

        if not position_to_close:
            return # Nothing to do

        # Reset entry_confirmed in monitoring data so re-entry is allowed after this trade exits
        side_key = 'ce_data' if direction == 'CALL' else 'pe_data'
        if self.dual_sr_monitoring_data and side_key in self.dual_sr_monitoring_data:
            self.dual_sr_monitoring_data[side_key]['entry_confirmed'] = False
            logger.debug(f"V2: Reset entry_confirmed for {direction} in monitoring data on trade exit.")

        if is_backtest:
            logger.debug(f"[Backtest] Cleared {direction} position in StateManager.")
            return # In backtest, PnL is handled by the tracker.

        # --- Live Mode Logic ---
        # Cooldown removed to allow immediate reversal (flip) per user request.

        final_pnl = self._calculate_pnl(position_to_close)
        self.last_trade_pnl = final_pnl
        self.total_pnl += final_pnl
        logger.info(f"Closed {direction} trade. Final PNL: {self.last_trade_pnl:.2f}, Total Session PNL: {self.total_pnl:.2f}")
        await self.save_state()

    def set_waiting_for_resubscription(self, keys):
        """Sets the state to indicate we are waiting for initial ticks from a new set of instruments."""
        self.waiting_for_resubscription_keys = set(keys)
        logger.debug(f"StateManager is now waiting for initial data from {len(keys)} instruments.")

    async def enter_trade(self, direction, instrument_key, instrument_symbol, ltp, strike_price, timestamp=None, signal_strike=None, s1_fast_tf=1, s1_slow_tf=5, entry_type='BUY', signal_expiry_date=None, quantity_multiplier=1):
        if timestamp is None:
            timestamp = datetime.datetime.now()

        # Check if we are already in this trade (e.g. immediate setup before broker confirmation)
        existing_pos = self.call_position if direction == "CALL" else self.put_position
        is_update = bool(existing_pos and existing_pos.get('instrument_key') == instrument_key)

        if is_update:
            # Update existing dictionary to preserve metadata (like order_sent, last_mgmt_check_candle)
            update_data = {
                "instrument_key": instrument_key,
                "instrument_symbol": instrument_symbol,
                "entry_timestamp": timestamp,
                "strike_price": strike_price,
                "direction": direction,
                "s1_fast_tf": s1_fast_tf,
                "s1_slow_tf": s1_slow_tf,
                "entry_type": entry_type,
                "signal_expiry_date": signal_expiry_date,
                "quantity_multiplier": quantity_multiplier
            }
            if ltp and ltp > 0:
                update_data["entry_price"] = ltp
                update_data["ltp"] = ltp

            if signal_strike is not None:
                update_data["signal_strike"] = signal_strike
                update_data["exit_monitoring_strike"] = signal_strike
                update_data["s1_monitoring_strike"] = signal_strike

            existing_pos.update(update_data)
            existing_pos['pnl'] = self._calculate_pnl(existing_pos)
            return existing_pos
        else:
            position = {
                "instrument_key": instrument_key,
                "instrument_symbol": instrument_symbol,
                "entry_price": ltp,
                "ltp": ltp,
                "peak_price": ltp,
                "min_price": ltp,
                "vwap_peak": None,
                "vwap_trough": None,
                "entry_timestamp": timestamp,
                "pnl": 0.0,
                "strike_price": strike_price,
                "signal_strike": signal_strike,
                "signal_expiry_date": signal_expiry_date,
                "direction": direction,
                "entry_type": entry_type,
                "quantity_multiplier": quantity_multiplier,
                "exit_monitoring_strike": signal_strike,
                "s1_monitoring_strike": signal_strike,
                "s1_fast_tf": s1_fast_tf,
                "s1_slow_tf": s1_slow_tf,
                "order_sent": False,
                "exit_sent": False,
                "s1_double_drop_triggered": False,
                "pattern_exit_triggered": False,
                "slope_info": "Initializing..."
            }
            if direction == "CALL":
                self.call_position = position
                self.current_trade = position
                # Fresh start for monotonic tracker
                ce_keys = [k for k in self.s1_monotonic_tracker.keys() if '_CE_' in k]
                for k in ce_keys:
                    del self.s1_monotonic_tracker[k]
                    logger.debug(f"V2: Cleared monotonic S1 tracker for {k} on trade entry.")
            elif direction == "PUT":
                self.put_position = position
                self.current_trade = position
                # Fresh start for monotonic tracker
                pe_keys = [k for k in self.s1_monotonic_tracker.keys() if '_PE_' in k]
                for k in pe_keys:
                    del self.s1_monotonic_tracker[k]
                    logger.debug(f"V2: Cleared monotonic S1 tracker for {k} on trade entry.")

            self.trade_count += 1
            self.last_trade_pnl = 0.0 # Reset last PNL when a new trade is entered
            await self.save_state()
            return position

    def is_in_trade(self, direction):
        """Checks if a trade is active for a specific direction."""
        if direction == "CALL":
            return bool(self.call_position)
        elif direction == "PUT":
            return bool(self.put_position)
        return False

    def is_in_any_trade(self):
        """Checks if any trade is currently active."""
        return bool(self.call_position or self.put_position)

    def _calculate_pnl(self, position):
        """Calculates PNL based on points, quantity, and lot size. Mode-aware (Long/Short)."""
        if not position or 'ltp' not in position or 'entry_price' not in position:
            return 0.0
        
        entry_type = position.get('entry_type', 'BUY')
        multiplier = position.get('quantity_multiplier', 1)

        if entry_type == 'SELL':
            # Short position: Profit when price goes down
            point_pnl = position['entry_price'] - position['ltp']
        else:
            # Long position: Profit when price goes up
            point_pnl = position['ltp'] - position['entry_price']

        return point_pnl * self.quantity * self.lot_size * multiplier

    def update_pnl_for_tick(self, instrument_key, ltp):
        """
        Updates the P&L for an active position based on a new LTP tick.
        """
        if self.call_position and instrument_key == self.call_position.get('instrument_key'):
            self.call_position['ltp'] = ltp
            self.call_position['pnl'] = self._calculate_pnl(self.call_position)

        if self.put_position and instrument_key == self.put_position.get('instrument_key'):
            self.put_position['ltp'] = ltp
            self.put_position['pnl'] = self._calculate_pnl(self.put_position)

    async def save_state(self):
        """Saves the critical state to a JSON file and Redis if enabled."""
        state_to_save = {
            'call_position': self.call_position,
            'put_position': self.put_position,
            'total_pnl': self.total_pnl,
            'trade_count': self.trade_count,
            'dual_sr_monitoring_data': self.dual_sr_monitoring_data,
            'v2_monitoring_atm_strike': self.v2_monitoring_atm_strike,
            's1_monotonic_tracker': self.s1_monotonic_tracker
        }

        # 1. Save to JSON (Local Fallback)
        try:
            # Ensure config directory exists
            os.makedirs(os.path.dirname(self.state_file_path), exist_ok=True)
            with open(self.state_file_path, 'w') as f:
                json.dump(state_to_save, f, indent=4, default=str) # Use default=str for datetimes
            logger.debug(f"Successfully saved state to {self.state_file_path}")
        except IOError as e:
            logger.error(f"Failed to save state to {self.state_file_path}: {e}")

        # 2. Save to Redis
        if self.redis_manager and self.redis_manager.client:
            try:
                # Use a Hash to store user state
                await self.redis_manager.client.hset(
                    f"{self.redis_key_prefix}:state",
                    mapping={
                        'call_position': json.dumps(self.call_position, default=str),
                        'put_position': json.dumps(self.put_position, default=str),
                        'total_pnl': str(self.total_pnl),
                        'trade_count': str(self.trade_count)
                    }
                )
                logger.debug(f"Successfully saved state to Redis for {self.redis_key_prefix}")
            except Exception as e:
                logger.error(f"Failed to save state to Redis: {e}")

    async def load_state(self):
        """Loads the state from Redis (priority) or JSON file."""
        # BACKTEST ISOLATION: Always start with a clean slate in backtest mode
        if self.is_backtest:
            logger.info("StateManager: Backtest mode detected. Starting with a fresh state (skipping load).")
            # Ensure any previous backtest state file is cleared if it exists to prevent accidental bleed
            if os.path.exists(self.state_file_path):
                try:
                    os.remove(self.state_file_path)
                    logger.debug(f"Cleared previous backtest state file: {self.state_file_path}")
                except Exception as e:
                    logger.error(f"Failed to clear backtest state file: {e}")
            return

        loaded_state = None

        # 1. Try loading from Redis
        if self.redis_manager and self.redis_manager.client:
            try:
                redis_data = await self.redis_manager.client.hgetall(f"{self.redis_key_prefix}:state")
                if redis_data:
                    loaded_state = {
                        'call_position': json.loads(redis_data.get('call_position', '{}')),
                        'put_position': json.loads(redis_data.get('put_position', '{}')),
                        'total_pnl': float(redis_data.get('total_pnl', 0.0)),
                        'trade_count': int(redis_data.get('trade_count', 0))
                    }
                    logger.info(f"Successfully loaded state from Redis for {self.redis_key_prefix}")
            except Exception as e:
                logger.error(f"Failed to load state from Redis: {e}")

        # 2. Fallback to JSON
        if not loaded_state:
            if not os.path.exists(self.state_file_path):
                logger.info("No saved state file found. Starting with a fresh state.")
                return

            try:
                with open(self.state_file_path, 'r') as f:
                    loaded_state = json.load(f)
                logger.info(f"Successfully loaded state from {self.state_file_path}")
            except (IOError, json.JSONDecodeError) as e:
                logger.error(f"Failed to load state file {self.state_file_path}: {e}")

        if loaded_state:
            self.call_position = loaded_state.get('call_position', {})
            self.put_position = loaded_state.get('put_position', {})
            self.total_pnl = loaded_state.get('total_pnl', 0.0)
            self.trade_count = loaded_state.get('trade_count', 0)
            self.dual_sr_monitoring_data = loaded_state.get('dual_sr_monitoring_data')
            self.v2_monitoring_atm_strike = loaded_state.get('v2_monitoring_atm_strike')
            self.s1_monotonic_tracker = loaded_state.get('s1_monotonic_tracker', {})
            
            # Convert timestamp strings back to datetime objects in positions
            for pos in [self.call_position, self.put_position]:
                if pos and isinstance(pos, dict):
                    timestamp_keys = ['entry_timestamp', 'entry_time', 'last_s1_refresh_ts', 'last_exit_check_candle', 'r1_last_above_time', '_vwap_close_last_candle']
                    # Also convert dynamic TF activation timestamps
                    for key in list(pos.keys()):
                        if key in timestamp_keys or key.endswith('_activation'):
                            if isinstance(pos[key], str):
                                try:
                                    # Use pd.to_datetime for better robust parsing
                                    ts = pd.to_datetime(pos[key])
                                    if ts.tzinfo is None:
                                        ts = ts.tz_localize('Asia/Kolkata')
                                    else:
                                        ts = ts.tz_convert('Asia/Kolkata')
                                    pos[key] = ts
                                except Exception:
                                    pass

                    # Convert signal_expiry_date from JSON string back to datetime.date.
                    # contract_lookup keys are datetime.date objects; a string never matches,
                    # causing find_instrument_key_by_strike to return None on every lookup.
                    if 'signal_expiry_date' in pos and isinstance(pos['signal_expiry_date'], str):
                        try:
                            import datetime as _dt
                            pos['signal_expiry_date'] = _dt.date.fromisoformat(pos['signal_expiry_date'])
                        except Exception:
                            pass

            # Convert timestamps in monitoring data
            if self.dual_sr_monitoring_data:
                for key in ['monitoring_start_time', 'trigger_time', 'last_exit_time']:
                    if key in self.dual_sr_monitoring_data and isinstance(self.dual_sr_monitoring_data[key], str):
                        try:
                            ts = pd.to_datetime(self.dual_sr_monitoring_data[key])
                            if ts.tzinfo is None: ts = ts.tz_localize('Asia/Kolkata')
                            else: ts = ts.tz_convert('Asia/Kolkata')
                            self.dual_sr_monitoring_data[key] = ts
                        except: pass

                for side in ['ce_data', 'pe_data']:
                    side_data = self.dual_sr_monitoring_data.get(side)
                    if side_data and isinstance(side_data, dict):
                        for k in ['last_checked_candle', 'last_checked_timestamp']:
                            if k in side_data and isinstance(side_data[k], str):
                                try:
                                    ts = pd.to_datetime(side_data[k])
                                    if ts.tzinfo is None: ts = ts.tz_localize('Asia/Kolkata')
                                    else: ts = ts.tz_convert('Asia/Kolkata')
                                    side_data[k] = ts
                                except: pass

    def is_initial_data_complete(self):
        """
        Checks if the baseline data (LTP and Delta) has been received for all
        initially subscribed instruments.
        """
        if not self.initial_instrument_keys:
            return False

        for key in self.initial_instrument_keys:
            if key not in self.option_prices or self.option_deltas.get(key) is None:
                return False
        return True

    async def get_ltp_for_instrument(self, instrument_key, timeout=5):
        """
        Waits for the LTP of a specific instrument to become available.
        Returns the LTP or None if it's not available within the timeout.
        """
        start_time = datetime.datetime.now()
        while datetime.datetime.now() - start_time < datetime.timedelta(seconds=timeout):
            ltp = self.option_prices.get(instrument_key)
            if ltp is not None:
                return ltp
            await asyncio.sleep(0.1)
        return None

    def get_last_tick_timestamp(self):
        """Returns the timestamp of the most recent tick."""
        if not self.last_tick_times:
            return None
        return max(self.last_tick_times.values())

    def get_ltp(self, instrument_key: str):
        """
        A simple, synchronous getter for the last known LTP.
        Works for both live and backtest modes as option_prices is the source of truth for LTP by key.
        """
        return self.option_prices.get(instrument_key)


    def get_position(self, side):
        """Returns the active position dictionary for a given side ('CALL' or 'PUT')."""
        if side.upper() == 'CALL':
            return self.call_position
        elif side.upper() == 'PUT':
            return self.put_position
        return None

    def clear_all_state(self):
        """Clears all market data and positions for a fresh start."""
        self.option_prices.clear()
        self.option_atps.clear()
        self.option_deltas.clear()
        self.option_data.clear()
        self.option_gammas.clear()
        self.option_thetas.clear()
        self.option_ivs.clear()
        self.option_vegas.clear()
        self.option_volumes.clear()
        self.option_bid_ask.clear()
        self.last_tick_times.clear()
        self.call_position.clear()
        self.put_position.clear()
        self.current_trade.clear()
        self.dual_sr_monitoring_data.clear()
        self.s1_monotonic_tracker.clear()
        self.total_pnl = 0.0
        self.trade_count = 0
        self.last_trade_pnl = 0.0
        self.call_cooldown_until = None
        self.put_cooldown_until = None
        self.exit_signals = {'CALL': [], 'PUT': []}
