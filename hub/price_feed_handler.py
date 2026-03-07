import asyncio
import datetime
import pytz
from collections import deque
from utils.logger import logger
from hub.event_bus import event_bus

class PriceFeedHandler:
    def __init__(self, state_manager, atm_manager, trade_orchestrator, entry_aggregator, exit_aggregator, one_min_aggregator, five_min_aggregator):
        self.state_manager = state_manager
        self.atm_manager = atm_manager
        self.trade_orchestrator = trade_orchestrator
        self.entry_aggregator = entry_aggregator
        self.exit_aggregator = exit_aggregator
        self.one_min_aggregator = one_min_aggregator
        self.five_min_aggregator = five_min_aggregator
        self._last_process_tick_time = 0

        # Dedicated worker for tick processing
        self._tick_event = asyncio.Event()
        self._worker_task = asyncio.create_task(self._tick_worker())
        self._cache_rebuild_task = asyncio.create_task(self._keys_rebuild_loop())

        self._relevant_keys_cache = set()
        self._last_keys_rebuild_time = 0
        self._kolkata_tz = pytz.timezone('Asia/Kolkata')

        # Get the keys for differentiation directly from the orchestrator
        # to ensure consistency with initial subscriptions.
        self.index_instrument_key = self.trade_orchestrator.index_instrument_key
        self.futures_instrument_key = self.trade_orchestrator.futures_instrument_key

    def _extract_timestamp(self, feed, fallback_ts):
        """Extracts the best available exchange timestamp (LTT) from a feed."""
        ltt_ms = 0
        if feed.HasField('ltpc'):
            ltt_ms = feed.ltpc.ltt
        elif feed.HasField('fullFeed'):
            if feed.fullFeed.HasField('indexFF'):
                ltt_ms = feed.fullFeed.indexFF.ltpc.ltt
            elif feed.fullFeed.HasField('marketFF'):
                ltt_ms = feed.fullFeed.marketFF.ltpc.ltt
        elif feed.HasField('firstLevelWithGreeks'):
             ltt_ms = feed.firstLevelWithGreeks.ltpc.ltt

        if ltt_ms > 0:
            # Upstox V3 uses milliseconds for currentTs and LTT
            ts_s = ltt_ms / 1000.0 if ltt_ms > 10**11 else ltt_ms
            return datetime.datetime.fromtimestamp(ts_s, tz=self._kolkata_tz)

        return fallback_ts

    async def handle_message(self, feed_response):
        """
        This is the entry point for all websocket messages.
        It parses the message and routes the data to the correct handler.
        """
        # --- EXCHANGE TIMESTAMP EXTRACTION ---
        # Upstox currentTs is in milliseconds since epoch.
        # We use this as the single source of truth for candle aggregation.
        packet_now = None
        if hasattr(feed_response, 'currentTs') and feed_response.currentTs > 0:
            ts_s = feed_response.currentTs / 1000.0 if feed_response.currentTs > 10**11 else feed_response.currentTs
            packet_now = datetime.datetime.fromtimestamp(ts_s, tz=self._kolkata_tz)
            # Store the latest exchange time in state for other components to access
            self.state_manager.last_exchange_time = packet_now
        else:
            packet_now = self.trade_orchestrator._get_timestamp()

        if not feed_response.feeds:
            return

        # Optimization: Use a cached set of relevant keys to skip irrelevant instruments quickly.
        # The cache is rebuilt in a background task to keep handle_message lightning fast.
        if not self._relevant_keys_cache:
            self._rebuild_relevant_keys()

        tasks = []
        for instrument_key, feed in feed_response.feeds.items():
            # FAST PATH: Skip if the instrument is not relevant to this orchestrator
            is_underlying = instrument_key in [self.futures_instrument_key, self.index_instrument_key]
            if not is_underlying and instrument_key not in self._relevant_keys_cache:
                continue

            # Prioritize individual Tick LTT over Packet Timestamp
            now = self._extract_timestamp(feed, packet_now)

            if instrument_key == self.futures_instrument_key:
                tasks.append(self._handle_spot_feed(instrument_key, feed, now, is_futures=True))
            elif instrument_key == self.index_instrument_key:
                tasks.append(self._handle_spot_feed(instrument_key, feed, now, is_futures=False))
            else:
                tasks.append(self._handle_option_feed(instrument_key, feed, now))

        if tasks:
            await asyncio.gather(*tasks)

    def _rebuild_relevant_keys(self):
        """Rebuilds the cache of instrument keys this orchestrator cares about."""
        try:
            relevant_keys = {self.futures_instrument_key, self.index_instrument_key}

            for strike_info in self.atm_manager.contracts.values():
                for opt_type in ['CE', 'PE']:
                    contract = strike_info.get(opt_type, {})
                    key = contract.get('key') if isinstance(contract, dict) else getattr(contract, 'instrument_key', None)
                    if key: relevant_keys.add(key)

            # Multi-tenant: Check all user sessions for active trades or monitoring
            for session in self.trade_orchestrator.user_sessions.values():
                sm = session.state_manager
                expiry = self.atm_manager.signal_expiry_date

                for pos in [sm.call_position, sm.put_position]:
                    if pos:
                        relevant_keys.add(pos.get('instrument_key'))
                        # MANDATORY: Add instruments for S1LOW monitoring strike
                        sl_strike = pos.get('s1_monitoring_strike')
                        if sl_strike and expiry:
                            relevant_keys.add(self.atm_manager.find_instrument_key_by_strike(sl_strike, 'CALL', expiry))
                            relevant_keys.add(self.atm_manager.find_instrument_key_by_strike(sl_strike, 'PUT', expiry))

                # IMPORTANT: Include crossover monitoring instruments
                monitoring_data = sm.dual_sr_monitoring_data
                if monitoring_data:
                    for side_key in ['ce_data', 'pe_data']:
                        side_data = monitoring_data.get(side_key)
                        if side_data and side_data.get('instrument_key'):
                            relevant_keys.add(side_data['instrument_key'])

            # Legacy/Global check
            global_monitoring = self.state_manager.dual_sr_monitoring_data
            if global_monitoring:
                for side_key in ['ce_data', 'pe_data']:
                    side_data = global_monitoring.get(side_key)
                    if side_data and side_data.get('instrument_key'):
                        relevant_keys.add(side_data['instrument_key'])

            # Include SellManager strangle keys (SELL legs + HEDGE legs)
            if hasattr(self.trade_orchestrator, 'sell_manager'):
                sell_mgr = self.trade_orchestrator.sell_manager
                for k in [sell_mgr.sell_ce_key, sell_mgr.sell_pe_key, sell_mgr.buy_ce_key, sell_mgr.buy_pe_key]:
                    if k:
                        relevant_keys.add(k)
                for _, k in (sell_mgr.ce_candidates or []):
                    if k: relevant_keys.add(k)
                for _, k in (sell_mgr.pe_candidates or []):
                    if k: relevant_keys.add(k)

            self._relevant_keys_cache = relevant_keys
            self._last_keys_rebuild_time = asyncio.get_event_loop().time()
        except Exception as e:
            logger.error(f"Error rebuilding keys cache: {e}")

    async def _keys_rebuild_loop(self):
        """Periodically rebuilds the keys cache to account for strike changes or new trades."""
        while True:
            try:
                self._rebuild_relevant_keys()
                await asyncio.sleep(10) # Every 10 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in keys rebuild loop: {e}")
                await asyncio.sleep(5)

    def _sync_market_data(self, attr_name, key, value, is_primitive=False):
        """Synchronizes market data updates across all active user sessions."""
        if not self.trade_orchestrator.user_sessions:
            return

        for session in self.trade_orchestrator.user_sessions.values():
            if is_primitive:
                setattr(session.state_manager, attr_name, value)
            else:
                # Defensive check for missing attributes in session state manager
                if not hasattr(session.state_manager, attr_name):
                    setattr(session.state_manager, attr_name, {})

                target_dict = getattr(session.state_manager, attr_name)
                target_dict[key] = value

        # logger.debug(f"Synced {attr_name} to {len(self.trade_orchestrator.user_sessions)} sessions.")

    async def _handle_spot_feed(self, instrument_key, feed, timestamp, is_futures):
        """Handles feed data for the spot (index) or futures instrument."""
        ltp = None

        # 1. Check for Full Feed
        if feed.HasField('fullFeed'):
            if feed.fullFeed.HasField('indexFF'):
                ff = feed.fullFeed.indexFF
                if ff and ff.ltpc and ff.ltpc.ltp > 0:
                    ltp = ff.ltpc.ltp
            elif feed.fullFeed.HasField('marketFF'):
                ff = feed.fullFeed.marketFF
                if ff and ff.ltpc and ff.ltpc.ltp > 0:
                    ltp = ff.ltpc.ltp

        # 2. Check for top-level LTPC (Direct fallback)
        if ltp is None and feed.HasField('ltpc'):
            if feed.ltpc.ltp > 0:
                ltp = feed.ltpc.ltp

        if ltp is not None:
            # logger.debug(f"[{self.trade_orchestrator.instrument_name}] Spot update for {instrument_key}: {ltp}")
            self.entry_aggregator.add_tick(instrument_key, ltp, timestamp)
            self.exit_aggregator.add_tick(instrument_key, ltp, timestamp)
            self.one_min_aggregator.add_tick(instrument_key, ltp, timestamp)
            self.five_min_aggregator.add_tick(instrument_key, ltp, timestamp)

            if is_futures:
                if self.state_manager.spot_price is None:
                    logger.info(f"V2: First FUTURES price received for {self.trade_orchestrator.instrument_name}: {ltp}")

                # This is the futures price. Used ONLY for target strike selection (BUY activation).
                self.state_manager.spot_price = ltp
                self._sync_market_data('spot_price', None, ltp, is_primitive=True)

                # Also trigger subscription update for futures-based target strikes
                await event_bus.publish('SPOT_PRICE_UPDATE', {
                    'instrument': self.trade_orchestrator.instrument_name,
                    'ltp': ltp,
                    'is_futures': True
                })

            else:
                if self.state_manager.index_price is None:
                    logger.info(f"V2: First INDEX price (Spot) received for {self.trade_orchestrator.instrument_name}: {ltp}")

                # This is the index price (Spot). Used for ATM, OI monitoring, and ITM selection.
                self.state_manager.index_price = ltp
                self._sync_market_data('index_price', None, ltp, is_primitive=True)

                # Trigger ATM recalculation and resubscription based on INDEX SPOT as requested.
                await event_bus.publish('SPOT_PRICE_UPDATE', {
                    'instrument': self.trade_orchestrator.instrument_name,
                    'ltp': ltp,
                    'is_futures': False
                })

            # Always signal the tick worker to check for strike changes/breaches on any underlying movement
            self._tick_event.set()

    async def _handle_option_feed(self, instrument_key, feed, timestamp):
        """Handles feed data for a single option instrument."""
        # Use the cached relevance set to ensure all needed strikes (ATM, Active Trade, Monitored)
        # are processed and added to aggregators.
        if instrument_key in self._relevant_keys_cache:
            self.state_manager.last_tick_times[instrument_key] = timestamp
            self._sync_market_data('last_tick_times', instrument_key, timestamp)

            greeks, ltp, iv, volume, market_level, atp, oi_value = None, None, None, None, None, None, 0

            if feed.HasField('fullFeed'):
                ff = feed.fullFeed.marketFF
                greeks, ltp, iv, volume, market_level, atp = ff.optionGreeks, ff.ltpc.ltp, ff.iv, ff.vtt, ff.marketLevel, ff.atp
                oi_value = getattr(ff, 'oi', 0) or 0

                # Extract day's OHLC for recording/monitoring
                if ff.ltpc:
                    if instrument_key not in self.state_manager.option_data:
                        self.state_manager.option_data[instrument_key] = {}

                    self.state_manager.option_data[instrument_key].update({
                        'open': ff.ltpc.o,
                        'high': ff.ltpc.h,
                        'low': ff.ltpc.l,
                        'close': ff.ltpc.cp # Previous close or current? Usually prev.
                    })
                    self._sync_market_data('option_data', instrument_key, self.state_manager.option_data[instrument_key])

            elif feed.HasField('ltpc'):
                ltp = feed.ltpc.ltp

            # --- Update StateManager ---
            if ltp and ltp > 0:
                self.state_manager.option_prices[instrument_key] = ltp
                self._sync_market_data('option_prices', instrument_key, ltp)

                if atp and atp > 0:
                    if not hasattr(self.state_manager, 'option_atps'):
                        self.state_manager.option_atps = {}
                    self.state_manager.option_atps[instrument_key] = atp
                    self._sync_market_data('option_atps', instrument_key, atp)

                    minute_ts = timestamp.replace(second=0, microsecond=0) \
                        if hasattr(timestamp, 'replace') else None
                    if minute_ts:
                        if not hasattr(self.state_manager, 'atp_history'):
                            self.state_manager.atp_history = {}
                        if instrument_key not in self.state_manager.atp_history:
                            self.state_manager.atp_history[instrument_key] = {}
                        prev_minute_ts = self.state_manager.atp_history[instrument_key].get('_last_minute')
                        self.state_manager.atp_history[instrument_key][minute_ts] = atp

                        if prev_minute_ts and prev_minute_ts != minute_ts:
                            prev_atp = self.state_manager.atp_history[instrument_key].get(prev_minute_ts)
                            data_recorder = getattr(self.trade_orchestrator, 'data_recorder', None)
                            if data_recorder and prev_atp:
                                atm_mgr = getattr(self.trade_orchestrator, 'atm_manager', None)
                                strike, side = None, None
                                if atm_mgr:
                                    contract = getattr(atm_mgr, 'contracts', {}).get(instrument_key)
                                    if contract:
                                        strike = getattr(contract, 'strike_price', None)
                                        side = 'CE' if getattr(contract, 'instrument_type', '') in ('CE',) else 'PE'
                                spot = getattr(self.state_manager, 'index_price', None)
                                futures = getattr(self.state_manager, 'futures_price', None)
                                data_recorder.record_atp_snapshot(
                                    prev_minute_ts, instrument_key, strike, side,
                                    prev_atp, ltp, spot, futures,
                                    oi=self.state_manager.option_oi.get(instrument_key)
                                )

                        self.state_manager.atp_history[instrument_key]['_last_minute'] = minute_ts
            

            # --- Throttle delta updates to once per 60 seconds per instrument ---
            if greeks:
                now = timestamp if isinstance(timestamp, datetime.datetime) else datetime.datetime.now()
                last_delta_update = getattr(self.state_manager, 'last_delta_update', {})
                if not hasattr(self.state_manager, 'last_delta_update'):
                    self.state_manager.last_delta_update = {}
                    last_delta_update = self.state_manager.last_delta_update
                last_time = last_delta_update.get(instrument_key)

                if greeks.delta != 0.0:
                    if last_time is None or (now - last_time).total_seconds() >= 60:
                        self.state_manager.option_deltas[instrument_key] = greeks.delta
                        self._sync_market_data('option_deltas', instrument_key, greeks.delta)
                        last_delta_update[instrument_key] = now

                if greeks.gamma != 0.0:
                    self.state_manager.option_gammas[instrument_key] = greeks.gamma
                    self._sync_market_data('option_gammas', instrument_key, greeks.gamma)
                if greeks.theta != 0.0:
                    self.state_manager.option_thetas[instrument_key] = greeks.theta
                    self._sync_market_data('option_thetas', instrument_key, greeks.theta)
                if greeks.vega != 0.0:
                    self.state_manager.option_vegas[instrument_key] = greeks.vega
                    self._sync_market_data('option_vegas', instrument_key, greeks.vega)

            if iv is not None and iv != 0.0:
                self.state_manager.option_ivs[instrument_key] = iv
                self._sync_market_data('option_ivs', instrument_key, iv)

            # --- Store OI unconditionally (always, regardless of oi_exit enabled flag) ---
            if oi_value and oi_value > 0:
                self.state_manager.option_oi[instrument_key] = oi_value
                self._sync_market_data('option_oi', instrument_key, oi_value)

            # Volume and incremental volume calculation
            volume_inc = 0
            if volume is not None:
                prev_volume = self.state_manager.option_volumes.get(instrument_key, 0)
                if volume > prev_volume and prev_volume > 0:
                    volume_inc = volume - prev_volume
                
                self.state_manager.option_volumes[instrument_key] = volume
                self._sync_market_data('option_volumes', instrument_key, volume)

            # --- Update Aggregators with LTP and Volume Increment ---
            if ltp and ltp > 0:
                # Use a dict for kwargs to be safer across potential version mismatches
                tick_kwargs = {'volume_inc': volume_inc}
                try:
                    self.entry_aggregator.add_tick(instrument_key, ltp, timestamp, **tick_kwargs)
                    self.exit_aggregator.add_tick(instrument_key, ltp, timestamp, **tick_kwargs)
                    self.one_min_aggregator.add_tick(instrument_key, ltp, timestamp, **tick_kwargs)
                    self.five_min_aggregator.add_tick(instrument_key, ltp, timestamp, **tick_kwargs)
                except TypeError:
                    # Fallback for old version of add_tick if restart hasn't happened
                    self.entry_aggregator.add_tick(instrument_key, ltp, timestamp)
                    self.exit_aggregator.add_tick(instrument_key, ltp, timestamp)
                    self.one_min_aggregator.add_tick(instrument_key, ltp, timestamp)
                    self.five_min_aggregator.add_tick(instrument_key, ltp, timestamp)

            if market_level and market_level.bidAskQuote:
                top_quote = market_level.bidAskQuote[0]
                quote_data = {'bid': top_quote.bidP, 'ask': top_quote.askP}
                self.state_manager.option_bid_ask[instrument_key] = quote_data
                self._sync_market_data('option_bid_ask', instrument_key, quote_data)

            if ltp and ltp > 0:
                self.state_manager.update_pnl_for_tick(instrument_key, ltp)
                # Update PNL for all user sessions too
                for session in self.trade_orchestrator.user_sessions.values():
                    session.state_manager.update_pnl_for_tick(instrument_key, ltp)

            # --- Check for startup or exit conditions ---
            if not self.atm_manager.initial_data_received.is_set():
                await self._check_and_set_initial_data_event()

            # After any relevant option tick, trigger the main processing logic.
            # We signal the background worker to run process_tick.
            self._tick_event.set()

    async def _tick_worker(self):
        """Dedicated background task to run process_tick one at a time."""
        logger.info(f"Tick worker started for {self.trade_orchestrator.instrument_name}.")
        while True:
            await self._tick_event.wait()
            self._tick_event.clear()

            # PROTECT: Ensure orchestrator is fully initialized before processing
            if not getattr(self.trade_orchestrator, 'tick_processor', None):
                continue

            try:
                # THROTTLE: Minimum delay between runs to prevent event loop saturation
                now_ts = asyncio.get_event_loop().time()
                elapsed = now_ts - self._last_process_tick_time

                if elapsed < 0.2: # 200ms min interval
                    await asyncio.sleep(0.2 - elapsed)

                self._last_process_tick_time = asyncio.get_event_loop().time()

                # Protect the entire tick processing flow with a 60s timeout
                try:
                    await asyncio.wait_for(self.trade_orchestrator.process_tick(), timeout=60.0)
                except asyncio.TimeoutError:
                    logger.error(f"V2: FATAL - Tick processing HANG detected for {self.trade_orchestrator.instrument_name}. Loop timed out after 60s.")
                except Exception as e:
                    logger.error(f"V2: Error during tick processing for {self.trade_orchestrator.instrument_name}: {e}", exc_info=True)

            except Exception as e:
                logger.error(f"Error in tick worker loop for {self.trade_orchestrator.instrument_name}: {e}", exc_info=True)

    def _log_missing_initial_data(self):
        """Logs which instruments are still missing baseline data."""
        missing_ltp, missing_delta = [], []
        for key in self.state_manager.initial_instrument_keys:
            if key not in self.state_manager.option_prices: missing_ltp.append(key)
            if self.state_manager.option_deltas.get(key) is None: missing_delta.append(key)
        
        # if missing_ltp or missing_delta:
        #     logger.debug(f"Waiting for baseline data. Missing LTP for: {missing_ltp}. Missing Delta for: {missing_delta}.")

    async def _check_and_set_initial_data_event(self):
        """
        Checks if the baseline data (LTP and Delta) for all initial instruments
        has been received. If so, it sets and publishes the INITIAL_DATA_COMPLETE event.
        """
        if self.state_manager.is_initial_data_complete():
            if not self.atm_manager.initial_data_received.is_set():
                logger.info("All initial instrument data, including deltas, has been received. Publishing event.")
                self.atm_manager.initial_data_received.set()
                await event_bus.publish('INITIAL_DATA_COMPLETE')
        else:
            self._log_missing_initial_data()
