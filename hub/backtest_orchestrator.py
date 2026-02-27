from .base_orchestrator import BaseOrchestrator
from utils.logger import logger
import pandas as pd
from .backtest_data_manager import BacktestDataManager

class BacktestOrchestrator(BaseOrchestrator):
    def __init__(self, *args, **kwargs):
        kwargs['is_backtest'] = True
        super().__init__(*args, **kwargs)
        self.current_timestamp = None
        self.index_instrument_key = self.config_manager.get(self.instrument_name, 'instrument_symbol')
        self.backtest_data_mgr = BacktestDataManager(self)
        from hub.sell_manager import SellManager
        self.sell_manager = SellManager(self)
        self._backtest_strangle_triggered = False
        self.profit_target_hit = False

    async def prepare_backtest(self):
        """Pre-fetches all necessary data before starting the backtest."""
        self.json_config.load()
        if hasattr(self.websocket, 'backtest_date') and self.websocket.backtest_date:
            backtest_date = self.websocket.backtest_date
            backtest_date_str = backtest_date.strftime('%Y-%m-%d')
            await self.data_manager.load_contracts()
            self.atm_manager.all_contracts = self.data_manager.all_options
            self.atm_manager.near_expiry_date = self.data_manager.near_expiry_date
            self.atm_manager.monthly_expiries = self.data_manager.monthly_expiries
            self.atm_manager._build_contract_lookup_table()
            self.atm_manager.set_ready()
            self.atm_manager._determine_expiries(backtest_date)
            await self.backtest_data_mgr.pre_fetch_underlying_data(backtest_date_str)
        else:
            logger.error("Backtest date not found. Cannot pre-fetch data.")

    def _get_timestamp(self):
        return self.current_timestamp

    def _is_trade_active(self):
        return self.pnl_tracker and self.pnl_tracker.is_trade_active()

    async def run_backtest_strategy_for_timestamp(self, timestamp, current_group):
        self.current_timestamp = timestamp
        # 1. Populate market state and ATM/Strike watchlist (anchored to Futures)
        await self._populate_state_for_tick(timestamp, current_group)

        # 2. Execute short strangle if time (anchored to Spot inside sell_manager)
        import datetime as _dt
        _strangle_start = _dt.datetime.strptime(
            self.config_manager.get('settings', 'strangle_start_time', fallback='09:15:00'),
            '%H:%M:%S'
        ).time()
        if not self._backtest_strangle_triggered and timestamp.time() >= _strangle_start:
            await self.sell_manager.execute_short_strangle(timestamp)
            self._backtest_strangle_triggered = True

        # 3. Find and sync target crossover strike (anchored to Futures)
        self.orchestrator_state.v2_target_strike_pair = self.strike_manager.find_and_get_target_strike_pair(
            expiry=self.atm_manager.signal_expiry_date
        )
        if self.orchestrator_state.v2_target_strike_pair:
             target_strike = float(self.orchestrator_state.v2_target_strike_pair['strike'])
             self.state_manager.target_strike = target_strike
             for session in self.user_sessions.values():
                 session.state_manager.target_strike = target_strike

        # 4. Feeding aggregators
        aggregators = [self.entry_aggregator, self.exit_aggregator, self.one_min_aggregator, self.five_min_aggregator]
        if self.index_instrument_key and self.state_manager.index_price:
            for agg in aggregators: agg.add_tick(self.index_instrument_key, self.state_manager.index_price, timestamp)
        if self.futures_instrument_key and self.state_manager.spot_price:
            for agg in aggregators: agg.add_tick(self.futures_instrument_key, self.state_manager.spot_price, timestamp)

        keys_needing_ticks = self._get_keys_needing_ticks()
        for inst_key in keys_needing_ticks:
            ohlc_df = await self.data_manager.get_historical_ohlc(inst_key, 1, current_timestamp=timestamp, for_full_day=True, include_current=True)
            if ohlc_df is not None and timestamp in ohlc_df.index:
                candle = ohlc_df.loc[timestamp]
                # To avoid look-ahead bias, we only feed the 'open' price to represent the tick at exactly the timestamp.
                # In live market, at exactly 09:18:00, we don't know the High/Low/Close of the 09:18 minute yet.
                price = float(candle['open'])
                v_inc = 1 # Nominal volume for the first tick
                for agg in aggregators: agg.add_tick(inst_key, price, timestamp, volume_inc=v_inc)
                self.state_manager.option_prices[inst_key] = price
            else:
                ltp = self.state_manager.option_prices.get(inst_key) or await self._get_ltp_for_backtest_instrument(inst_key, timestamp)
                if ltp:
                    self.state_manager.option_prices[inst_key] = ltp
                    for agg in aggregators: agg.add_tick(inst_key, ltp, timestamp)

        if current_group.duplicated(subset='strike_price').any():
            current_group = current_group.drop_duplicates(subset='strike_price', keep='first')

        current_data_for_logic = self.state_manager.option_data
        current_atm_fut = self.atm_manager.strikes.get('atm')

        # Calculate Spot ATM for summary log
        interval = self.config_manager.get_int(self.instrument_name, 'strike_interval', 50)
        idx_p = self.state_manager.index_price
        current_atm_spot = float(round(idx_p / interval) * interval) if idx_p else current_atm_fut

        summary_extra_info = ""
        for session in self.user_sessions.values():
            await session.signal_monitor.check_crossover_breach(timestamp=timestamp, current_atm=current_atm_fut)
            if session.is_in_trade():
                pos = session.state_manager.call_position or session.state_manager.put_position
                if self.pnl_tracker.is_trade_active(): await self._update_pnl_for_active_trades(timestamp)
                await session.manage_active_trades(timestamp=timestamp, current_ticks=current_data_for_logic, current_atm=current_atm_fut)
                if pos and not summary_extra_info:
                    summary_extra_info = self._get_summary_extra(pos)

        from utils.logger import log_trade_summary
        pnl = self.pnl_tracker.get_real_time_pnl() if self.pnl_tracker else 0

        # --- Profit target exit check ---
        if not self.profit_target_hit and self.pnl_tracker:
            pt_cfg = (self.json_config.get_value(
                f"{self.instrument_name}.profit_target_exit") or {})
            if pt_cfg.get('enabled'):
                realized = sum(t.get('pnl', 0) for t in self.pnl_tracker.trade_history)
                total_pnl = realized + pnl
                lot = self.config_manager.get_int(self.instrument_name, 'lot_size', 1)
                threshold = pt_cfg.get('points', 60) * lot
                if total_pnl >= threshold:
                    logger.info(
                        f"[{self.instrument_name}] PROFIT TARGET HIT: "
                        f"₹{total_pnl:.2f} >= ₹{threshold:.2f} "
                        f"({pt_cfg['points']} pts × lot={lot}). "
                        f"Closing all positions — done for today."
                    )
                    self.profit_target_hit = True
                    await self.close_open_backtest_positions(timestamp)

        log_trade_summary(f"Timestamp: {timestamp} | ATM(F): {current_atm_fut} | ATM(S): {current_atm_spot} | Status: {'RUNNING' if self._is_trade_active() else 'IDLE'} | P&L: {pnl:.2f}{summary_extra_info}")

    def _get_keys_needing_ticks(self):
        keys = set()
        if self.orchestrator_state.v2_target_strike_pair:
            ts = self.orchestrator_state.v2_target_strike_pair['strike']
            exp = self.atm_manager.signal_expiry_date
            for s in ['CALL', 'PUT']:
                k = self.atm_manager.find_instrument_key_by_strike(ts, s, exp)
                if k: keys.add(k)
        for session in self.user_sessions.values():
            for p in [session.state_manager.call_position, session.state_manager.put_position]:
                if p:
                    if p.get('instrument_key'): keys.add(p['instrument_key'])
                    ms = p.get('s1_monitoring_strike')
                    if ms:
                        sd = 'CE' if p.get('direction') == 'CALL' else 'PE'
                        mk = self.atm_manager.find_instrument_key_by_strike(ms, sd, self.atm_manager.signal_expiry_date)
                        if mk: keys.add(mk)
        return keys

    def _get_summary_extra(self, pos):
        info = ""
        mode = 'buy' if pos.get('entry_type') == 'BUY' else 'sell'
        try:
            exit_formula = self.json_config.get_value(f"{self.instrument_name}.{mode}.exit_formula") or ''
        except Exception:
            exit_formula = ''
        f_lower = exit_formula.lower()
        sr_indicators = ['s1_low', 'r1_high', 's1_double_drop', 'r1_falling', 'r1_low_breach', 's1_confirm']

        tgt = pos.get('current_target')
        if tgt: info += f" | TARGET: {float(tgt):.1f}"
        tf = pos.get('active_s1_tf', 'N/A')
        s1 = pos.get('active_s1', 0)
        tsl = pos.get('trailing_sl', 0)
        if s1 and any(ind in f_lower for ind in sr_indicators):
            info += f" | {pos.get('s1_label', 'S1LOW')}({tf}m): {float(s1):.1f}"
        if tsl and any(ind in f_lower for ind in ['tsl', 'atr_tsl']):
            info += f" | TSL: {float(tsl):.1f}"
        return info

    async def _check_backtest_exit_conditions(self, timestamp):
        """Deprecated in V2: Exit logic is now handled per-session in run_backtest_strategy_for_timestamp."""
        pass

    async def _update_pnl_for_active_trades(self, timestamp):
        for side in ['CALL', 'PUT']:
            trade = self.pnl_tracker.active_call_trade if side == 'CALL' else self.pnl_tracker.active_put_trade
            if trade:
                ltp = await self._get_ltp_for_backtest_instrument(trade['instrument_key'], timestamp)
                if ltp is not None:
                    self.pnl_tracker.update_pnl(side, ltp, timestamp)
                    for session in self.user_sessions.values():
                        pos = session.state_manager.call_position if side == 'CALL' else session.state_manager.put_position
                        if pos:
                            pos['ltp'] = ltp
                            pos['pnl'] = trade.get('pnl', 0)

    async def _get_ltp_for_backtest_instrument(self, instrument_key, timestamp):
        """
        Fetches the historical LTP for an instrument at a specific timestamp.
        For normal market hours (before 15:30), uses the Open price of the current minute.
        For EOD closure (15:30:00), uses the Close price of the final minute (15:29:00).
        """
        import pytz
        kolkata = pytz.timezone('Asia/Kolkata')
        ts = kolkata.localize(timestamp) if timestamp.tzinfo is None else timestamp.astimezone(kolkata)
        bucket_ts = ts.replace(second=0, microsecond=0)

        # Determine if this is EOD closure
        is_eod = ts.time() >= datetime.time(15, 30)

        ohlc_df = await self.data_manager.get_historical_ohlc(instrument_key, 1, current_timestamp=bucket_ts, for_full_day=True)
        if ohlc_df is None or ohlc_df.empty: return None
        if ohlc_df.index.tz is None: ohlc_df.index = ohlc_df.index.tz_localize('Asia/Kolkata')

        if is_eod:
            # For 15:30, we want the LAST closed price of the day
            relevant = ohlc_df[ohlc_df.index.time < datetime.time(15, 30)]
            return float(relevant.iloc[-1]['close']) if not relevant.empty else None

        if bucket_ts in ohlc_df.index:
            return float(ohlc_df.loc[bucket_ts]['open'])

        relevant = ohlc_df[ohlc_df.index < bucket_ts]
        return float(relevant.iloc[-1]['close']) if not relevant.empty else None

    async def _populate_state_for_tick(self, timestamp, tick_df):
        if tick_df.empty: return
        fut_p = tick_df['spot_price'].iloc[0] if 'spot_price' in tick_df.columns and pd.notna(tick_df['spot_price'].iloc[0]) else 0
        if not fut_p: fut_p = self.backtest_data_mgr.get_futures_price(timestamp) or 0
        idx_p = tick_df['index_price'].iloc[0] if 'index_price' in tick_df.columns and pd.notna(tick_df['index_price'].iloc[0]) else 0
        if not idx_p: idx_p = self.backtest_data_mgr.get_index_price(timestamp) or fut_p

        self.state_manager.timestamp = timestamp
        self.state_manager.spot_price = fut_p
        self.state_manager.index_price = idx_p
        self.state_manager.option_data.clear()

        # ATM and Target Strike calculation should be anchored to the Futures price
        await self.atm_manager.update_strikes_and_subscribe(fut_p)
        atm_fut = self.atm_manager.strikes.get('atm')
        self.state_manager.atm_strike = atm_fut

        watchlist = set(self.strike_manager.get_strike_watchlist(atm_fut))

        # Include sell legs in watchlist to ensure summary logs are populated
        if hasattr(self, 'sell_manager') and self.sell_manager.strangle_placed:
            if self.sell_manager.sell_ce_strike: watchlist.add(float(self.sell_manager.sell_ce_strike))
            if self.sell_manager.sell_pe_strike: watchlist.add(float(self.sell_manager.sell_pe_strike))

        # Include strikes around Spot ATM for Sell selection to prevent "Data not found"
        interval = self.config_manager.get_int(self.instrument_name, 'strike_interval', 50)
        atm_spot = float(round(idx_p / interval) * interval) if idx_p and interval else None
        if atm_spot:
            # Add range to watchlist
            for i in range(-15, 16):
                watchlist.add(float(atm_spot + i * interval))

        for session in self.user_sessions.values():
            if session.state_manager.dual_sr_monitoring_data: watchlist.add(float(session.state_manager.dual_sr_monitoring_data['target_strike']))
            for p in [session.state_manager.call_position, session.state_manager.put_position]:
                if p:
                    for k in ['strike_price', 'signal_strike', 's1_monitoring_strike', 'exit_monitoring_strike']:
                        if p.get(k): watchlist.add(float(p[k]))

        tick_df_idx = tick_df.drop_duplicates(subset='strike_price').set_index('strike_price') if 'strike_price' in tick_df.columns else pd.DataFrame()
        for strike in watchlist:
            api_ce = await self._get_ltp_for_strike(strike, 'CALL', timestamp)
            api_pe = await self._get_ltp_for_strike(strike, 'PUT', timestamp)
            s_data = tick_df_idx.loc[strike] if strike in tick_df_idx.index else pd.Series()
            ce_p = s_data.get('ce_ltp') if pd.notna(s_data.get('ce_ltp')) and s_data.get('ce_ltp') > 0 else api_ce
            pe_p = s_data.get('pe_ltp') if pd.notna(s_data.get('pe_ltp')) and s_data.get('pe_ltp') > 0 else api_pe

            self.state_manager.option_data[strike] = {
                'ce_ltp': ce_p, 'pe_ltp': pe_p,
                'ce_delta': s_data.get('ce_delta'), 'pe_delta': s_data.get('pe_delta')
            }
            exp = self.atm_manager.signal_expiry_date
            ck, pk = self.atm_manager.find_instrument_key_by_strike(strike, 'CALL', exp), self.atm_manager.find_instrument_key_by_strike(strike, 'PUT', exp)
            if ck and ce_p: self.state_manager.option_prices[ck] = ce_p
            if pk and pe_p: self.state_manager.option_prices[pk] = pe_p

            # POPULATE ATP FROM EXTERNAL CSV OR TICK DATA
            ce_atp = s_data.get('ce_atp') if pd.notna(s_data.get('ce_atp')) else self.backtest_data_mgr.get_atp(ck, timestamp)
            pe_atp = s_data.get('pe_atp') if pd.notna(s_data.get('pe_atp')) else self.backtest_data_mgr.get_atp(pk, timestamp)

            if not hasattr(self.state_manager, 'option_atps'): self.state_manager.option_atps = {}
            if ck and ce_atp:
                self.state_manager.option_atps[ck] = float(ce_atp)
                # POPULATE ATP HISTORY FOR SLOPE LOGIC
                if not hasattr(self.state_manager, 'atp_history'): self.state_manager.atp_history = {}
                if ck not in self.state_manager.atp_history: self.state_manager.atp_history[ck] = {}
                self.state_manager.atp_history[ck][timestamp.replace(second=0, microsecond=0)] = float(ce_atp)

            if pk and pe_atp:
                self.state_manager.option_atps[pk] = float(pe_atp)
                # POPULATE ATP HISTORY FOR SLOPE LOGIC
                if not hasattr(self.state_manager, 'atp_history'): self.state_manager.atp_history = {}
                if pk not in self.state_manager.atp_history: self.state_manager.atp_history[pk] = {}
                self.state_manager.atp_history[pk][timestamp.replace(second=0, microsecond=0)] = float(pe_atp)

        for session in self.user_sessions.values():
            session.state_manager.timestamp = timestamp
            session.state_manager.spot_price = fut_p
            session.state_manager.index_price = idx_p
            session.state_manager.option_prices.update(self.state_manager.option_prices)
            if not hasattr(session.state_manager, 'option_atps'): session.state_manager.option_atps = {}
            session.state_manager.option_atps.update(getattr(self.state_manager, 'option_atps', {}))
            if not hasattr(session.state_manager, 'atp_history'): session.state_manager.atp_history = {}
            session.state_manager.atp_history.update(getattr(self.state_manager, 'atp_history', {}))
            session.state_manager.option_data.update(self.state_manager.option_data)

    async def _get_ltp_for_strike(self, strike, side, timestamp):
        expiry = self.atm_manager.signal_expiry_date
        key = self.atm_manager.find_instrument_key_by_strike(strike, side, expiry)
        return await self._get_ltp_for_backtest_instrument(key, timestamp) if key else None

    async def close_open_backtest_positions(self, timestamp):
        if not self.pnl_tracker: return
        for side in ['CALL', 'PUT']:
            trade = self.pnl_tracker.active_call_trade if side == 'CALL' else self.pnl_tracker.active_put_trade
            if trade:
                ohlc = await self.data_manager.get_historical_ohlc(trade['instrument_key'], 1, current_timestamp=timestamp, for_full_day=True)
                ltp = ohlc.asof(timestamp)['close'] if ohlc is not None and not ohlc.empty else None
                if ltp: self.pnl_tracker.exit_trade(side, ltp, timestamp, reason="End of backtest")

        if hasattr(self, 'sell_manager') and self.sell_manager.strangle_placed and not self.sell_manager.strangle_closed:
            await self.sell_manager.close_all(timestamp)
