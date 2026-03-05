import asyncio
import datetime
import os
from utils.logger import logger

class DisplayManager:
    def __init__(self, orchestrators, config_manager=None):
        # Support both single orchestrator (backtest) and multi (live)
        if not isinstance(orchestrators, dict):
            # Wrap single orchestrator into a dict
            self.orchestrators = {orchestrators.instrument_name: orchestrators}
        else:
            self.orchestrators = orchestrators

        self.config_manager = config_manager

    async def start_display_loop(self):
        """Starts the continuous loop to refresh the console display."""
        if self.config_manager:
            enabled = self.config_manager.get('settings', 'display_enabled', fallback='true').lower() == 'true'
            if not enabled:
                return
        loop = asyncio.get_running_loop()
        while True:
            # RUN SYNCHRONOUS display_data in a separate thread to avoid blocking the event loop.
            # This is critical for high-frequency trading bots where terminal I/O (like print)
            # or terminal suspension (Ctrl+S) can otherwise freeze the entire application.
            try:
                await loop.run_in_executor(None, self.display_data)
            except Exception as e:
                logger.error(f"Error in display executor: {e}")
            await asyncio.sleep(1) # Refresh rate

    def display_data(self):
        """Clears the console and renders all the display components."""
        try:
            # Use ANSI escape sequence to clear screen (non-blocking)
            print("\033[H\033[2J", end="")

            header_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            entry_tf = "1"
            exit_tf = "1"
            exit_logic = "CROSSOVER + FLIP"
            if self.config_manager:
                entry_tf = self.config_manager.get('settings', 'entry_timeframe_minutes', fallback='1')
                exit_tf = self.config_manager.get('settings', 'exit_timeframe_minutes', fallback='1')

            print(f"--- AlgoSoft Bot [Crossover Strategy | {entry_tf}m / {exit_tf}m / {exit_logic}] --- {header_time}\n")

            total_running_pnl = 0.0
            total_closed_pnl = 0.0
            total_trades = 0

            for instrument_name, orchestrator in self.orchestrators.items():
                sm = orchestrator.state_manager
                atm_m = orchestrator.atm_manager

                spot_price = sm.spot_price
                index_price = sm.index_price
                atm_strike = atm_m.strikes.get('atm', 'N/A')
                spot_price_str = f"{spot_price:.2f}" if isinstance(spot_price, (int, float)) else "N/A"
                index_price_str = f"{index_price:.2f}" if isinstance(index_price, (int, float)) else "N/A"
                target_strike = sm.target_strike
                target_strike_str = f"{target_strike}" if target_strike else "N/A"

                print(f"[{instrument_name}] FUTURES: {spot_price_str} | INDEX: {index_price_str} | ATM: {atm_strike} | TARGET: {target_strike_str}")

                self._display_monitored_strikes(orchestrator)
                self._display_strangle_positions(orchestrator)
                self._display_active_trades(orchestrator)

                # Aggregate PNL
                display_state = sm
                if orchestrator.user_sessions:
                    sessions_with_trades = [s for s in orchestrator.user_sessions.values() if s.is_in_trade()]
                    if sessions_with_trades:
                        display_state = sessions_with_trades[0].state_manager
                    elif None in orchestrator.user_sessions:
                        display_state = orchestrator.user_sessions[None].state_manager

                call_pnl = display_state.call_position.get('pnl', 0.0) if display_state.call_position else 0.0
                put_pnl = display_state.put_position.get('pnl', 0.0) if display_state.put_position else 0.0
                total_running_pnl += (call_pnl + put_pnl)
                total_closed_pnl += display_state.total_pnl
                total_trades += display_state.trade_count

                print("-" * 110)

            # Combined Summary at the bottom
            print(f"\n--- GLOBAL SESSION SUMMARY ---")
            print(f"  Closed Trades PNL: {total_closed_pnl:.2f}")
            print(f"  Running PNL: {total_running_pnl:.2f}")
            print(f"  NET SESSION PNL: {(total_closed_pnl + total_running_pnl):.2f} | Total Trades: {total_trades}")

        except Exception as e:
            logger.error(f"Error in DisplayManager main loop: {e}", exc_info=True)

    def _display_monitored_strikes(self, orchestrator):
        """Displays a detailed table of all strikes being monitored for Crossover."""
        atm_manager = orchestrator.atm_manager
        state_manager = orchestrator.state_manager
        strike_manager = orchestrator.strike_manager

        if not atm_manager.contracts:
            return

        print(f"  {'STRIKE':<10} | {'CE_LTP':<10} | {'PE_LTP':<10} | {'DIFF':<8} | {'HIGHER':<8} | {'WAITING FOR':<15} | {'STATUS'}")

        # Get monitoring info from the first session (for Legacy Mode visibility)
        monitored_strike = None
        current_sequence = []
        ce_close = None
        pe_close = None
        ce_vwap = None
        pe_vwap = None
        pending_vwap_side = None

        if orchestrator.user_sessions:
            # Multi-tenant: Find a session with active monitoring
            monitoring_sessions = [s for s in orchestrator.user_sessions.values() if s.state_manager.dual_sr_monitoring_data]
            session_to_use = monitoring_sessions[0] if monitoring_sessions else next(iter(orchestrator.user_sessions.values()))

            sm = session_to_use.state_manager
            if sm.dual_sr_monitoring_data:
                monitored_strike = sm.dual_sr_monitoring_data.get('target_strike')
                current_sequence = sm.dual_sr_monitoring_data.get('current_sequence', [])
                ce_close = sm.dual_sr_monitoring_data.get('ce_data', {}).get('last_close')
                pe_close = sm.dual_sr_monitoring_data.get('pe_data', {}).get('last_close')
                ce_vwap = sm.dual_sr_monitoring_data.get('ce_data', {}).get('vwap')
                pe_vwap = sm.dual_sr_monitoring_data.get('pe_data', {}).get('vwap')
                pending_vwap_side = sm.dual_sr_monitoring_data.get('pending_vwap_side')

        # Get the watchlist from strike_manager
        atm_strike = atm_manager.strikes.get('atm')
        if not atm_strike:
            return

        watchlist = strike_manager.get_strike_watchlist(atm_strike)

        for strike in watchlist:
            try:
                # Ensure strike is treated as float for consistent lookups
                strike_key = float(strike)
                contracts = atm_manager.contracts.get(strike_key, {})

                # Robust key extraction
                ce_entry = contracts.get('CE')
                pe_entry = contracts.get('PE')

                ce_key = None
                if ce_entry:
                    if isinstance(ce_entry, dict): ce_key = ce_entry.get('key')
                    else: ce_key = getattr(ce_entry, 'instrument_key', None)

                pe_key = None
                if pe_entry:
                    if isinstance(pe_entry, dict): pe_key = pe_entry.get('key')
                    else: pe_key = getattr(pe_entry, 'instrument_key', None)

                ce_ltp = state_manager.option_prices.get(ce_key) if ce_key else None
                pe_ltp = state_manager.option_prices.get(pe_key) if pe_key else None

                is_atm = "*" if strike == atm_strike else " "
                strike_label = f"{int(strike)}{is_atm}"

                if ce_ltp is not None and pe_ltp is not None:
                    abs_diff = abs(ce_ltp - pe_ltp)
                    higher_side = 'CE' if ce_ltp > pe_ltp else 'PE'

                    waiting_for = "---"
                    status_parts = []

                    # Target Identification
                    if abs(strike_key - (state_manager.target_strike or 0)) < 0.1:
                        status_parts.append("TARGET")

                    # Active Monitoring Logic
                    if abs(strike_key - (monitored_strike or 0)) < 0.1:
                        status_parts.append("WATCHING")
                        waiting_for = "PE-CE/CE-PE"

                        if pending_vwap_side:
                            waiting_for = f"AWAITING VWAP ({pending_vwap_side})"
                            status_parts.append("PENDING_VWAP")

                        if current_sequence:
                            seq_str = ",".join(current_sequence[-3:])
                            status_parts.append(f"SEQ:[{seq_str}]")

                        if ce_close and pe_close:
                            status_parts.append(f"1M_CLOSE({ce_close:.1f}v{pe_close:.1f})")

                        if ce_vwap:
                            status_parts.append(f"CE_ATP:{ce_vwap:.1f}")
                        if pe_vwap:
                            status_parts.append(f"PE_ATP:{pe_vwap:.1f}")

                        ce_r1 = sm.dual_sr_monitoring_data.get('ce_data', {}).get('r1_high')
                        pe_r1 = sm.dual_sr_monitoring_data.get('pe_data', {}).get('r1_high')
                        if ce_r1: status_parts.append(f"CE_R1:{ce_r1:.1f}")
                        if pe_r1: status_parts.append(f"PE_R1:{pe_r1:.1f}")

                    status = " | ".join(status_parts) if status_parts else ""

                    ce_ltp_str = f"{ce_ltp:>7.2f}"
                    pe_ltp_str = f"{pe_ltp:>7.2f}"
                    diff_str = f"{abs_diff:>6.2f}"

                    print(f"  {strike_label:<10} | {ce_ltp_str:<10} | {pe_ltp_str:<10} | {diff_str:<8} | {higher_side:<8} | {waiting_for:<15} | {status}")
                else:
                    print(f"  {strike_label:<10} | {'N/A':<10} | {'N/A':<10} | {'N/A':<8} | {'N/A':<8} | {'---':<15} | Waiting...")
            except Exception as e:
                # Log error and show a recovery row to avoid console blanking
                logger.error(f"Error displaying strike {strike}: {e}")
                print(f"  {int(strike):<10} | {'ERROR':<10} | {'ERROR':<10} | {'ERROR':<10} | {'ERROR':<10} | Check Logs")

    def _display_strangle_positions(self, orchestrator):
        """Displays the active short strangle SELL positions with running PNL."""
        if not hasattr(orchestrator, 'sell_manager'):
            return
        sm = orchestrator.sell_manager
        if not sm.strangle_placed:
            return

        state_manager = orchestrator.state_manager
        ce_ltp = state_manager.option_prices.get(sm.sell_ce_key)
        pe_ltp = state_manager.option_prices.get(sm.sell_pe_key)
        hedge_ce_ltp = state_manager.option_prices.get(sm.buy_ce_key)
        hedge_pe_ltp = state_manager.option_prices.get(sm.buy_pe_key)

        ce_entry = sm.sell_ce_entry_ltp or 0.0
        pe_entry = sm.sell_pe_entry_ltp or 0.0

        ce_ltp_str = f"{ce_ltp:.2f}" if ce_ltp is not None else "---"
        pe_ltp_str = f"{pe_ltp:.2f}" if pe_ltp is not None else "---"
        hedge_ce_str = f"{hedge_ce_ltp:.2f}" if hedge_ce_ltp is not None else "---"
        hedge_pe_str = f"{hedge_pe_ltp:.2f}" if hedge_pe_ltp is not None else "---"

        ce_pnl = (ce_entry - ce_ltp) if ce_ltp is not None else 0.0
        pe_pnl = (pe_entry - pe_ltp) if pe_ltp is not None else 0.0
        total_pnl = ce_pnl + pe_pnl

        pnl_sign = "+" if total_pnl >= 0 else ""
        print(f"\n    --- STRANGLE POSITIONS (SELL NRML) ---")
        if sm.sell_ce_strike is not None:
            print(f"    SELL CE {int(sm.sell_ce_strike)} | Entry: {ce_entry:.2f} | LTP: {ce_ltp_str} | PNL: {'+' if ce_pnl >= 0 else ''}{ce_pnl:.2f}")
        if sm.sell_pe_strike is not None:
            print(f"    SELL PE {int(sm.sell_pe_strike)} | Entry: {pe_entry:.2f} | LTP: {pe_ltp_str} | PNL: {'+' if pe_pnl >= 0 else ''}{pe_pnl:.2f}")
        hedge_ce_strike = int(sm.sell_ce_strike + 100) if sm.sell_ce_strike is not None else "---"
        hedge_pe_strike = int(sm.sell_pe_strike - 100) if sm.sell_pe_strike is not None else "---"
        print(f"    Hedge CE {hedge_ce_strike} (LTP: {hedge_ce_str}) | Hedge PE {hedge_pe_strike} (LTP: {hedge_pe_str})")
        print(f"    Strangle PNL: {pnl_sign}{total_pnl:.2f} per lot")

    def _display_active_trades(self, orchestrator):
        """Displays the status and P&L of any active ITM weekly trades for an instrument."""
        # Resolve which StateManager to use for positions (Global vs Multi-Tenant)
        display_state = orchestrator.state_manager
        if orchestrator.user_sessions:
            # Check for any active trades in user sessions
            sessions_with_trades = [s for s in orchestrator.user_sessions.values() if s.is_in_trade()]
            if sessions_with_trades:
                display_state = sessions_with_trades[0].state_manager
            elif None in orchestrator.user_sessions:
                # Fallback to default/legacy session
                display_state = orchestrator.user_sessions[None].state_manager

        call_pos = display_state.call_position
        put_pos = display_state.put_position

        if not call_pos and not put_pos:
            return

        print("\n    --- ACTIVE POSITIONS ---")

        def format_side_status(direction, pos):
            if pos and pos.get('instrument_key'):
                pnl = pos.get('pnl', 0.0)
                traded_strike = pos.get('strike_price', 'N/A')
                main_line = (f"IN TRADE | {pos.get('instrument_symbol', 'N/A')} ({traded_strike}) | "
                             f"Entry: {pos.get('entry_price', 0):.2f}, "
                             f"Peak: {pos.get('peak_price', 0):.2f}, "
                             f"LTP: {pos.get('ltp', 0):.2f}, "
                             f"Live PNL: {pnl:.2f}")

                # SL Monitor Info
                sl_line = ""
                sl_strike = pos.get('s1_monitoring_strike') # Use currently monitored strike
                if sl_strike:
                    # Find LTP of the signal strike
                    expiry = orchestrator.atm_manager.signal_expiry_date
                    ce_key = orchestrator.atm_manager.find_instrument_key_by_strike(sl_strike, 'CALL', expiry)
                    pe_key = orchestrator.atm_manager.find_instrument_key_by_strike(sl_strike, 'PUT', expiry)
                    ce_ltp = orchestrator.state_manager.option_prices.get(ce_key)
                    pe_ltp = orchestrator.state_manager.option_prices.get(pe_key)

                    ce_ltp_str = f"{ce_ltp:.2f}" if ce_ltp else "N/A"
                    pe_ltp_str = f"{pe_ltp:.2f}" if pe_ltp else "N/A"

                    s1_fast = pos.get('s1_fast')
                    s1_slow = pos.get('s1_slow')
                    fast_tf = pos.get('s1_fast_tf', 1)
                    slow_tf = pos.get('s1_slow_tf', 5)
                    tsl = pos.get('trailing_sl')

                    s1_fast_str = f"{s1_fast:.2f}" if s1_fast is not None else "N/A"
                    s1_slow_str = f"{s1_slow:.2f}" if s1_slow is not None else "N/A"
                    active_tf = pos.get('active_s1_tf') or 'N/A'

                    s1_fast_display = f"[{s1_fast_str}]" if str(active_tf) == str(fast_tf) else s1_fast_str
                    s1_slow_display = f"[{s1_slow_str}]" if str(active_tf) == str(slow_tf) else s1_slow_str

                    sl_label = f"S1LOW (CHECKING:{sl_strike})"
                    if pos.get('s1_transition_pending'):
                        pending_strike = pos.get('s1_pending_new_strike')
                        sl_label = f"S1LOW (STRIKE SWITCH PENDING -> {pending_strike})"

                    target_val = pos.get('current_target')

                    target_exit_enabled = False
                    if self.config_manager:
                        target_exit_enabled = self.config_manager.get_boolean('settings', 'target_exit', fallback=False)

                    target_str = "N/A"
                    if target_exit_enabled and target_val is not None:
                        try:
                            t_float = float(target_val)
                            target_str = f"{t_float:.2f}"
                        except (TypeError, ValueError): pass

                    # Get R1 from monitoring data if available
                    r1_val = None
                    if display_state.dual_sr_monitoring_data:
                        side_key = 'ce_data' if direction == 'CALL' else 'pe_data'
                        r1_val = display_state.dual_sr_monitoring_data.get(side_key, {}).get('r1_high')

                    sl_line = f"\n      "
                    if target_exit_enabled:
                         sl_line += f"TARGET: {target_str} | "

                    sl_line += f"{sl_label}: {fast_tf}m={s1_fast_display}, {slow_tf}m={s1_slow_display} (Active:{active_tf}m)"
                    if r1_val:
                        sl_line += f" | R1: {r1_val:.2f}"
                    if tsl is not None:
                        sl_line += f" | TRAILING_SL: {tsl:.2f}"

                    sl_line += f" | SIG_LTP({sl_strike}): {ce_ltp_str if direction == 'CALL' else pe_ltp_str}"

                return f"{main_line}{sl_line}"
            return None

        call_status = format_side_status('CALL', call_pos)
        if call_status: print(f"    CALL: {call_status}")

        put_status = format_side_status('PUT', put_pos)
        if put_status: print(f"    PUT:  {put_status}")


    def _display_pnl_summary(self):
        """Redundant in aggregated mode, handled in display_data."""
        pass
