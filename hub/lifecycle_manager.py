import asyncio
import pandas as pd
from utils.logger import logger
from hub.csv_data_feeder import CSVDataFeeder

class LifecycleManager:
    def __init__(self, trade_orchestrator, websocket_manager, broker_manager, display_manager, is_backtest=False):
        self.trade_orchestrator = trade_orchestrator
        self.websocket_manager = websocket_manager
        self.broker_manager = broker_manager
        self.display_manager = display_manager
        self.is_running = False
        self.display_task = None
        self.is_backtest = is_backtest
        self.backtest_dates = []

        if self.is_backtest:
            self.trade_orchestrator.data_feeder = self.websocket_manager

    async def start(self, start_display=True):
        """
        Delegates the startup sequence to the TradeOrchestrator and then
        starts the remaining components like the websocket and display.
        """
        self.is_running = True
        self.trade_orchestrator.is_active = True
        logger.debug("LifecycleManager started. Delegating startup sequence...")

        try:
            # In the new architecture, startup is simplified. The orchestrator is already
            # initialized, and the WebSocket is connected centrally in main.py.

            # Start the UI loop if requested (typically only for single-instrument/backtest)
            # USER REQUIREMENT: Disable display loop for backtest to allow cleaner log verification
            if start_display and not self.is_backtest:
                logger.info("LifecycleManager starting display loop...")
                self.display_task = asyncio.create_task(self._run_display_loop())

            logger.info("Trading bot is now live.")

            # Now, start the data feed
            is_backtest = isinstance(self.websocket_manager, CSVDataFeeder)
            if is_backtest:
                # In backtest mode, we run a synchronous loop instead of the async feed
                logger.info("Starting synchronous backtest loop...")
                await self._run_backtest_loop()
                logger.info("Backtest loop finished.")
            else:
                # In live mode, the WebSocket is already connected centrally in main.py.
                # The orchestrator's start_up method subscribes to instruments,
                # which is all that's needed.
                pass

        except asyncio.TimeoutError:
            logger.critical("Bot startup failed within 60s timeout. Check network or API token.", exc_info=True)
            await self.stop()
        except Exception as e:
            logger.critical(f"An unexpected error occurred during startup: {e}", exc_info=True)
            await self.stop()

    async def _run_backtest_loop(self):
        """
        The main synchronous loop for processing backtest data from the CSV.
        Supports multi-day backtesting by iterating over a list of dates.
        """
        data_feeder = self.websocket_manager
        dates_to_run = self.backtest_dates if self.backtest_dates else [None]

        for current_date in dates_to_run:
            if current_date:
                logger.info(f"\n{'='*20} STARTING BACKTEST FOR DATE: {current_date} {'='*20}")
                # Reset state and aggregators for new day
                if hasattr(self.trade_orchestrator, 'clear_for_new_run'):
                    self.trade_orchestrator.clear_for_new_run()

                # Re-load data for specific date
                data_feeder.load_data(target_date=current_date)
            else:
                data_feeder.load_data()

            if data_feeder.data is None or data_feeder.data.empty:
                logger.error(f"Backtest data for {current_date} is not loaded or empty. Skipping.")
                continue

            # Prepare orchestrator (pre-fetch underlyings, etc.)
            if hasattr(self.trade_orchestrator, 'prepare_backtest'):
                await self.trade_orchestrator.prepare_backtest()

            # The `tick_history` in BaseOrchestrator will manage the previous tick's data.
            last_timestamp = None
            for timestamp, group in data_feeder.data.groupby('timestamp'):
                try:
                    # The orchestrator now handles its own history, so we just pass the current tick.
                    await self.trade_orchestrator.run_backtest_strategy_for_timestamp(
                        timestamp,
                        group
                    )
                except Exception as e:
                    logger.error(f"Error processing backtest data for timestamp {timestamp}: {e}", exc_info=True)
                last_timestamp = timestamp

            # After the loop, close any open positions using the last known timestamp.
            if last_timestamp:
                await self.trade_orchestrator.close_open_backtest_positions(last_timestamp)

            # --- Final Report Generation for the Day ---
            if self.trade_orchestrator.pnl_tracker:
                pnl_tracker = self.trade_orchestrator.pnl_tracker
                all_trades = pnl_tracker.trade_history

                active_broker_sections_str = self.trade_orchestrator.config_manager.get('settings', 'active_broker', fallback='')
                first_broker_section = active_broker_sections_str.split(',')[0].strip()
                temp_broker_for_reporting = self.broker_manager.get_broker_instance(first_broker_section)

                logger.info(f"--- Detailed Backtest Trade Log for {current_date or data_feeder.backtest_date} ---")
                for trade in all_trades:
                    symbol = "N/A"
                    if 'contract' in trade and trade['contract']:
                        c = trade['contract']
                        symbol = f"{self.trade_orchestrator.instrument_name} {c.strike_price} {c.instrument_type}"
                    elif temp_broker_for_reporting and 'contract' in trade:
                        try:
                            signal_expiry = self.trade_orchestrator.atm_manager.signal_expiry_date
                            symbol = temp_broker_for_reporting.construct_zerodha_symbol(trade['contract'], signal_expiry)
                        except Exception as e:
                            logger.warning(f"Could not construct symbol for a trade: {e}")

                    side_label = f"{trade.get('side', 'N/A')} ({trade.get('entry_type', 'BUY')})"
                    log_message = (
                        f"Symbol: {symbol}, "
                        f"Side: {side_label}, "
                        f"Entry Time: {trade.get('entry_timestamp', 'N/A')}, "
                        f"Entry Price: {trade.get('entry_price', 0):.2f}, "
                        f"Exit Time: {trade.get('exit_timestamp', 'N/A')}, "
                        f"Exit Price: {trade.get('exit_price', 0):.2f}, "
                        f"PnL: {trade.get('pnl', 0):.2f}"
                    )
                    logger.info(log_message)

                logger.info(f"--- End of Detailed Log for {current_date or data_feeder.backtest_date} ---")

                # Generate and log the final summary report
                pnl_tracker.generate_summary_report()


    async def _run_display_loop(self):
        """
        Internal loop for display updates that respects the is_running flag.
        """
        last_heartbeat = 0
        while self.is_running:
            try:
                now = asyncio.get_event_loop().time()
                if now - last_heartbeat >= 10.0:
                    logger.info("Lifecycle heartbeat: Display loop is active.")
                    last_heartbeat = now

                self.display_manager.display_data()
            except Exception as e:
                logger.error(f"Display update error: {e}")
            await asyncio.sleep(1.0)

    async def stop(self):
        """Gracefully stops all components of the application."""
        if not self.is_running:
            return

        logger.info("Initiating graceful shutdown...")
        self.is_running = False
        self.trade_orchestrator.is_active = False
        
        self.trade_orchestrator.stop()

        if self.display_task and not self.display_task.done():
            self.display_task.cancel()
            try:
                await self.display_task
            except asyncio.CancelledError:
                logger.info("Display manager task cancelled.")

        all_subscriptions = list(self.trade_orchestrator.state_manager.option_prices.keys())
        spot_key = self.trade_orchestrator.atm_manager.spot_instrument_key
        if spot_key and spot_key not in all_subscriptions:
            all_subscriptions.append(spot_key)
        
        if all_subscriptions:
            self.websocket_manager.unsubscribe(all_subscriptions)
        
        await self.websocket_manager.close()
        self.broker_manager.close_all_positions()
        
        # --- FIX: Call the new shutdown method to close file handles ---
        self.broker_manager.shutdown()

        await self.trade_orchestrator.state_manager.reset_trade_state("CALL")
        await self.trade_orchestrator.state_manager.reset_trade_state("PUT")
        
        logger.info("Shutdown complete.")
        self.is_running = False