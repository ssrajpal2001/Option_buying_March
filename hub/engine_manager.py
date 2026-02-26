import asyncio
import datetime
from utils.logger import logger
from hub.provider_factory import ProviderFactory
from hub.lifecycle_manager import LifecycleManager
from hub.data_manager import DataManager
from hub.backtest_orchestrator import BacktestOrchestrator
from hub.live_orchestrator import LiveOrchestrator

class EngineManager:
    def __init__(self, config_manager, api_client_manager, broker_manager, db_manager, redis_manager, shared_display_manager):
        self.config_manager = config_manager
        self.api_client_manager = api_client_manager
        self.broker_manager = broker_manager
        self.db_manager = db_manager
        self.redis_manager = redis_manager
        self.shared_display_manager = shared_display_manager
        self.lifecycle_managers = {}

    async def create_orchestrator(self, instrument_name, rest_client, websocket_manager, is_backtest):
        instrument_name = instrument_name.upper().strip()
        symbol = self.config_manager.get(instrument_name, 'instrument_symbol')
        if not symbol:
            fallbacks = {'NIFTY': 'NSE_INDEX|Nifty 50', 'BANKNIFTY': 'NSE_INDEX|Nifty Bank', 'FINNIFTY': 'NSE_INDEX|Nifty Fin Service', 'SENSEX': 'BSE_INDEX|SENSEX', 'MIDCAP': 'NSE_INDEX|NIFTY MID SELECT'}
            symbol = fallbacks.get(instrument_name, 'NSE_INDEX|Nifty 50')

        data_manager = DataManager(rest_client, symbol, self.config_manager)
        OrchestratorClass = BacktestOrchestrator if is_backtest else LiveOrchestrator
        orch = OrchestratorClass(instrument_name=instrument_name, rest_client=rest_client, websocket_manager=websocket_manager, broker_manager=self.broker_manager, config_manager=self.config_manager, data_manager=data_manager, redis_manager=self.redis_manager)

        orch.atm_manager.data_manager = data_manager
        data_manager.atm_manager = orch.atm_manager
        if not await data_manager.load_contracts(): raise RuntimeError(f"Failed to load contracts for {instrument_name}.")

        orch.atm_manager.all_contracts = data_manager.all_options
        orch.atm_manager.near_expiry_date = data_manager.near_expiry_date
        orch.atm_manager.monthly_expiries = data_manager.monthly_expiries
        orch.atm_manager._build_contract_lookup_table()
        orch.finalize_initialization()
        orch.atm_manager._determine_expiries()
        return orch

    async def run_engines(self):
        start_time = datetime.datetime.strptime(self.config_manager.get('settings', 'start_time'), '%H:%M:%S').time()
        end_time = datetime.datetime.strptime(self.config_manager.get('settings', 'end_time'), '%H:%M:%S').time()

        is_initialized, is_trading_active = False, False
        ws_mgr, ws_task, disp_task = None, None, None

        while True:
            now = datetime.datetime.now().time()
            if start_time <= now < end_time:
                if not is_initialized:
                    try:
                        await self.broker_manager.load_brokers()
                        instruments = self.broker_manager.get_all_unique_instruments()
                        if not instruments:
                            await asyncio.sleep(60); continue

                        rest, ws_mgr = await ProviderFactory.create_data_provider(self.api_client_manager, self.config_manager, False, self.redis_manager)
                        ws_task, disp_task = ws_mgr.start(), asyncio.create_task(self.shared_display_manager.start_display_loop())

                        orchestrators = {}
                        for inst in instruments:
                            orchestrators[inst] = await self.create_orchestrator(inst, rest, ws_mgr, False)

                        # Add Users to relevant Orchestrators
                        db_users = await self.db_manager.get_active_users_and_brokers() if self.db_manager else []
                        unique_users = {u['user_id']: u for u in db_users}

                        if unique_users:
                            for user_id, user_data in unique_users.items():
                                for inst_name, orch in orchestrators.items():
                                    if any(b.user_id == user_id and b.is_configured_for_instrument(inst_name) for b in self.broker_manager.brokers):
                                        await orch.add_user_session(user_id, user_data['email'], await self.db_manager.get_user_strategy(user_id, inst_name))
                        else:
                            for inst_name, orch in orchestrators.items():
                                await orch.add_user_session(None, "default_user@local")

                        self.shared_display_manager.orchestrators.update(orchestrators)
                        for inst, orch in orchestrators.items():
                            self.lifecycle_managers[inst] = LifecycleManager(orch, ws_mgr, self.broker_manager, self.shared_display_manager, False)

                        subs = []
                        for orch in orchestrators.values(): subs.extend(orch.get_initial_subscriptions())
                        if ws_mgr and subs: ws_mgr.subscribe(list(set(subs)))
                        is_initialized = True
                    except Exception as e:
                        logger.error(f"Init error: {e}"); await asyncio.sleep(10); continue

                if is_initialized and not is_trading_active:
                    await asyncio.gather(*(m.start(False) for m in self.lifecycle_managers.values()))
                    is_trading_active = True
                    for orch in orchestrators.values():
                        if hasattr(orch, 'sell_manager') and not orch.sell_manager.strangle_placed:
                            await orch.sell_manager.execute_short_strangle(datetime.datetime.now())
            else:
                if is_trading_active:
                    for orch in orchestrators.values():
                        if hasattr(orch, 'sell_manager') and not orch.sell_manager.strangle_closed:
                            await orch.sell_manager.close_all(datetime.datetime.now())
                    await asyncio.gather(*(m.stop() for m in self.lifecycle_managers.values()))
                    is_trading_active = False
                if is_initialized:
                    if ws_mgr: await ws_mgr.close()
                    if ws_task: ws_task.cancel()
                    if disp_task: disp_task.cancel()
                    self.broker_manager.shutdown(); self.lifecycle_managers.clear()
                    self.shared_display_manager.orchestrators.clear(); is_initialized = False
            await asyncio.sleep(5)
