from utils.logger import logger
import asyncio

class SubscriptionManager:
    def __init__(self, websocket, config_manager, data_manager, orchestrator):
        self.websocket = websocket
        self.config = config_manager
        self.data_manager = data_manager
        self.orchestrator = orchestrator

    async def perform_resubscription(self, atm_strike, protected_keys, signal_expiry_date, contracts_dict, find_contracts_func):
        logger.debug(f"V2: Performing resubscription around ATM: {atm_strike}")
        is_backtest = self.orchestrator.is_backtest

        if not is_backtest:
            old_keys = {contract['key'] for strike in contracts_dict.values() for contract in strike.values() if contract and 'key' in contract}
            keys_to_unsub = list(old_keys - protected_keys)
            if keys_to_unsub: self.websocket.unsubscribe(keys_to_unsub)

        strike_interval = self.config.get_int(self.orchestrator.instrument_name, 'strike_interval')
        num_strikes = self.config.get_int('settings', 'strikes_to_monitor', 1)
        monitored_strikes = {atm_strike + i * strike_interval for i in range(-num_strikes, num_strikes + 1)}

        contracts_dict.clear()
        keys_to_sub = set()

        # Simple logic for now, AtmManager will pass necessary info
        for strike in sorted(list(monitored_strikes)):
            ce_c, pe_c = find_contracts_func(strike, signal_expiry_date)
            if ce_c: keys_to_sub.add(ce_c.instrument_key)
            if pe_c: keys_to_sub.add(pe_c.instrument_key)
            contracts_dict[strike] = {
                'CE': {'key': ce_c.instrument_key if ce_c else None},
                'PE': {'key': pe_c.instrument_key if pe_c else None}
            }

        final_keys = list(keys_to_sub.union(protected_keys))
        if self.data_manager:
            await self._prime_history(keys_to_sub)

        if not is_backtest and final_keys:
            self.websocket.subscribe(final_keys, mode='full')
        return final_keys

    async def _prime_history(self, keys):
        timestamp = self.orchestrator._get_timestamp()
        prime_tasks = [self.data_manager.prime_aggregator(self.orchestrator.one_min_aggregator, k, timestamp) for k in keys if k]
        if prime_tasks:
            if self.orchestrator.is_backtest:
                await asyncio.gather(*prime_tasks)
            else:
                async def run_prime():
                    try: await asyncio.gather(*prime_tasks)
                    except Exception as e: logger.error(f"Prime failed: {e}")
                asyncio.create_task(run_prime())
