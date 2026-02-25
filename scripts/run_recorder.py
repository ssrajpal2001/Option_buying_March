import asyncio
import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import logger, configure_logger
from utils.config_manager import ConfigManager
from utils.api_client_manager import ApiClientManager
from hub.provider_factory import ProviderFactory
from hub.data_manager import DataManager
from hub.atm_manager import AtmManager
from hub.data_recorder import DataRecorder
from hub.strike_manager import StrikeManager
from hub.state_manager import StateManager
from hub.price_feed_handler import PriceFeedHandler
import argparse

class RecordingOrchestrator:
    """Minimal orchestrator for data recording only."""
    def __init__(self, instrument_name, config_manager, rest_client, websocket_manager):
        self.instrument_name = instrument_name
        self.config_manager = config_manager
        self.rest_client = rest_client
        self.websocket = websocket_manager
        self.is_backtest = False

        self.state_manager = StateManager(config_manager, instrument_name)
        self.atm_manager = AtmManager(config_manager, websocket_manager, self.state_manager, rest_client, instrument_name, orchestrator=self)
        self.strike_manager = StrikeManager(self.state_manager, self.atm_manager, config_manager, instrument_name=instrument_name)
        self.data_recorder = DataRecorder(instrument_name)

        self.futures_instrument_key = config_manager.get(instrument_name, 'futures_instrument_key')
        self.index_instrument_key = config_manager.get(instrument_name, 'instrument_symbol')
        self.atm_manager.spot_instrument_key = self.futures_instrument_key

        # Dummy aggregators
        from utils.ohlc_aggregator import OHLCAggregator
        self.price_feed_handler = PriceFeedHandler(
            self.state_manager, self.atm_manager, self,
            OHLCAggregator(1), OHLCAggregator(5), OHLCAggregator(1)
        )

        self.websocket.register_message_handler(self.price_feed_handler.handle_message)

        # V2 Tick Loop logic simplified
        self.last_record_time = 0
        self.user_sessions = {} # Compatibility for PriceFeedHandler

    def _get_timestamp(self):
        """Returns the current localized timestamp (Asia/Kolkata)."""
        import pytz
        return datetime.datetime.now(pytz.timezone('Asia/Kolkata'))

    async def start(self):
        logger.info(f"[{self.instrument_name}] Recorder starting...")

        # Load contracts
        dm = DataManager(self.rest_client, self.index_instrument_key, self.config_manager)
        await dm.load_contracts()
        self.atm_manager.all_contracts = dm.all_options
        self.atm_manager._build_contract_lookup_table()
        self.atm_manager._determine_expiries()
        self.atm_manager.set_ready()

        # Subscribe to base instruments
        self.websocket.subscribe([self.futures_instrument_key, self.index_instrument_key])

        logger.info(f"[{self.instrument_name}] Recorder is now LIVE and tracking ATM +/- 10.")

        while True:
            await self._process_tick()
            await asyncio.sleep(0.5)

    async def _process_tick(self):
        now_ts = asyncio.get_event_loop().time()
        if now_ts - self.last_record_time < 1.0:
            return

        futures_price = self.state_manager.spot_price
        index_price = self.state_manager.index_price

        if not futures_price:
            return

        self.last_record_time = now_ts
        strike_interval = self.config_manager.get_int(self.instrument_name, 'strike_interval')
        current_atm = float(round(futures_price / strike_interval) * strike_interval)

        recording_strikes = self.strike_manager.get_recording_watchlist(current_atm)
        recording_data = {}

        new_keys = set()
        for strike in recording_strikes:
            ce_contract, pe_contract = self.atm_manager.find_contracts_for_strike(strike, self.atm_manager.signal_expiry_date)
            ce_key = ce_contract.instrument_key if ce_contract else None
            pe_key = pe_contract.instrument_key if pe_contract else None

            if ce_key: new_keys.add(ce_key)
            if pe_key: new_keys.add(pe_key)

            recording_data[strike] = {
                'ce_symbol': ce_key,
                'ce_ltp': self.state_manager.option_prices.get(ce_key),
                'ce_delta': self.state_manager.option_deltas.get(ce_key),
                'pe_symbol': pe_key,
                'pe_ltp': self.state_manager.option_prices.get(pe_key),
                'pe_delta': self.state_manager.option_deltas.get(pe_key),
            }

        # DYNAMIC SUBSCRIPTION: Ensure we are subscribed to the instruments we are recording
        current_subscribed = getattr(self, '_subscribed_keys', set())
        to_subscribe = list(new_keys - current_subscribed)
        if to_subscribe:
            logger.info(f"Recorder: Subscribing to {len(to_subscribe)} new option instruments for recording.")
            self.websocket.subscribe(to_subscribe)
            self._subscribed_keys = current_subscribed.union(new_keys)

        self.data_recorder.record_ticks(datetime.datetime.now(), futures_price, index_price, current_atm, recording_data)

async def main():
    parser = argparse.ArgumentParser(description="Standalone Data Recorder")
    parser.add_argument('--instrument', type=str, default='NIFTY', help='Instrument to record')
    args = parser.parse_args()

    config_manager = ConfigManager(config_file='config/config_trader.ini')
    configure_logger(config_manager)

    api_client_manager = ApiClientManager(config_manager)
    await api_client_manager.async_init()

    rest_client, websocket_manager = await ProviderFactory.create_data_provider(api_client_manager, config_manager, is_backtest=False)

    recorder = RecordingOrchestrator(args.instrument, config_manager, rest_client, websocket_manager)

    # Start tasks
    try:
        await asyncio.gather(
            websocket_manager.start(),
            recorder.start()
        )
    except KeyboardInterrupt:
        logger.info("Recorder stopped by user.")

if __name__ == '__main__':
    asyncio.run(main())
