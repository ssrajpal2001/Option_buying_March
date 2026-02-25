from utils.logger import logger
from utils.api_client_manager import ApiClientManager
from hub.csv_data_feeder import CSVDataFeeder
from utils.rest_api_client import RestApiClient

class ProviderFactory:
    @staticmethod
    async def create_data_provider(api_client_manager, config_manager, is_backtest, contract_map=None, redis_manager=None):
        """
        Creates instances of the REST API client and WebSocket manager based on the
        'data_provider' setting in the configuration.
        It uses the provided ApiClientManager and ConfigManager.
        """
        if is_backtest:
            logger.info("Backtest mode enabled. Using CSVDataFeeder for WebSocket and enabling REST client for historical data.")
            
            backtest_file = config_manager.get('settings', 'backtest_csv_path', fallback='tick_data_log.csv')
            websocket_manager = CSVDataFeeder(
                file_path=backtest_file,
                contract_map=contract_map,
                config_manager=config_manager
            )
            
            # Allow pure offline mode if API client manager is missing or has no clients
            rest_client = None
            if api_client_manager:
                rest_client = api_client_manager.get_active_client()

            if not rest_client:
                logger.warning("No active API client found for backtest. Operating in OFFLINE mode (CSV only).")
                class MockRest:
                    async def get_ltp(self, *args, **kwargs): return 0
                    async def get_ltps(self, *args, **kwargs): return {}
                    async def get_historical_candle_data(self, *args, **kwargs): return None
                    async def get_option_contracts(self, *args, **kwargs): return []
                    def get_active_client(self): return self
                rest_client = MockRest()

            return rest_client, websocket_manager

        provider_name = config_manager.get('settings', 'data_provider', fallback='upstox').lower()
        logger.info(f"Live mode enabled. Selected data provider: {provider_name}")

        if provider_name == 'upstox':
            # Check for Shared Redis Price Feed
            redis_enabled = config_manager.get_boolean('redis', 'enabled', fallback=False)
            price_feed_mode = config_manager.get('redis', 'price_feed_mode', fallback='direct').lower()

            if redis_enabled and price_feed_mode == 'shared' and redis_manager:
                from hub.redis_data_feed import RedisDataFeed

                logger.info("Using Shared Price Feed via Redis Pub/Sub.")
                websocket_manager = RedisDataFeed(redis_manager)
                rest_client = api_client_manager.get_active_client()
                return rest_client, websocket_manager

            from utils.websocket_manager import WebSocketManager

            # Create instances
            rest_client = api_client_manager.get_active_client()
            websocket_manager = WebSocketManager(api_client_manager)
            
            return rest_client, websocket_manager
        
        # --- Future providers can be added here ---
        # elif provider_name == 'newbroker':
        #     from utils.newbroker_client import NewBrokerClient
        #     from utils.newbroker_websocket import NewBrokerWebsocket
        #     ...
        
        else:
            logger.error(f"Unsupported data provider '{provider_name}' configured in config.ini")
            raise ValueError(f"Unsupported data provider: {provider_name}")
