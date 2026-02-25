from .logger import logger
from .auth_manager import handle_login
from .exceptions import AuthenticationError
import itertools

class ApiClientManager:
    def __init__(self, config_manager):
        self.config_manager = config_manager
        self.clients = []
        self.client_cycle = None
        self.active_client = None

    async def async_init(self):
        """Asynchronously initializes a pool of API clients from data provider credentials."""
        logger.info("Initializing API Client Manager for data providers...")

        data_providers = self.config_manager.get_data_providers()
        if not data_providers:
            if self.config_manager.get_boolean('settings', 'backtest_enabled', fallback=False):
                logger.warning("No data providers found. Proceeding in OFFLINE backtest mode.")
                return
            else:
                logger.error("No data providers found in configuration. The bot will not be able to fetch market data.")
                return

        for provider_creds in data_providers:
            section_name = provider_creds.get('name')
            logger.info(f"Initializing data provider client from section: [{section_name}]")
            try:
                client = await handle_login(credentials_section=section_name, config_manager=self.config_manager)
                if not client:
                    raise AuthenticationError(f"Authentication process for {section_name} returned None.")
                    
                client.auth_handler.api_client_manager = self
                self.clients.append(client)
            except AuthenticationError as e:
                logger.critical(f"Failed to authenticate data provider [{section_name}]. Reason: {e}")
                logger.critical("The application cannot proceed without all data providers being authenticated. Shutting down.")
                raise SystemExit(f"Authentication failed for {section_name}.")
        
        if self.clients:
            self.client_cycle = itertools.cycle(self.clients)
            self.active_client = next(self.client_cycle)
            logger.info(f"API client pool initialized with {len(self.clients)} clients. Active client is set.")
        else:
            logger.error("Failed to initialize any data provider clients.")

    def get_active_client(self):
        """Returns the currently active API client."""
        return self.active_client

    def switch_to_next_client(self):
        """Switches the active client to the next one in the pool."""
        if not self.client_cycle:
            logger.error("Cannot switch clients: client pool is not initialized.")
            return False

        previous_client = self.active_client
        self.active_client = next(self.client_cycle)
        logger.warning(f"Switching active data provider. New active client corresponds to section: [{self.active_client.auth_handler.credentials_section}]")

        if self.active_client == previous_client and len(self.clients) > 1:
            logger.warning("Cycled through all available data providers and returned to the starting client.")

        return True

    async def close(self):
        """Closes the sessions of all API clients in the pool."""
        logger.info("Closing all data provider client sessions...")
        for client in self.clients:
            if client:
                await client.close()
