from abc import ABC, abstractmethod
from utils.trade_logger import TradeLogger

class BaseBroker(ABC):
    """
    Abstract base class for all broker clients.
    Defines the standard interface for broker-specific operations.
    """
    def __init__(self, instance_name, config_manager, user_id=None, db_config=None):
        self.instance_name = instance_name
        self.config_manager = config_manager
        self.user_id = user_id
        self.db_config = db_config # Sourced from database (multi-tenant)
        self.state_manager = None
        self.is_connected = False
        self.trade_logger = TradeLogger()
        self._load_config()

    def _load_config(self):
        """Loads broker-specific configuration from DB (multi-tenant) or INI (legacy)."""
        if self.db_config:
            # Multi-tenant DB path
            self.mode = self.db_config.get('mode', 'paper')
            self.paper_trade = self.mode.lower() == 'paper'
            settings = self.db_config.get('broker_settings', {})
            instruments_str = settings.get('instruments_to_trade', '')
            self.instruments = {i.strip().upper() for i in instruments_str.split(',') if i.strip()}
            self.api_key = self.db_config.get('api_key')
            self.api_secret = self.db_config.get('api_secret') # Already decrypted by Manager
        else:
            # Legacy INI path
            self.mode = self.config_manager.get(self.instance_name, 'mode', fallback='paper')
            self.paper_trade = self.mode.lower() == 'paper'
            instruments_str = self.config_manager.get(self.instance_name, 'instruments_to_trade', fallback='')
            self.instruments = {i.strip().upper() for i in instruments_str.split(',') if i.strip()}

    def is_configured_for_instrument(self, instrument_name):
        """Checks if this broker instance is configured to trade the given instrument."""
        return instrument_name.upper() in self.instruments

    def set_state_manager(self, state_manager):
        """Receives the shared StateManager instance."""
        self.state_manager = state_manager

    @abstractmethod
    def connect(self):
        """Connects to the broker's API."""
        pass

    @abstractmethod
    async def handle_entry_signal(self, **kwargs):
        """Handles a trade entry signal."""
        pass

    @abstractmethod
    async def handle_close_signal(self, **kwargs):
        """Handles a trade exit signal."""
        pass

    @abstractmethod
    def place_order(self, contract, transaction_type, quantity):
        """
        Places an order with the broker. This method is responsible for
        generating the broker-specific symbol from the contract object.
        """
        pass
