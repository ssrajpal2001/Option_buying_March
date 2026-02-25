from abc import ABC, abstractmethod
from hub.event_bus import event_bus
from utils.logger import logger

class BaseClient(ABC):
    def __init__(self, broker_instance_name, config_manager):
        self.instance_name = broker_instance_name
        self.config_manager = config_manager
        self.state_manager = None # This will be injected by the BrokerManager

    def set_state_manager(self, state_manager):
        self.state_manager = state_manager

    def broadcast_trade_confirmation(self, **kwargs):
        """
        Publishes a trade confirmation event that the Orchestrator can listen to.
        This is crucial for backtesting where there's no real broker confirmation.
        """
        # Add the instance name to the data being broadcasted
        kwargs['broker_instance'] = self.instance_name
        logger.info(f"[{self.instance_name}] Broadcasting trade confirmation: {kwargs}")
        event_bus.publish('BROKER_TRADE_CONFIRMATION', trade_data=kwargs)

    @abstractmethod
    def handle_entry_signal(self, **kwargs):
        pass

    @abstractmethod
    def handle_close_signal(self, direction, **kwargs):
        pass

    @abstractmethod
    def place_order(self, symbol, direction):
        pass

    @abstractmethod
    def close_position(self, symbol, direction):
        pass

    @abstractmethod
    def get_ltp(self, symbol):
        pass

    @abstractmethod
    def close_all_positions(self):
        pass