import importlib
from utils.logger import logger
from hub.event_bus import event_bus

from utils.encryption_manager import EncryptionManager
from utils.database_manager import DatabaseManager

class BrokerManager:
    def __init__(self, config_manager, db_manager=None):
        self.config_manager = config_manager
        self.db_manager = db_manager or DatabaseManager()
        self.encryption_manager = EncryptionManager()
        self.brokers = []
        self.state_manager = None

    def set_state_manager(self, state_manager):
        """Receives the shared StateManager instance from the main application."""
        self.state_manager = state_manager
        # Pass the state_manager to already loaded brokers
        for broker in self.brokers:
            broker.set_state_manager(self.state_manager)

    def _get_broker_class(self, client_name):
        """Dynamically imports and returns a broker client class."""
        module_name = None
        class_name = None
        try:
            if not client_name:
                raise ValueError("client_name cannot be None or empty.")
            module_name = f"brokers.{client_name.lower()}_client"
            class_name = f"{client_name}Client"
            module = importlib.import_module(module_name)
            return getattr(module, class_name)
        except (ImportError, AttributeError, ValueError) as e:
            logger.error(f"Could not find or load the client class '{class_name}' from '{module_name}'. Please check the file and class names. Error: {e}")
            return None

    async def load_brokers(self):
        """
        Commercial Path: Loads all active broker instances for all users from the Database.
        Fallback Path: Loads from .ini if DB is not populated or configured.
        """
        try:
            logger.debug("BrokerManager: Discovering multi-tenant brokers from database...")
            rows = await self.db_manager.get_active_users_and_brokers()

            if rows:
                for row in rows:
                    try:
                        # Decrypt secret
                        decrypted_secret = self.encryption_manager.decrypt(row['api_secret_encrypted'])
                        row['api_secret'] = decrypted_secret

                        client_name = row['broker_name']
                        broker_class = self._get_broker_class(client_name)

                        if broker_class:
                            # Instantiate with user_id and db_sourced config
                            broker_instance = broker_class(
                                broker_instance_name=row['instance_name'],
                                config_manager=self.config_manager,
                                user_id=row['user_id'],
                                db_config=row
                            )
                            if self.state_manager:
                                broker_instance.set_state_manager(self.state_manager)
                            self.brokers.append(broker_instance)
                            logger.info(f"Loaded DB Broker: User={row['email']} | {row['instance_name']} ({client_name})")
                    except Exception as e:
                        logger.error(f"Failed to load DB broker {row.get('instance_name')}: {e}")

                if self.brokers:
                    return # Successfully loaded from DB

        except Exception as e:
            logger.warning(f"Database broker discovery skipped or failed: {e}. Falling back to .ini")

        # --- FALLBACK TO LEGACY INI LOADING ---
        active_broker_sections_str = self.config_manager.get('settings', 'active_broker', fallback='')
        active_broker_sections = [b.strip() for b in active_broker_sections_str.split(',') if b.strip()]

        if not active_broker_sections:
            logger.warning("No active brokers are defined in the [settings] section under 'active_broker'.")
            return

        for broker_section in active_broker_sections:
            if not self.config_manager.has_section(broker_section):
                logger.error(f"Configuration section '[{broker_section}]' not found for the active broker.")
                continue

            client_name = self.config_manager.get(broker_section, 'client')
            broker_class = self._get_broker_class(client_name)

            if broker_class:
                try:
                    broker_instance = broker_class(broker_section, self.config_manager)
                    if self.state_manager:
                        broker_instance.set_state_manager(self.state_manager)
                    self.brokers.append(broker_instance)
                    logger.info(f"Successfully loaded broker: {broker_section} (Client: {client_name})")
                except Exception as e:
                    logger.error(f"An unexpected error occurred while loading broker {broker_section}: {e}", exc_info=True)


    async def handle_execute_trade_request(self, trade_data):
        """
        Commercial Route: Sends the trade request only to the brokers belonging to the specific user.
        """
        instrument_name = trade_data.get("instrument_name")
        target_user_id = trade_data.get("user_id")

        if not instrument_name:
            logger.error(f"Trade request is missing 'instrument_name'. Cannot route.")
            return

        logger.info(f"BrokerManager routing {instrument_name} signal for user_id={target_user_id}")

        for broker in self.brokers:
            # Check if this broker instance belongs to the target user
            # and if it is configured for this instrument
            is_user_match = (target_user_id is None) or (broker.user_id == target_user_id)

            if is_user_match and broker.is_configured_for_instrument(instrument_name):
                try:
                    logger.info(f"Executing trade on broker '{broker.instance_name}' (User: {broker.user_id})")
                    await broker.handle_entry_signal(**trade_data)
                except Exception as e:
                    logger.error(f"Error executing trade on {broker.instance_name}: {e}", exc_info=True)

    async def handle_exit_request(self, exit_data):
        """
        Commercial Route: Sends the exit request only to the brokers belonging to the specific user.
        """
        instrument_name = exit_data.get("instrument_name")
        target_user_id = exit_data.get("user_id")

        if not instrument_name:
            logger.error(f"Exit request is missing 'instrument_name'. Cannot route.")
            return

        logger.info(f"BrokerManager routing exit signal for user_id={target_user_id}")

        for broker in self.brokers:
            is_user_match = (target_user_id is None) or (broker.user_id == target_user_id)

            if is_user_match and broker.is_configured_for_instrument(instrument_name):
                try:
                    logger.info(f"Executing exit on broker '{broker.instance_name}' (User: {broker.user_id})")
                    await broker.handle_close_signal(**exit_data)
                except Exception as e:
                    logger.error(f"Error executing exit on {broker.instance_name}: {e}", exc_info=True)

    def broadcast_entry_signal(self, **kwargs):
        """Helper to initiate an entry signal to all brokers."""
        logger.info(f"DIAGNOSTIC: Broadcasting entry signal with data: {kwargs}")
        if not self.brokers:
            logger.error("DIAGNOSTIC: No brokers loaded. Cannot broadcast entry signal.")
            return

        for broker in self.brokers:
            try:
                logger.info(f"DIAGNOSTIC: Calling handle_entry_signal for broker '{broker.instance_name}'...")
                broker.handle_entry_signal(**kwargs)
                logger.info(f"DIAGNOSTIC: Call to handle_entry_signal for broker '{broker.instance_name}' complete.")
            except Exception as e:
                logger.error(f"DIAGNOSTIC: Error broadcasting entry signal to broker '{broker.instance_name}': {e}", exc_info=True)

    def broadcast_close_signal(self, **kwargs):
        """Helper to initiate a close signal to all brokers."""
        logger.info(f"Broadcasting close signal with data: {kwargs}")
        for broker in self.brokers:
            try:
                broker.handle_close_signal(**kwargs)
            except Exception as e:
                logger.error(f"Error broadcasting close signal to broker '{broker.instance_name}': {e}", exc_info=True)

    def close_all_positions(self):
        """Instructs all brokers to close any open positions."""
        logger.info("Broadcasting 'close all positions' to all brokers.")
        for broker in self.brokers:
            try:
                broker.close_all_positions()
            except Exception as e:
                logger.error(f"Error instructing broker '{broker.instance_name}' to close all positions: {e}", exc_info=True)

    def get_all_unique_instruments(self):
        """
        Scans all loaded broker instances to find all the unique instruments they are configured to trade.
        """
        unique_instruments = set()

        # In multi-client mode, `active_broker` lists the sections for each client.
        active_broker_sections_str = self.config_manager.get('settings', 'active_broker', fallback='')
        active_broker_sections = [b.strip() for b in active_broker_sections_str.split(',') if b.strip()]

        if not active_broker_sections:
            logger.warning("No active brokers defined in settings. Cannot determine instruments to trade.")
            return unique_instruments

        for section in active_broker_sections:
            instruments_str = self.config_manager.get(section, 'instruments_to_trade', fallback='')
            if instruments_str:
                instruments = [i.strip().upper() for i in instruments_str.split(',')]
                unique_instruments.update(instruments)
            else:
                logger.warning(f"Broker section '[{section}]' is active but has no 'instruments_to_trade' defined.")

        return unique_instruments

    def get_broker_instance(self, broker_section_name):
        """
        Factory method to create a temporary, non-trading instance of a broker client.
        This is used by the reporting module to access broker-specific formatting
        without interfering with live trading instances.
        """
        if not self.config_manager.has_section(broker_section_name):
            logger.error(f"Configuration section '[{broker_section_name}]' not found for the requested broker instance.")
            return None

        client_name = self.config_manager.get(broker_section_name, 'client')
        broker_class = self._get_broker_class(client_name)

        if broker_class:
            try:
                # Instantiate the client without requiring a login, safe for reporting.
                broker_instance = broker_class(broker_section_name, self.config_manager, login_required=False)
                return broker_instance
            except Exception as e:
                logger.error(f"Failed to create temporary instance of broker {broker_section_name}: {e}", exc_info=True)
                return None
        return None

    def shutdown(self):
        """Gracefully shuts down all broker instances and clears the list."""
        logger.debug("BrokerManager shutdown called.")
        for broker in self.brokers:
            try:
                # If the broker client has a shutdown method, call it
                if hasattr(broker, 'shutdown'):
                    broker.shutdown()
            except Exception as e:
                logger.error(f"Error shutting down broker: {e}")
        self.brokers.clear()
