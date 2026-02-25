from utils.logger import logger

class ConfigValidator:
    def __init__(self, config_manager):
        self.config = config_manager
        self.errors = []

    def validate(self, context='trader'):
        """
        Runs validation checks based on the context ('trader' or 'recorder')
        and returns True if the configuration is valid, otherwise False.
        """
        self.errors = [] # Reset errors before each validation run
        logger.info(f"Starting configuration validation for context: '{context}'...")

        if context == 'trader':
            self._validate_settings_section()
            self._validate_instrument_sections()
            self._validate_broker_sections()
        elif context == 'recorder':
            self._validate_recorder_settings()
            self._validate_instrument_sections() # Recorders also need instrument info
        else:
            self.errors.append(f"Unknown validation context: '{context}'")

        if self.errors:
            logger.error("Configuration validation failed with the following errors:")
            for error in self.errors:
                logger.error(f"- {error}")
            return False
        
        logger.info(f"Configuration validation for '{context}' successful.")
        return True

    def _check_required_key(self, section, key, value_type):
        """
        Checks if a required key exists in a section and is of the correct type.
        """
        try:
            if value_type == 'float':
                self.config.get_float(section, key)
            elif value_type == 'int':
                self.config.get_int(section, key)
            elif value_type == 'str':
                value = self.config.get(section, key)
                if not value:
                    raise ValueError("String value cannot be empty.")
            else:
                 self.config.get(section, key) # For any other type, just check existence
        except (ValueError, KeyError, Exception) as e:
            self.errors.append(f"In section '[{section}]', key '{key}' is missing, empty, or has an invalid format. Expected type: {value_type}. Error: {e}")

    def _validate_recorder_settings(self):
        """Validates the [settings] section for the data_recorder."""
        section = 'settings'
        required_keys = {
            'instrument_to_trade': 'str',
        }
        for key, value_type in required_keys.items():
            self._check_required_key(section, key, value_type)

    def _validate_settings_section(self):
        """Validates the [settings] section of the config for the trader."""
        section = 'settings'
        required_keys = {
            'instrument_to_trade': 'str',
            'start_time': 'str',
            'end_time': 'str',
            'default_qty': 'int'
        }
        for key, value_type in required_keys.items():
            self._check_required_key(section, key, value_type)
            


    def _validate_instrument_sections(self):
        """Validates the instrument-specific sections (e.g., [NIFTY])."""
        instruments_str = self.config.get('settings', 'instrument_to_trade', fallback=None)
        if not instruments_str:
            self.errors.append("'instrument_to_trade' not defined in [settings], cannot validate instrument section.")
            return

        instruments = [i.strip().upper() for i in instruments_str.split(',')]

        for instrument_name in instruments:
            required_keys = {
                'instrument_symbol': 'str',
            }
            # Lot size and strike interval are typically needed for the trader
            required_keys['strike_interval'] = 'int'
            required_keys['lot_size'] = 'int'

            for key, value_type in required_keys.items():
                self._check_required_key(instrument_name, key, value_type)

    def _validate_broker_sections(self):
        """Validates the broker configuration sections."""
        enabled_brokers_str = self.config.get('brokers', 'enabled_brokers', fallback='')
        if not enabled_brokers_str:
            logger.warning("No brokers are enabled in the [brokers] section. This is valid, but no trades will be executed.")
            return

        enabled_brokers = [b.strip() for b in enabled_brokers_str.split(',')]
        for broker_instance in enabled_brokers:
            try:
                # Every broker section must have a 'client' key.
                self.config.get(broker_instance, 'client')
            except Exception:
                 self.errors.append(f"Broker instance '{broker_instance}' is enabled, but its configuration section '[{broker_instance}]' is missing or invalid.")
                 continue

            # Validate common keys for all brokers
            self._check_required_key(broker_instance, 'client', 'str')
            self._check_required_key(broker_instance, 'mode', 'str')
            self._check_required_key(broker_instance, 'credentials', 'str')
