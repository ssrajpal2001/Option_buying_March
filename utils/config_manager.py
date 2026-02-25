import configparser
import os

class ConfigManager:
    def __init__(self, config_file='config.ini'):
        # Construct absolute paths relative to this file's location
        base_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.join(base_dir, '..') # Assumes utils is one level down from root
        
        self.credentials_path = os.path.join(project_root, 'config', 'credentials.ini')
        if os.path.isabs(config_file):
            self.config_path = config_file
        else:
            # Robust path handling: check if 'config/' is already in the relative path
            if config_file.startswith('config' + os.sep) or config_file.startswith('config/'):
                self.config_path = os.path.join(project_root, config_file)
            else:
                self.config_path = os.path.join(project_root, 'config', config_file)

        self.credentials = self._load_ini(self.credentials_path)
        self.settings = self._load_ini(self.config_path)
        self.overrides = {} # section -> key -> value

    def set_override(self, section, key, value):
        """Sets a temporary runtime override for a configuration setting."""
        if section not in self.overrides:
            self.overrides[section] = {}
        self.overrides[section][key] = str(value)

    def _load_ini(self, path):
        config = configparser.ConfigParser()
        if not os.path.exists(path):
            raise FileNotFoundError(f"Configuration file not found at: {path}")
        config.read(path)
        return config

    def get_credential(self, section, key, fallback=None):
        return self.credentials.get(section, key, fallback=fallback)

    def set_credential(self, section, key, value):
        if not self.credentials.has_section(section):
            self.credentials.add_section(section)
        self.credentials.set(section, key, value)
        
        with open(self.credentials_path, 'w') as configfile:
            self.credentials.write(configfile)

    def get(self, section, key, fallback=None):
        # 1. Check Runtime Overrides (Highest Priority)
        if section in self.overrides and key in self.overrides[section]:
            return self.overrides[section][key]

        # 2. Check Settings File
        if self.settings.has_option(section, key):
            return self.settings.get(section, key)

        # 3. Check Credentials File
        if self.credentials.has_option(section, key):
            return self.credentials.get(section, key)

        return fallback

    def get_int(self, section, key, fallback=None):
        value = self.get(section, key, fallback=fallback)
        if value is not None and value is not fallback:
            return int(value)
        return fallback

    def get_boolean(self, section, key, fallback=False):
        value = self.get(section, key, fallback=str(fallback))
        if isinstance(value, bool):
            return value
        if value.lower() in ('true', '1', 't', 'y', 'yes'):
            return True
        return False

    def get_float(self, section, key, fallback=None):
        value = self.get(section, key, fallback=fallback)
        if value is not None and value is not fallback:
            return float(value)
        return fallback

    def get_section(self, section_name):
        if self.settings.has_section(section_name):
            return dict(self.settings.items(section_name))
        if self.credentials.has_section(section_name):
            return dict(self.credentials.items(section_name))
        return None

    def get_instrument_by_symbol(self, symbol):
        """Finds the instrument name (e.g. NIFTY) that matches the given underlying symbol."""
        for section in self.settings.sections():
            if self.settings.get(section, 'instrument_symbol', fallback='') == symbol:
                return section
        return None

    def get_data_providers(self):
        """
        Finds data provider credentials by discovering sections in credentials.ini
        that start with the names listed in the config file's [data_providers] section.
        """
        provider_string = self.get('data_providers', 'provider_list', fallback='')
        if not provider_string:
            return []

        # These are the base names to search for, e.g., ['upstox']
        base_provider_names = [name.strip().lower() for name in provider_string.split(',')]

        provider_credentials = []
        # Get all section names from the credentials file, e.g., ['upstox_1', 'upstox_2', 'zerodha_1']
        all_credential_sections = self.credentials.sections()

        # Iterate through each base name (e.g., 'upstox')
        for base_name in base_provider_names:
            # 1. Look for exact section name first (allows specific account selection to split load)
            exact_sections = [s for s in all_credential_sections if s.lower() == base_name]
            if exact_sections:
                for section_name in exact_sections:
                    creds = dict(self.credentials.items(section_name))
                    creds['name'] = section_name
                    provider_credentials.append(creds)
            else:
                # 2. Fallback to prefix match for legacy pooling behavior
                for section_name in all_credential_sections:
                    if section_name.lower().startswith(base_name):
                        creds = dict(self.credentials.items(section_name))
                        creds['name'] = section_name  # Use the actual section name for identification
                        provider_credentials.append(creds)

        if not provider_credentials:
            print(f"Warning: No credential sections found starting with the names in provider_list: {base_provider_names}")

        return provider_credentials

    def has_section(self, section_name):
        return self.settings.has_section(section_name) or self.credentials.has_section(section_name)

    def get_section(self, section_name):
        if self.settings.has_section(section_name):
            return dict(self.settings.items(section_name))
        if self.credentials.has_section(section_name):
            return dict(self.credentials.items(section_name))
        return None
