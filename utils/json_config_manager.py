import json
import os
import re
from utils.logger import logger

class JsonConfigManager:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(JsonConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, json_file='config/strategy_logic.json'):
        if self._initialized:
            return

        base_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.join(base_dir, '..')

        if os.path.isabs(json_file):
            self.json_path = json_file
        else:
            self.json_path = os.path.join(project_root, json_file)

        self.data = {}
        self.load()
        self._initialized = True

    def load(self):
        try:
            if os.path.exists(self.json_path):
                with open(self.json_path, 'r') as f:
                    self.data = json.load(f)
                logger.info(f"Strategy logic loaded from {self.json_path}")
            else:
                logger.warning(f"Strategy logic file not found: {self.json_path}")
                self.data = {}
        except Exception as e:
            logger.error(f"Error loading strategy logic JSON: {e}")
            self.data = {}

    def get_instrument_settings(self, instrument):
        return self.data.get(instrument.upper(), {})

    def get_mode_settings(self, instrument, mode):
        """Returns 'buy' or 'sell' settings for the instrument."""
        inst_settings = self.get_instrument_settings(instrument)
        return inst_settings.get(mode.lower(), {})

    def get_active_modes(self, instrument):
        """Returns a list of active modes (buy and/or sell) for the instrument."""
        inst_settings = self.get_instrument_settings(instrument)
        active_modes = []
        for mode in ['buy', 'sell']:
            if inst_settings.get(mode, {}).get('enabled', False):
                active_modes.append(mode)
        return active_modes

    def get_value(self, path, fallback=None):
        """
        Retrieves a value from the JSON using a dot-separated path.
        Example: get_value('NIFTY.buy.indicators.vwap.tf')
        Handles both dot and slash separators for backward compatibility.
        """
        keys = path.replace('/', '.').split('.')
        val = self.data
        try:
            for key in keys:
                val = val[key]
            return val
        except (KeyError, TypeError):
            return fallback

    def evaluate_formula(self, formula, results_dict):
        """Evaluates a logical formula string against a dictionary of boolean results."""
        if not formula:
            return False

        eval_str = formula.lower()
        for key, val in results_dict.items():
            # Use regex for word-boundary matching to avoid partial matches
            eval_str = re.sub(rf'\b{key.lower()}\b', str(val), eval_str)

        # Replace any remaining unresolved identifiers (e.g. cross_slope_comparison not in
        # results_dict) with False so the formula still evaluates cleanly.
        _keywords = {'and', 'or', 'not', 'true', 'false'}
        eval_str = re.sub(
            r'\b[a-z][a-z0-9_]*\b',
            lambda m: m.group(0) if m.group(0) in _keywords else 'false',
            eval_str
        )

        # Basic sanitization: only allow ( ), and, or, True, False, space, not
        allowed_chars = set("() andortruefalsenot ")
        if not all(c in allowed_chars for c in eval_str.lower()):
            logger.error(f"Invalid characters in formula: {formula}. Evaluated as: {eval_str}")
            return False

        # Capitalize only True and False for Python's eval
        eval_str = eval_str.replace('true', 'True').replace('false', 'False')

        try:
            return eval(eval_str)
        except Exception as e:
            logger.error(f"Error evaluating formula {formula}: {e}")
            return False
