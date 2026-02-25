from cryptography.fernet import Fernet
import os
import base64
from utils.logger import logger

class EncryptionManager:
    """
    Handles encryption/decryption of sensitive API keys using Fernet (AES-128).
    In a real production environment, the key should come from AWS KMS or Vault.
    """
    def __init__(self, master_key=None):
        if master_key:
            self.key = master_key
        else:
            # Fallback to env var or generate one for this session (not recommended for persistent DB)
            self.key = os.environ.get("TRADING_BOT_MASTER_KEY")
            if not self.key:
                logger.warning("TRADING_BOT_MASTER_KEY not found in environment. Sensitive data will be inaccessible.")
                # For development only, we can generate one if missing,
                # but in production this must be stable.
                self.key = Fernet.generate_key()

        try:
            self.fernet = Fernet(self.key)
        except Exception as e:
            logger.error(f"Invalid encryption key provided: {e}")
            raise

    def encrypt(self, plain_text: str) -> str:
        """Encrypts a string and returns a base64 encoded string."""
        if not plain_text:
            return ""
        return self.fernet.encrypt(plain_text.encode()).decode()

    def decrypt(self, encrypted_text: str) -> str:
        """Decrypts a base64 encoded string."""
        if not encrypted_text:
            return ""
        try:
            return self.fernet.decrypt(encrypted_text.encode()).decode()
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return ""
