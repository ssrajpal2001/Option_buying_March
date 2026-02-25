from .logger import logger
from .rest_api_client import RestApiClient
from .interactive_auth import get_new_token_interactively

class AuthHandler:
    def __init__(self, config_manager, credentials_section):
        self.config_manager = config_manager
        self.credentials_section = credentials_section
        self.api_client_manager = None

    def get_access_token(self):
        return self.config_manager.get(self.credentials_section, 'access_token')

    def switch_client(self):
        if self.api_client_manager:
            return self.api_client_manager.switch_to_next_client()
        return False

async def handle_login(config_manager, credentials_section='upstox_trading'):
    """
    Handles the Upstox authentication and verifies the access token's validity asynchronously.
    If the token is invalid, it prompts the user for a new one and updates it.
    """
    logger.info(f"Attempting to authenticate Upstox account: [{credentials_section}]")

    while True:
        try:
            auth_handler = AuthHandler(config_manager, credentials_section)
            api_client = RestApiClient(auth_handler)

            is_valid = await api_client.verify_authentication()

            if is_valid:
                logger.info(f"Authentication successful and token verified for [{credentials_section}].")
                return api_client
            else:
                logger.critical(f"AUTHENTICATION FAILED for account [{credentials_section}].")
                new_token = get_new_token_interactively(config_manager, credentials_section)
                if not new_token:
                    return None

        except Exception as e:
            logger.error(f"An unexpected error occurred during authentication for [{credentials_section}]: {e}", exc_info=True)
            return None
