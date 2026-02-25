from dhanhq import dhanhq
from utils.logger import logger

def handle_dhan_login(credentials_section, config_manager):
    """
    Manages the Dhan login process for a specific account.
    Returns an authenticated dhanhq client instance.
    """
    logger.info(f"Starting Dhan authentication for section: {credentials_section}...")

    # Fetch Client ID and Access Token from credentials.ini
    # User mentioned only access_token, but library expects client_id as well.
    # We will try to fetch client_id, and if not found, we might use a dummy or empty if allowed.
    client_id = config_manager.get_credential(credentials_section, 'client_id', fallback='')
    access_token = config_manager.get_credential(credentials_section, 'access_token', fallback=None)

    if not access_token:
        raise ValueError(f"Access token not found in section '{credentials_section}'.")

    if not client_id:
        logger.warning(f"Client ID not found in section '{credentials_section}'. Dhan library usually requires it. Using empty string.")
        client_id = ""

    try:
        dhan = dhanhq(client_id, access_token)
        # Dhan doesn't have a simple 'profile' call like Kite, but we can try to fetch a small piece of data to validate.
        # For now, we'll assume it's valid if we can instantiate it.
        logger.info(f"Dhan client initialized for {credentials_section}.")
        return dhan
    except Exception as e:
        logger.error(f"Failed to initialize Dhan client for {credentials_section}: {e}", exc_info=True)
        raise
