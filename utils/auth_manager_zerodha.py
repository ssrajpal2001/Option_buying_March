from kiteconnect import KiteConnect
from utils.logger import logger
import os

def handle_zerodha_login(credentials_section, config_manager):
    """
    Manages the Zerodha login process for a specific account,
    including handling the access token.
    Returns an authenticated KiteConnect client instance.
    """
    logger.info(f"Starting Zerodha authentication for section: {credentials_section}...")
    
    # --- FIX: Fetch API key and secret from credentials.ini, not config.ini ---
    api_key = config_manager.get_credential(credentials_section, 'api_key')
    api_secret = config_manager.get_credential(credentials_section, 'api_secret')

    if not api_key or not api_secret:
        raise ValueError(f"API key or secret not found in section '{credentials_section}'.")

    # Read the access token from the credentials file using the same section name
    access_token = config_manager.get_credential(credentials_section, 'access_token')

    kite = KiteConnect(api_key=api_key)

    if access_token and access_token != 'YOUR_ACCESS_TOKEN':
        try:
            logger.info(f"Validating Zerodha access token for {credentials_section}...")
            kite.set_access_token(access_token)
            kite.profile()
            logger.info(f"Zerodha access token for {credentials_section} is valid.")
            return kite
        except Exception as e:
            logger.warning(f"Zerodha access token validation failed for {credentials_section}: {e}. Proceeding to login.")
            access_token = None

    # If no valid access token, start the login flow
    logger.info(f"No valid Zerodha access token for {credentials_section}. Starting login flow.")
    login_url = kite.login_url()
    print(f"\nPlease login to Zerodha for account '{credentials_section}' using this URL: {login_url}")
    
    request_token = input("Enter the request_token from the redirect URL: ")
    
    try:
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
        logger.info(f"Zerodha session for {credentials_section} generated successfully.")
        
        # Save the new access token under its specific section
        config_manager.set_credential(credentials_section, 'access_token', access_token)
        logger.info(f"New Zerodha access token for {credentials_section} saved.")
        
        kite.set_access_token(access_token)
        return kite
        
    except Exception as e:
        logger.error(f"Failed to generate Zerodha session for {credentials_section}", exc_info=True)
        raise
