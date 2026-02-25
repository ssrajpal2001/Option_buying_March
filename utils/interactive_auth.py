import sys
from .logger import logger

def get_new_token_interactively(config_manager, credentials_section):
    """
    Handles the interactive process of asking the user for a new access token.

    This function checks if the application is running in an interactive terminal.
    If so, it prompts the user to enter a new token. If not, it returns None.

    Args:
        config_manager: The application's ConfigManager instance.
        credentials_section (str): The section name in credentials.ini to update.

    Returns:
        str: The new token if provided by the user, otherwise None.
    """
    if not sys.stdout.isatty():
        logger.critical("Non-interactive mode detected. Cannot prompt for new access token.")
        return None

    try:
        print(f"\n--- ACTION REQUIRED ---")
        print(f"The access token for '{credentials_section}' is invalid or expired.")
        new_token = input(f"Please paste the new access token for '{credentials_section}' and press Enter: ").strip()

        if new_token:
            config_manager.set_credential(credentials_section, 'access_token', new_token)
            logger.info(f"Access token for '{credentials_section}' has been updated.")
            return new_token
        else:
            logger.critical("No new token was provided. Exiting.")
            sys.exit(1)

    except (EOFError, KeyboardInterrupt):
        logger.critical("\nUser cancelled the token input. Shutting down.")
        sys.exit(1)
