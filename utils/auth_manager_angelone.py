import pyotp
import datetime
import time
import requests
import email.utils
from SmartApi import SmartConnect
from utils.logger import logger

def get_server_time_drift():
    """
    Calculates the time drift between the local machine and reliable sources.
    Returns the drift in seconds.
    """
    # Sources to try for time synchronization
    # We include both HTTPS and HTTP to bypass potential SSL-specific connection resets.
    # We also include a mix of global and Indian-proximate sources.
    sources = [
        "https://apiconnect.angelone.in",
        "https://www.google.com",
        "http://www.google.com",
        "https://api.upstox.com",
        "https://www.cloudflare.com",
        "http://worldtimeapi.org/api/timezone/Etc/UTC" # Fallback to user's previously tried source
    ]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': '*/*',
        'Connection': 'close' # Help prevent reset by peer by closing immediately
    }

    for url in sources:
        try:
            logger.info(f"AngelOne Auth: Attempting time sync with {url}...")
            start = time.time()

            # Use a Session for better connection management
            with requests.Session() as session:
                if "worldtimeapi" in url:
                    # Specialized handling for JSON time API
                    response = session.get(url, headers=headers, timeout=5)
                    server_timestamp = response.json().get('unixtime')
                    if not server_timestamp: continue
                else:
                    # Use GET with a small timeout but handle headers
                    # Some firewalls block HEAD requests, so we use GET.
                    response = session.get(url, headers=headers, timeout=5, stream=True)
                    server_date_str = response.headers.get('Date')
                    if not server_date_str:
                        logger.debug(f"AngelOne Auth: No 'Date' header from {url}")
                        continue
                    server_dt = email.utils.parsedate_to_datetime(server_date_str)
                    server_timestamp = server_dt.timestamp()

            end = time.time()

            # Approximate the actual time the server sent the response (adding half round-trip time)
            actual_server_time = server_timestamp + (end - start) / 2
            local_time = time.time()

            drift = int(actual_server_time - local_time)
            logger.info(f"AngelOne Auth: Sync successful via {url}. Drift: {drift}s")

            # Additional logic: If drift is almost exactly 5.5 hours, it's definitely the IST-as-UTC bug.
            if abs(drift - (-19800)) < 60:
                logger.info("AngelOne Auth: Confirmed IST-as-UTC clock misalignment (5.5h). Correcting.")

            return drift

        except Exception as e:
            logger.warning(f"AngelOne Auth: Could not sync time with {url}: {e}")
            continue

    # CRITICAL FALLBACK: If all network checks fail, we look at the system clock.
    # If the hour matches IST but the timezone is UTC, we assume the 5.5h drift.
    # This is a common Cloud9/EC2 issue in India.
    local_now = datetime.datetime.now()
    # If it's late evening in India (e.g. 19:00 IST) but the server thinks it's 19:00 UTC (which would be 00:30 IST tomorrow)
    # Angel One will fail.
    logger.error("AngelOne Auth: All time sync sources failed. Checking for common IST/UTC clock misalignment...")

    # Heuristic: If we are in UTC (timezone 0) and the system time is between 10:00 and 23:00,
    # but real UTC would likely be earlier, it's very probable this is an Indian server with IST set as UTC.
    if time.timezone == 0:
        logger.warning("AngelOne Auth: System is in UTC but network sync failed. Applying 5.5h (IST) drift correction as a best-effort fallback.")
        return -19800 # -5.5 hours in seconds

    return 0

def handle_angelone_login(credentials, config_manager=None):
    """
    Handles the authentication flow for Angel One SmartAPI.
    Expects credentials to be a dictionary or a section name from config.
    """
    try:
        if isinstance(credentials, str):
            # It's a section name
            api_key = config_manager.get(credentials, 'api_key')
            # MANDATORY: Angel One Client Code (e.g. S123456)
            username = config_manager.get(credentials, 'client_code') or \
                       config_manager.get(credentials, 'username')
            password = config_manager.get(credentials, 'pin')
            totp_key = config_manager.get(credentials, 'totp')
        else:
            # It's a dictionary (from DB)
            api_key = credentials.get('api_key')
            username = credentials.get('client_code') or \
                       credentials.get('username') or \
                       credentials.get('client_id')
            password = credentials.get('pin') or credentials.get('password')
            totp_key = credentials.get('totp')

        if not all([api_key, username, password, totp_key]):
            logger.error(f"AngelOne Login: Missing required credentials. Username: {username}, Has API Key: {bool(api_key)}, Has PIN: {bool(password)}, Has TOTP: {bool(totp_key)}")
            return None

        smart_api = SmartConnect(api_key=api_key.strip())

        # Generate TOTP (ensure no spaces in seed and base32 compatibility)
        totp_seed = totp_key.replace(" ", "").strip().upper()

        # Angel One TOTP seeds are base32. Ensure correct padding.
        missing_padding = len(totp_seed) % 8
        if missing_padding:
            totp_seed += '=' * (8 - missing_padding)

        try:
            # Apply time drift correction for Cloud9/EC2 environments with unsynced clocks
            drift = get_server_time_drift()
            adjusted_time = datetime.datetime.now() + datetime.timedelta(seconds=drift)
            totp = pyotp.TOTP(totp_seed).at(adjusted_time)
        except Exception as e:
            logger.error(f"AngelOne: Failed to generate TOTP from seed. Ensure seed is a valid Base32 string. Error: {e}")
            return None

        system_time = datetime.datetime.now().strftime('%H:%M:%S')
        logger.info(f"AngelOne: Attempting login for client {username} (TOTP: {totp}, System Time: {system_time})...")

        # Note: We use .strip().upper() as standard Angel One IDs are uppercase.
        login_id = username.strip().upper()
        login_pin = str(password).strip()

        data = smart_api.generateSession(login_id, login_pin, totp)

        if data.get('status'):
            logger.info(f"AngelOne: Session generated successfully for {username}")
            # Refresh token is also available in data['data']['refreshToken']
            return smart_api
        else:
            logger.error(f"AngelOne Login Failed for {username}: {data.get('message')}")
            return None

    except Exception as e:
        logger.error(f"Exception during AngelOne login: {e}", exc_info=True)
        return None
