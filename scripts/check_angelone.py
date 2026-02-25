import pyotp
import sys
import os
import datetime
import time
import requests

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from SmartApi import SmartConnect
from utils.config_manager import ConfigManager

def check_login():
    config = ConfigManager(config_file='config/config_trader.ini')
    creds = 'AngelOne_1'

    api_key = config.get(creds, 'api_key')
    client_code = config.get(creds, 'client_code')
    pin = config.get(creds, 'pin')
    totp_seed = config.get(creds, 'totp')

    print(f"--- Angel One Login Diagnostic ---")
    print(f"Client Code: {client_code}")
    print(f"API Key:     {api_key}")
    print(f"PIN:         {pin}")
    print(f"TOTP Seed:   {totp_seed}")

    if not all([api_key, client_code, pin, totp_seed]):
        print("ERROR: Missing credentials in credentials.ini")
        return

    # Time Drift Check
    print("\n--- Checking Time Synchronization ---")
    from utils.auth_manager_angelone import get_server_time_drift
    drift = get_server_time_drift()

    local_time = int(time.time())
    remote_time = local_time + drift

    print(f"Local UTC Time:  {datetime.datetime.fromtimestamp(local_time, tz=datetime.timezone.utc)}")
    print(f"Remote UTC Time: {datetime.datetime.fromtimestamp(remote_time, tz=datetime.timezone.utc)}")
    print(f"Clock Drift:     {drift} seconds")

    if drift == 0:
        print("WARNING: Could not determine drift, or drift is zero. If TOTP fails, check internet connectivity.")

    # Manual Offset Check for India (Common Mistake)
    local_now = datetime.datetime.now()
    if local_now.hour >= 5:
        ist_as_utc = local_now - datetime.timedelta(hours=5, minutes=30)
        print(f"\n--- India Clock Check ---")
        print(f"If your server is in India, your real UTC time should be: {ist_as_utc.strftime('%H:%M:%S')}")
        print(f"If your 'Local UTC Time' above matches IST, your clock is WRONG.")

    try:
        smart_api = SmartConnect(api_key=api_key.strip())

        # Generate TOTP (ensure robust seed format)
        seed = totp_seed.replace(" ", "").strip().upper()
        # Add padding if necessary
        missing_padding = len(seed) % 8
        if missing_padding:
            seed += '=' * (8 - missing_padding)

        totp_obj = pyotp.TOTP(seed)

        # Apply the same drift adjustment as the bot
        now_adjusted = datetime.datetime.now() + datetime.timedelta(seconds=drift)
        print("\n--- Generated TOTP Codes ---")
        print(f"Code for -30s: {totp_obj.at(now_adjusted - datetime.timedelta(seconds=30))}")
        print(f"Code for Now:  {totp_obj.at(now_adjusted)}    <--- Bot will use this")
        print(f"Code for +30s: {totp_obj.at(now_adjusted + datetime.timedelta(seconds=30))}")

        totp = totp_obj.at(now_adjusted)

        print("\nAttempting generateSession...")
        data = smart_api.generateSession(client_code.strip().upper(), str(pin).strip(), totp)

        if data['status']:
            print("SUCCESS: Login successful!")
            print(f"Name: {data['data']['name']}")
            print(f"Last Login: {data['data']['lastlogintime']}")
        else:
            print(f"FAILED: {data['message']}")
            print(f"Error Code: {data['errorcode']}")
            if data['errorcode'] == 'AB1050':
                print("Tip: 'Invalid totp' usually means either the Seed is wrong or your Clock is out of sync.")

    except Exception as e:
        print(f"EXCEPTION: {e}")

if __name__ == "__main__":
    check_login()
