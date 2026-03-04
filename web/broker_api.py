import configparser
import os
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter()
PROJECT_ROOT = Path(__file__).parent.parent
CREDS_FILE = PROJECT_ROOT / "config" / "credentials.ini"
CONFIG_FILE = PROJECT_ROOT / "config" / "config_trader.ini"

SENSITIVE_KEYS = {"api_secret", "totp", "pin", "password", "client_secret"}
MASK = "***"


def _mask(key: str, value: str) -> str:
    k = key.lower()
    if k in SENSITIVE_KEYS:
        return MASK
    if k in ("access_token", "api_key") and value and len(value) > 12:
        return value[:10] + "..."
    return value


def _load_creds():
    cfg = configparser.ConfigParser()
    cfg.read(str(CREDS_FILE))
    return cfg


def _save_creds(cfg: configparser.ConfigParser):
    tmp = CREDS_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        cfg.write(f)
    os.replace(tmp, CREDS_FILE)


@router.get("/brokers")
async def list_brokers():
    cfg = _load_creds()
    brokers = []
    for section in cfg.sections():
        items = {}
        for key, val in cfg.items(section):
            items[key] = _mask(key, val)
        brokers.append({"name": section, "fields": items})
    return JSONResponse({"brokers": brokers})


class TokenUpdate(BaseModel):
    token: str


@router.post("/brokers/{broker_name}/token")
async def update_token(broker_name: str, body: TokenUpdate):
    cfg = _load_creds()
    if broker_name not in cfg:
        raise HTTPException(status_code=404, detail=f"Broker '{broker_name}' not found")
    cfg[broker_name]["access_token"] = body.token.strip()
    _save_creds(cfg)
    return JSONResponse({"success": True, "message": f"Token updated for {broker_name}. Restart bot to apply."})


class ActiveBrokerUpdate(BaseModel):
    active_broker: str


@router.post("/brokers/active")
async def set_active_broker(body: ActiveBrokerUpdate):
    cfg = configparser.ConfigParser()
    cfg.read(str(CONFIG_FILE))
    if "settings" not in cfg:
        raise HTTPException(status_code=500, detail="settings section not found in config")
    cfg["settings"]["active_broker"] = body.active_broker
    tmp = CONFIG_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        cfg.write(f)
    os.replace(tmp, CONFIG_FILE)
    return JSONResponse({"success": True, "message": "Active broker updated. Restart bot to apply."})


class CredentialsUpdate(BaseModel):
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    client_id: Optional[str] = None
    access_token: Optional[str] = None
    pin: Optional[str] = None
    totp: Optional[str] = None


@router.post("/brokers/{broker_name}/credentials")
async def update_credentials(broker_name: str, body: CredentialsUpdate):
    cfg = _load_creds()
    if broker_name not in cfg:
        cfg.add_section(broker_name)
    fields = body.dict(exclude_none=True)
    for key, val in fields.items():
        if val:
            cfg[broker_name][key] = val.strip()
    _save_creds(cfg)
    return JSONResponse({"success": True, "message": f"Credentials updated for {broker_name}. Restart bot to apply."})


@router.get("/brokers/zerodha/accounts")
async def list_zerodha_accounts():
    creds = _load_creds()
    trader_cfg = configparser.ConfigParser()
    trader_cfg.read(str(CONFIG_FILE))
    candidates = []
    if 'Zerodha' in trader_cfg and trader_cfg.get('Zerodha', 'client_list', fallback=''):
        raw = trader_cfg.get('Zerodha', 'client_list', fallback='')
        candidates = [x.strip() for x in raw.split(',') if x.strip()]
    if not candidates:
        for section in trader_cfg.sections():
            if trader_cfg.get(section, 'client', fallback='').strip().lower() == 'zerodha':
                cred_section = trader_cfg.get(section, 'credentials', fallback=section).strip()
                candidates.append(cred_section)
    seen = set()
    result = []
    for name in candidates:
        cred_section = trader_cfg.get(name, 'credentials', fallback=name).strip() if name in trader_cfg else name
        if cred_section not in seen:
            seen.add(cred_section)
            result.append(cred_section)
    return JSONResponse({"accounts": sorted(result)})


@router.get("/brokers/zerodha/login-url")
async def zerodha_login_url(account: str):
    cfg = _load_creds()
    if account not in cfg:
        raise HTTPException(status_code=404, detail=f"Account '{account}' not found in credentials")
    api_key = cfg[account].get("api_key", "").strip()
    if not api_key:
        raise HTTPException(status_code=400, detail=f"No api_key found for '{account}'")
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=api_key)
        url = kite.login_url()
        return JSONResponse({"success": True, "login_url": url, "account": account})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class ZerodhaTokenExchange(BaseModel):
    account: str
    request_token: str


@router.post("/brokers/zerodha/exchange-token")
async def zerodha_exchange_token(body: ZerodhaTokenExchange):
    cfg = _load_creds()
    account = body.account.strip()
    request_token = body.request_token.strip()
    if account not in cfg:
        raise HTTPException(status_code=404, detail=f"Account '{account}' not found in credentials")
    api_key = cfg[account].get("api_key", "").strip()
    api_secret = cfg[account].get("api_secret", "").strip()
    if not api_key or not api_secret:
        raise HTTPException(status_code=400, detail=f"api_key or api_secret missing for '{account}'")
    try:
        from kiteconnect import KiteConnect
        kite = KiteConnect(api_key=api_key)
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
        cfg[account]["access_token"] = access_token
        _save_creds(cfg)
        return JSONResponse({
            "success": True,
            "message": f"Zerodha access token generated and saved for {account}. Restart bot to apply.",
            "token_preview": access_token[:10] + "..."
        })
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Token exchange failed: {str(e)}")
