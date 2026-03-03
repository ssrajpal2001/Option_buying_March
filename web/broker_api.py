import configparser
import os
from pathlib import Path
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional

router = APIRouter()
CREDS_FILE = Path("config/credentials.ini")
CONFIG_FILE = Path("config/config_trader.ini")

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
