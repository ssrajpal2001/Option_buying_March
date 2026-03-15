import json
import time
import hashlib
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import Optional
from web.deps import get_current_user
from web.db import db_fetchone, db_fetchall, db_execute
from web.auth import encrypt_secret, decrypt_secret, _fernet
from hub.instance_manager import instance_manager

router = APIRouter(prefix="/client", tags=["client"])

IST = timezone(timedelta(hours=5, minutes=30))
KITE_LOGIN_URL = "https://kite.trade/connect/login?v=3&api_key={api_key}"


def _is_token_fresh(token_updated_at: str) -> bool:
    if not token_updated_at:
        return False
    try:
        updated = datetime.fromisoformat(token_updated_at).replace(tzinfo=IST)
        now_ist = datetime.now(IST)
        today_6am = now_ist.replace(hour=6, minute=0, second=0, microsecond=0)
        if now_ist < today_6am:
            today_6am -= timedelta(days=1)
        return updated > today_6am
    except Exception:
        return False


def _is_dhan_token_fresh(token_updated_at: str) -> bool:
    if not token_updated_at:
        return False
    try:
        updated = datetime.fromisoformat(token_updated_at).replace(tzinfo=IST)
        now_ist = datetime.now(IST)
        return (now_ist - updated).days < 30
    except Exception:
        return False


def _get_active_instance(user_id: int, broker: str = None):
    if broker:
        instance = db_fetchone(
            "SELECT * FROM client_broker_instances WHERE client_id=? AND broker=? AND status != 'removed' ORDER BY CASE WHEN status='running' THEN 0 ELSE 1 END, id DESC LIMIT 1",
            (user_id, broker)
        )
    else:
        instance = db_fetchone(
            "SELECT * FROM client_broker_instances WHERE client_id=? AND status != 'removed' ORDER BY CASE WHEN status='running' THEN 0 ELSE 1 END, id DESC LIMIT 1",
            (user_id,)
        )
    return instance


# ── Broker Setup ─────────────────────────────────────────────────────────────

class BrokerSetup(BaseModel):
    broker: str = "zerodha"
    api_key: str
    api_secret: Optional[str] = ""
    access_token: Optional[str] = ""
    trading_mode: str = "paper"
    instrument: str = "NIFTY"
    quantity: int = 25
    strategy_version: str = "V3"


@router.get("/broker")
async def get_broker_config(user=Depends(get_current_user)):
    rows = db_fetchall(
        "SELECT id, broker, trading_mode, instrument, quantity, strategy_version, status, last_heartbeat, token_updated_at FROM client_broker_instances WHERE client_id=? AND status != 'removed'",
        (user["id"],)
    )
    result = []
    for r in rows:
        d = dict(r)
        if d["broker"] == "dhan":
            d["token_fresh"] = _is_dhan_token_fresh(d.get("token_updated_at"))
        else:
            d["token_fresh"] = _is_token_fresh(d.get("token_updated_at"))
        result.append(d)
    return {"instances": result, "max_brokers": user["max_brokers"]}


@router.post("/broker")
async def save_broker_config(body: BrokerSetup, user=Depends(get_current_user)):
    if body.broker not in ("zerodha", "dhan"):
        raise HTTPException(400, "Broker must be 'zerodha' or 'dhan'.")

    existing = db_fetchall(
        "SELECT id FROM client_broker_instances WHERE client_id=? AND status != 'removed'", (user["id"],)
    )
    max_b = user["max_brokers"]
    if len(existing) >= max_b:
        existing_broker = db_fetchone(
            "SELECT id FROM client_broker_instances WHERE client_id=? AND broker=? AND status != 'removed'",
            (user["id"], body.broker)
        )
        if not existing_broker:
            raise HTTPException(400, f"Your plan allows {max_b} broker(s). Upgrade to Premium for more.")

    if body.trading_mode not in ("paper", "live"):
        raise HTTPException(400, "trading_mode must be 'paper' or 'live'")

    existing_row = db_fetchone(
        "SELECT api_key_encrypted, api_secret_encrypted, access_token_encrypted FROM client_broker_instances WHERE client_id=? AND broker=?",
        (user["id"], body.broker)
    )

    is_new_key = body.api_key and body.api_key != "unchanged"
    is_new_secret = body.api_secret and body.api_secret != "unchanged"
    is_new_token = body.access_token and body.access_token != "unchanged"

    if body.broker == "dhan":
        if existing_row:
            enc_key = encrypt_secret(body.api_key) if is_new_key else existing_row["api_key_encrypted"]
            enc_token = encrypt_secret(body.access_token) if is_new_token else existing_row["access_token_encrypted"]
        else:
            if not is_new_key:
                raise HTTPException(400, "Client ID is required for first-time Dhan setup.")
            if not is_new_token:
                raise HTTPException(400, "Access Token is required for first-time Dhan setup.")
            enc_key = encrypt_secret(body.api_key)
            enc_token = encrypt_secret(body.access_token)

        if is_new_token:
            token_ts = datetime.now(IST).isoformat()
        elif existing_row:
            token_ts_row = db_fetchone(
                "SELECT token_updated_at FROM client_broker_instances WHERE client_id=? AND broker='dhan'",
                (user["id"],)
            )
            token_ts = token_ts_row["token_updated_at"] if token_ts_row and token_ts_row["token_updated_at"] else datetime.now(IST).isoformat()
        else:
            token_ts = datetime.now(IST).isoformat()

        db_execute("""
            INSERT INTO client_broker_instances
              (client_id, broker, api_key_encrypted, access_token_encrypted,
               token_updated_at, trading_mode, instrument, quantity, strategy_version)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT(client_id, broker) DO UPDATE SET
              api_key_encrypted=excluded.api_key_encrypted,
              access_token_encrypted=excluded.access_token_encrypted,
              token_updated_at=excluded.token_updated_at,
              trading_mode=excluded.trading_mode,
              instrument=excluded.instrument,
              quantity=excluded.quantity,
              strategy_version=excluded.strategy_version
        """, (user["id"], "dhan", enc_key, enc_token, token_ts,
              body.trading_mode, body.instrument, body.quantity, body.strategy_version))

        msg = "Dhan credentials saved. Your access token is valid for 30 days."
        if not is_new_key and not is_new_token:
            msg = "Trading configuration updated."
        return {"success": True, "message": msg}

    if existing_row:
        enc_key = encrypt_secret(body.api_key) if is_new_key else existing_row["api_key_encrypted"]
        enc_secret = encrypt_secret(body.api_secret) if is_new_secret else existing_row["api_secret_encrypted"]
    else:
        if not is_new_key or not is_new_secret:
            raise HTTPException(400, "API Key and API Secret are required for first-time setup.")
        enc_key = encrypt_secret(body.api_key)
        enc_secret = encrypt_secret(body.api_secret)

    db_execute("""
        INSERT INTO client_broker_instances
          (client_id, broker, api_key_encrypted, api_secret_encrypted,
           trading_mode, instrument, quantity, strategy_version)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(client_id, broker) DO UPDATE SET
          api_key_encrypted=excluded.api_key_encrypted,
          api_secret_encrypted=excluded.api_secret_encrypted,
          trading_mode=excluded.trading_mode,
          instrument=excluded.instrument,
          quantity=excluded.quantity,
          strategy_version=excluded.strategy_version
    """, (user["id"], "zerodha", enc_key, enc_secret,
          body.trading_mode, body.instrument, body.quantity, body.strategy_version))

    msg = "Broker credentials saved. Click 'Login with Zerodha' to generate your access token."
    if not is_new_key and not is_new_secret:
        msg = "Trading configuration updated."
    return {"success": True, "message": msg}


# ── Zerodha OAuth ────────────────────────────────────────────────────────────

@router.get("/zerodha/login-url")
async def zerodha_login_url(user=Depends(get_current_user)):
    instance = db_fetchone(
        "SELECT * FROM client_broker_instances WHERE client_id=? AND broker='zerodha'",
        (user["id"],)
    )
    if not instance or not instance.get("api_key_encrypted"):
        raise HTTPException(400, "Save your API Key and API Secret in Settings first.")

    api_key = decrypt_secret(instance["api_key_encrypted"])
    if not api_key:
        raise HTTPException(400, "Could not decrypt API key. Please re-enter your credentials.")

    state_payload = f'{user["id"]}:{int(time.time())}'
    state_encrypted = _fernet.encrypt(state_payload.encode()).decode()

    login_url = KITE_LOGIN_URL.format(api_key=api_key)
    login_url += "&redirect_params=state=" + urllib.parse.quote(state_encrypted)

    return {"success": True, "login_url": login_url}


# ── Bot Control ───────────────────────────────────────────────────────────────

class BotStartRequest(BaseModel):
    broker: Optional[str] = None


@router.post("/bot/start")
async def start_bot(body: BotStartRequest = BotStartRequest(), user=Depends(get_current_user)):
    requested_broker = body.broker if body.broker and body.broker in ("zerodha", "dhan") else None
    instance = _get_active_instance(user["id"], broker=requested_broker)
    if not instance:
        raise HTTPException(400, "No broker configured. Please set up your broker first.")
    if not instance.get("api_key_encrypted"):
        raise HTTPException(400, "Broker credentials missing. Please re-enter your credentials.")
    if not instance.get("access_token_encrypted"):
        if instance["broker"] == "zerodha":
            raise HTTPException(400, "Access token missing. Click 'Login with Zerodha' in Settings to generate your token.")
        else:
            raise HTTPException(400, "Access token missing. Please enter your Dhan access token in Settings.")

    if instance["broker"] == "zerodha":
        if not _is_token_fresh(instance.get("token_updated_at")):
            raise HTTPException(400, "Zerodha token expired (resets daily at 6:00 AM IST). Go to Settings and click 'Login with Zerodha' to reconnect.")
    elif instance["broker"] == "dhan":
        if not _is_dhan_token_fresh(instance.get("token_updated_at")):
            raise HTTPException(400, "Dhan access token expired (30-day validity). Go to Settings and enter a new access token from Dhan's developer portal.")

    pending_change = db_fetchone(
        "SELECT id FROM broker_change_requests WHERE client_id=? AND status='pending'",
        (user["id"],)
    )
    if pending_change:
        raise HTTPException(400, "You have a pending broker change request. Please wait for admin approval before starting the bot.")

    dp = db_fetchone("SELECT * FROM data_providers WHERE provider='upstox'")
    if not dp or dp["status"] != "configured":
        raise HTTPException(503, "Data provider not configured by admin. Please contact admin.")

    if instance["status"] == "running":
        return {"success": False, "message": "Bot is already running."}

    ok, msg = instance_manager.start_instance(
        instance_id=instance["id"],
        client_id=user["id"],
        username=user["username"],
        broker=instance["broker"],
        instrument=instance["instrument"],
        quantity=instance["quantity"],
        strategy_version=instance["strategy_version"],
        trading_mode=instance["trading_mode"],
        api_key=decrypt_secret(instance["api_key_encrypted"]),
        access_token=decrypt_secret(instance["access_token_encrypted"]),
        upstox_api_key=decrypt_secret(dp["api_key_encrypted"]) if dp.get("api_key_encrypted") else "",
        upstox_token=decrypt_secret(dp["access_token_encrypted"]) if dp.get("access_token_encrypted") else "",
    )
    if ok:
        db_execute("UPDATE client_broker_instances SET status='running' WHERE id=?", (instance["id"],))
    return {"success": ok, "message": msg}


@router.post("/bot/stop")
async def stop_bot(user=Depends(get_current_user)):
    instance = _get_active_instance(user["id"])
    if not instance:
        raise HTTPException(400, "No broker configured.")

    ok, msg = instance_manager.stop_instance(instance["id"])
    if ok:
        db_execute("UPDATE client_broker_instances SET status='idle', bot_pid=NULL WHERE id=?", (instance["id"],))
    return {"success": ok, "message": msg}


@router.post("/bot/restart")
async def restart_bot(body: BotStartRequest = BotStartRequest(), user=Depends(get_current_user)):
    await stop_bot(user=user)
    return await start_bot(body=body, user=user)


@router.get("/bot/status")
async def bot_status(user=Depends(get_current_user)):
    instance = _get_active_instance(user["id"])
    if not instance:
        return {"configured": False}

    live_status = instance_manager.get_instance_status(instance["id"])
    trades = db_fetchall("""
        SELECT trade_type, direction, strike, entry_price, exit_price,
               pnl_pts, pnl_rs, exit_reason, closed_at
        FROM trade_history WHERE instance_id=? ORDER BY closed_at DESC LIMIT 50
    """, (instance["id"],))

    bot_data = {}
    status_file = Path(f'config/bot_status_client_{user["id"]}.json')
    if status_file.exists():
        try:
            with open(status_file, 'r') as f:
                bot_data = json.load(f)
            heartbeat = bot_data.get('heartbeat', 0)
            age = time.time() - heartbeat
            bot_data['stale'] = age > 30
            bot_data['stale_seconds'] = round(age)
        except (json.JSONDecodeError, OSError):
            bot_data = {}

    inst_dict = dict(instance)
    safe_keys = ["id", "broker", "status", "trading_mode", "instrument", "quantity", "strategy_version", "last_heartbeat", "token_updated_at"]
    inst_safe = {k: inst_dict.get(k) for k in safe_keys}

    return {
        "configured": True,
        "instance": inst_safe,
        "live": live_status,
        "trade_history": trades,
        "bot_data": bot_data,
    }


# ── Broker Change Requests ────────────────────────────────────────────────

class BrokerChangeRequest(BaseModel):
    current_broker: str
    requested_broker: str
    reason: Optional[str] = ""


@router.post("/broker-change-request")
async def submit_broker_change_request(body: BrokerChangeRequest, user=Depends(get_current_user)):
    if body.current_broker not in ("zerodha", "dhan") or body.requested_broker not in ("zerodha", "dhan"):
        raise HTTPException(400, "Invalid broker name.")
    if body.current_broker == body.requested_broker:
        raise HTTPException(400, "Current and requested broker cannot be the same.")

    current_instance = db_fetchone(
        "SELECT id FROM client_broker_instances WHERE client_id=? AND broker=? AND status != 'removed'",
        (user["id"], body.current_broker)
    )
    if not current_instance:
        raise HTTPException(400, f"You don't have {body.current_broker} configured as a broker.")

    requested_instance = db_fetchone(
        "SELECT id FROM client_broker_instances WHERE client_id=? AND broker=? AND status != 'removed'",
        (user["id"], body.requested_broker)
    )
    if requested_instance:
        raise HTTPException(400, f"You already have {body.requested_broker} configured. No change request needed.")

    existing = db_fetchone(
        "SELECT id FROM broker_change_requests WHERE client_id=? AND status='pending'",
        (user["id"],)
    )
    if existing:
        raise HTTPException(400, "You already have a pending broker change request. Please wait for admin approval.")

    running = db_fetchone(
        "SELECT id FROM client_broker_instances WHERE client_id=? AND status='running'",
        (user["id"],)
    )
    if running:
        raise HTTPException(400, "Please stop your bot before requesting a broker change.")

    db_execute(
        "INSERT INTO broker_change_requests (client_id, current_broker, requested_broker, reason) VALUES (?,?,?,?)",
        (user["id"], body.current_broker, body.requested_broker, body.reason or "")
    )
    return {"success": True, "message": "Broker change request submitted. Admin will review it shortly."}


@router.get("/broker-change-request")
async def get_broker_change_request(user=Depends(get_current_user)):
    pending = db_fetchone(
        "SELECT id, current_broker, requested_broker, reason, status, created_at FROM broker_change_requests WHERE client_id=? AND status='pending' LIMIT 1",
        (user["id"],)
    )
    recent = db_fetchall(
        "SELECT id, current_broker, requested_broker, reason, status, created_at, resolved_at FROM broker_change_requests WHERE client_id=? ORDER BY created_at DESC LIMIT 5",
        (user["id"],)
    )
    return {"pending": pending, "recent": recent}


# ── Trade History ─────────────────────────────────────────────────────────────

@router.get("/trades")
async def get_trades(user=Depends(get_current_user)):
    trades = db_fetchall("""
        SELECT t.trade_type, t.direction, t.strike, t.entry_price, t.exit_price,
               t.pnl_pts, t.pnl_rs, t.exit_reason, t.instrument, t.trading_mode,
               t.closed_at, cbi.broker
        FROM trade_history t
        JOIN client_broker_instances cbi ON cbi.id=t.instance_id
        WHERE t.client_id=?
        ORDER BY t.closed_at DESC LIMIT 100
    """, (user["id"],))
    return {"trades": trades}
