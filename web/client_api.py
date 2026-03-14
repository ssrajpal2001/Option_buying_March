from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
from web.deps import get_current_user
from web.db import db_fetchone, db_fetchall, db_execute
from web.auth import encrypt_secret, decrypt_secret
from hub.instance_manager import instance_manager

router = APIRouter(prefix="/client", tags=["client"])


# ── Broker Setup ─────────────────────────────────────────────────────────────

class BrokerSetup(BaseModel):
    broker: str = "zerodha"
    api_key: str
    access_token: str
    trading_mode: str = "paper"    # paper or live
    instrument: str = "NIFTY"
    quantity: int = 25
    strategy_version: str = "V3"


@router.get("/broker")
async def get_broker_config(user=Depends(get_current_user)):
    rows = db_fetchall(
        "SELECT id, broker, trading_mode, instrument, quantity, strategy_version, status, last_heartbeat FROM client_broker_instances WHERE client_id=?",
        (user["id"],)
    )
    return {"instances": rows, "max_brokers": user["max_brokers"]}


@router.post("/broker")
async def save_broker_config(body: BrokerSetup, user=Depends(get_current_user)):
    existing = db_fetchall(
        "SELECT id FROM client_broker_instances WHERE client_id=?", (user["id"],)
    )
    max_b = user["max_brokers"]
    if len(existing) >= max_b:
        existing_broker = db_fetchone(
            "SELECT id FROM client_broker_instances WHERE client_id=? AND broker=?",
            (user["id"], body.broker)
        )
        if not existing_broker:
            raise HTTPException(400, f"Your plan allows {max_b} broker(s). Upgrade to Premium for more.")

    if body.trading_mode not in ("paper", "live"):
        raise HTTPException(400, "trading_mode must be 'paper' or 'live'")

    enc_key = encrypt_secret(body.api_key)
    enc_token = encrypt_secret(body.access_token)

    db_execute("""
        INSERT INTO client_broker_instances
          (client_id, broker, api_key_encrypted, access_token_encrypted,
           trading_mode, instrument, quantity, strategy_version)
        VALUES (?,?,?,?,?,?,?,?)
        ON CONFLICT(client_id, broker) DO UPDATE SET
          api_key_encrypted=excluded.api_key_encrypted,
          access_token_encrypted=excluded.access_token_encrypted,
          trading_mode=excluded.trading_mode,
          instrument=excluded.instrument,
          quantity=excluded.quantity,
          strategy_version=excluded.strategy_version
    """, (user["id"], body.broker, enc_key, enc_token,
          body.trading_mode, body.instrument, body.quantity, body.strategy_version))

    return {"success": True, "message": "Broker configuration saved."}


# ── Bot Control ───────────────────────────────────────────────────────────────

@router.post("/bot/start")
async def start_bot(user=Depends(get_current_user)):
    instance = db_fetchone(
        "SELECT * FROM client_broker_instances WHERE client_id=? AND broker='zerodha'",
        (user["id"],)
    )
    if not instance:
        raise HTTPException(400, "No broker configured. Please set up your broker first.")
    if not instance.get("api_key_encrypted"):
        raise HTTPException(400, "Broker credentials missing. Please re-enter your API key.")

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
    instance = db_fetchone(
        "SELECT * FROM client_broker_instances WHERE client_id=? AND broker='zerodha'",
        (user["id"],)
    )
    if not instance:
        raise HTTPException(400, "No broker configured.")

    ok, msg = instance_manager.stop_instance(instance["id"])
    if ok:
        db_execute("UPDATE client_broker_instances SET status='idle', bot_pid=NULL WHERE id=?", (instance["id"],))
    return {"success": ok, "message": msg}


@router.post("/bot/restart")
async def restart_bot(user=Depends(get_current_user)):
    await stop_bot(user=user)
    return await start_bot(user=user)


@router.get("/bot/status")
async def bot_status(user=Depends(get_current_user)):
    instance = db_fetchone(
        "SELECT id, broker, status, trading_mode, instrument, quantity, strategy_version, last_heartbeat FROM client_broker_instances WHERE client_id=? AND broker='zerodha'",
        (user["id"],)
    )
    if not instance:
        return {"configured": False}

    live_status = instance_manager.get_instance_status(instance["id"])
    trades = db_fetchall("""
        SELECT trade_type, direction, strike, entry_price, exit_price,
               pnl_pts, pnl_rs, exit_reason, closed_at
        FROM trade_history WHERE instance_id=? ORDER BY closed_at DESC LIMIT 50
    """, (instance["id"],))

    return {
        "configured": True,
        "instance": dict(instance),
        "live": live_status,
        "trade_history": trades,
    }


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
