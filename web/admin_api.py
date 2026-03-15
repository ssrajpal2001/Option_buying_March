import json
import time
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from typing import Optional
from datetime import datetime, timezone

from web.deps import require_admin
from web.db import db_fetchone, db_fetchall, db_execute
from web.auth import encrypt_secret, decrypt_secret
from hub.instance_manager import instance_manager

router = APIRouter(prefix="/admin", tags=["admin"])


# ── Data Provider ───────────────────────────────────────────────────────────

class DataProviderUpdate(BaseModel):
    api_key: str
    access_token: str


@router.get("/data-provider")
async def get_data_provider(admin=Depends(require_admin)):
    row = db_fetchone("SELECT * FROM data_providers WHERE provider='upstox'")
    if not row:
        return {"provider": "upstox", "status": "not_configured", "api_key": ""}
    return {
        "provider": row["provider"],
        "status": row["status"],
        "api_key": decrypt_secret(row["api_key_encrypted"]) if row.get("api_key_encrypted") else "",
        "access_token_set": bool(row.get("access_token_encrypted")),
    }


@router.post("/data-provider")
async def update_data_provider(body: DataProviderUpdate, request: Request, admin=Depends(require_admin)):
    enc_key = encrypt_secret(body.api_key)
    enc_token = encrypt_secret(body.access_token)
    now = datetime.now(timezone.utc).isoformat()
    db_execute(
        """INSERT INTO data_providers (provider, api_key_encrypted, access_token_encrypted, status, updated_at, updated_by)
           VALUES ('upstox', ?, ?, 'configured', ?, ?)
           ON CONFLICT(provider) DO UPDATE SET
             api_key_encrypted=excluded.api_key_encrypted,
             access_token_encrypted=excluded.access_token_encrypted,
             status='configured',
             updated_at=excluded.updated_at,
             updated_by=excluded.updated_by""",
        (enc_key, enc_token, now, admin["id"])
    )
    return {"success": True, "message": "Upstox data provider configured."}


# ── Client Management ────────────────────────────────────────────────────────

@router.get("/clients")
async def list_clients(admin=Depends(require_admin)):
    clients = db_fetchall("""
        SELECT u.id, u.username, u.email, u.is_active, u.subscription_tier,
               u.max_brokers, u.created_at, u.activated_at,
               COUNT(cbi.id) as broker_count,
               SUM(CASE WHEN cbi.status='running' THEN 1 ELSE 0 END) as running_count,
               GROUP_CONCAT(DISTINCT cbi.broker) as brokers
        FROM users u
        LEFT JOIN client_broker_instances cbi ON cbi.client_id=u.id
        WHERE u.role='client'
        GROUP BY u.id
        ORDER BY u.created_at DESC
    """)
    return clients


@router.post("/clients/{client_id}/activate")
async def activate_client(client_id: int, admin=Depends(require_admin)):
    user = db_fetchone("SELECT * FROM users WHERE id=? AND role='client'", (client_id,))
    if not user:
        raise HTTPException(404, "Client not found")
    now = datetime.now(timezone.utc).isoformat()
    db_execute(
        "UPDATE users SET is_active=1, activated_at=?, activated_by=? WHERE id=?",
        (now, admin["id"], client_id)
    )
    return {"success": True, "message": f"Client '{user['username']}' activated."}


@router.post("/clients/{client_id}/deactivate")
async def deactivate_client(client_id: int, admin=Depends(require_admin)):
    user = db_fetchone("SELECT * FROM users WHERE id=? AND role='client'", (client_id,))
    if not user:
        raise HTTPException(404, "Client not found")
    instance_manager.stop_all_for_client(client_id)
    db_execute("UPDATE users SET is_active=0 WHERE id=?", (client_id,))
    return {"success": True, "message": f"Client '{user['username']}' deactivated."}


@router.post("/clients/{client_id}/set-tier")
async def set_subscription_tier(client_id: int, request: Request, admin=Depends(require_admin)):
    body = await request.json()
    tier = body.get("tier", "FREE")
    max_brokers = {"FREE": 1, "PREMIUM": 3}.get(tier, 1)
    db_execute(
        "UPDATE users SET subscription_tier=?, max_brokers=? WHERE id=?",
        (tier, max_brokers, client_id)
    )
    return {"success": True, "tier": tier, "max_brokers": max_brokers}


@router.get("/clients/{client_id}/detail")
async def client_detail(client_id: int, admin=Depends(require_admin)):
    user = db_fetchone("SELECT id, username, email, is_active, subscription_tier, max_brokers, created_at FROM users WHERE id=?", (client_id,))
    if not user:
        raise HTTPException(404, "Client not found")
    instances = db_fetchall("""
        SELECT id, broker, trading_mode, instrument, quantity, strategy_version,
               status, bot_pid, last_heartbeat, token_updated_at, created_at
        FROM client_broker_instances WHERE client_id=?
    """, (client_id,))
    failures = db_fetchall("""
        SELECT order_side, failure_reason, retry_attempt, paired_leg_closed, created_at
        FROM order_failures WHERE client_id=? ORDER BY created_at DESC LIMIT 20
    """, (client_id,))
    trades = db_fetchall("""
        SELECT trade_type, direction, strike, entry_price, exit_price,
               pnl_pts, pnl_rs, exit_reason, closed_at
        FROM trade_history WHERE client_id=? ORDER BY closed_at DESC LIMIT 50
    """, (client_id,))
    broker_requests = db_fetchall("""
        SELECT id, current_broker, requested_broker, reason, status, created_at, resolved_at
        FROM broker_change_requests WHERE client_id=? ORDER BY created_at DESC LIMIT 10
    """, (client_id,))
    return {"client": user, "instances": instances, "failures": failures, "trades": trades, "broker_requests": broker_requests}


@router.post("/clients/{client_id}/force-close")
async def force_close_positions(client_id: int, admin=Depends(require_admin)):
    instance_manager.stop_all_for_client(client_id)
    return {"success": True, "message": "All bot instances stopped for client."}


# ── Admin live overview ──────────────────────────────────────────────────────

@router.get("/overview")
async def overview(admin=Depends(require_admin)):
    total_clients = db_fetchone("SELECT COUNT(*) as c FROM users WHERE role='client'")["c"]
    active_clients = db_fetchone("SELECT COUNT(*) as c FROM users WHERE role='client' AND is_active=1")["c"]
    pending_clients = db_fetchone("SELECT COUNT(*) as c FROM users WHERE role='client' AND is_active=0")["c"]
    running_instances = db_fetchone("SELECT COUNT(*) as c FROM client_broker_instances WHERE status='running'")["c"]
    failures_today = db_fetchone(
        "SELECT COUNT(*) as c FROM order_failures WHERE date(created_at)=date('now')"
    )["c"]
    dp = db_fetchone("SELECT status FROM data_providers WHERE provider='upstox'")

    live_details = []
    running_rows = db_fetchall("""
        SELECT cbi.id, cbi.client_id, cbi.broker, cbi.instrument, cbi.quantity,
               cbi.trading_mode, cbi.strategy_version, cbi.bot_pid, u.username
        FROM client_broker_instances cbi
        JOIN users u ON u.id = cbi.client_id
        WHERE cbi.status = 'running'
    """)
    for row in running_rows:
        entry = dict(row)
        status_file = Path(f'config/bot_status_client_{row["client_id"]}.json')
        if status_file.exists():
            try:
                with open(status_file, 'r') as f:
                    bd = json.load(f)
                entry["session_pnl"] = float(bd.get("session_pnl") or 0)
                entry["trade_count"] = int(bd.get("trade_count") or 0)
                entry["heartbeat"] = float(bd.get("heartbeat") or 0)
                hb_age = time.time() - entry["heartbeat"]
                entry["stale"] = hb_age > 30
            except (json.JSONDecodeError, OSError, TypeError, ValueError):
                entry["session_pnl"] = 0
                entry["stale"] = True
        else:
            entry["session_pnl"] = 0
            entry["stale"] = True
        live_details.append(entry)

    pending_broker_requests = db_fetchall("""
        SELECT bcr.id, bcr.client_id, bcr.current_broker, bcr.requested_broker,
               bcr.reason, bcr.created_at, u.username
        FROM broker_change_requests bcr
        JOIN users u ON u.id = bcr.client_id
        WHERE bcr.status='pending'
        ORDER BY bcr.created_at DESC
    """)

    return {
        "total_clients": total_clients,
        "active_clients": active_clients,
        "pending_clients": pending_clients,
        "running_instances": running_instances,
        "failures_today": failures_today,
        "data_provider_status": dp["status"] if dp else "not_configured",
        "live_instances": live_details,
        "pending_broker_requests": pending_broker_requests,
    }


# ── Admin bot monitor for any client ─────────────────────────────────────

@router.get("/clients/{client_id}/bot-status")
async def client_bot_status(client_id: int, admin=Depends(require_admin)):
    user = db_fetchone("SELECT id, username FROM users WHERE id=?", (client_id,))
    if not user:
        raise HTTPException(404, "Client not found")

    instance = db_fetchone(
        "SELECT id, broker, status, trading_mode, instrument, quantity, strategy_version FROM client_broker_instances WHERE client_id=? ORDER BY CASE WHEN status='running' THEN 0 ELSE 1 END, id DESC LIMIT 1",
        (client_id,)
    )
    if not instance:
        return {"configured": False, "bot_data": {}}

    live_status = instance_manager.get_instance_status(instance["id"])
    bot_data = {}
    status_file = Path(f'config/bot_status_client_{client_id}.json')
    if status_file.exists():
        try:
            with open(status_file, 'r') as f:
                bot_data = json.load(f)
            heartbeat = float(bot_data.get('heartbeat') or 0)
            age = time.time() - heartbeat
            bot_data['stale'] = age > 30
            bot_data['stale_seconds'] = round(age)
            if not live_status["running"] and heartbeat > 0 and age < 30:
                live_status["running"] = True
                live_status["pid"] = bot_data.get("pid")
        except (json.JSONDecodeError, OSError, TypeError, ValueError):
            bot_data = {}

    return {
        "configured": True,
        "instance": dict(instance),
        "live": live_status,
        "bot_data": bot_data,
        "username": user["username"],
    }


# ── Broker Change Requests ─────────────────────────────────────────────────

@router.get("/broker-requests")
async def list_broker_requests(admin=Depends(require_admin)):
    rows = db_fetchall("""
        SELECT bcr.*, u.username, u.email
        FROM broker_change_requests bcr
        JOIN users u ON u.id = bcr.client_id
        ORDER BY CASE WHEN bcr.status='pending' THEN 0 ELSE 1 END, bcr.created_at DESC
        LIMIT 50
    """)
    return rows


@router.post("/broker-requests/{request_id}/approve")
async def approve_broker_request(request_id: int, admin=Depends(require_admin)):
    req = db_fetchone("SELECT * FROM broker_change_requests WHERE id=?", (request_id,))
    if not req:
        raise HTTPException(404, "Request not found")
    if req["status"] != "pending":
        raise HTTPException(400, "Request already resolved")
    now = datetime.now(timezone.utc).isoformat()
    db_execute(
        "UPDATE broker_change_requests SET status='approved', resolved_at=?, resolved_by_id=? WHERE id=?",
        (now, admin["id"], request_id)
    )
    db_execute(
        "DELETE FROM client_broker_instances WHERE client_id=? AND broker=?",
        (req["client_id"], req["current_broker"])
    )
    return {"success": True, "message": f"Broker change approved. Old {req['current_broker']} instance removed. Client can now set up {req['requested_broker']}."}


@router.post("/broker-requests/{request_id}/deny")
async def deny_broker_request(request_id: int, admin=Depends(require_admin)):
    req = db_fetchone("SELECT * FROM broker_change_requests WHERE id=?", (request_id,))
    if not req:
        raise HTTPException(404, "Request not found")
    if req["status"] != "pending":
        raise HTTPException(400, "Request already resolved")
    now = datetime.now(timezone.utc).isoformat()
    db_execute(
        "UPDATE broker_change_requests SET status='denied', resolved_at=?, resolved_by_id=? WHERE id=?",
        (now, admin["id"], request_id)
    )
    return {"success": True, "message": "Broker change request denied."}
