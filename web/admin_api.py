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
               SUM(CASE WHEN cbi.status='running' THEN 1 ELSE 0 END) as running_count
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
               status, bot_pid, last_heartbeat, created_at
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
    return {"client": user, "instances": instances, "failures": failures, "trades": trades}


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
    running_list = instance_manager.list_running()
    return {
        "total_clients": total_clients,
        "active_clients": active_clients,
        "pending_clients": pending_clients,
        "running_instances": running_instances,
        "failures_today": failures_today,
        "data_provider_status": dp["status"] if dp else "not_configured",
        "live_instances": running_list,
    }
