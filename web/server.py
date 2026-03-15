import time
import hashlib
import urllib.parse
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

from web.config_api import router as config_router
from web.broker_api import router as broker_router
from web.status_api import router as status_router
from web.bot_control import router as bot_router
from web.auth_api import router as auth_router
from web.admin_api import router as admin_router
from web.client_api import router as client_router
from web.auth import decode_token, encrypt_secret, decrypt_secret, _fernet
from web.db import get_db, db_fetchone, db_execute

BASE_DIR = Path(__file__).parent

app = FastAPI(title="AlgoSoft", version="2.0.0")

app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Existing bot APIs
app.include_router(config_router, prefix="/api")
app.include_router(broker_router, prefix="/api")
app.include_router(status_router, prefix="/api")
app.include_router(bot_router, prefix="/api")

# Multi-tenant APIs
app.include_router(auth_router, prefix="/api")
app.include_router(admin_router, prefix="/api")
app.include_router(client_router, prefix="/api")


def _get_user_from_request(request: Request):
    token = request.cookies.get("access_token")
    if not token:
        return None
    payload = decode_token(token)
    if not payload:
        return None
    try:
        conn = get_db()
        row = conn.execute("SELECT id, role, is_active, username FROM users WHERE id=?", (int(payload["sub"]),)).fetchone()
        return dict(row) if row else None
    except Exception:
        return None


# ─── Auth Pages ───────────────────────────────────────────────────────────────

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    user = _get_user_from_request(request)
    if user and user["is_active"]:
        return RedirectResponse("/admin" if user["role"] == "admin" else "/dashboard")
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})


@app.get("/logout")
async def logout():
    response = RedirectResponse("/login")
    response.delete_cookie("access_token")
    return response


# ─── Admin Pages ──────────────────────────────────────────────────────────────

@app.get("/admin", response_class=HTMLResponse)
async def admin_overview(request: Request):
    user = _get_user_from_request(request)
    if not user or user["role"] != "admin":
        return RedirectResponse("/login")
    return templates.TemplateResponse("admin_overview.html", {"request": request})


@app.get("/admin/clients", response_class=HTMLResponse)
async def admin_clients(request: Request):
    user = _get_user_from_request(request)
    if not user or user["role"] != "admin":
        return RedirectResponse("/login")
    return templates.TemplateResponse("admin_clients.html", {"request": request})


@app.get("/admin/clients/{client_id}", response_class=HTMLResponse)
async def admin_client_detail(request: Request, client_id: int):
    user = _get_user_from_request(request)
    if not user or user["role"] != "admin":
        return RedirectResponse("/login")
    return templates.TemplateResponse("admin_client_detail.html", {"request": request, "client_id": client_id})


@app.get("/admin/data-provider", response_class=HTMLResponse)
async def admin_data_provider(request: Request):
    user = _get_user_from_request(request)
    if not user or user["role"] != "admin":
        return RedirectResponse("/login")
    return templates.TemplateResponse("admin_data_provider.html", {"request": request})


# ─── Client Pages ─────────────────────────────────────────────────────────────

@app.get("/dashboard", response_class=HTMLResponse)
async def client_dashboard(request: Request):
    user = _get_user_from_request(request)
    if not user:
        return RedirectResponse("/login")
    if user["role"] == "admin":
        return RedirectResponse("/admin")
    return templates.TemplateResponse("client_dashboard.html", {"request": request})


# ─── Zerodha OAuth Callback ──────────────────────────────────────────────

IST = timezone(timedelta(hours=5, minutes=30))

@app.get("/auth/zerodha/callback")
async def zerodha_oauth_callback(
    request: Request,
    request_token: str = Query(default=None),
    action: str = Query(default=None),
    status: str = Query(default=None),
    state: str = Query(default=None),
):
    if action == "login" and status != "success":
        return RedirectResponse("/dashboard?zerodha=denied")

    if not request_token or not state:
        return RedirectResponse("/dashboard?zerodha=error&msg=missing_params")

    try:
        state_payload = _fernet.decrypt(state.encode()).decode()
        client_id_str, ts_str = state_payload.split(":")
        client_id = int(client_id_str)
        state_age = time.time() - int(ts_str)
        if state_age > 600:
            return RedirectResponse("/dashboard?zerodha=error&msg=state_expired")
    except Exception:
        return RedirectResponse("/dashboard?zerodha=error&msg=invalid_state")

    instance = db_fetchone(
        "SELECT * FROM client_broker_instances WHERE client_id=? AND broker='zerodha'",
        (client_id,)
    )
    if not instance:
        return RedirectResponse("/dashboard?zerodha=error&msg=no_broker")

    api_key = decrypt_secret(instance["api_key_encrypted"])
    api_secret = decrypt_secret(instance.get("api_secret_encrypted", ""))
    if not api_key or not api_secret:
        return RedirectResponse("/dashboard?zerodha=error&msg=missing_credentials")

    try:
        checksum = hashlib.sha256(
            (api_key + request_token + api_secret).encode()
        ).hexdigest()

        import requests as http_requests
        resp = http_requests.post(
            "https://api.kite.trade/session/token",
            data={
                "api_key": api_key,
                "request_token": request_token,
                "checksum": checksum,
            },
        )
        resp_data = resp.json()

        if resp.status_code != 200 or resp_data.get("status") == "error":
            error_msg = resp_data.get("message", "token_exchange_failed")
            return RedirectResponse(f"/dashboard?zerodha=error&msg={urllib.parse.quote(error_msg)}")

        access_token = resp_data.get("data", {}).get("access_token", "")
        if not access_token:
            return RedirectResponse("/dashboard?zerodha=error&msg=no_token_in_response")

        enc_token = encrypt_secret(access_token)
        now_ist = datetime.now(IST).isoformat()
        db_execute(
            "UPDATE client_broker_instances SET access_token_encrypted=?, token_updated_at=? WHERE client_id=? AND broker='zerodha'",
            (enc_token, now_ist, client_id)
        )
        return RedirectResponse("/dashboard?zerodha=success")
    except Exception as e:
        return RedirectResponse(f"/dashboard?zerodha=error&msg={urllib.parse.quote(str(e)[:100])}")


# ─── Root redirect ────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    user = _get_user_from_request(request)
    if not user:
        return RedirectResponse("/login")
    if user["role"] == "admin":
        return RedirectResponse("/admin")
    return RedirectResponse("/dashboard")


# ─── Legacy pages (keep for backward compat) ─────────────────────────────────

@app.get("/strategy", response_class=HTMLResponse)
async def strategy_page(request: Request):
    return templates.TemplateResponse("strategy.html", {"request": request})


@app.get("/brokers", response_class=HTMLResponse)
async def brokers_page(request: Request):
    return templates.TemplateResponse("brokers.html", {"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("web.server:app", host="0.0.0.0", port=5000, reload=True)
