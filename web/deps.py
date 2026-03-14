from fastapi import Request, HTTPException
from web.auth import decode_token
from web.db import db_fetchone


def get_current_user(request: Request):
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(401, "Not authenticated")
    payload = decode_token(token)
    if not payload:
        raise HTTPException(401, "Invalid or expired session")
    user = db_fetchone("SELECT * FROM users WHERE id=?", (int(payload["sub"]),))
    if not user or not user["is_active"]:
        raise HTTPException(401, "Account not found or inactive")
    return user


def require_admin(request: Request):
    user = get_current_user(request)
    if user["role"] != "admin":
        raise HTTPException(403, "Admin access required")
    return user


def require_client(request: Request):
    user = get_current_user(request)
    if user["role"] not in ("client", "admin"):
        raise HTTPException(403, "Access denied")
    return user
