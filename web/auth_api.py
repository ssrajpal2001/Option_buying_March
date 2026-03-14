from fastapi import APIRouter, Request, Response, HTTPException, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, EmailStr
from web.auth import hash_password, verify_password, create_access_token
from web.db import db_fetchone, db_execute, db_fetchall
from web.deps import get_current_user, require_admin

router = APIRouter(prefix="/auth", tags=["auth"])


class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str


class LoginRequest(BaseModel):
    username: str
    password: str


@router.post("/register")
async def register(body: RegisterRequest):
    if len(body.password) < 6:
        raise HTTPException(400, "Password must be at least 6 characters")
    if db_fetchone("SELECT id FROM users WHERE username=?", (body.username,)):
        raise HTTPException(400, "Username already taken")
    if db_fetchone("SELECT id FROM users WHERE email=?", (body.email,)):
        raise HTTPException(400, "Email already registered")

    hashed = hash_password(body.password)
    db_execute(
        "INSERT INTO users (username, email, password_hash, role, is_active) VALUES (?,?,?,?,?)",
        (body.username, body.email, hashed, "client", 0)
    )
    return {"success": True, "message": "Registration successful. Awaiting admin activation."}


@router.post("/login")
async def login(body: LoginRequest, response: Response):
    user = db_fetchone("SELECT * FROM users WHERE username=?", (body.username,))
    if not user:
        raise HTTPException(401, "Invalid username or password")
    if not verify_password(body.password, user["password_hash"]):
        raise HTTPException(401, "Invalid username or password")
    if not user["is_active"]:
        raise HTTPException(403, "Account not activated. Contact admin.")

    token = create_access_token({"sub": str(user["id"]), "role": user["role"], "username": user["username"]})
    response.set_cookie("access_token", token, httponly=True, samesite="lax", max_age=86400)
    return {
        "success": True,
        "role": user["role"],
        "username": user["username"],
        "redirect": "/admin" if user["role"] == "admin" else "/dashboard"
    }


@router.post("/logout")
async def logout(response: Response):
    response.delete_cookie("access_token")
    return {"success": True}


@router.get("/me")
async def me(user=Depends(get_current_user)):
    return {
        "id": user["id"],
        "username": user["username"],
        "email": user["email"],
        "role": user["role"],
        "subscription_tier": user["subscription_tier"],
        "max_brokers": user["max_brokers"],
    }
