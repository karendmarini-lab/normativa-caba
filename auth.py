"""
Authentication module for EdificIA.

Google OAuth2 + email/password login.
Users table in SQLite. JWT sessions.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import sqlite3
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import jwt
import requests
from fastapi import Cookie, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse

DB_PATH = Path(__file__).resolve().parent / "caba_normativa.db"

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
JWT_SECRET = os.environ.get("JWT_SECRET", secrets.token_hex(32))
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_SECONDS = 30 * 24 * 3600  # 30 days

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"


def init_users_table() -> None:
    """Create users table if it doesn't exist."""
    conn = sqlite3.connect(str(DB_PATH), timeout=20)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            nombre TEXT,
            hash_password TEXT,
            google_id TEXT,
            activo INTEGER DEFAULT 0,
            plan TEXT DEFAULT 'free',
            mp_payment_id TEXT,
            created_at REAL DEFAULT (strftime('%s', 'now'))
        )
    """)
    conn.commit()
    conn.close()


def _hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
    return f"{salt}:{h.hex()}"


def _verify_password(password: str, stored: str) -> bool:
    salt, h = stored.split(":")
    check = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
    return hmac.compare_digest(check.hex(), h)


def _create_token(user_id: int, email: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "exp": int(time.time()) + JWT_EXPIRY_SECONDS,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def _decode_token(token: str) -> dict[str, Any] | None:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
        return None


def get_current_user(request: Request) -> dict[str, Any] | None:
    """Extract user from JWT cookie. Returns None if not logged in."""
    token = request.cookies.get("session")
    if not token:
        return None
    payload = _decode_token(token)
    if not payload:
        return None
    conn = sqlite3.connect(str(DB_PATH), timeout=20)
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT id, email, nombre, activo, plan FROM users WHERE id = ?",
        (payload["sub"],),
    ).fetchone()
    conn.close()
    if not row:
        return None
    return dict(row)


def require_active_user(request: Request) -> dict[str, Any]:
    """Raise 401 if not logged in, 403 if not active."""
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not user["activo"]:
        raise HTTPException(status_code=403, detail="Account not active")
    return user


# --- Route handlers ---


def handle_google_login(request: Request) -> RedirectResponse:
    """Redirect to Google OAuth2 consent screen."""
    callback = str(request.base_url).rstrip("/") + "/api/auth/callback"
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": callback,
        "response_type": "code",
        "scope": "openid email profile",
        "access_type": "offline",
        "prompt": "select_account",
    }
    return RedirectResponse(f"{GOOGLE_AUTH_URL}?{urlencode(params)}")


def handle_google_callback(request: Request, code: str) -> RedirectResponse:
    """Exchange Google auth code for user info, create/update user, set cookie."""
    callback = str(request.base_url).rstrip("/") + "/api/auth/callback"

    # Exchange code for tokens
    token_resp = requests.post(GOOGLE_TOKEN_URL, data={
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": callback,
        "grant_type": "authorization_code",
    }, timeout=10)

    if token_resp.status_code != 200:
        raise HTTPException(status_code=400, detail="Google auth failed")

    access_token = token_resp.json()["access_token"]

    # Get user info
    info_resp = requests.get(
        GOOGLE_USERINFO_URL,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=10,
    )
    info = info_resp.json()
    email = info["email"]
    nombre = info.get("name", "")
    google_id = info["id"]

    # Upsert user
    conn = sqlite3.connect(str(DB_PATH), timeout=20)
    conn.row_factory = sqlite3.Row
    existing = conn.execute("SELECT id, activo FROM users WHERE email = ?", (email,)).fetchone()

    if existing:
        user_id = existing["id"]
        conn.execute(
            "UPDATE users SET google_id = ?, nombre = ? WHERE id = ?",
            (google_id, nombre, user_id),
        )
    else:
        cursor = conn.execute(
            "INSERT INTO users (email, nombre, google_id, activo) VALUES (?, ?, ?, 0)",
            (email, nombre, google_id),
        )
        user_id = cursor.lastrowid

    conn.commit()
    conn.close()

    # Set JWT cookie and redirect to full HTTPS URL
    token = _create_token(user_id, email)
    base = str(request.base_url).rstrip("/")
    response = RedirectResponse(base + "/", status_code=302)
    response.set_cookie(
        "session", token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=JWT_EXPIRY_SECONDS,
    )
    return response


def handle_register(email: str, password: str, nombre: str = "") -> JSONResponse:
    """Register with email/password."""
    if not email or not password:
        raise HTTPException(status_code=400, detail="Email and password required")
    if len(password) < 8:
        raise HTTPException(status_code=400, detail="Password must be 8+ chars")

    conn = sqlite3.connect(str(DB_PATH), timeout=20)
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        conn.close()
        raise HTTPException(status_code=409, detail="Email already registered")

    hashed = _hash_password(password)
    cursor = conn.execute(
        "INSERT INTO users (email, nombre, hash_password, activo) VALUES (?, ?, ?, 0)",
        (email, nombre, hashed),
    )
    user_id = cursor.lastrowid
    conn.commit()
    conn.close()

    token = _create_token(user_id, email)
    response = JSONResponse({"ok": True, "email": email})
    response.set_cookie(
        "session", token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=JWT_EXPIRY_SECONDS,
    )
    return response


def handle_login(email: str, password: str) -> JSONResponse:
    """Login with email/password."""
    conn = sqlite3.connect(str(DB_PATH), timeout=20)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT id, email, hash_password FROM users WHERE email = ?", (email,)).fetchone()
    conn.close()

    if not row or not row["hash_password"]:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    if not _verify_password(password, row["hash_password"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = _create_token(row["id"], row["email"])
    response = JSONResponse({"ok": True, "email": row["email"]})
    response.set_cookie(
        "session", token,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=JWT_EXPIRY_SECONDS,
    )
    return response


def handle_logout() -> RedirectResponse:
    response = RedirectResponse("/", status_code=302)
    response.delete_cookie("session")
    return response


def handle_me(request: Request) -> dict[str, Any]:
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user
