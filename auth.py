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
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

DB_PATH = Path(__file__).resolve().parent / "caba_normativa.db"

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
JWT_SECRET = os.environ.get("JWT_SECRET", secrets.token_hex(32))
JWT_ALGORITHM = "HS256"
JWT_EXPIRY_SECONDS = 30 * 24 * 3600  # 30 days


def _base_url(request: Request) -> str:
    """Get base URL respecting X-Forwarded-Proto from nginx."""
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("host", request.url.netloc)
    return f"{proto}://{host}"

GOOGLE_AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_USERINFO_URL = "https://www.googleapis.com/oauth2/v2/userinfo"

# --- Plan tiers ---

PLAN_DEFAULTS: dict[str, dict[str, Any]] = {
    "free": {
        "usd_mes_max": 0.02,
        "modelos_habilitados": '["haiku"]',
        "mb_mes_max": 1,
    },
    "pro": {
        "usd_mes_max": 5.0,
        "modelos_habilitados": '["haiku"]',
        "mb_mes_max": 5,
    },
    "enterprise": {
        "usd_mes_max": 999,
        "modelos_habilitados": '["haiku","sonnet","opus"]',
        "mb_mes_max": 999,
    },
}


def init_users_table() -> None:
    """Create users table, migrate columns, seed users."""
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
            acceso_hasta TEXT,
            mp_payment_id TEXT,
            created_at REAL DEFAULT (strftime('%s', 'now'))
        )
    """)
    # Migrate: add columns if missing
    for col, default in [
        ("acceso_hasta TEXT", None),
        ("creditos_usd REAL", "0"),
        ("modelos_habilitados TEXT", "'[\"haiku\"]'"),
        ("mb_mes_max REAL", "1"),
        ("usd_mes_max REAL", "0.02"),
    ]:
        try:
            stmt = f"ALTER TABLE users ADD COLUMN {col}"
            if default is not None:
                stmt += f" DEFAULT {default}"
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS user_usage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            month TEXT NOT NULL,
            tokens_in INTEGER DEFAULT 0,
            tokens_out INTEGER DEFAULT 0,
            usd_used REAL DEFAULT 0,
            mb_used REAL DEFAULT 0,
            UNIQUE(user_id, month),
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)

    # Seed users
    _upsert_seed(conn, "juanwisznia@gmail.com", "enterprise")
    _upsert_seed(conn, "karendmarini@gmail.com", "pro")
    conn.commit()
    conn.close()


def _upsert_seed(conn: sqlite3.Connection, email: str, plan: str) -> None:
    defaults = PLAN_DEFAULTS[plan]
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if not existing:
        conn.execute(
            "INSERT INTO users (email, activo, plan, acceso_hasta, creditos_usd, "
            "modelos_habilitados, usd_mes_max, mb_mes_max) "
            "VALUES (?, 1, ?, '2099-12-31', ?, ?, ?, ?)",
            (email, plan, defaults["usd_mes_max"], defaults["modelos_habilitados"],
             defaults["usd_mes_max"], defaults["mb_mes_max"]),
        )
    else:
        conn.execute(
            "UPDATE users SET activo=1, plan=?, acceso_hasta='2099-12-31', "
            "creditos_usd=MAX(creditos_usd, ?), modelos_habilitados=?, usd_mes_max=?, mb_mes_max=? "
            "WHERE email=?",
            (plan, defaults["usd_mes_max"], defaults["modelos_habilitados"],
             defaults["usd_mes_max"], defaults["mb_mes_max"], email),
        )


def upsert_user(
    email: str,
    acceso_hasta: str,
    plan: str = "free",
    nombre: str = "",
    creditos_usd: float | None = None,
    modelos_habilitados: list[str] | None = None,
    mb_mes_max: float | None = None,
    usd_mes_max: float | None = None,
) -> dict[str, Any]:
    """Create or update a user. Custom values override plan defaults."""
    defaults = PLAN_DEFAULTS.get(plan, PLAN_DEFAULTS["free"])
    modelos = json.dumps(modelos_habilitados) if modelos_habilitados else defaults["modelos_habilitados"]
    usd_max = usd_mes_max if usd_mes_max is not None else defaults["usd_mes_max"]
    mb_max = mb_mes_max if mb_mes_max is not None else defaults["mb_mes_max"]
    cred = creditos_usd if creditos_usd is not None else usd_max

    conn = sqlite3.connect(str(DB_PATH), timeout=20)
    conn.row_factory = sqlite3.Row
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        conn.execute(
            "UPDATE users SET activo=1, plan=?, nombre=?, acceso_hasta=?, "
            "creditos_usd=?, modelos_habilitados=?, usd_mes_max=?, mb_mes_max=? WHERE email=?",
            (plan, nombre, acceso_hasta, cred, modelos, usd_max, mb_max, email),
        )
    else:
        conn.execute(
            "INSERT INTO users (email, nombre, activo, plan, acceso_hasta, creditos_usd, "
            "modelos_habilitados, usd_mes_max, mb_mes_max) VALUES (?,?,1,?,?,?,?,?,?)",
            (email, nombre, plan, acceso_hasta, cred, modelos, usd_max, mb_max),
        )
    conn.commit()
    row = conn.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    conn.close()
    return dict(row)


def track_usage(
    user_id: int, tokens_in: int, tokens_out: int, usd_cost: float,
) -> None:
    """Record token usage for the current month and decrement credits."""
    month = time.strftime("%Y-%m")
    conn = sqlite3.connect(str(DB_PATH), timeout=20)
    conn.execute(
        "INSERT INTO user_usage (user_id, month, tokens_in, tokens_out, usd_used) "
        "VALUES (?, ?, ?, ?, ?) "
        "ON CONFLICT(user_id, month) DO UPDATE SET "
        "tokens_in = tokens_in + ?, tokens_out = tokens_out + ?, usd_used = usd_used + ?",
        (user_id, month, tokens_in, tokens_out, usd_cost,
         tokens_in, tokens_out, usd_cost),
    )
    conn.execute(
        "UPDATE users SET creditos_usd = MAX(0, creditos_usd - ?) WHERE id = ?",
        (usd_cost, user_id),
    )
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
        "sub": str(user_id),
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
        "SELECT id, email, nombre, activo, plan, acceso_hasta, "
        "creditos_usd, modelos_habilitados, usd_mes_max, mb_mes_max "
        "FROM users WHERE id = ?",
        (int(payload["sub"]),),
    ).fetchone()
    if not row:
        conn.close()
        return None
    user = dict(row)
    # Check expiry
    if user.get("acceso_hasta"):
        from datetime import date
        if date.fromisoformat(user["acceso_hasta"]) < date.today():
            user["activo"] = 0
            user["expired"] = True
    # Fetch current month usage
    month = time.strftime("%Y-%m")
    usage = conn.execute(
        "SELECT usd_used, mb_used FROM user_usage WHERE user_id = ? AND month = ?",
        (user["id"], month),
    ).fetchone()
    conn.close()
    user["usd_used_this_month"] = usage["usd_used"] if usage else 0
    user["mb_used_this_month"] = usage["mb_used"] if usage else 0
    return user


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
    callback = _base_url(request) + "/api/auth/callback"
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
    callback = _base_url(request) + "/api/auth/callback"

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

    # Check whitelist
    conn = sqlite3.connect(str(DB_PATH), timeout=20)
    conn.row_factory = sqlite3.Row
    existing = conn.execute("SELECT id, activo, acceso_hasta FROM users WHERE email = ?", (email,)).fetchone()

    if not existing:
        # New user — create with no plan, redirect to pricing
        conn.execute(
            "INSERT INTO users (email, nombre, google_id, activo) VALUES (?, ?, ?, 1)",
            (email, nombre, google_id),
        )
        user_id = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()["id"]
    else:
        user_id = existing["id"]
        conn.execute(
            "UPDATE users SET google_id = ?, nombre = ? WHERE id = ?",
            (google_id, nombre, user_id),
        )

    # Check expiry
    from datetime import date
    acceso_hasta = existing["acceso_hasta"]
    if acceso_hasta and date.fromisoformat(acceso_hasta) < date.today():
        conn.commit()
        conn.close()
        return HTMLResponse(
            '<html><body style="background:#000;color:#fff;font-family:system-ui;display:flex;align-items:center;justify-content:center;height:100vh;flex-direction:column">'
            f'<h2>Acceso expirado</h2>'
            f'<p style="color:#999;margin-top:12px">Tu acceso venció el {acceso_hasta}.</p>'
            '<p style="color:#999;margin-top:8px">Contacto: <a href="mailto:karendmarini@gmail.com" style="color:#e8c547">karendmarini@gmail.com</a></p>'
            '</body></html>',
            status_code=403,
        )

    conn.commit()
    conn.close()

    # Set JWT cookie and redirect
    token = _create_token(user_id, email)
    base = _base_url(request)
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
