"""
FastAPI server for EdificIA.

Run:
    python3 -m uvicorn server:app --host 127.0.0.1 --port 8765

Then open:
    http://127.0.0.1:8765/3d.html
    http://127.0.0.1:8765/docs
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
from pathlib import Path
from typing import Any

logging.basicConfig(
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "caba_normativa.db"

from auth import (
    get_current_user,
    handle_google_callback,
    handle_google_login,
    handle_login,
    handle_logout,
    handle_me,
    handle_microsoft_callback,
    handle_microsoft_login,
    handle_register,
    init_users_table,
    login_page,
    require_active_user,
    track_usage,
    upsert_user,
)

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse, StreamingResponse

from chat import (
    DOWNLOADS_DIR,
    SessionManager,
    cleanup_old_downloads,
    create_sse_stream,
    init_chat_tables,
    _persist_entry,
)


class AuthMiddleware(BaseHTTPMiddleware):
    """Redirect to Google SSO if not authenticated."""

    OPEN_PATHS = (
        "/api/auth/", "/api/health", "/api/payments/", "/pricing.html",
        "/static/", "/favicon.ico",
    )

    async def dispatch(self, request: Request, call_next) -> StarletteResponse:
        # Skip auth entirely if no Google credentials configured (local dev)
        if not os.environ.get("GOOGLE_CLIENT_ID"):
            return await call_next(request)
        path = request.url.path
        # Let auth endpoints and health check through
        if any(path.startswith(p) for p in self.OPEN_PATHS):
            return await call_next(request)
        # Check session cookie
        user = get_current_user(request)
        if not user:
            return login_page(request)
        if not user.get("activo"):
            from starlette.responses import HTMLResponse
            return HTMLResponse(
                '<html><head><link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500&display=swap"></head>'
                '<body style="background:#000;color:#fff;font-family:Inter,system-ui,sans-serif;display:flex;align-items:center;justify-content:center;height:100vh">'
                '<div style="text-align:center;max-width:420px">'
                '<div style="font-size:10px;letter-spacing:5px;text-transform:uppercase;color:rgba(255,255,255,.25);margin-bottom:8px">E D I F I C <span style="color:rgba(255,215,0,.4)">I A</span></div>'
                '<h2 style="font-weight:300;margin:0 0 12px">Tu período de prueba terminó</h2>'
                '<p style="color:rgba(255,255,255,.4);font-size:14px;line-height:1.6;margin:0 0 32px">'
                'Contactá al equipo de EdificIA para continuar usando la plataforma.</p>'
                '<a href="mailto:karendmarini@gmail.com" style="display:inline-block;padding:14px 32px;border-radius:100px;'
                'background:#E8C547;color:#000;text-decoration:none;font-size:12px;font-weight:600;letter-spacing:2px;text-transform:uppercase">'
                'Contactar equipo</a>'
                '<div style="margin-top:24px"><a href="/api/auth/logout" style="color:rgba(255,255,255,.2);font-size:11px;text-decoration:none">Cerrar sesión</a></div>'
                '</div></body></html>', status_code=403,
            )
        # Redirect users without a plan to pricing page
        if not user.get("plan") and path != "/pricing.html":
            return RedirectResponse("/pricing.html", status_code=302)
        return await call_next(request)


_enable_docs = os.environ.get("ENABLE_DOCS", "").lower() == "true"
app = FastAPI(
    title="EdificIA API",
    version="0.1.0",
    root_path="",
    docs_url="/docs" if _enable_docs else None,
    redoc_url="/redoc" if _enable_docs else None,
    openapi_url="/openapi.json" if _enable_docs else None,
)
app.add_middleware(AuthMiddleware)
# Trust X-Forwarded-Proto from nginx so request.url.scheme = "https"
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=["127.0.0.1"])
_allowed_origins = os.environ.get(
    "ALLOWED_ORIGINS", "http://localhost:8765,http://127.0.0.1:8765"
).split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


sessions = SessionManager()


@app.on_event("startup")
def startup():
    init_users_table()
    init_chat_tables()
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)


async def _session_cleanup_loop() -> None:
    while True:
        await asyncio.sleep(60)
        await sessions.cleanup_expired()
        cleanup_old_downloads()


@app.on_event("startup")
async def startup_background_tasks():
    global _cleanup_task
    _cleanup_task = asyncio.create_task(_session_cleanup_loop())
    # Warmup agent in background thread
    asyncio.get_event_loop().run_in_executor(None, _warmup_sync)
    # Precache parcelas after a delay so server can respond to initial requests first
    asyncio.create_task(_delayed_precache())


_cleanup_task: asyncio.Task[None] | None = None


async def _delayed_precache() -> None:
    """Precache parcelas_geo one barrio at a time, yielding between each."""
    await asyncio.sleep(3)
    log = logging.getLogger("edificia.cache")
    log.info("precache: starting")
    list_barrios()
    barrios = _barrios_cache or []
    for b in barrios:
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, lambda name=b["name"]: parcelas_geo(barrio=name, metric="delta", limit=3000)
            )
        except Exception:
            pass
        await asyncio.sleep(0.1)  # Yield to event loop between barrios
    log.info("precache: done (%d barrios)", len(barrios))


def _warmup_sync() -> None:
    """Run SDK warmup in a thread so it doesn't block the event loop."""
    import asyncio as _aio
    loop = _aio.new_event_loop()
    loop.run_until_complete(sessions.warmup())
    loop.close()


# --- Auth routes ---

@app.get("/api/auth/google")
def auth_google(request: Request):
    return handle_google_login(request)


@app.get("/api/auth/callback")
def auth_callback(request: Request, code: str = Query(...)):
    return handle_google_callback(request, code)


@app.get("/api/auth/microsoft")
def auth_microsoft(request: Request):
    return handle_microsoft_login(request)


@app.get("/api/auth/microsoft/callback")
def auth_microsoft_callback(request: Request, code: str = Query(...)):
    return handle_microsoft_callback(request, code)


class RegisterRequest(BaseModel):
    email: str
    password: str
    nombre: str = ""


class LoginRequest(BaseModel):
    email: str
    password: str


@app.post("/api/auth/register")
def auth_register(body: RegisterRequest):
    return handle_register(body.email, body.password, body.nombre)


@app.post("/api/auth/login")
def auth_login_password(body: LoginRequest):
    return handle_login(body.email, body.password)


@app.get("/api/auth/logout")
def auth_logout():
    return handle_logout()


@app.get("/api/auth/me")
def auth_me(request: Request):
    return handle_me(request)


@app.get("/api/auth/plan")
def auth_plan(request: Request) -> dict[str, Any]:
    """Return current user's plan info for frontend model selector."""
    user = require_active_user(request)
    return {
        "plan": user.get("plan", "free"),
        "modelos_habilitados": json.loads(user.get("modelos_habilitados") or '["haiku"]'),
        "creditos_usd": user.get("creditos_usd", 0),
        "usd_mes_max": user.get("usd_mes_max", 0.02),
        "usd_used": user.get("usd_used_this_month", 0),
        "mb_mes_max": user.get("mb_mes_max", 1),
        "mb_used": user.get("mb_used_this_month", 0),
        "acceso_hasta": user.get("acceso_hasta"),
        "days_remaining": user.get("days_remaining"),
        "trial": user.get("trial", False),
    }


class UpsertUserRequest(BaseModel):
    email: str
    acceso_hasta: str
    plan: str = "free"
    nombre: str = ""
    creditos_usd: float | None = None
    modelos_habilitados: list[str] | None = None
    mb_mes_max: float | None = None
    usd_mes_max: float | None = None


ADMIN_EMAILS = frozenset({"juanwisznia@gmail.com"})


@app.post("/api/admin/users")
def admin_upsert_user(body: UpsertUserRequest, request: Request) -> dict[str, Any]:
    """Create or update a user. Enterprise-only."""
    user = require_active_user(request)
    if user["email"] not in ADMIN_EMAILS:
        raise HTTPException(403, "Admin access required")
    return upsert_user(
        email=body.email,
        acceso_hasta=body.acceso_hasta,
        plan=body.plan,
        nombre=body.nombre,
        creditos_usd=body.creditos_usd,
        modelos_habilitados=body.modelos_habilitados,
        mb_mes_max=body.mb_mes_max,
        usd_mes_max=body.usd_mes_max,
    )


# ── MercadoPago Suscripciones ─────────────────────────────────────

MP_ACCESS_TOKEN = os.environ.get("MP_ACCESS_TOKEN", "")
MP_PLAN_PRICE = 10000  # ARS/mes (100k con 90% off beta)
MP_PLAN_ID: str | None = None  # Set on first subscribe call


async def _get_or_create_mp_plan() -> str:
    """Get or create the EdificIA Pro subscription plan in MercadoPago."""
    global MP_PLAN_ID
    if MP_PLAN_ID:
        return MP_PLAN_ID
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.mercadopago.com/preapproval_plan",
            headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}"},
            json={
                "reason": "EdificIA Pro — Beta",
                "auto_recurring": {
                    "frequency": 1,
                    "frequency_type": "months",
                    "transaction_amount": MP_PLAN_PRICE,
                    "currency_id": "ARS",
                },
                "back_url": "https://edificia.website",
            },
        )
        data = resp.json()
        MP_PLAN_ID = data.get("id")
        return MP_PLAN_ID


@app.post("/api/payments/choose-free")
def choose_free(request: Request) -> dict[str, bool]:
    """Activate the free plan for the current user."""
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "Not authenticated")
    from auth import upsert_user, PLAN_DEFAULTS
    upsert_user(
        email=user["email"],
        acceso_hasta="2099-12-31",
        plan="free",
    )
    return {"ok": True}


@app.post("/api/payments/subscribe")
async def subscribe(request: Request) -> dict[str, Any]:
    """Create a subscription for the current user. Returns MP payment URL."""
    user = require_active_user(request)
    if user.get("plan") in ("pro", "enterprise"):
        return {"already_subscribed": True}

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://api.mercadopago.com/preapproval",
            headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}"},
            json={
                "reason": "EdificIA Pro — Beta",
                "auto_recurring": {
                    "frequency": 1,
                    "frequency_type": "months",
                    "transaction_amount": MP_PLAN_PRICE,
                    "currency_id": "ARS",
                },
                "payer_email": user["email"],
                "back_url": "https://edificia.website",
                "status": "pending",
            },
        )
        data = resp.json()

    if "init_point" not in data:
        raise HTTPException(500, f"MercadoPago error: {data.get('message', 'unknown')}")

    # Store subscription ID
    conn = db_connect()
    try:
        conn.execute(
            "UPDATE users SET mp_payment_id = ? WHERE id = ?",
            (data["id"], user["id"]),
        )
        conn.commit()
    finally:
        conn.close()

    return {"url": data["init_point"], "subscription_id": data["id"]}


@app.post("/api/payments/webhook")
async def mp_webhook(request: Request) -> dict[str, str]:
    """MercadoPago IPN webhook. Activates pro plan on confirmed payment."""
    body = await request.json()
    action = body.get("action", "")
    data_id = body.get("data", {}).get("id")

    if action == "payment.created" and data_id:
        # Fetch payment details from MP
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://api.mercadopago.com/v1/payments/{data_id}",
                headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}"},
            )
            payment = resp.json()

        if payment.get("status") == "approved":
            payer_email = payment.get("payer", {}).get("email", "")
            if payer_email:
                from auth import upsert_user, PLAN_DEFAULTS
                from datetime import date, timedelta
                expiry = (date.today() + timedelta(days=30)).isoformat()
                upsert_user(
                    email=payer_email,
                    acceso_hasta=expiry,
                    plan="pro",
                )
                logging.getLogger("edificia.payments").info(
                    "subscription activated: %s plan=pro until=%s", payer_email, expiry,
                )

    return {"status": "ok"}


class SearchResult(BaseModel):
    smp: str
    smp_norm: str
    direccion: str | None
    barrio: str | None
    comuna: str | None
    cpu: str | None
    area: float | None
    pisos: int | None
    has_cur3d: bool


def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=20)
    conn.row_factory = sqlite3.Row
    return conn


def smp_norm(value: str) -> str:
    if not value:
        return ""
    value = value.replace(" ", "").upper()
    parts = value.split("-")
    normalized: list[str] = []
    for part in parts:
        digits = re.sub(r"[^0-9]", "", part)
        letters = re.sub(r"[^A-Z]", "", part)
        normalized.append(f"{int(digits)}{letters}" if digits else part)
    return "-".join(normalized)


def normalize_query(value: str) -> str:
    cleaned = value.upper().strip()
    cleaned = cleaned.replace(",", " ")
    cleaned = re.sub(r"\s+", " ", cleaned)
    return cleaned


def get_polygon(data: dict[str, Any]) -> list[list[float]] | None:
    """Extract polygon from DB row's polygon_geojson field."""
    raw = data.get("polygon_geojson")
    if not raw:
        return None
    return json.loads(raw) if isinstance(raw, str) else raw


def serialize_row(row: sqlite3.Row) -> dict[str, Any]:
    data: dict[str, Any] = {key: row[key] for key in row.keys()}
    if data.get("edif_linderas"):
        try:
            data["edif_linderas"] = json.loads(data["edif_linderas"])
        except json.JSONDecodeError:
            pass
    return data


def fetch_parcel_by_smp(conn: sqlite3.Connection, value: str) -> sqlite3.Row | None:
    norm = smp_norm(value)
    return conn.execute(
        "SELECT * FROM parcelas WHERE smp = ? OR smp_norm = ? LIMIT 1",
        (value, norm),
    ).fetchone()


def search_rows(conn: sqlite3.Connection, query: str, limit: int) -> list[sqlite3.Row]:
    clean = normalize_query(query)
    norm = smp_norm(clean)
    like = f"%{clean}%"

    return conn.execute(
        """
        SELECT
            smp,
            smp_norm,
            COALESCE(epok_direccion, uso_calle || ' ' || COALESCE(uso_puerta, '')) AS epok_direccion,
            barrio,
            comuna,
            cpu,
            area,
            pisos,
            COALESCE(cur3d_enriched, 0) AS cur3d_enriched
        FROM parcelas
        WHERE
            smp_norm = :norm
            OR upper(replace(COALESCE(epok_direccion, ''), ',', '')) LIKE :like
            OR upper(COALESCE(epok_calle, '')) LIKE :like
            OR upper(COALESCE(uso_calle, '') || ' ' || COALESCE(uso_puerta, '')) LIKE :like
        ORDER BY
            CASE
                WHEN smp_norm = :norm THEN 0
                WHEN upper(replace(COALESCE(epok_direccion, ''), ',', '')) = :clean THEN 1
                ELSE 2
            END,
            COALESCE(cur3d_enriched, 0) DESC,
            COALESCE(epok_enriched, 0) DESC,
            id
        LIMIT :limit
        """,
        {"norm": norm, "like": like, "clean": clean, "limit": limit},
    ).fetchall()


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/index.html")


@app.get("/api/health")
def health() -> dict[str, Any]:
    import psutil

    with db_connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM parcelas").fetchone()[0]
        cur3d = conn.execute(
            "SELECT COUNT(*) FROM parcelas WHERE COALESCE(cur3d_enriched, 0) = 1"
        ).fetchone()[0]
        epok = conn.execute(
            "SELECT COUNT(*) FROM parcelas WHERE COALESCE(epok_enriched, 0) = 1"
        ).fetchone()[0]

    # System metrics
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    proc = psutil.Process()
    cli_procs = []
    for child in proc.children(recursive=True):
        try:
            cmd = " ".join(child.cmdline())
            if "claude" in cmd:
                cli_procs.append({"pid": child.pid, "rss_mb": round(child.memory_info().rss / 1e6, 1), "status": child.status()})
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    return {
        "ok": True,
        "total": total,
        "epok": epok,
        "cur3d": cur3d,
        "system": {
            "ram_total_mb": round(mem.total / 1e6),
            "ram_used_mb": round(mem.used / 1e6),
            "ram_available_mb": round(mem.available / 1e6),
            "swap_used_mb": round(swap.used / 1e6),
            "server_rss_mb": round(proc.memory_info().rss / 1e6, 1),
            "cli_processes": cli_procs,
            "precache_keys": len(_parcelas_geo_cache),
            "chat_sessions": sessions.active_count,
        },
    }


@app.get("/api/search", response_model=list[SearchResult])
def search(
    q: str = Query(..., min_length=2, description="Address fragment or SMP"),
    limit: int = Query(8, ge=1, le=25),
) -> list[SearchResult]:
    with db_connect() as conn:
        rows = search_rows(conn, q, limit)

    results = [
        SearchResult(
            smp=row["smp"],
            smp_norm=row["smp_norm"],
            direccion=row["epok_direccion"],
            barrio=row["barrio"],
            comuna=row["comuna"],
            cpu=row["cpu"],
            area=row["area"],
            pisos=row["pisos"],
            has_cur3d=bool(row["cur3d_enriched"]),
        )
        for row in rows
    ]
    return results


@app.get("/api/parcela_nearest")
async def get_nearest_parcel(
    lat: float = Query(...), lng: float = Query(...),
) -> dict[str, Any]:
    """Find the parcel at a lat/lng coordinate.

    First tries USIG reverse geocoding for exact SMP, then falls back to
    nearest-neighbor SQL.
    """
    # Step 1: USIG reverse geocoding for exact SMP
    exact_smp = None
    try:
        gk_ox, gk_oy = 107253.769166, 102807.160072
        lng_o, lat_o = -58.384222, -34.603939
        gkx = gk_ox + (lng - lng_o) * 91724.6547
        gky = gk_oy + (lat - lat_o) * 111003.9032
        async with httpx.AsyncClient(timeout=4) as client:
            resp = await client.get(
                f"https://ws.usig.buenosaires.gob.ar/geocoder/2.2/reversegeocoding?x={gkx:.3f}&y={gky:.3f}"
            )
            if resp.status_code == 200:
                raw = resp.text.strip().strip("()")
                parsed = json.loads(raw)
                if parsed.get("parcela"):
                    exact_smp = parsed["parcela"]
    except Exception:
        pass

    with db_connect() as conn:
        row = None
        if exact_smp:
            norm = smp_norm(exact_smp)
            row = conn.execute(
                "SELECT * FROM parcelas WHERE smp_norm = ?", (norm,)
            ).fetchone()
        if not row:
            row = conn.execute(
                "SELECT * FROM parcelas WHERE lat IS NOT NULL "
                "ORDER BY (lat-?)*(lat-?)+(lng-?)*(lng-?) LIMIT 1",
                (lat, lat, lng, lng),
            ).fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="No parcels found")
    data = serialize_row(row)
    data["polygon"] = get_polygon(data)
    return data


@app.get("/api/parcela/{parcel_smp}")
def get_parcel(parcel_smp: str) -> dict[str, Any]:
    with db_connect() as conn:
        row = fetch_parcel_by_smp(conn, parcel_smp)

    if row is None:
        raise HTTPException(status_code=404, detail="Parcel not found")

    data = serialize_row(row)
    data["polygon"] = get_polygon(data)
    data["has_polygon"] = data["polygon"] is not None
    data["has_cur3d"] = bool(data.get("cur3d_enriched"))
    data["has_epok"] = bool(data.get("epok_enriched"))
    return data


@app.get("/api/parcela")
def get_parcel_by_query(
    smp: str | None = Query(None, description="Exact SMP"),
    q: str | None = Query(None, description="Address fragment or SMP"),
) -> dict[str, Any]:
    if not smp and not q:
        raise HTTPException(status_code=400, detail="Provide smp or q")

    with db_connect() as conn:
        row: sqlite3.Row | None = None
        matches: list[SearchResult] = []

        if smp:
            row = fetch_parcel_by_smp(conn, smp)
        else:
            rows = search_rows(conn, q or "", 5)
            matches = [
                SearchResult(
                    smp=item["smp"],
                    smp_norm=item["smp_norm"],
                    direccion=item["epok_direccion"],
                    barrio=item["barrio"],
                    comuna=item["comuna"],
                    cpu=item["cpu"],
                    area=item["area"],
                    pisos=item["pisos"],
                    has_cur3d=bool(item["cur3d_enriched"]),
                )
                for item in rows
            ]

            if len(rows) == 1:
                row = fetch_parcel_by_smp(conn, rows[0]["smp"])

    if row is None:
        if matches:
            raise HTTPException(
                status_code=409,
                detail={"message": "Ambiguous query", "matches": [m.model_dump() for m in matches]},
            )
        raise HTTPException(status_code=404, detail="Parcel not found")

    data = serialize_row(row)
    data["polygon"] = get_polygon(data)
    data["has_polygon"] = data["polygon"] is not None
    data["has_cur3d"] = bool(data.get("cur3d_enriched"))
    data["has_epok"] = bool(data.get("epok_enriched"))
    return data


@app.get("/api/parcelas_geo")
def parcelas_geo(
    barrio: str | None = Query(None),
    metric: str = Query("delta"),
    limit: int = Query(2000, ge=100, le=5000),
    pisos_min: int | None = Query(None),
    pisos_max: int | None = Query(None),
    area_min: float | None = Query(None),
    area_max: float | None = Query(None),
    fot_min: float | None = Query(None),
    pl_min: float | None = Query(None),
    uso: str | None = Query(None),
    aph: str | None = Query(None),
    riesgo_hidrico: str | None = Query(None),
    enrase: str | None = Query(None),
) -> dict[str, Any]:
    """Return GeoJSON of top parcels by metric, with optional filters."""
    # Cache for common case: barrio + metric, no extra filters
    has_filters = any(v is not None for v in [pisos_min, pisos_max, area_min, area_max, fot_min, pl_min, uso, aph, riesgo_hidrico, enrase])
    if barrio and not has_filters:
        cache_key = f"{barrio}:{metric}:{limit}"
        if cache_key in _parcelas_geo_cache:
            return _parcelas_geo_cache[cache_key]

    metric_col = {
        "delta": "CASE WHEN tejido_altura_max IS NOT NULL THEN plano_san - tejido_altura_max ELSE 0 END",
        "vol": "COALESCE(vol_edificable, 0)",
        "pisos": "COALESCE(pisos, 0)",
        "area": "COALESCE(area, 0)",
    }.get(metric, "COALESCE(plano_san - COALESCE(tejido_altura_max, 0), 0)")

    where = "polygon_geojson IS NOT NULL AND area > 50"
    params: dict[str, Any] = {"limit": limit}
    if barrio:
        where += " AND barrio = :barrio"
        params["barrio"] = barrio
    if pisos_min is not None:
        where += " AND pisos >= :pisos_min"
        params["pisos_min"] = pisos_min
    if pisos_max is not None:
        where += " AND pisos <= :pisos_max"
        params["pisos_max"] = pisos_max
    if area_min is not None:
        where += " AND area >= :area_min"
        params["area_min"] = area_min
    if area_max is not None:
        where += " AND area <= :area_max"
        params["area_max"] = area_max
    if fot_min is not None:
        where += " AND fot >= :fot_min"
        params["fot_min"] = fot_min
    if pl_min is not None:
        where += " AND plano_san >= :pl_min"
        params["pl_min"] = pl_min
    if uso:
        where += " AND uso_tipo1 = :uso"
        params["uso"] = uso
    if aph == "1":
        where += " AND (es_aph = 1 OR edif_catalogacion_proteccion IN ('CAUTELAR','ESTRUCTURAL','GENERAL'))"
    elif aph == "0":
        where += " AND (es_aph IS NULL OR es_aph = 0) AND (edif_catalogacion_proteccion IS NULL OR edif_catalogacion_proteccion = 'DESESTIMADO')"
    if riesgo_hidrico == "1":
        where += " AND edif_riesgo_hidrico IS NOT NULL AND edif_riesgo_hidrico != ''"
    if enrase == "1":
        where += " AND edif_enrase = 1"

    with db_connect() as conn:
        rows = conn.execute(
            f"""SELECT smp, lat, lng, polygon_geojson,
                cpu, barrio, area, pisos, plano_san, tejido_altura_max,
                vol_edificable, sup_vendible, fot, uso_tipo1, uso_tipo2, epok_direccion,
                frente, fondo, delta_pisos, epok_pisos_sobre,
                es_aph, edif_catalogacion_proteccion, edif_riesgo_hidrico,
                edif_enrase, edif_plusvalia_incidencia_uva, edif_plusvalia_alicuota,
                {metric_col} as score
            FROM parcelas
            WHERE {where}
            ORDER BY {metric_col} DESC
            LIMIT :limit""",
            params,
        ).fetchall()

    features = []
    for r in rows:
        coords = json.loads(r["polygon_geojson"])
        # GeoJSON polygon needs nested array: [[ [lng,lat], ... ]]
        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [coords]},
            "properties": {
                "smp": r["smp"],
                "dir": r["epok_direccion"],
                "cpu": r["cpu"],
                "barrio": r["barrio"],
                "area": r["area"],
                "pisos": r["pisos"],
                "pl": r["plano_san"],
                "tj": r["tejido_altura_max"],
                "vol": r["vol_edificable"],
                "vendible": r["sup_vendible"],
                "fot": r["fot"],
                "uso": r["uso_tipo1"],
                "uso2": r["uso_tipo2"],
                "fr": r["frente"],
                "fo": r["fondo"],
                "delta_pisos": r["delta_pisos"],
                "pisos_actual": r["epok_pisos_sobre"],
                "aph": r["es_aph"],
                "catalogacion": r["edif_catalogacion_proteccion"],
                "riesgo": r["edif_riesgo_hidrico"],
                "enrase": r["edif_enrase"],
                "plusvalia_uva": r["edif_plusvalia_incidencia_uva"],
                "plusvalia_alic": r["edif_plusvalia_alicuota"],
                "score": r["score"],
            },
        })

    result = {"type": "FeatureCollection", "features": features}

    # Cache default queries
    if barrio and not has_filters:
        _parcelas_geo_cache[f"{barrio}:{metric}:{limit}"] = result

    return result


_barrios_cache: list[dict[str, Any]] | None = None
_parcelas_geo_cache: dict[str, dict[str, Any]] = {}  # barrio -> geojson


@app.get("/api/barrios")
def list_barrios() -> list[dict[str, Any]]:
    global _barrios_cache
    if _barrios_cache is not None:
        return _barrios_cache
    with db_connect() as conn:
        rows = conn.execute("""
            SELECT barrio, COUNT(*) as n, ROUND(AVG(plano_san - COALESCE(tejido_altura_max,0)),1) as avg_delta
            FROM parcelas WHERE barrio IS NOT NULL AND barrio != ''
            GROUP BY barrio ORDER BY barrio
        """).fetchall()
    _barrios_cache = [{"name": r["barrio"], "count": r["n"], "avg_delta": r["avg_delta"]} for r in rows]
    return _barrios_cache



@app.get("/api/linderos/{parcel_smp}")
def get_linderos(parcel_smp: str) -> dict:
    """Devuelve las alturas reales (tejido) de las parcelas linderas."""
    with db_connect() as conn:
        row = fetch_parcel_by_smp(conn, parcel_smp)
        if not row:
            raise HTTPException(404, "Parcela no encontrada")
        data = serialize_row(row)
        linderas_raw = data.get("edif_linderas") or []
        if isinstance(linderas_raw, str):
            try:
                linderas_raw = json.loads(linderas_raw)
            except Exception:
                linderas_raw = []
        # Puede ser una lista de SMPs o un dict con smp_linderas
        if isinstance(linderas_raw, dict):
            smps = linderas_raw.get("smp_linderas", [])
        elif isinstance(linderas_raw, list):
            smps = linderas_raw
        else:
            smps = []

        results = []
        for smp in smps[:6]:  # máximo 6 linderos
            norm = smp_norm(str(smp))
            r2 = conn.execute(
                "SELECT smp, tejido_altura_max, tejido_altura_avg, h, plano_san, cur_distrito FROM parcelas WHERE smp_norm = ? LIMIT 1",
                (norm,)
            ).fetchone()
            if r2:
                results.append({
                    "smp": r2["smp"],
                    "tejido_altura_max": r2["tejido_altura_max"],
                    "tejido_altura_avg": r2["tejido_altura_avg"],
                    "h": r2["h"],
                    "plano_san": r2["plano_san"],
                    "cur_distrito": r2["cur_distrito"],
                })
        return {
            "smp": parcel_smp,
            "enrase_flag": bool(data.get("edif_enrase")),
            "plano_san": data.get("plano_san"),
            "cur_distrito": data.get("cur_distrito"),
            "linderos": results,
        }

@app.get("/api/envelope/{parcel_smp}")
def get_envelope(parcel_smp: str) -> dict[str, Any]:
    """Return the stepped buildable envelope geometry for a parcel."""
    with db_connect() as conn:
        row = fetch_parcel_by_smp(conn, parcel_smp)

    if row is None:
        raise HTTPException(status_code=404, detail="Parcel not found")

    data = dict(row)
    polygon = get_polygon(data)

    if not polygon:
        raise HTTPException(status_code=404, detail="No polygon for this parcel")

    altura_max = data.get("edif_altura_max_1") or data.get("plano_san") or data.get("h")
    plano_limite = data.get("edif_plano_limite") or data.get("plano_san")
    frente = data.get("frente") or data.get("epok_frente") or 8.66
    fondo = data.get("fondo") or data.get("epok_fondo") or 30.0
    sup_edif = data.get("edif_sup_edificable_planta")
    sup_parc = data.get("edif_superficie_parcela") or data.get("area")

    # Try GCBA precomputed envelope sections first
    with db_connect() as conn:
        vt_sections = conn.execute(
            "SELECT tipo, altura_inicial, altura_fin, polygon_geojson FROM envelope_sections WHERE UPPER(smp) = UPPER(?)",
            (data["smp"],),
        ).fetchall()

    if vt_sections:
        sections = []
        for s in vt_sections:
            coords = json.loads(s["polygon_geojson"])
            # Vector tile coords are already [[[lng,lat],...]]
            poly = coords[0] if coords else []
            sections.append({
                "polygon": poly,
                "base": s["altura_inicial"],
                "top": s["altura_fin"],
                "label": s["tipo"],
            })
    else:
        # Fallback to computed envelope
        from envelope import compute_envelope
        sections = compute_envelope(
            polygon=polygon,
            altura_max=altura_max,
            plano_limite=plano_limite,
            frente_m=frente,
            fondo_m=fondo,
            sup_edificable=sup_edif,
            sup_parcela=sup_parc,
        )

    return {
        "smp": data["smp"],
        "direccion": data.get("epok_direccion"),
        "cpu": data.get("cpu"),
        "barrio": data.get("barrio"),
        "altura_max": altura_max,
        "plano_limite": plano_limite,
        "frente": frente,
        "fondo": fondo,
        "sup_edificable": sup_edif,
        "sup_parcela": sup_parc,
        "parcel_polygon": polygon,
        "sections": sections,
    }


# ── Chat routes ──────────────────────────────────────────────────


class ChatRequest(BaseModel):
    session_id: str
    message: str
    model: str = "sonnet"


class EntryRequest(BaseModel):
    session_id: str
    kind: str
    content: str


@app.post("/api/chat")
async def chat_endpoint(request: Request) -> StreamingResponse:
    user = get_current_user(request)
    user_id = user["id"] if user else None
    if os.environ.get("GOOGLE_CLIENT_ID"):
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")
        if not user.get("activo"):
            raise HTTPException(status_code=403, detail="Account not active")

    body = ChatRequest(**(await request.json()))

    # Check model access and limits
    if user:
        allowed = json.loads(user.get("modelos_habilitados") or '["haiku"]')
        if body.model not in allowed:
            raise HTTPException(403, f"Modelo {body.model} no disponible en tu plan")
        if user.get("usd_used_this_month", 0) >= (user.get("usd_mes_max") or 0.02):
            raise HTTPException(403, "Límite mensual de uso alcanzado")
        if (user.get("creditos_usd") or 0) <= 0:
            raise HTTPException(403, "Créditos agotados")

    client = await sessions.get_or_create(body.session_id, body.model)

    return StreamingResponse(
        create_sse_stream(
            client, body.message,
            session_id=body.session_id, user_id=user_id, model=body.model,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


@app.get("/api/chat/sessions")
def list_chat_sessions(request: Request) -> list[dict[str, Any]]:
    """List the authenticated user's chat sessions."""
    user = require_active_user(request)
    conn = db_connect()
    try:
        rows = conn.execute(
            "SELECT id, created_at, last_used, preview, model "
            "FROM chat_sessions WHERE user_id = ? ORDER BY last_used DESC LIMIT 50",
            (user["id"],),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/api/chat/sessions/{session_id}")
def get_chat_session(session_id: str, request: Request) -> dict[str, Any]:
    """Get all entries for a chat session."""
    user = require_active_user(request)
    conn = db_connect()
    try:
        session = conn.execute(
            "SELECT * FROM chat_sessions WHERE id = ? AND user_id = ?",
            (session_id, user["id"]),
        ).fetchone()
        if not session:
            raise HTTPException(status_code=404, detail="Session not found")
        entries = conn.execute(
            "SELECT id, kind, content, created_at FROM chat_entries "
            "WHERE session_id = ? ORDER BY id", (session_id,),
        ).fetchall()
        return {"session": dict(session), "entries": [dict(e) for e in entries]}
    finally:
        conn.close()


@app.post("/api/chat/entries")
def save_chat_entry(body: EntryRequest, request: Request) -> dict[str, bool]:
    """Save a programmatic entry (map click, barrio selection). No LLM call."""
    user = get_current_user(request)
    user_id = user["id"] if user else None
    _persist_entry(
        body.session_id, body.kind, body.content, user_id=user_id,
    )
    return {"ok": True}


@app.delete("/api/chat/{session_id}")
async def delete_chat_session(session_id: str) -> dict[str, bool]:
    await sessions.delete(session_id)
    return {"ok": True}


@app.get("/api/downloads/{filename}")
async def download_file(filename: str) -> FileResponse:
    safe_name = Path(filename).name
    file_path = DOWNLOADS_DIR / safe_name
    if not file_path.exists() or not file_path.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path=str(file_path), filename=safe_name, media_type="application/octet-stream")


_SAFE_STATIC_EXTS = frozenset({
    ".html", ".js", ".css", ".json", ".png", ".jpg", ".jpeg",
    ".svg", ".ico", ".woff", ".woff2", ".gif", ".webp",
})


class StaticFileFilterMiddleware(BaseHTTPMiddleware):
    """Allowlist middleware: only serve files with safe extensions."""

    async def dispatch(self, request: Request, call_next) -> StarletteResponse:
        path = request.url.path
        if path.startswith("/api/"):
            return await call_next(request)
        ext = Path(path).suffix.lower()
        if ext and ext not in _SAFE_STATIC_EXTS:
            from starlette.responses import Response

            return Response(status_code=404)
        response = await call_next(request)
        # Prevent browser caching of HTML/JS/CSS so deploys take effect immediately
        if ext in (".html", ".js", ".css") or ext == "":
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"
        return response


app.add_middleware(StaticFileFilterMiddleware)
app.mount("/", StaticFiles(directory=BASE_DIR, html=True), name="static")
