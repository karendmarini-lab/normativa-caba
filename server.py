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
    handle_register,
    init_users_table,
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

    OPEN_PATHS = ("/api/auth/", "/api/health")

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
            login_url = str(request.base_url).rstrip("/") + "/api/auth/google"
            return RedirectResponse(login_url, status_code=302)
        if not user.get("activo"):
            from starlette.responses import HTMLResponse
            msg = "Tu acceso expiró." if user.get("expired") else "Tu cuenta no está activa."
            return HTMLResponse(
                f'<html><body style="background:#000;color:#fff;font-family:system-ui;display:flex;align-items:center;justify-content:center;height:100vh;flex-direction:column">'
                f'<h2>{msg}</h2>'
                f'<p style="color:#999;margin-top:12px">Contacto: <a href="mailto:karendmarini@gmail.com" style="color:#e8c547">karendmarini@gmail.com</a></p>'
                f'<a href="/api/auth/logout" style="color:#666;margin-top:20px;font-size:12px">Cerrar sesión</a>'
                f'</body></html>', status_code=403,
            )
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
    asyncio.create_task(sessions.warmup())


_cleanup_task: asyncio.Task[None] | None = None


# --- Auth routes ---

@app.get("/api/auth/google")
def auth_google(request: Request):
    return handle_google_login(request)


@app.get("/api/auth/callback")
def auth_callback(request: Request, code: str = Query(...)):
    return handle_google_callback(request, code)


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
    with db_connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM parcelas").fetchone()[0]
        cur3d = conn.execute(
            "SELECT COUNT(*) FROM parcelas WHERE COALESCE(cur3d_enriched, 0) = 1"
        ).fetchone()[0]
        epok = conn.execute(
            "SELECT COUNT(*) FROM parcelas WHERE COALESCE(epok_enriched, 0) = 1"
        ).fetchone()[0]

    return {"ok": True, "total": total, "epok": epok, "cur3d": cur3d}


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

    return {"type": "FeatureCollection", "features": features}


@app.get("/api/barrios")
def list_barrios() -> list[dict[str, Any]]:
    with db_connect() as conn:
        rows = conn.execute("""
            SELECT barrio, COUNT(*) as n, ROUND(AVG(plano_san - COALESCE(tejido_altura_max,0)),1) as avg_delta
            FROM parcelas WHERE barrio IS NOT NULL AND barrio != ''
            GROUP BY barrio ORDER BY barrio
        """).fetchall()
    return [{"name": r["barrio"], "count": r["n"], "avg_delta": r["avg_delta"]} for r in rows]


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
        # Prevent browser caching of JS/CSS so deploys take effect immediately
        if ext in (".js", ".css"):
            response.headers["Cache-Control"] = "no-cache, must-revalidate"
        return response


app.add_middleware(StaticFileFilterMiddleware)
app.mount("/", StaticFiles(directory=BASE_DIR, html=True), name="static")
