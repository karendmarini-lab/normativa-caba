"""
FastAPI server for EdificIA.

Run:
    python3 -m uvicorn server:app --host 127.0.0.1 --port 8765

Then open:
    http://127.0.0.1:8765/3d.html
    http://127.0.0.1:8765/docs
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
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
)

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse


class AuthMiddleware(BaseHTTPMiddleware):
    """Redirect to Google SSO if not authenticated."""

    OPEN_PATHS = ("/api/auth/", "/api/health")

    async def dispatch(self, request: Request, call_next) -> StarletteResponse:
        path = request.url.path
        # Let auth endpoints and health check through
        if any(path.startswith(p) for p in self.OPEN_PATHS):
            return await call_next(request)
        # Check session cookie
        user = get_current_user(request)
        if not user:
            login_url = str(request.base_url).rstrip("/") + "/api/auth/google"
            return RedirectResponse(login_url, status_code=302)
        return await call_next(request)


app = FastAPI(title="EdificIA API", version="0.1.0")
app.add_middleware(AuthMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_users_table()


# --- Auth routes ---

@app.get("/api/auth/google")
def auth_google(request: Request):
    return handle_google_login(request)


@app.get("/api/auth/callback")
def auth_callback(request: Request, code: str = Query(...)):
    return handle_google_callback(request, code)


@app.post("/api/auth/register")
def auth_register(email: str = Query(...), password: str = Query(...), nombre: str = Query("")):
    return handle_register(email, password, nombre)


@app.post("/api/auth/login")
def auth_login_password(email: str = Query(...), password: str = Query(...)):
    return handle_login(email, password)


@app.get("/api/auth/logout")
def auth_logout():
    return handle_logout()


@app.get("/api/auth/me")
def auth_me(request: Request):
    return handle_me(request)


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
) -> dict[str, Any]:
    """Return GeoJSON of top parcels by metric, optionally filtered by barrio."""
    metric_col = {
        "delta": "CASE WHEN tejido_altura_max IS NOT NULL THEN plano_san - tejido_altura_max ELSE 0 END",
        "vol": "COALESCE(vol_edificable, 0)",
        "pisos": "COALESCE(pisos, 0)",
        "area": "COALESCE(area, 0)",
        "reconversion": "CASE WHEN uso_tipo1 IN ('GARAGE COMERCIAL','INDUSTRIAL','SIN USO IDENTIFICADO','ESTACION DE SERVICIO') THEN 1 ELSE 0 END",
    }.get(metric, "COALESCE(plano_san - COALESCE(tejido_altura_max, 0), 0)")

    where = "polygon_geojson IS NOT NULL AND area > 50"
    params: dict[str, Any] = {"limit": limit}
    if barrio:
        where += " AND barrio = :barrio"
        params["barrio"] = barrio

    with db_connect() as conn:
        rows = conn.execute(
            f"""SELECT smp, lat, lng, polygon_geojson,
                cpu, barrio, area, pisos, plano_san, tejido_altura_max,
                vol_edificable, uso_tipo1, epok_direccion,
                frente, fondo,
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
                "uso": r["uso_tipo1"],
                "fr": r["frente"],
                "fo": r["fondo"],
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
    from envelope import compute_envelope

    with db_connect() as conn:
        row = fetch_parcel_by_smp(conn, parcel_smp)

    if row is None:
        raise HTTPException(status_code=404, detail="Parcel not found")

    data = dict(row)

    # Get polygon from DB or CSV fallback
    polygon = get_polygon(data)

    if not polygon:
        raise HTTPException(status_code=404, detail="No polygon for this parcel")

    altura_max = data.get("edif_altura_max_1") or data.get("plano_san") or data.get("h")
    plano_limite = data.get("edif_plano_limite") or data.get("plano_san")
    frente = data.get("frente") or data.get("epok_frente") or 8.66
    fondo = data.get("fondo") or data.get("epok_fondo") or 30.0
    sup_edif = data.get("edif_sup_edificable_planta")
    sup_parc = data.get("edif_superficie_parcela") or data.get("area")

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


app.mount("/", StaticFiles(directory=BASE_DIR, html=True), name="static")
