#!/usr/bin/env python3
"""
Pre-compute GeoJSON files for all barrios and metrics.

Generates static/geo/{barrio}_{metric}.json files that nginx serves
directly, eliminating SQLite reads at request time.

Usage:
    python3 precompute_geojson.py              # Generate all
    python3 precompute_geojson.py --barrio Palermo  # Single barrio
"""

import json
import sqlite3
import sys
import time
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "caba_normativa.db"
OUT_DIR = Path(__file__).resolve().parent / "static" / "geo"

METRICS = {
    "delta": "CASE WHEN tejido_altura_max IS NOT NULL THEN plano_san - tejido_altura_max ELSE 0 END",
    "vol": "COALESCE(vol_edificable, 0)",
    "pisos": "COALESCE(pisos, 0)",
    "area": "COALESCE(area, 0)",
}

LIMIT = 3000

SELECT_COLS = """smp, lat, lng, polygon_geojson,
    cpu, barrio, area, pisos, plano_san, tejido_altura_max,
    vol_edificable, sup_vendible, fot, uso_tipo1, uso_tipo2, epok_direccion,
    frente, fondo, delta_pisos, epok_pisos_sobre,
    es_aph, edif_catalogacion_proteccion, edif_riesgo_hidrico,
    edif_enrase, edif_plusvalia_incidencia_uva, edif_plusvalia_alicuota"""


def build_geojson(conn: sqlite3.Connection, barrio: str, metric: str) -> dict:
    """Build a GeoJSON FeatureCollection for a barrio+metric."""
    metric_col = METRICS[metric]
    rows = conn.execute(
        f"""SELECT {SELECT_COLS}, {metric_col} as score
        FROM parcelas
        WHERE polygon_geojson IS NOT NULL AND area > 50
          AND barrio = :barrio
        ORDER BY {metric_col} DESC
        LIMIT :limit""",
        {"barrio": barrio, "limit": LIMIT},
    ).fetchall()

    features = []
    for r in rows:
        coords = json.loads(r["polygon_geojson"])
        props = {
            "smp": r["smp"], "score": r["score"],
            "barrio": r["barrio"], "cpu": r["cpu"],
            "area": r["area"], "pisos": r["pisos"],
            "plano_san": r["plano_san"], "tejido": r["tejido_altura_max"],
            "vol": r["vol_edificable"], "vendible": r["sup_vendible"],
            "fot": r["fot"], "uso1": r["uso_tipo1"], "uso2": r["uso_tipo2"],
            "dir": r["epok_direccion"], "fr": r["frente"], "fo": r["fondo"],
            "dp": r["delta_pisos"], "pisos_e": r["epok_pisos_sobre"],
            "aph": r["es_aph"], "cat": r["edif_catalogacion_proteccion"],
            "rh": r["edif_riesgo_hidrico"], "enrase": r["edif_enrase"],
            "plusv_uva": r["edif_plusvalia_incidencia_uva"],
            "plusv_al": r["edif_plusvalia_alicuota"],
        }
        features.append({
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [coords]},
            "properties": props,
        })

    return {"type": "FeatureCollection", "features": features}


def main():
    only_barrio = None
    if "--barrio" in sys.argv:
        idx = sys.argv.index("--barrio")
        only_barrio = sys.argv[idx + 1] if idx + 1 < len(sys.argv) else None

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")

    if only_barrio:
        barrios = [only_barrio]
    else:
        rows = conn.execute(
            "SELECT DISTINCT barrio FROM parcelas "
            "WHERE barrio IS NOT NULL AND polygon_geojson IS NOT NULL "
            "ORDER BY barrio"
        ).fetchall()
        barrios = [r["barrio"] for r in rows]

    total_files = 0
    total_bytes = 0
    start = time.time()

    for barrio in barrios:
        for metric in METRICS:
            geojson = build_geojson(conn, barrio, metric)
            # Sanitize barrio name for filename
            safe_name = barrio.replace(" ", "_").replace(".", "")
            path = OUT_DIR / f"{safe_name}_{metric}.json"
            data = json.dumps(geojson, ensure_ascii=False, separators=(",", ":"))
            path.write_text(data)
            total_files += 1
            total_bytes += len(data)
            print(f"  {path.name}: {len(geojson['features'])} features, {len(data) // 1024}KB")

    conn.close()
    elapsed = time.time() - start
    print(f"\nDone: {total_files} files, {total_bytes // 1024 // 1024}MB total, {elapsed:.1f}s")


if __name__ == "__main__":
    main()
