"""Rebuild tile_volumen.db handling both Polygon and MultiPolygon.

The original download_volumen_tiles.py skipped MultiPolygon features
(3.8% of parcels). This script re-downloads all tiles and sums areas
across sub-polygons.

Source: vectortiles.usig.buenosaires.gob.ar/cur3d/volumen_edif/{z}/{x}/{y}.pbf
"""

import math
import re
import sqlite3
import time

import mapbox_vector_tile as mvt
import requests
from shapely.geometry import Polygon

LAT_MIN, LAT_MAX = -34.705, -34.535
LON_MIN, LON_MAX = -58.535, -58.335
ZOOM = 15
EXTENT = 4096
TIMEOUT = 30
DB_PATH = "tile_volumen_v3.db"


def tile_range() -> list[tuple[int, int]]:
    """All tile (x, y) pairs covering CABA at ZOOM level."""
    n = 2**ZOOM
    x_min = int((LON_MIN + 180) / 360 * n)
    x_max = int((LON_MAX + 180) / 360 * n)
    y_min = int(
        (1 - math.log(math.tan(math.radians(LAT_MAX))
                      + 1 / math.cos(math.radians(LAT_MAX))) / math.pi)
        / 2 * n
    )
    y_max = int(
        (1 - math.log(math.tan(math.radians(LAT_MIN))
                      + 1 / math.cos(math.radians(LAT_MIN))) / math.pi)
        / 2 * n
    )
    return [(x, y) for x in range(x_min, x_max + 1)
            for y in range(y_min, y_max + 1)]


def tile_bounds(x: int, y: int) -> tuple[float, float, float, float]:
    """Return (west, east, north, south) in WGS84."""
    n = 2**ZOOM
    west = x / n * 360 - 180
    east = (x + 1) / n * 360 - 180
    north = math.atan(math.sinh(math.pi * (1 - 2 * y / n))) * 180 / math.pi
    south = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n))) * 180 / math.pi
    return west, east, north, south


def ring_area_m2(
    ring_px: list[list[int]],
    west: float, east: float, north: float, south: float,
) -> float:
    """Convert one pixel ring to WGS84 polygon and compute area in m²."""
    ring_wgs = [
        [west + (p[0] / EXTENT) * (east - west),
         north - (p[1] / EXTENT) * (north - south)]
        for p in ring_px
    ]
    poly = Polygon(ring_wgs)
    if not poly.is_valid or poly.area <= 0:
        return 0.0
    cos_lat = math.cos(math.radians(-34.6))
    return poly.area * 111000 * cos_lat * 111000


def normalize_smp(s: str) -> str:
    """019-016-026 -> 19-16-26"""
    parts = s.split("-")
    return "-".join(re.sub(r"^0+(\d)", r"\1", p) for p in parts)


def extract_sections(
    feat: dict, west: float, east: float, north: float, south: float,
) -> list[tuple]:
    """Extract sections from a feature, handling Polygon and MultiPolygon."""
    props = feat.get("properties", {})
    smp = props.get("smp", "")
    if not smp:
        return []

    geom = feat.get("geometry", {})
    gtype = geom.get("type", "")
    coords = geom.get("coordinates", [])

    # Collect all polygon rings
    if gtype == "Polygon":
        polygons = [coords]
    elif gtype == "MultiPolygon":
        polygons = coords
    else:
        return []

    # Sum areas across sub-polygons
    total_area = 0.0
    for poly_coords in polygons:
        if poly_coords:
            total_area += ring_area_m2(poly_coords[0], west, east, north, south)

    if total_area <= 0:
        return []

    h_ini = props.get("altura_inicial", 0) or 0
    h_fin = props.get("altura_fin", 0) or props.get("altura_final", 0) or 0
    tipo = props.get("tipo", "")
    edificabil = props.get("edificabil", "")

    return [(normalize_smp(smp), tipo, h_ini, h_fin, round(total_area, 1), edificabil)]


def main() -> None:
    tiles = tile_range()
    print(f"Downloading {len(tiles)} tiles at zoom {ZOOM}")

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tile_sections (
            smp_norm TEXT NOT NULL,
            tipo TEXT NOT NULL,
            h_ini REAL,
            h_fin REAL,
            area_m2 REAL,
            edificabil TEXT,
            PRIMARY KEY (smp_norm, tipo, h_ini)
        )
    """)
    conn.execute("DELETE FROM tile_sections")
    conn.commit()

    session = requests.Session()
    session.headers["User-Agent"] = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
    )
    total_sections = 0
    total_parcels: set[str] = set()
    multi_count = 0
    errors = 0

    for i, (x, y) in enumerate(tiles):
        try:
            url = (
                f"https://vectortiles.usig.buenosaires.gob.ar"
                f"/cur3d/volumen_edif/{ZOOM}/{x}/{y}.pbf"
            )
            resp = session.get(url, timeout=TIMEOUT)
            if resp.status_code != 200 or len(resp.content) < 10:
                continue

            tile = mvt.decode(resp.content)
            west, east, north, south = tile_bounds(x, y)

            batch = []
            for layer in tile.values():
                for feat in layer.get("features", []):
                    gtype = feat.get("geometry", {}).get("type", "")
                    if gtype == "MultiPolygon":
                        multi_count += 1

                    rows = extract_sections(feat, west, east, north, south)
                    batch.extend(rows)

            if batch:
                conn.executemany(
                    """INSERT OR REPLACE INTO tile_sections
                       (smp_norm, tipo, h_ini, h_fin, area_m2, edificabil)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    batch,
                )
                total_sections += len(batch)
                total_parcels.update(r[0] for r in batch)

            if (i + 1) % 20 == 0:
                conn.commit()
                print(
                    f"  [{i+1}/{len(tiles)}] "
                    f"{total_sections:,} sections, "
                    f"{len(total_parcels):,} parcels, "
                    f"{multi_count} MultiPolygon, "
                    f"{errors} errors",
                    flush=True,
                )

        except Exception as e:
            errors += 1
            if errors > 50:
                print(f"Too many errors ({errors}): {e}")
                break
            time.sleep(1)

    conn.commit()

    # Create summary table
    conn.execute("DROP TABLE IF EXISTS tile_pisada")
    conn.execute("""
        CREATE TABLE tile_pisada AS
        SELECT
            smp_norm,
            SUM(CASE WHEN tipo LIKE '%cuerpo%' THEN area_m2 ELSE 0 END)
                AS pisada_cuerpo,
            SUM(CASE WHEN tipo LIKE '%retiro 1%' THEN area_m2 ELSE 0 END)
                AS area_retiro1,
            SUM(CASE WHEN tipo LIKE '%retiro 2%' THEN area_m2 ELSE 0 END)
                AS area_retiro2,
            MAX(h_fin) AS h_max,
            COUNT(*) AS n_sections,
            MAX(edificabil) AS edificabil
        FROM tile_sections
        GROUP BY smp_norm
    """)
    conn.execute("CREATE INDEX idx_tile_pisada_smp ON tile_pisada(smp_norm)")
    conn.commit()

    count = conn.execute("SELECT COUNT(*) FROM tile_pisada").fetchone()[0]
    print(
        f"\nDone. {total_sections:,} sections, "
        f"{count:,} parcels in tile_pisada, "
        f"{multi_count} MultiPolygon features processed, "
        f"{errors} errors"
    )
    conn.close()


if __name__ == "__main__":
    main()
