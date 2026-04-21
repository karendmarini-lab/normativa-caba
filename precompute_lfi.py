"""
Precompute LFI (Línea de Frente Interno) per parcel from manzana geometry.

LFI = 1/4 of the distance between opposing L.O. midpoints of the manzana.
(Art. 6.4.2, CUR Ley 6099)

Algorithm:
1. Group parcels by seccion_mzna → union polygons → manzana outline
2. Compute minimum rotated rectangle of manzana
3. For each parcel: determine which "face" it's on (by centroid proximity to edges)
4. LFI = 1/4 × manzana depth perpendicular to that face

Stores result as `lfi` column in parcelas table.
"""

import json
import math
import sqlite3
import time

import numpy as np
from shapely.geometry import Polygon, MultiPolygon, box
from shapely.ops import unary_union

DB_PATH = "caba_normativa.db"
LFI_DB_PATH = "lfi_data.db"


def compute_manzana_lfi(parcels: list[dict]) -> dict[str, float]:
    """Compute LFI for each parcel in a manzana.

    Returns dict of smp_norm → lfi_meters.
    """
    # Build manzana polygon from union of parcel polygons
    polys = []
    for p in parcels:
        coords = p["coords"]
        if len(coords) >= 4:
            try:
                poly = Polygon(coords)
                if poly.is_valid and poly.area > 0:
                    polys.append(poly)
            except Exception:
                continue

    if len(polys) < 2:
        # Single parcel manzana — LFI = fondo (no constraint)
        return {p["smp"]: p["fondo"] for p in parcels}

    manzana = unary_union(polys)
    if manzana.is_empty:
        return {p["smp"]: p["fondo"] for p in parcels}

    # Minimum rotated rectangle of manzana
    mrr = manzana.minimum_rotated_rectangle
    if mrr.is_empty:
        return {p["smp"]: p["fondo"] for p in parcels}

    # Get the 4 edges of the minimum rotated rectangle
    mrr_coords = list(mrr.exterior.coords)[:4]
    edges = []
    for i in range(4):
        p1 = np.array(mrr_coords[i])
        p2 = np.array(mrr_coords[(i + 1) % 4])
        midpoint = (p1 + p2) / 2
        length = np.linalg.norm(p2 - p1)
        direction = (p2 - p1) / length if length > 0 else np.array([1, 0])
        edges.append({
            "p1": p1, "p2": p2, "mid": midpoint,
            "length": length, "dir": direction,
        })

    # Two pairs of opposing edges (short sides and long sides)
    # Manzana depth for each pair = distance between opposing edges
    cos_lat = math.cos(math.radians(-34.6))
    m_per_deg_lon = 111000 * cos_lat  # ~91,500 m/deg
    m_per_deg_lat = 111000  # ~111,000 m/deg

    def edge_dist_m(e1, e2):
        """Distance between midpoints of two edges in meters."""
        dx = (e2["mid"][0] - e1["mid"][0]) * m_per_deg_lon
        dy = (e2["mid"][1] - e1["mid"][1]) * m_per_deg_lat
        return math.sqrt(dx * dx + dy * dy)

    # Pair edges: 0-2 and 1-3 are opposing
    dist_02 = edge_dist_m(edges[0], edges[2])
    dist_13 = edge_dist_m(edges[1], edges[3])

    # For each parcel, find which pair of opposing edges it's between
    results = {}
    for p in parcels:
        coords = p["coords"]
        if len(coords) < 3:
            results[p["smp"]] = p["fondo"]
            continue

        # Parcel centroid
        try:
            poly = Polygon(coords)
            cx, cy = poly.centroid.x, poly.centroid.y
        except Exception:
            results[p["smp"]] = p["fondo"]
            continue

        # Distance from centroid to each edge pair's midpoints
        def dist_to_edge(edge):
            dx = (cx - edge["mid"][0]) * m_per_deg_lon
            dy = (cy - edge["mid"][1]) * m_per_deg_lat
            return math.sqrt(dx * dx + dy * dy)

        # Find closest edge — that's the "front" face
        d0 = dist_to_edge(edges[0])
        d1 = dist_to_edge(edges[1])
        d2 = dist_to_edge(edges[2])
        d3 = dist_to_edge(edges[3])

        # If closest to edge 0 or 2: manzana depth is dist_02
        # If closest to edge 1 or 3: manzana depth is dist_13
        min_d = min(d0, d1, d2, d3)
        if min_d == d0 or min_d == d2:
            manzana_depth = dist_02
        else:
            manzana_depth = dist_13

        # LFI = 1/4 of manzana depth, but at least 16m (banda mínima)
        lfi = max(16.0, manzana_depth / 4.0)

        # LFI can't exceed parcel fondo
        lfi = min(lfi, p["fondo"])

        results[p["smp"]] = lfi

    return results


def main() -> None:
    conn = sqlite3.connect(DB_PATH, timeout=30)

    # Output DB for LFI values (avoids locking issues with server)
    out = sqlite3.connect(LFI_DB_PATH)
    out.execute("PRAGMA journal_mode=WAL")
    out.execute("""
        CREATE TABLE IF NOT EXISTS parcel_lfi (
            smp_norm TEXT PRIMARY KEY,
            lfi REAL NOT NULL
        )
    """)
    out.execute("DELETE FROM parcel_lfi")
    out.commit()

    # Load all parcels with polygon data, grouped by manzana
    print("Loading parcels...", flush=True)
    rows = conn.execute("""
        SELECT smp_norm, seccion_mzna, frente, fondo, polygon_geojson
        FROM parcelas
        WHERE pisos >= 1
            AND frente > 0 AND fondo > 0
            AND polygon_geojson IS NOT NULL AND polygon_geojson != ''
            AND seccion_mzna IS NOT NULL
    """).fetchall()

    manzanas: dict[str, list[dict]] = {}
    for smp, mzna, frente, fondo, geojson in rows:
        try:
            coords = json.loads(geojson)
        except (json.JSONDecodeError, TypeError):
            continue
        manzanas.setdefault(mzna, []).append({
            "smp": smp, "frente": frente, "fondo": fondo, "coords": coords,
        })

    print(f"  {len(rows):,} parcels in {len(manzanas):,} manzanas", flush=True)

    # Process each manzana
    t0 = time.time()
    total_updated = 0
    errors = 0
    batch = []

    for i, (mzna, parcels) in enumerate(manzanas.items()):
        try:
            lfi_map = compute_manzana_lfi(parcels)
            for smp, lfi in lfi_map.items():
                batch.append((lfi, smp))
        except Exception:
            errors += 1
            # Fallback: lfi = fondo for this manzana
            for p in parcels:
                batch.append((p["fondo"], p["smp"]))

        # Batch insert every 500 manzanas
        if len(batch) >= 5000 or i == len(manzanas) - 1:
            out.executemany(
                "INSERT OR REPLACE INTO parcel_lfi (lfi, smp_norm) VALUES (?, ?)",
                batch,
            )
            out.commit()
            total_updated += len(batch)
            batch = []

            elapsed = time.time() - t0
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta = (len(manzanas) - i - 1) / rate / 60 if rate > 0 else 0
            print(
                f"  [{i+1:,}/{len(manzanas):,}] "
                f"{total_updated:,} parcels, "
                f"{rate:.0f} mzna/s, "
                f"ETA {eta:.1f}min, "
                f"{errors} errors",
                flush=True,
            )

    print(f"\nDone. {total_updated:,} parcels updated, {errors} errors")

    # Validation: compare LFI vs tile effective depth
    print("\nValidation: LFI vs tile effective depth (sample)...")
    sample_smps = "('66-47-2','66-47-12','48-117-14','35-115-34','71-35-21')"
    sample = conn.execute(f"""
        SELECT p.smp_norm, p.frente, p.fondo, p.cur_distrito
        FROM parcelas p
        WHERE p.frente > 0 AND p.fondo > 20
            AND p.smp_norm IN {sample_smps}
    """).fetchall()
    for smp, frente, fondo, dist in sample:
        lfi_row = out.execute(
            "SELECT lfi FROM parcel_lfi WHERE smp_norm = ?", (smp,)
        ).fetchone()
        lfi = lfi_row[0] if lfi_row else 0
        print(f"  {smp:14s} frente={frente:.1f} fondo={fondo:.1f} LFI={lfi:.1f} dist={dist}")

    conn.close()
    out.close()


if __name__ == "__main__":
    main()
