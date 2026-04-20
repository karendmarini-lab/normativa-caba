"""
Compare buildable area models against GCBA ground truth.

Model A: Competitor's simplified rectangle (width × buildableDepth × floors)
Model B: Our envelope.py (Sutherland-Hodgman polygon clipping)

Ground truth: edif_sup_max_edificable from GCBA CUR3D API
"""

import json
import math
import sqlite3
import sys

from envelope import compute_envelope, clip_polygon, _edge_length_m

DB = "caba_normativa.db"


# ── Model A: Competitor's rectangle method ──────────────────────────

def competitor_model(
    width: float,
    depth: float,
    zoning: str,
    height: float,
    lfi: float | None,
    pisos: int,
) -> float:
    """Competitor's simplified buildable area calculation."""
    if width <= 0 or depth <= 0 or height <= 0:
        return 0

    # Retiro trasero mínimo
    zoning_upper = (zoning or "").upper()
    if any(z in zoning_upper for z in ["USAB0", "USAB1", "USAB2"]):
        T = 4
    elif any(z in zoning_upper for z in ["USAM", "USAA"]):
        T = 6
    else:
        T = 8

    # Buildable depth
    if depth <= 16.01:
        P = depth
    elif lfi and lfi > 0:
        P = max(16, lfi)
        if depth - P < T:
            P = max(0, depth - T)
    else:
        P = max(0, depth - T)

    # Floors
    A = 1  # PB
    if height > 3:
        A += math.floor((height - 3) / 2.8)

    # Total
    W = width * P  # area per floor
    J = W * A

    return J


# ── Model B: Our envelope.py ────────────────────────────────────────

def polygon_area_m2(coords: list[list[float]]) -> float:
    """Compute area in m² from WGS84 polygon using shoelace + lat correction."""
    pts = coords if coords[0] != coords[-1] else coords[:-1]
    if len(pts) < 3:
        return 0

    avg_lat = sum(p[1] for p in pts) / len(pts)
    cos_lat = math.cos(math.radians(avg_lat))

    # Convert to meters relative to first point
    origin = pts[0]
    mx = [(p[0] - origin[0]) * 111000 * cos_lat for p in pts]
    my = [(p[1] - origin[1]) * 111000 for p in pts]

    # Shoelace
    n = len(pts)
    area = 0
    for i in range(n):
        j = (i + 1) % n
        area += mx[i] * my[j] - mx[j] * my[i]
    return abs(area) / 2


def envelope_model(
    polygon_geojson: str | None,
    altura_max: float,
    plano_limite: float,
    frente: float,
    fondo: float,
    pisos: int,
) -> float:
    """Our envelope.py model — compute buildable area from real polygon."""
    if not polygon_geojson:
        return 0

    try:
        geom = json.loads(polygon_geojson)
    except (json.JSONDecodeError, TypeError):
        return 0

    if isinstance(geom, dict):
        coords = geom.get("coordinates", [[]])[0]
    elif isinstance(geom, list):
        coords = geom[0] if geom and isinstance(geom[0], list) and isinstance(geom[0][0], list) else geom
    else:
        return 0
    if len(coords) < 4:
        return 0

    sections = compute_envelope(
        polygon=coords,
        altura_max=altura_max,
        plano_limite=plano_limite,
        frente_m=frente,
        fondo_m=fondo,
    )

    if not sections:
        return 0

    total = 0
    for section in sections:
        poly = section.get("polygon", [])
        base = section.get("base", 0)
        top = section.get("top", 0)
        height = top - base

        area = polygon_area_m2(poly)

        # floors in this section
        if base == 0:
            # PB section: first 3m is PB, rest is 2.8m floors
            floors = 1
            if height > 3:
                floors += math.floor((height - 3) / 2.8)
        else:
            floors = max(1, math.floor(height / 2.8))

        total += area * floors

    return total


# ── Compare ─────────────────────────────────────────────────────────

def naive_fot_model(area: float, pisos: int, fot: float) -> float:
    """Model C: Naive FOT — area × pisos × 0.65 (ocupación típica)."""
    if area <= 0 or pisos <= 0:
        return 0
    return area * pisos * 0.65


def planta_tipo_model(gcba_planta: float, pisos: int, area: float) -> float:
    """Model D: GCBA planta_tipo × pisos (when available)."""
    if not gcba_planta or gcba_planta <= 0 or pisos <= 0:
        return 0
    # If planta/area < 1, it's per-floor → multiply by pisos
    # If planta/area >= 1, it's already total
    if area > 0 and gcba_planta / area < 1:
        return gcba_planta * pisos
    else:
        return gcba_planta


def hybrid_model(
    polygon_geojson: str | None,
    altura_max: float,
    plano_limite: float,
    frente: float,
    fondo: float,
    pisos: int,
    zoning: str,
    lfi: float | None,
    area: float,
) -> float:
    """Model E: Best of both — envelope geometry + competitor's edge cases."""
    # Try envelope first (real geometry)
    env = envelope_model(polygon_geojson, altura_max, plano_limite, frente, fondo, pisos)

    if env > 0:
        # Apply competitor's corrections:
        # 1. FOT cap
        fot_cap = area * 5  # typical max FOT
        env = min(env, fot_cap)
        return env

    # Fallback: competitor rectangle
    return competitor_model(frente, fondo, zoning, altura_max, lfi, pisos)


def main() -> None:
    conn = sqlite3.connect(DB)

    rows = conn.execute("""
        SELECT smp_norm, area, frente, fondo, pisos, plano_san,
            edif_sup_max_edificable, edif_sup_edificable_planta,
            edif_altura_max_1, edif_plano_limite,
            cur_distrito, polygon_geojson,
            barrio, fot
        FROM parcelas
        WHERE edif_sup_max_edificable > 500
            AND polygon_geojson IS NOT NULL
            AND polygon_geojson != ''
            AND frente > 0 AND fondo > 0
            AND pisos >= 4
            AND area > 50
        ORDER BY RANDOM()
        LIMIT 1000
    """).fetchall()

    print(f"Evaluating {len(rows)} parcels with GCBA ground truth\n")

    results = []
    for row in rows:
        (smp, area, frente, fondo, pisos, plano_san,
         gcba_total, gcba_planta, alt_max_1, plano_lim,
         cur_dist, poly_json, barrio, fot) = row

        lfi_approx = None

        altura = alt_max_1 if alt_max_1 and alt_max_1 > 0 else plano_san
        plano = plano_lim if plano_lim and plano_lim > 0 else plano_san

        # All 5 models
        m_a = competitor_model(frente, fondo, cur_dist, altura, lfi_approx, pisos)
        m_b = envelope_model(poly_json, altura, plano, frente, fondo, pisos)
        m_c = naive_fot_model(area, pisos, fot or 0)
        m_d = planta_tipo_model(gcba_planta, pisos, area)
        m_e = hybrid_model(poly_json, altura, plano, frente, fondo, pisos,
                           cur_dist, lfi_approx, area)

        if gcba_total > 0:
            r = {
                "smp": smp, "barrio": barrio, "area": area,
                "pisos": pisos, "dist": cur_dist, "gcba": gcba_total,
            }
            models = {
                "competitor": m_a,
                "envelope": m_b,
                "naive_fot": m_c,
                "planta_tipo": m_d,
                "hybrid": m_e,
            }
            for name, val in models.items():
                if val > 0:
                    r[name] = val
                    r[f"err_{name}"] = (val - gcba_total) / gcba_total
            results.append(r)

    print(f"Successfully computed both models for {len(results)} parcels\n")

    if not results:
        print("No results to compare")
        return

    # Stats
    model_names = ["competitor", "envelope", "naive_fot", "planta_tipo", "hybrid"]

    def pct(vals, p):
        s = sorted(vals)
        idx = min(int(len(s) * p / 100), len(s) - 1)
        return s[idx]

    def stats_for(name):
        errs = [abs(r[f"err_{name}"]) for r in results if f"err_{name}" in r]
        bias = [r[f"err_{name}"] for r in results if f"err_{name}" in r]
        if not errs:
            return None
        return {
            "n": len(errs),
            "mae": sum(errs) / len(errs),
            "median": pct(errs, 50),
            "p90": pct(errs, 90),
            "bias": sum(bias) / len(bias),
            "within_20": sum(1 for e in errs if e < 0.2) / len(errs),
            "within_10": sum(1 for e in errs if e < 0.1) / len(errs),
        }

    header = f"{'Metric':<25s}"
    for name in model_names:
        header += f" {name:>12s}"
    print(header)
    print("-" * (25 + 13 * len(model_names)))

    all_stats = {name: stats_for(name) for name in model_names}

    for metric, label in [
        ("n", "N parcels"),
        ("mae", "Mean Abs Error"),
        ("median", "Median Abs Error"),
        ("p90", "90th pctile Error"),
        ("bias", "Mean Bias"),
        ("within_20", "Within ±20%"),
        ("within_10", "Within ±10%"),
    ]:
        row = f"{label:<25s}"
        for name in model_names:
            s = all_stats[name]
            if not s:
                row += f" {'N/A':>12s}"
            elif metric == "n":
                row += f" {s[metric]:>12d}"
            elif metric in ("bias",):
                row += f" {s[metric]:>+11.1%}"
            else:
                row += f" {s[metric]:>11.1%}"
        print(row)

    # By zoning type — best model per zone
    print(f"\nBy zoning type (MAE):")
    by_zone = {}
    for r in results:
        z = r["dist"] or "?"
        by_zone.setdefault(z, []).append(r)

    header = f"{'Zone':<20s} {'N':>4s}"
    for name in model_names:
        header += f" {name[:8]:>9s}"
    header += f" {'Winner':>10s}"
    print(header)
    print("-" * (35 + 10 * len(model_names) + 10))

    for zone in sorted(by_zone, key=lambda z: -len(by_zone[z])):
        rs = by_zone[zone]
        if len(rs) < 5:
            continue
        row = f"{zone:<20s} {len(rs):>4d}"
        best_mae = float("inf")
        best_name = ""
        for name in model_names:
            errs = [abs(r[f"err_{name}"]) for r in rs if f"err_{name}" in r]
            if errs:
                mae = sum(errs) / len(errs)
                row += f" {mae:>8.1%}"
                if mae < best_mae:
                    best_mae = mae
                    best_name = name
            else:
                row += f" {'N/A':>8s}"
        row += f" {best_name:>10s}"
        print(row)

    # Sample: show 10 parcels with all model predictions
    print(f"\nSample parcels (10):")
    print(f"{'SMP':>15s} {'Zone':>12s} {'GCBA':>7s} {'Comp':>7s} {'Env':>7s} "
          f"{'Naive':>7s} {'Planta':>7s} {'Hybrid':>7s}")
    print("-" * 80)
    for r in results[:10]:
        print(
            f"{r['smp']:>15s} {(r['dist'] or '?')[:12]:>12s} "
            f"{r['gcba']:>7,.0f} "
            f"{r.get('competitor', 0):>7,.0f} "
            f"{r.get('envelope', 0):>7,.0f} "
            f"{r.get('naive_fot', 0):>7,.0f} "
            f"{r.get('planta_tipo', 0):>7,.0f} "
            f"{r.get('hybrid', 0):>7,.0f}"
        )

    conn.close()


if __name__ == "__main__":
    main()
