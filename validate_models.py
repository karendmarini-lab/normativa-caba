"""
Cross-validate Model A (tiles) vs Model B (normativa) and test against
25 professional RE/MAX prefactibilidad studies.

Outputs:
- Per-district divergence stats (A vs B)
- Accuracy against RE/MAX ground truth (both models)
- Coverage report
"""

import json
import sqlite3
from pathlib import Path

from buildable import (
    Construibles,
    ParcelData,
    TileData,
    compute_from_normativa,
    compute_from_tiles,
    load_lfi_data,
    load_tile_data,
    RATIO_VENDIBLE,
)

NORMATIVA_DB = "caba_normativa.db"
TILE_DB = "tile_volumen_v3.db"
LFI_DB = "lfi_data.db"

# 25 RE/MAX professional studies (m²v real from prefactibilidad images)
REMAX_STUDIES = [
    ("47-125-28A", 675),
    ("41-89-30A", 1430),
    ("48-117-14", 4634),
    ("35-115-34", 2682),
    ("10-8-11E", 9280),
    ("36-24-46", 3621),
    ("55-216-13", 599),
    ("18-110-11", 1537),
    ("17-1-12B", 3166),
    ("89-12-4", 430),
    ("71-35-21", 1400),
    ("12-44-21", 3326),
    ("22-33-32", 5013),
    ("30-96-38", 1465),
    ("66-47-2", 1785),
    ("17-45-6", 1676),
    ("22-27-27", 1301),
    ("71-53-24", 1000),
    ("47-101-26", 874),
    ("43-101B-3", 592),
    ("83-207-2A", 982),
    ("5-23-1A", 1042),
    ("30-7-13", 1055),
    ("5-5-33", 1645),
    ("85-24-18", 527),
]


def load_parcels(db_path: str) -> dict[str, ParcelData]:
    """Load all edificable parcels."""
    conn = sqlite3.connect(db_path)
    rows = conn.execute("""
        SELECT smp_norm, frente, fondo, area, cur_distrito, plano_san
        FROM parcelas
        WHERE pisos >= 1
            AND cur_distrito IS NOT NULL AND cur_distrito != ''
            AND frente > 0 AND fondo > 0 AND area > 0
    """).fetchall()
    conn.close()
    return {
        r[0]: ParcelData(
            smp_norm=r[0], frente=r[1], fondo=r[2], area=r[3],
            cur_distrito=r[4], plano_san=r[5] or 0,
        )
        for r in rows
    }


def main() -> None:
    print("Loading data...", flush=True)
    parcels = load_parcels(NORMATIVA_DB)
    tiles = load_tile_data(TILE_DB)
    lfi_map = load_lfi_data(LFI_DB)
    print(f"  {len(parcels):,} parcels, {len(tiles):,} tiles, {len(lfi_map):,} LFI values",
          flush=True)

    # ─── Cross-validation: A vs B on overlap ──────────────────────────────
    print("\n" + "=" * 70)
    print("CROSS-VALIDATION: Model A (tiles) vs Model B (normativa)")
    print("=" * 70)

    district_stats: dict[str, list[float]] = {}
    total_compared = 0
    within_20 = 0
    within_30 = 0

    for smp, tile in tiles.items():
        parcel = parcels.get(smp)
        if not parcel:
            continue

        a = compute_from_tiles(parcel, tile)
        b = compute_from_normativa(parcel, lfi_map.get(smp))

        if a.m2_construibles < 50 or b.m2_construibles < 50:
            continue

        total_compared += 1
        error = (b.m2_construibles - a.m2_construibles) / a.m2_construibles

        dist = parcel.cur_distrito
        district_stats.setdefault(dist, []).append(error)

        if abs(error) <= 0.20:
            within_20 += 1
        if abs(error) <= 0.30:
            within_30 += 1

    print(f"\nCompared: {total_compared:,} parcels")
    print(f"Within ±20%: {within_20:,} ({100*within_20/total_compared:.1f}%)")
    print(f"Within ±30%: {within_30:,} ({100*within_30/total_compared:.1f}%)")

    print(f"\n{'District':20s} {'N':>7s} {'Med%':>7s} {'±20%':>6s} {'±30%':>6s}")
    print("-" * 50)
    for dist in sorted(district_stats, key=lambda d: -len(district_stats[d])):
        errs = sorted(district_stats[dist])
        n = len(errs)
        if n < 100:
            continue
        median = errs[n // 2] * 100
        w20 = sum(1 for e in errs if abs(e) <= 0.20) / n * 100
        w30 = sum(1 for e in errs if abs(e) <= 0.30) / n * 100
        print(f"{dist:20s} {n:>7,} {median:>+6.1f}% {w20:>5.0f}% {w30:>5.0f}%")

    # ─── Validation against RE/MAX studies ────────────────────────────────
    print("\n" + "=" * 70)
    print("VALIDATION: 25 RE/MAX professional studies")
    print("=" * 70)

    print(f"\n{'SMP':14s} {'Real':>6s} {'TileV':>6s} {'Terr%':>6s} "
          f"{'NormV':>6s} {'Nerr%':>6s} {'Dist':>15s}")
    print("-" * 75)

    tile_hits = 0
    tile_total = 0
    norm_hits = 0
    norm_total = 0

    for smp, m2v_real in REMAX_STUDIES:
        parcel = parcels.get(smp)
        if not parcel:
            print(f"{smp:14s} {'NO PARCEL':>6s}")
            continue

        # Model B (normativa) — always available
        b = compute_from_normativa(parcel, lfi_map.get(smp))
        norm_total += 1
        norm_err = (b.m2_vendibles - m2v_real) / m2v_real * 100
        if abs(norm_err) <= 20:
            norm_hits += 1

        # Model A (tiles) — only if available
        tile = tiles.get(smp)
        tile_str = ""
        tile_err_str = ""
        if tile:
            a = compute_from_tiles(parcel, tile)
            tile_total += 1
            tile_err = (a.m2_vendibles - m2v_real) / m2v_real * 100
            tile_str = f"{a.m2_vendibles:>6,.0f}"
            tile_err_str = f"{tile_err:>+5.0f}%"
            if abs(tile_err) <= 20:
                tile_hits += 1
        else:
            tile_str = "  —"
            tile_err_str = "  —"

        print(f"{smp:14s} {m2v_real:>6,} {tile_str} {tile_err_str} "
              f"{b.m2_vendibles:>6,.0f} {norm_err:>+5.0f}% "
              f"{parcel.cur_distrito:>15s}")

    print(f"\nModel A (tiles): {tile_hits}/{tile_total} within ±20% "
          f"({100*tile_hits/max(1,tile_total):.0f}%)")
    print(f"Model B (norm):  {norm_hits}/{norm_total} within ±20% "
          f"({100*norm_hits/max(1,norm_total):.0f}%)")

    # ─── Coverage report ──────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("COVERAGE")
    print("=" * 70)
    has_tile = sum(1 for smp in parcels if smp in tiles)
    no_tile = len(parcels) - has_tile
    print(f"Total edificable parcels: {len(parcels):,}")
    print(f"With tile (Model A):      {has_tile:,} ({100*has_tile/len(parcels):.1f}%)")
    print(f"Normativa only (Model B): {no_tile:,} ({100*no_tile/len(parcels):.1f}%)")
    print(f"Total coverage:           100%")


if __name__ == "__main__":
    main()
