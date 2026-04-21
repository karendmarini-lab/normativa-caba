"""Update sup_vendible for all parcels using best available model.

Priority: Model A (tiles) when available, Model B (normativa) otherwise.
Also stores m2_construibles and model source for transparency.
"""

import sqlite3
import time

from buildable import (
    ParcelData,
    compute_from_normativa,
    compute_from_tiles,
    load_tile_data,
)

DB_PATH = "caba_normativa.db"
TILE_DB = "tile_volumen_v3.db"


def main() -> None:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")

    # Add columns if missing
    cols = {r[1] for r in conn.execute("PRAGMA table_info(parcelas)").fetchall()}
    for col, typ in [
        ("m2_construibles", "REAL"),
        ("m2_vendibles_source", "TEXT"),
    ]:
        if col not in cols:
            conn.execute(f"ALTER TABLE parcelas ADD COLUMN {col} {typ}")
    conn.commit()

    # Load tile data
    print("Loading tiles...", flush=True)
    tiles = load_tile_data(TILE_DB)
    print(f"  {len(tiles):,} tiles loaded", flush=True)

    # Load all edificable parcels
    print("Loading parcels...", flush=True)
    rows = conn.execute("""
        SELECT smp_norm, frente, fondo, area, cur_distrito, plano_san
        FROM parcelas
        WHERE pisos >= 1
            AND cur_distrito IS NOT NULL AND cur_distrito != ''
            AND frente > 0 AND fondo > 0 AND area > 0
    """).fetchall()
    print(f"  {len(rows):,} parcels to update", flush=True)

    t0 = time.time()
    batch = []
    tile_count = 0
    norm_count = 0

    for i, (smp, frente, fondo, area, dist, plano) in enumerate(rows):
        parcel = ParcelData(
            smp_norm=smp, frente=frente, fondo=fondo, area=area,
            cur_distrito=dist, plano_san=plano or 0,
        )

        tile = tiles.get(smp)
        if tile:
            result = compute_from_tiles(parcel, tile)
            source = "tile"
            tile_count += 1
        else:
            result = compute_from_normativa(parcel)
            source = "normativa"
            norm_count += 1

        batch.append((
            round(result.m2_vendibles, 1),
            round(result.m2_construibles, 1),
            source,
            smp,
        ))

        if len(batch) >= 5000 or i == len(rows) - 1:
            conn.executemany(
                """UPDATE parcelas
                   SET sup_vendible = ?, m2_construibles = ?, m2_vendibles_source = ?
                   WHERE smp_norm = ?""",
                batch,
            )
            conn.commit()
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            print(
                f"  [{i+1:,}/{len(rows):,}] "
                f"tile={tile_count:,} norm={norm_count:,} "
                f"({rate:.0f}/s)",
                flush=True,
            )
            batch = []

    # Verify
    stats = conn.execute("""
        SELECT m2_vendibles_source, COUNT(*), ROUND(AVG(sup_vendible), 0)
        FROM parcelas
        WHERE pisos >= 1 AND cur_distrito IS NOT NULL AND cur_distrito != ''
        GROUP BY m2_vendibles_source
    """).fetchall()

    print(f"\nDone. {tile_count:,} tile + {norm_count:,} normativa = {tile_count+norm_count:,}")
    for source, n, avg in stats:
        print(f"  {source}: {n:,} parcels, avg {avg:,.0f} m²v")

    conn.close()


if __name__ == "__main__":
    main()
