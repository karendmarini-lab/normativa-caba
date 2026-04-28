#!/usr/bin/env python3
"""
Recompute sup_vendible for all parcels using:
1. Ciudad 3D pisada (edif_sup_edificable_planta) when available (86%)
2. LFI fallback (frente × 22m) otherwise
3. 2-feature efficiency model (density + frente, 150 studies)

Updates parcelas.sup_vendible and parcelas.vol_edificable in-place.
"""

import sqlite3
import math
import time
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "caba_normativa.db"


def compute_pisos(plano_san: float) -> int:
    if plano_san <= 0:
        return 1
    return max(1, 1 + math.floor((plano_san - 3.30) / 2.90))


def compute_pisada(edif_planta: float, frente: float, fondo: float, area: float) -> float:
    if edif_planta and edif_planta > 0:
        return edif_planta
    if frente and frente > 0 and fondo and fondo > 0:
        if fondo <= 16:
            return min(frente * fondo, area)
        return min(frente * 22, area)
    return area * 0.65


def compute_volumen(pisada: float, pisos: int, plano_san: float, frente: float) -> float:
    if plano_san <= 14.6 or pisos <= 2:
        return pisada * pisos
    prof = pisada / frente if frente > 0 else 20
    retiro1 = max(0, frente * (prof - 4)) if frente > 0 else pisada * 0.8
    retiro2 = max(0, frente * (prof - 8)) if frente > 0 else pisada * 0.6
    return pisada * (pisos - 2) + retiro1 + retiro2


def compute_efficiency(volumen: float, area: float, frente: float) -> float:
    density = volumen / area if area > 0 else 5
    fr = frente if frente and frente > 0 else 8.7
    ef = 0.78 - 0.02 * max(0, density - 5) + 0.002 * max(0, fr - 8)
    return max(0.55, min(0.95, ef))


def main():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")

    rows = conn.execute("""
        SELECT id, area, frente, fondo, plano_san,
               edif_sup_edificable_planta, pisos
        FROM parcelas
        WHERE area > 0
    """).fetchall()

    print(f"Processing {len(rows)} parcels...")
    start = time.time()

    updates = []
    for r in rows:
        area = r["area"] or 0
        frente = r["frente"] or 0
        fondo = r["fondo"] or 0
        plano = r["plano_san"] or 0
        edif_planta = r["edif_sup_edificable_planta"] or 0

        pisos = compute_pisos(plano)
        pisada = compute_pisada(edif_planta, frente, fondo, area)
        vol = compute_volumen(pisada, pisos, plano, frente)
        ef = compute_efficiency(vol, area, frente)
        vendible = vol * ef

        # Balcones
        balc_width = max(0, frente - 1.20) if frente > 0 else 0
        balcones = balc_width * 1.5 * max(0, pisos - 1) if pisos > 1 else 0
        total_vendible = vendible + balcones

        source = "ciudad3d" if edif_planta > 0 else "normativa"

        updates.append((
            pisos, round(pisada, 1), round(vol, 1), round(total_vendible, 1),
            source, r["id"]
        ))

    # Batch update
    conn.executemany(
        "UPDATE parcelas SET pisos=?, pisada=?, vol_edificable=?, sup_vendible=?, "
        "m2_vendibles_source=? WHERE id=?",
        updates,
    )
    conn.commit()
    conn.close()

    elapsed = time.time() - start
    ciudad3d = sum(1 for u in updates if u[4] == "ciudad3d")
    print(f"Done: {len(updates)} parcels updated in {elapsed:.1f}s")
    print(f"  Ciudad 3D pisada: {ciudad3d} ({100*ciudad3d/len(updates):.1f}%)")
    print(f"  LFI fallback: {len(updates) - ciudad3d}")


if __name__ == "__main__":
    main()
