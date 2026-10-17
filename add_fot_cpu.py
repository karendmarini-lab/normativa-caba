"""Agrega y completa la columna `fot_cpu` (FOT del CPU anterior) en la base.

`update_vendibles.py` pisa la columna `fot` con el FOT que calcula el modelo
(m² construibles / superficie), por eso la plusvalía necesita el FOT original,
que sigue estando en cur_optimizado.json.

Es seguro y repetible: solo toca `fot_cpu`, no modifica ninguna otra columna.

    python3 add_fot_cpu.py
"""

import json
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "caba_normativa.db"
JSON_PATH = BASE_DIR / "cur_optimizado.json"


def main() -> None:
    data = json.load(open(JSON_PATH))["data"]
    rows = [(p.get("fot") or None, p["smp"]) for p in data if p.get("smp")]

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(parcelas)").fetchall()}
    if "fot_cpu" not in cols:
        conn.execute("ALTER TABLE parcelas ADD COLUMN fot_cpu REAL")

    conn.executemany("UPDATE parcelas SET fot_cpu = ? WHERE smp = ?", rows)
    conn.commit()

    total, con_fot = conn.execute(
        "SELECT COUNT(*), SUM(fot_cpu IS NOT NULL) FROM parcelas"
    ).fetchone()
    print(f"fot_cpu cargado: {con_fot:,} de {total:,} parcelas")
    conn.close()


if __name__ == "__main__":
    main()
