"""
Precompute all CABA parcels from cur_optimizado.json into a SQLite database.

Replicates the sanitization and derivation logic from index.html:
- sanitizarDatosCUR(): fixes known CUR shapefile bugs
- CPU_TO_CUR mapping: translates legacy CPU codes to current CUR district names
- Plano límite sanitization
- Floor estimation (pisos) from sanitized plano
- Buildable volume estimation (pisada × pisos)
"""

import json
import re
import sqlite3
from pathlib import Path

# ── CPU → CUR mapping (from index.html:793) ─────────────────────

CPU_TO_CUR: dict[str, str] = {
    "R2a I": "U.S.A.A.",
    "R2a II": "U.S.A.A.",
    "C3 I": "Corredor Medio",
    "C3 II": "Corredor Medio",
    "C2": "Corredor Alto",
    "C1": "Corredor Alto",
    "R2b I": "U.S.A.B. 2",
    "R2b I 1": "U.S.A.B. 2",
    "R2b II": "U.S.A.B. 1",
    "R2b III": "U.S.A.B. 1",
    "R1b I": "U.S.A.B. 2",
    "R1b II": "U.S.A.B. 2",
    "E1": "E1",
    "E2": "E2",
    "E3": "E3",
}


def smp_norm(smp: str) -> str:
    """Normalize SMP removing leading zeros: '011-049-026' → '11-49-26'."""
    if not smp:
        return ""
    parts = smp.upper().split("-")
    normalized = []
    for p in parts:
        digits = re.sub(r"[^0-9]", "", p)
        letters = re.sub(r"[^A-Z]", "", p)
        if digits:
            normalized.append(str(int(digits)) + letters)
        else:
            normalized.append(p)
    return "-".join(normalized)


def sanitizar_datos_cur(h: float, plano: float) -> float:
    """Replicate sanitizarDatosCUR() from index.html:606."""
    # REGLA 1: Distritos bajos (U.S.A.B.) — plano = h
    if h > 0 and h <= 14.6:
        plano = h

    # REGLA 2: Corredor Alto — corrige bug 39.2m → 38.2m del Shapefile
    if plano and 38 < plano < 40:
        plano = 38.2

    # REGLA 3: Corredor Medio — asegura 31.2m
    if plano and 31 < plano < 32:
        plano = 31.2

    # REGLA 4: U.S.A.A. — asegura 29.8m de plano límite
    if plano and 29 < plano < 30:
        plano = 29.8

    # REGLA 5: plano=0 en CUR → usar h como fallback
    if not plano or plano <= 0:
        plano = h or 0

    return plano


def calcular_plano_sanitizado(h: float, plano: float) -> float:
    """Second-pass sanitization from mostrar() in index.html:824."""
    if h and (h <= 14.6 or not plano or plano < h):
        return h
    return plano


def calcular_pisos(plano_san: float) -> int:
    """Floor estimation from index.html:831. PB=3.30m, typical=2.90m."""
    if plano_san <= 0:
        return 0
    return max(1, 1 + int((plano_san - 3.30) / 2.90))


def calcular_edificabilidad(
    area: float,
    fr: float,
    fo: float,
    pisada_pct: float,
    pisos: int,
) -> tuple[float, float, float]:
    """Replicate buildable volume calc from index.html:867-898.

    Returns:
        (pisada, volumen_edificable, superficie_vendible)
    """
    if area <= 0 or pisos <= 0:
        return (0.0, 0.0, 0.0)

    # Pisada calculation (from index.html:877-888)
    if fr > 0 and fo > 0:
        if fo <= 16:
            pisada = min(round(fr * fo), round(area))
        else:
            pisada = min(round(fr * 22), round(area))  # LFI = 22m
    else:
        pisada = round(area * pisada_pct)

    # Volume calculation (from recalcularRendimiento, index.html:697-706)
    pisos_norm = max(1, pisos - 2)
    volumen = (pisada * pisos_norm) + (pisada * 0.8 * min(2, pisos))
    vendible = volumen * 0.85

    return (float(pisada), round(volumen, 1), round(vendible, 1))


def extract_seccion_manzana(smp: str) -> str:
    """Extract sección-manzana from SMP for block-level grouping."""
    parts = smp.split("-")
    if len(parts) >= 2:
        return f"{parts[0]}-{parts[1]}"
    return ""


def main() -> None:
    base_dir = Path(__file__).parent
    json_path = base_dir / "cur_optimizado.json"
    db_path = base_dir / "caba_normativa.db"

    print(f"Loading {json_path}...")
    with open(json_path) as f:
        raw = json.load(f)

    points = raw["points"]
    parcels = raw["data"]
    total = len(parcels)
    print(f"  {total:,} parcels loaded")

    # Remove old DB if exists
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE parcelas (
            id            INTEGER PRIMARY KEY,
            smp           TEXT NOT NULL,
            smp_norm      TEXT NOT NULL,
            seccion_mzna  TEXT NOT NULL,
            lat           REAL NOT NULL,
            lng           REAL NOT NULL,
            cpu           TEXT,
            cur_distrito  TEXT,
            h             REAL,
            fot           REAL,
            plano_raw     REAL,
            plano_san     REAL,
            pisos         INTEGER,
            area          REAL,
            frente        REAL,
            fondo         REAL,
            pisada_pct    REAL,
            pisada        REAL,
            vol_edificable REAL,
            sup_vendible  REAL,
            es_aph        INTEGER DEFAULT 0
        )
    """)

    print("Processing parcels...")
    rows = []
    for i, (parcel, point) in enumerate(zip(parcels, points)):
        smp = parcel.get("smp", "")
        cpu = parcel.get("cpu", "")
        h = parcel.get("h", 0) or 0
        fot = parcel.get("fot") or None
        plano_raw = parcel.get("plano", 0) or 0
        area = parcel.get("area", 0) or 0
        fr = parcel.get("fr", 0) or 0
        fo = parcel.get("fo", 0) or 0
        pisada_pct = parcel.get("pisada_pct", 0.65) or 0.65

        lat, lng = point[0], point[1]

        # Sanitize
        plano_after_sanitizer = sanitizar_datos_cur(h, plano_raw)
        plano_san = calcular_plano_sanitizado(h, plano_after_sanitizer)

        # Derive
        pisos = calcular_pisos(plano_san)
        cur_distrito = CPU_TO_CUR.get(cpu, "")
        es_aph = 1 if cpu and str(cpu).startswith("APH") else 0
        smp_n = smp_norm(smp)
        sec_mzna = extract_seccion_manzana(smp_n)

        pisada, vol_edif, sup_vend = calcular_edificabilidad(
            area, fr, fo, pisada_pct, pisos
        )

        rows.append((
            smp, smp_n, sec_mzna, lat, lng,
            cpu or None, cur_distrito or None,
            h, fot, plano_raw, plano_san, pisos,
            area, fr, fo, pisada_pct,
            pisada, vol_edif, sup_vend, es_aph,
        ))

        if (i + 1) % 50000 == 0:
            print(f"  {i + 1:,} / {total:,}")

    print("Inserting into SQLite...")
    cur.executemany("""
        INSERT INTO parcelas (
            smp, smp_norm, seccion_mzna, lat, lng,
            cpu, cur_distrito,
            h, fot, plano_raw, plano_san, pisos,
            area, frente, fondo, pisada_pct,
            pisada, vol_edificable, sup_vendible, es_aph
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, rows)

    # Indexes for common queries
    print("Creating indexes...")
    cur.execute("CREATE INDEX idx_smp_norm ON parcelas(smp_norm)")
    cur.execute("CREATE INDEX idx_cpu ON parcelas(cpu)")
    cur.execute("CREATE INDEX idx_cur_distrito ON parcelas(cur_distrito)")
    cur.execute("CREATE INDEX idx_seccion_mzna ON parcelas(seccion_mzna)")
    cur.execute("CREATE INDEX idx_pisos ON parcelas(pisos)")
    cur.execute("CREATE INDEX idx_h ON parcelas(h)")
    cur.execute("CREATE INDEX idx_area ON parcelas(area)")
    cur.execute("CREATE INDEX idx_es_aph ON parcelas(es_aph)")

    conn.commit()

    # Summary stats
    cur.execute("SELECT COUNT(*) FROM parcelas")
    count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT cpu) FROM parcelas WHERE cpu IS NOT NULL")
    n_cpus = cur.fetchone()[0]

    cur.execute("SELECT COUNT(DISTINCT seccion_mzna) FROM parcelas")
    n_manzanas = cur.fetchone()[0]

    cur.execute("""
        SELECT cpu, COUNT(*) as n, ROUND(AVG(h),1), ROUND(AVG(plano_san),1),
               ROUND(AVG(pisos),1), ROUND(AVG(area),0)
        FROM parcelas WHERE cpu IS NOT NULL
        GROUP BY cpu ORDER BY n DESC LIMIT 10
    """)
    top_cpus = cur.fetchall()

    db_size_mb = db_path.stat().st_size / (1024 * 1024)

    conn.close()

    print(f"\n{'='*60}")
    print(f"  caba_normativa.db created: {db_size_mb:.1f} MB")
    print(f"  {count:,} parcels | {n_cpus} districts | {n_manzanas:,} blocks")
    print(f"{'='*60}")
    print("\nTop 10 districts:")
    print(f"  {'CPU':<12} {'Count':>7} {'H avg':>6} {'PL avg':>7} {'Pisos':>6} {'Area':>6}")
    for row in top_cpus:
        print(f"  {row[0]:<12} {row[1]:>7,} {row[2]:>6} {row[3]:>7} {row[4]:>6} {row[5]:>6}")


if __name__ == "__main__":
    main()
