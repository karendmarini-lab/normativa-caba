"""
Compute m² construibles for any parcel in CABA.

Two-model approach for 100% coverage:
  Model A: GCBA tiles (volumetría 3D con LFI real) — 77% coverage, ~0% error
  Model B: Normativa CUR (reglas Ley 6099 + Ley 6776) — 100% coverage

Uses A when tile data exists, B otherwise. Reports source.
Calibrated against 25 professional RE/MAX prefactibilidad studies.
"""

import math
import sqlite3
from dataclasses import dataclass
from typing import Optional

# Patio mínimo por distrito (Art. 6.4.4.4.1, Ley 6776)
PATIO_MIN_AREA = {
    "U.S.A.B. 0": 20, "U.S.A.B. 1": 20, "U.S.A.B. 2": 20,
    "U.S.A.M.": 26, "U.S.A.A.": 26,
    "Corredor Medio": 26, "Corredor Alto": 26,
    "E1": 20, "E2": 20, "E3": 26,
}

# Fixed circulation per floor (m²)
CIRC_BASE = 16  # 1 stair + elevator + palier + walls
CIRC_EXTRA_STAIR = 6  # 2nd stair required if H > 12m


def _compute_ratio(construibles: float, area: float, **_kwargs) -> float:
    """Compute vendible/construible ratio from building density.

    density = construibles / area (≈ effective floor count).
    Higher density → more losses to CUR patios, circulation, parking, lobbies.

    Calibrated from 25 RE/MAX professional studies:
      density ≤ 5: 0.88 (low-rise, minimal common areas)
      density 5-9: linear interpolation
      density ≥ 9: 0.65 (high-rise, large common area overhead)
    Validated: Estomba 3569 (Bercovich) density=3.47 → 0.88, real=0.845.
    """
    # Calibrated from 25 RE/MAX professional studies.
    # 551 text-extracted values confirmed trend but too noisy for fine-tuning.
    density = construibles / area if area > 0 else 5.0
    if density <= 5.0:
        return 0.88
    if density >= 9.0:
        return 0.65
    return 0.88 - (density - 5.0) * (0.88 - 0.65) / (9.0 - 5.0)

# District height limits (Art. 6.2, Ley 6776 dic 2024)
ALTURA_MAX: dict[str, float] = {
    "Corredor Alto": 38.0,
    "Corredor Medio": 31.20,
    "U.S.A.A.": 22.80,
    "U.S.A.M.": 17.20,
    "U.S.A.B. 2": 14.60,
    "U.S.A.B. 1": 12.0,
    "U.S.A.B. 0": 9.0,
    "E3": 29.8,
    "E2": 12.0,
    "E1": 9.0,
}

# Retiro fondo mínimo por distrito (Art. 6.4.2.4)
RETIRO_FONDO: dict[str, int] = {
    "Corredor Alto": 8,
    "Corredor Medio": 8,
    "U.S.A.A.": 6,
    "U.S.A.M.": 6,
    "U.S.A.B. 2": 6,
    "U.S.A.B. 1": 4,
    "U.S.A.B. 0": 4,
    "E3": 6,
    "E2": 4,
    "E1": 4,
}

H_PB_USAB = 2.60
H_PB_ALTA = 3.00
H_PISO = 3.00


@dataclass(frozen=True)
class ParcelData:
    """All data needed for construibles calculation."""

    smp_norm: str
    frente: float
    fondo: float
    area: float
    cur_distrito: str
    plano_san: float  # sanitized height limit from precompute


@dataclass(frozen=True)
class TileData:
    """Tile-derived buildable data for a parcel."""

    total_construibles: float  # sum of (area × floors) per section
    pisada_cuerpo: float
    h_max: float


@dataclass(frozen=True)
class Construibles:
    """Result of m² construibles calculation."""

    m2_construibles: float
    m2_vendibles: float
    source: str  # "tile" or "normativa"
    pisos: int
    pisada: float


# ─── Model A: Tiles ──────────────────────────────────────────────────────────


def compute_from_tiles(parcel: ParcelData, tile: TileData) -> Construibles:
    """Compute construibles from GCBA volumetric tiles.

    Total = sum of (section_area × floors_in_section) across all sections.
    Already computed and stored in tile_construibles table.
    """
    total = tile.total_construibles
    h = tile.h_max if tile.h_max > 0 else parcel.plano_san or 14.6
    pisos = _compute_pisos(h, parcel.cur_distrito)

    ratio = _compute_ratio(total, parcel.area)
    return Construibles(
        m2_construibles=max(0, total),
        m2_vendibles=max(0, total) * ratio,
        source="tile",
        pisos=pisos,
        pisada=tile.pisada_cuerpo,
    )


# ─── Model B: Normativa pura ─────────────────────────────────────────────────


def compute_from_normativa(
    parcel: ParcelData, lfi: float | None = None,
) -> Construibles:
    """Compute construibles from calibrated multiplier table.

    construibles = frente × fondo × pisos × multiplier(distrito, fondo)

    Multiplier table calibrated from 193k tile parcels (median per bucket).
    Captures LFI, retiros, and envelope effects in a single empirical value.
    """
    dist = parcel.cur_distrito or ""
    altura = parcel.plano_san if parcel.plano_san > 3 else _district_altura(dist)
    pisos = _compute_pisos(altura, dist)

    # Direct multiplier from calibration table
    mult = _get_multiplier(dist, parcel.fondo)
    total = parcel.frente * parcel.fondo * pisos * mult
    pisada = parcel.frente * parcel.fondo * mult

    ratio = _compute_ratio(total, parcel.area)
    return Construibles(
        m2_construibles=max(0, total),
        m2_vendibles=max(0, total) * ratio,
        source="normativa",
        pisos=pisos,
        pisada=pisada,
    )


# ─── Internal helpers ─────────────────────────────────────────────────────────


def _district_altura(dist: str) -> float:
    """Default height for a district."""
    return ALTURA_MAX.get(dist, 14.6)


def _compute_pisos(altura: float, dist: str) -> int:
    """Floor count from height limit (integer for reporting)."""
    h_pb = H_PB_USAB if _is_usab(dist) else H_PB_ALTA
    if altura <= h_pb:
        return 1
    return 1 + math.floor((altura - h_pb) / H_PISO)


def _continuous_floors(altura: float, dist: str) -> float:
    """Continuous floor equivalent (for area calculation accuracy)."""
    return max(1.0, altura / H_PISO)


def _is_usab(dist: str) -> bool:
    """Check if district is USAB0/1/2."""
    return "U.S.A.B." in dist or "USAB" in dist.upper()


def _compute_pisada(
    frente: float, fondo: float, area: float, dist: str,
    lfi: float | None = None,
) -> float:
    """Compute per-floor footprint.

    Not used when CONSTR_MULTIPLIER is available (compute_from_normativa
    uses the multiplier table directly). Kept as fallback for districts
    not in the table.
    """
    depth = fondo * _constr_multiplier_fallback(fondo)
    return frente * max(16.0, depth)


def _constr_multiplier_fallback(fondo: float) -> float:
    """Fallback depth fraction when district not in multiplier table."""
    if fondo <= 15:
        return 0.90
    if fondo <= 20:
        return 0.90
    if fondo >= 50:
        return 0.48
    # Linear interpolation 20→50
    return 0.90 - (fondo - 20) * (0.90 - 0.48) / (50 - 20)


# Construibles multiplier: tile_construibles / (frente × fondo × pisos)
# Calibrated from 193k tile parcels (median per district × fondo bucket)
CONSTR_MULTIPLIER: dict[tuple[str, int], float] = {
    ("Corredor Alto", 10): 0.960, ("Corredor Alto", 15): 0.976,
    ("Corredor Alto", 20): 0.940, ("Corredor Alto", 25): 0.896,
    ("Corredor Alto", 30): 0.859, ("Corredor Alto", 35): 0.743,
    ("Corredor Alto", 40): 0.633, ("Corredor Alto", 45): 0.547,
    ("Corredor Alto", 50): 0.528, ("Corredor Alto", 55): 0.505,
    ("Corredor Alto", 60): 0.500, ("Corredor Alto", 65): 0.493,
    ("Corredor Alto", 70): 0.394,
    ("Corredor Medio", 10): 0.871, ("Corredor Medio", 15): 0.892,
    ("Corredor Medio", 20): 0.876, ("Corredor Medio", 25): 0.800,
    ("Corredor Medio", 30): 0.701, ("Corredor Medio", 35): 0.613,
    ("Corredor Medio", 40): 0.546, ("Corredor Medio", 45): 0.516,
    ("Corredor Medio", 50): 0.490, ("Corredor Medio", 55): 0.479,
    ("Corredor Medio", 60): 0.454, ("Corredor Medio", 65): 0.448,
    ("Corredor Medio", 70): 0.399,
    ("E1", 10): 0.976, ("E1", 15): 0.977, ("E1", 20): 0.927,
    ("E1", 25): 0.800, ("E1", 30): 0.709, ("E1", 35): 0.617,
    ("E1", 40): 0.577, ("E1", 45): 0.528, ("E1", 50): 0.432,
    ("E1", 55): 0.512, ("E1", 60): 0.482, ("E1", 70): 0.471,
    ("E2", 10): 0.917, ("E2", 15): 0.928, ("E2", 20): 0.940,
    ("E2", 25): 0.858, ("E2", 30): 0.710, ("E2", 35): 0.651,
    ("E2", 40): 0.625, ("E2", 45): 0.548, ("E2", 50): 0.554,
    ("E2", 55): 0.521, ("E2", 60): 0.475, ("E2", 65): 0.475,
    ("E2", 70): 0.493,
    ("E3", 10): 0.909, ("E3", 15): 0.921, ("E3", 20): 0.921,
    ("E3", 25): 0.814, ("E3", 30): 0.708, ("E3", 35): 0.651,
    ("E3", 40): 0.584, ("E3", 45): 0.521, ("E3", 50): 0.498,
    ("E3", 55): 0.473, ("E3", 60): 0.472, ("E3", 65): 0.451,
    ("E3", 70): 0.437,
    ("U.S.A.A.", 10): 0.902, ("U.S.A.A.", 15): 0.926,
    ("U.S.A.A.", 20): 0.897, ("U.S.A.A.", 25): 0.808,
    ("U.S.A.A.", 30): 0.710, ("U.S.A.A.", 35): 0.625,
    ("U.S.A.A.", 40): 0.557, ("U.S.A.A.", 45): 0.515,
    ("U.S.A.A.", 50): 0.507, ("U.S.A.A.", 55): 0.498,
    ("U.S.A.A.", 60): 0.466, ("U.S.A.A.", 65): 0.456,
    ("U.S.A.A.", 70): 0.409,
    ("U.S.A.B. 1", 10): 0.931, ("U.S.A.B. 1", 15): 0.937,
    ("U.S.A.B. 1", 20): 0.921, ("U.S.A.B. 1", 25): 0.814,
    ("U.S.A.B. 1", 30): 0.674, ("U.S.A.B. 1", 35): 0.612,
    ("U.S.A.B. 1", 40): 0.504, ("U.S.A.B. 1", 45): 0.493,
    ("U.S.A.B. 1", 50): 0.496, ("U.S.A.B. 1", 55): 0.481,
    ("U.S.A.B. 1", 60): 0.474, ("U.S.A.B. 1", 65): 0.436,
    ("U.S.A.B. 1", 70): 0.411,
    ("U.S.A.B. 2", 10): 0.911, ("U.S.A.B. 2", 15): 0.922,
    ("U.S.A.B. 2", 20): 0.904, ("U.S.A.B. 2", 25): 0.723,
    ("U.S.A.B. 2", 30): 0.631, ("U.S.A.B. 2", 35): 0.572,
    ("U.S.A.B. 2", 40): 0.497, ("U.S.A.B. 2", 45): 0.483,
    ("U.S.A.B. 2", 50): 0.477, ("U.S.A.B. 2", 55): 0.465,
    ("U.S.A.B. 2", 60): 0.444, ("U.S.A.B. 2", 65): 0.437,
    ("U.S.A.B. 2", 70): 0.382,
}


def _get_multiplier(dist: str, fondo: float) -> float:
    """Get construibles multiplier from calibration table.

    Interpolates between fondo buckets for the given district.
    Falls back to cross-district average if district not in table.
    """
    fb_lo = max(10, int(fondo / 5) * 5)
    fb_hi = fb_lo + 5

    m_lo = CONSTR_MULTIPLIER.get((dist, fb_lo))
    m_hi = CONSTR_MULTIPLIER.get((dist, min(70, fb_hi)))

    if m_lo is not None and m_hi is not None:
        t = (fondo - fb_lo) / 5.0
        return m_lo + t * (m_hi - m_lo)
    if m_lo is not None:
        return m_lo
    if m_hi is not None:
        return m_hi

    return _constr_multiplier_fallback(fondo)


def _apply_envelope(
    pisada: float, pisos: int, altura: float, dist: str, frente: float,
) -> float:
    """Total construibles = cuerpo floors × pisada + retiro floors.

    The plano limit INCLUDES retiro height. So:
    - cuerpo height = plano - 7m (retiros)
    - Then retiro floors are added with reduced pisada.

    Retiros (Art. 6.3, Ley 6776):
    - USAB0/1/2, E-zones: NO retiros (plano = cuerpo height limit)
    - USAM/USAA: retiro 1 (1 floor, 2m from L.O.) + retiro 2 (1 floor, 4m from L.O. + LFI)
    - CM/CA: same retiros, plus basamento at ground level
    """
    # USAB and E-zones: no retiros, use integer floors
    if _is_usab(dist) or "E" in dist.upper().replace("CORREDOR", "").replace("MEDIO", ""):
        return pisada * pisos

    # Districts with retiros (USAM, USAA, CM, CA):
    # Retiro 1 = 3m, Retiro 2 = 4m (total 7m above cuerpo)
    # Cuerpo height = plano - 7m (retiros eat from the top)
    h_retiro_total = 7.0  # 3m + 4m
    h_cuerpo = max(3.0, altura - h_retiro_total)
    cuerpo_floors = h_cuerpo / H_PISO
    total = pisada * cuerpo_floors

    # Retiro 1: 3m (1 floor), 2m setback from L.O.
    ret1_pisada = max(0, pisada - 2 * frente)
    total += ret1_pisada * (3.0 / H_PISO)

    # Retiro 2: 4m (1.33 floors), 4m from L.O. + 4m from LFI
    ret2_pisada = max(0, pisada - 8 * frente)
    total += ret2_pisada * (4.0 / H_PISO)

    return total


# ─── Data loading helpers ─────────────────────────────────────────────────────


def load_tile_data(tile_db_path: str) -> dict[str, TileData]:
    """Load precomputed tile construibles indexed by smp_norm."""
    conn = sqlite3.connect(tile_db_path)
    rows = conn.execute("""
        SELECT smp_norm, total_construibles, pisada_cuerpo, h_max
        FROM tile_construibles
        WHERE total_construibles > 0
    """).fetchall()
    conn.close()
    return {
        r[0]: TileData(
            total_construibles=r[1],
            pisada_cuerpo=r[2] or 0,
            h_max=r[3] or 0,
        )
        for r in rows
    }


def load_lfi_data(lfi_db_path: str) -> dict[str, float]:
    """Load precomputed LFI values indexed by smp_norm."""
    conn = sqlite3.connect(lfi_db_path)
    rows = conn.execute("SELECT smp_norm, lfi FROM parcel_lfi").fetchall()
    conn.close()
    return {r[0]: r[1] for r in rows}


def get_m2_vendibles(m2_construibles: float, area: float = 0) -> float:
    """Vendibles = construibles × ratio (from density)."""
    return m2_construibles * _compute_ratio(m2_construibles, area)
