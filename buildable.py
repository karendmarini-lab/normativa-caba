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
    """Compute construibles from CUR rules + calibrated depth curve.

    Steps:
    1. Compute altura from plano_san or district default
    2. Compute pisos from altura
    3. Compute pisada from depth curve (calibrated on 170k tiles)
    4. Apply envelope (pisos × pisada + retiros)
    """
    dist = parcel.cur_distrito or ""
    altura = parcel.plano_san if parcel.plano_san > 3 else _district_altura(dist)
    pisos = _compute_pisos(altura, dist)
    pisada = _compute_pisada(parcel.frente, parcel.fondo, parcel.area, dist, lfi)
    total = _apply_envelope(pisada, pisos, altura, dist, parcel.frente)

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
    """Compute per-floor footprint from LFI + calibrated depth curve.

    Two sources blended:
    1. LFI from manzana geometry (parcel-specific, ×0.92 calibration)
    2. Depth curve from 170k tiles (universal average by fondo)

    Uses LFI when available (better per-parcel accuracy), capped by
    depth curve to prevent overestimates on shallow lots.
    """
    # Depth from universal curve (always available)
    depth_curve = fondo * _depth_fraction(fondo)

    if lfi and lfi > 0:
        # LFI from manzana geometry, calibrated ×0.92
        depth_lfi = min(fondo, lfi * 0.92)
        # Use LFI but cap with curve (+10% tolerance)
        banda = min(depth_lfi, depth_curve * 1.10)
    else:
        banda = depth_curve

    return frente * max(16.0, banda)


# Depth fraction curve: calibrated from 170k tile parcels (all districts)
# depth = fondo × fraction. Interpolated linearly between control points.
_DEPTH_CURVE = [
    (10, 0.94), (15, 0.94), (20, 0.93),  # shallow: nearly full fondo
    (25, 0.81), (30, 0.69),               # LFI starts constraining
    (35, 0.61), (40, 0.53),               # LFI dominates
    (50, 0.50), (65, 0.45),               # deep: ~half fondo
]


def _depth_fraction(fondo: float) -> float:
    """Interpolate edificable depth fraction from calibrated curve."""
    if fondo <= _DEPTH_CURVE[0][0]:
        return _DEPTH_CURVE[0][1]
    if fondo >= _DEPTH_CURVE[-1][0]:
        return _DEPTH_CURVE[-1][1]
    for i in range(len(_DEPTH_CURVE) - 1):
        f1, r1 = _DEPTH_CURVE[i]
        f2, r2 = _DEPTH_CURVE[i + 1]
        if fondo <= f2:
            t = (fondo - f1) / (f2 - f1)
            return r1 + t * (r2 - r1)
    return _DEPTH_CURVE[-1][1]


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
