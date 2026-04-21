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

    # Direct multiplier from 3D calibration table
    mult = _get_multiplier(dist, parcel.fondo, parcel.frente)
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
# 3D calibration table: tile_constr / (frente × fondo × pisos)
# Key: (district, fondo_bucket_5m, frente_bucket: 0=<10m, 1=10-15m, 2=>15m)
# Calibrated from 193k tile parcels (median per bucket, ≥15 samples each)
CONSTR_MULTIPLIER: dict[tuple[str, int, int], float] = {
    ("Corredor Alto", 10, 0): 0.960, ("Corredor Alto", 10, 1): 0.965,
    ("Corredor Alto", 15, 0): 0.986, ("Corredor Alto", 15, 1): 0.947, ("Corredor Alto", 15, 2): 0.954,
    ("Corredor Alto", 20, 0): 0.949, ("Corredor Alto", 20, 1): 0.932, ("Corredor Alto", 20, 2): 0.939,
    ("Corredor Alto", 25, 0): 0.890, ("Corredor Alto", 25, 1): 0.899, ("Corredor Alto", 25, 2): 0.912,
    ("Corredor Alto", 30, 0): 0.858, ("Corredor Alto", 30, 1): 0.844, ("Corredor Alto", 30, 2): 0.888,
    ("Corredor Alto", 35, 0): 0.731, ("Corredor Alto", 35, 1): 0.756, ("Corredor Alto", 35, 2): 0.754,
    ("Corredor Alto", 40, 0): 0.597, ("Corredor Alto", 40, 1): 0.752, ("Corredor Alto", 40, 2): 0.644,
    ("Corredor Alto", 45, 0): 0.547, ("Corredor Alto", 45, 1): 0.562, ("Corredor Alto", 45, 2): 0.547,
    ("Corredor Alto", 50, 0): 0.539, ("Corredor Alto", 50, 1): 0.505, ("Corredor Alto", 50, 2): 0.534,
    ("Corredor Alto", 55, 0): 0.514, ("Corredor Alto", 55, 1): 0.503, ("Corredor Alto", 55, 2): 0.505,
    ("Corredor Alto", 60, 0): 0.508, ("Corredor Alto", 60, 1): 0.482, ("Corredor Alto", 60, 2): 0.497,
    ("Corredor Alto", 65, 0): 0.503, ("Corredor Alto", 65, 1): 0.518, ("Corredor Alto", 65, 2): 0.472,
    ("Corredor Alto", 70, 2): 0.392,
    ("Corredor Medio", 10, 0): 0.874, ("Corredor Medio", 10, 1): 0.864,
    ("Corredor Medio", 15, 0): 0.912, ("Corredor Medio", 15, 1): 0.864, ("Corredor Medio", 15, 2): 0.805,
    ("Corredor Medio", 20, 0): 0.899, ("Corredor Medio", 20, 1): 0.860, ("Corredor Medio", 20, 2): 0.832,
    ("Corredor Medio", 25, 0): 0.798, ("Corredor Medio", 25, 1): 0.834, ("Corredor Medio", 25, 2): 0.777,
    ("Corredor Medio", 30, 0): 0.685, ("Corredor Medio", 30, 1): 0.747, ("Corredor Medio", 30, 2): 0.706,
    ("Corredor Medio", 35, 0): 0.606, ("Corredor Medio", 35, 1): 0.631, ("Corredor Medio", 35, 2): 0.644,
    ("Corredor Medio", 40, 0): 0.549, ("Corredor Medio", 40, 1): 0.549, ("Corredor Medio", 40, 2): 0.531,
    ("Corredor Medio", 45, 0): 0.516, ("Corredor Medio", 45, 1): 0.509, ("Corredor Medio", 45, 2): 0.524,
    ("Corredor Medio", 50, 0): 0.493, ("Corredor Medio", 50, 1): 0.491, ("Corredor Medio", 50, 2): 0.482,
    ("Corredor Medio", 55, 0): 0.490, ("Corredor Medio", 55, 1): 0.464, ("Corredor Medio", 55, 2): 0.461,
    ("Corredor Medio", 60, 0): 0.479, ("Corredor Medio", 60, 1): 0.439, ("Corredor Medio", 60, 2): 0.422,
    ("Corredor Medio", 65, 0): 0.455, ("Corredor Medio", 65, 1): 0.465, ("Corredor Medio", 65, 2): 0.416,
    ("Corredor Medio", 70, 0): 0.434, ("Corredor Medio", 70, 1): 0.428, ("Corredor Medio", 70, 2): 0.377,
    ("E1", 10, 0): 0.979, ("E1", 15, 0): 1.003, ("E1", 15, 1): 0.926,
    ("E1", 20, 0): 0.927, ("E1", 20, 1): 0.919, ("E1", 20, 2): 0.943,
    ("E1", 25, 0): 0.801, ("E1", 25, 1): 0.882, ("E1", 25, 2): 0.744,
    ("E1", 30, 0): 0.701, ("E1", 30, 1): 0.813, ("E1", 30, 2): 0.773,
    ("E1", 35, 0): 0.606, ("E1", 35, 2): 0.747,
    ("E1", 40, 0): 0.582, ("E1", 40, 2): 0.577,
    ("E1", 45, 0): 0.522, ("E1", 45, 2): 0.570,
    ("E1", 50, 0): 0.432, ("E1", 55, 0): 0.518,
    ("E1", 60, 0): 0.476, ("E1", 60, 1): 0.471, ("E1", 60, 2): 0.495,
    ("E1", 70, 2): 0.374,
    ("E2", 10, 0): 0.908, ("E2", 10, 1): 0.980,
    ("E2", 15, 0): 0.934, ("E2", 15, 1): 0.893, ("E2", 15, 2): 0.864,
    ("E2", 20, 0): 0.941, ("E2", 20, 1): 0.940, ("E2", 20, 2): 0.867,
    ("E2", 25, 0): 0.848, ("E2", 25, 1): 0.957, ("E2", 25, 2): 0.836,
    ("E2", 30, 0): 0.706, ("E2", 30, 1): 0.774, ("E2", 30, 2): 0.695,
    ("E2", 35, 0): 0.645, ("E2", 35, 1): 0.743, ("E2", 35, 2): 0.620,
    ("E2", 40, 0): 0.618, ("E2", 40, 1): 1.006, ("E2", 40, 2): 0.614,
    ("E2", 45, 0): 0.548, ("E2", 45, 1): 0.592, ("E2", 45, 2): 0.545,
    ("E2", 50, 0): 0.557, ("E2", 50, 1): 0.597, ("E2", 50, 2): 0.507,
    ("E2", 55, 0): 0.518, ("E2", 55, 1): 0.602, ("E2", 55, 2): 0.498,
    ("E2", 60, 0): 0.475, ("E2", 60, 2): 0.424,
    ("E2", 65, 0): 0.522, ("E2", 65, 2): 0.472, ("E2", 70, 2): 0.488,
    ("E3", 10, 0): 0.910, ("E3", 10, 1): 0.903,
    ("E3", 15, 0): 0.926, ("E3", 15, 1): 0.892, ("E3", 15, 2): 0.893,
    ("E3", 20, 0): 0.929, ("E3", 20, 1): 0.865, ("E3", 20, 2): 0.868,
    ("E3", 25, 0): 0.811, ("E3", 25, 1): 0.860, ("E3", 25, 2): 0.751,
    ("E3", 30, 0): 0.710, ("E3", 30, 1): 0.697, ("E3", 30, 2): 0.659,
    ("E3", 35, 0): 0.644, ("E3", 35, 1): 0.674, ("E3", 35, 2): 0.659,
    ("E3", 40, 0): 0.585, ("E3", 40, 1): 0.559, ("E3", 40, 2): 0.581,
    ("E3", 45, 0): 0.530, ("E3", 45, 1): 0.524, ("E3", 45, 2): 0.477,
    ("E3", 50, 0): 0.505, ("E3", 50, 1): 0.484, ("E3", 50, 2): 0.482,
    ("E3", 55, 0): 0.473, ("E3", 55, 1): 0.472, ("E3", 55, 2): 0.470,
    ("E3", 60, 0): 0.482, ("E3", 60, 1): 0.469, ("E3", 60, 2): 0.437,
    ("E3", 65, 0): 0.482, ("E3", 65, 2): 0.432,
    ("E3", 70, 0): 0.445, ("E3", 70, 1): 0.418, ("E3", 70, 2): 0.437,
    ("U.S.A.A.", 10, 0): 0.899, ("U.S.A.A.", 10, 1): 0.908,
    ("U.S.A.A.", 15, 0): 0.942, ("U.S.A.A.", 15, 1): 0.904, ("U.S.A.A.", 15, 2): 0.915,
    ("U.S.A.A.", 20, 0): 0.899, ("U.S.A.A.", 20, 1): 0.895, ("U.S.A.A.", 20, 2): 0.894,
    ("U.S.A.A.", 25, 0): 0.792, ("U.S.A.A.", 25, 1): 0.855, ("U.S.A.A.", 25, 2): 0.822,
    ("U.S.A.A.", 30, 0): 0.699, ("U.S.A.A.", 30, 1): 0.753, ("U.S.A.A.", 30, 2): 0.719,
    ("U.S.A.A.", 35, 0): 0.622, ("U.S.A.A.", 35, 1): 0.637, ("U.S.A.A.", 35, 2): 0.633,
    ("U.S.A.A.", 40, 0): 0.566, ("U.S.A.A.", 40, 1): 0.546, ("U.S.A.A.", 40, 2): 0.530,
    ("U.S.A.A.", 45, 0): 0.520, ("U.S.A.A.", 45, 1): 0.512, ("U.S.A.A.", 45, 2): 0.498,
    ("U.S.A.A.", 50, 0): 0.512, ("U.S.A.A.", 50, 1): 0.507, ("U.S.A.A.", 50, 2): 0.481,
    ("U.S.A.A.", 55, 0): 0.516, ("U.S.A.A.", 55, 1): 0.500, ("U.S.A.A.", 55, 2): 0.456,
    ("U.S.A.A.", 60, 0): 0.471, ("U.S.A.A.", 60, 1): 0.473, ("U.S.A.A.", 60, 2): 0.440,
    ("U.S.A.A.", 65, 0): 0.474, ("U.S.A.A.", 65, 1): 0.462, ("U.S.A.A.", 65, 2): 0.447,
    ("U.S.A.A.", 70, 0): 0.430, ("U.S.A.A.", 70, 1): 0.418, ("U.S.A.A.", 70, 2): 0.383,
    ("U.S.A.B. 1", 10, 0): 0.956, ("U.S.A.B. 1", 10, 1): 0.911,
    ("U.S.A.B. 1", 15, 0): 0.954, ("U.S.A.B. 1", 15, 1): 0.921, ("U.S.A.B. 1", 15, 2): 0.871,
    ("U.S.A.B. 1", 20, 0): 0.926, ("U.S.A.B. 1", 20, 1): 0.906, ("U.S.A.B. 1", 20, 2): 0.869,
    ("U.S.A.B. 1", 25, 0): 0.810, ("U.S.A.B. 1", 25, 1): 0.851, ("U.S.A.B. 1", 25, 2): 0.754,
    ("U.S.A.B. 1", 30, 0): 0.670, ("U.S.A.B. 1", 30, 1): 0.693, ("U.S.A.B. 1", 30, 2): 0.681,
    ("U.S.A.B. 1", 35, 0): 0.613, ("U.S.A.B. 1", 35, 1): 0.613, ("U.S.A.B. 1", 35, 2): 0.601,
    ("U.S.A.B. 1", 40, 0): 0.505, ("U.S.A.B. 1", 40, 1): 0.506, ("U.S.A.B. 1", 40, 2): 0.488,
    ("U.S.A.B. 1", 45, 0): 0.494, ("U.S.A.B. 1", 45, 1): 0.482, ("U.S.A.B. 1", 45, 2): 0.480,
    ("U.S.A.B. 1", 50, 0): 0.497, ("U.S.A.B. 1", 50, 1): 0.490, ("U.S.A.B. 1", 50, 2): 0.484,
    ("U.S.A.B. 1", 55, 0): 0.485, ("U.S.A.B. 1", 55, 1): 0.470, ("U.S.A.B. 1", 55, 2): 0.459,
    ("U.S.A.B. 1", 60, 0): 0.486, ("U.S.A.B. 1", 60, 1): 0.471, ("U.S.A.B. 1", 60, 2): 0.431,
    ("U.S.A.B. 1", 65, 0): 0.451, ("U.S.A.B. 1", 65, 1): 0.438, ("U.S.A.B. 1", 65, 2): 0.402,
    ("U.S.A.B. 1", 70, 0): 0.401, ("U.S.A.B. 1", 70, 1): 0.420, ("U.S.A.B. 1", 70, 2): 0.424,
    ("U.S.A.B. 2", 10, 0): 0.914, ("U.S.A.B. 2", 10, 1): 0.899,
    ("U.S.A.B. 2", 15, 0): 0.933, ("U.S.A.B. 2", 15, 1): 0.904, ("U.S.A.B. 2", 15, 2): 0.896,
    ("U.S.A.B. 2", 20, 0): 0.909, ("U.S.A.B. 2", 20, 1): 0.881, ("U.S.A.B. 2", 20, 2): 0.851,
    ("U.S.A.B. 2", 25, 0): 0.719, ("U.S.A.B. 2", 25, 1): 0.775, ("U.S.A.B. 2", 25, 2): 0.713,
    ("U.S.A.B. 2", 30, 0): 0.627, ("U.S.A.B. 2", 30, 1): 0.693, ("U.S.A.B. 2", 30, 2): 0.634,
    ("U.S.A.B. 2", 35, 0): 0.573, ("U.S.A.B. 2", 35, 1): 0.588, ("U.S.A.B. 2", 35, 2): 0.554,
    ("U.S.A.B. 2", 40, 0): 0.501, ("U.S.A.B. 2", 40, 1): 0.483, ("U.S.A.B. 2", 40, 2): 0.480,
    ("U.S.A.B. 2", 45, 0): 0.484, ("U.S.A.B. 2", 45, 1): 0.475, ("U.S.A.B. 2", 45, 2): 0.462,
    ("U.S.A.B. 2", 50, 0): 0.481, ("U.S.A.B. 2", 50, 1): 0.466, ("U.S.A.B. 2", 50, 2): 0.444,
    ("U.S.A.B. 2", 55, 0): 0.475, ("U.S.A.B. 2", 55, 1): 0.422, ("U.S.A.B. 2", 55, 2): 0.424,
    ("U.S.A.B. 2", 60, 0): 0.459, ("U.S.A.B. 2", 60, 1): 0.430, ("U.S.A.B. 2", 60, 2): 0.402,
    ("U.S.A.B. 2", 65, 0): 0.457, ("U.S.A.B. 2", 65, 1): 0.429, ("U.S.A.B. 2", 65, 2): 0.325,
    ("U.S.A.B. 2", 70, 0): 0.393, ("U.S.A.B. 2", 70, 1): 0.394, ("U.S.A.B. 2", 70, 2): 0.373,
}


def _get_multiplier(dist: str, fondo: float, frente: float = 8.7) -> float:
    """Get construibles multiplier from 3D calibration table.

    Looks up (district, fondo_bucket, frente_bucket), interpolating
    between fondo buckets. Falls back to 2D (without frente) then
    to cross-district average.
    """
    fb_lo = max(10, int(fondo / 5) * 5)
    fb_hi = min(70, fb_lo + 5)
    fr = 0 if frente < 10 else (1 if frente < 15 else 2)

    # Try 3D lookup first
    m_lo = CONSTR_MULTIPLIER.get((dist, fb_lo, fr))
    m_hi = CONSTR_MULTIPLIER.get((dist, fb_hi, fr))

    # Fallback: try nearby frente buckets
    if m_lo is None:
        for alt_fr in [0, 1, 2]:
            m_lo = CONSTR_MULTIPLIER.get((dist, fb_lo, alt_fr))
            if m_lo is not None:
                break
    if m_hi is None:
        for alt_fr in [0, 1, 2]:
            m_hi = CONSTR_MULTIPLIER.get((dist, fb_hi, alt_fr))
            if m_hi is not None:
                break

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
