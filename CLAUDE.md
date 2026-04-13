# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

EdificIA is a Buenos Aires urban development feasibility platform. It combines CUR zoning data (Código Urbanístico, Ley 6099/2018) with real building heights, land use, and cadastral data for ~280k parcels across CABA. Users search by address or SMP, and get normative parameters (height limits, FOT, floors), subutilization metrics (delta between allowed and built), and 3D buildable envelope visualization.

## Architecture

**Backend**: FastAPI (`server.py`) + SQLite (`caba_normativa.db`, ~400MB, WAL mode). No ORM — raw SQL queries.

**Frontend**: 3 standalone HTML files, no build tools, vanilla JS:
- `index.html` — Hero search page with result cards (SVG map background, AJAX autocomplete)
- `mapa.html` — Leaflet heatmap of parcels colored by metric (delta, volume, floors, reconversion, area). Barrio filter dropdown, per-parcel detail card with official GCBA doc links
- `3d.html` — Cesium.js 3D viewer rendering stepped buildable envelopes (3 CUR sections with retiros)

**Data pipeline** (offline → online enrichment):
1. `precompute_caba.py` — Reads `cur_optimizado.json` (280k parcels) → creates SQLite DB with sanitized CUR rules, derived floors/volume/pisada
2. `integrate_datasets.py` — Merges 8 BA Data CSVs (tejido heights, land use, construction permits, etc.) into DB via SMP joins
3. `enrich_fast.py` — Parallel online enrichment: 5 CUR3D + 2 EPOK concurrent workers (ThreadPoolExecutor) fetching live GCBA API data. Rate-limited by server (~0.8 req/s for CUR3D)

**Envelope computation** (`envelope.py`): Sutherland-Hodgman polygon clipping. Computes 3-section stepped envelope per CUR rules (cuerpo → retiro 1 → retiro 2) using real parcel polygon coordinates.

## Commands

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run server
python3 -m uvicorn server:app --host 127.0.0.1 --port 8765

# Rebuild database from scratch (takes ~5 min total)
python3 precompute_caba.py        # CUR JSON → SQLite (~2 min)
python3 integrate_datasets.py      # Merge CSVs into DB (~3 min)

# Online enrichment (runs indefinitely, safe to interrupt)
python3 enrich_fast.py             # 5 CUR3D + 2 EPOK workers

# Check enrichment progress
python3 -c "import sqlite3; c=sqlite3.connect('caba_normativa.db'); print(dict(c.execute('SELECT COUNT(*) as total, SUM(epok_enriched=1) as epok, SUM(cur3d_enriched=1) as cur3d FROM parcelas').fetchone()))"
```

No tests, no linter, no CI configured.

## API Endpoints (server.py)

| Endpoint | Purpose |
|----------|---------|
| `/api/search?q=...&limit=8` | Autocomplete by address or SMP fragment |
| `/api/parcela/{smp}` | Full parcel details + polygon |
| `/api/parcelas_geo?barrio=...&metric=delta` | GeoJSON for map (metrics: delta, vol, pisos, area, reconversion) |
| `/api/barrios` | List barrios with parcel counts |
| `/api/envelope/{smp}` | Compute 3-section stepped envelope geometry |
| `/api/health` | Enrichment progress stats |

## Database Schema

Single table `parcelas` with 100+ columns. Key groups:

- **CUR normative**: `smp`, `smp_norm`, `cpu`, `cur_distrito`, `h`, `fot`, `plano_san` (sanitized height limit), `pisos`, `area`, `frente`, `fondo`, `pisada`, `vol_edificable`
- **EPOK catastro**: `epok_direccion`, `epok_sup_cubierta`, `epok_pisos_sobre`, `epok_frente`, `epok_fondo` (`epok_enriched`: 0=pending, 1=done, -1=failed)
- **CUR3D edificabilidad**: `edif_sup_edificable_planta`, `edif_altura_max_1..4`, `edif_plano_limite`, `edif_croquis_url`, `edif_perimetro_url`, `polygon_geojson` (`cur3d_enriched`: same flags)
- **Tejido**: `tejido_altura_max`, `tejido_altura_avg`, `delta_altura` (plano_san - tejido = subutilization gap)
- **Land use**: `uso_tipo1`, `uso_tipo2`, `uso_estado`
- **Admin**: `barrio`, `comuna`, `lat`, `lng`

**SMP normalization**: `011-049-026` → `11-49-26` (strip leading zeros). Indexed on `smp_norm`.

## Key Business Logic

**Floor calculation**: `pisos = 1 + floor((plano_san - 3.30) / 2.90)` — PB=3.30m, typical floor=2.90m

**Pisada (footprint)**: If fondo ≤ 16m → `frente × fondo`. If fondo > 16m → `frente × 22m` (LFI setback). Fallback: `area × 0.65`.

**Delta**: `plano_san - tejido_altura_max` — the gap between what's allowed and what's built. Core metric for identifying development opportunity.

**CUR sanitization** (`sanitizarDatosCUR`): Fixes known CUR shapefile bugs — USAB zones (h≤14.6m: plano=h), Corredor Alto (39.2→38.2m), Corredor Medio (31.2m enforcement).

## Data Sources

- `cur_optimizado.json` (37MB) — Master CUR parcel data (280k entries)
- `data/*.csv` — BA Open Data downloads: tejido (real heights), usos_suelo (land use), obras (construction permits), superficie_edificable, volumen_edificable
- `manzanas_heatmap.json` — Precomputed block-level aggregations for the heatmap view

**External APIs** (no auth, no documented rate limits, CC-BY-2.5-AR):
- EPOK: `epok.buenosaires.gob.ar/catastro/parcela/?smp={smp}` — cadastral data
- CUR3D: `epok.buenosaires.gob.ar/cur3d/seccion_edificabilidad/?smp={smp}` — buildability sections (~4s latency, server-side compute bottleneck)
- USIG: `servicios.usig.buenosaires.gob.ar/normalizar` — address geocoding

## Domain Concepts

- **SMP** — Sección-Manzana-Parcela (cadastral ID, e.g., "016-044-038")
- **CPU** — Legacy zoning code, mapped to CUR districts via `CPU_TO_CUR`
- **CUR** — Current zoning system (Ley 6099/2018)
- **Plano Límite (PL)** — Maximum building envelope height
- **FOT** — Floor Area Ratio (buildable area / parcel area)
- **LFI** — Interior setback line (~22m from front for deep parcels)
- **Delta** — Height gap between PL and existing building (subutilization metric)
- **Pisada** — Building footprint area
- **Tejido** — Real built height from photogrammetry
- **EPOK/USIG** — GCBA geospatial services
