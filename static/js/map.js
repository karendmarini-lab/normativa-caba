/**
 * map.js — Leaflet map rendering and interaction layer.
 *
 * Responsibilities:
 *   - Leaflet instance lifecycle (init, tile layers)
 *   - Manzana-level heatmap circles (all-CABA view)
 *   - Parcel-level GeoJSON polygons (barrio view)
 *   - Metric-based color scaling (percentile-normalized)
 *   - Spatial filtering (barrio, pisos, area, FOT, uso, etc.)
 *   - Single-parcel marker for search results
 *
 * Does NOT touch DOM outside its map container.
 * Communicates outward exclusively via callbacks set at init.
 */

// ── State ────────────────────────────────────────────────────────

let _map = null;
let _geoLayer = null;
let _circleLayer = null;
let _marker = null;
let _manzanasData = [];
let _activeMetric = 'delta';
let _activeBarrio = null;
let _selectedSmp = null;
let _filters = {};
let _callbacks = {};

// ── Constants ────────────────────────────────────────────────────

const METRICS = [
  { id: 'delta', label: 'Delta oportunidad',
    desc: 'Plano Límite (CUR) − Altura construida (tejido fotogramétrico GCBA), en metros. Parcelas sin dato de tejido se excluyen. Fuente: CUR Ley 6099/2018, relevamiento fotogramétrico GCBA.' },
  { id: 'vol', label: 'm² vendibles',
    desc: 'vol_edificable en m². Pisada × pisos × 0.85. Pisada = frente × min(fondo, 22m) si fondo > 16m. Pisos = 1 + floor((PL − 3.30) / 2.90). Fuente: EPOK catastro GCBA.' },
  { id: 'pisos', label: 'Pisos',
    desc: 'Pisos permitidos: 1 + floor((PL − 3.30) / 2.90). PB = 3.30m, piso tipo = 2.90m. No contempla premios, enrase ni basamento diferenciado. Fuente: CUR Ley 6099/2018.' },
  { id: 'area', label: 'Superficie',
    desc: 'Área del lote en m² según catastro EPOK/AGIP. Sin transformación.' },
];

const METRIC_KEYS = { delta: 'd', vol: 'v', pisos: 'p', reconversion: 'r', area: 'ta' };

const CABA_CENTER = [-34.615, -58.435];
const CABA_ZOOM = 13;

// ── Color ────────────────────────────────────────────────────────

function colorForScore(score, p5, p95) {
  const t = Math.max(0, Math.min(1, (score - p5) / (p95 - p5 || 1)));
  if (t < 0.5) {
    const s = t * 2;
    return `rgb(${75 + (157 * s) | 0},${50 + (147 * s) | 0},${120 - (49 * s) | 0})`;
  }
  const s = (t - 0.5) * 2;
  return `rgb(${232 + (17 * s) | 0},${197 - (72 * s) | 0},${71 - (49 * s) | 0})`;
}

function percentileBounds(values) {
  const sorted = values.filter(v => v > 0).sort((a, b) => a - b);
  if (!sorted.length) return { p5: 0, p95: 1 };
  return {
    p5: sorted[Math.floor(sorted.length * 0.05)],
    p95: sorted[Math.floor(sorted.length * 0.95)] || sorted[sorted.length - 1],
  };
}

// ── Init ─────────────────────────────────────────────────────────

/**
 * @param {string} containerId
 * @param {Object} callbacks
 * @param {(props: Object) => void} callbacks.onParcelClick
 * @param {(barrio: string) => void} callbacks.onBarrioClick
 */
export function initMap(containerId, callbacks = {}) {
  _callbacks = callbacks;
  _map = L.map(containerId, {
    zoomControl: false,
    attributionControl: false,
  }).setView(CABA_CENTER, CABA_ZOOM);

  L.control.zoom({ position: 'topright' }).addTo(_map);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    subdomains: 'abcd',
    maxZoom: 19,
    opacity: 0.85,
  }).addTo(_map);

  return _map;
}

// ── Manzana circles (all-CABA heatmap) ───────────────────────────

export async function loadManzanas() {
  const resp = await fetch('/manzanas_heatmap.json');
  _manzanasData = await resp.json();
  if (!_activeBarrio) return renderCircles();
}

export function renderCircles() {
  _clearLayers();

  const key = METRIC_KEYS[_activeMetric] || 'd';
  const { p5, p95 } = percentileBounds(_manzanasData.map(m => m[key] || 0));
  const z = _map.getZoom();
  const r = z >= 15 ? 10 : z >= 14 ? 7 : z >= 13 ? 5 : 3;

  _circleLayer = L.layerGroup();
  let count = 0, totalVol = 0, totalDelta = 0, dCount = 0;

  for (const mz of _manzanasData) {
    const val = mz[key] || 0;
    const opacity = Math.max(0.25, Math.min(0.8, (val - p5) / (p95 - p5 || 1)));

    L.circleMarker([mz.lt, mz.ln], {
      radius: r,
      fillColor: colorForScore(val, p5, p95),
      fillOpacity: opacity,
      color: 'rgba(255,255,255,0.1)',
      weight: 0.5,
    }).on('click', () => {
      if (_callbacks.onBarrioClick) _callbacks.onBarrioClick(mz.b);
    }).addTo(_circleLayer);

    count++;
    totalVol += mz.v || 0;
    if (mz.d) { totalDelta += mz.d; dCount++; }
  }

  _circleLayer.addTo(_map);
  return { count, totalVol, avgDelta: dCount ? totalDelta / dCount : 0 };
}

// ── Parcel polygons (barrio view) ────────────────────────────────

export async function loadParcels() {
  _clearLayers();

  const params = _buildFilterParams();
  params.set('barrio', _activeBarrio);
  params.set('metric', _activeMetric);
  params.set('limit', '3000');

  const resp = await fetch(`/api/parcelas_geo?${params}`);
  const geojson = await resp.json();
  const { p5, p95 } = percentileBounds(
    geojson.features.map(f => f.properties.score)
  );

  _geoLayer = L.geoJSON(geojson, {
    style: feature => _parcelStyle(feature, p5, p95),
    onEachFeature: (feature, layer) => {
      layer.on('click', () => _selectParcel(feature, layer));
      layer.on('mouseover', () => {
        if (feature.properties.smp !== _selectedSmp) {
          layer.setStyle({ fillOpacity: 0.75, color: 'rgba(255,255,255,0.5)', weight: 1 });
        }
      });
      layer.on('mouseout', () => {
        if (feature.properties.smp !== _selectedSmp) _geoLayer.resetStyle(layer);
      });
    },
  }).addTo(_map);

  if (geojson.features.length) {
    _map.fitBounds(_geoLayer.getBounds(), { padding: [60, 60] });
  }

  return _computeStats(geojson.features);
}

// ── Public commands ──────────────────────────────────────────────

export function flyTo(lat, lng, zoom = 17) {
  _map.flyTo([lat, lng], zoom, { duration: 1.2 });
}

export function setMetric(metricId) {
  _activeMetric = metricId;
  return _activeBarrio ? loadParcels() : renderCircles();
}

export function setBarrio(barrio) {
  _activeBarrio = barrio || null;
  if (_activeBarrio) return loadParcels();
  _map.setView(CABA_CENTER, CABA_ZOOM);
  return renderCircles();
}

export function setFilters(values) {
  _filters = values || {};
}

export function highlightParcel(smp) {
  _selectedSmp = smp;
  if (_geoLayer) _geoLayer.resetStyle();
}

export function addMarker(lat, lng, label) {
  clearMarker();
  const icon = L.divIcon({
    className: '',
    html: '<div style="width:16px;height:16px;background:#E8C547;border:3px solid #0d1117;border-radius:50%;box-shadow:0 0 0 2px #E8C547,0 0 24px rgba(232,197,71,.5)"></div>',
    iconSize: [16, 16],
    iconAnchor: [8, 8],
  });
  _marker = L.marker([lat, lng], { icon }).addTo(_map);
  if (label) {
    _marker.bindPopup(
      `<span style="font-family:monospace;font-size:12px;color:#E8C547">${label}</span>`
    ).openPopup();
  }
}

export function clearMarker() {
  if (_marker) { _map.removeLayer(_marker); _marker = null; }
}

// ── Read-only getters ────────────────────────────────────────────

export function getActiveBarrio() { return _activeBarrio; }
export function getActiveMetric() { return METRICS.find(m => m.id === _activeMetric); }
export function getMetrics() { return METRICS; }
export function getMap() { return _map; }

export async function fetchBarrios() {
  const resp = await fetch('/api/barrios');
  const data = await resp.json();
  return data.map(b => b.name);
}

// ── Internal helpers ─────────────────────────────────────────────

function _clearLayers() {
  if (_geoLayer) { _map.removeLayer(_geoLayer); _geoLayer = null; }
  if (_circleLayer) { _map.removeLayer(_circleLayer); _circleLayer = null; }
}

function _parcelStyle(feature, p5, p95) {
  const s = feature.properties.score || 0;
  const selected = feature.properties.smp === _selectedSmp;
  return {
    fillColor: colorForScore(s, p5, p95),
    fillOpacity: selected ? 0.9 : 0.55,
    color: selected ? '#fff' : 'rgba(232,197,71,0.3)',
    weight: selected ? 2 : 0.5,
  };
}

function _selectParcel(feature, layer) {
  _selectedSmp = feature.properties.smp;
  if (_geoLayer) _geoLayer.resetStyle();
  layer.setStyle({ fillOpacity: 0.9, color: '#fff', weight: 2 });
  if (_callbacks.onParcelClick) _callbacks.onParcelClick(feature.properties);
}

function _computeStats(features) {
  const totalVol = features.reduce((s, f) => s + (f.properties.vol || 0), 0);
  const deltas = features
    .map(f => (f.properties.pl || 0) - (f.properties.tj || 0))
    .filter(d => d > 0);
  const avgDelta = deltas.length ? deltas.reduce((a, b) => a + b, 0) / deltas.length : 0;
  return { count: features.length, totalVol, avgDelta };
}

function _buildFilterParams() {
  const p = new URLSearchParams();
  const f = _filters;
  if (f.pisosMin) p.set('pisos_min', f.pisosMin);
  if (f.pisosMax) p.set('pisos_max', f.pisosMax);
  if (f.areaMin) p.set('area_min', f.areaMin);
  if (f.areaMax) p.set('area_max', f.areaMax);
  if (f.fotMin) p.set('fot_min', f.fotMin);
  if (f.plMin) p.set('pl_min', f.plMin);
  if (f.uso) p.set('uso', f.uso);
  if (f.aph) p.set('aph', f.aph);
  if (f.riesgo) p.set('riesgo_hidrico', '1');
  if (f.enrase) p.set('enrase', '1');
  return p;
}
