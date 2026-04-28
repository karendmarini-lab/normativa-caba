/**
 * app.js — Application orchestrator for EdificIA unified view.
 *
 * Responsibilities:
 *   - Search flow: address input → USIG geocoding → parcel lookup → map + detail
 *   - Parcel detail panel: normative params, calculator, financial analysis
 *   - Filter UI: metric chips, barrio dropdown, advanced filters
 *   - Stats display
 *   - Preloaded spatial data: CUR KD-tree, centroids, grid
 *
 * Imports map.js for all spatial rendering.
 * Does NOT own the map instance — delegates to map module.
 */

import * as Map from './map.js';
import { getSessionId } from './chat.js';
import { addParcelCard, addParcelDocs, addInfoMessage, getChatMode } from './chat.js';

// ── DOM helpers ──────────────────────────────────────────────────

const $ = id => document.getElementById(id);
const fmt = n => Math.round(n).toLocaleString('es-AR');

// ── Preloaded data ───────────────────────────────────────────────

let _centroids = null;
let _gridArr = null;

// ── Financial state ──────────────────────────────────────────────

let _finMetrosVendibles = 0;
let _finMetrosTotales = 0;
let _dbVendible = 0;  // precomputed from DB (source of truth)
let _dbVolumen = 0;
let _dbPisada = 0;
let _pisosEstimados = 1;
let _planoSanitizado = 0;
let _frente = 0;
let _fondo = 0;
let _pisadaSource = 'lfi'; // 'ciudad3d' | 'lfi' | 'estimado'

// ── CPU → CUR mapping ───────────────────────────────────────────

const CPU_TO_CUR = {
  'R2a I': 'U.S.A.A.', 'R2a II': 'U.S.A.A.',
  'C3 I': 'Corredor Medio', 'C3 II': 'Corredor Medio',
  'C2': 'Corredor Alto', 'C1': 'Corredor Alto',
  'R2b I': 'U.S.A.B. 2', 'R2b I 1': 'U.S.A.B. 2',
  'R2b II': 'U.S.A.B. 1', 'R2b III': 'U.S.A.B. 1',
  'R1b I': 'U.S.A.B. 2', 'R1b II': 'U.S.A.B. 2',
  'E1': 'E1', 'E2': 'E2', 'E3': 'E3',
};

// ── Init ─────────────────────────────────────────────────────────

export async function initApp() {
  // Map
  Map.initMap('map', {
    onParcelClick: props => showParcelFromMap(props),
    onBarrioClick: barrio => selectBarrio(barrio),
  });

  // Load barrios and setup filters
  const barrios = await Map.fetchBarrios();
  setupFilters(barrios);

  // Load heatmap
  Map.loadManzanas().then(stats => { if (stats) updateStats(stats); });

  // Search bindings
  const input = $('inp-addr');
  input.addEventListener('input', debounce(onSearchInput, 300));
  input.addEventListener('keydown', e => {
    if (e.key === 'Enter') searchParcel();
    if (e.key === 'Escape') $('suggestions').style.display = 'none';
  });
  document.addEventListener('click', e => {
    if (!e.target.closest('.acwrap')) $('suggestions').style.display = 'none';
  });

  $('btn-main').addEventListener('click', searchParcel);
  $('btn-reset').addEventListener('click', resetSearch);

  // Preload spatial data in background
  setTimeout(() => {
    loadCentroids().catch(e => console.warn('Centroids preload:', e));
  }, 500);
}

// ── Search flow ──────────────────────────────────────────────────

async function searchParcel() {
  clearMessages();
  $('suggestions').style.display = 'none';

  const dir = $('inp-addr').value.trim();
  if (!dir) { showMessage('err', 'Ingresá la dirección para consultar.'); return; }

  setLoading(true);
  try {
    const norm = await geocodeAddress(dir);
    if (!norm.coords) {
      showMessage('err', 'No se pudo geocodificar la dirección.');
      return;
    }

    const { lat, lng } = norm.coords;
    const parcel = await findParcel(lat, lng, norm.direccion);

    if (!parcel || parcel.dist > 30) {
      const msg = parcel && parcel.dist === 0
        ? 'Esta parcela no tiene normativa CUR asignada. Verificá en ciudad3d.buenosaires.gob.ar'
        : `No se pudo asociar la dirección a una parcela con precisión suficiente${parcel ? ` (más cercana a ${parcel.dist}m)` : ''}`;
      showMessage('err', msg);
      showParcelDetail(norm.direccion, null, lat, lng);
      return;
    }

    showParcelDetail(norm.direccion, parcel, lat, lng);
  } catch (e) {
    showMessage('err', e.message);
  } finally {
    setLoading(false);
  }
}

function resetSearch() {
  const input = $('inp-addr');
  if (input) { input.value = ''; input.focus(); }
  $('results').classList.remove('on');
  clearMessages();
  Map.clearMarker();
}

// ── USIG Geocoding ───────────────────────────────────────────────

async function geocodeAddress(dir) {
  const url = 'https://servicios.usig.buenosaires.gob.ar/normalizar/?direccion='
    + encodeURIComponent(dir) + '&geocodificar=true';
  const r = await fetch(url);
  if (!r.ok) throw new Error('No se pudo conectar con USIG.');
  const data = await r.json();
  const lista = data.direccionesNormalizadas;
  if (!lista?.length) throw new Error('Dirección no encontrada en CABA.');

  const dn = lista[0];
  const partido = (dn.nombre_partido || dn.cod_partido || '').toUpperCase();
  if (partido && !['CABA', ''].includes(partido) && !partido.includes('CIUDAD'))
    throw new Error('La dirección parece estar fuera de CABA.');

  const coords = dn.coordenadas
    ? { lat: parseFloat(dn.coordenadas.y), lng: parseFloat(dn.coordenadas.x) }
    : null;
  return { direccion: dn.direccion || dir, coords };
}

// ── Parcel lookup ────────────────────────────────────────────────

async function findParcel(lat, lng, addr) {
  const params = `lat=${lat}&lng=${lng}` + (addr ? `&addr=${encodeURIComponent(addr)}` : '');
  const resp = await fetch(`/api/parcela_nearest?${params}`);
  if (!resp.ok) return null;
  const data = await resp.json();
  // Map DB column names to the field names showParcelDetail expects
  return {
    smp: data.smp, cpu: data.cpu, cur_distrito: data.cur_distrito,
    h: data.h, fot: data.fot,
    plano: data.plano_san, plano_san: data.plano_san,
    pisos: data.pisos, area: data.area,
    fr: data.frente || data.epok_frente,
    fo: data.fondo || data.epok_fondo,
    frente: data.frente || data.epok_frente,
    fondo: data.fondo || data.epok_fondo,
    barrio: data.barrio, comuna: data.comuna,
    epok_direccion: data.epok_direccion,
    es_aph: data.es_aph,
    vol_edificable: data.vol_edificable,
    sup_vendible: data.sup_vendible,
    pisada: data.pisada, pisada_pct: data.pisada_pct,
    tejido_altura_max: data.tejido_altura_max,
    tejido_altura_avg: data.tejido_altura_avg,
    delta_altura: data.delta_altura,
    uso_tipo1: data.uso_tipo1, uso_tipo2: data.uso_tipo2,
    epok_pisos_sobre: data.epok_pisos_sobre,
    epok_sup_cubierta: data.epok_sup_cubierta,
    epok_sup_total: data.epok_sup_total,
    edif_plusvalia_incidencia_uva: data.edif_plusvalia_incidencia_uva,
    edif_plusvalia_alicuota: data.edif_plusvalia_alicuota,
    edif_riesgo_hidrico: data.edif_riesgo_hidrico,
    edif_enrase: data.edif_enrase,
    edif_catalogacion_proteccion: data.edif_catalogacion_proteccion,
    edif_croquis_url: data.edif_croquis_url,
    edif_plano_indice_url: data.edif_plano_indice_url,
    edif_perimetro_url: data.edif_perimetro_url,
    edif_sup_edificable_planta: data.edif_sup_edificable_planta,
    edif_plano_limite: data.edif_plano_limite,
    edif_altura_max_1: data.edif_altura_max_1,
    source: 'server',
  };
}

// ── Display ──────────────────────────────────────────────────────

function showParcelDetail(addr, parcel, lat, lng) {
  // Header
  $('res-addr').textContent = addr;
  $('res-coords').textContent = lat ? `${lat.toFixed(6)}, ${lng.toFixed(6)}  · WGS84` : '';

  const cpu = parcel?.cpu;
  const h = parcel?.h;
  const fot = parcel?.fot;
  const plano = parcel?.plano;

  // Badge
  const cpuLabel = cpu
    ? (CPU_TO_CUR[cpu] ? `${CPU_TO_CUR[cpu]} (${cpu})` : cpu)
    : 'Distrito no det.';
  const badge = $('res-badge');
  badge.textContent = cpuLabel;
  badge.className = 'dbadge ' + (cpu ? 'residential' : 'unknown');

  // Alerts
  ['aph-box', 'u-box', 'cur-box'].forEach(id => $(id)?.classList.remove('on'));
  if (cpu?.toString().startsWith('APH')) $('aph-box')?.classList.add('on');

  // Cards
  $('res-alt').textContent = h || '—';

  const planoSan = (h && (h <= 14.6 || !plano || plano < h)) ? h : plano;
  $('res-plano').textContent = planoSan || '—';

  if (planoSan > 0) {
    const pisos = Math.max(1, 1 + Math.floor((planoSan - 3.30) / 2.90));
    $('res-pis').textContent = pisos <= 1 ? 'PB' : `PB + ${pisos - 1}`;
    const u = $('res-pis').closest?.('.card')?.querySelector('.card-unit');
    if (u) u.textContent = `pisos · PL ${planoSan}m`;
  } else {
    $('res-pis').textContent = '—';
  }

  $('res-fot').textContent = fot || '—';
  $('res-fos').textContent = '—';
  $('res-dis').textContent = cpu || '?';

  // Calculator
  setupCalculator(parcel, planoSan);

  // Enrase: mostrar bloque y calcular asincrónicamente
  const enraseBloque = document.getElementById('enrase-bloque');
  if (enraseBloque) enraseBloque.classList.add('visible');
  const enraseResult = document.getElementById('enrase-resultado');
  if (enraseResult) enraseResult.innerHTML = '<div class="enrase-no"><span class="enrase-icon">—</span><span>Calculando linderos...</span></div>';
  if (parcel?.smp) {
    calcularEnrase(parcel).then(res => {
      window._enraseData = res;
      mostrarEnrase(res);
    });
  }

  // Guardar estado para el Full Report
  window._currentParcelData = parcel;
  window._currentLat = lat;
  window._currentLng = lng;

  // Map
  if (lat && lng) {
    Map.flyTo(lat, lng);
    Map.addMarker(lat, lng, addr);
  }

  // Show results section
  const res = $('results');
  res.classList.remove('on');
  void res.offsetWidth;
  res.classList.add('on');
  setTimeout(() => res.scrollIntoView({ behavior: 'smooth', block: 'start' }), 80);
}

function showParcelFromMap(props) {
  // Click en parcela del mapa → mismo panel derecho que una búsqueda

  const addr = props.dir || props.smp || 'Parcela';
  const lat  = props.lat  || null;
  const lng  = props.lng  || null;

  // Mapear campos del GeoJSON al formato que espera showParcelDetail
  // Soporta tanto nombres compactos (pl, tj) como completos (plano_san, tejido)
  const parcel = {
    cpu:    props.cpu,
    h:      props.tejido || props.tj || props.plano_san || props.pl || null,
    plano:  props.plano_san || props.pl || null,
    plano_san: props.plano_san || props.pl || null,
    fot:    props.fot || null,
    fos:    props.fos || null,
    area:   props.area || props.ta || 0,
    fr:     props.fr  || 0,
    fo:     props.fo  || 0,
    frente: props.fr || 0,
    fondo:  props.fo || 0,
    pisos:  props.pisos || null,
    smp:    props.smp,
    barrio: props.barrio,
    tejido_altura_max: props.tejido || props.tj || null,
    delta_altura: props.score || null,
    vol_edificable: props.vol || null,
    sup_vendible: props.vendible || null,
    uso_tipo1: props.uso1 || null,
    uso_tipo2: props.uso2 || null,
    epok_direccion: props.dir || null,
    epok_pisos_sobre: props.pisos_e || null,
    edif_plusvalia_incidencia_uva: props.plusv_uva || props.plusvalia_uva || null,
    edif_plusvalia_alicuota:       props.plusv_al  || props.plusvalia_alic || null,
    edif_riesgo_hidrico:           props.rh || props.riesgo || null,
    edif_enrase:                   props.enrase || null,
    edif_catalogacion_proteccion:  props.cat || props.catalogacion || null,
    es_aph: props.aph || null,
    delta_pisos: props.dp || null,
  };

  // Guardar lat/lng y datos de parcela globalmente
  window._currentLat        = lat;
  window._currentLng        = lng;
  window._currentParcelData = parcel;

  // Mostrar panel derecho (mismo que búsqueda por dirección)
  showParcelDetail(addr, parcel, lat, lng);

  // Chat: mensaje de contexto breve (no la card entera)
  const { addInfoMessage } = Map._chatImports || {};
  try {
    // addInfoMessage está importado en el módulo
    _rcChatContext(addr, parcel);
  } catch (_) {}

  // Fetch datos enriquecidos del backend (croquis, plusvalía exacta, polygon)
  fetch(`/api/parcela/${encodeURIComponent(props.smp)}`)
    .then(r => r.ok ? r.json() : null)
    .then(data => {
      if (!data) return;
      // Enriquecer _currentParcelData con los datos completos
      window._currentParcelData = {
        ...parcel,
        edif_croquis_url:           data.edif_croquis_url,
        edif_plano_indice_url:      data.edif_plano_indice_url,
        edif_perimetro_url:         data.edif_perimetro_url,
        edif_plusvalia_incidencia_uva: data.edif_plusvalia_incidencia_uva,
        edif_plusvalia_alicuota:    data.edif_plusvalia_alicuota,
        edif_catalogacion_proteccion: data.edif_catalogacion_proteccion,
        edif_riesgo_hidrico:        data.edif_riesgo_hidrico,
        edif_enrase:                data.edif_enrase,
        edif_sup_edificable_planta: data.edif_sup_edificable_planta,
        edif_plano_limite:          data.edif_plano_limite,
        lat: data.lat || lat,
        lng: data.lng || lng,
      };
      // Actualizar coordenadas si el backend las tiene
      if (data.lat) window._currentLat = data.lat;
      if (data.lng) window._currentLng = data.lng;
    }).catch(() => {});
}

// Enviar contexto al chat principal cuando se selecciona una parcela del mapa
function _rcChatContext(addr, parcel) {
  const fields = [
    addr && `Dirección: ${addr}`,
    parcel.cpu && `Distrito: ${parcel.cpu}`,
    parcel.pl && `Plano límite: ${parcel.pl}m`,
    parcel.fot && `FOT: ${parcel.fot}`,
    parcel.area && `Lote: ${Math.round(parcel.area)} m²`,
  ].filter(Boolean).join(' · ');
  addInfoMessage(`Parcela seleccionada — ${fields}. Los datos técnicos están cargados en el panel derecho.`);
}


// ── Calculator ───────────────────────────────────────────────────

function setupCalculator(parcel, planoSan) {
  const cb = $('calc-block');
  const area = parcel?.area || 0;

  // Store DB precomputed values as source of truth
  _dbVendible = parcel?.sup_vendible || 0;
  _dbVolumen = parcel?.vol_edificable || 0;
  _dbPisada = parcel?.pisada || parcel?.edif_sup_edificable_planta || 0;

  if (!area || area <= 0) {
    if (cb) cb.style.display = 'none';
    const fb = $('fin-block');
    if (fb) fb.style.display = 'none';
    return;
  }

  const altRef = planoSan > 0 ? planoSan : (parcel.h || 0);
  _pisosEstimados = altRef > 0 ? Math.max(1, 1 + Math.floor((altRef - 3.30) / 2.90)) : 1;
  _planoSanitizado = planoSan;
  _frente = parcel.fr || 0;
  _fondo = parcel.fo || 0;

  const fr = parcel.fr || 0;
  const fo = parcel.fo || 0;
  const edifPlanta = parcel.edif_sup_edificable_planta || 0;
  let pisadaCalc, bandaLabel;

  if (edifPlanta > 0) {
    pisadaCalc = edifPlanta;
    bandaLabel = '⬡ Ciudad 3D oficial';
    _pisadaSource = 'ciudad3d';
  } else if (fr > 0 && fo > 0) {
    if (fo <= 16) {
      pisadaCalc = Math.min(fr * fo, area);
      bandaLabel = `${fr.toFixed(2)} x ${fo.toFixed(2)} m (100%)`;
      _pisadaSource = 'lfi';
    } else {
      pisadaCalc = Math.min(fr * 22, area);
      bandaLabel = `${fr.toFixed(2)} x 22 m (LFI)`;
      _pisadaSource = 'lfi';
    }
  } else {
    pisadaCalc = area * 0.65;
    bandaLabel = 'estimado 65%';
    _pisadaSource = 'estimado';
  }

  $('c-sup').value = Math.round(area);
  $('c-pb').value = Math.round(pisadaCalc);

  const lblSup = $('c-sup')?.closest('.citem')?.querySelector('.cunit');
  if (lblSup) lblSup.textContent = (fr && fo) ? `m² · ${fr.toFixed(2)} x ${fo.toFixed(2)} m` : 'm² · catastro';

  const lblPb = $('c-pb')?.closest('.citem')?.querySelector('.cunit');
  if (lblPb) lblPb.textContent = `m² · ${bandaLabel}`;

  cb.style.display = 'block';
  recalculate();
  const fb = $('fin-block');
  if (fb) fb.style.display = 'block';
}

export function recalculate() {
  const supInput = $('c-sup');
  const pbInput = $('c-pb');
  if (!supInput || !pbInput) return;

  const inputAreaOficial = $('input-area-edificable');
  const areaEdifManual = inputAreaOficial ? parseFloat(inputAreaOficial.value) : NaN;
  const areaLote = parseFloat(supInput.value) || 0;

  let nuevaPisada = 0;
  let modoAtipica = false;

  if (!isNaN(areaEdifManual) && areaEdifManual > 0) {
    nuevaPisada = Math.min(areaEdifManual, areaLote);
    modoAtipica = true;
  } else {
    const pbVal = parseFloat(pbInput.value) || 0;
    nuevaPisada = pbVal > 0 ? pbVal
      : (_fondo <= 16 ? areaLote : Math.min(_frente * 22, areaLote));
  }
  if (nuevaPisada <= 0 || _pisosEstimados <= 0) return;

  const lblPisada = $('c-pb')?.closest('.citem')?.querySelector('.cunit');
  const pisadaLabels = { ciudad3d: '⬡ Ciudad 3D oficial', lfi: '✦ Calculado: LFI a 22m', estimado: '~ Estimado 65%' };
  if (lblPisada) lblPisada.textContent = modoAtipica ? '⬡ Dato Oficial: Manzana Atípica' : (pisadaLabels[_pisadaSource] || '✦ Calculado');

  const profEdificio = _frente > 0 ? nuevaPisada / _frente : 20;

  // Volume with CUR retiros
  let volumen = 0;
  if (_planoSanitizado <= 14.6) {
    volumen = nuevaPisada * _pisosEstimados;
  } else if (_pisosEstimados > 2) {
    const retiro1 = Math.max(0, _frente > 0 ? _frente * (profEdificio - 4) : nuevaPisada * 0.8);
    const retiro2 = Math.max(0, _frente > 0 ? _frente * (profEdificio - 8) : nuevaPisada * 0.6);
    volumen = (nuevaPisada * (_pisosEstimados - 2)) + retiro1 + retiro2;
  } else {
    volumen = nuevaPisada * _pisosEstimados;
  }

  // 2-feature efficiency: density + frente (calibrated on 150 professional studies, 85% combined)
  const density = areaLote > 0 ? volumen / areaLote : 5;
  const fr_eff = _frente || 8.7;
  let eficiencia = 0.78 - 0.02 * Math.max(0, density - 5) + 0.002 * Math.max(0, fr_eff - 8);
  eficiencia = Math.max(0.55, Math.min(0.95, eficiencia));

  let vendibleCubierto = volumen * eficiencia;

  // Balconies
  const anchoBalcon = Math.max(0, (_frente || 0) - 1.20);
  let totalBalcones = 0;
  if (_pisosEstimados > 1 && anchoBalcon > 0) {
    totalBalcones = (anchoBalcon * 1.20) * 2 * (_pisosEstimados - 1);
  }

  let vendibleTotal = vendibleCubierto + totalBalcones;

  // Use DB precomputed values when user hasn't edited inputs
  const pbUnchanged = Math.abs(nuevaPisada - (_dbPisada || 0)) < 1;
  if (pbUnchanged && _dbVolumen > 0 && _dbVendible > 0) {
    volumen = _dbVolumen;
    vendibleTotal = _dbVendible;
    vendibleCubierto = vendibleTotal - totalBalcones;
    eficiencia = volumen > 0 ? vendibleCubierto / volumen : 0.85;
  }

  // Display
  $('c-edif').textContent = fmt(volumen);
  const unitEdif = $('c-edif')?.closest('.citem')?.querySelector('.cunit');
  if (unitEdif && _frente > 0) unitEdif.textContent = `prof. edif. ${profEdificio.toFixed(1)}m`;

  const cvendEl = $('c-vend');
  if (cvendEl) {
    cvendEl.innerHTML =
      '<div style="font-size:11px;color:var(--muted);margin-bottom:2px">Vendible cubierto</div>' +
      `<div style="font-size:20px;font-weight:400"><span data-frm="cub">${fmt(vendibleCubierto)}</span> <span style="font-size:10px">m²</span></div>` +
      `<div style="font-size:10px;color:var(--muted);margin-bottom:8px"><span data-frm="ef">${Math.round(eficiencia * 100)}%</span></div>` +
      '<div style="font-size:11px;color:var(--muted);margin-bottom:2px">Balcones (semicubierto)</div>' +
      `<div style="font-size:20px;font-weight:400"><span data-frm="balc">${fmt(totalBalcones)}</span> <span style="font-size:10px">m²</span></div>` +
      '<div style="border-top:1px solid var(--border);margin:8px 0"></div>' +
      '<div style="font-size:11px;color:var(--muted);margin-bottom:2px">Total vendible</div>' +
      `<div style="font-size:24px;font-weight:500"><span data-frm="total">${fmt(vendibleTotal)}</span> <span style="font-size:11px">m²</span></div>`;
  }

  _finMetrosTotales = volumen;
  _finMetrosVendibles = vendibleTotal;
  recalcFinancials();
}

function recalcFinancials() {
  const terreno = parseFloat($('fin-terreno')?.value) || 0;
  const precioM2 = parseFloat($('fin-precio-m2')?.value) || 0;
  const container = $('resultados-financieros');
  if (!container) return;

  if (!_finMetrosVendibles || !_finMetrosTotales || (!terreno && !precioM2)) {
    container.innerHTML = '';
    return;
  }

  const fmtUSD = n => 'USD ' + Math.round(n).toLocaleString('en-US');
  const incidencia = terreno ? Math.round(terreno / _finMetrosVendibles) : null;
  const costoCons = Math.round(_finMetrosTotales * 800);
  const gdv = precioM2 ? Math.round(_finMetrosVendibles * precioM2) : null;

  const card = (label, value, sub) => `
    <div style="background:#0a1409;border:1px solid #22c55e33;border-radius:4px;padding:12px 14px">
      <div style="font-size:9px;letter-spacing:2px;color:#4ade80;text-transform:uppercase;margin-bottom:4px">${label}</div>
      <div style="font-size:20px;font-weight:600;color:#f0fdf4">${value}</div>
      ${sub ? `<div style="font-size:10px;color:#6b7280;margin-top:2px">${sub}</div>` : ''}
    </div>`;

  container.innerHTML = `<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:10px">
    ${incidencia != null ? card('Incidencia del Terreno', `${fmtUSD(incidencia)}<span style="font-size:11px;color:#6b7280"> /m²</span>`) : ''}
    ${card('Costo Construcción Est.', fmtUSD(costoCons), `${Math.round(_finMetrosTotales).toLocaleString('en-US')} m² × USD 800`)}
    ${gdv != null ? card('Volumen de Ventas Est. (GDV)', fmtUSD(gdv), `${Math.round(_finMetrosVendibles).toLocaleString('en-US')} m² vendibles`) : ''}
  </div>`;
}

// ── Autocomplete ─────────────────────────────────────────────────

async function onSearchInput() {
  const v = $('inp-addr').value.trim();
  if (v.length < 3) { $('suggestions').style.display = 'none'; return; }

  try {
    const url = 'https://servicios.usig.buenosaires.gob.ar/normalizar/?direccion='
      + encodeURIComponent(v) + '&geocodificar=false&max_calles=5';
    const r = await fetch(url);
    const d = await r.json();
    const items = d.direccionesNormalizadas || [];
    if (!items.length) { $('suggestions').style.display = 'none'; return; }

    const sug = $('suggestions');
    sug.innerHTML = items.slice(0, 5).map(i =>
      `<div class="si" data-addr="${i.direccion}">${i.direccion}</div>`
    ).join('');
    sug.style.display = 'block';

    sug.querySelectorAll('.si').forEach(el => {
      el.addEventListener('click', () => {
        $('inp-addr').value = el.dataset.addr;
        sug.style.display = 'none';
      });
    });
  } catch { $('suggestions').style.display = 'none'; }
}

// ── Filters ──────────────────────────────────────────────────────

function setupFilters(barrios) {
  // Metric chips
  const mRow = $('metricFilters');
  for (const m of Map.getMetrics()) {
    const chip = document.createElement('button');
    chip.className = 'chip' + (m.id === Map.getActiveMetric()?.id ? ' active' : '');
    chip.textContent = m.label;
    chip.addEventListener('click', () => {
      mRow.querySelectorAll('.chip').forEach(c => c.classList.remove('active'));
      chip.classList.add('active');
      updateMetricDesc();
      Map.setMetric(m.id).then(stats => { if (stats) updateStats(stats); });
    });
    mRow.appendChild(chip);
  }
  updateMetricDesc();

  // Barrio dropdown
  const btn = $('barrioBtn');
  const list = $('barrioList');

  const addBarrioItem = (name, display) => {
    const el = document.createElement('div');
    el.style.cssText = 'padding:8px 12px;border-radius:8px;cursor:pointer;font-size:13px;color:var(--text)';
    el.textContent = display;
    el.addEventListener('mouseenter', () => el.style.background = 'rgba(255,255,255,.08)');
    el.addEventListener('mouseleave', () => el.style.background = 'none');
    el.addEventListener('click', () => selectBarrio(name));
    list.appendChild(el);
  };

  addBarrioItem(null, 'Todo CABA');
  barrios.forEach(b => addBarrioItem(b, b));

  btn.addEventListener('click', () => {
    list.style.display = list.style.display === 'none' ? 'block' : 'none';
  });
  document.addEventListener('click', e => {
    if (!e.target.closest('#barrioDropdown')) list.style.display = 'none';
  });

  // Advanced filters
  $('btn-apply-filters')?.addEventListener('click', applyFilters);
}

function selectBarrio(barrio) {
  $('barrioBtn').textContent = barrio || 'Todo CABA';
  $('barrioList').style.display = 'none';
  Map.setBarrio(barrio).then(stats => {
    if (stats) updateStats(stats);
    if (barrio) {
      addInfoMessage(`Mostrando ${stats?.count || ''} parcelas en ${barrio}`);
    }
  });
}

function applyFilters() {
  Map.setFilters({
    pisosMin: $('fPisosMin')?.value,
    pisosMax: $('fPisosMax')?.value,
    areaMin: $('fAreaMin')?.value,
    areaMax: $('fAreaMax')?.value,
    fotMin: $('fFotMin')?.value,
    plMin: $('fPlMin')?.value,
    uso: $('fUso')?.value,
    aph: $('fAph')?.value,
    riesgo: $('fRiesgo')?.checked,
    enrase: $('fEnrase')?.checked,
  });

  const barrio = Map.getActiveBarrio();
  if (barrio) {
    Map.loadParcels().then(stats => updateStats(stats));
  } else {
    Map.renderCircles();
  }
}

// ── Stats display ────────────────────────────────────────────────

function updateStats(stats) {
  if (!stats) return;
  $('statCount').textContent = stats.count.toLocaleString('es-AR');
  $('statVol').textContent = Math.round(stats.totalVol / 1e6) + 'M m²';
  $('statDelta').textContent = stats.avgDelta ? stats.avgDelta.toFixed(1) + 'm' : '-';
  const metric = Map.getActiveMetric();
  if (metric) $('legendTitle').textContent = metric.label;
}

function updateMetricDesc() {
  const m = Map.getActiveMetric();
  $('metricDesc').textContent = m?.desc || '';
}

// ── Data loading ─────────────────────────────────────────────────

// loadCurData removed — parcel lookup is now server-side via /api/parcela_nearest

async function loadCentroids() {
  if (_centroids) return;
  try {
    const r = await fetch('./cur_centroids.json?v=7');
    _centroids = await r.json();
  } catch { _centroids = []; }
}

// ── UI helpers ───────────────────────────────────────────────────

function setLoading(on) {
  const btn = $('btn-main');
  if (!btn) return;
  btn.classList.toggle('loading', on);
  btn.disabled = on;
}

function showMessage(type, text) {
  clearMessages();
  const el = $('msg-' + type);
  if (el) { el.textContent = text; el.classList.add('on'); }
}

function clearMessages() {
  ['err', 'warn', 'info'].forEach(t => $('msg-' + t)?.classList.remove('on'));
}

function debounce(fn, ms) {
  let timer;
  return (...args) => { clearTimeout(timer); timer = setTimeout(() => fn(...args), ms); };
}

// ── Expose for inline handlers ───────────────────────────────────

window.recalculate = recalculate;
window.recalcFinancials = recalcFinancials;


// ── FULL REPORT MODAL ────────────────────────────────────────────

function openFullReport() {
  const modal = document.getElementById('full-report-modal');
  if (!modal) return;

  // Setter limpio con innerText
  const set = (id, val) => {
    const el = document.getElementById(id);
    if (!el) return;
    const v = (val || '').toString().trim();
    el.innerText = v || 'No disponible';
  };
  const getText = id => (document.getElementById(id)?.textContent || '').trim();
  const getVal  = id => (document.getElementById(id)?.value || '').trim();
  const stripM2 = s => s.replace(/m²|m2/gi,'').trim();

  // ── Frozen snapshot from module state — NOT from DOM ──────
  const pd = window._currentParcelData || {};
  const snap = {
    addr: document.getElementById('res-addr')?.textContent || pd.epok_direccion || '—',
    badge: pd.cpu ? (CPU_TO_CUR[pd.cpu] ? `${CPU_TO_CUR[pd.cpu]} (${pd.cpu})` : pd.cpu) : '—',
    lat: window._currentLat,
    lng: window._currentLng,
    h: pd.h || pd.tejido_altura_max || null,
    plano: pd.plano_san || pd.plano || null,
    pisos: _pisosEstimados,
    fot: pd.fot || null,
    area: pd.area || 0,
    frente: _frente || pd.frente || pd.fr || 0,
    fondo: _fondo || pd.fondo || pd.fo || 0,
    pisada: _dbPisada || parseFloat(document.getElementById('c-pb')?.value) || 0,
    vendible: _dbVendible || _finMetrosVendibles || 0,
    volumen: _dbVolumen || _finMetrosTotales || 0,
    cubierto: _finMetrosVendibles ? _finMetrosVendibles - ((_frente - 1.2) * 1.2 * 2 * Math.max(0, _pisosEstimados - 1)) : 0,
    eficiencia: (() => {
      const v = _dbVolumen || _finMetrosTotales || 1;
      const a = pd.area || 1;
      const f = _frente || pd.frente || pd.fr || 8.7;
      const d = v / a;
      return Math.round(Math.max(0.55, Math.min(0.95, 0.78 - 0.02 * Math.max(0, d - 5) + 0.002 * Math.max(0, f - 8))) * 100);
    })(),
    balcones: _frente > 0 && _pisosEstimados > 1 ? Math.round((_frente - 1.2) * 1.2 * 2 * (_pisosEstimados - 1)) : 0,
    distrito: pd.cpu || '—',
  };
  const planoSan = snap.plano && snap.h ? ((snap.h <= 14.6 || !snap.plano || snap.plano < snap.h) ? snap.h : snap.plano) : (snap.plano || snap.h || null);
  const pisosLabel = snap.pisos > 1 ? `PB + ${snap.pisos - 1}` : (snap.pisos === 1 ? 'PB' : '—');

  // ── A: Cabecera
  set('full-address', snap.addr);
  set('full-district-badge', snap.badge);
  set('full-coordinates', snap.lat ? `${snap.lat.toFixed(6)}, ${snap.lng.toFixed(6)} · WGS84` : '');
  set('full-dis', snap.distrito);

  // ── B: Vendibles
  set('full-total', snap.vendible ? fmt(snap.vendible) : '—');
  set('full-cubierto', snap.cubierto ? fmt(snap.cubierto) : '—');
  set('full-balcones', fmt(snap.balcones));
  set('full-efficiency', snap.eficiencia ? `Eficiencia: ${snap.eficiencia}%` : '');
  set('full-volumen', snap.volumen ? fmt(snap.volumen) : '—');

  // ── C: Parámetros normativos
  set('full-h', snap.h || '—');
  set('full-plano', planoSan || '—');
  set('full-pisos', pisosLabel);
  set('full-fot', snap.fot || '—');
  set('full-lote', snap.area ? snap.area + ' m²' : '—');
  set('full-pisada', snap.pisada ? Math.round(snap.pisada) + ' m²' : '—');

  // ── D: Frente y fondo
  set('full-frente', snap.frente ? snap.frente + ' m' : 'No disponible');
  set('full-fondo', snap.fondo ? snap.fondo + ' m' : 'No disponible');
  // ── D bis: Enrase ──────────────────────────────────────
  const enraseData = window._enraseData;
  const frmEnr = document.getElementById('frm-enrase-bloque');
  const frmEnrCont = document.getElementById('frm-enrase-contenido');
  if (enraseData && enraseData.aplica && frmEnr && frmEnrCont) {
    frmEnr.style.display = 'block';
    if (enraseData.aplica) {
      frmEnrCont.innerHTML =
        '<div class="frm-analysis-item"><span>Lindero más alto</span><span>' + enraseData.altura_lindero_max + 'm</span></div>' +
        '<div class="frm-analysis-item"><span>Plano límite parcela</span><span>' + enraseData.plano_san + 'm</span></div>' +
        '<div class="frm-analysis-item"><span>Pisos extra</span><span style="color:#E8C547;font-weight:500">+' + enraseData.pisos_extra + ' pisos</span></div>' +
        '<div class="frm-analysis-item"><span>M² extra</span><span style="color:#E8C547;font-weight:500">+' + enraseData.m2_extra.toLocaleString('es-AR') + ' m²</span></div>' +
        '<div style="font-size:10px;color:rgba(255,255,255,.25);margin-top:8px;font-style:italic">Verificar linderos en Ciudad 3D.</div>';
    } else {
      frmEnrCont.innerHTML = '<div class="frm-analysis-item"><span>Estado</span><span>' + enraseData.mensaje + '</span></div>';
    }
  }

  if (pd) {
    // Plusvalía — dual moneda USD/UVA
    const inc = pd.edif_plusvalia_incidencia_uva;
    const al  = pd.edif_plusvalia_alicuota;
    if (inc) {
      const uvaRnd = Math.round(inc);
      const usdVal = (_fcDolarBlue > 0 && _fcUVA > 0)
        ? Math.round(uvaRnd * _fcUVA / _fcDolarBlue)
        : null;
      const elInc = document.getElementById('full-plusvalia-incidencia');
      if (elInc) {
        if (usdVal) {
          elInc.innerHTML =
            `<span class="frm-plusvalia-usd">USD ${usdVal.toLocaleString('es-AR')}</span>` +
            `<span class="frm-plusvalia-uva">&nbsp;(${uvaRnd.toLocaleString('es-AR')} UVA)</span>`;
        } else {
          elInc.textContent = uvaRnd.toLocaleString('es-AR') + ' UVA';
        }
      }
    } else {
      set('full-plusvalia-incidencia', 'No disponible');
    }
    set('full-plusvalia-alicuota', al ? al + '%' : 'No disponible');

    // Afectaciones
    const cat = pd.edif_catalogacion_proteccion;
    set('full-catalogacion', (cat && cat !== 'DESESTIMADO') ? cat : 'Sin registro especial');
    set('full-riesgo',  pd.edif_riesgo_hidrico ? 'Sí — verificar' : 'No presenta');
    set('full-enrase',  pd.edif_enrase          ? 'Sí — aplica'   : 'No presenta');

    // Croquis
    const cContainer = document.getElementById('croquis-container');
    const cInner     = document.getElementById('croquis-links-inner');
    if (cContainer && cInner) {
      const links = [];
      if (pd.edif_croquis_url)      links.push(['Descargar croquis oficial', pd.edif_croquis_url]);
      if (pd.edif_perimetro_url)    links.push(['Perímetro',                 pd.edif_perimetro_url]);
      if (pd.edif_plano_indice_url) links.push(['Plano índice',              pd.edif_plano_indice_url]);
      if (links.length) {
        cInner.innerHTML = links.map(([lbl, url]) =>
          `<a class="croquis-link" href="${url}" target="_blank">${lbl} ↗</a>`
        ).join('');
        cContainer.classList.remove('hidden');
      } else {
        cContainer.classList.add('hidden');
      }
    }
  }

  // ── E: Mapa Leaflet secundario ────────────────────────────
  const lat = window._currentLat;
  const lng = window._currentLng;
  if (lat && lng && typeof L !== 'undefined') {
    if (window._reportMap) { window._reportMap.remove(); window._reportMap = null; }
    setTimeout(() => {
      const mapEl = document.getElementById('report-location-map');
      if (!mapEl) return;
      const rmap = L.map('report-location-map', {
        zoomControl: false, attributionControl: false, scrollWheelZoom: false
      }).setView([lat, lng], 17);
      // Base oscura sin etiquetas
      L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_nolabels/{z}/{x}/{y}{r}.png', {
        maxZoom: 20,
        crossOrigin: 'anonymous',
      }).addTo(rmap);
      // Etiquetas: light_only_labels (negras) → invert CSS → blanco hueso #E8E8E8
      L.tileLayer('https://{s}.basemaps.cartocdn.com/light_only_labels/{z}/{x}/{y}{r}.png', {
        maxZoom: 20,
        crossOrigin: 'anonymous',
        className: 'map-labels-layer',
      }).addTo(rmap);
      const goldIcon = L.divIcon({
        html: '<div style="width:14px;height:14px;background:#FFBF00;border-radius:50%;border:2px solid #fff;box-shadow:0 0 10px rgba(255,191,0,.9)"></div>',
        className:'', iconAnchor:[7,7]
      });
      L.marker([lat, lng], { icon: goldIcon }).addTo(rmap);
      window._reportMap = rmap;
      setTimeout(() => rmap.invalidateSize(), 80);
    }, 150);
  }

  // Inicializar chat
  const _rcCtx2 = [getText('res-addr'),getText('res-badge'),
    'Lote: '+getVal('c-sup')+'m²','Vendible: '+getText('full-total')+'m²'
  ].filter(Boolean).join('\n');
  modal.classList.remove('hidden');
  // El modal tiene su propio scroll — no bloquear el body
  // document.body.style.overflow = 'hidden'; // REMOVIDO
  // Inicializar calculadora con valores de la parcela
  setTimeout(() => initFeasCalc(), 60);

  // Mostrar botón PDF (fijo en la esquina superior del modal)
  const dlBtn = document.getElementById('btn-download-pdf');
  if (dlBtn) { dlBtn.style.display = 'flex'; dlBtn.onclick = downloadPDF; }

  // Inicializar chat DESPUÉS de mostrar el modal
  setTimeout(() => rcInit(_rcCtx2), 50);
}

// Snapshot the pristine modal body HTML once at load time
let _reportBodyTemplate = '';
document.addEventListener('DOMContentLoaded', () => {
  const body = document.querySelector('#full-report-modal .frm-body');
  if (body) _reportBodyTemplate = body.innerHTML;
});

function closeFullReport() {
  // Abort everything in flight
  if (_rcAbortCtrl) { _rcAbortCtrl.abort(); _rcAbortCtrl = null; }
  _rcStreaming = false;

  // Destroy Leaflet map
  if (window._reportMap) { window._reportMap.remove(); window._reportMap = null; }

  // Nuclear reset: restore pristine HTML — kills all listeners, dirty state, stale DOM
  const body = document.querySelector('#full-report-modal .frm-body');
  if (body && _reportBodyTemplate) body.innerHTML = _reportBodyTemplate;

  // Reset all initialization flags
  _rcBound = false;

  // Hide modal and PDF button
  const modal = document.getElementById('full-report-modal');
  if (modal) modal.classList.add('hidden');
  document.body.style.overflow = '';
  const dlBtn = document.getElementById('btn-download-pdf');
  if (dlBtn) dlBtn.style.display = 'none';
}

function downloadPDF() {
  window.print();
}

// Mostrar/ocultar botón junto con el panel de resultados
const _origShow = typeof showParcelDetail === 'function' ? showParcelDetail : null;

document.addEventListener('DOMContentLoaded', () => {
  // Botón abrir
  document.getElementById('open-full-report')
    ?.addEventListener('click', openFullReport);
  // Botón cerrar
  document.getElementById('close-full-report')
    ?.addEventListener('click', closeFullReport);
  // Cerrar con Escape
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') closeFullReport();
  });
});

// Mostrar el botón cuando los resultados estén visibles
const _resultsObserver = new MutationObserver(() => {
  const res = document.getElementById('results');
  const btn = document.getElementById('open-full-report');
  if (!res || !btn) return;
  if (res.classList.contains('on')) {
    btn.classList.add('visible');
  } else {
    btn.classList.remove('visible');
  }
});
document.addEventListener('DOMContentLoaded', () => {
  const res = document.getElementById('results');
  if (res) _resultsObserver.observe(res, { attributes: true, attributeFilter: ['class'] });
});


// ── REPORT CHAT ─────────────────────────────────────────────────
// Mini-chat independiente dentro del modal de informe.
// NO modifica chat.js. Llama a /api/chat via SSE.
// Event delegation: listeners en el contenedor, no en elementos clonados.

let _rcStreaming  = false;
let _rcAbortCtrl  = null;
let _rcBound      = false; // prevenir bindings duplicados

function rcInit(parcelContext) {
  _rcStreaming  = false;
  window._rcPendingContext = parcelContext || '';

  // Limpiar mensajes anteriores
  const messagesEl = document.getElementById('rc-messages');
  if (messagesEl) {
    messagesEl.innerHTML = '';
    const info = document.createElement('div');
    info.className = 'rc-msg info';
    info.textContent = parcelContext
      ? '📍 ' + parcelContext.split('\n')[0]
      : 'Informe cargado. Podés preguntar sobre esta parcela.';
    messagesEl.appendChild(info);
  }

  // Bindear una sola vez usando el contenedor (event delegation)
  if (!_rcBound) {
    const container = document.getElementById('report-chat-container');
    if (container) {
      // Enter en el textarea
      container.addEventListener('keydown', e => {
        if (e.target.id === 'rc-input' && e.key === 'Enter' && !e.shiftKey) {
          e.preventDefault();
          rcSend();
        }
      });
      // Click en el botón enviar
      container.addEventListener('click', e => {
        if (e.target.closest('#rc-send-btn')) rcSend();
      });
      // Click en chips
      container.addEventListener('click', e => {
        const chip = e.target.closest('.rc-chip');
        if (chip && chip.dataset.question) rcSend(chip.dataset.question);
      });
      // Close button
      container.addEventListener('click', e => {
        if (e.target.closest('#rc-close')) {
          const modal = container.closest('.formal-report') || container.closest('[id*="report"]');
          if (modal) modal.style.display = 'none';
        }
      });
      // Expand/collapse
      container.addEventListener('click', e => {
        if (e.target.closest('#rc-expand')) {
          container.classList.toggle('rc-fullscreen');
        }
      });
      // Model selector
      const rcModelEl = document.getElementById('rc-model');
      if (rcModelEl) {
        rcModelEl.addEventListener('change', () => {
          // Sync with main chat model
          try {
            const { setModel } = import('./chat.js');
          } catch(_) {}
        });
      }
      _rcBound = true;
    }
  }
}

function rcScrollBottom() {
  const el = document.getElementById('rc-messages');
  if (el) el.scrollTop = el.scrollHeight;
}

async function rcSend(textOverride) {
  if (_rcStreaming) return;

  const inputEl   = document.getElementById('rc-input');
  const messagesEl = document.getElementById('rc-messages');
  if (!messagesEl) return;

  const text = (textOverride || inputEl?.value || '').trim();
  if (!text) return;

  // Limpiar input antes del fetch
  if (inputEl && !textOverride) inputEl.value = '';

  // Construir mensaje con contexto si es el primero
  let agentMessage = text;
  if (window._rcPendingContext) {
    agentMessage = window._rcPendingContext + '\n\n' + text;
    window._rcPendingContext = '';
  }

  // Mensaje del usuario
  const userEl = document.createElement('div');
  userEl.className = 'rc-msg user';
  userEl.textContent = text;
  messagesEl.appendChild(userEl);

  // Placeholder del asistente
  const assistEl = document.createElement('div');
  assistEl.className = 'rc-msg assistant';
  messagesEl.appendChild(assistEl);

  const workEl = document.createElement('div');
  workEl.className = 'rc-msg working';
  workEl.textContent = 'Analizando...';
  messagesEl.appendChild(workEl);
  rcScrollBottom();

  _rcStreaming  = true;
  _rcAbortCtrl  = new AbortController();

  const sendBtn = document.getElementById('rc-send-btn');
  if (sendBtn) sendBtn.disabled = true;

  // Obtener modelo: primero del selector del report chat, sino del principal
  const rcModelEl = document.getElementById('rc-model');
  let model = rcModelEl?.value || 'haiku';
  if (!model || model === 'haiku') {
    try {
      const { getModel } = await import('./chat.js');
      model = getModel() || 'haiku';
    } catch(_) {}
  }

  let accumulated = '';

  try {
    const resp = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: getSessionId(), message: agentMessage, model }),
      signal: _rcAbortCtrl.signal,
    });

    workEl.remove();

    if (!resp.ok) {
      assistEl.className = 'rc-msg error';
      assistEl.textContent = resp.status === 401
        ? 'Iniciá sesión para usar el chat IA.'
        : resp.status === 403
        ? 'Tu plan no incluye acceso al chat IA.'
        : 'Error ' + resp.status + ' — intentá nuevamente.';
      return;
    }

    const reader  = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop();

      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          const ev = JSON.parse(line.slice(6));
          if (ev.type === 'text') {
            accumulated += ev.data;
            assistEl.innerHTML = accumulated
              .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
              .replace(/\*\*([^*]+)\*\*/g,'<b>$1</b>')
              .replace(/`([^`]+)`/g,'<code>$1</code>')
              .replace(/\n/g,'<br>');
            rcScrollBottom();
          } else if (ev.type === 'working') {
            if (ev.data) {
              // Show thinking indicator (append after any text already shown)
              if (!assistEl.querySelector('.rc-thinking')) {
                const dot = document.createElement('div');
                dot.className = 'rc-thinking';
                dot.innerHTML = 'Pensando…';
                assistEl.appendChild(dot);
              }
              rcScrollBottom();
            } else {
              // Remove thinking indicator when done
              const dot = assistEl.querySelector('.rc-thinking');
              if (dot) dot.remove();
            }
          } else if (ev.type === 'error') {
            assistEl.className = 'rc-msg error';
            assistEl.textContent = ev.data;
          }
        } catch(_) {}
      }
    }

    assistEl.classList.add('done');

  } catch(e) {
    workEl.remove();
    if (e.name !== 'AbortError') {
      assistEl.className = 'rc-msg error';
      assistEl.textContent = e.message;
    }
  } finally {
    _rcStreaming = false;
    _rcAbortCtrl = null;
    if (sendBtn) sendBtn.disabled = false;
    rcScrollBottom();
  }
}
// ── FIN REPORT CHAT



// ── MÓDULO ENRASE (Completamiento de Tejido CUR) ─────────────────
// Consulta los linderos vía /api/linderos/{smp} y calcula si hay
// oportunidad de ganar pisos por enrase.

// Distritos donde el enrase aplica según CUR
const ENRASE_DISTRITOS = new Set([
  'USAA','USAA1','USAA2','USAM','USAM1','USAM2',
  'CM','CA','C1','C2','C3','R1','R2','R2A','R2B',
]);

// Distritos USAB donde NO aplica
const USAB_DISTRITOS = new Set([
  'USAB','USAB1','USAB2','USAB1I','USAB2I','R2BI','R2AI',
]);

async function calcularEnrase(parcel) {
  const resultado = {
    aplica: false,
    altura_lindero_max: null,
    plano_san: null,
    pisos_extra: 0,
    m2_extra: 0,
    distrito: null,
    mensaje: '',
    linderos: [],
  };

  if (!parcel?.smp) return resultado;

  // Si la DB dice que no aplica enrase, no consultar linderos
  const enraseFlag = parcel.edif_enrase || window._currentParcelData?.edif_enrase;
  if (enraseFlag === 0 || enraseFlag === false) {
    resultado.mensaje = 'No aplica enrase para esta parcela.';
    return resultado;
  }

  const smp     = parcel.smp;
  const cpu     = parcel.cpu || '';
  const planoSan = _planoSanitizado || parcel.plano || parcel.h || 0;
  const frente  = _frente || parcel.fr || 0;
  const fondo   = _fondo  || parcel.fo || 0;
  resultado.plano_san = planoSan;
  resultado.distrito  = cpu;

  // Verificar si el distrito permite enrase
  const cpuLimpio = cpu.toUpperCase().replace(/[^A-Z0-9]/g,'');
  const esUSAB = Array.from(USAB_DISTRITOS).some(d => cpuLimpio.includes(d.replace(/[^A-Z0-9]/g,'')));

  if (esUSAB) {
    resultado.mensaje = 'No aplica enrase: distrito USAB (altura libre limitada).';
    return resultado;
  }

  // Consultar alturas de linderos
  let data;
  try {
    const resp = await fetch(`/api/linderos/${encodeURIComponent(smp)}`);
    if (!resp.ok) throw new Error('Error ' + resp.status);
    data = await resp.json();
  } catch(e) {
    resultado.mensaje = 'No se pudieron consultar los linderos.';
    return resultado;
  }

  resultado.linderos = data.linderos || [];

  // Altura máxima de los linderos
  const alturas = resultado.linderos
    .map(l => l.tejido_altura_max || l.h || 0)
    .filter(a => a > 0);

  if (!alturas.length) {
    resultado.mensaje = 'Sin datos de altura en linderos disponibles.';
    return resultado;
  }

  const alturaLinderoPMax = Math.max(...alturas);
  resultado.altura_lindero_max = alturaLinderoPMax;

  // ¿El lindero supera el plano límite de la parcela?
  if (alturaLinderoPMax <= planoSan) {
    resultado.mensaje = `No aplica enrase: altura lindero (${alturaLinderoPMax}m) ≤ plano límite (${planoSan}m).`;
    return resultado;
  }

  // ── Cálculo de pisos y metros extra ──────────────────────────
  const deltaMts   = alturaLinderoPMax - planoSan;
  const pisosExtra = Math.floor(deltaMts / 2.8);

  if (pisosExtra <= 0) {
    resultado.mensaje = `Delta insuficiente (${deltaMts.toFixed(1)}m) para un piso completo.`;
    return resultado;
  }

  // Superficie de enrase: frente × min(fondo, 22m)
  const profEnrase  = Math.min(fondo > 0 ? fondo : 22, 22);
  const supEnrase   = frente > 0 ? frente * profEnrase : (parcel.area || 0) * 0.65;
  const m2Extra     = Math.round(supEnrase * pisosExtra * 0.82);

  resultado.aplica             = true;
  resultado.pisos_extra        = pisosExtra;
  resultado.m2_extra           = m2Extra;
  resultado.mensaje            = `Oportunidad de enrase detectada: +${pisosExtra} piso${pisosExtra>1?'s':''} para igualar lindero de ${alturaLinderoPMax}m.`;

  return resultado;
}

// Mostrar resultado de enrase en la UI
function mostrarEnrase(res) {
  const el = document.getElementById('enrase-resultado');
  if (!el) return;

  if (!res.aplica) {
    el.innerHTML = `
      <div class="enrase-no">
        <span class="enrase-icon">—</span>
        <span>${res.mensaje}</span>
      </div>`;
    return;
  }

  el.innerHTML = `
    <div class="enrase-si">
      <div class="enrase-row">
        <span class="enrase-label">Lindero más alto</span>
        <span class="enrase-val">${res.altura_lindero_max}m</span>
      </div>
      <div class="enrase-row">
        <span class="enrase-label">Tu plano límite</span>
        <span class="enrase-val">${res.plano_san}m</span>
      </div>
      <div class="enrase-row highlight">
        <span class="enrase-label">Pisos extra por enrase</span>
        <span class="enrase-val">+${res.pisos_extra} piso${res.pisos_extra>1?'s':''}</span>
      </div>
      <div class="enrase-row highlight">
        <span class="enrase-label">M² vendibles extra</span>
        <span class="enrase-val accent">+${res.m2_extra.toLocaleString('es-AR')} m²</span>
      </div>
      <div class="enrase-nota">
        Estimación basada en tejido fotogramétrico GCBA. Verificar linderos en Ciudad 3D.
      </div>
    </div>`;
}
// ── FIN MÓDULO ENRASE ─────────────────────────────────────────────




// ── CALCULADORA FACTIBILIDAD EN MODAL ────────────────────────────
// Precios de venta por barrio (USD/m²) — Zonaprop scraping 2025
const PRECIOS_BARRIO = {"Palermo": 3503, "Belgrano": 3439, "Núñez": 3634, "Recoleta": 3231, "Caballito": 2569, "Villa Urquiza": 2676, "Villa Devoto": 2767, "Villa Crespo": 2597, "Almagro": 2433, "Flores": 2146, "Saavedra": 2616, "Palermo Hollywood": 3277, "Colegiales": 2787, "Las Cañitas": 3789, "Puerto Madero": 5515, "Balvanera": 2157, "Barrio Norte": 2711, "Villa del Parque": 2488, "Coghlan": 2652, "Palermo Chico": 4650, "Palermo Soho": 3262, "Belgrano R": 3476, "Boedo": 2067, "Barracas": 2085, "Monserrat": 1971, "San Cristobal": 2039, "Villa Luro": 2244, "San Telmo": 2346, "Mataderos": 2040, "Villa Ortuzar": 2786, "Chacarita": 2535, "Villa Santa Rita": 2104, "Villa Pueyrredón": 2334, "Caballito Sur": 2937, "Congreso": 1938, "Retiro": 2995, "Liniers": 2161, "La Paternal": 2016, "Floresta": 1809, "Palermo Nuevo": 4091, "Belgrano C": 3617, "Centro / Microcentro": 2164, "San Nicolás": 2068, "Caballito Norte": 2899, "Constitución": 1582, "Villa Lugano": 2151, "Parque Chas": 2272, "Monte Castro": 2260, "Botánico": 3626, "Parque Patricios": 1955, "Palermo Viejo": 2880, "Parque Chacabuco": 2361, "Flores Norte, Flores": 1996, "Once": 1685, "Belgrano Chico": 3460, "Villa General Mitre": 2365, "Abasto": 2316, "Parque Centenario": 2305, "Barrio Chino": 2572, "Velez Sarsfield": 2040};

// Costo de construcción tiered por pisos (de feasibility.py)
function getCostoObra(pisos) {
  if (pisos <= 3)  return 1050;
  if (pisos <= 7)  return 1300;
  if (pisos <= 12) return 1550;
  return 1800;
}

// Valores por defecto — se sobreescriben al abrir el informe
let _fcDefaults = {
  m2Vendibles: 0,
  m2Totales: 0,
  costoObra: 1100,
  precioVenta: 2500,
  honorarios: 10,
  comerc: 5,
  margen: 20,
};

function initFeasCalc() {
  // Cargar indicadores macro en tiempo real
  fetchMacroIndicators();

  const pd     = window._currentParcelData;
  const barrio = pd?.barrio || '';
  const pisos  = window._pisosEstimados || 0;

  // M² vendibles: from DB precomputed values (source of truth)
  const parseLocale = s => parseFloat((s || '').replace(/\./g, '').replace(',', '.')) || 0;
  const m2v = _dbVendible || window._finMetrosVendibles || 0;

  // Costo obra tiered por pisos
  const costoObra = pisos > 0 ? getCostoObra(pisos) : 1100;

  // Precio de venta: buscar en tabla por barrio
  let precioVenta = 2500; // fallback
  if (barrio) {
    // Buscar match parcial en la tabla
    const key = Object.keys(PRECIOS_BARRIO).find(k =>
      barrio.toLowerCase().includes(k.toLowerCase()) ||
      k.toLowerCase().includes(barrio.toLowerCase())
    );
    if (key) precioVenta = PRECIOS_BARRIO[key];
  }

  // M² totales obra: from DB precomputed values (source of truth)
  const m2total = _dbVolumen || window._finMetrosTotales || 0;

  _fcDefaults = { m2Vendibles: m2v, m2Totales: m2total, costoObra, precioVenta, honorarios: 10, comerc: 5, margen: 20 };

  // Setear los inputs
  const setVal = (id, val) => { const el = document.getElementById(id); if (el) el.value = (val != null && val !== '') ? val : ''; };
  setVal('fc-m2-vendibles', Math.round(m2v) || '');
  setVal('fc-m2-totales',   Math.round(m2total) || '');
  setVal('fc-costo-obra',   costoObra);
  setVal('fc-precio-venta', precioVenta);
  setVal('fc-honorarios',   10);
  setVal('fc-comerc',        5);
  setVal('fc-margen',        20);

  // Hints
  const hintCosto = document.getElementById('fc-costo-hint');
  if (hintCosto && pisos > 0) hintCosto.textContent = `Estimado: ${pisos} pisos → USD ${costoObra}/m²`;

  const hintPrecio = document.getElementById('fc-precio-hint');
  if (hintPrecio) {
    const key = Object.keys(PRECIOS_BARRIO).find(k =>
      barrio.toLowerCase().includes(k.toLowerCase()) ||
      k.toLowerCase().includes(barrio.toLowerCase())
    );
    hintPrecio.textContent = key
      ? `Ref. ${key}: USD ${precioVenta}/m²`
      : 'Referencia general';
  }

  // Bindear eventos (event delegation en la sección)
  const section = document.querySelector('.frm-calc-section');
  if (section && !section._fcBound) {
    section.addEventListener('input', e => {
      if (!e.target.classList.contains('frm-calc-input')) return;
      recalcFeas();
      // Flash en el card de incidencia cuando cambia el margen deseado
      if (e.target.id === 'fc-margen') {
        const card = document.getElementById('fc-r-incidencia-card');
        if (card) {
          card.classList.remove('incid-flash');
          void card.offsetWidth; // reflow para reiniciar la animación
          card.classList.add('incid-flash');
        }
      }
    });
    section._fcBound = true;
  }

  // Botón reset
  const resetBtn = document.getElementById('frm-calc-reset-btn');
  if (resetBtn && !resetBtn._fcBound) {
    resetBtn.addEventListener('click', () => {
      setVal('fc-m2-vendibles', Math.round(_fcDefaults.m2Vendibles) || '');
      setVal('fc-m2-totales',   Math.round(_fcDefaults.m2Totales) || '');
      setVal('fc-costo-obra',   _fcDefaults.costoObra);
      setVal('fc-precio-venta', _fcDefaults.precioVenta);
      setVal('fc-honorarios',   _fcDefaults.honorarios);
      setVal('fc-comerc',        _fcDefaults.comerc);
      setVal('fc-margen',        _fcDefaults.margen);
      recalcFeas();
    });
    resetBtn._fcBound = true;
  }

  recalcFeas();
}

function recalcFeas() {
  const getN = id => parseFloat(document.getElementById(id)?.value) || 0;
  const m2v          = getN('fc-m2-vendibles');                        // m² vendibles (ingresos)
  const m2total      = getN('fc-m2-totales') || window._finMetrosTotales || m2v / 0.85; // m² totales obra
  const costoM2      = getN('fc-costo-obra');                          // USD/m² sobre m² TOTALES
  const precioM2     = getN('fc-precio-venta');                        // USD/m² vendible
  const honorPct     = getN('fc-honorarios') / 100;                   // % sobre costo de construcción
  const comercPct    = getN('fc-comerc') / 100;                        // % sobre ingresos brutos
  const margenDeseado = getN('fc-margen') / 100;                       // % de margen objetivo

  const fmtUSD = n => 'USD ' + Math.round(n).toLocaleString('es-AR');
  const set    = (id, val) => { const el = document.getElementById(id); if (el) el.innerText = val; };
  const setSub = (id, val) => { const el = document.getElementById(id); if (el) el.innerText = val; };

  if (!m2v || !costoM2) {
    ['fc-r-costo','fc-r-gdv','fc-r-ganancia','fc-r-incidencia'].forEach(id => set(id, '—'));
    return;
  }

  // 1. COSTO DE OBRA sobre m² totales construibles
  const costoObra    = m2total * costoM2;
  const honorarios   = costoObra * honorPct;

  // 2. INGRESOS BRUTOS sobre m² vendibles (sin descontar nada)
  const gdv          = precioM2 ? m2v * precioM2 : null;

  // 3. GASTOS COMERCIALIZACIÓN sobre ingresos brutos
  const gastosVenta  = gdv ? gdv * comercPct : 0;

  // 4. COSTO TOTAL = Obra + Honorarios + Comercialización
  const costoTotal   = costoObra + honorarios + gastosVenta;

  // 5. GANANCIA BRUTA = Ingresos Brutos - Costo Total
  const ganancia     = gdv != null ? gdv - costoTotal : null;
  const margenPct    = gdv ? (ganancia / gdv * 100) : null;

  // 6. INCIDENCIA MÁX = (Ingresos - Costo Total - Margen Deseado sobre Ingresos) / m² Vendibles
  const incidMax     = gdv ? Math.max(0, (gdv - costoTotal - gdv * margenDeseado) / m2v) : null;

  // Mostrar resultados

  // Cuadro 1: COSTO TOTAL DE PROYECTO = Obra + Honorarios + Comercialización
  set('fc-r-costo', fmtUSD(costoTotal));
  setSub('fc-r-costo-sub',
    `Obra ${fmtUSD(costoObra)} + Hon. ${fmtUSD(honorarios)} + Comerc. ${fmtUSD(gastosVenta)}`);

  // Cuadro 2: Ingresos = m² vendibles × precio (sin deducciones)
  set('fc-r-gdv', gdv ? fmtUSD(gdv) : '—');
  setSub('fc-r-gdv-sub', gdv
    ? `${Math.round(m2v).toLocaleString('es-AR')} m² vendibles × USD ${Math.round(precioM2)}`
    : 'Ingresá precio de venta');

  // Cuadro 3: Ganancia = Ingresos - Costo Total
  set('fc-r-ganancia', ganancia != null ? fmtUSD(ganancia) : '—');
  setSub('fc-r-ganancia-sub', margenPct != null ? `Margen sobre ingresos: ${margenPct.toFixed(1)}%` : '');

  // Cuadro 4: Incidencia máxima del terreno + sub dinámico
  const margenPctDisplay = Math.round(margenDeseado * 100);
  set('fc-r-incidencia', incidMax != null ? fmtUSD(incidMax) : '—');
  setSub('fc-r-incidencia-sub', `USD/m² vendible · Valor sugerido para mantener un margen del ${margenPctDisplay}%`);
}
// ── FIN CALCULADORA FACTIBILIDAD ──────────────────────────────────


// ── INDICADORES MACRO EN VIVO ─────────────────────────────────────
const FC_FALLBACK_DOLAR = 1430;
const FC_FALLBACK_UVA   = 1908;

let _fcDolarBlue = FC_FALLBACK_DOLAR;
let _fcUVA       = FC_FALLBACK_UVA;
let _fcMacroLive = false;

async function fetchMacroIndicators() {
  const fmtARS = n => '$' + Math.round(n).toLocaleString('es-AR');
  const setDolar = (val, live) => {
    const el = document.getElementById('fc-dolar-val');
    if (el) el.textContent = fmtARS(val);
    _fcDolarBlue = val;
  };
  const setUVA = (val) => {
    const el = document.getElementById('fc-uva-val');
    if (el) el.textContent = fmtARS(val);
    _fcUVA = val;
  };
  const setStatus = (live, msg) => {
    const dot   = document.getElementById('fc-live-dot');
    const label = document.getElementById('fc-live-label');
    if (dot)   dot.className   = 'frm-macro-dot' + (live ? '' : ' manual');
    if (label) label.textContent = msg;
    _fcMacroLive = live;
  };

  // Mostrar fallback mientras carga
  setDolar(FC_FALLBACK_DOLAR, false);
  setUVA(FC_FALLBACK_UVA);
  setStatus(false, 'CARGANDO...');

  try {
    const [rDolar, rUVA] = await Promise.allSettled([
      fetch('https://dolarapi.com/v1/dolares/blue'),
      fetch('https://api.argentinadatos.com/v1/finanzas/indices/uva').then(r => {
        if (!r.ok) throw new Error(r.status);
        return r.json().then(arr => new Response(JSON.stringify(arr[arr.length - 1])));
      }),
    ]);

    let dolarOk = false, uvaOk = false;

    if (rDolar.status === 'fulfilled' && rDolar.value.ok) {
      const d = await rDolar.value.json();
      const venta = d.venta || d.value || FC_FALLBACK_DOLAR;
      setDolar(venta, true);
      dolarOk = true;
    }

    if (rUVA.status === 'fulfilled' && rUVA.value.ok) {
      const d = await rUVA.value.json();
      const val = d.valor || d.value || d.venta || FC_FALLBACK_UVA;
      setUVA(val);
      uvaOk = true;
    }

    if (dolarOk && uvaOk) {
      setStatus(true, 'EN VIVO');
    } else if (dolarOk || uvaOk) {
      setStatus(true, 'EN VIVO');
    } else {
      setStatus(false, 'REF. MANUAL');
    }

  } catch(e) {
    setStatus(false, 'VALORES DE REFERENCIA');
  }
}

// Cuando el usuario modifica el dólar manualmente (no hay campo directo,
// pero si modifica los inputs de costo/precio, el indicador sigue EN VIVO)
// Si en el futuro se agrega un input de dólar, bindearlo aquí.
// ── FIN INDICADORES MACRO ─────────────────────────────────────────
