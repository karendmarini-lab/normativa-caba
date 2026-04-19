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
let _pisosEstimados = 1;
let _planoSanitizado = 0;
let _frente = 0;
let _fondo = 0;

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
    const parcel = await findParcel(lat, lng);

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

async function findParcel(lat, lng) {
  const resp = await fetch(`/api/parcela_nearest?lat=${lat}&lng=${lng}`);
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
  // When clicking a parcel on the map (from heatmap view), show its card
  const card = $('parcelCard');
  if (!card) return;

  card.classList.add('visible');
  $('parcelTitle').textContent = props.dir || props.smp;

  // Badges
  const badges = [];
  if (props.aph || (props.catalogacion && props.catalogacion !== 'DESESTIMADO'))
    badges.push(['APH ' + (props.catalogacion || ''), 'rgba(239,68,68,.2)', '#ef4444']);
  if (props.riesgo)
    badges.push(['Riesgo hídrico', 'rgba(59,130,246,.2)', '#3b82f6']);
  if (props.enrase)
    badges.push(['Enrase', 'rgba(168,85,247,.2)', '#a855f7']);

  $('parcelSub').innerHTML =
    [props.smp, props.cpu, props.barrio].filter(Boolean).join(' · ')
    + badges.map(([t, bg, c]) =>
      ` <span style="background:${bg};color:${c};padding:1px 6px;border-radius:4px;font-size:10px">${t}</span>`
    ).join('');

  // Metrics grid
  const fmtM2 = v => v ? Math.round(v).toLocaleString('es-AR') + ' m²' : '-';
  const items = [
    ['m² vendibles', fmtM2(props.vendible)],
    ['Pisos permitidos', props.pisos || '-'],
    ['FOT', props.fot || '-'],
    ['PL', props.pl ? props.pl + 'm' : '-'],
    ['Lote', fmtM2(props.area)],
    ['Frente', props.fr ? props.fr + 'm' : '-'],
    ['Fondo', props.fo ? props.fo + 'm' : '-'],
    ['Uso', props.uso || '-'],
    ['Delta', props.tj ? (props.pl - props.tj).toFixed(1) + 'm' : '-'],
    ['Tejido', props.tj ? props.tj + 'm' : '-'],
  ];
  $('parcelGrid').innerHTML = items.map(([l, v]) =>
    `<div class="parcel-metric"><div class="parcel-metric-label">${l}</div><div class="parcel-metric-value">${v}</div></div>`
  ).join('');

  // Links
  $('parcelLink').href = `https://ciudad3d.buenosaires.gob.ar/?smp=${encodeURIComponent(props.smp)}`;

  // Send parcel card to chat (0 LLM tokens)
  const chatCard = addParcelCard(props);

  // Fetch extra doc links and add to both panels
  fetch(`/api/parcela/${encodeURIComponent(props.smp)}`)
    .then(r => r.ok ? r.json() : null)
    .then(data => {
      if (!data) return;
      const links = [];
      if (data.edif_croquis_url) links.push(['Croquis', data.edif_croquis_url]);
      if (data.edif_plano_indice_url) links.push(['Plano índice', data.edif_plano_indice_url]);
      if (data.edif_perimetro_url) links.push(['Perímetro', data.edif_perimetro_url]);
      links.push(['Ciudad 3D', `https://ciudad3d.buenosaires.gob.ar/?smp=${props.smp}`]);
      // Add to left panel (when visible)
      const docsInner = $('parcelDocsInner');
      if (docsInner) {
        docsInner.innerHTML = links.map(([label, url]) =>
          `<a href="${url}" target="_blank" style="color:var(--accent);text-decoration:none;font-size:11px">${label} ↗</a>`
        ).join('');
        $('parcelDocs').style.display = 'block';
      }
      // Add to chat card (croquis embedded + doc links)
      addParcelDocs(chatCard, links, data.edif_croquis_url);
    }).catch(() => {});
}

// ── Calculator ───────────────────────────────────────────────────

function setupCalculator(parcel, planoSan) {
  const cb = $('calc-block');
  const area = parcel?.area || 0;

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
  let pisadaCalc, bandaLabel;

  if (fr > 0 && fo > 0) {
    if (fo <= 16) {
      pisadaCalc = Math.min(Math.round(fr * fo), Math.round(area));
      bandaLabel = `${fr.toFixed(2)} x ${fo.toFixed(2)} m (100%)`;
    } else {
      pisadaCalc = Math.min(Math.round(fr * 22), Math.round(area));
      bandaLabel = `${fr.toFixed(2)} x 22 m (LFI)`;
    }
  } else {
    pisadaCalc = Math.round(area * 0.65);
    bandaLabel = 'estimado 65%';
  }

  $('c-sup').value = Math.round(area);
  $('c-pb').value = pisadaCalc;

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
  if (lblPisada) lblPisada.textContent = modoAtipica ? '⬡ Dato Oficial: Manzana Atípica' : '✦ Calculado: LFI a 22m';

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

  // Dynamic efficiency
  let eficiencia = 0.85;
  if (profEdificio <= 13) eficiencia = 0.88;
  else if (profEdificio > 18) eficiencia = 0.82;

  const vendibleCubierto = volumen * eficiencia;

  // Balconies
  const anchoBalcon = Math.max(0, (_frente || 0) - 1.20);
  let totalBalcones = 0;
  if (_pisosEstimados > 1 && anchoBalcon > 0) {
    totalBalcones = (anchoBalcon * 1.20) * 2 * (_pisosEstimados - 1);
  }

  const vendibleTotal = vendibleCubierto + totalBalcones;

  // Display
  $('c-edif').textContent = fmt(volumen);
  const unitEdif = $('c-edif')?.closest('.citem')?.querySelector('.cunit');
  if (unitEdif && _frente > 0) unitEdif.textContent = `prof. edif. ${profEdificio.toFixed(1)}m`;

  const cvendEl = $('c-vend');
  if (cvendEl) {
    cvendEl.innerHTML =
      '<div style="font-size:11px;color:var(--muted);margin-bottom:2px">Vendible cubierto</div>' +
      `<div style="font-size:20px;font-weight:400">${fmt(vendibleCubierto)} <span style="font-size:10px">m²</span></div>` +
      `<div style="font-size:10px;color:var(--muted);margin-bottom:8px">Eficiencia: ${Math.round(eficiencia * 100)}%</div>` +
      '<div style="font-size:11px;color:var(--muted);margin-bottom:2px">Balcones (semicubierto)</div>' +
      `<div style="font-size:20px;font-weight:400">${fmt(totalBalcones)} <span style="font-size:10px">m²</span></div>` +
      '<div style="border-top:1px solid var(--border);margin:8px 0"></div>' +
      '<div style="font-size:11px;color:var(--muted);margin-bottom:2px">Total vendible</div>' +
      `<div style="font-size:24px;font-weight:500">${fmt(vendibleTotal)} <span style="font-size:11px">m²</span></div>`;
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

  // Lee valores ya renderizados — NO recalcula nada
  const get    = id => document.getElementById(id)?.textContent?.trim() || '—';
  const getVal = id => document.getElementById(id)?.value?.trim() || '—';
  const set    = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };

  // A: Dirección + badge
  set('frm-address', get('res-addr'));
  set('frm-badge',   get('res-badge'));

  // B: Total vendible — parsear el c-vend que tiene sub-divs
  const cvendEl = document.getElementById('c-vend');
  if (cvendEl) {
    const cubEl  = cvendEl.querySelector('[data-frm="cub"]');
    const balcEl = cvendEl.querySelector('[data-frm="balc"]');
    const totEl  = cvendEl.querySelector('[data-frm="total"]');
    const efEl   = cvendEl.querySelector('[data-frm="ef"]');
    // Números limpios (sin "m²")
    const clean  = str => str?.replace(/m²|m2/gi,'').trim() || '—';
    set('frm-total-vend', clean(totEl  ? totEl.textContent  : cvendEl.textContent));
    set('frm-vend-cub',   clean(cubEl  ? cubEl.textContent  : '—'));
    set('frm-balcones',   clean(balcEl ? balcEl.textContent : '—'));
    set('frm-eficiencia', efEl ? 'Eficiencia ' + efEl.textContent : '');
  } else {
    set('frm-total-vend', get('c-vend').replace(/m²|m2/gi,'').trim());
  }

  // B: Volumen
  set('frm-volumen', get('c-edif').replace(/m²|m2/gi,'').trim());

  // C: Parámetros normativos
  set('frm-altura',   get('res-alt'));
  set('frm-plano',    get('res-plano'));
  set('frm-pisos',    get('res-pis'));
  set('frm-distrito', get('res-dis') || get('res-badge'));
  set('frm-lote',     getVal('c-sup'));
  set('frm-pisada',   getVal('c-pb'));

  modal.classList.add('open');
  document.body.style.overflow = 'hidden';
}

function closeFullReport() {
  const modal = document.getElementById('full-report-modal');
  if (modal) modal.classList.remove('open');
  document.body.style.overflow = '';
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
