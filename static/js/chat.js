/**
 * chat.js — Chat interface for EdificIA.
 *
 * Display modes:
 *   - hidden:     chat closed, left panel (filters) visible
 *   - sidebar:    chat replaces left panel (~340px)
 *   - fullscreen: chat takes full screen
 *
 * Features:
 *   - Model selector (haiku / sonnet / opus)
 *   - SSE streaming from POST /api/chat
 *   - Markdown rendering (marked.js + DOMPurify)
 *   - HTML report artifacts with download (HTML/PDF)
 *   - Programmatic report messages (map clicks, barrio changes)
 */

// ── State ────────────────────────────────────────────────────────

let _mode = 'hidden';
let _model = 'haiku';
let _sessionId = crypto.randomUUID();
let _streaming = false;
let _abortController = null;
let _userPlan = null;
let _pendingContext = [];  // programmatic messages since last user query

// ── DOM refs ─────────────────────────────────────────────────────

let _chatContainer, _messagesEl, _inputEl, _modelSelect, _historyPanel;
let _historyOpen = false;

// ── Init ─────────────────────────────────────────────────────────

export function initChat() {
  _buildDOM();
  _bindKeys();
  _listenIframeResize();
  _loadUserPlan();

  // Bind the static header toggle button
  const toggle = document.getElementById('chat-toggle');
  if (toggle) toggle.addEventListener('click', () => setChatMode(_mode === 'hidden' ? 'sidebar' : 'hidden'));
}

function _listenIframeResize() {
  window.addEventListener('message', (e) => {
    if (e.data?.type !== 'iframe-resize' || typeof e.data.height !== 'number') return;
    const iframes = document.querySelectorAll('#chat-messages iframe');
    for (const iframe of iframes) {
      if (iframe.contentWindow === e.source) {
        iframe.style.height = Math.min(e.data.height + 2, 600) + 'px';
        break;
      }
    }
  });
}

function _buildDOM() {
  _chatContainer = document.createElement('div');
  _chatContainer.id = 'chat-container';
  _chatContainer.innerHTML = `
    <div id="chat-header">
      <div style="display:flex;align-items:center;gap:10px">
        <button id="chat-history-btn" title="Historial" style="display:none;background:none;border:none;color:rgba(255,255,255,.4);font-size:16px;cursor:pointer;padding:4px">☰</button>
        <span style="font-size:10px;letter-spacing:3px;text-transform:uppercase;color:rgba(255,255,255,.4)">EdificIA Chat</span>
        <select id="chat-model" style="background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);
          border-radius:6px;color:#fff;font:inherit;font-size:11px;padding:4px 8px;cursor:pointer;outline:none">
          <option value="haiku" selected>Haiku</option>
          <option value="sonnet">Sonnet</option>
          <option value="opus">Opus</option>
        </select>
      </div>
      <div style="display:flex;gap:6px">
        <button id="chat-new" title="Nueva sesión" style="background:none;border:none;color:rgba(255,255,255,.4);font-size:16px;cursor:pointer;padding:4px">+</button>
        <button id="chat-expand" title="Pantalla completa" style="background:none;border:none;color:rgba(255,255,255,.4);font-size:14px;cursor:pointer;padding:4px">⛶</button>
        <button id="chat-close" title="Cerrar" style="background:none;border:none;color:rgba(255,255,255,.4);font-size:16px;cursor:pointer;padding:4px">×</button>
      </div>
    </div>
    <div id="chat-body">
      <div id="chat-history" style="display:none">
        <div id="history-tabs">
          <button class="htab active" data-tab="sessions">Historial</button>
        </div>
        <div id="history-list"></div>
      </div>
      <div id="chat-main">
        <div id="chat-messages"></div>
        <div id="chat-input-wrap">
          <div id="chat-input-bar">
            <input type="text" id="chat-input" placeholder="Preguntá sobre normativa, parcelas, zonificación…" autocomplete="off">
            <button id="chat-send">↑</button>
          </div>
        </div>
      </div>
    </div>
  `;
  document.body.appendChild(_chatContainer);

  _messagesEl = document.getElementById('chat-messages');
  _inputEl = document.getElementById('chat-input');
  _modelSelect = document.getElementById('chat-model');
  _historyPanel = document.getElementById('chat-history');

  _modelSelect.addEventListener('change', () => { _model = _modelSelect.value; });
  document.getElementById('chat-close').addEventListener('click', () => setChatMode('hidden'));
  document.getElementById('chat-expand').addEventListener('click', () => {
    setChatMode(_mode === 'fullscreen' ? 'sidebar' : 'fullscreen');
  });
  document.getElementById('chat-new').addEventListener('click', newSession);
  document.getElementById('chat-send').addEventListener('click', _onSend);
  _inputEl.addEventListener('keydown', e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); _onSend(); } });

  // History toggle (only in fullscreen)
  document.getElementById('chat-history-btn').addEventListener('click', _toggleHistory);
  document.querySelectorAll('.htab').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.htab').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      _loadHistoryTab(btn.dataset.tab);
    });
  });

  _applyStyles();
}

async function _loadUserPlan() {
  try {
    const r = await fetch('/api/auth/plan');
    if (!r.ok) return;
    _userPlan = await r.json();
    _updateModelSelector();
  } catch { /* not logged in or no auth */ }
}

function _updateModelSelector() {
  const allowed = _userPlan?.modelos_habilitados || ['haiku'];
  const models = [
    { id: 'haiku', label: 'Haiku' },
    { id: 'sonnet', label: 'Sonnet' },
    { id: 'opus', label: 'Opus' },
  ];
  _modelSelect.innerHTML = models.map(m => {
    const locked = !allowed.includes(m.id);
    const title = locked ? `Comunicate con el equipo de EdificIA para tener acceso a ${m.label}` : '';
    return `<option value="${m.id}" ${locked ? 'disabled' : ''} ${title ? `title="${title}"` : ''}>${locked ? '\u{1F512} ' : ''}${m.label}</option>`;
  }).join('');
  if (!allowed.includes(_model)) {
    _model = allowed[0] || 'haiku';
  }
  _modelSelect.value = _model;
}

function _bindKeys() {
  document.addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
      e.preventDefault();
      setChatMode(_mode === 'hidden' ? 'sidebar' : 'hidden');
    }
    if (e.key === 'Escape' && _mode !== 'hidden') {
      if (_streaming && _abortController) {
        _abortController.abort();
      } else {
        setChatMode('hidden');
      }
    }
  });
}

// ── Mode management ──────────────────────────────────────────────

export function setChatMode(mode) {
  _mode = mode;
  const c = _chatContainer;
  const leftPanel = document.getElementById('leftPanel');
  const histBtn = document.getElementById('chat-history-btn');
  const toggleBtn = document.getElementById('chat-toggle');

  c.className = '';
  if (mode === 'hidden') {
    c.style.display = 'none';
    if (leftPanel) leftPanel.style.display = '';
    histBtn.style.display = 'none';
    _historyPanel.style.display = 'none';
    _historyOpen = false;
    // Reappear the access button
    if (toggleBtn) toggleBtn.classList.remove('hidden');
  } else if (mode === 'sidebar') {
    c.style.display = 'flex';
    c.classList.add('chat-sidebar');
    if (leftPanel) leftPanel.style.display = 'none';
    histBtn.style.display = 'none';
    _historyPanel.style.display = 'none';
    _historyOpen = false;
    _inputEl.focus();
    // Hide the access button so it doesn't overlap the input
    if (toggleBtn) toggleBtn.classList.add('hidden');
  } else if (mode === 'fullscreen') {
    c.style.display = 'flex';
    c.classList.add('chat-fullscreen');
    if (leftPanel) leftPanel.style.display = 'none';
    histBtn.style.display = '';
    _inputEl.focus();
    // Hide the access button in fullscreen too
    if (toggleBtn) toggleBtn.classList.add('hidden');
  }
}

export function getChatMode() { return _mode; }

// ── Send message ─────────────────────────────────────────────────

async function _onSend() {
  const text = _inputEl.value.trim();
  if (!text || _streaming) return;

  _inputEl.value = '';
  _renderUserMessage(text);
  _streaming = true;
  _abortController = new AbortController();

  // Prepend programmatic context (parcel clicks, barrio changes) to the message
  let agentMessage = text;
  if (_pendingContext.length) {
    agentMessage = _pendingContext.join('\n') + '\n\n' + text;
    _pendingContext = [];
  }

  const assistantId = 'msg-' + Date.now();
  const updater = _renderAssistantMessage(assistantId);

  try {
    const resp = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: _sessionId, message: agentMessage, model: _model }),
      signal: _abortController.signal,
    });

    if (!resp.ok) {
      updater.finish();
      _renderError(`Error ${resp.status}: ${resp.statusText}`);
      return;
    }

    const reader = resp.body.getReader();
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
          const event = JSON.parse(line.slice(6));
          _handleSSEEvent(event, updater);
        } catch { /* skip malformed */ }
      }
    }

    updater.finish();
  } catch (e) {
    updater.finish();
    if (e.name !== 'AbortError') _renderError(e.message);
  } finally {
    _streaming = false;
    _abortController = null;
  }
}

function _handleSSEEvent(event, updater) {
  switch (event.type) {
    case 'text':
      updater.append(event.data);
      break;
    case 'working':
      updater.setWorking(event.data);
      break;
    case 'artifact':
      _renderReport(event.data.title, event.data.html, !!event.data.collapsed);
      break;
    case 'error':
      _renderError(event.data);
      break;
    case 'done':
      break;
  }
}

// ── Programmatic messages (map interactions) ─────────────────────

/**
 * Add a report to the chat timeline without calling the LLM.
 * Used for parcel clicks, barrio selections, etc.
 * Also persists to backend via POST /api/chat/entries.
 */
export function addReport(title, html) {
  if (_mode === 'hidden') setChatMode('sidebar');
  _renderReport(title, html);
  fetch('/api/chat/entries', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      session_id: _sessionId,
      kind: 'report',
      content: JSON.stringify({ title, html, source: 'map', size: html.length }),
    }),
  }).catch(() => {});
}

/**
 * Add a compact parcel card to the chat (no iframe, just a styled div).
 */
export function addParcelCard(props) {
  if (_mode === 'hidden') setChatMode('sidebar');
  const delta = props.tj ? (props.pl - props.tj).toFixed(1) : null;
  const fmt = v => v ? Math.round(v).toLocaleString('es-AR') : null;

  const badges = [];
  if (props.aph || (props.catalogacion && props.catalogacion !== 'DESESTIMADO'))
    badges.push(`<span class="cpc-badge" style="color:#ef4444;border-color:rgba(239,68,68,.3)">APH</span>`);
  if (props.riesgo)
    badges.push(`<span class="cpc-badge" style="color:#3b82f6;border-color:rgba(59,130,246,.3)">Riesgo hídrico</span>`);
  if (props.enrase)
    badges.push(`<span class="cpc-badge" style="color:#a855f7;border-color:rgba(168,85,247,.3)">Enrase</span>`);

  const items = [
    props.pl && `PL <b>${props.pl}m</b>`,
    props.pisos && `Pisos <b>${props.pisos}</b>`,
    props.pisos_actual && `Actual <b>${props.pisos_actual}p</b>`,
    delta && `Delta <b>${delta}m</b>`,
    props.fot && `FOT <b>${props.fot}</b>`,
    fmt(props.vol) && `Vol <b>${fmt(props.vol)} m³</b>`,
    fmt(props.vendible) && `Vendible <b>${fmt(props.vendible)} m²</b>`,
    fmt(props.area) && `Lote <b>${fmt(props.area)} m²</b>`,
    props.fr && `Frente <b>${props.fr}m</b>`,
    props.fo && `Fondo <b>${props.fo}m</b>`,
    props.tj && `Tejido <b>${props.tj}m</b>`,
    props.uso && `Uso <b>${_escapeHtml(props.uso)}</b>`,
    props.plusvalia_uva && `Plusvalía <b>${props.plusvalia_uva} UVA</b>`,
    props.plusvalia_alic && `Alícuota <b>${props.plusvalia_alic}%</b>`,
  ].filter(Boolean).map(s => `<span>${s}</span>`).join('');

  // Track full data for agent context
  const fields = [
    `SMP: ${props.smp}`,
    props.dir && `Dirección: ${props.dir}`,
    props.barrio && `Barrio: ${props.barrio}`,
    props.cpu && `CPU: ${props.cpu}`,
    props.pl && `Plano Límite: ${props.pl}m`,
    props.pisos && `Pisos permitidos: ${props.pisos}`,
    props.pisos_actual && `Pisos construidos: ${props.pisos_actual}`,
    delta && `Delta: ${delta}m`,
    props.fot && `FOT: ${props.fot}`,
    props.vol && `Volumen edificable: ${Math.round(props.vol)} m³`,
    props.vendible && `Sup vendible: ${Math.round(props.vendible)} m²`,
    props.area && `Área lote: ${Math.round(props.area)} m²`,
    props.fr && `Frente: ${props.fr}m`,
    props.fo && `Fondo: ${props.fo}m`,
    props.tj && `Tejido (altura real): ${props.tj}m`,
    props.uso && `Uso: ${props.uso}`,
    props.uso2 && `Uso 2: ${props.uso2}`,
    props.plusvalia_uva && `Plusvalía incidencia: ${props.plusvalia_uva} UVA`,
    props.plusvalia_alic && `Plusvalía alícuota: ${props.plusvalia_alic}%`,
    props.aph && `APH: sí`,
    props.catalogacion && `Catalogación: ${props.catalogacion}`,
    props.riesgo && `Riesgo hídrico: sí`,
    props.enrase && `Enrase: sí`,
  ].filter(Boolean).join('\n');
  _pendingContext.push(`[Parcela seleccionada]\n${fields}`);

  const el = document.createElement('div');
  el.className = 'chat-parcel-card';
  el.innerHTML = `
    <div class="cpc-header">
      <div class="cpc-title">${_escapeHtml(props.smp)}${props.dir ? ' · ' + _escapeHtml(props.dir) : ''}</div>
      <a href="https://ciudad3d.buenosaires.gob.ar/?smp=${encodeURIComponent(props.smp)}" target="_blank" class="cpc-link">3D ↗</a>
    </div>
    <div class="cpc-sub">${[props.barrio, props.cpu].filter(Boolean).join(' · ')}${badges.length ? ' ' + badges.join('') : ''}</div>
    <div class="cpc-grid">${items}</div>
  `;
  _messagesEl.appendChild(el);
  _scrollToBottom();
  fetch('/api/chat/entries', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      session_id: _sessionId, kind: 'report',
      content: JSON.stringify({ title: `Parcela ${props.smp}`, source: 'map', props }),
    }),
  }).catch(() => {});
  return el;
}

/**
 * Append doc links to an existing parcel card element.
 */
export function addParcelDocs(cardEl, links, croquisUrl) {
  if (!cardEl) return;
  // Embed croquis PDF directly
  if (croquisUrl) {
    const croquisDiv = document.createElement('div');
    croquisDiv.className = 'cpc-croquis';
    croquisDiv.innerHTML = `<iframe src="${_escapeHtml(croquisUrl)}" style="width:100%;height:280px;border:1px solid rgba(255,255,255,.08);border-radius:6px;background:#fff"></iframe>`;
    cardEl.appendChild(croquisDiv);
  }
  // Doc links
  if (links?.length) {
    const docsDiv = document.createElement('div');
    docsDiv.className = 'cpc-docs';
    docsDiv.innerHTML = links.map(([label, url]) =>
      `<a href="${_escapeHtml(url)}" target="_blank">${_escapeHtml(label)} ↗</a>`
    ).join('');
    cardEl.appendChild(docsDiv);
  }
}

/**
 * Add a short info message to the chat (e.g., barrio change).
 */
export function addInfoMessage(text) {
  if (_mode === 'hidden') return;
  _pendingContext.push(`[${text}]`);
  const el = document.createElement('div');
  el.className = 'chat-msg chat-msg-info';
  el.textContent = text;
  _messagesEl.appendChild(el);
  _scrollToBottom();
  fetch('/api/chat/entries', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ session_id: _sessionId, kind: 'info', content: text }),
  }).catch(() => {});
}

// ── Message rendering ────────────────────────────────────────────

function _escapeHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function _renderUserMessage(text) {
  const el = document.createElement('div');
  el.className = 'chat-msg chat-msg-user';
  el.textContent = text;
  _messagesEl.appendChild(el);
  _scrollToBottom();
}

function _renderAssistantMessage(id) {
  const el = document.createElement('div');
  el.className = 'chat-msg chat-msg-assistant';
  el.id = id;
  _messagesEl.appendChild(el);

  const workingEl = document.createElement('div');
  workingEl.className = 'chat-msg chat-msg-working';
  workingEl.textContent = 'Pensando...';
  workingEl.style.display = 'none';
  _messagesEl.appendChild(workingEl);

  let accumulated = '';

  return {
    append(text) {
      accumulated += text;
      if (window.marked) {
        const parsed = window.marked.parse(accumulated);
        el.innerHTML = window.DOMPurify ? window.DOMPurify.sanitize(parsed) : parsed;
      } else {
        el.innerHTML = accumulated
          .replace(/&/g, '&amp;').replace(/</g, '&lt;')
          .replace(/`([^`]+)`/g, '<code>$1</code>')
          .replace(/\*\*([^*]+)\*\*/g, '<b>$1</b>')
          .replace(/\n/g, '<br>');
      }
      _scrollToBottom();
    },
    setWorking(isWorking) {
      workingEl.style.display = isWorking ? 'block' : 'none';
      _scrollToBottom();
    },
    finish() {
      el.classList.add('done');
      workingEl.remove();
    },
  };
}

function _renderReport(title, html, startCollapsed = false) {
  const fmtSize = b => b > 1024 ? (b / 1024).toFixed(1) + ' KB' : b + ' B';
  const wrapper = document.createElement('div');
  wrapper.className = 'chat-report';

  const headerEl = document.createElement('div');
  headerEl.className = 'cr-header';
  headerEl.innerHTML = `
    <div style="display:flex;align-items:center;gap:8px;flex:1;min-width:0">
      <span class="cr-toggle" style="cursor:pointer">${startCollapsed ? '▸' : '▾'}</span>
      <span class="cr-title">${_escapeHtml(title)}</span>
      <span style="font-size:10px;color:rgba(255,255,255,.25)">${fmtSize(new Blob([html]).size)}</span>
    </div>
    <div style="display:flex;gap:4px">
      <button class="art-dl-btn" data-fmt="html">HTML</button>
    </div>
  `;
  wrapper.appendChild(headerEl);

  const bodyEl = document.createElement('div');
  bodyEl.className = 'cr-body';
  bodyEl.style.display = startCollapsed ? 'none' : 'block';
  const artId = 'art-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
  bodyEl.innerHTML = `<iframe id="${artId}" sandbox="allow-scripts allow-modals" srcdoc="${html.replace(/"/g, '&quot;')}" style="width:100%;min-height:60px;height:60px;border:1px solid rgba(255,255,255,.08);border-radius:8px;background:#0a0a0a;transition:height .2s"></iframe>`;
  wrapper.appendChild(bodyEl);

  // Toggle expand/collapse — entire header is clickable
  const toggleFn = (e) => {
    if (e.target.closest('.art-dl-btn')) return; // don't toggle on download click
    const toggle = headerEl.querySelector('.cr-toggle');
    const visible = bodyEl.style.display !== 'none';
    bodyEl.style.display = visible ? 'none' : 'block';
    toggle.textContent = visible ? '▸' : '▾';
  };
  headerEl.addEventListener('click', toggleFn);

  // Download
  const dlName = title.replace(/[^a-zA-Z0-9áéíóúñ _-]/gi, '_').slice(0, 60) + '.html';
  headerEl.querySelector('[data-fmt="html"]').addEventListener('click', () => {
    const blob = new Blob([html], { type: 'text/html' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = dlName;
    a.click();
    URL.revokeObjectURL(a.href);
  });
  _messagesEl.appendChild(wrapper);
  _scrollToBottom();
}

function _renderError(message) {
  const el = document.createElement('div');
  el.className = 'chat-msg chat-msg-error';
  el.textContent = message;
  _messagesEl.appendChild(el);
  _scrollToBottom();
}

function _scrollToBottom() {
  _messagesEl.scrollTop = _messagesEl.scrollHeight;
}

// ── History panel (fullscreen only) ──────────────────────────────

function _toggleHistory() {
  _historyOpen = !_historyOpen;
  _historyPanel.style.display = _historyOpen ? 'flex' : 'none';
  if (_historyOpen) _loadHistoryTab('sessions');
}

async function _loadHistoryTab(tab) {
  const list = document.getElementById('history-list');
  list.innerHTML = '<div style="color:rgba(255,255,255,.3);padding:16px;font-size:12px">Cargando...</div>';

  try {
    const resp = await fetch('/api/chat/sessions');
    if (!resp.ok) throw new Error(resp.statusText);
    const sessions = await resp.json();

    _renderSessionsList(list, sessions);
  } catch {
    list.innerHTML = '<div style="color:#f87171;padding:16px;font-size:12px">Error cargando historial</div>';
  }
}

function _renderSessionsList(container, sessions) {
  if (!sessions.length) {
    container.innerHTML = '<div style="color:rgba(255,255,255,.3);padding:16px;font-size:12px">Sin conversaciones</div>';
    return;
  }
  const fmtTime = ts => new Date(ts * 1000).toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit' });
  const fmtDate = ts => new Date(ts * 1000).toLocaleDateString('es-AR', { day: 'numeric', month: 'short' });

  container.innerHTML = sessions.map(s => `
    <div class="hist-session" data-id="${s.id}">
      <div class="hist-preview">${_escapeHtml(s.preview || '...')}</div>
      <div class="hist-meta">${fmtDate(s.created_at)} · ${fmtTime(s.created_at)}</div>
    </div>
  `).join('');

  container.querySelectorAll('.hist-session').forEach(el => {
    el.addEventListener('click', () => _loadSession(el.dataset.id));
  });
}

async function _loadSession(sessionId) {
  try {
    const resp = await fetch(`/api/chat/sessions/${sessionId}`);
    if (!resp.ok) return;
    const data = await resp.json();

    // Show entries read-only (keep history open)
    _messagesEl.innerHTML = '';

    for (const entry of data.entries) {
      if (entry.kind === 'user') {
        _renderUserMessage(entry.content);
      } else if (entry.kind === 'assistant') {
        const el = document.createElement('div');
        el.className = 'chat-msg chat-msg-assistant done';
        if (window.marked) {
          const parsed = window.marked.parse(entry.content);
          el.innerHTML = window.DOMPurify ? window.DOMPurify.sanitize(parsed) : parsed;
        } else {
          el.textContent = entry.content;
        }
        _messagesEl.appendChild(el);
      } else if (entry.kind === 'report') {
        try {
          const data = JSON.parse(entry.content);
          if (data.html) _renderReport(data.title || 'Report', data.html);
        } catch { /* skip */ }
      } else if (entry.kind === 'info') {
        const el = document.createElement('div');
        el.className = 'chat-msg chat-msg-info';
        el.textContent = entry.content;
        _messagesEl.appendChild(el);
      }
    }
  } catch { /* silent */ }
}

// ── Session ──────────────────────────────────────────────────────

export function newSession() {
  _sessionId = crypto.randomUUID();
  _pendingContext = [];
  if (_messagesEl) _messagesEl.innerHTML = '';
}

export function getSessionId() { return _sessionId; }
export function setModel(model) { _model = model; if (_modelSelect) _modelSelect.value = model; }
export function getModel() { return _model; }

// ── Styles ───────────────────────────────────────────────────────

function _applyStyles() {
  const style = document.createElement('style');
  style.textContent = `
    #chat-container {
      display: none;
      flex-direction: column;
      position: fixed;
      z-index: 400;
      font-family: 'Inter', system-ui, sans-serif;
    }

    /* Sidebar: replaces left panel */
    #chat-container.chat-sidebar {
      top: 68px; left: 16px; bottom: 16px; width: 340px;
      background: rgba(6,6,6,.97);
      border: 1px solid rgba(255,255,255,.08);
      border-radius: 16px;
      backdrop-filter: blur(20px);
    }

    /* Body: flex row for history + main */
    #chat-body { display: flex; flex: 1; overflow: hidden; }
    #chat-main { display: flex; flex-direction: column; flex: 1; overflow: hidden; }

    /* History sidebar */
    #chat-history {
      width: 240px;
      flex-shrink: 0;
      flex-direction: column;
      border-right: 1px solid rgba(255,255,255,.06);
      overflow-y: auto;
    }
    #history-tabs {
      display: flex;
      padding: 8px;
      gap: 4px;
    }
    .htab {
      flex: 1;
      background: none;
      border: 1px solid rgba(255,255,255,.08);
      color: rgba(255,255,255,.4);
      font: inherit;
      font-size: 10px;
      padding: 6px;
      border-radius: 6px;
      cursor: pointer;
      text-transform: uppercase;
      letter-spacing: 1px;
    }
    .htab.active { background: rgba(255,255,255,.06); color: rgba(255,255,255,.7); }
    #history-list { flex: 1; overflow-y: auto; }
    .hist-session, .hist-file {
      padding: 10px 12px;
      cursor: pointer;
      border-bottom: 1px solid rgba(255,255,255,.04);
      transition: background .15s;
    }
    .hist-session:hover, .hist-file:hover { background: rgba(255,255,255,.04); }
    .hist-preview {
      font-size: 12px;
      color: rgba(255,255,255,.7);
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .hist-meta { font-size: 10px; color: rgba(255,255,255,.3); margin-top: 2px; }

    /* Fullscreen */
    #chat-container.chat-fullscreen {
      inset: 0;
      background: #000;
    }
    #chat-container.chat-fullscreen #chat-header {
      border-bottom: none;
    }
    #chat-container.chat-fullscreen #chat-messages {
      padding: 24px 32px;
    }
    #chat-container.chat-fullscreen #chat-input-wrap {
      background: transparent;
      border-top: none;
      padding: 0 16px 16px;
    }
    #chat-container.chat-fullscreen #chat-input-bar {
      border: 1px solid rgba(255,255,255,.08);
      border-radius: 12px;
      background: rgba(255,255,255,.03);
    }
    #chat-container.chat-fullscreen #chat-input {
      padding: 14px 16px;
      font-size: 14px;
    }

    #chat-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 12px 16px;
      border-bottom: 1px solid rgba(255,255,255,.06);
      flex-shrink: 0;
    }

    #chat-messages {
      flex: 1;
      overflow-y: auto;
      padding: 16px;
      display: flex;
      flex-direction: column;
      gap: 12px;
    }
    #chat-messages::-webkit-scrollbar { width: 4px; }
    #chat-messages::-webkit-scrollbar-thumb { background: rgba(255,255,255,.1); border-radius: 2px; }

    #chat-input-wrap {
      flex-shrink: 0;
      padding: 12px 16px;
      border-top: 1px solid rgba(255,255,255,.06);
    }
    #chat-input-bar {
      display: flex;
      align-items: center;
      gap: 8px;
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.1);
      border-radius: 12px;
      padding: 4px 4px 4px 16px;
    }
    #chat-input {
      flex: 1;
      background: transparent;
      border: none;
      outline: none;
      color: #fff;
      font: inherit;
      font-size: 13px;
      font-weight: 300;
      padding: 10px 0;
    }
    #chat-input::placeholder { color: rgba(255,255,255,.25); }
    #chat-send {
      width: 28px; height: 28px;
      border-radius: 6px;
      background: rgba(255,255,255,.08);
      color: rgba(255,255,255,.4);
      border: none;
      font-size: 14px;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
      transition: all .15s;
    }
    #chat-send:hover { background: rgba(255,255,255,.15); color: rgba(255,255,255,.7); }

    .chat-msg { font-size: 13px; line-height: 1.6; max-width: 90%; }
    .chat-msg-user {
      align-self: flex-end;
      background: rgba(255,255,255,.08);
      padding: 10px 14px;
      border-radius: 14px 14px 4px 14px;
      color: rgba(255,255,255,.85);
    }
    .chat-msg-assistant {
      align-self: flex-start;
      color: rgba(255,255,255,.8);
      padding: 4px 0;
    }
    .chat-msg-assistant code {
      background: rgba(255,255,255,.06);
      padding: 2px 6px;
      border-radius: 4px;
      font-size: 12px;
    }
    .chat-msg-assistant pre {
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.06);
      border-radius: 8px;
      padding: 12px;
      overflow-x: auto;
      font-size: 12px;
      margin: 8px 0;
    }
    .chat-msg-error {
      align-self: flex-start;
      color: #f87171;
      font-size: 12px;
      padding: 8px 12px;
      background: rgba(220,38,38,.08);
      border-radius: 8px;
      border: 1px solid rgba(220,38,38,.15);
    }
    .chat-msg-working {
      color: rgba(255,255,255,.25);
      font-size: 12px;
      font-style: italic;
      padding: 4px 0;
      animation: pulse 1.5s ease-in-out infinite;
    }
    @keyframes pulse { 0%,100% { opacity: .4; } 50% { opacity: 1; } }
    .chat-msg-info {
      align-self: center;
      color: rgba(255,255,255,.3);
      font-size: 11px;
      padding: 4px 12px;
    }

    .chat-view { margin: 4px 0; }
    .chat-report { margin: 8px 0; }
    .cr-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 8px 12px;
      background: rgba(255,255,255,.03);
      border: 1px solid rgba(255,255,255,.06);
      border-radius: 8px;
      gap: 8px;
      cursor: pointer;
    }
    .cr-header:hover { background: rgba(255,255,255,.05); }
    .cr-toggle { color: rgba(255,255,255,.25); font-size: 8px; flex-shrink: 0; transition: transform .15s; }
    .cr-title { font-size: 11px; color: rgba(255,255,255,.5); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .cr-body { margin-top: 6px; }

    .chat-parcel-card {
      background: rgba(255,255,255,.04);
      border: 1px solid rgba(255,255,255,.08);
      border-radius: 10px;
      padding: 10px 14px;
      font-size: 12px;
    }
    .cpc-header { display: flex; justify-content: space-between; align-items: flex-start; }
    .cpc-title { font-size: 13px; font-weight: 500; color: rgba(255,255,255,.85); }
    .cpc-link { font-size: 10px; color: #E8C547; text-decoration: none; white-space: nowrap; padding-top: 2px; }
    .cpc-sub { font-size: 11px; color: rgba(255,255,255,.35); margin: 2px 0 8px; }
    .cpc-badge { font-size: 9px; padding: 1px 5px; border: 1px solid; border-radius: 3px; margin-left: 4px; }
    .cpc-grid { display: flex; flex-wrap: wrap; gap: 4px 12px; color: rgba(255,255,255,.5); font-size: 11px; }
    .cpc-grid b { color: rgba(255,255,255,.8); font-weight: 500; }
    .cpc-croquis { margin-top: 8px; }
    .cpc-docs { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; padding-top: 8px; border-top: 1px solid rgba(255,255,255,.06); }
    .cpc-docs a { color: rgba(255,255,255,.5); text-decoration: none; font-size: 10px; padding: 3px 8px; border: 1px solid rgba(255,255,255,.1); border-radius: 4px; transition: all .15s; }
    .cpc-docs a:hover { color: #E8C547; border-color: rgba(232,197,71,.3); }
    .art-dl-btn {
      background: none; border: none; color: rgba(255,255,255,.25);
      font: inherit; font-size: 10px; padding: 3px 6px; cursor: pointer;
      border-radius: 4px; transition: all .15s;
    }
    .art-dl-btn:hover { color: rgba(255,255,255,.6); background: rgba(255,255,255,.06); }

    @media (max-width: 640px) {
      #chat-container.chat-sidebar { left: 0; right: 0; width: auto; border-radius: 0; top: 52px; bottom: 0; }
    }
  `;
  document.head.appendChild(style);
}
