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
let _model = 'sonnet';
let _sessionId = crypto.randomUUID();
let _streaming = false;
let _abortController = null;

// ── DOM refs ─────────────────────────────────────────────────────

let _chatContainer, _messagesEl, _inputEl, _modelSelect;

// ── Init ─────────────────────────────────────────────────────────

export function initChat() {
  _buildDOM();
  _bindKeys();
  _listenIframeResize();

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
        <span style="font-size:10px;letter-spacing:3px;text-transform:uppercase;color:rgba(255,255,255,.4)">EdificIA Chat</span>
        <select id="chat-model" style="background:rgba(255,255,255,.06);border:1px solid rgba(255,255,255,.1);
          border-radius:6px;color:#fff;font:inherit;font-size:11px;padding:4px 8px;cursor:pointer;outline:none">
          <option value="haiku">Haiku</option>
          <option value="sonnet" selected>Sonnet</option>
          <option value="opus">Opus</option>
        </select>
      </div>
      <div style="display:flex;gap:6px">
        <button id="chat-new" title="Nueva sesión" style="background:none;border:none;color:rgba(255,255,255,.4);font-size:16px;cursor:pointer;padding:4px">+</button>
        <button id="chat-expand" title="Pantalla completa" style="background:none;border:none;color:rgba(255,255,255,.4);font-size:14px;cursor:pointer;padding:4px">⛶</button>
        <button id="chat-close" title="Cerrar" style="background:none;border:none;color:rgba(255,255,255,.4);font-size:16px;cursor:pointer;padding:4px">×</button>
      </div>
    </div>
    <div id="chat-messages"></div>
    <div id="chat-input-wrap">
      <div id="chat-input-bar">
        <input type="text" id="chat-input" placeholder="Preguntá sobre normativa, parcelas, zonificación…" autocomplete="off">
        <button id="chat-send">↑</button>
      </div>
    </div>
  `;
  document.body.appendChild(_chatContainer);

  _messagesEl = document.getElementById('chat-messages');
  _inputEl = document.getElementById('chat-input');
  _modelSelect = document.getElementById('chat-model');

  _modelSelect.addEventListener('change', () => { _model = _modelSelect.value; });
  document.getElementById('chat-close').addEventListener('click', () => setChatMode('hidden'));
  document.getElementById('chat-expand').addEventListener('click', () => {
    setChatMode(_mode === 'fullscreen' ? 'sidebar' : 'fullscreen');
  });
  document.getElementById('chat-new').addEventListener('click', newSession);
  document.getElementById('chat-send').addEventListener('click', _onSend);
  _inputEl.addEventListener('keydown', e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); _onSend(); } });

  _applyStyles();
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

  c.className = '';
  if (mode === 'hidden') {
    c.style.display = 'none';
    if (leftPanel) leftPanel.style.display = '';
  } else if (mode === 'sidebar') {
    c.style.display = 'flex';
    c.classList.add('chat-sidebar');
    if (leftPanel) leftPanel.style.display = 'none';
    _inputEl.focus();
  } else if (mode === 'fullscreen') {
    c.style.display = 'flex';
    c.classList.add('chat-fullscreen');
    if (leftPanel) leftPanel.style.display = 'none';
    _inputEl.focus();
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

  const assistantId = 'msg-' + Date.now();
  const updater = _renderAssistantMessage(assistantId);

  try {
    const resp = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: _sessionId, message: text, model: _model }),
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
    case 'artifact':
      _renderReport(event.data.title, event.data.html);
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
 * Add a short info message to the chat (e.g., barrio change).
 */
export function addInfoMessage(text) {
  if (_mode === 'hidden') return;
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
    finish() {
      el.classList.add('done');
    },
  };
}

function _renderReport(title, html) {
  const artId = 'art-' + Date.now() + '-' + Math.random().toString(36).slice(2, 6);
  const wrapper = document.createElement('div');
  wrapper.className = 'chat-view';
  wrapper.innerHTML = `
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
      <div style="font-size:10px;letter-spacing:2px;text-transform:uppercase;color:rgba(255,255,255,.3)">${_escapeHtml(title)}</div>
      <div style="display:flex;gap:4px">
        <button class="art-dl-btn" data-fmt="html" title="Descargar HTML" style="background:none;border:1px solid rgba(255,255,255,.1);color:rgba(255,255,255,.4);font-size:10px;padding:2px 8px;border-radius:4px;cursor:pointer">↓ HTML</button>
        <button class="art-dl-btn" data-fmt="pdf" title="Imprimir / PDF" style="background:none;border:1px solid rgba(255,255,255,.1);color:rgba(255,255,255,.4);font-size:10px;padding:2px 8px;border-radius:4px;cursor:pointer">↓ PDF</button>
      </div>
    </div>
    <iframe id="${artId}" sandbox="allow-scripts allow-modals" srcdoc="${html.replace(/"/g, '&quot;')}" style="width:100%;min-height:60px;height:60px;border:1px solid rgba(255,255,255,.08);border-radius:8px;background:#0a0a0a;transition:height .2s"></iframe>
  `;

  // Download handlers
  wrapper.querySelector('[data-fmt="html"]').addEventListener('click', () => {
    const blob = new Blob([html], { type: 'text/html' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = title.replace(/[^a-zA-Z0-9áéíóúñ _-]/gi, '_').slice(0, 60) + '.html';
    a.click();
    URL.revokeObjectURL(a.href);
  });
  wrapper.querySelector('[data-fmt="pdf"]').addEventListener('click', () => {
    const iframe = document.getElementById(artId);
    if (iframe?.contentWindow) iframe.contentWindow.print();
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

// ── Session ──────────────────────────────────────────────────────

export function newSession() {
  _sessionId = crypto.randomUUID();
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

    /* Fullscreen */
    #chat-container.chat-fullscreen {
      inset: 0;
      background: #000;
    }
    #chat-container.chat-fullscreen #chat-header {
      border-bottom: none;
    }
    #chat-container.chat-fullscreen #chat-messages {
      max-width: 720px;
      margin: 0 auto;
      padding: 40px 24px;
    }
    #chat-container.chat-fullscreen #chat-input-wrap {
      background: transparent;
      border-top: 1px solid rgba(255,255,255,.15);
      padding: 0;
    }
    #chat-container.chat-fullscreen #chat-input-bar {
      max-width: 720px;
      margin: 0 auto;
      border: none;
      border-radius: 0;
      background: transparent;
      border-bottom: 1px solid rgba(255,255,255,.15);
    }
    #chat-container.chat-fullscreen #chat-input {
      padding: 20px 16px;
      font-size: 15px;
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
      width: 32px; height: 32px;
      border-radius: 8px;
      background: #fff;
      color: #000;
      border: none;
      font-size: 16px;
      cursor: pointer;
      display: flex;
      align-items: center;
      justify-content: center;
      flex-shrink: 0;
      transition: opacity .15s;
    }
    #chat-send:hover { opacity: .8; }

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
    .chat-msg-info {
      align-self: center;
      color: rgba(255,255,255,.3);
      font-size: 11px;
      padding: 4px 12px;
    }

    .chat-view { margin: 4px 0; }
    .art-dl-btn:hover { color: rgba(255,255,255,.7) !important; border-color: rgba(255,255,255,.25) !important; }

    @media (max-width: 640px) {
      #chat-container.chat-sidebar { left: 0; right: 0; width: auto; border-radius: 0; top: 52px; bottom: 0; }
    }
  `;
  document.head.appendChild(style);
}
