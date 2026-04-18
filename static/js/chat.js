/**
 * chat.js — Terminal-style chat interface for EdificIA.
 *
 * Three display modes:
 *   - hidden:     completely invisible
 *   - sidebar:    right panel (~420px), map shrinks
 *   - fullscreen: black screen, input bar at bottom (2 white lines)
 *
 * Features:
 *   - Model selector (haiku / sonnet / opus)
 *   - SSE streaming from POST /api/chat
 *   - Markdown rendering (marked.js)
 *   - Tool call status lines (collapsible)
 *   - HTML views (sandboxed iframes)
 *   - Download links
 *   - Custom views sidebar (localStorage persistence)
 */

// ── State ────────────────────────────────────────────────────────

let _mode = 'hidden';
let _model = 'sonnet';
let _sessionId = crypto.randomUUID();
let _messages = [];       // {role, content, id}
let _views = [];           // {id, title, html, created_at}
let _streaming = false;

// ── DOM refs (set in initChat) ───────────────────────────────────

let _chatContainer, _messagesEl, _inputEl, _modelSelect;
let _viewsSidebar, _viewsList;

// ── Init ─────────────────────────────────────────────────────────

export function initChat() {
  _loadViews();
  _buildDOM();
  _bindKeys();
}

function _buildDOM() {
  // Chat toggle button
  const toggle = document.createElement('button');
  toggle.id = 'chat-toggle';
  toggle.innerHTML = '⌘';
  toggle.title = 'Chat (Ctrl+K)';
  Object.assign(toggle.style, {
    position: 'fixed', bottom: '20px', right: '20px', zIndex: '500',
    width: '44px', height: '44px', borderRadius: '50%',
    background: '#fff', color: '#000', border: 'none',
    fontSize: '18px', cursor: 'pointer', fontFamily: 'Inter, system-ui',
    boxShadow: '0 2px 12px rgba(0,0,0,.4)',
    transition: 'transform .15s, opacity .15s',
  });
  toggle.addEventListener('click', () => setChatMode(_mode === 'hidden' ? 'sidebar' : 'hidden'));
  document.body.appendChild(toggle);

  // Chat container
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
      <div style="display:flex;gap:8px">
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

  // Views sidebar
  _viewsSidebar = document.createElement('div');
  _viewsSidebar.id = 'views-sidebar';
  _viewsSidebar.innerHTML = `
    <div style="font-size:9px;letter-spacing:2px;text-transform:uppercase;color:rgba(255,255,255,.3);padding:16px 16px 8px">Vistas guardadas</div>
    <div id="views-list"></div>
  `;
  document.body.appendChild(_viewsSidebar);

  // Refs
  _messagesEl = document.getElementById('chat-messages');
  _inputEl = document.getElementById('chat-input');
  _modelSelect = document.getElementById('chat-model');
  _viewsList = document.getElementById('views-list');

  // Events
  _modelSelect.addEventListener('change', () => { _model = _modelSelect.value; });
  document.getElementById('chat-close').addEventListener('click', () => setChatMode('hidden'));
  document.getElementById('chat-expand').addEventListener('click', () => {
    setChatMode(_mode === 'fullscreen' ? 'sidebar' : 'fullscreen');
  });
  document.getElementById('chat-send').addEventListener('click', _onSend);
  _inputEl.addEventListener('keydown', e => { if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); _onSend(); } });

  _applyStyles();
  _renderViewsList();
}

function _bindKeys() {
  document.addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
      e.preventDefault();
      setChatMode(_mode === 'hidden' ? 'sidebar' : 'hidden');
    }
    if (e.key === 'Escape' && _mode !== 'hidden') {
      setChatMode('hidden');
    }
  });
}

// ── Mode management ──────────────────────────────────────────────

export function setChatMode(mode) {
  _mode = mode;
  const c = _chatContainer;
  const t = document.getElementById('chat-toggle');
  const v = _viewsSidebar;

  c.className = '';
  if (mode === 'hidden') {
    c.style.display = 'none';
    t.style.display = 'block';
    v.style.display = 'none';
    document.body.style.overflow = 'hidden';
  } else if (mode === 'sidebar') {
    c.style.display = 'flex';
    c.classList.add('chat-sidebar');
    t.style.display = 'none';
    v.style.display = _views.length ? 'block' : 'none';
    v.classList.add('views-sidebar-active');
    document.body.style.overflow = 'hidden';
    _inputEl.focus();
  } else if (mode === 'fullscreen') {
    c.style.display = 'flex';
    c.classList.add('chat-fullscreen');
    t.style.display = 'none';
    v.style.display = 'none';
    document.body.style.overflow = 'hidden';
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

  const assistantId = 'msg-' + Date.now();
  const updater = _renderAssistantMessage(assistantId);

  try {
    const resp = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ session_id: _sessionId, message: text, model: _model }),
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
        } catch { /* skip malformed events */ }
      }
    }

    updater.finish();
  } catch (e) {
    updater.finish();
    _renderError(e.message);
  } finally {
    _streaming = false;
  }
}

function _handleSSEEvent(event, updater) {
  switch (event.type) {
    case 'text':
      updater.append(event.data);
      break;
    case 'tool':
      _renderToolCall(event.data.name, event.data.status);
      break;
    case 'html_view':
      _renderHtmlView(event.data.title, event.data.html);
      _saveView(event.data.title, event.data.html);
      break;
    case 'download':
      _renderDownload(event.data.filename, event.data.url);
      break;
    case 'error':
      _renderError(event.data);
      break;
    case 'rate_limit':
      _renderError(`Rate limit alcanzado. Se reinicia en ${new Date(event.data.resets_at * 1000).toLocaleTimeString()}`);
      break;
    case 'done':
      break;
  }
}

// ── Message rendering ────────────────────────────────────────────

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
      // Use marked.js if available, otherwise plain text with basic formatting
      if (window.marked) {
        el.innerHTML = window.marked.parse(accumulated);
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

function _renderToolCall(name, status) {
  const toolNames = {
    sql: 'Consultando base de datos',
    http: 'Consultando API GCBA',
    render_html: 'Generando vista',
    create_download: 'Creando archivo',
    Read: 'Leyendo documento',
    Grep: 'Buscando en normativa',
    Glob: 'Buscando archivos',
  };

  const el = document.createElement('div');
  el.className = 'chat-tool';
  el.innerHTML = `<span class="chat-tool-icon">${status === 'running' ? '⟳' : '✓'}</span> ${toolNames[name] || name}…`;
  _messagesEl.appendChild(el);
  _scrollToBottom();
}

function _renderHtmlView(title, html) {
  const wrapper = document.createElement('div');
  wrapper.className = 'chat-view';
  wrapper.innerHTML = `
    <div style="font-size:10px;letter-spacing:2px;text-transform:uppercase;color:rgba(255,255,255,.3);margin-bottom:6px">${title}</div>
    <iframe sandbox="allow-scripts" srcdoc="${html.replace(/"/g, '&quot;')}" style="width:100%;height:300px;border:1px solid rgba(255,255,255,.08);border-radius:8px;background:#0a0a0a"></iframe>
  `;
  _messagesEl.appendChild(wrapper);
  _scrollToBottom();
}

function _renderDownload(filename, url) {
  const el = document.createElement('div');
  el.className = 'chat-download';
  el.innerHTML = `<a href="${url}" download="${filename}" style="color:#e8c547;text-decoration:none;font-size:12px">⬇ ${filename}</a>`;
  _messagesEl.appendChild(el);
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

// ── Custom views ─────────────────────────────────────────────────

function _saveView(title, html) {
  const view = { id: crypto.randomUUID(), title, html, created_at: Date.now() };
  _views.unshift(view);
  if (_views.length > 50) _views.pop();
  localStorage.setItem('edificia_views', JSON.stringify(_views));
  _renderViewsList();
  if (_mode === 'sidebar') _viewsSidebar.style.display = 'block';
}

function _loadViews() {
  try {
    _views = JSON.parse(localStorage.getItem('edificia_views') || '[]');
  } catch { _views = []; }
}

function _renderViewsList() {
  if (!_viewsList) return;
  _viewsList.innerHTML = _views.map(v => `
    <div class="view-item" data-id="${v.id}">
      <span style="font-size:12px;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${v.title}</span>
      <button class="view-delete" data-id="${v.id}" style="background:none;border:none;color:rgba(255,255,255,.2);cursor:pointer;font-size:14px;padding:2px 4px">×</button>
    </div>
  `).join('');

  _viewsList.querySelectorAll('.view-item').forEach(el => {
    el.addEventListener('click', () => loadView(el.dataset.id));
  });
  _viewsList.querySelectorAll('.view-delete').forEach(el => {
    el.addEventListener('click', e => { e.stopPropagation(); deleteView(el.dataset.id); });
  });
}

export function loadView(viewId) {
  const view = _views.find(v => v.id === viewId);
  if (!view) return;
  _renderHtmlView(view.title, view.html);
}

export function deleteView(viewId) {
  _views = _views.filter(v => v.id !== viewId);
  localStorage.setItem('edificia_views', JSON.stringify(_views));
  _renderViewsList();
}

export function getViews() { return _views; }

// ── Session ──────────────────────────────────────────────────────

export function newSession() {
  _sessionId = crypto.randomUUID();
  _messages = [];
  if (_messagesEl) _messagesEl.innerHTML = '';
}

export function getSessionId() { return _sessionId; }
export function setModel(model) { _model = model; if (_modelSelect) _modelSelect.value = model; }
export function getModel() { return _model; }

// ── Styles ───────────────────────────────────────────────────────

function _applyStyles() {
  const style = document.createElement('style');
  style.textContent = `
    /* Chat container */
    #chat-container {
      display: none;
      flex-direction: column;
      position: fixed;
      z-index: 400;
      font-family: 'Inter', system-ui, sans-serif;
    }

    /* Sidebar mode */
    #chat-container.chat-sidebar {
      top: 52px; right: 0; bottom: 0; width: 420px;
      background: rgba(6,6,6,.97);
      border-left: 1px solid rgba(255,255,255,.08);
      backdrop-filter: blur(20px);
    }

    /* Fullscreen mode */
    #chat-container.chat-fullscreen {
      inset: 0;
      background: #000;
    }
    #chat-container.chat-fullscreen #chat-header {
      border-bottom: none;
      background: transparent;
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

    /* Header */
    #chat-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 12px 16px;
      border-bottom: 1px solid rgba(255,255,255,.06);
      flex-shrink: 0;
    }

    /* Messages area */
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

    /* Input area */
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

    /* Messages */
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

    /* Tool calls */
    .chat-tool {
      font-size: 11px;
      color: rgba(255,255,255,.3);
      padding: 4px 0;
    }
    .chat-tool-icon { margin-right: 4px; }

    /* HTML views */
    .chat-view { margin: 4px 0; }

    /* Downloads */
    .chat-download { padding: 4px 0; }

    /* Views sidebar */
    #views-sidebar {
      display: none;
      position: fixed;
      top: 52px; left: 0; bottom: 0;
      width: 220px;
      background: rgba(6,6,6,.95);
      border-right: 1px solid rgba(255,255,255,.06);
      z-index: 350;
      overflow-y: auto;
    }
    .view-item {
      display: flex;
      align-items: center;
      padding: 10px 16px;
      cursor: pointer;
      color: rgba(255,255,255,.6);
      transition: background .15s;
      gap: 8px;
    }
    .view-item:hover { background: rgba(255,255,255,.04); }

    @media (max-width: 640px) {
      #chat-container.chat-sidebar { width: 100%; }
      #views-sidebar { display: none !important; }
    }
  `;
  document.head.appendChild(style);
}
