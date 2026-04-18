"""
Chat agent backend for EdificIA.

Provides a Claude Agent SDK-powered chat that can query the parcelas DB,
call GCBA APIs, read normativa files, render HTML views, and create
downloadable files. Streams responses as SSE events.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Literal

logger = logging.getLogger("edificia.chat")

import httpx
from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    PermissionResultAllow,
    PermissionResultDeny,
    ResultMessage,
    StreamEvent,
    SystemMessage,
    TextBlock,
    ToolPermissionContext,
    create_sdk_mcp_server,
    tool,
)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "caba_normativa.db"
NORMATIVA_DIR = BASE_DIR / "normativa"
DOWNLOADS_DIR = Path("/tmp/edificia_downloads")

ALLOWED_HTTP_DOMAINS = frozenset({
    "epok.buenosaires.gob.ar",
    "servicios.usig.buenosaires.gob.ar",
    "cdn.buenosaires.gob.ar",
})

MAX_SQL_ROWS = 1000
SQL_TIMEOUT_S = 10
HTTP_TIMEOUT_S = 15
SESSION_TTL_S = 30 * 60  # 30 minutes

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
Sos el asistente de EdificIA, una plataforma de factibilidad urbanistica \
de Buenos Aires (CABA). Ayudas a usuarios a evaluar oportunidades de \
desarrollo inmobiliario.

- Responde siempre en espanol rioplatense.
- Se conciso y preciso. Cita fuentes (SMP, ley, API) cuando corresponda.
- No reveles detalles internos de la plataforma, herramientas ni esquema \
  de base de datos al usuario.
"""


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------


@tool(
    "sql",
    "Ejecutar una consulta SELECT de solo lectura contra la base de datos "
    "de parcelas. Devuelve hasta 1000 filas como lista de diccionarios.",
    {"query": str},
)
async def tool_sql(args: dict[str, Any]) -> dict[str, Any]:
    """Run read-only SQL against caba_normativa.db."""
    query: str = args.get("query", "").strip()
    logger.info("tool_sql query=%.100s", query)
    if not query:
        return _tool_error("La consulta SQL esta vacia.")

    # Basic safety: only SELECT allowed
    first_word = query.lstrip("(").split()[0].upper() if query.split() else ""
    if first_word not in ("SELECT", "WITH"):
        return _tool_error(
            "Solo se permiten consultas SELECT / WITH."
        )

    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=SQL_TIMEOUT_S)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        cursor = conn.execute(query)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchmany(MAX_SQL_ROWS)
        results = [dict(zip(columns, row)) for row in rows]
    except sqlite3.Error as exc:
        return _tool_error(f"Error SQL: {exc}")
    finally:
        if conn:
            conn.close()

    return _tool_text(json.dumps(results, ensure_ascii=False, default=str))


@tool(
    "schema",
    "Devolver el esquema completo de la base de datos (tablas, columnas, tipos).",
    {},
)
async def tool_schema(args: dict[str, Any]) -> dict[str, Any]:
    """Return the full DB schema."""
    conn: sqlite3.Connection | None = None
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=SQL_TIMEOUT_S)
        conn.row_factory = sqlite3.Row
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()

        schema_info: dict[str, list[dict[str, str]]] = {}
        for table_row in tables:
            table_name = table_row["name"]
            cols = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            schema_info[table_name] = [
                {"name": c["name"], "type": c["type"], "notnull": bool(c["notnull"])}
                for c in cols
            ]
    except sqlite3.Error as exc:
        return _tool_error(f"Error leyendo schema: {exc}")
    finally:
        if conn:
            conn.close()

    return _tool_text(json.dumps(schema_info, ensure_ascii=False))


@tool(
    "http",
    "Hacer un request HTTP GET o POST a APIs del GCBA. Dominios permitidos: "
    "epok.buenosaires.gob.ar, servicios.usig.buenosaires.gob.ar, "
    "cdn.buenosaires.gob.ar.",
    {"url": str, "method": str, "body": str},
)
async def tool_http(args: dict[str, Any]) -> dict[str, Any]:
    """GET/POST to allowlisted GCBA APIs."""
    url: str = args.get("url", "")
    method: str = args.get("method", "GET").upper()
    body: str | None = args.get("body")

    if method not in ("GET", "POST"):
        return _tool_error("Solo se permiten metodos GET y POST.")

    # Domain allowlist check
    try:
        from urllib.parse import urlparse

        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return _tool_error(f"Esquema '{parsed.scheme}' no permitido. Solo http/https.")
        if parsed.hostname not in ALLOWED_HTTP_DOMAINS:
            return _tool_error(
                f"Dominio '{parsed.hostname}' no permitido. "
                f"Dominios validos: {', '.join(sorted(ALLOWED_HTTP_DOMAINS))}"
            )
    except Exception:
        return _tool_error(f"URL invalida: {url}")

    try:
        async with httpx.AsyncClient(timeout=HTTP_TIMEOUT_S) as client:
            if method == "GET":
                resp = await client.get(url)
            else:
                resp = await client.post(url, content=body)
        return _tool_text(resp.text)
    except httpx.TimeoutException:
        return _tool_error(f"Timeout al conectar con {url}")
    except httpx.HTTPError as exc:
        return _tool_error(f"Error HTTP: {exc}")


# Pending renders keyed by session_id to avoid cross-session leaks.
_pending_renders: dict[str, list[dict[str, str]]] = defaultdict(list)

# Active session_id for the current render_html call.
_active_session_id: str | None = None


@tool(
    "render_html",
    "Crear un reporte HTML para el usuario (tabla, grafico, mapa, etc). "
    "Se renderiza en un iframe con tema oscuro y auto-resize. "
    "Usar collapsed=true para archivos de descarga que no necesitan vista previa.",
    {"title": str, "html": str, "collapsed": bool},
)
async def tool_render_html(args: dict[str, Any]) -> dict[str, Any]:
    """Wrap HTML in dark-theme template and queue for SSE stream."""
    title: str = args.get("title", "Vista")
    html: str = args.get("html", "")
    collapsed: bool = args.get("collapsed", False)
    logger.info("tool_render_html title=%s html_len=%d collapsed=%s", title, len(html), collapsed)
    wrapped = _wrap_html_for_iframe(html)
    if _active_session_id:
        _pending_renders[_active_session_id].append({
            "title": title, "html": wrapped, "collapsed": collapsed,
        })
    return _tool_text(
        "Reporte creado correctamente. El usuario puede verlo y descargarlo."
    )


# ---------------------------------------------------------------------------
# HTML iframe wrapper
# ---------------------------------------------------------------------------


_IFRAME_TEMPLATE = """<!DOCTYPE html>
<html><head><meta charset="utf-8">
<style>
*, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ background: #0a0a0a; color: rgba(255,255,255,.85);
  font-family: Inter, system-ui, sans-serif; font-size: 13px;
  line-height: 1.5; padding: 12px; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ padding: 6px 10px; border-bottom: 1px solid rgba(255,255,255,.08);
  text-align: left; font-size: 12px; }}
th {{ color: rgba(255,255,255,.4); font-weight: 500;
  text-transform: uppercase; font-size: 10px; letter-spacing: 1px; }}
tr:hover {{ background: rgba(255,255,255,.03); }}
a {{ color: #E8C547; }}
code {{ background: rgba(255,255,255,.06); padding: 2px 6px;
  border-radius: 4px; font-size: 12px; }}
h1, h2, h3 {{ color: rgba(255,255,255,.9); font-weight: 500;
  margin-bottom: 8px; }}
h1 {{ font-size: 16px; }} h2 {{ font-size: 14px; }} h3 {{ font-size: 13px; }}
</style></head><body>
{content}
<script>
new ResizeObserver(function() {{
  window.parent.postMessage(
    {{ type: "iframe-resize", height: document.documentElement.scrollHeight }}, "*"
  );
}}).observe(document.documentElement);
window.parent.postMessage(
  {{ type: "iframe-resize", height: document.documentElement.scrollHeight }}, "*"
);
</script></body></html>"""


def _wrap_html_for_iframe(raw_html: str) -> str:
    """Wrap agent HTML in dark-theme template with auto-resize postMessage."""
    return _IFRAME_TEMPLATE.format(content=raw_html)


# ---------------------------------------------------------------------------
# Tool helpers
# ---------------------------------------------------------------------------


def _tool_text(text: str) -> dict[str, Any]:
    """Return a successful tool result with text content."""
    return {"content": [{"type": "text", "text": text}]}


def _tool_error(message: str) -> dict[str, Any]:
    """Return an error tool result."""
    return {"content": [{"type": "text", "text": f"ERROR: {message}"}], "isError": True}


# ---------------------------------------------------------------------------
# MCP server & agent factory
# ---------------------------------------------------------------------------

edificia_mcp = create_sdk_mcp_server(
    "edificia",
    tools=[tool_sql, tool_schema, tool_http, tool_render_html],
)


async def _sandbox_reads(
    tool_name: str,
    input_data: dict[str, Any],
    context: ToolPermissionContext,
) -> PermissionResultAllow | PermissionResultDeny:
    """Restrict Read/Grep/Glob to normativa/ directory only."""
    if tool_name in ("Read", "Grep", "Glob"):
        path = input_data.get("file_path") or input_data.get("path") or ""
        resolved = Path(path).resolve()
        if not resolved.is_relative_to(NORMATIVA_DIR):
            return PermissionResultDeny(
                message="Solo se permite leer archivos en normativa/.",
                interrupt=False,
            )
    return PermissionResultAllow(updated_input=input_data)


def create_agent(model: str = "sonnet") -> ClaudeAgentOptions:
    """Build ClaudeAgentOptions for the EdificIA chat agent.

    Returns options (not a client) so SessionManager can construct the
    ClaudeSDKClient with proper async lifecycle.
    """
    model_id = _resolve_model(model)
    return ClaudeAgentOptions(
        system_prompt=SYSTEM_PROMPT,
        allowed_tools=["Read", "Grep", "Glob", "mcp__edificia__*"],
        disallowed_tools=[
            "Bash", "Edit", "Write", "WebSearch", "WebFetch", "Agent",
        ],
        can_use_tool=_sandbox_reads,
        permission_mode="default",
        setting_sources=["project"],
        mcp_servers={"edificia": edificia_mcp},
        cwd=str(NORMATIVA_DIR),
        model=model_id,
        max_turns=25,
    )


def _resolve_model(short_name: str) -> str:
    """Map user-facing model names to Claude model IDs."""
    mapping: dict[str, str] = {
        "sonnet": "claude-sonnet-4-6",
        "opus": "claude-opus-4-6",
        "haiku": "claude-haiku-4-5",
    }
    return mapping.get(short_name, short_name)


# ---------------------------------------------------------------------------
# Session manager
# ---------------------------------------------------------------------------


@dataclass
class _SessionEntry:
    """Tracks a ClaudeSDKClient and its metadata."""

    client: ClaudeSDKClient
    model: str
    last_used: float = field(default_factory=time.time)


class SessionManager:
    """Manages ClaudeSDKClient instances per session with TTL eviction."""

    def __init__(self, ttl_seconds: int = SESSION_TTL_S) -> None:
        self._sessions: dict[str, _SessionEntry] = {}
        self._ttl = ttl_seconds

    async def get_or_create(
        self, session_id: str, model: str = "sonnet"
    ) -> ClaudeSDKClient:
        """Return existing client or create a new one.

        If the session exists but the model changed, tear down and recreate.
        """
        entry = self._sessions.get(session_id)
        if entry is not None:
            if entry.model == model:
                entry.last_used = time.time()
                return entry.client
            # Model changed: close old client
            await self._close_entry(entry)

        options = create_agent(model)
        client = ClaudeSDKClient(options=options)
        await client.__aenter__()
        self._sessions[session_id] = _SessionEntry(
            client=client, model=model
        )
        return client

    async def delete(self, session_id: str) -> None:
        """Close and remove a session."""
        entry = self._sessions.pop(session_id, None)
        if entry is not None:
            await self._close_entry(entry)

    async def cleanup_expired(self) -> int:
        """Remove sessions that exceeded TTL. Returns count removed."""
        now = time.time()
        expired = [
            sid
            for sid, entry in self._sessions.items()
            if now - entry.last_used > self._ttl
        ]
        for sid in expired:
            await self.delete(sid)
        return len(expired)

    @staticmethod
    async def _close_entry(entry: _SessionEntry) -> None:
        """Gracefully close a client."""
        try:
            await entry.client.__aexit__(None, None, None)
        except Exception:
            pass  # Best-effort cleanup

    @property
    def active_count(self) -> int:
        return len(self._sessions)


# ---------------------------------------------------------------------------
# SSE event types
# ---------------------------------------------------------------------------

SSEEventType = Literal["text", "artifact", "error", "done"]


@dataclass(frozen=True)
class SSEEvent:
    """A single SSE event to send to the client."""

    event_type: SSEEventType
    data: Any

    def serialize(self) -> str:
        """Format as an SSE message string."""
        payload = json.dumps(
            {"type": self.event_type, "data": self.data},
            ensure_ascii=False,
            default=str,
        )
        return f"data: {payload}\n\n"


# ---------------------------------------------------------------------------
# SSE stream generator
# ---------------------------------------------------------------------------


async def create_sse_stream(
    client: ClaudeSDKClient,
    message: str,
    session_id: str = "",
    user_id: int | None = None,
    model: str = "sonnet",
) -> AsyncIterator[str]:
    """Send a message to the agent and yield SSE event strings.

    Persists user/assistant messages and artifacts to chat_entries.
    """
    global _active_session_id
    total_input_tokens = 0
    total_output_tokens = 0
    artifact_count = 0
    assistant_text = ""
    start = time.monotonic()

    logger.info("chat_start session=%s msg_len=%d", session_id[:8], len(message))

    # Persist user message and upsert session
    _persist_entry(
        session_id, "user", message,
        user_id=user_id, preview=message[:80], model=model,
    )

    # Initialize per-request render state
    _active_session_id = session_id
    _pending_renders.pop(session_id, None)

    try:
        await client.query(message)
        working = False

        async for msg in client.receive_response():
            if isinstance(msg, StreamEvent):
                event = msg.event
                etype = event.get("type", "")
                if etype == "content_block_delta":
                    delta = event.get("delta", {})
                    if delta.get("type") == "text_delta":
                        text = delta.get("text", "")
                        if text:
                            if working:
                                yield SSEEvent("working", False).serialize()
                                working = False
                            assistant_text += text
                            yield SSEEvent("text", text).serialize()
                elif etype == "content_block_start":
                    block = event.get("content_block", {})
                    if block.get("type") == "tool_use" and not working:
                        working = True
                        yield SSEEvent("working", True).serialize()

            elif isinstance(msg, AssistantMessage):
                if msg.usage:
                    total_input_tokens += msg.usage.get("input_tokens", 0)
                    total_output_tokens += msg.usage.get("output_tokens", 0)

            elif isinstance(msg, ResultMessage):
                if working:
                    yield SSEEvent("working", False).serialize()
                    working = False
                if assistant_text:
                    _persist_entry(session_id, "assistant", assistant_text)
                    assistant_text = ""

                for render_data in _pending_renders.pop(session_id, []):
                    yield SSEEvent("artifact", render_data).serialize()
                    artifact_count += 1
                    _persist_entry(
                        session_id, "report",
                        json.dumps({
                            "title": render_data["title"],
                            "html": render_data["html"],
                            "source": "agent",
                            "size": len(render_data["html"]),
                        }, ensure_ascii=False),
                    )

            elif isinstance(msg, SystemMessage):
                pass

    except Exception as exc:
        logger.error("chat_error session=%s: %s", session_id[:8], exc)
        yield SSEEvent("error", str(exc)).serialize()
    finally:
        # Persist any remaining assistant text
        if assistant_text:
            _persist_entry(session_id, "assistant", assistant_text)
        _active_session_id = None
        _pending_renders.pop(session_id, None)

    elapsed = time.monotonic() - start
    logger.info(
        "chat_end session=%s in=%d out=%d artifacts=%d elapsed=%.1fs",
        session_id[:8], total_input_tokens, total_output_tokens,
        artifact_count, elapsed,
    )

    yield SSEEvent(
        "done",
        {
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
        },
    ).serialize()


# ---------------------------------------------------------------------------
# Chat persistence
# ---------------------------------------------------------------------------


def init_chat_tables() -> None:
    """Create chat_sessions and chat_entries tables if they don't exist."""
    conn = sqlite3.connect(str(DB_PATH), timeout=SQL_TIMEOUT_S)
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS chat_sessions (
                id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                created_at REAL DEFAULT (strftime('%s','now')),
                last_used REAL DEFAULT (strftime('%s','now')),
                preview TEXT,
                model TEXT DEFAULT 'sonnet',
                FOREIGN KEY (user_id) REFERENCES users(id)
            );
            CREATE TABLE IF NOT EXISTS chat_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at REAL DEFAULT (strftime('%s','now')),
                FOREIGN KEY (session_id) REFERENCES chat_sessions(id)
            );
        """)
    finally:
        conn.close()


def _persist_entry(
    session_id: str, kind: str, content: str,
    user_id: int | None = None, preview: str | None = None,
    model: str = "sonnet",
) -> None:
    """Save a chat entry and upsert the session row."""
    conn = sqlite3.connect(str(DB_PATH), timeout=SQL_TIMEOUT_S)
    try:
        now = time.time()
        if user_id is not None:
            conn.execute(
                "INSERT INTO chat_sessions (id, user_id, created_at, last_used, preview, model) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET last_used=?, preview=COALESCE(preview, ?)",
                (session_id, user_id, now, now, preview, model, now, preview),
            )
        else:
            conn.execute(
                "UPDATE chat_sessions SET last_used=? WHERE id=?", (now, session_id)
            )
        conn.execute(
            "INSERT INTO chat_entries (session_id, kind, content) VALUES (?, ?, ?)",
            (session_id, kind, content),
        )
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Download cleanup
# ---------------------------------------------------------------------------


def cleanup_old_downloads(max_age_seconds: int = 3600) -> int:
    """Remove download files older than max_age_seconds. Returns count."""
    if not DOWNLOADS_DIR.exists():
        return 0
    now = time.time()
    removed = 0
    for f in DOWNLOADS_DIR.iterdir():
        if f.is_file() and (now - f.stat().st_mtime) > max_age_seconds:
            f.unlink(missing_ok=True)
            removed += 1
    return removed
