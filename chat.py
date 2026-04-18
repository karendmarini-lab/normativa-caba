"""
Chat agent backend for EdificIA.

Provides a Claude Agent SDK-powered chat that can query the parcelas DB,
call GCBA APIs, read normativa files, render HTML views, and create
downloadable files. Streams responses as SSE events.
"""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, AsyncIterator, Literal

import httpx
from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    PermissionResultAllow,
    PermissionResultDeny,
    ResultMessage,
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
DOWNLOAD_DAILY_LIMIT_BYTES = 5 * 1024 * 1024  # 5 MB per user per day

# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
Sos el asistente de EdificIA, una plataforma de factibilidad urbanistica \
de Buenos Aires (CABA). Ayudas a usuarios a evaluar oportunidades de \
desarrollo inmobiliario.

- Responde siempre en espanol.
- Antes de tu primera consulta SQL, usa `schema` para conocer las tablas \
  y columnas disponibles.
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
    "Mostrar una vista HTML al usuario (tabla, grafico, mapa, etc). "
    "El HTML se renderiza en el frontend.",
    {"title": str, "html": str},
)
async def tool_render_html(args: dict[str, Any]) -> dict[str, Any]:
    """Store HTML for the SSE stream to pick up."""
    title: str = args.get("title", "Vista")
    html: str = args.get("html", "")
    if _active_session_id:
        _pending_renders[_active_session_id].append({"title": title, "html": html})
    return _tool_text(f"HTML renderizado correctamente (render_id={render_id})")


# In-memory download byte counters: {user_id: {date_str: bytes_used}}
_download_usage: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))


@tool(
    "create_download",
    "Crear un archivo descargable para el usuario. Limite: 5 MB/dia por usuario. "
    "Tipos comunes: text/csv, application/json, text/plain.",
    {"filename": str, "content": str, "mime_type": str},
)
async def tool_create_download(args: dict[str, Any]) -> dict[str, Any]:
    """Write content to a temp file and return a download URL."""
    filename: str = args.get("filename", "archivo.txt")
    content: str = args.get("content", "")
    mime_type: str = args.get("mime_type", "text/plain")

    # Sanitize filename
    safe_name = Path(filename).name
    if not safe_name:
        safe_name = "archivo.txt"

    content_bytes = content.encode("utf-8")

    # Check daily limit (use a placeholder user_id — real tracking via session)
    today = time.strftime("%Y-%m-%d")
    user_key = "global"  # Simplified; could be per-session
    used = _download_usage[user_key][today]
    if used + len(content_bytes) > DOWNLOAD_DAILY_LIMIT_BYTES:
        remaining = max(0, DOWNLOAD_DAILY_LIMIT_BYTES - used)
        return _tool_error(
            f"Limite diario de descargas excedido. "
            f"Quedan {remaining} bytes de {DOWNLOAD_DAILY_LIMIT_BYTES}."
        )

    # Write to temp dir with UUID prefix
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    unique_name = f"{uuid.uuid4().hex[:12]}_{safe_name}"
    file_path = DOWNLOADS_DIR / unique_name
    file_path.write_bytes(content_bytes)

    _download_usage[user_key][today] += len(content_bytes)

    download_url = f"/api/downloads/{unique_name}"
    return _tool_text(
        json.dumps(
            {"url": download_url, "filename": safe_name, "mime_type": mime_type},
            ensure_ascii=False,
        )
    )


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
    tools=[tool_sql, tool_schema, tool_http, tool_render_html, tool_create_download],
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
    client: ClaudeSDKClient, message: str, session_id: str = ""
) -> AsyncIterator[str]:
    """Send a message to the agent and yield SSE event strings.

    Yields serialized SSE events for each piece of the agent response:
    text chunks, rendered HTML artifacts, errors, and a final "done" event.
    """
    global _active_session_id
    total_input_tokens = 0
    total_output_tokens = 0

    # Initialize per-request render state
    _active_session_id = session_id
    _pending_renders.pop(session_id, None)

    try:
        await client.query(message)

        async for msg in client.receive_response():
            if isinstance(msg, AssistantMessage):
                # Track usage
                if msg.usage:
                    total_input_tokens += msg.usage.get("input_tokens", 0)
                    total_output_tokens += msg.usage.get("output_tokens", 0)

                for block in msg.content:
                    if isinstance(block, TextBlock):
                        yield SSEEvent("text", block.text).serialize()

            elif isinstance(msg, ResultMessage):
                # Emit any pending render_html artifacts from this turn
                for render_data in _pending_renders.pop(session_id, []):
                    yield SSEEvent("artifact", render_data).serialize()

            elif isinstance(msg, SystemMessage):
                # System messages (init, etc.) — skip
                pass

    except Exception as exc:
        yield SSEEvent("error", str(exc)).serialize()
    finally:
        _active_session_id = None
        _pending_renders.pop(session_id, None)

    yield SSEEvent(
        "done",
        {
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
        },
    ).serialize()


# ---------------------------------------------------------------------------
# Download cleanup
# ---------------------------------------------------------------------------


def cleanup_old_downloads(max_age_seconds: int = 3600) -> int:
    """Remove download files older than max_age_seconds. Returns count."""
    # Prune stale date keys from download usage tracking
    today = time.strftime("%Y-%m-%d")
    stale_keys = [k for k in _download_usage if k != today]
    for k in stale_keys:
        del _download_usage[k]

    if not DOWNLOADS_DIR.exists():
        return 0
    now = time.time()
    removed = 0
    for f in DOWNLOADS_DIR.iterdir():
        if f.is_file() and (now - f.stat().st_mtime) > max_age_seconds:
            f.unlink(missing_ok=True)
            removed += 1
    return removed
