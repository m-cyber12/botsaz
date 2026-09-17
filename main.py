"""
Rubika Bot Builder — Backend v2
================================
A full-featured FastAPI backend for building & managing Rubika bots.

Features:
- Multi-bot connect / disconnect with background polling (one task per bot)
- Auto-replies: /start welcome, custom commands, keyword triggers, fallback
- Chat keypad + inline buttons support (Rubika keypad format)
- Button / callback handling
- Per-bot stats, chat/user tracking, in-memory logs + SSE live stream
- Manual send, broadcast, block/unblock chats
- Export / import config as JSON
- JSON file persistence (survives restarts) + auto-resume polling
- Serves the frontend (index.html) from / when present
- Persian error messages, token masking in logs, health endpoint

Run:
    pip install -r requirements.txt
    uvicorn main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Configuration (env overridable)
# ---------------------------------------------------------------------------

API_BASE: str = os.getenv("RUBIKA_API_BASE", "https://botapi.rubika.ir/v3").rstrip("/")
CORS_ORIGINS: List[str] = [
    o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()
]
DATA_FILE: str = os.getenv("DATA_FILE", str(Path(__file__).parent / "bots_data.json"))
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
POLL_LIMIT: int = int(os.getenv("POLL_LIMIT", "100"))
POLL_INTERVAL: float = float(os.getenv("POLL_INTERVAL", "0.6"))
POLL_ERROR_RETRY: float = float(os.getenv("POLL_ERROR_RETRY", "5"))
MAX_LOGS_PER_BOT: int = int(os.getenv("MAX_LOGS_PER_BOT", "300"))
MAX_CHATS_PER_BOT: int = int(os.getenv("MAX_CHATS_PER_BOT", "2000"))
MAX_RECENT_MESSAGES: int = int(os.getenv("MAX_RECENT_MESSAGES", "60"))

VERSION = "2.0.0"
SERVICE_NAME = "rubika-bot-builder"
START_TIME = time.time()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(SERVICE_NAME)


def mask_token(token: str) -> str:
    """Never log full tokens."""
    t = (token or "").strip()
    if len(t) <= 8:
        return "***"
    return f"***{t[-6:]}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_ts() -> float:
    return time.time()


# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

bots: Dict[str, Dict[str, Any]] = {}
tasks: Dict[str, asyncio.Task] = {}
offsets: Dict[str, Optional[str]] = {}
bot_logs: Dict[str, Deque[Dict[str, Any]]] = {}
_save_lock = asyncio.Lock()
_http_client: Optional[httpx.AsyncClient] = None


def default_bot_config(token: str) -> Dict[str, Any]:
    return {
        "token": token,
        "welcome": "سلام! 👋\nبه ربات ما خوش آمدی.\n\nبرای شروع پیام خودت را ارسال کن.",
        "fallback": "پیامت دریافت شد. 🤖",
        "unknown_command": "دستور ناشناخته است! /help را بفرست. 🤔",
        "help_text": "",  # empty => auto-generated from commands
        "enabled": True,
        "auto_reply": True,
        "typing_delay": 0.3,
        "show_menu_on_start": True,
        "attach_menu_to_all": False,
        "commands": [
            {"trigger": "/help", "response": ""},  # empty => auto help
            {"trigger": "/about", "response": "🤖 ساخته شده با ربات‌ساز روبیکا ✨"},
        ],
        "keywords": [
            {"keyword": "سلام", "response": "سلام عزیز! 👋 چطور می‌تونم کمکت کنم؟", "match": "starts"},
            {"keyword": "مرسی", "response": "خواهش می‌کنم! 🌹", "match": "contains"},
        ],
        # Each inner list is a row of buttons (labels). Empty => no keypad.
        "chat_keypad": [["🚀 شروع", "ℹ️ راهنما"]],
        "inline_buttons": [],
        "blocked_chats": [],
        "bot": {},
        "stats": {
            "messages_received": 0,
            "messages_sent": 0,
            "commands_used": 0,
            "keywords_matched": 0,
            "buttons_pressed": 0,
            "errors": 0,
            "started_at": now_iso(),
            "last_message_at": None,
        },
        "chats": {},  # chat_id -> {count, first_seen, last_seen, last_text, name}
        "recent": [],  # recent messages [{chat_id, text, reply, at, kind}]
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }


def push_log(token: str, level: str, message: str) -> None:
    dq = bot_logs.setdefault(token, deque(maxlen=MAX_LOGS_PER_BOT))
    dq.append({"at": now_iso(), "level": level, "message": message})
    # Mirror important logs to server log with masked token
    if level in ("error", "warning"):
        getattr(logger, level if level != "error" else "error")(
            "[%s] %s", mask_token(token), message
        )


def record_recent(token: str, entry: Dict[str, Any]) -> None:
    cfg = bots.get(token)
    if not cfg:
        return
    recent = cfg.setdefault("recent", [])
    recent.append(entry)
    if len(recent) > MAX_RECENT_MESSAGES:
        del recent[: len(recent) - MAX_RECENT_MESSAGES]


def touch_chat(token: str, chat_id: str, text: Optional[str], name: Optional[str]) -> None:
    cfg = bots.get(token)
    if not cfg:
        return
    chats = cfg.setdefault("chats", {})
    if chat_id not in chats and len(chats) >= MAX_CHATS_PER_BOT:
        # Evict oldest to bound memory
        try:
            oldest = min(chats.items(), key=lambda kv: kv[1].get("last_seen", ""))
            chats.pop(oldest[0], None)
        except Exception:
            pass
    c = chats.setdefault(
        chat_id,
        {"count": 0, "first_seen": now_iso(), "last_seen": now_iso(), "last_text": "", "name": ""},
    )
    c["count"] = int(c.get("count", 0)) + 1
    c["last_seen"] = now_iso()
    if text:
        c["last_text"] = text[:300]
    if name and not c.get("name"):
        c["name"] = name[:80]


# ---------------------------------------------------------------------------
# Persistence
# ---------------------------------------------------------------------------

def _serializable_bots() -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for token, cfg in bots.items():
        # Don't persist volatile/derived fields excessively; keep chats trimmed.
        chats = cfg.get("chats", {})
        if isinstance(chats, dict) and len(chats) > 500:
            # Keep most recent 500 on disk
            items = sorted(
                chats.items(), key=lambda kv: str(kv[1].get("last_seen", "")), reverse=True
            )[:500]
            chats = dict(items)
        data[token] = {**cfg, "chats": chats, "recent": cfg.get("recent", [])[-30:]}
    return data


async def save_state() -> None:
    try:
        async with _save_lock:
            payload = {
                "version": VERSION,
                "saved_at": now_iso(),
                "bots": _serializable_bots(),
                "offsets": offsets,
            }
            path = Path(DATA_FILE)
            if path.parent and str(path.parent) not in ("", "."):
                path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            tmp.replace(path)
    except Exception:
        logger.exception("Failed to save state to %s", DATA_FILE)


def load_state() -> None:
    path = Path(DATA_FILE)
    if not path.exists():
        logger.info("No state file at %s — starting fresh", DATA_FILE)
        return
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        stored = payload.get("bots", {})
        if not isinstance(stored, dict):
            return
        for token, cfg in stored.items():
            if not isinstance(cfg, dict):
                continue
            base = default_bot_config(token)
            # Merge: stored values win, but ensure new keys exist.
            merged = {**base, **cfg}
            merged["token"] = token
            # Ensure nested defaults
            merged["stats"] = {**base["stats"], **(cfg.get("stats") or {})}
            bots[token] = merged
            bot_logs.setdefault(token, deque(maxlen=MAX_LOGS_PER_BOT))
        off = payload.get("offsets", {})
        if isinstance(off, dict):
            for k, v in off.items():
                offsets[k] = v
        logger.info("Loaded %d bot(s) from %s", len(bots), DATA_FILE)
    except Exception:
        logger.exception("Failed to load state from %s", DATA_FILE)


async def periodic_saver() -> None:
    while True:
        try:
            await asyncio.sleep(60)
            if bots:
                await save_state()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("periodic saver error")


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ConnectRequest(BaseModel):
    token: str = Field(default="", max_length=500)

    @field_validator("token")
    @classmethod
    def _strip(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("توکن وارد نشده است.")
        if len(v) < 6:
            raise ValueError("توکن معتبر نیست (خیلی کوتاه است).")
        return v


class CommandRule(BaseModel):
    trigger: str = Field(min_length=1, max_length=64)
    response: str = Field(default="", max_length=4000)

    @field_validator("trigger")
    @classmethod
    def _norm_trigger(cls, v: str) -> str:
        v = (v or "").strip()
        if not v.startswith("/"):
            v = "/" + v
        return v.lower()


class KeywordRule(BaseModel):
    keyword: str = Field(min_length=1, max_length=200)
    response: str = Field(default="", max_length=4000)
    match: str = Field(default="contains", pattern=r"^(contains|exact|starts)$")

    @field_validator("keyword")
    @classmethod
    def _strip_kw(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("کلیدواژه خالی است.")
        return v


class ConfigRequest(BaseModel):
    token: str
    welcome: Optional[str] = Field(default=None, max_length=4000)
    fallback: Optional[str] = Field(default=None, max_length=4000)
    unknown_command: Optional[str] = Field(default=None, max_length=2000)
    help_text: Optional[str] = Field(default=None, max_length=4000)
    enabled: Optional[bool] = None
    auto_reply: Optional[bool] = None
    typing_delay: Optional[float] = Field(default=None, ge=0, le=10)
    show_menu_on_start: Optional[bool] = None
    attach_menu_to_all: Optional[bool] = None
    commands: Optional[List[CommandRule]] = None
    keywords: Optional[List[KeywordRule]] = None
    chat_keypad: Optional[List[List[str]]] = None
    inline_buttons: Optional[List[List[str]]] = None
    blocked_chats: Optional[List[str]] = None

    @field_validator("token")
    @classmethod
    def _strip_token(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("توکن وارد نشده است.")
        return v


class SendRequest(BaseModel):
    token: str = ""
    chat_id: str = Field(default="", max_length=200)
    text: str = Field(default="", max_length=4000)

    @field_validator("token", "chat_id", "text")
    @classmethod
    def _strip(cls, v: str) -> str:
        return (v or "").strip()


class BroadcastRequest(BaseModel):
    token: str = ""
    text: str = Field(default="", max_length=4000)

    @field_validator("token", "text")
    @classmethod
    def _strip(cls, v: str) -> str:
        return (v or "").strip()


class ToggleRequest(BaseModel):
    token: str
    enabled: bool


class BlockRequest(BaseModel):
    token: str = ""
    chat_id: str = Field(default="", max_length=200)
    blocked: bool = True


class ImportRequest(BaseModel):
    token: str
    config: Dict[str, Any]


# ---------------------------------------------------------------------------
# Rubika API helpers
# ---------------------------------------------------------------------------

async def get_http() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=15.0, read=35.0, write=15.0, pool=15.0),
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=20),
        )
    return _http_client


async def rubika(
    method: str,
    token: str,
    data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    url = f"{API_BASE}/{token}/{method}"
    client = await get_http()
    try:
        response = await client.post(url, json=data or {})
    except httpx.TimeoutException as exc:
        raise RuntimeError(f"اتصال به روبیکا timeout شد ({method}).") from exc
    except httpx.RequestError as exc:
        raise RuntimeError(f"خطای شبکه در اتصال به روبیکا: {exc}") from exc

    logger.info("Rubika API %s -> HTTP %s (%s)", method, response.status_code, mask_token(token))

    if response.status_code >= 400:
        body = response.text[:500]
        raise RuntimeError(f"API روبیکا خطای HTTP {response.status_code} داد. {body}")

    try:
        result = response.json()
    except Exception as exc:
        raise RuntimeError("پاسخ API روبیکا JSON معتبر نیست.") from exc

    if not isinstance(result, dict):
        raise RuntimeError("پاسخ API روبیکا معتبر نیست.")

    # Rubika sometimes returns {status: "ERROR", ...} with HTTP 200
    status = str(result.get("status", "")).upper()
    if status and status not in ("OK", "SUCCESS"):
        err = (
            result.get("message")
            or result.get("error")
            or result.get("dev_message")
            or result
        )
        raise RuntimeError(f"خطای API روبیکا ({method}): {str(err)[:300]}")

    return result


def get_result(data: Dict[str, Any]) -> Dict[str, Any]:
    result = data.get("result")
    if isinstance(result, dict):
        return result
    return data


def extract_bot_info(data: Dict[str, Any]) -> Dict[str, Any]:
    result = get_result(data)
    bot = result.get("bot")
    if isinstance(bot, dict):
        return bot
    nested = result.get("data")
    if isinstance(nested, dict):
        bot = nested.get("bot")
        if isinstance(bot, dict):
            return bot
        # Some APIs return bot fields directly under data
        if any(k in nested for k in ("first_name", "username", "bot_id", "id")):
            return nested
    if any(k in result for k in ("first_name", "username", "bot_id", "id")):
        return {k: result[k] for k in ("first_name", "username", "bot_id", "id", "bio") if k in result}
    return {}


def extract_updates(data: Dict[str, Any]) -> list:
    result = get_result(data)
    candidates = [result.get("updates"), data.get("updates")]
    for container in (result.get("data"), data.get("data")):
        if isinstance(container, dict):
            candidates.append(container.get("updates"))
    for value in candidates:
        if isinstance(value, list):
            return value
    # Some shapes: result is a list directly
    if isinstance(result, list):
        return result
    return []


def extract_next_offset(data: Dict[str, Any]) -> Optional[str]:
    result = get_result(data)
    candidates = [result.get("next_offset_id"), data.get("next_offset_id")]
    for container in (result.get("data"), data.get("data")):
        if isinstance(container, dict):
            candidates.append(container.get("next_offset_id"))
    for value in candidates:
        if value is not None and str(value) != "":
            return str(value)
    return None


def find_value(obj: Any, keys: Tuple[str, ...], _depth: int = 0) -> Any:
    if _depth > 8:
        return None
    if isinstance(obj, dict):
        for key in keys:
            if key in obj and obj[key] is not None and obj[key] != "":
                return obj[key]
        for value in obj.values():
            found = find_value(value, keys, _depth + 1)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = find_value(item, keys, _depth + 1)
            if found is not None:
                return found
    return None


def extract_update_kind(update: Dict[str, Any]) -> Dict[str, Any]:
    """Parse any Rubika update shape into a normalized dict.

    Returns: {kind, chat_id, text, message_id, button_id, sender_name, raw_type}
    kind ∈ {message, callback, unknown}
    """
    out: Dict[str, Any] = {
        "kind": "unknown",
        "chat_id": None,
        "text": None,
        "message_id": None,
        "button_id": None,
        "sender_name": None,
        "raw_type": None,
    }
    if not isinstance(update, dict):
        return out

    raw_type = update.get("type") or update.get("update_type")
    out["raw_type"] = raw_type
    normalized = str(raw_type or "").lower().replace("-", "_") if raw_type else ""

    callback_markers = ("callback", "inline", "button", "keypad")
    is_callback = any(m in normalized for m in callback_markers)

    # Direct callback payload shapes
    aux = update.get("aux_data") or update.get("auxData") or {}
    inline_msg = update.get("inline_message") or update.get("inlineMessage") or {}
    cb_data = update.get("callback_data") or update.get("callbackData")

    button_id = None
    if isinstance(aux, dict):
        button_id = aux.get("button_id") or aux.get("buttonId")
    if not button_id and isinstance(inline_msg, dict):
        button_id = inline_msg.get("button_id") or inline_msg.get("buttonId")
        aux2 = inline_msg.get("aux_data") or inline_msg.get("auxData")
        if not button_id and isinstance(aux2, dict):
            button_id = aux2.get("button_id") or aux2.get("buttonId")
    if not button_id and cb_data is not None:
        button_id = cb_data
    if not button_id:
        button_id = find_value(update, ("button_id", "buttonId", "callback_data", "callbackData"))
    if button_id is not None:
        out["button_id"] = str(button_id)
        is_callback = True

    message = (
        update.get("new_message")
        or update.get("newMessage")
        or update.get("message")
        or update.get("updated_message")
        or {}
    )
    if not isinstance(message, dict):
        message = {}

    chat_id = (
        update.get("chat_id")
        or update.get("chatId")
        or update.get("object_guid")
        or update.get("chat_guid")
        or message.get("chat_id")
        or message.get("chatId")
        or message.get("object_guid")
        or message.get("chat_guid")
    )
    if chat_id is None:
        chat_id = find_value(update, ("chat_id", "chatId", "object_guid", "chat_guid", "peer_guid"))
    if chat_id is not None:
        out["chat_id"] = str(chat_id)

    text = (
        message.get("text")
        or message.get("message")
        or message.get("body")
        or message.get("caption")
        or update.get("text")
    )
    if text is None:
        text = find_value(update, ("text", "raw_text", "body", "caption"))
    if text is not None:
        try:
            out["text"] = str(text).strip() or None
        except Exception:
            out["text"] = None

    mid = message.get("message_id") or message.get("messageId") or update.get("message_id")
    if mid is not None:
        out["message_id"] = str(mid)

    sender = find_value(
        {"m": message, "u": update},
        ("sender_name", "first_name", "username", "sender_username"),
    )
    if sender is not None:
        out["sender_name"] = str(sender)[:80]

    # If it looks like a message update, mark as message even without explicit type.
    message_markers = ("new_message", "newmessage", "message", "updated_message", "")
    if normalized in message_markers and (out["chat_id"] or out["text"]):
        out["kind"] = "callback" if is_callback and out["button_id"] else "message"
    elif is_callback:
        out["kind"] = "callback"
    elif out["chat_id"] and (out["text"] is not None or out["button_id"]):
        out["kind"] = "message"

    return out


# Backwards-compatible wrapper used by older logic/tests
def extract_message(update: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    parsed = extract_update_kind(update)
    if parsed["kind"] not in ("message", "callback"):
        return None, None
    return parsed["chat_id"], parsed["text"]


# ---------------------------------------------------------------------------
# Keypads (Rubika format)
# ---------------------------------------------------------------------------

def _button_id(label: str, r: int, c: int) -> str:
    h = hashlib.md5(f"{r}:{c}:{label}".encode("utf-8")).hexdigest()[:8]
    return f"btn_{r}_{c}_{h}"


def build_keypad(rows: Optional[List[List[str]]]) -> Optional[Dict[str, Any]]:
    """Convert [["A","B"],["C"]] to Rubika keypad dict. Returns None if empty."""
    if not rows:
        return None
    clean_rows: List[Dict[str, Any]] = []
    for r, row in enumerate(rows):
        if not isinstance(row, list):
            continue
        buttons = []
        for c, label in enumerate(row):
            label = str(label or "").strip()
            if not label:
                continue
            buttons.append(
                {"id": _button_id(label, r, c), "type": "Simple", "button_text": label[:60]}
            )
        if buttons:
            clean_rows.append({"buttons": buttons})
    if not clean_rows:
        return None
    return {"rows": clean_rows}


def sanitize_keypad_rows(rows: Any, max_rows: int = 6, max_cols: int = 4) -> List[List[str]]:
    if not isinstance(rows, list):
        return []
    out: List[List[str]] = []
    for row in rows[:max_rows]:
        if isinstance(row, str):
            row = [row]
        if not isinstance(row, list):
            continue
        clean = [str(x or "").strip()[:60] for x in row[:max_cols]]
        clean = [x for x in clean if x]
        if clean:
            out.append(clean)
    return out


# ---------------------------------------------------------------------------
# Reply resolution
# ---------------------------------------------------------------------------

def build_help_text(cfg: Dict[str, Any]) -> str:
    custom = (cfg.get("help_text") or "").strip()
    if custom:
        return custom
    lines = ["📖 راهنما:", ""]
    commands = cfg.get("commands") or []
    shown = 0
    for cmd in commands:
        if not isinstance(cmd, dict):
            continue
        trig = str(cmd.get("trigger", "")).strip()
        if not trig or trig == "/help":
            continue
        lines.append(f"• {trig}")
        shown += 1
    if shown == 0:
        lines.append("• /start — شروع")
        lines.append("• /help — همین راهنما")
    else:
        lines.append("")
        lines.append("برای شروع /start را بفرستید.")
    keywords = cfg.get("keywords") or []
    if keywords:
        lines.append("")
        lines.append("💡 می‌توانید مستقیماً پیام بدهید؛ ربات جواب می‌دهد.")
    return "\n".join(lines)


def resolve_reply(
    cfg: Dict[str, Any], text: Optional[str], button_id: Optional[str] = None
) -> Tuple[Optional[str], str]:
    """Returns (reply_text_or_None, rule_kind).

    rule_kind ∈ {start, help, command, keyword, button, fallback, blocked, disabled, silent}
    """
    if not cfg.get("enabled", True):
        return None, "disabled"

    raw = (text or "").strip()
    lowered = raw.lower()

    commands: List[Dict[str, Any]] = cfg.get("commands") or []
    cmd_map = {}
    for cmd in commands:
        if isinstance(cmd, dict) and cmd.get("trigger"):
            cmd_map[str(cmd["trigger"]).strip().lower()] = str(cmd.get("response") or "")

    # 1) /start
    if lowered == "/start":
        return cfg.get("welcome", "سلام! 👋"), "start"

    # 2) /help
    if lowered == "/help":
        if "/help" in cmd_map and cmd_map["/help"].strip():
            return cmd_map["/help"], "command"
        return build_help_text(cfg), "help"

    # 3) custom commands (exact, case-insensitive, ignore args after space)
    if lowered.startswith("/"):
        first = lowered.split()[0] if lowered.split() else lowered
        if first in cmd_map:
            resp = cmd_map[first].strip()
            if first == "/help" and not resp:
                return build_help_text(cfg), "help"
            return resp or cfg.get("fallback", ""), "command"
        return cfg.get("unknown_command", "دستور ناشناخته است!"), "command"

    # 4) button press without text (callback): try to match button label rules
    if button_id and not raw:
        return None, "button"  # caller decides generic ack

    # 5) keywords
    if raw:
        keywords: List[Dict[str, Any]] = cfg.get("keywords") or []
        for kw in keywords:
            if not isinstance(kw, dict):
                continue
            key = str(kw.get("keyword", "") or "").strip()
            if not key:
                continue
            mode = str(kw.get("match", "contains") or "contains").lower()
            kl, tl = key.lower(), lowered
            hit = (
                (tl == kl)
                if mode == "exact"
                else (tl.startswith(kl) if mode == "starts" else (kl in tl))
            )
            if hit:
                return str(kw.get("response", "") or ""), "keyword"

    # 6) fallback
    if not cfg.get("auto_reply", True):
        return None, "silent"
    if raw:
        return cfg.get("fallback", "پیامت دریافت شد. 🤖"), "fallback"
    return None, "silent"


# ---------------------------------------------------------------------------
# Sending
# ---------------------------------------------------------------------------

async def send_text(
    token: str,
    chat_id: str,
    text: str,
    chat_keypad: Optional[Dict[str, Any]] = None,
    inline_keypad: Optional[Dict[str, Any]] = None,
    reply_to: Optional[str] = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"chat_id": chat_id, "text": text}
    if chat_keypad:
        payload["chat_keypad"] = chat_keypad
        payload["chat_keypad_type"] = "New"
    if inline_keypad:
        payload["inline_keypad"] = inline_keypad
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    logger.info("Sending reply to chat_id=%s (%s): %r", chat_id, mask_token(token), text[:120])
    result = await rubika("sendMessage", token, payload)
    logger.info("sendMessage result (%s): %s", mask_token(token), str(result)[:400])
    return result


# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------

async def handle_update(token: str, update: Dict[str, Any]) -> None:
    cfg = bots.get(token)
    if cfg is None:
        return
    parsed = extract_update_kind(update)
    logger.info("RAW UPDATE (%s): %s", mask_token(token), str(update)[:800])
    push_log(token, "info", f"آپدیت: {str(update)[:300]}")

    if parsed["kind"] == "unknown":
        push_log(token, "warning", f"نوع آپدیت ناشناخته نادیده گرفته شد: {parsed.get('raw_type')}")
        return

    chat_id = parsed["chat_id"]
    text = parsed["text"]
    button_id = parsed["button_id"]
    sender = parsed["sender_name"]

    if not chat_id:
        push_log(token, "warning", "chat_id پیدا نشد؛ آپدیت نادیده گرفته شد.")
        return

    # Blocked?
    if chat_id in (cfg.get("blocked_chats") or []):
        push_log(token, "info", f"پیام کاربر بلاک‌شده نادیده گرفته شد: {chat_id}")
        return

    stats = cfg.setdefault("stats", {})
    stats["messages_received"] = int(stats.get("messages_received", 0)) + 1
    stats["last_message_at"] = now_iso()
    touch_chat(token, chat_id, text or (f"[button:{button_id}]" if button_id else ""), sender)

    reply, kind = resolve_reply(cfg, text, button_id)

    # Button press with no text: acknowledge + try keyword match on button label is
    # impossible (we only get button_id). Send a generic helpful reply if auto_reply.
    if kind == "button":
        stats["buttons_pressed"] = int(stats.get("buttons_pressed", 0)) + 1
        if cfg.get("auto_reply", True):
            reply = cfg.get("fallback", "دکمه دریافت شد. ✅")
            kind = "fallback"
        else:
            record_recent(token, {"chat_id": chat_id, "text": f"[button:{button_id}]", "reply": None, "at": now_iso(), "kind": "button"})
            return

    if reply is None:
        reason = {"disabled": "ربات غیرفعال است", "silent": "پاسخ خودکار خاموش است"}.get(kind, kind)
        push_log(token, "info", f"بدون پاسخ ({reason}) — chat={chat_id} text={text!r}")
        record_recent(token, {"chat_id": chat_id, "text": text or "", "reply": None, "at": now_iso(), "kind": kind})
        return

    if kind == "command":
        stats["commands_used"] = int(stats.get("commands_used", 0)) + 1
    elif kind == "keyword":
        stats["keywords_matched"] = int(stats.get("keywords_matched", 0)) + 1

    # Typing delay (feels human)
    delay = float(cfg.get("typing_delay", 0) or 0)
    if delay > 0:
        await asyncio.sleep(min(delay, 10))

    # Attach keypads
    chat_kp = None
    inline_kp = None
    if kind == "start" and cfg.get("show_menu_on_start", True):
        chat_kp = build_keypad(cfg.get("chat_keypad"))
        inline_kp = build_keypad(cfg.get("inline_buttons"))
    elif cfg.get("attach_menu_to_all"):
        chat_kp = build_keypad(cfg.get("chat_keypad"))

    try:
        await send_text(token, chat_id, reply, chat_keypad=chat_kp, inline_keypad=inline_kp)
        stats["messages_sent"] = int(stats.get("messages_sent", 0)) + 1
        push_log(token, "info", f"پاسخ ({kind}) به {chat_id}: {reply[:120]}")
        record_recent(token, {"chat_id": chat_id, "text": text or (f"[button:{button_id}]" if button_id else ""), "reply": reply[:500], "at": now_iso(), "kind": kind})
    except Exception as exc:
        stats["errors"] = int(stats.get("errors", 0)) + 1
        push_log(token, "error", f"خطا در ارسال پاسخ به {chat_id}: {exc}")
        logger.exception("send failed (%s)", mask_token(token))


async def poll_bot(token: str) -> None:
    logger.info("Polling started for bot %s", mask_token(token))
    push_log(token, "info", "پولینگ شروع شد ✅")

    while True:
        try:
            if token not in bots:
                logger.info("Bot %s no longer exists. Polling stopped.", mask_token(token))
                return

            payload: Dict[str, Any] = {"limit": POLL_LIMIT}
            offset_id = offsets.get(token)
            if offset_id:
                payload["offset_id"] = offset_id

            data = await rubika("getUpdates", token, payload)

            # IMPORTANT: process updates independently of next_offset_id.
            updates = extract_updates(data)
            if updates:
                logger.info("Received %d update(s) (%s)", len(updates), mask_token(token))

            for update in updates:
                try:
                    if isinstance(update, dict):
                        await handle_update(token, update)
                except Exception:
                    logger.exception("Error while processing one update (%s)", mask_token(token))
                await asyncio.sleep(0.05)

            next_offset = extract_next_offset(data)
            if next_offset:
                offsets[token] = next_offset

            # Light persistence of offsets every loop is cheap enough at this rate;
            # full state saved periodically + on config change.
            await asyncio.sleep(POLL_INTERVAL)

        except asyncio.CancelledError:
            logger.info("Polling cancelled (%s)", mask_token(token))
            push_log(token, "info", "پولینگ متوقف شد.")
            raise
        except Exception:
            logger.exception("Polling error (%s); retrying in %ss", mask_token(token), POLL_ERROR_RETRY)
            cfg = bots.get(token)
            if cfg is not None:
                stats = cfg.setdefault("stats", {})
                stats["errors"] = int(stats.get("errors", 0)) + 1
            push_log(token, "error", f"خطای پولینگ؛ تلاش مجدد بعد از {POLL_ERROR_RETRY} ثانیه")
            await asyncio.sleep(POLL_ERROR_RETRY)


def ensure_polling(token: str) -> bool:
    """Start polling task if not running. Returns True if (re)started."""
    old = tasks.get(token)
    if old is None or old.done():
        tasks[token] = asyncio.create_task(poll_bot(token))
        logger.info("Polling task created (%s)", mask_token(token))
        return True
    logger.info("Polling task already running (%s)", mask_token(token))
    return False


async def stop_polling(token: str) -> None:
    task = tasks.pop(token, None)
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()
    # Resume polling for all saved bots
    for token in list(bots.keys()):
        try:
            ensure_polling(token)
        except Exception:
            logger.exception("Failed to resume polling for %s", mask_token(token))
    saver = asyncio.create_task(periodic_saver())
    yield
    saver.cancel()
    try:
        await saver
    except asyncio.CancelledError:
        pass
    for token in list(tasks.keys()):
        await stop_polling(token)
    await save_state()
    global _http_client
    if _http_client is not None and not _http_client.is_closed:
        await _http_client.aclose()


app = FastAPI(title="Rubika Bot Builder", version=VERSION, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def _validation_error(request: Request, exc: RequestValidationError):
    msgs: List[str] = []
    for err in exc.errors():
        ctx = err.get("ctx") or {}
        # Pydantic wraps ValueError messages; prefer the raw Persian message.
        raw = ctx.get("error")
        msg = str(raw) if raw is not None else str(err.get("msg", ""))
        # Hide noisy English wrappers, keep Persian custom messages intact.
        if "Value error," in msg:
            msg = msg.split("Value error,", 1)[1].strip()
        loc = [str(x) for x in err.get("loc", []) if x not in ("body", "query")]
        msgs.append(f"{'-'.join(loc)}: {msg}" if loc else msg)
    detail = "؛ ".join(dict.fromkeys(msgs)) or "ورودی نامعتبر است."
    return JSONResponse(status_code=400, content={"ok": False, "detail": detail})


@app.exception_handler(Exception)
async def _unhandled(request: Request, exc: Exception):
    if isinstance(exc, HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"ok": False, "detail": exc.detail})
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(status_code=500, content={"ok": False, "detail": "خطای داخلی سرور."})


def require_bot(token: str) -> Dict[str, Any]:
    token = (token or "").strip()
    if not token:
        raise HTTPException(status_code=400, detail="توکن وارد نشده است.")
    cfg = bots.get(token)
    if cfg is None:
        raise HTTPException(status_code=404, detail="ربات پیدا نشد؛ ابتدا متصل کنید.")
    return cfg


def public_bot_summary(token: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    task = tasks.get(token)
    return {
        "token_masked": mask_token(token),
        "connected": True,
        "running": bool(task is not None and not task.done()),
        "enabled": cfg.get("enabled", True),
        "auto_reply": cfg.get("auto_reply", True),
        "bot": cfg.get("bot", {}),
        "stats": {
            **cfg.get("stats", {}),
            "unique_users": len(cfg.get("chats", {}) or {}),
        },
        "counts": {
            "commands": len(cfg.get("commands") or []),
            "keywords": len(cfg.get("keywords") or []),
            "blocked": len(cfg.get("blocked_chats") or []),
            "chats": len(cfg.get("chats", {}) or {}),
        },
        "updated_at": cfg.get("updated_at"),
    }


# ---------------------------------------------------------------------------
# Frontend
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
async def serve_frontend():
    index = Path(__file__).parent / "index.html"
    if index.exists():
        return FileResponse(index, media_type="text/html; charset=utf-8")
    return {"ok": True, "service": SERVICE_NAME, "version": VERSION}


@app.get("/health")
async def health():
    return {
        "ok": True,
        "service": SERVICE_NAME,
        "version": VERSION,
        "time": now_iso(),
        "uptime_seconds": round(time.time() - START_TIME, 1),
        "bots_count": len(bots),
        "running_tasks": sum(1 for t in tasks.values() if not t.done()),
    }


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

@app.post("/connect")
async def connect(request: ConnectRequest):
    token = request.token.strip()

    try:
        data = await rubika("getMe", token, {})
    except Exception as exc:
        logger.exception("getMe error (%s)", mask_token(token))
        raise HTTPException(status_code=400, detail=f"اتصال به API روبیکا ناموفق بود: {exc}")

    bot_info = extract_bot_info(data)

    old = bots.get(token)
    base = default_bot_config(token)
    if old:
        # Preserve user settings on reconnect
        for key in (
            "welcome", "fallback", "unknown_command", "help_text", "enabled",
            "auto_reply", "typing_delay", "show_menu_on_start", "attach_menu_to_all",
            "commands", "keywords", "chat_keypad", "inline_buttons",
            "blocked_chats", "stats", "chats", "recent",
        ):
            if key in old:
                base[key] = old[key]
    base["bot"] = bot_info
    base["updated_at"] = now_iso()
    bots[token] = base
    bot_logs.setdefault(token, deque(maxlen=MAX_LOGS_PER_BOT))
    push_log(token, "info", "ربات متصل شد ✅")

    ensure_polling(token)
    await save_state()

    logger.info("Bot connected successfully (%s)", mask_token(token))
    return {
        "ok": True,
        "message": "ربات با موفقیت متصل شد.",
        "bot": bot_info,
        "config": {k: v for k, v in base.items() if k != "token"},
        "summary": public_bot_summary(token, base),
    }


@app.post("/disconnect")
async def disconnect(request: ConnectRequest):
    token = request.token.strip()
    await stop_polling(token)
    bots.pop(token, None)
    offsets.pop(token, None)
    # Keep logs for inspection after disconnect? Clear to free memory.
    bot_logs.pop(token, None)
    await save_state()
    logger.info("Bot disconnected (%s)", mask_token(token))
    return {"ok": True, "message": "اتصال ربات قطع شد."}


# ---------------------------------------------------------------------------
# Bots & config
# ---------------------------------------------------------------------------

@app.get("/bots")
async def list_bots():
    return {
        "ok": True,
        "count": len(bots),
        "bots": [public_bot_summary(t, c) for t, c in bots.items()],
    }


@app.get("/bot")
async def get_bot(token: str = Query(...)):
    cfg = require_bot(token.strip())
    task = tasks.get(token.strip())
    return {
        "ok": True,
        "summary": public_bot_summary(token.strip(), cfg),
        "config": {k: v for k, v in cfg.items() if k not in ("token", "chats", "recent")},
        "running": bool(task is not None and not task.done()),
    }


@app.post("/config")
async def update_config(request: ConfigRequest):
    cfg = require_bot(request.token)
    data = request.model_dump(exclude_unset=True)
    data.pop("token", None)

    # Normalize nested structures
    if data.get("commands") is not None:
        data["commands"] = [
            {"trigger": c.trigger, "response": c.response} for c in (request.commands or [])
        ][:100]
    if data.get("keywords") is not None:
        data["keywords"] = [
            {"keyword": k.keyword, "response": k.response, "match": k.match}
            for k in (request.keywords or [])
        ][:200]
    if data.get("chat_keypad") is not None:
        data["chat_keypad"] = sanitize_keypad_rows(request.chat_keypad)
    if data.get("inline_buttons") is not None:
        data["inline_buttons"] = sanitize_keypad_rows(request.inline_buttons)
    if data.get("blocked_chats") is not None:
        data["blocked_chats"] = [str(x).strip() for x in (request.blocked_chats or []) if str(x).strip()][:2000]

    for key, value in data.items():
        if value is not None:
            cfg[key] = value
    cfg["updated_at"] = now_iso()
    push_log(request.token, "info", "تنظیمات به‌روزرسانی شد ⚙️")
    await save_state()
    logger.info("Bot configuration updated (%s)", mask_token(request.token))
    return {"ok": True, "message": "تنظیمات با موفقیت ذخیره شد.", "summary": public_bot_summary(request.token, cfg)}


@app.post("/toggle")
async def toggle_bot(request: ToggleRequest):
    cfg = require_bot(request.token)
    cfg["enabled"] = bool(request.enabled)
    cfg["updated_at"] = now_iso()
    push_log(request.token, "info", "ربات فعال شد ✅" if request.enabled else "ربات غیرفعال شد ⏸️")
    await save_state()
    return {"ok": True, "enabled": cfg["enabled"], "message": "وضعیت ربات تغییر کرد."}


# ---------------------------------------------------------------------------
# Messaging
# ---------------------------------------------------------------------------

@app.post("/send")
async def send_message(request: SendRequest):
    cfg = require_bot(request.token)
    if not request.chat_id or not request.text:
        raise HTTPException(status_code=400, detail="chat_id و text الزامی هستند.")
    try:
        result = await send_text(request.token, request.chat_id, request.text)
        stats = cfg.setdefault("stats", {})
        stats["messages_sent"] = int(stats.get("messages_sent", 0)) + 1
        push_log(request.token, "info", f"ارسال دستی به {request.chat_id}: {request.text[:120]}")
        await save_state()
        return {"ok": True, "message": "پیام ارسال شد.", "result": result}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"ارسال ناموفق بود: {exc}")


@app.post("/broadcast")
async def broadcast(request: BroadcastRequest):
    if not (request.text or "").strip():
        raise HTTPException(status_code=400, detail="متن پیام همگانی خالی است.")
    cfg = require_bot(request.token)
    chats = cfg.get("chats", {}) or {}
    if not chats:
        raise HTTPException(status_code=400, detail="هنوز کاربری با ربات گفتگو نکرده است.")
    sent, failed = 0, 0
    errors: List[str] = []
    for chat_id in list(chats.keys()):
        if chat_id in (cfg.get("blocked_chats") or []):
            continue
        try:
            await send_text(request.token, chat_id, request.text)
            sent += 1
        except Exception as exc:
            failed += 1
            errors.append(f"{chat_id}: {str(exc)[:100]}")
        await asyncio.sleep(0.25)  # be kind to the API
    stats = cfg.setdefault("stats", {})
    stats["messages_sent"] = int(stats.get("messages_sent", 0)) + sent
    push_log(request.token, "info", f"همگانی: موفق={sent} ناموفق={failed}")
    await save_state()
    return {
        "ok": True,
        "message": f"همگانی تمام شد. موفق: {sent} | ناموفق: {failed}",
        "sent": sent,
        "failed": failed,
        "errors": errors[:10],
    }


# ---------------------------------------------------------------------------
# Stats / chats / logs
# ---------------------------------------------------------------------------

@app.get("/stats")
async def stats(token: Optional[str] = Query(default=None)):
    if token:
        cfg = require_bot(token.strip())
        s = dict(cfg.get("stats", {}))
        s["unique_users"] = len(cfg.get("chats", {}) or {})
        s["total_chats"] = len(cfg.get("chats", {}) or {})
        return {"ok": True, "stats": s}
    # Global
    total_rx = sum(int(c.get("stats", {}).get("messages_received", 0)) for c in bots.values())
    total_tx = sum(int(c.get("stats", {}).get("messages_sent", 0)) for c in bots.values())
    users = sum(len(c.get("chats", {}) or {}) for c in bots.values())
    return {
        "ok": True,
        "stats": {
            "bots": len(bots),
            "messages_received": total_rx,
            "messages_sent": total_tx,
            "unique_users": users,
            "uptime_seconds": round(time.time() - START_TIME, 1),
        },
    }


@app.get("/chats")
async def list_chats(token: str = Query(...), limit: int = Query(100, ge=1, le=500)):
    cfg = require_bot(token.strip())
    chats = cfg.get("chats", {}) or {}
    blocked = set(cfg.get("blocked_chats") or [])
    items = [
        {"chat_id": cid, **info, "blocked": cid in blocked}
        for cid, info in chats.items()
    ]
    items.sort(key=lambda x: str(x.get("last_seen", "")), reverse=True)
    return {"ok": True, "count": len(items), "chats": items[:limit]}


@app.get("/recent")
async def recent_messages(token: str = Query(...), limit: int = Query(30, ge=1, le=100)):
    cfg = require_bot(token.strip())
    recent = cfg.get("recent", []) or []
    return {"ok": True, "count": len(recent), "recent": recent[-limit:][::-1]}


@app.get("/logs")
async def get_logs(token: str = Query(...), limit: int = Query(100, ge=1, le=300)):
    require_bot(token.strip())
    dq = bot_logs.get(token.strip(), deque())
    items = list(dq)[-limit:][::-1]
    return {"ok": True, "count": len(items), "logs": items}


@app.delete("/logs")
async def clear_logs(token: str = Query(...)):
    require_bot(token.strip())
    bot_logs[token.strip()] = deque(maxlen=MAX_LOGS_PER_BOT)
    return {"ok": True, "message": "لاگ‌ها پاک شدند."}


@app.post("/block")
async def block_chat(request: BlockRequest):
    if not (request.chat_id or "").strip():
        raise HTTPException(status_code=400, detail="chat_id خالی است.")
    cfg = require_bot(request.token)
    blocked = cfg.setdefault("blocked_chats", [])
    cid = request.chat_id.strip()
    if request.blocked:
        if cid not in blocked:
            blocked.append(cid)
        msg = f"کاربر {cid} بلاک شد."
    else:
        if cid in blocked:
            blocked.remove(cid)
        msg = f"کاربر {cid} آنبلاک شد."
    cfg["updated_at"] = now_iso()
    push_log(request.token, "info", msg)
    await save_state()
    return {"ok": True, "message": msg, "blocked_chats": blocked}


# ---------------------------------------------------------------------------
# Export / Import
# ---------------------------------------------------------------------------

EXPORT_KEYS = (
    "welcome", "fallback", "unknown_command", "help_text", "enabled",
    "auto_reply", "typing_delay", "show_menu_on_start", "attach_menu_to_all",
    "commands", "keywords", "chat_keypad", "inline_buttons", "blocked_chats",
)


@app.post("/export")
async def export_config(request: ConnectRequest):
    cfg = require_bot(request.token.strip())
    data = {k: cfg.get(k) for k in EXPORT_KEYS}
    data["_meta"] = {"service": SERVICE_NAME, "version": VERSION, "exported_at": now_iso()}
    return {"ok": True, "config": data}


@app.post("/import")
async def import_config(request: ImportRequest):
    cfg = require_bot(request.token.strip())
    incoming = request.config or {}
    if not isinstance(incoming, dict):
        raise HTTPException(status_code=400, detail="فایل کانفیگ معتبر نیست.")
    applied = []
    for key in EXPORT_KEYS:
        if key in incoming and incoming[key] is not None:
            if key in ("chat_keypad", "inline_buttons"):
                cfg[key] = sanitize_keypad_rows(incoming[key])
            elif key == "commands":
                cmds = []
                for c in (incoming[key] or [])[:100]:
                    if isinstance(c, dict) and c.get("trigger"):
                        t = str(c["trigger"]).strip()
                        if not t.startswith("/"):
                            t = "/" + t
                        cmds.append({"trigger": t.lower(), "response": str(c.get("response", ""))[:4000]})
                cfg[key] = cmds
            elif key == "keywords":
                kws = []
                for k in (incoming[key] or [])[:200]:
                    if isinstance(k, dict) and str(k.get("keyword", "")).strip():
                        m = str(k.get("match", "contains")).lower()
                        if m not in ("contains", "exact", "starts"):
                            m = "contains"
                        kws.append({
                            "keyword": str(k["keyword"]).strip()[:200],
                            "response": str(k.get("response", ""))[:4000],
                            "match": m,
                        })
                cfg[key] = kws
            else:
                cfg[key] = incoming[key]
            applied.append(key)
    cfg["updated_at"] = now_iso()
    push_log(request.token, "info", f"ایمپورت تنظیمات انجام شد: {', '.join(applied) or 'هیچ'}")
    await save_state()
    return {"ok": True, "message": f"ایمپورت شد ({len(applied)} بخش).", "applied": applied}


# ---------------------------------------------------------------------------
# Live events (SSE): stats + latest log lines every few seconds
# ---------------------------------------------------------------------------

@app.get("/events")
async def events(token: str = Query(...)):
    cfg = require_bot(token.strip())

    async def gen():
        last_log_count = 0
        try:
            yield "retry: 3000\n\n"
            while True:
                if token.strip() not in bots:
                    yield 'event: done\ndata: {"ok": false}\n\n'
                    return
                c = bots.get(token.strip(), {})
                s = dict(c.get("stats", {}))
                s["unique_users"] = len(c.get("chats", {}) or {})
                dq = bot_logs.get(token.strip(), deque())
                new_logs = list(dq)[last_log_count:]
                last_log_count = len(dq)
                payload = json.dumps(
                    {"ok": True, "stats": s, "logs": new_logs[-10:], "at": now_iso()},
                    ensure_ascii=False,
                )
                yield f"data: {payload}\n\n"
                await asyncio.sleep(3)
        except asyncio.CancelledError:
            return

    return StreamingResponse(gen(), media_type="text/event-stream")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,
    )
