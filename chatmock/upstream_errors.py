from __future__ import annotations

import re
from typing import Any, overload

_TRUNCATION_MARKER = "...(truncated)"
_MAX_SNIPPET_CHARS = 160
_MAX_MESSAGE_CHARS = 500
_REDACTION_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"Authorization:\s*Bearer\s+[^\s,;]+", re.IGNORECASE), "Authorization: Bearer [redacted]"),
    (re.compile(r"\bBearer\s+(?!\[redacted\]\b)[^\s,;]+", re.IGNORECASE), "Bearer [redacted]"),
    (re.compile(r"\bsk-[A-Za-z0-9_-]+\b"), "[redacted]"),
    (re.compile(r"\b(session_id|access_token|token)=([^&\s,;]+)", re.IGNORECASE), r"\1=[redacted]"),
)


def build_upstream_error(
    message: Any,
    *,
    status_code: Any = None,
    body: Any = None,
    content_type: Any = None,
    exception: Any = None,
    phase: Any = None,
) -> dict[str, dict[str, str]]:
    base_message = _normalize_message(message)
    details: list[str] = []

    phase_text = _normalize_value(phase)
    if phase_text:
        details.append(f"phase={phase_text}")

    status_text = _normalize_value(status_code)
    if status_text:
        details.append(f"upstream_status={status_text}")

    content_type_text = _normalize_value(content_type)
    if content_type_text:
        details.append(f"content_type={content_type_text}")

    exception_text = _normalize_exception(exception)
    if exception_text:
        details.append(f"exception={exception_text}")

    body_text = _normalize_body(body)
    if body_text:
        details.append(f"body={body_text}")

    full_message = base_message
    if details:
        full_message = f"{base_message} ({'; '.join(details)})"

    return {"error": {"message": _truncate(full_message, _MAX_MESSAGE_CHARS)}}


def _normalize_message(message: Any) -> str:
    value = _normalize_value(message)
    if not value:
        return "Upstream error"
    return value


def _normalize_exception(exception: Any) -> str | None:
    if exception is None:
        return None
    if isinstance(exception, BaseException):
        detail = _normalize_value(exception)
        name = exception.__class__.__name__
        if detail and detail != name:
            return _truncate(f"{name}: {detail}", _MAX_SNIPPET_CHARS)
        return name
    return _truncate(_normalize_value(exception), _MAX_SNIPPET_CHARS)


def _normalize_body(body: Any) -> str | None:
    if body is None:
        return None
    return _truncate(_normalize_value(body), _MAX_SNIPPET_CHARS)


def _normalize_value(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="ignore")
    elif isinstance(value, str):
        text = value
    else:
        text = _safe_string(value)
    text = _redact(text).strip()
    if not text:
        return None
    return text


def _safe_string(value: Any) -> str:
    try:
        return str(value)
    except Exception:
        try:
            name = value.__class__.__name__
        except Exception:
            name = "object"
        return f"<unprintable {name}>"


def _redact(text: str) -> str:
    redacted = text
    for pattern, replacement in _REDACTION_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


@overload
def _truncate(value: str, limit: int) -> str: ...


@overload
def _truncate(value: None, limit: int) -> None: ...


def _truncate(value: str | None, limit: int) -> str | None:
    if value is None:
        return None
    if len(value) <= limit:
        return value
    return value[: limit - len(_TRUNCATION_MARKER)] + _TRUNCATION_MARKER