from __future__ import annotations

import json
import logging
import sys
from typing import Any


VERBOSE_LOGGER_NAME = "chatmock.verbose"
VERBOSE_STDOUT_HANDLER_NAME = "chatmock.verbose.stdout"


def configure_verbose_logger() -> logging.Logger:
    logger = logging.getLogger(VERBOSE_LOGGER_NAME)
    logger.disabled = False
    logger.setLevel(logging.INFO)
    logger.propagate = False

    named_handlers = [
        handler
        for handler in logger.handlers
        if handler.get_name() == VERBOSE_STDOUT_HANDLER_NAME
    ]

    if named_handlers:
        stdout_handler = named_handlers[0]
        for duplicate_handler in named_handlers[1:]:
            logger.removeHandler(duplicate_handler)
            duplicate_handler.close()
    else:
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.set_name(VERBOSE_STDOUT_HANDLER_NAME)
        logger.addHandler(stdout_handler)

    stdout_handler.setFormatter(logging.Formatter("%(message)s"))
    if getattr(stdout_handler, "stream", None) is not sys.stdout:
        stdout_handler.setStream(sys.stdout)

    return logger


def log_verbose_message(message: str, *, enabled: bool) -> None:
    if not enabled:
        return

    configure_verbose_logger().info(message)


def log_verbose_json(prefix: str, payload: Any, *, enabled: bool) -> None:
    if not enabled:
        return

    try:
        rendered_payload = json.dumps(payload, indent=2, ensure_ascii=False)
    except Exception:
        try:
            rendered_payload = str(payload)
        except Exception:
            return

    log_verbose_message(f"{prefix}\n{rendered_payload}", enabled=True)


def wrap_verbose_stream(label: str, iterator, *, enabled: bool):
    if not enabled:
        return iterator

    def _generator():
        for chunk in iterator:
            try:
                rendered_chunk = (
                    chunk.decode("utf-8", errors="replace")
                    if isinstance(chunk, (bytes, bytearray))
                    else str(chunk)
                )
            except Exception:
                rendered_chunk = None

            if rendered_chunk is not None:
                log_verbose_message(f"{label}\n{rendered_chunk}", enabled=True)
            yield chunk

    return _generator()