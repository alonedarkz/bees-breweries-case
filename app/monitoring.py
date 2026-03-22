from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


LOGGER = logging.getLogger("bees.pipeline")

if not LOGGER.handlers:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def emit_log(event: str, **fields: Any) -> None:
    payload = {"timestamp": utc_now_iso(), "event": event, **fields}
    LOGGER.info(json.dumps(payload, ensure_ascii=True, default=str))


def persist_run_report(report: dict[str, Any], destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(report, ensure_ascii=True, indent=2, sort_keys=True, default=str),
        encoding="utf-8",
    )
