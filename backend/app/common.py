from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Iterable


def safe_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_int_list(values: Iterable[Any] | None) -> list[int]:
    if not values:
        return []
    normalized: list[int] = []
    seen: set[int] = set()
    for value in values:
        normalized_value = safe_int(value)
        if normalized_value is None or normalized_value in seen:
            continue
        seen.add(normalized_value)
        normalized.append(normalized_value)
    return normalized


def normalize_message_text(value: Any) -> str:
    if not isinstance(value, str):
        return ''
    return ' '.join(value.splitlines()).strip()


def normalize_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value
        return value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
        if parsed.tzinfo is not None:
            return parsed
        return parsed.replace(tzinfo=timezone.utc)
    return None
