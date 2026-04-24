"""ASCII-safe slug helpers for report artifact names."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from typing import Any


def ascii_slug(value: Any, *, fallback_prefix: str = "item", max_length: int = 32) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    normalized = unicodedata.normalize("NFKD", text)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^A-Za-z0-9]+", "_", ascii_text).strip("_").lower()
    slug = re.sub(r"_+", "_", slug)
    had_non_ascii = any(not char.isascii() for char in text)
    if slug and not had_non_ascii:
        return slug[:max_length].strip("_") or fallback_prefix

    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:8]
    if slug:
        trimmed = slug[: max(1, max_length - len(digest) - 1)].strip("_") or fallback_prefix
        return f"{trimmed}_{digest}"
    return f"{fallback_prefix}_{digest}"
