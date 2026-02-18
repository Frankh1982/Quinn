# -*- coding: utf-8 -*-
"""
Path / naming safety helpers.

Extracted from server.py (Step 6B).
"""

from __future__ import annotations

import re
import time
from typing import List


def safe_project_name(name: str, *, default_project_name: str = "default") -> str:
    """
    Normalize a project name while preserving folder nesting like "User/Project".
    Each path segment is sanitized independently.
    """
    raw = (name or "").strip().replace("\\", "/")
    parts = [p for p in raw.split("/") if p.strip()]
    if not parts:
        return default_project_name

    cleaned_parts: List[str] = []
    for part in parts:
        cleaned = re.sub(r"[^a-zA-Z0-9_-]", "_", part.strip())
        cleaned_parts.append(cleaned or default_project_name)

    return "/".join(cleaned_parts)


def safe_filename(name: str) -> str:
    base = (name or "").strip()
    base = base.replace("\\", "/").split("/")[-1]
    base = re.sub(r"[^a-zA-Z0-9_.-]", "_", base)
    return base or "file.bin"


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
