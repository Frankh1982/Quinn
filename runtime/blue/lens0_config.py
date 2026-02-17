# -*- coding: utf-8 -*-
"""
Lens-0 config primitives (pure constants + pure helpers).

Hard rules:
- This module must NOT import server.py (no circular imports).
- Keep it stdlib-only.
"""

from __future__ import annotations


TEXT_LIKE_SUFFIXES = {
    ".py", ".js", ".ts", ".jsx", ".tsx",
    ".html", ".htm", ".css",
    ".json", ".txt", ".md", ".yaml", ".yml",
}

IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".tif", ".tiff"}


def is_text_suffix(suffix: str) -> bool:
    return (suffix or "").lower() in TEXT_LIKE_SUFFIXES


def mime_for_image_suffix(suffix: str) -> str:
    s = (suffix or "").lower()
    if s == ".png":
        return "image/png"
    if s in (".jpg", ".jpeg"):
        return "image/jpeg"
    if s == ".webp":
        return "image/webp"
    if s == ".gif":
        return "image/gif"
    if s == ".bmp":
        return "image/bmp"
    if s in (".tif", ".tiff"):
        return "image/tiff"
    return "application/octet-stream"
