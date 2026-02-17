# -*- coding: utf-8 -*-
"""
visual_semantics.py

Modular, deterministic image semantics extraction.

Goals:
- Provide "human-like" interpretation hooks for uploaded images (beyond OCR).
- Keep this module safe to import from project_store/upload_pipeline.
- No imports from server.py (no circular deps).
- Optional deps only (Pillow, pytesseract). If missing, degrade gracefully.

Current detectors:
- Wordle board (v1): detect grid, classify tile colors, (optional) OCR letters.

Returned object (generic):
{
  "version": 1,
  "type": "<semantic_type>",
  "confidence": 0.0..1.0,
  "summary": "<short human summary>",
  "data": {...},
  "notes": ["..."]
}
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import math


# Optional deps
try:
    from PIL import Image  # type: ignore
except Exception:
    Image = None  # type: ignore

try:
    import pytesseract  # type: ignore
except Exception:
    pytesseract = None  # type: ignore


# ----------------------------
# Color helpers
# ----------------------------

def _srgb_dist(a: Tuple[int, int, int], b: Tuple[int, int, int]) -> float:
    # Euclidean in sRGB (good enough for robust thresholds here)
    return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2 + (a[2] - b[2]) ** 2)

def _avg_rgb(im, box: Tuple[int, int, int, int]) -> Tuple[int, int, int]:
    """
    Average RGB in a box. box = (l,t,r,b)
    """
    try:
        crop = im.crop(box)
        crop = crop.convert("RGB")
        px = crop.load()
        w, h = crop.size
        if w <= 0 or h <= 0:
            return (0, 0, 0)
        total = [0, 0, 0]
        n = 0
        # sample grid (avoid heavy full-scan)
        step_x = max(1, w // 12)
        step_y = max(1, h // 12)
        for y in range(0, h, step_y):
            for x in range(0, w, step_x):
                r, g, b = px[x, y]
                total[0] += int(r)
                total[1] += int(g)
                total[2] += int(b)
                n += 1
        if n <= 0:
            return (0, 0, 0)
        return (total[0] // n, total[1] // n, total[2] // n)
    except Exception:
        return (0, 0, 0)

def _classify_wordle_tile_color(rgb: Tuple[int, int, int]) -> str:
    """
    Robust Wordle tile color classification using HSV.
    Returns: "green" | "yellow" | "gray" | "empty"
    """
    r, g, b = rgb

    # Normalize
    rf, gf, bf = r / 255.0, g / 255.0, b / 255.0
    mx = max(rf, gf, bf)
    mn = min(rf, gf, bf)
    diff = mx - mn

    # Brightness check (empty / background)
    if mx < 0.18:
        return "empty"

    # Gray check: low saturation
    if diff < 0.05:
        return "gray"

    # Hue calculation
    if diff == 0:
        h = 0
    elif mx == rf:
        h = (60 * ((gf - bf) / diff) + 360) % 360
    elif mx == gf:
        h = (60 * ((bf - rf) / diff) + 120) % 360
    else:
        h = (60 * ((rf - gf) / diff) + 240) % 360

    # Yellow range (Wordle yellows vary a lot in dark mode)
    if 35 <= h <= 75:
        return "yellow"

    # Green range
    if 80 <= h <= 160:
        return "green"

    # Fallback
    return "gray"



# ----------------------------
# Wordle detector (v1)
# ----------------------------

@dataclass
class WordleTile:
    letter: str
    status: str
    rgb: Tuple[int, int, int]

def _find_board_bbox(im) -> Optional[Tuple[int, int, int, int]]:
    """
    Heuristic: find the largest region in the upper 75% of the screen that contains
    mid-bright tiles (not pure background), by scanning rows/cols for "tile-ish" pixels.

    This is intentionally conservative and avoids heavy CV dependencies.
    """
    try:
        w, h = im.size
        if w < 200 or h < 200:
            return None

        # Focus search above keyboard area (Wordle mobile has keyboard in bottom ~35%)
        top_h = int(h * 0.70)

        im2 = im.convert("RGB")
        px = im2.load()

        # Sample a grid to estimate background brightness
        bg_samples: List[int] = []
        for y in (10, 30, 60):
            if y >= top_h:
                continue
            for x in (10, w // 2, w - 10):
                try:
                    r, g, b = px[x, y]
                    bg_samples.append(int((r + g + b) / 3))
                except Exception:
                    pass
        bg = sum(bg_samples) / max(1, len(bg_samples))

        # Identify rows/cols that contain "tile-ish" pixels (brightness away from bg)
        def is_tileish(r: int, g: int, b: int) -> bool:
            br = (r + g + b) / 3.0
            return abs(br - bg) > 18 and br > 35

        row_hits = [0] * top_h
        col_hits = [0] * w

        step = max(2, min(w, top_h) // 220)

        for y in range(0, top_h, step):
            hits = 0
            for x in range(0, w, step):
                r, g, b = px[x, y]
                if is_tileish(r, g, b):
                    hits += 1
                    col_hits[x] += 1
            row_hits[y] = hits

        # Find contiguous row band with high hits
        thresh_row = max(6, int(w / step) // 10)
        ys = [y for y in range(0, top_h, step) if row_hits[y] >= thresh_row]
        if not ys:
            return None

        y0 = min(ys)
        y1 = max(ys)

        # Find contiguous col band with high hits inside that y band
        # Re-scan only in y band for accuracy
        col_hits2 = [0] * w
        for y in range(y0, y1 + 1, step):
            for x in range(0, w, step):
                r, g, b = px[x, y]
                if is_tileish(r, g, b):
                    col_hits2[x] += 1

        thresh_col = max(6, int((y1 - y0) / step) // 8)
        xs = [x for x in range(0, w, step) if col_hits2[x] >= thresh_col]
        if not xs:
            return None

        x0 = min(xs)
        x1 = max(xs)

        # Expand slightly for safety
        pad = max(6, step * 2)
        x0 = max(0, x0 - pad)
        y0 = max(0, y0 - pad)
        x1 = min(w - 1, x1 + pad)
        y1 = min(h - 1, y1 + pad)

        # Sanity aspect: Wordle board is roughly square-ish
        bw = x1 - x0
        bh = y1 - y0
        if bw < 180 or bh < 180:
            return None
        if bh > int(h * 0.78):
            return None

        return (x0, y0, x1, y1)
    except Exception:
        return None

def _ocr_single_letter(tile_im) -> str:
    """
    Attempt to OCR a single uppercase letter.
    If pytesseract is not available, returns "".
    """
    if pytesseract is None or Image is None:
        return ""
    try:
        # Improve OCR: grayscale + threshold-ish
        g = tile_im.convert("L")
        # resize up (OCR likes bigger)
        g = g.resize((g.size[0] * 3, g.size[1] * 3))
        # pytesseract config for single character, uppercase only
        cfg = "--psm 10 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        txt = (pytesseract.image_to_string(g, config=cfg) or "").strip().upper()
        if not txt:
            return ""
        # keep first alpha
        for ch in txt:
            if "A" <= ch <= "Z":
                return ch
        return ""
    except Exception:
        return ""

def extract_wordle_state_from_image(path: Path) -> Dict[str, Any]:
    """
    Returns a semantic object. Never throws.
    """
    out: Dict[str, Any] = {
        "version": 1,
        "type": "none",
        "confidence": 0.0,
        "summary": "",
        "data": {},
        "notes": [],
    }

    if Image is None:
        out["notes"].append("Pillow not installed; cannot parse image semantics.")
        return out

    try:
        im = Image.open(str(path))
        im = im.convert("RGB")
    except Exception as e:
        out["notes"].append(f"Image open error: {e!r}")
        return out

    bbox = _find_board_bbox(im)
    if not bbox:
        out["notes"].append("No Wordle-like board region detected.")
        return out

    x0, y0, x1, y1 = bbox
    board = im.crop((x0, y0, x1, y1))
    bw, bh = board.size

    # Wordle board is 5 columns by up to 6 rows; estimate tile size by width/5
    tile_w = bw / 5.0
    # Use square tiles: tile_h approx tile_w; allow vertical spacing
    tile_h = tile_w

    # Determine row count by how many tile-heights fit (cap 6)
    est_rows = int(min(6, max(1, round(bh / tile_h))))
    if est_rows < 1:
        est_rows = 6
    est_rows = min(6, max(1, est_rows))

    tiles: List[List[WordleTile]] = []
    filled_rows = 0

    for r in range(est_rows):
        row: List[WordleTile] = []
        row_has_nonempty = False
        for c in range(5):
            # Centered sampling window per tile
            cx = int((c + 0.5) * tile_w)
            cy = int((r + 0.5) * tile_h)

            # sampling box for color (small patch)
            half = int(max(6, tile_w * 0.12))
            l = max(0, cx - half)
            t = max(0, cy - half)
            rr = min(bw, cx + half)
            bb = min(bh, cy + half)

            rgb = _avg_rgb(board, (l, t, rr, bb))
            status = _classify_wordle_tile_color(rgb)

            # Crop tile for OCR (bigger than color patch)
            pad = int(max(10, tile_w * 0.38))
            l2 = max(0, cx - pad)
            t2 = max(0, cy - pad)
            r2 = min(bw, cx + pad)
            b2 = min(bh, cy + pad)
            tile_img = board.crop((l2, t2, r2, b2))

            letter = ""
            if status != "empty":
                letter = _ocr_single_letter(tile_img)

            if status != "empty":
                row_has_nonempty = True

            row.append(WordleTile(letter=letter, status=status, rgb=rgb))
        if row_has_nonempty:
            filled_rows += 1
        tiles.append(row)

    # Confidence heuristic:
    # - must have at least 1 filled row
    # - must have at least 2 non-empty tiles total
    nonempty = sum(1 for r in tiles for t in r if t.status != "empty")
    conf = 0.0
    if filled_rows >= 1 and nonempty >= 2:
        conf = 0.72
    if filled_rows >= 1 and nonempty >= 5:
        conf = 0.86

    # Build JSON-friendly structure
    rows_out: List[List[Dict[str, Any]]] = []
    for r in tiles:
        rows_out.append([
            {
                "letter": (t.letter or ""),
                "status": t.status,
                "rgb": [int(t.rgb[0]), int(t.rgb[1]), int(t.rgb[2])],
            }
            for t in r
        ])

    out["type"] = "wordle_state"
    out["confidence"] = float(conf)
    out["summary"] = f"Wordle-like board detected. Rows={len(rows_out)}; nonempty_tiles={nonempty}; filled_rows={filled_rows}."
    out["data"] = {
        "board_bbox": [int(x0), int(y0), int(x1), int(y1)],
        "rows": rows_out,
    }

    # Notes about OCR availability
    if pytesseract is None:
        out["notes"].append("pytesseract not installed; letters may be blank (colors still extracted).")

    return out

def analyze_image(path: Path) -> Dict[str, Any]:
    """
    Top-level deterministic entrypoint for image semantics.

    Current enabled detectors:
    - Wordle board (v1): detect grid + tile colors + optional OCR letters.

    Deterministic. Never throws.
    """
    try:
        # Wordle is the only semantic detector currently enabled.
        obj = extract_wordle_state_from_image(Path(path))
        if isinstance(obj, dict) and str(obj.get("type") or "") == "wordle_state":
            # Return only if we have non-trivial confidence.
            try:
                conf = float(obj.get("confidence") or 0.0)
            except Exception:
                conf = 0.0
            if conf > 0.0:
                return obj
    except Exception:
        pass

    return {
        "version": 1,
        "type": "none",
        "confidence": 0.0,
        "summary": "",
        "data": {},
        "notes": ["No supported image semantic detectors matched."],
    }
