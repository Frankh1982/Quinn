# -*- coding: utf-8 -*-
"""
upload_pipeline.py

Extracted async upload processing subsystem from server.py.

Scope:
- Upload queue + worker tasks
- Upload status map + locks
- Project-scoped WS client registry + broadcast
- Upload status tail helpers
- Enqueue entrypoint for [FILE_ADDED] path
- start()/stop() lifecycle for worker(s)

Hard rules:
- No behavior changes
- Keep upload.status frame schema identical
"""

from __future__ import annotations

import asyncio
import json
import shutil
import difflib
import hashlib
import re
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from lens0_config import TEXT_LIKE_SUFFIXES

import visual_semantics
import sys

from project_store import (
    load_manifest,
    load_decisions,
    save_manifest,
    register_raw_file,
    raw_dir,
    state_dir,
    load_project_state,
    create_artifact,
    read_artifact_text,
    ingest_uploaded_file,
    load_pending_upload_question,
    save_pending_upload_question,
    is_pending_upload_question_unexpired,
    append_inbox_item,
    find_current_decision_by_text,
    append_link_event,
    save_active_object,
    write_canonical_entry,
    register_upload_batch,
    ANALYSIS_PIPELINE_VERSION,
    INGEST_PIPELINE_VERSION,
    read_text_file,
    read_bytes_file,
)


# ---------------------------------------------------------------------------
# Module configuration (set by start())
# ---------------------------------------------------------------------------

_PROJECT_ROOT: Optional[Path] = None
_CAPTION_IMAGE_FN = None
_CLASSIFY_IMAGE_FN = None


def configure(
    *,
    project_root: Path,
    caption_image_fn=None,
    classify_image_fn=None,
) -> None:
    global _PROJECT_ROOT, _CAPTION_IMAGE_FN, _CLASSIFY_IMAGE_FN
    _PROJECT_ROOT = Path(project_root).resolve()
    _CAPTION_IMAGE_FN = caption_image_fn
    _CLASSIFY_IMAGE_FN = classify_image_fn


def now_iso() -> str:
    import time
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ---------------------------------------------------------------------------
# ZIP ingestion helpers (deterministic, bounded)
# ---------------------------------------------------------------------------

def _safe_zip_member_name(member_name: str, *, zip_base: str) -> str:
    raw = (member_name or "").replace("\\", "/").strip()
    # Remove leading ./ and dangerous parent traversal
    raw = re.sub(r"^\./+", "", raw)
    raw = raw.replace("../", "").replace("..\\", "")

    # Flatten path separators into "__" for a single raw folder
    flat = raw.replace("/", "__")
    if not flat:
        flat = "file"

    # Keep extension if present
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", flat)
    if len(safe) > 140:
        safe = safe[:140].rstrip("_")

    # Add a short hash of the original path to ensure uniqueness
    h = hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()[:8]
    if "." in safe:
        stem, ext = safe.rsplit(".", 1)
        safe = f"{stem}_{h}.{ext}"
    else:
        safe = f"{safe}_{h}"

    if zip_base:
        z = re.sub(r"[^A-Za-z0-9_.-]", "_", zip_base)
        safe = f"{z}__{safe}"
    return safe

def _extract_zip_to_raw(project_full: str, *, zip_abs: Path, zip_orig_name: str) -> Dict[str, Any]:
    """
    Extract a ZIP to raw/ with safe filenames, then register files.
    Returns summary with extracted list and skips.
    """
    summary = {
        "extracted": [],  # list of dicts {canonical_rel, orig_name}
        "skipped": [],    # list of str reasons
        "total_files": 0,
        "total_bytes": 0,
    }

    if not zip_abs.exists() or not zip_abs.is_file():
        summary["skipped"].append("zip_missing")
        return summary

    # Bounds (deterministic, conservative)
    max_files = 240
    max_total_bytes = 350 * 1024 * 1024  # 350 MB uncompressed
    max_file_bytes = 120 * 1024 * 1024   # 120 MB per file

    zip_base = Path(zip_orig_name or zip_abs.name).stem or "archive"

    try:
        zf = zipfile.ZipFile(zip_abs, "r")
    except Exception:
        summary["skipped"].append("zip_open_failed")
        return summary

    try:
        infos = zf.infolist()
    except Exception:
        infos = []

    for info in infos:
        if summary["total_files"] >= max_files:
            summary["skipped"].append("file_limit_reached")
            break

        try:
            if info.is_dir():
                continue
        except Exception:
            # zipfile may not have is_dir in older Python; treat names ending with / as dirs
            name0 = str(info.filename or "")
            if name0.endswith("/"):
                continue

        name = str(getattr(info, "filename", "") or "")
        if not name:
            continue

        fsize = int(getattr(info, "file_size", 0) or 0)
        if fsize <= 0:
            continue
        if fsize > max_file_bytes:
            summary["skipped"].append(f"file_too_large:{name}")
            continue
        if (summary["total_bytes"] + fsize) > max_total_bytes:
            summary["skipped"].append("size_limit_reached")
            break

        safe_name = _safe_zip_member_name(name, zip_base=zip_base)
        dest_abs = raw_dir(project_full) / safe_name
        dest_abs.parent.mkdir(parents=True, exist_ok=True)

        try:
            with zf.open(info) as src, dest_abs.open("wb") as out:
                shutil.copyfileobj(src, out)
        except Exception:
            summary["skipped"].append(f"extract_failed:{name}")
            continue

        summary["total_files"] += 1
        summary["total_bytes"] += fsize

        # Register with orig_name preserving zip context
        orig_name = f"{Path(zip_orig_name or zip_abs.name).name}::{name}"
        try:
            updated, canonical_rel, _prev = register_raw_file(project_full, dest_abs, orig_name)
        except Exception:
            # If register fails, fall back to canonical relative path
            canonical_rel = str(dest_abs)
            updated = False

        summary["extracted"].append(
            {"canonical_rel": canonical_rel, "orig_name": orig_name, "updated": bool(updated)}
        )

    try:
        zf.close()
    except Exception:
        pass

    return summary

async def _enqueue_extracted_file(
    *,
    project_full: str,
    canonical_rel: str,
    orig_name: str,
    updated: bool,
    batch_id: str = "",
) -> None:
    """
    Enqueue a file that was already registered (no duplicate register_raw_file).
    """
    rel = (canonical_rel or "").strip().replace("\\", "/")
    if not rel:
        return

    global UPLOAD_QUEUE
    if UPLOAD_QUEUE is None:
        UPLOAD_QUEUE = asyncio.Queue()

    try:
        append_jsonl(
            state_dir(project_full) / "assets_index.jsonl",
            {
                "ts": now_iso(),
                "state": "queued",
                "orig_name": orig_name,
                "canonical_rel": rel,
                "prev_rel": "",
                "updated": bool(updated),
            },
        )
    except Exception:
        pass

    async with UPLOAD_STATUS_LOCK:
        UPLOAD_STATUS[rel] = {
            "state": "queued",
            "ts": now_iso(),
            "project_full": project_full,
            "orig_name": orig_name,
            "batch_id": batch_id,
        }

    await UPLOAD_QUEUE.put(
        {
            "project_full": project_full,
            "canonical_rel": rel,
            "prev_rel": "",
            "orig_name": orig_name,
            "updated": bool(updated),
            "batch_id": batch_id,
        }
    )


def append_jsonl(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(obj, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def is_text_suffix(suffix: str) -> bool:
    return (suffix or "").lower() in TEXT_LIKE_SUFFIXES


def _analysis_hat_active(project_full: str) -> bool:
    try:
        st = load_project_state(project_full) or {}
    except Exception:
        return False
    try:
        last_ex = str(st.get("last_active_expert") or "").strip().lower()
    except Exception:
        last_ex = ""
    return last_ex == "analysis"


def _resolve_ensure_image_semantics_for_file():
    """
    Late-bind to the authoritative implementation in runtime/blue/server.py without
    creating an import cycle (server.py imports upload_pipeline).
    server.py is executed as __main__ (ROOT SHIM runpy), so we can pull it from sys.modules.
    Returns a callable or None.
    """
    try:
        main_mod = sys.modules.get("__main__")
        fn = getattr(main_mod, "ensure_image_semantics_for_file", None)
        if callable(fn):
            return fn
    except Exception:
        return None
    return None


def _find_latest_image_semantics_text(project_full: str, canonical_rel: str, cap_chars: int = 6000) -> str:
    """
    Verify artifact exists (type=image_semantics) linked via from_files.
    Returns artifact text or "".
    """
    try:
        m = load_manifest(project_full)
        arts = m.get("artifacts") or []
        for a in reversed(arts):
            if a.get("type") != "image_semantics":
                continue
            ff = a.get("from_files") or []
            ff_norm = [(x or "").replace("\\", "/").strip() for x in ff] if isinstance(ff, list) else []
            if canonical_rel not in ff_norm:
                continue
            return (read_artifact_text(project_full, a, cap_chars=cap_chars) or "").strip()
    except Exception:
        return ""
    return ""


async def _ensure_image_semantics_required(project_full: str, canonical_rel: str, orig_name: str) -> Tuple[bool, str]:
    """
    Enforce mandatory vision semantics for image uploads.
    Returns (ok, note). If not ok, caller should emit a user-visible message and stop.
    """
    fn = _resolve_ensure_image_semantics_for_file()
    if fn is None:
        return False, "vision_semantics_entrypoint_missing"

    try:
        ok_v, _sem_json, note_v = await fn(
            project_full,
            file_rel=canonical_rel,
            force=False,
            reason="upload_pipeline_worker",
        )
    except Exception as e:
        return False, f"vision_semantics_exception:{e!r}"

    if not ok_v:
        return False, str(note_v or "vision_semantics_failed")

    sem_text = _find_latest_image_semantics_text(project_full, canonical_rel, cap_chars=6000)
    if not sem_text:
        return False, "image_semantics_artifact_missing_after_ensure"

    return True, ""


# ---------------------------------------------------------------------------
# Wordle deterministic constraint compiler + action engine (upload-time only)
# ---------------------------------------------------------------------------

_WORDLE_WORDLIST_CANDIDATES = (
    "wordlists/wordle_allowed.txt",
    "wordlists/wordle_words.txt",
    "wordlists/wordle.txt",
)

def _looks_like_wordle_classification(cls_obj: Dict[str, Any]) -> bool:
    """
    Deterministic check based on stored image classification artifact.
    No model calls.
    """
    if not isinstance(cls_obj, dict):
        return False
    label = str(cls_obj.get("label") or "").strip().lower()
    if "wordle" in label:
        return True
    tags = cls_obj.get("tags")
    if isinstance(tags, list):
        for t in tags:
            if "wordle" in str(t or "").strip().lower():
                return True
    return False

def _wordle_state_paths(project_full: str) -> Tuple[Path, Path]:
    """
    Returns (state_path, constraints_path) inside state_dir(project_full).
    """
    sd = state_dir(project_full)
    return (sd / "wordle_state.json", sd / "wordle_constraints.json")

def _compile_wordle_constraints(wordle_state_obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compile constraints from a visual_semantics Wordle state object.
    Deterministic, conservative.

    Output schema:
    {
      "word_length": 5,
      "fixed_positions": {"0":"C", ...},
      "forbidden_positions": {"E":[4], ...},
      "present_letters": ["C","E", ...],
      "excluded_letters": ["R","A","N", ...]
    }
    """
    out: Dict[str, Any] = {
        "word_length": 5,
        "fixed_positions": {},
        "forbidden_positions": {},
        "present_letters": [],
        "excluded_letters": [],
    }

    if not isinstance(wordle_state_obj, dict):
        return out
    data = wordle_state_obj.get("data")
    if not isinstance(data, dict):
        return out
    rows = data.get("rows")
    if not isinstance(rows, list):
        return out

    fixed: Dict[str, str] = {}
    forb: Dict[str, List[int]] = {}
    present: set[str] = set()
    excluded_raw: set[str] = set()

    for r in rows:
        if not isinstance(r, list):
            continue
        for idx, cell in enumerate(r[:5]):
            if not isinstance(cell, dict):
                continue
            status = str(cell.get("status") or "").strip().lower()
            letter = str(cell.get("letter") or "").strip().upper()
            if not letter or len(letter) != 1 or not ("A" <= letter <= "Z"):
                # If OCR missed the letter, we cannot safely compile letter constraints from it.
                continue

            if status == "green":
                fixed[str(idx)] = letter
                present.add(letter)
            elif status == "yellow":
                present.add(letter)
                lst = forb.get(letter)
                if lst is None:
                    lst = []
                    forb[letter] = lst
                if idx not in lst:
                    lst.append(int(idx))
            elif status == "gray":
                excluded_raw.add(letter)

    # If a letter is present (yellow/green), it must NOT be excluded even if it appeared gray elsewhere.
    excluded = sorted([x for x in excluded_raw if x not in present])

    # Normalize forbidden positions lists (sorted, unique)
    forb2: Dict[str, List[int]] = {}
    for k, v in forb.items():
        vv = sorted(list({int(x) for x in (v or [])}))
        forb2[str(k)] = vv

    out["fixed_positions"] = fixed
    out["forbidden_positions"] = forb2
    out["present_letters"] = sorted(list(present))
    out["excluded_letters"] = excluded
    return out

def _load_wordle_wordlist(project_root: Path) -> List[str]:
    """
    Load a local Wordle word list.
    If none found, returns [].
    """
    pr = Path(project_root).resolve()
    for rel in _WORDLE_WORDLIST_CANDIDATES:
        p = (pr / rel).resolve()
        try:
            p.relative_to(pr)
        except Exception:
            continue
        if not p.exists() or not p.is_file():
            continue
        try:
            raw = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            raw = ""
        words: List[str] = []
        for ln in (raw or "").splitlines():
            w = (ln or "").strip().lower()
            if not w:
                continue
            if len(w) != 5:
                continue
            if not re.fullmatch(r"[a-z]{5}", w):
                continue
            # No trivial plural hacks: avoid recommending words ending in 's'
            if w.endswith("s"):
                continue
            words.append(w)
        # Stable de-dupe preserve order
        seen = set()
        out = []
        for w in words:
            if w in seen:
                continue
            seen.add(w)
            out.append(w)
        return out
    return []

def _pick_next_wordle_guess(words: List[str], constraints: Dict[str, Any]) -> str:
    """
    Deterministic filter-then-score.
    Returns:
      - 5-letter uppercase word
      - OR "NO VALID MOVE — CHECK INPUT"
    """
    if not words:
        return "NO VALID MOVE — CHECK INPUT"

    fixed = constraints.get("fixed_positions")
    forb = constraints.get("forbidden_positions")
    present = constraints.get("present_letters")
    excluded = constraints.get("excluded_letters")

    if not isinstance(fixed, dict):
        fixed = {}
    if not isinstance(forb, dict):
        forb = {}
    if not isinstance(present, list):
        present = []
    if not isinstance(excluded, list):
        excluded = []

    fixed2: Dict[int, str] = {}
    for k, v in fixed.items():
        try:
            i = int(k)
        except Exception:
            continue
        L = str(v or "").strip().upper()
        if len(L) == 1 and ("A" <= L <= "Z"):
            fixed2[i] = L

    forb2: Dict[str, set[int]] = {}
    for k, v in forb.items():
        L = str(k or "").strip().upper()
        if len(L) != 1 or not ("A" <= L <= "Z"):
            continue
        if not isinstance(v, list):
            continue
        s = set()
        for x in v:
            try:
                s.add(int(x))
            except Exception:
                pass
        forb2[L] = s

    present2 = [str(x or "").strip().upper() for x in present if str(x or "").strip()]
    present_set = {x for x in present2 if len(x) == 1 and ("A" <= x <= "Z")}
    excluded_set = {str(x or "").strip().upper() for x in excluded if str(x or "").strip()}
    excluded_set = {x for x in excluded_set if len(x) == 1 and ("A" <= x <= "Z")}
    # Never exclude letters we know are present
    excluded_set = {x for x in excluded_set if x not in present_set}

    # Precompute corpus letter frequency for scoring (deterministic)
    freq: Dict[str, int] = {}
    for w in words:
        for ch in set(w):
            freq[ch] = freq.get(ch, 0) + 1

    def is_valid(w: str) -> bool:
        # Excluded letters never used
        for ch in excluded_set:
            if ch.lower() in w:
                return False

        # Fixed positions
        for pos, ch in fixed2.items():
            if pos < 0 or pos >= 5:
                return False
            if w[pos].upper() != ch:
                return False

        # Forbidden positions (yellow letters must not be in the same spot)
        for ch, bad_positions in forb2.items():
            cl = ch.lower()
            for pos in bad_positions:
                if 0 <= pos < 5 and w[pos] == cl:
                    return False

        # Present letters must exist somewhere
        for ch in present_set:
            if ch.lower() not in w:
                return False

        return True

    # Filter candidates
    candidates = [w for w in words if is_valid(w)]
    if not candidates:
        return "NO VALID MOVE — CHECK INPUT"

    # Score: prefer (a) new unique letters (not already present), (b) higher corpus freq
    def score(w: str) -> int:
        uniq = set(w)
        s = 0
        for ch in uniq:
            s += int(freq.get(ch, 0))
            if ch.upper() not in present_set:
                s += 50  # strong bias toward exploring new letters
        # Small penalty for repeated letters
        if len(uniq) < 5:
            s -= 40
        return s

    # Deterministic pick: max score, then lexicographic
    candidates.sort(key=lambda x: (-score(x), x))
    return candidates[0].upper()


# ---------------------------------------------------------------------------
# C3 v1 — Relevance Router (deterministic only, no model calls)
# ---------------------------------------------------------------------------

def _c3_keywords(text: str) -> List[str]:
    import re
    t = (text or "").lower()
    parts = re.split(r"[^a-z0-9]+", t)
    out: List[str] = []
    for w in parts:
        w = (w or "").strip()
        if len(w) < 4:
            continue
        out.append(w)
    # stable de-dupe
    seen = set()
    dedup: List[str] = []
    for w in out:
        if w in seen:
            continue
        seen.add(w)
        dedup.append(w)
    return dedup[:120]


def _c3_best_decision_match(decision_texts: List[str], evidence_text: str) -> Tuple[str, int]:
    """
    Returns (best_decision_text, overlap_score). Overlap is shared keyword count.
    Conservative and bounded.
    """
    e = (evidence_text or "").strip()
    if not e:
        return "", 0

    ekw = set(_c3_keywords(e))
    if not ekw:
        return "", 0

    best_t = ""
    best_s = 0

    # tail only to keep it fast
    for dt in (decision_texts or [])[-120:]:
        dkw = set(_c3_keywords(dt))
        if not dkw:
            continue
        s = len(dkw.intersection(ekw))
        if s > best_s:
            best_s = s
            best_t = dt

    return best_t, best_s

# ---------------------------------------------------------------------------
# C3 v1 — Decision-aware upload interpretation (Relevance Router v1)
# Deterministic only. No model calls. No silent decision mutation.
# ---------------------------------------------------------------------------

def _tokenize_keywords(text: str) -> List[str]:
    """
    Lowercase, split on non-alnum, drop short words.
    Conservative: prefer fewer, higher-signal tokens.
    """
    import re
    t = (text or "").lower()
    parts = re.split(r"[^a-z0-9]+", t)
    out = []
    for w in parts:
        w = (w or "").strip()
        if len(w) < 4:
            continue
        out.append(w)
    # de-dupe (stable order)
    seen = set()
    dedup = []
    for w in out:
        if w in seen:
            continue
        seen.add(w)
        dedup.append(w)
    return dedup[:120]


def _read_jsonl_objects(path: Path, *, max_lines: int = 1200) -> List[Dict[str, Any]]:
    try:
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    # tail only (keeps it bounded)
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    out: List[Dict[str, Any]] = []
    for ln in lines:
        try:
            obj = json.loads(ln)
        except Exception:
            continue
        if isinstance(obj, dict):
            out.append(obj)
    return out


def _load_final_decision_texts(project_full: str) -> List[str]:
    p = state_dir(project_full) / "decisions.jsonl"
    items = _read_jsonl_objects(p, max_lines=1200)
    out: List[str] = []
    for it in items:
        t = str(it.get("text") or "").strip()
        if t:
            out.append(t)
    return out


def _load_candidate_decision_texts(project_full: str) -> List[str]:
    """
    Optional: use only candidate rows that include text and are pending.
    (Status update rows don't include text; ignore them.)
    """
    p = state_dir(project_full) / "decision_candidates.jsonl"
    items = _read_jsonl_objects(p, max_lines=1600)
    out: List[str] = []
    for it in items:
        if str(it.get("status") or "").strip().lower() != "pending":
            continue
        t = str(it.get("text") or "").strip()
        if t:
            out.append(t)
    return out


def _overlap_score(decision_text: str, upload_text: str) -> Tuple[int, List[str]]:
    dkw = set(_tokenize_keywords(decision_text))
    ukw = set(_tokenize_keywords(upload_text))
    inter = sorted(list(dkw.intersection(ukw)))
    return len(inter), inter


def _detect_simple_conflict(decision_text: str, upload_text: str) -> Tuple[bool, str]:
    """
    Dumb-but-useful conflict check (v1):
    only fires on clear material-direction conflicts.
    """
    d = set(_tokenize_keywords(decision_text))
    u = set(_tokenize_keywords(upload_text))

    # Minimal conservative pairs (expand later if needed)
    conflict_pairs = [
        ("tile", "concrete"),
        ("tile", "carpet"),
        ("wood", "tile"),
    ]

    for a, b in conflict_pairs:
        if (a in d and b in u) or (b in d and a in u):
            return True, f"{a} vs {b}"

    return False, ""

# ---------------------------------------------------------------------------
# Upload frames (must match server.py schema exactly)
# ---------------------------------------------------------------------------

def ws_frame(v: int, type_: str, **fields: Any) -> str:
    obj: Dict[str, Any] = {"v": int(v), "type": str(type_)}
    for k, val in fields.items():
        if val is None:
            continue
        obj[k] = val
    return json.dumps(obj, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Upload queue / worker state (moved)
# ---------------------------------------------------------------------------

UPLOAD_QUEUE: Optional[asyncio.Queue] = None
UPLOAD_WORKER_TASKS: List[asyncio.Task] = []
UPLOAD_STATUS: Dict[str, Dict[str, Any]] = {}  # key: canonical_rel -> status dict
UPLOAD_STATUS_LOCK = asyncio.Lock()

# Project-scoped websocket registry so background workers can post "Processed:" updates.
WS_CLIENTS: Dict[str, set] = {}  # key: current_project_full -> set(websocket)


def ws_add_client(project_full: str, ws: Any) -> None:
    pf = (project_full or "").strip()
    if not pf:
        return
    s = WS_CLIENTS.get(pf)
    if s is None:
        s = set()
        WS_CLIENTS[pf] = s
    s.add(ws)


def ws_remove_client(project_full: str, ws: Any) -> None:
    pf = (project_full or "").strip()
    if not pf:
        return
    s = WS_CLIENTS.get(pf)
    if not s:
        return
    try:
        s.discard(ws)
    except Exception:
        pass
    if not s:
        try:
            del WS_CLIENTS[pf]
        except Exception:
            pass


def ws_move_client(old_project_full: str, new_project_full: str, ws: Any) -> None:
    if old_project_full == new_project_full:
        return
    ws_remove_client(old_project_full, ws)
    ws_add_client(new_project_full, ws)


async def broadcast_to_project(project_full: str, text: str) -> None:
    pf = (project_full or "").strip()
    if not pf:
        return
    clients = list(WS_CLIENTS.get(pf) or [])
    if not clients:
        return
    for ws in clients:
        try:
            await ws.send(text)
        except Exception:
            # Drop dead sockets quietly
            try:
                ws_remove_client(pf, ws)
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Tail helpers used for upload status
# ---------------------------------------------------------------------------

def _tail_text_lines(path: Path, max_lines: int = 50, max_bytes: int = 256_000) -> List[str]:
    try:
        if not path.exists():
            return []
        size = path.stat().st_size
        start = max(0, size - max_bytes)
        with path.open("rb") as f:
            f.seek(start)
            data = f.read()
        txt = data.decode("utf-8", errors="replace")
        lines = [ln for ln in txt.splitlines() if ln.strip()]
        return lines[-max_lines:]
    except Exception:
        return []


def get_recent_upload_status(current_project_full: str, limit: int = 20) -> List[Dict[str, Any]]:
    """
    Read the tail of assets_index.jsonl and return recent upload records.
    """
    p = state_dir(current_project_full) / "assets_index.jsonl"
    lines = _tail_text_lines(p, max_lines=max(200, limit * 5))
    out: List[Dict[str, Any]] = []
    for ln in reversed(lines):
        try:
            obj = json.loads(ln)
            if isinstance(obj, dict):
                out.append(obj)
        except Exception:
            continue
        if len(out) >= limit:
            break
    return list(reversed(out))
def get_project_pending_uploads(project_full: str) -> List[Dict[str, Any]]:
    """
    Return a list of status dicts for uploads in this project that are not ready yet.
    Deterministic, reads in-memory UPLOAD_STATUS only.
    """
    pf = (project_full or "").strip()
    if not pf:
        return []

    out: List[Dict[str, Any]] = []
    try:
        for _k, v in (UPLOAD_STATUS or {}).items():
            if not isinstance(v, dict):
                continue
            if str(v.get("project_full") or "").strip() != pf:
                continue
            st = str(v.get("state") or "").strip().lower()
            if st and st not in ("ready", "done"):
                out.append(v)
    except Exception:
        return out

    return out


async def wait_for_project_uploads_ready(project_full: str, timeout_s: float = 2.0) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Wait briefly (bounded) for all uploads in this project to reach state 'ready' (or 'done').
    Returns (all_ready, pending_list).
    """
    pf = (project_full or "").strip()
    if not pf:
        return True, []

    try:
        timeout = float(timeout_s or 0.0)
    except Exception:
        timeout = 0.0

    if timeout <= 0.0:
        pend0 = get_project_pending_uploads(pf)
        return (len(pend0) == 0), pend0

    deadline = asyncio.get_running_loop().time() + timeout

    # Tight poll, bounded time. No sleeps on the event loop longer than 0.1s.
    while True:
        pend = get_project_pending_uploads(pf)
        if not pend:
            return True, []

        if asyncio.get_running_loop().time() >= deadline:
            return False, pend

        await asyncio.sleep(0.08)


# ---------------------------------------------------------------------------
# Delta helper (moved because worker depends on it)
# ---------------------------------------------------------------------------

def build_upload_delta(project_root: Path, project_name: str, new_rel_path: str, prev_rel_path: str, orig_name: str) -> Tuple[str, str]:
    new_rel_path = (new_rel_path or "").replace("\\", "/")
    prev_rel_path = (prev_rel_path or "").replace("\\", "/")
    suffix = Path(orig_name or new_rel_path).suffix.lower()

    if not prev_rel_path:
        brief = f"{orig_name}: first upload (no prior version)."
        md = "\n".join([
            f"# File delta: {orig_name}",
            "",
            "- Status: first upload (no previous canonical version found).",
            f"- New path: {new_rel_path}",
        ])
        return brief, md

    old_path = project_root / prev_rel_path
    new_path = project_root / new_rel_path
    if not old_path.exists():
        brief = f"{orig_name}: previous version missing on disk; treating as first upload."
        md = "\n".join([
            f"# File delta: {orig_name}",
            "",
            "- Status: previous canonical version not found on disk.",
            f"- Previous path: {prev_rel_path}",
            f"- New path: {new_rel_path}",
        ])
        return brief, md

    if is_text_suffix(suffix):
        old_text = read_text_file(old_path)
        new_text = read_text_file(new_path)
        old_lines = old_text.splitlines()
        new_lines = new_text.splitlines()

        sm = difflib.SequenceMatcher(a=old_lines, b=new_lines)
        adds = dels = reps = 0
        for tag, i1, i2, j1, j2 in sm.get_opcodes():
            if tag == "insert":
                adds += (j2 - j1)
            elif tag == "delete":
                dels += (i2 - i1)
            elif tag == "replace":
                reps += max(i2 - i1, j2 - j1)

        struct_note = ""
        if suffix == ".py":
            import re
            pat = re.compile(r"^\s*(def|class)\s+([A-Za-z_][A-Za-z0-9_]*)\b")
            old_syms = {m.group(2) for m in (pat.match(line) for line in old_lines) if m}
            new_syms = {m.group(2) for m in (pat.match(line) for line in new_lines) if m}
            struct_note = f"- Top-level symbols: +{len(new_syms-old_syms)} / -{len(old_syms-new_syms)}"

        brief = f"{orig_name}: +{adds} lines, -{dels} lines, ~{reps} changed blocks."
        md_lines = [
            f"# File delta: {orig_name}",
            "",
            f"- Previous path: {prev_rel_path}",
            f"- New path: {new_rel_path}",
            "",
            "## Summary",
            f"- Line delta (approx): +{adds}, -{dels}, ~{reps}",
        ]
        if struct_note:
            md_lines.append(struct_note)
        return brief, "\n".join(md_lines)

    old_b = read_bytes_file(old_path)
    new_b = read_bytes_file(new_path)
    brief = f"{orig_name}: binary/unknown diff — {len(old_b)}B → {len(new_b)}B."
    md = "\n".join([
        f"# File delta: {orig_name}",
        "",
        f"- Previous path: {prev_rel_path}",
        f"- New path: {new_rel_path}",
        "",
        "## Summary",
        f"- Size: {len(old_b)} bytes → {len(new_b)} bytes",
    ])
    return brief, md


# ---------------------------------------------------------------------------
# Enqueue entrypoint (called by server.py handle_file_added)
# ---------------------------------------------------------------------------

async def enqueue_file_added(
    *,
    current_project_full: str,
    rel: str,
    current_project: str,
    project_root: Optional[Path] = None,
) -> str:
    """
    Enqueue an uploaded file for background processing.
    Returns the upload.status frame (queued) as a JSON string.
    """
    rel = (rel or "").strip().replace("\\", "/")
    if not rel:
        return "Received [FILE_ADDED] with no path; skipping."

    pr = project_root or _PROJECT_ROOT
    if pr is None:
        return "[ERROR] upload pipeline not configured (missing project_root)."
    pr = Path(pr).resolve()

    saved_abs = pr / rel
    orig_name = Path(rel).name

    deduped = False
    try:
        updated, canonical_rel, prev_rel = register_raw_file(current_project_full, saved_abs, orig_name)
        deduped = (canonical_rel or "").replace("\\", "/").strip() != (rel or "").replace("\\", "/").strip()

        # Ensure the file physically exists in the canonical raw directory.
        # register_raw_file may update metadata without copying bytes.
        try:
            dest_abs = pr / canonical_rel
            if not dest_abs.exists() and saved_abs.exists():
                dest_abs.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(saved_abs, dest_abs)
        except Exception:
            pass
        # AOF v2: set ephemeral focus to the canonical rel path immediately.
        # This makes follow-up chat turns deterministically refer to the newest upload.
        try:
            focus_abs = (pr / canonical_rel).resolve()
            focus_bytes = b""
            try:
                if focus_abs.exists() and focus_abs.is_file():
                    focus_bytes = read_bytes_file(focus_abs)
            except Exception:
                focus_bytes = b""

            sha = ""
            if focus_bytes:
                try:
                    sha = hashlib.sha256(focus_bytes).hexdigest()
                except Exception:
                    sha = ""

            suf = Path(canonical_rel).suffix.lower()
            mime = ""
            if suf in (".png", ".jpg", ".jpeg", ".webp", ".gif"):
                mime = "image/jpeg" if suf in (".jpg", ".jpeg") else f"image/{suf.lstrip('.')}"
            elif suf == ".pdf":
                mime = "application/pdf"
            elif suf:
                mime = "text/plain"

            save_active_object(
                current_project_full,
                {
                    "rel_path": canonical_rel,
                    "orig_name": orig_name,
                    "sha256": sha,
                    "mime": mime,
                    "set_reason": "file_uploaded_canonical",
                },
            )
        except Exception:
            pass

        global UPLOAD_QUEUE
        if UPLOAD_QUEUE is None:
            UPLOAD_QUEUE = asyncio.Queue()

        # Track queued state immediately
        try:
            append_jsonl(
                state_dir(current_project_full) / "assets_index.jsonl",
                {
                    "ts": now_iso(),
                    "state": "queued",
                    "orig_name": orig_name,
                    "canonical_rel": canonical_rel,
                    "prev_rel": prev_rel,
                    "updated": bool(updated),
                    "deduped": bool(deduped),
                    "sha256": sha,
                    "ingest_version": INGEST_PIPELINE_VERSION,
                    "analysis_version": ANALYSIS_PIPELINE_VERSION,
                },
            )
        except Exception:
            pass

        async with UPLOAD_STATUS_LOCK:
            UPLOAD_STATUS[canonical_rel] = {
                "state": "queued",
                "ts": now_iso(),
                "project_full": current_project_full,
                "orig_name": orig_name,
            }

        await UPLOAD_QUEUE.put(
            {
                "project_full": current_project_full,
                "canonical_rel": canonical_rel,
                "prev_rel": prev_rel,
                "orig_name": orig_name,
                "updated": bool(updated),
                "deduped": bool(deduped),
                "sha256": sha,
                "analysis_version": ANALYSIS_PIPELINE_VERSION,
            }
        )

        qsize = 0
        try:
            qsize = int(UPLOAD_QUEUE.qsize())
        except Exception:
            qsize = 0

        status = "updated" if updated else "added"
        if deduped:
            status = "duplicate"
        return ws_frame(
            1,
            "upload.status",
            project=current_project,
            file_key=orig_name,
            status="queued",
            detail=f"{orig_name} — {status}. ({qsize} pending)",
            relative_path=canonical_rel,
            analysis_version=ANALYSIS_PIPELINE_VERSION,
        )

    except Exception as e:
        return f"[ERROR] ingest failed: {e!r}"


# ---------------------------------------------------------------------------
# Worker loop (moved)
# ---------------------------------------------------------------------------

async def _upload_worker_loop(worker_id: int) -> None:
    global UPLOAD_QUEUE
    while True:
        if UPLOAD_QUEUE is None:
            await asyncio.sleep(0.25)
            continue

        job = await UPLOAD_QUEUE.get()
        try:
            pr = _PROJECT_ROOT
            if pr is None:
                raise RuntimeError("upload pipeline not configured (project_root not set).")

            project_full = str(job.get("project_full") or "")
            canonical_rel = str(job.get("canonical_rel") or "")
            prev_rel = str(job.get("prev_rel") or "")
            orig_name = str(job.get("orig_name") or "")
            updated = bool(job.get("updated", False))
            batch_id = str(job.get("batch_id") or "").strip()
            delta_brief = ""
            # Ensure msg is always defined even if later branches skip assignment.
            # This is a safe default and will be overwritten on success paths.
            msg = f"Got it — I saved {orig_name}."

            # Mark processing
            async with UPLOAD_STATUS_LOCK:
                UPLOAD_STATUS[canonical_rel] = {
                    "state": "processing",
                    "ts": now_iso(),
                    "project_full": project_full,
                    "orig_name": orig_name,
                }

            # Run the SAME ingest pipeline (quality unchanged)
            # IMPORTANT: this can be CPU/blocking (pdf2image + tesseract), so run it off the event loop.
            suf0 = Path(canonical_rel).suffix.lower()
            zip_handled = False
            if suf0 == ".zip":
                # ZIP: extract to raw/ and enqueue each file for normal processing
                summary = _extract_zip_to_raw(project_full, zip_abs=(pr / canonical_rel), zip_orig_name=orig_name)
                extracted = summary.get("extracted") if isinstance(summary, dict) else []
                skipped = summary.get("skipped") if isinstance(summary, dict) else []
                if not isinstance(extracted, list):
                    extracted = []
                if not isinstance(skipped, list):
                    skipped = []

                batch_id = ""
                try:
                    files_for_batch = [str(e.get("canonical_rel") or "") for e in extracted if isinstance(e, dict)]
                    batch_id = register_upload_batch(project_full, zip_name=orig_name, files=files_for_batch)
                except Exception:
                    batch_id = ""

                # Enqueue extracted files
                for e in extracted:
                    if not isinstance(e, dict):
                        continue
                    try:
                        await _enqueue_extracted_file(
                            project_full=project_full,
                            canonical_rel=str(e.get("canonical_rel") or ""),
                            orig_name=str(e.get("orig_name") or ""),
                            updated=bool(e.get("updated", False)),
                            batch_id=batch_id,
                        )
                    except Exception:
                        continue

                # Mark ready for the zip container itself
                try:
                    append_jsonl(
                        state_dir(project_full) / "assets_index.jsonl",
                        {
                            "ts": now_iso(),
                            "state": "ready",
                            "orig_name": orig_name,
                            "canonical_rel": canonical_rel,
                            "prev_rel": prev_rel,
                            "updated": bool(updated),
                            "zip_files": len(extracted),
                            "zip_skipped": len(skipped),
                            "analysis_version": ANALYSIS_PIPELINE_VERSION,
                            "ingest_version": INGEST_PIPELINE_VERSION,
                        },
                    )
                except Exception:
                    pass

                async with UPLOAD_STATUS_LOCK:
                    UPLOAD_STATUS[canonical_rel] = {
                        "state": "ready",
                        "ts": now_iso(),
                        "project_full": project_full,
                        "orig_name": orig_name,
                    }

                msg = f"Unpacked {len(extracted)} file(s) from {orig_name}."
                if skipped:
                    msg = msg + f" Skipped {len(skipped)} item(s)."

                zip_handled = True

            if not zip_handled:
                _summary = await asyncio.to_thread(
                    ingest_uploaded_file,
                    project_full,
                    canonical_rel,
                    caption_image_fn=_CAPTION_IMAGE_FN,
                    classify_image_fn=_CLASSIFY_IMAGE_FN,
                )

            # -----------------------------------------------------------------
            # HARD REQUIREMENT: image uploads MUST have vision semantics
            # (except in analysis mode, where OCR-first is allowed).
            # -----------------------------------------------------------------
            suf = Path(canonical_rel).suffix.lower()
            analysis_mode = _analysis_hat_active(project_full)
            if (not zip_handled) and (not analysis_mode) and (suf in (".png", ".jpg", ".jpeg", ".webp", ".gif")):
                ok_v, note_v = await _ensure_image_semantics_required(project_full, canonical_rel, orig_name)

                if not ok_v:
                    short_proj = (project_full or "").split("/", 1)[-1].strip() or "default"
                    await broadcast_to_project(
                        project_full,
                        ws_frame(
                            1,
                            "upload.status",
                            project=short_proj,
                            file_key=orig_name,
                            status="error",
                            detail=(
                                "Vision semantics are REQUIRED for image uploads.\n"
                                "This image could not be processed with vision.\n"
                                f"Reason: {note_v or 'unknown'}\n\n"
                                "Fix:\n"
                                "- Set OPENAI_VISION_MODEL to a vision-capable model\n"
                                "- Set OPENAI_API_KEY\n"
                                "- Restart the server and re-upload the image"
                            ),
                            relative_path=canonical_rel,
                            analysis_version=ANALYSIS_PIPELINE_VERSION,
                        ),
                    )
                    raise RuntimeError("vision_semantics_required_but_unavailable")

            # Delta computation can also touch disk; keep it off the loop for safety.
            if not zip_handled:
                delta_brief, delta_md = await asyncio.to_thread(
                    build_upload_delta,
                    pr,
                    project_full,
                    canonical_rel,
                    prev_rel,
                    orig_name,
                )

                create_artifact(
                    project_full,
                    f"file_delta_{Path(orig_name).stem or 'upload'}",
                    delta_md,
                    artifact_type="file_delta",
                    from_files=[p for p in [prev_rel, canonical_rel] if p],
                    file_ext=".md",
                )

            # Pull latest classification for this file (best-effort)
            cls_obj: Dict[str, Any] = {}
            try:
                m2 = load_manifest(project_full)
                arts2 = m2.get("artifacts") or []
                for a in reversed(arts2):
                    if a.get("type") != "image_classification":
                        continue
                    ff = a.get("from_files") or []
                    ff_norm = [(x or "").replace("\\", "/").strip() for x in ff] if isinstance(ff, list) else []
                    if canonical_rel in ff_norm:
                        raw_cls = read_artifact_text(project_full, a, cap_chars=8000).strip()
                        if raw_cls.startswith("{"):
                            cls_obj = json.loads(raw_cls) if raw_cls else {}
                        break
            except Exception:
                cls_obj = {}

            # Update assets index with "ready"
            try:
                append_jsonl(
                    state_dir(project_full) / "assets_index.jsonl",
                    {
                        "ts": now_iso(),
                        "state": "ready",
                        "orig_name": orig_name,
                        "canonical_rel": canonical_rel,
                        "prev_rel": prev_rel,
                        "updated": bool(updated),
                        "delta_brief": delta_brief,
                        "classification": cls_obj,
                        "analysis_version": ANALYSIS_PIPELINE_VERSION,
                        "ingest_version": INGEST_PIPELINE_VERSION,
                    },
                )
            except Exception:
                pass

            # Mark ready
            async with UPLOAD_STATUS_LOCK:
                UPLOAD_STATUS[canonical_rel] = {
                    "state": "ready",
                    "ts": now_iso(),
                    "project_full": project_full,
                    "orig_name": orig_name,
                    "batch_id": batch_id,
                }

            # Push a compact completion message
            label = (cls_obj.get("label") or "").strip()
            bucket = (cls_obj.get("suggested_bucket") or "").strip()
            rel_score = cls_obj.get("relevance", None)
            tags = cls_obj.get("tags") or []
            if not isinstance(tags, list):
                tags = []

            ask_user = bool(cls_obj.get("ask_user", False))
            question = (cls_obj.get("question") or "").strip()

            status = "updated" if updated else "added"

            extra = []
            if label:
                extra.append(label)
            if bucket:
                extra.append(f"→ {bucket}")
            if rel_score is not None:
                try:
                    extra.append(f"({float(rel_score):.2f})")
                except Exception:
                    pass

            # Pull project goal (if any) to keep responses aligned
            goal = ""
            try:
                m = load_manifest(project_full)
                goal = str(m.get("goal") or "").strip()
            except Exception:
                goal = ""

            # Upload worker must NOT speak in the expert voice.
            # Only emit machine/status frames; the expert will synthesize when the user inserts into chat.
            line1 = ""

            # Plain-language "where it lives" without showing raw bucket paths.
            bucket_label_map = {
                "screenshots/": "with your screenshots",
                "references/": "as a reference",
                "specs/": "with your specs",
                "design/": "with your design references",
                "receipts/": "with receipts",
                "inputs/": "with your inputs",
                "code/": "with your code references",
                "misc/": "with the rest of the project files",
            }
            place = bucket_label_map.get((bucket or "").strip(), "as a reference")

            desc = ""

            # Prefer human-facing vision semantics summary if available
            sem_text = ""
            try:
                sem_text = _find_latest_image_semantics_text(
                    project_full,
                    canonical_rel,
                    cap_chars=2400,
                )
            except Exception:
                sem_text = ""

            sem_summary = ""
            if sem_text:
                try:
                    sem_obj = json.loads(sem_text)
                    if isinstance(sem_obj, dict):
                        out = sem_obj.get("outputs")
                        if isinstance(out, dict):
                            sem_summary = str(out.get("summary") or "").strip()
                except Exception:
                    sem_summary = ""

            if sem_summary:
                desc = sem_summary
            elif label:
                # Fallback: classification label only if semantics unavailable
                desc = f"It looks like {label}."

            line2 = desc
            line3 = f"I’ve filed it {place} for now."

            line4 = ""
            if ask_user:
                # No instructions, no triggers—just a soft door-open.
                line4 = "If this should drive a decision or a deliverable later, we can use it."

            # C3 v1: build a deterministic decision-aware note (optional).
            # Evidence comes from the best-available summary/classification fields we already computed.
            evidence_parts: List[str] = []
            try:
                if label:
                    evidence_parts.append(str(label))
                if tags:
                    evidence_parts.append(" ".join([str(t) for t in tags[:12] if str(t).strip()]))
                # Pull a small slice from classification rationale/question if present (best-effort)
                rat = str(cls_obj.get("rationale") or "").strip()
                if rat:
                    evidence_parts.append(rat[:260])
                q0 = str(cls_obj.get("question") or "").strip()
                if q0:
                    evidence_parts.append(q0[:220])

                # Also pull latest caption/OCR artifacts if present (bounded, best-effort)
                m3 = load_manifest(project_full)
                arts3 = m3.get("artifacts") or []
                # Search backwards for artifacts tied to this file
                for a3 in reversed(arts3[-120:]):
                    if a3.get("type") not in ("image_caption", "ocr_text"):
                        continue
                    ff3 = a3.get("from_files") or []
                    ff3_norm = [(x or "").replace("\\", "/").strip() for x in ff3] if isinstance(ff3, list) else []
                    if canonical_rel not in ff3_norm:
                        continue
                    t3 = read_artifact_text(project_full, a3, cap_chars=900).strip()
                    if t3:
                        evidence_parts.append(t3[:500])
                    # Grab at most one of each
                    # (We don't need both to match; keep it bounded.)
                    if a3.get("type") == "image_caption":
                        # keep looking for ocr_text too
                        continue
            except Exception:
                pass

            evidence_text = " ".join([p for p in evidence_parts if p]).strip()

            # C3 v1 relevance router (deterministic): load decisions via project_store and do overlap.
            # Append ONE contextual sentence max. No decision mutation. No model calls.
            best_text = ""
            best_score = 0
            c3_note = ""
            try:
                dec_rows = load_decisions(project_full)
                dec_texts = [
                    str(r.get("text") or "").strip()
                    for r in (dec_rows or [])
                    if isinstance(r, dict) and str(r.get("text") or "").strip()
                ]

                best_text, best_score = _c3_best_decision_match(dec_texts, evidence_text)

                # Conservative threshold
                if best_text and best_score >= 3:
                    topic = best_text.strip()
                    if len(topic) > 72:
                        topic = topic[:71].rstrip() + "…"

                    ev_l = evidence_text.lower()
                    dt_l = best_text.lower()

                    # Scope-check cue (still ONE sentence)
                    if ("floor" in dt_l) and (("shower" in ev_l) or ("wall" in ev_l)):
                        c3_note = f"This looks related to your earlier decision on “{topic}”, but this upload reads more like shower/wall than floor."
                    else:
                        c3_note = f"This looks consistent with your earlier decision on “{topic}”."
            except Exception:
                best_text = ""
                best_score = 0
                c3_note = ""
            # C8.1.A — If we deterministically matched this upload to an existing decision text,
            # write an upload_to_decision link as evidence (append-only).
            try:
                if best_text and best_score >= 3:
                    drow = find_current_decision_by_text(project_full, best_text)
                    if isinstance(drow, dict):
                        did = str(drow.get("id") or "").strip()
                        if did:
                            append_link_event(
                                project_full,
                                type_="upload_to_decision",
                                from_ref=f"upload:{canonical_rel}",
                                to_ref=f"decision:{did}",
                                reason="keyword_overlap_match",
                                confidence="medium",
                            )
            except Exception:
                pass

            # -----------------------------------------------------------------
            # C4 v1 — Upload Clarification Loop (single question, deterministic)
            #
            # UX CHANGE:
            # Do NOT ask or persist clarification questions at upload-ready time.
            # We only surface upload interpretation when the user clicks "Insert to chat"
            # (which sends the filename into chat). The server will handle the question then.
            # -----------------------------------------------------------------
            c4_question = ""

            # Default: human-friendly upload completion message
            msg_lines = [line1]
            if line2:
                msg_lines.append(line2)

            # If we have a clarification question, show it prominently (one question max).
            if c4_question:
                msg_lines.append(c4_question)

            # Context note (optional, deterministic)
            if c3_note:
                msg_lines.append(c3_note)

            if line4:
                msg_lines.append(line4)
            # C10/C4 guard: ensure msg is ALWAYS defined for broadcast,
            # even when the Wordle branch does not run.
            #
            # Companion Flow rule: do not broadcast human/narrative text from the upload worker.
            # The expert response happens only on a normal chat turn.
            if not zip_handled:
                msg = ""

            # Persist a human-facing assistant_output artifact for uploads.
            # This ensures the UI can always surface a natural description (and not a generic intake template),
            # even if the user later references the file indirectly.
            try:
                suf3 = Path(canonical_rel).suffix.lower()
                if suf3 in (".png", ".jpg", ".jpeg", ".webp", ".gif"):
                    sem_text2 = _find_latest_image_semantics_text(project_full, canonical_rel, cap_chars=220000)
                    sem_summary2 = ""
                    sem_obs2: List[str] = []
                    sem_top_line2 = ""

                    if sem_text2:
                        try:
                            sem_obj2 = json.loads(sem_text2)
                        except Exception:
                            sem_obj2 = {}

                        out2 = sem_obj2.get("outputs") if isinstance(sem_obj2, dict) else None
                        if not isinstance(out2, dict):
                            out2 = {}

                        sem_summary2 = str(out2.get("summary") or "").strip()

                        obs2 = out2.get("observations")
                        if isinstance(obs2, list):
                            sem_obs2 = [str(x).strip() for x in obs2 if str(x).strip()][:5]

                        cands2 = out2.get("candidates")
                        if isinstance(cands2, list) and cands2 and isinstance(cands2[0], dict):
                            top2 = cands2[0]
                            mk2 = str(top2.get("make") or "").strip()
                            md2 = str(top2.get("model") or "").strip()
                            try:
                                cf2 = float(top2.get("confidence")) if top2.get("confidence") is not None else None
                            except Exception:
                                cf2 = None
                            if mk2 or md2:
                                if isinstance(cf2, float):
                                    sem_top_line2 = f"Likely ID: {mk2} {md2}".strip() + f" (confidence {cf2:.2f})"
                                else:
                                    sem_top_line2 = f"Likely ID: {mk2} {md2}".strip()

                    if sem_summary2:
                        ao_lines: List[str] = []
                        ao_lines.append(sem_summary2)
                        if sem_top_line2:
                            ao_lines.append("")
                            ao_lines.append(sem_top_line2)
                        if sem_obs2:
                            ao_lines.append("")
                            ao_lines.append("Visible cues:")
                            for ob in sem_obs2:
                                ao_lines.append(f"- {ob}")

                        create_artifact(
                            project_full,
                            f"assistant_output_{Path(orig_name).stem or 'upload'}",
                            "\n".join(ao_lines).strip() + "\n",
                            artifact_type="assistant_output",
                            from_files=[canonical_rel],
                            file_ext=".md",
                        )
            except Exception:
                pass

            # -----------------------------------------------------------------
            # WORDLE: deterministic constraint enforcement + auto action on upload
            #
            # Requirement:
            # - Every Wordle screenshot upload MUST immediately produce:
            #   - state update
            #   - next-word suggestion
            # - No questions, no explanations, word-only
            # - If cannot produce a valid move:
            #   NO VALID MOVE — CHECK INPUT
            # -----------------------------------------------------------------
            try:
                is_wordle = False
                suf = Path(canonical_rel).suffix.lower()
                if suf in (".png", ".jpg", ".jpeg", ".webp", ".gif"):
                    is_wordle = _looks_like_wordle_classification(cls_obj)

                if is_wordle:
                    # Extract structured Wordle state deterministically
                    abs_img = (pr / canonical_rel).resolve()
                    ws_obj = visual_semantics.analyze_image(abs_img)

                    # Persist state + constraints (dynamic, authoritative)
                    constraints = _compile_wordle_constraints(ws_obj)

                    st_path, c_path = _wordle_state_paths(project_full)

                    _ = write_canonical_entry(
                        project_full,
                        target_path=st_path,
                        mode="json_overwrite",
                        data=(ws_obj if isinstance(ws_obj, dict) else {}),
                    )
                    _ = write_canonical_entry(
                        project_full,
                        target_path=c_path,
                        mode="json_overwrite",
                        data=constraints,
                    )

                    # Solve next guess deterministically
                    words = _load_wordle_wordlist(pr)
                    guess = _pick_next_wordle_guess(words, constraints)

                    # Wordle output must be word-only (or the sentinel string)
                    msg = guess
            except Exception:
                # If anything fails, do not guess. Emit sentinel.
                msg = "NO VALID MOVE — CHECK INPUT"

            short_proj = (project_full or "").split("/", 1)[-1].strip() or "default"
            await broadcast_to_project(
                project_full,
                ws_frame(
                    1,
                    "upload.status",
                    project=short_proj,
                    file_key=orig_name,
                    status="done",
                    detail=msg,
                    relative_path=canonical_rel,
                    batch_id=batch_id,
                    analysis_version=ANALYSIS_PIPELINE_VERSION,
                ),
            )

        except Exception as e:
            # Mark failed
            try:
                canonical_rel = str(job.get("canonical_rel") or "")
                project_full = str(job.get("project_full") or "")
                orig_name = str(job.get("orig_name") or "")
                async with UPLOAD_STATUS_LOCK:
                    UPLOAD_STATUS[canonical_rel] = {
                        "state": "failed",
                        "ts": now_iso(),
                        "project_full": project_full,
                        "orig_name": orig_name,
                        "error": repr(e),
                    }
                try:
                    append_jsonl(
                        state_dir(project_full) / "assets_index.jsonl",
                        {
                            "ts": now_iso(),
                            "state": "failed",
                            "orig_name": orig_name,
                            "canonical_rel": canonical_rel,
                            "error": repr(e),
                            "analysis_version": ANALYSIS_PIPELINE_VERSION,
                            "ingest_version": INGEST_PIPELINE_VERSION,
                        },
                    )
                except Exception:
                    pass
                short_proj = (project_full or "").split("/", 1)[-1].strip() or "default"
                await broadcast_to_project(
                    project_full,
                    ws_frame(
                        1,
                        "upload.status",
                        project=short_proj,
                        file_key=orig_name,
                        status="error",
                        detail=f"upload processing failed for {orig_name}: {e!r}",
                        relative_path=canonical_rel,
                        analysis_version=ANALYSIS_PIPELINE_VERSION,
                    ),
                )
            except Exception:
                pass
        finally:
            try:
                UPLOAD_QUEUE.task_done()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Lifecycle (called by server.py)
# ---------------------------------------------------------------------------

def start(
    *,
    project_root: Path,
    caption_image_fn=None,
    classify_image_fn=None,
    workers: int = 1,
) -> None:
    """
    Start upload worker(s). Must be called from within a running event loop context
    (server.py's async main()) because it uses asyncio.create_task().
    """
    configure(
        project_root=project_root,
        caption_image_fn=caption_image_fn,
        classify_image_fn=classify_image_fn,
    )

    global UPLOAD_QUEUE, UPLOAD_WORKER_TASKS
    if UPLOAD_QUEUE is None:
        UPLOAD_QUEUE = asyncio.Queue()

    if not UPLOAD_WORKER_TASKS:
        n = int(workers) if int(workers) > 0 else 1
        for i in range(n):
            UPLOAD_WORKER_TASKS.append(asyncio.create_task(_upload_worker_loop(i + 1)))


def stop() -> None:
    """
    Stop upload worker(s).
    """
    global UPLOAD_WORKER_TASKS
    try:
        for t in UPLOAD_WORKER_TASKS:
            try:
                t.cancel()
            except Exception:
                pass
        UPLOAD_WORKER_TASKS.clear()
    except Exception:
        pass
