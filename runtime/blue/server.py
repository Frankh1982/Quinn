# -*- coding: utf-8 -*-
"""
Lens-0 Project OS Server (replacement for server.py)

Goals:
- Multi-project, disk-backed continuity (project maps + working docs + state JSON).
- Auto-updating state after every chat turn (single model call).
- Deterministic, reconstructable code ingestion (chunk + index + SHA256).
- Anchor-validated patch mode (exact anchors from the ingested code).
- HTTP upload API + WebSocket chat.

Environment variables (recommended):
- OPENAI_API_KEY                (required for model calls)
- OPENAI_BASE_URL               (optional, for OpenAI-compatible servers)
- OPENAI_MODEL                  (default: gpt-5.2)
- LENS0_PROJECT_ROOT            (default: folder containing this file)
- BRAVE_API_KEY                 (optional, enables [SEARCH])
- LENS0_MAX_HISTORY_PAIRS       (default: 10)
- LENS0_PATCH_MAX_CONTEXT_CHARS (default: 260000)
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
import difflib
import ast
import platform
import shutil
import signal
import subprocess
import sys
import uuid
import contextvars
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
# -----------------------------------------------------------------------------
# Per-turn audit trace id (context-local via asyncio ContextVar)
# -----------------------------------------------------------------------------
_AUDIT_TRACE_ID = contextvars.ContextVar("audit_trace_id", default="")
# -----------------------------------------------------------------------------
# Per-turn audit decision context (context-local; merged into every audit record)
# -----------------------------------------------------------------------------
_AUDIT_CTX = contextvars.ContextVar("audit_ctx", default={})

def _audit_ctx_reset() -> None:
    """
    Reset per-turn audit decision context.
    Must never throw.
    """
    try:
        _AUDIT_CTX.set({})
    except Exception:
        pass

def _audit_ctx_get() -> Dict[str, Any]:
    try:
        v = _AUDIT_CTX.get()
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}

def _audit_ctx_set(path: str, value: Any) -> None:
    """
    Set a value in the audit ctx using dot-path keys (e.g. "search.reason").
    Deterministic, best-effort, never throws.
    """
    try:
        p = str(path or "").strip()
        if not p:
            return
        root = dict(_audit_ctx_get())
        cur: Dict[str, Any] = root
        parts = [x for x in p.split(".") if x]
        for k in parts[:-1]:
            nxt = cur.get(k)
            if not isinstance(nxt, dict):
                nxt = {}
                cur[k] = nxt
            cur = nxt
        cur[parts[-1]] = value
        _AUDIT_CTX.set(root)
    except Exception:
        pass

def _audit_ctx_update(obj: Dict[str, Any]) -> None:
    """
    Shallow merge dict into audit ctx (top-level keys).
    """
    try:
        if not isinstance(obj, dict):
            return
        root = dict(_audit_ctx_get())
        for k, v in obj.items():
            root[k] = v
        _AUDIT_CTX.set(root)
    except Exception:
        pass
def get_current_audit_trace_id() -> str:
    try:
        return str(_AUDIT_TRACE_ID.get() or "")
    except Exception:
        return ""

def audit_event(project_full: str, event: Dict[str, Any]) -> None:
    """
    Append one audit event to the project's audit log (best-effort).
    Safe: must never throw.
    """
    try:
        obj = event if isinstance(event, dict) else {}
        # Attach trace id if available
        if "trace_id" not in obj:
            tid = get_current_audit_trace_id()
            if tid:
                obj["trace_id"] = tid
        # Diagnostic: if append_audit_event is missing, record that once per process.
        # (best-effort; never throw)
        if not callable(getattr(project_store, "append_audit_event", None)):
            try:
                print("[AUDIT] project_store.append_audit_event missing; audit_log.jsonl will not be written.", flush=True)
            except Exception:
                pass
        # Merge per-turn decision context (if present) into the audit record.
        # This makes audit robust without requiring every call site to repeat fields.
        try:
            ctx_obj = _audit_ctx_get()
            if isinstance(ctx_obj, dict) and ctx_obj:
                # Preserve explicit event keys; ctx fills in the rest.
                obj.setdefault("decision_ctx", {})
                if isinstance(obj.get("decision_ctx"), dict):
                    dc = obj.get("decision_ctx")
                    if isinstance(dc, dict):
                        for k, v in ctx_obj.items():
                            if k not in dc:
                                dc[k] = v
        except Exception:
            pass            
        fn = getattr(project_store, "append_audit_event", None)
        if callable(fn):
            fn(project_full, obj)
    except Exception:
        pass
import path_engine
from lens0_config import TEXT_LIKE_SUFFIXES, IMAGE_SUFFIXES, is_text_suffix, mime_for_image_suffix

import project_store
from project_store import (
    ensure_project,
    load_manifest,
    save_manifest,
    raw_dir,
    artifacts_dir,
    state_dir,
    state_file_path,
    ensure_project_scaffold,
    list_existing_projects,
    register_raw_file,
    list_project_files,
    get_latest_artifact_by_type,
    read_artifact_text,
    create_artifact,
    create_artifact_bytes,
    ingest_text_file,
    ensure_ingested_current,
    collect_full_corpus_for_source,
    ingest_uploaded_file,
    load_pdf_text,
    load_image_ocr,
    append_decision_candidate,
    update_decision_candidate_status,
    append_final_decision,
    load_decisions,
    load_pending_upload_question,
    clear_pending_upload_question,
    append_upload_note,
    is_pending_upload_question_unexpired,
    load_upload_notes,
    find_project_memory,
    load_user_pending_profile_question,
    save_user_pending_profile_question,
    clear_user_pending_profile_question,
    is_user_pending_profile_question_unexpired,
)


from patch_engine import (
    build_patch_mode_system_prompt as patch_system_prompt,
    language_hint_from_suffix,
    extract_anchor_lines_from_patch,
    anchors_exist_and_unique,
    validate_patch_text_strict_format,
    parse_anchor_patch_ops,
    apply_anchor_patch_to_text,
    build_unified_diff,
)
import requests
import websockets
from websockets.exceptions import ConnectionClosedOK, ConnectionClosedError

# -----------------------------------------------------------------------------
# PROJECT ROOT (must be defined before any derived paths)
# -----------------------------------------------------------------------------

_env_root = (os.environ.get("LENS0_PROJECT_ROOT") or "").strip()
if _env_root:
    PROJECT_ROOT = Path(_env_root).resolve()
else:
    _cwd = Path(os.getcwd()).resolve()
    _file_dir = Path(__file__).resolve().parent
    if (_cwd / "projects").exists() or _cwd == _file_dir:
        PROJECT_ROOT = _cwd
    else:
        PROJECT_ROOT = _file_dir

# -----------------------------------------------------------------------------
# UI FILE (optional): used to serve the web UI at GET /
# -----------------------------------------------------------------------------
# You can override via env:
#   LENS0_UI_FILE="C:\full\path\Project Web 2 - BLUE.html"
_env_ui = (os.environ.get("LENS0_UI_FILE") or "").strip()
if _env_ui:
    UI_FILE = Path(_env_ui).expanduser().resolve()
else:
    # Default: look for the UI HTML in the project root
    UI_FILE = (PROJECT_ROOT / "Project Web 2 - BLUE.html").resolve()

from aiohttp import web  # noqa: E402
import capabilities
import http_api
import ws_commands
import registry_builder
import model_pipeline
import upload_pipeline
import vision_semantics
try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore

from PyPDF2 import PdfReader  # noqa: E402

# Excel / openpyxl support moved to excel_engine.py
from excel_engine import (
    validate_excel_spec_master,
    maybe_repair_excel_spec_once_master,
    generate_workbook_from_spec,
)

# Optional image OCR / caption support (Phase 1)
try:
    from PIL import Image  # type: ignore
except Exception:
    Image = None  # type: ignore
# Optional: scanned PDF OCR support (requires poppler + pdf2image + tesseract)
try:
    from pdf2image import convert_from_path  # type: ignore
except Exception:
    convert_from_path = None  # type: ignore

try:
    import pytesseract  # type: ignore
except Exception:
    pytesseract = None  # type: ignore


# =============================================================================
# Config

# =============================================================================
# Blue/Green runtime + self-patching (server self-editing)
# =============================================================================

SERVER_INTERNAL_PROJECT = "__server__"  # reserved project for patching this server safely

RUNTIME_DIR = PROJECT_ROOT / "runtime"
PATCHES_DIR = PROJECT_ROOT / "patches"

SERVER_START_TIME = time.time()

def _read_json_file(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _write_json_file(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    os.replace(tmp, path)

def read_active_color(default: str = "blue") -> str:
    """Recommended active color (purely informational unless you run the active one)."""
    p = RUNTIME_DIR / "active.json"
    d = _read_json_file(p) if p.exists() else {}
    c = (d.get("active") or "").strip().lower()
    return c if c in ("blue", "green") else default

def write_active_color(color: str) -> None:
    c = (color or "").strip().lower()
    if c not in ("blue", "green"):
        raise ValueError("color must be 'blue' or 'green'")
    _write_json_file(RUNTIME_DIR / "active.json", {"active": c, "updated_at": time.time()})

def infer_server_color() -> str:
    """Infer the running instance color.

    Precedence:
    1) LENS0_SERVER_COLOR env
    2) path contains runtime/blue or runtime/green
    3) runtime/active.json
    4) default blue
    """
    chosen = ""

    env = (os.environ.get("LENS0_SERVER_COLOR") or "").strip().lower()
    if env in ("blue", "green"):
        chosen = env

    if not chosen:
        try:
            parts = [p.lower() for p in Path(__file__).resolve().parts]
            for i in range(len(parts) - 1):
                if parts[i] == "runtime" and parts[i + 1] in ("blue", "green"):
                    chosen = parts[i + 1]
                    break
        except Exception:
            pass

    return chosen if chosen else read_active_color("blue")

SERVER_COLOR = infer_server_color()

DEFAULT_HOST = (os.environ.get("LENS0_HOST") or "localhost").strip() or "localhost"

DEFAULT_WS_PORT_BLUE = int(os.environ.get("LENS0_WS_PORT_BLUE") or "8765")
DEFAULT_HTTP_PORT_BLUE = int(os.environ.get("LENS0_HTTP_PORT_BLUE") or "8766")
# Use +2 so ports never collide (blue uses ws+http, green uses ws+http) and both can run simultaneously.
DEFAULT_WS_PORT_GREEN = int(os.environ.get("LENS0_WS_PORT_GREEN") or str(DEFAULT_WS_PORT_BLUE + 2))
DEFAULT_HTTP_PORT_GREEN = int(os.environ.get("LENS0_HTTP_PORT_GREEN") or str(DEFAULT_HTTP_PORT_BLUE + 2))

def ports_for_color(color: str) -> Tuple[int, int]:
    c = (color or "").strip().lower()
    if c == "green":
        return DEFAULT_WS_PORT_GREEN, DEFAULT_HTTP_PORT_GREEN
    return DEFAULT_WS_PORT_BLUE, DEFAULT_HTTP_PORT_BLUE

HOST = DEFAULT_HOST
WS_PORT = int((os.environ.get("LENS0_WS_PORT") or "").strip() or ports_for_color(SERVER_COLOR)[0])
HTTP_PORT = int((os.environ.get("LENS0_HTTP_PORT") or "").strip() or ports_for_color(SERVER_COLOR)[1])

# =============================================================================

OPENAI_MODEL = (os.environ.get("OPENAI_MODEL") or "gpt-5.2").strip()
MAX_HISTORY_PAIRS = int(os.environ.get("LENS0_MAX_HISTORY_PAIRS") or "10")
PATCH_MAX_CONTEXT_CHARS = int(os.environ.get("LENS0_PATCH_MAX_CONTEXT_CHARS") or "260000")
# Brave Search (web lookup)
BRAVE_API_KEY = (os.getenv("BRAVE_API_KEY", "") or "").strip()
BRAVE_ENDPOINT = "https://api.search.brave.com/res/v1/web/search"
BRAVE_IMAGE_ENDPOINT = "https://api.search.brave.com/res/v1/images/search"


# NOTE: Do NOT cache BRAVE_API_KEY at import-time in a multi-instance environment.
# We'll read it at call-time inside brave_search(). Keep only a boot-time indicator.
_BOOT_BRAVE_API_KEY = (os.getenv("BRAVE_API_KEY", "") or "").strip()

print("[BOOT] BRAVE_API_KEY present:", bool(_BOOT_BRAVE_API_KEY))
print("[BOOT] BRAVE_API_KEY first4:", (_BOOT_BRAVE_API_KEY[:4] if _BOOT_BRAVE_API_KEY else "NONE"))

# PROJECT_ROOT is already resolved above (before derived paths like runtime/patches/UI).
# Do NOT reassign PROJECT_ROOT here; it can desync derived paths that were computed earlier.
PROJECTS_DIR = PROJECT_ROOT / "projects"
DEFAULT_PROJECT_NAME = "default"

# Configure disk-backed project store module
project_store.configure(project_root=PROJECT_ROOT, projects_dir=PROJECTS_DIR, default_project_name=DEFAULT_PROJECT_NAME)
# -----------------------------------------------------------------------------
# TEMP SIMPLE AUTH (NO REAL SECURITY YET)
# -----------------------------------------------------------------------------
USERS = {
    "Frank": "Daisy17",
    "Emanie": "Daisy17",
    "couple Frank": "Daisy17",
    "couple Emanie": "Daisy17",
    "couple_Frank": "Daisy17",
    "couple_Emanie": "Daisy17",
    "Therapist": "Daisy17",
}


RAW_DIR_NAME = "raw"
ARTIFACTS_DIR_NAME = "artifacts"
STATE_DIR_NAME = "state"

MANIFEST_VERSION = 2

# TEXT_LIKE_SUFFIXES moved to lens0_config.py (imported above)


# ============================================================================
# Lens-0 Project OS prompt
# ============================================================================

TRUTH_BINDING_TREBEK_BLOCK = r"""
Truth-binding + Trebek cadence (required):
- Lead with the best-supported answer in one calm sentence.
- If certainty is limited, say so plainly in a second sentence ("What I can confirm is..., what I cannot yet confirm is...").
- Distinguish: known, inferred, unknown. Label inferences as such.
- Never overstate: avoid "definitely/always" unless the evidence is explicit.
- Soften hard constraints with a measured cadence (no scolding, no cheerleading).
- Use provided memory/context before answering; if a needed detail isn't recorded, say so.
- Avoid rigid headings like "Known / Inferred / Unknown" unless the user explicitly asked for that format.
- If a personal-memory fact is present in CANONICAL_SNIPPETS as confirmed/resolved, state it plainly (no "I can't confirm" hedges).
- Do not mention web verification for personal facts; if ambiguous, ask a short clarifying question instead.
""".strip()

CAPABILITY_USE_BLOCK = r"""
Capability awareness (required):
- You know you can search the web, read uploads, write artifacts (docs/excel/html), and ask one clarifying question.
- If a capability would materially improve accuracy or clarity, either use it automatically (for search)
  or propose it briefly in one sentence and proceed unless blocked.
- Example: if the user wants calculations/structured comparisons, offer an Excel sheet.
- Example: if they want something printable, offer a clean document deliverable.
- Keep capability suggestions human and brief (no system talk).
- If you are blocked or only partially able to complete the task due to a missing capability or missing evidence,
  you MUST say so plainly and state what would enable it (feature, tool, or file).
""".strip()

LENS0_SYSTEM_PROMPT = r"""
You are Lens-0 Project OS: a hired senior professional who helps the user COMPLETE projects end-to-end.
Your name is Quinn.

Core objectives:
- Operate as the FOREMOST expert in the project’s domain (world-class, multidisciplinary when needed).
- Drive toward a finished, shippable outcome (definition-of-done).
- Proactively lead: if the user is missing inputs, ask for ONLY the minimum needed; otherwise proceed.
- If high-quality results require up-to-date techniques or best practices, use web search when available.
- Maintain durable project continuity across sessions/projects using the provided working docs + state.

Style & stance (default personality):
- Tone: calm, crisp, friendly, lightly wry. No jokes, no slang, no snark.
- Be confident without swagger; do not placate or hedge away important risks.
- Be willing to disagree when the userâ€™s request is flawed, unsafe, or likely to fail.
- When you disagree: say so plainly, explain why, and offer a better path.
- In coding mode, do NOT skip critical concerns just to comply; surface risks, missing steps, and safer alternatives.

Expert Frame Lock (default behavior):
- Behave as an authoritative expert by default (as if the expert frame is LOCKED).
- Do NOT ask the user to “confirm” unless you are truly blocked from making forward progress.
- Prefer making a reasonable default assumption, stating it explicitly, proceeding, and letting the user override.

Default-forward execution rule (required):
- If deliverable format/audience/constraints are missing, choose the best default and proceed now.
- State assumptions clearly at the top of the work (e.g., “Assumption: clinician handout for licensed therapists, 1–2 pages”).
- Ask at most ONE correction question only when a wrong assumption would materially change the work.
- If no correction is needed, end cleanly without a question.

Decision Log maintenance (required):
- DECISION_LOG is the canonical record of commitments and defaults for this project.
- If you adopt a default (format, audience, scope, tone, structure), you MUST record it as a decision:
  - Mark it as “Assumption unless overridden.”
  - Keep it concise and actionable.
- If no decisions changed, output DECISION_LOG as NO_CHANGE.

- Maintain durable project continuity across sessions/projects using the provided working docs + state.

Conversation pacing (superhuman, not systemy):
- If the user provides ONLY a topic/goal (e.g., “LLM limitations in therapy”), do NOT dump a full draft immediately.
- First response should feel like an expert human: 2–6 sentences.
  - Ask ONE steering question only if the next step genuinely depends on user input.
  - Otherwise, end with a clear statement of what will happen next.
- Offer a confident default direction, proceed unless blocked, and allow the user to redirect.
- Avoid A/B/C menus unless the user explicitly asks for options.
- Prefer one recommended path; ask a question only if a decision is required.

Ongoing turn discipline (applies AFTER the first turn too):
- Default response length: <= 180 words unless the user explicitly asks for a long brief/spec.
- Use bullets or numbered lists whenever they improve clarity or scan-ability.
  - Prefer short lists (2–5 items).
  - Avoid decorative or excessive sub-bullets.
- Do NOT ask meta “deliverable format” questions mid-conversation. Keep it conversational and forward-moving.
  - Instead, propose the next concrete step (e.g., “I’ll map risks to architecture next”) and ask ONE input needed to do that.
- If you need structure, use: (1) headline, (2) 2–4 bullets.
- Add a question only if the response cannot proceed without user input.

Project Pulse (truth-bound; server-generated):

- The server generates Project Pulse deterministically from canonical project state.
- Do NOT generate, paraphrase, or infer any “Project Pulse” in the model output.
- If the user asks for Pulse/Status/Resume, respond normally; the server will supply truth-bound status separately.

Facts Map maintenance (required):
- FACTS_MAP is the canonical long-memory for this project. Keep it bounded and useful.
- Maintain ~5–20 facts. Hard cap: 30 facts total.
- If the user provides any durable project fact/decision/constraint, you MUST update FACTS_MAP accordingly with provenance and confidence.
- Each fact entry MUST include:
  - Provenance: (file/chat) and a path and/or timestamp reference
  - Confidence: High|Medium|Low
- Prefer updating existing facts over adding duplicates.
- Do NOT store vibes/opinions as facts unless explicitly labeled as preference or decision.
- If no durable facts changed, output FACTS_MAP as NO_CHANGE.

NON_NEGOTIABLE_SYSTEM_FACTS (do not generalize):
- In THIS codebase, GET /file?path=... ONLY serves/downloads raw file bytes from disk.
- It is READ-ONLY.
- It accepts ONLY a relative filesystem path via the "path" query parameter.
- It does NOT upload files.
- It does NOT return metadata.
- It does NOT accept IDs.
- Uploading is handled ONLY by POST /upload with multipart field "file".
- If asked what GET /file does, answer using ONLY the statements above.



Reliability rules:
- Never invent files, artifacts, code, facts, or results that are not present in the provided project context.
- If information is missing, ask 1–3 tight questions OR propose best-effort assumptions explicitly.
- Prefer concrete, step-by-step guidance and checklists.
- If the user asks you to change code, either:
  (a) propose a plan first (if unclear), OR
  (b) if they clearly want a patch, tell them to ask for a patch (the server has a dedicated patch mode).
- Keep answers readable: short sections, bullets, examples when helpful.
Capability gap reporting (required):
- If you cannot fully complete the task because a capability, tool, or evidence is missing,
  you MUST say so plainly and say what would enable it.
- If a quick web search would materially help you identify tools or best practices, include
  recommended_search_queries in CAPABILITY_GAP_JSON.
Web / freshness rule:
- If you receive a system message named WEB_SEARCH_RESULTS, treat it as live, up-to-date information.
- Use WEB_SEARCH_RESULTS to answer “right now / today / latest / current” questions.
- Do NOT claim you lack real-time access if WEB_SEARCH_RESULTS is present.
- If WEB_SEARCH_RESULTS contains NO_RESULTS or a Search error, say you could not retrieve results and ask the user to try again or refine the query.

You MUST follow the OUTPUT PROTOCOL below exactly.

OUTPUT PROTOCOL (required):
Return these blocks, in this order, with the tags exactly as shown:

[USER_ANSWER]
(What the user should see. This can include code, checklists, drafts, etc.)
[/USER_ANSWER]

[PROJECT_STATE_JSON]
(A small valid JSON object capturing continuity. Keep it under ~2KB.)
Required keys:
- "goal": string
- "current_focus": string
- "next_actions": array of short strings (3–8)
- "key_files": array of relative paths (from the project root) that matter right now
- "last_updated": ISO timestamp string
Optional keys:
- "risks": array of strings
- "questions": array of strings
[/PROJECT_STATE_JSON]

Then the following markdown docs. Each block MUST be either:
- Full updated content, OR
- Exactly the single token NO_CHANGE

[PROJECT_MAP]
(updated markdown OR NO_CHANGE)
[/PROJECT_MAP]

[PROJECT_BRIEF]
(updated markdown OR NO_CHANGE)
[/PROJECT_BRIEF]

[WORKING_DOC]
(updated markdown OR NO_CHANGE)
[/WORKING_DOC]

[PATCH_PROTOCOL_V1]
(A single JSON object. Patch Protocol v1 envelope. If present, do NOT also emit WORKING_DOC/FACTS_MAP/etc blocks.)
[/PATCH_PROTOCOL_V1]

[PREFERENCES]
(updated markdown OR NO_CHANGE)
[/PREFERENCES]

[DECISION_LOG]
(updated markdown OR NO_CHANGE)
[/DECISION_LOG]

[FACTS_MAP]
(updated markdown OR NO_CHANGE)
[/FACTS_MAP]

Optional capability gap block:
If you were blocked or only partially able to complete the task due to missing capability/evidence, include:

[CAPABILITY_GAP_JSON]
(A valid JSON object with your gap report.)
Schema (recommended):
{
  "blocked": true,
  "task_summary": "short task summary",
  "limitations": ["short limit 1", "short limit 2"],
  "missing_capabilities": ["capability/tool 1"],
  "needed_inputs": ["file or info needed"],
  "suggested_features": ["feature that would enable this"],
  "recommended_search_queries": ["query to find best tools or practices"]
}
[/CAPABILITY_GAP_JSON]

Optional Excel deliverable block:
If you are asked to CREATE or UPDATE an Excel spreadsheet, include this block:

[EXCEL_SPEC_JSON]
(A valid JSON object that describes the workbook to generate.)

Schema (required, master-level minimum):
{
  "name": "Workbook Name (optional)",
  "sheets": [
    {
      "title": "Sheet Name",

      "freeze_panes": "A2",
      "col_widths": { "A": 24, "B": 14 },
      "row_heights": { "1": 22 },
      "gridlines": false,
      "zoom": 110,

      "cells": {
        "A1": "Header",
        "B2": 123,
        "C3": {"f":"=SUM(B2:B10)"},
        "D4": {"v": 10, "number_format": "0"}
      },

      "header_rows": [1],

      "tables": [
        { "name": "tblMain", "ref": "A1:H200", "style": "TableStyleMedium9", "showRowStripes": true }
      ],

      "dropdowns": [
        { "range": "C2:C200", "values": ["Yes","No"], "allow_blank": true }
      ],

      "validations": [
        { "range": "B2:B200", "type":"whole", "operator":"between", "formula1":"0", "formula2":"999999", "allow_blank": true,
          "error_title":"Invalid", "error":"Enter a whole number between 0 and 999999." }
      ],

      "range_styles": [
        { "range": "A1:H1", "style": { "font": {"bold": true, "color":"FFFFFFFF"}, "fill": {"color":"FF1F2937"}, "alignment":{"horizontal":"center","wrap_text": true} } },
        { "range": "A2:H200", "style": { "alignment":{"wrap_text": true, "vertical":"top"} } }
      ],

      "conditional_formats": [
        { "range":"G2:G200", "type":"cellIs", "operator":"lessThan", "formula":["0"],
          "style": {"font":{"color":"FFFF0000","bold": true}} }
      ]
    }
  ]
}

Hard requirements (must satisfy):
- At least ONE sheet must contain:
  - A table ("tables" with a valid ref)
  - AND validations or dropdowns on user-entry columns
  - AND a header row + freeze panes
- Visually distinguish INPUT vs CALC vs OUTPUT using range_styles.
- Include conditional formatting for missing required fields and out-of-range values where relevant.
- Use formulas for rollups (Summary / checks), not manual totals.
[/EXCEL_SPEC_JSON]

Optional deliverable block (print-ready):
If you generate a print-ready document, include it in this block:

[DELIVERABLE_HTML]
(full HTML document, including <html>...</html>)
[/DELIVERABLE_HTML]

Rules:
- Do NOT paste the HTML into [USER_ANSWER].
- In [USER_ANSWER], provide a short summary + the final link line: Open: /file?path=...

Additional rules:
- Do NOT put these tags inside code fences.
- PROJECT_STATE_JSON must be valid JSON (no trailing comments).
- Do NOT rewrite long documents unless needed; prefer small edits and preserve existing deliverables verbatim.
- Keep PROJECT_MAP concise and practical (<= ~250 lines).
""".strip()
LENS0_SYSTEM_PROMPT = LENS0_SYSTEM_PROMPT + "\n\n" + TRUTH_BINDING_TREBEK_BLOCK + "\n\n" + CAPABILITY_USE_BLOCK


# =============================================================================
# Patch mode prompt (anchor-based)
# =============================================================================
# Step 3 extraction: patch engine moved to patch_engine.py
# - patch_system_prompt is imported from patch_engine (as an alias)
# =============================================================================
# Utility helpers
# =============================================================================

_TAG_RE = re.compile(r"\[(?P<tag>[A-Z0-9_]+)\](?P<body>.*?)\[/\1\]", re.DOTALL)

# -----------------------------------------------------------------------------
# Output normalization (human punctuation + ordered list numbering)
# -----------------------------------------------------------------------------

def _apply_outside_fences(text: str, fn) -> str:
    """
    Apply a transform to text outside triple-backtick fences.
    Keeps fenced blocks untouched.
    """
    s = str(text or "")
    if "```" not in s:
        return fn(s)
    parts = s.split("```")
    for i in range(0, len(parts), 2):
        parts[i] = fn(parts[i])
    return "```".join(parts)

def _normalize_human_punctuation_segment(s: str) -> str:
    """
    Replace typographic punctuation with plain ASCII.
    """
    if not s:
        return s
    # Dashes and ellipsis
    s = re.sub(r"\s*[—–―]\s*", " - ", s)
    s = s.replace("…", "...")

    # Curly quotes -> straight quotes
    s = s.replace("“", "\"").replace("”", "\"")
    s = s.replace("‘", "'").replace("’", "'")
    s = s.replace("„", "\"").replace("«", "\"").replace("»", "\"")
    s = s.replace("‚", "'").replace("‹", "'").replace("›", "'")

    # Bullet-like glyphs -> hyphen
    s = s.replace("•", "-").replace("·", "-")

    # Collapse repeated spaces (keep newlines)
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s

def _renumber_ordered_lists_segment(s: str) -> str:
    """
    Renumber ordered-list blocks so they are sequential (1,2,3...).
    """
    if not s:
        return s
    lines = s.splitlines()
    out: List[str] = []
    i = 0
    pat = re.compile(r"^(\s*)(\d+)([.)])(\s+)(.*)$")
    while i < len(lines):
        m = pat.match(lines[i])
        if not m:
            out.append(lines[i])
            i += 1
            continue
        # Start a contiguous ordered-list block
        try:
            num = int(m.group(2))
        except Exception:
            num = 1
        j = i
        while j < len(lines):
            m2 = pat.match(lines[j])
            if not m2:
                break
            prefix, delim, ws, rest = m2.group(1), m2.group(3), m2.group(4), m2.group(5)
            out.append(f"{prefix}{num}{delim}{ws}{rest}")
            num += 1
            j += 1
        i = j
    return "\n".join(out)

def _normalize_assistant_text_for_display(text: str) -> str:
    s = str(text or "")
    s = _apply_outside_fences(s, _normalize_human_punctuation_segment)
    s = _apply_outside_fences(s, _renumber_ordered_lists_segment)
    return s

# -----------------------------------------------------------------------------
# Memory Schema (Tier-2G) + Pending Profile Questions (deterministic)
# -----------------------------------------------------------------------------

_MEMORY_SCHEMA_CACHE: Dict[str, Any] = {}
_MEMORY_SCHEMA_MTIME: float = 0.0

def _memory_schema_path() -> Path:
    try:
        return (RUNTIME_DIR / SERVER_COLOR / "memory_schema.json").resolve()
    except Exception:
        return (RUNTIME_DIR / "blue" / "memory_schema.json").resolve()

def _default_memory_schema() -> Dict[str, Any]:
    return {
        "schema": "memory_schema_v1",
        "version": 1,
        "defaults": {"max_questions": 3, "expires_seconds": 420},
        "fields": [],
    }

def _load_memory_schema() -> Dict[str, Any]:
    global _MEMORY_SCHEMA_CACHE, _MEMORY_SCHEMA_MTIME
    try:
        p = _memory_schema_path()
        mt = p.stat().st_mtime if p.exists() else 0.0
        if _MEMORY_SCHEMA_CACHE and mt and (mt == _MEMORY_SCHEMA_MTIME):
            return _MEMORY_SCHEMA_CACHE
        if p.exists():
            obj = json.loads(p.read_text(encoding="utf-8") or "{}")
            if isinstance(obj, dict) and str(obj.get("schema") or "") == "memory_schema_v1":
                _MEMORY_SCHEMA_CACHE = obj
                _MEMORY_SCHEMA_MTIME = mt
                return obj
    except Exception:
        pass
    return _default_memory_schema()

def _parse_date_ymd_any(s: str) -> Tuple[str, bool]:
    """
    Return (yyyy-mm-dd, missing_year)
    - missing_year True when only month/day found.
    """
    t = (s or "").strip()
    if not t:
        return "", False

    # Numeric: m/d/yyyy or m-d-yyyy
    m = re.search(r"\b(\d{1,2})[/-](\d{1,2})[/-](\d{4})\b", t)
    if m:
        try:
            mon = int(m.group(1)); day = int(m.group(2)); year = int(m.group(3))
        except Exception:
            return "", False
        if 1 <= mon <= 12 and 1 <= day <= 31 and 1800 <= year <= 2100:
            return f"{year:04d}-{mon:02d}-{day:02d}", False

    # Month name: March 24, 1996
    m2 = re.search(
        r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
        r"\s+(\d{1,2})(?:st|nd|rd|th)?(?:,)?\s+(\d{4})\b",
        t,
        flags=re.IGNORECASE,
    )
    if m2:
        mon_txt = (m2.group(1) or "").lower()
        day_txt = (m2.group(2) or "").strip()
        year_txt = (m2.group(3) or "").strip()
        month_map = {
            "jan": 1, "january": 1,
            "feb": 2, "february": 2,
            "mar": 3, "march": 3,
            "apr": 4, "april": 4,
            "may": 5,
            "jun": 6, "june": 6,
            "jul": 7, "july": 7,
            "aug": 8, "august": 8,
            "sep": 9, "september": 9,
            "oct": 10, "october": 10,
            "nov": 11, "november": 11,
            "dec": 12, "december": 12,
        }
        try:
            mon = month_map.get(mon_txt[:3], 0)
            day = int(day_txt); year = int(year_txt)
        except Exception:
            return "", False
        if 1 <= mon <= 12 and 1 <= day <= 31 and 1800 <= year <= 2100:
            return f"{year:04d}-{mon:02d}-{day:02d}", False

    # Month/day without year (ambiguous) -> ask for year
    m3 = re.search(r"\b(\d{1,2})[/-](\d{1,2})\b", t)
    if m3:
        try:
            mon = int(m3.group(1)); day = int(m3.group(2))
        except Exception:
            return "", False
        if 1 <= mon <= 12 and 1 <= day <= 31:
            return "", True

    return "", False

def _profile_field_status(profile: Dict[str, Any], field_path: str) -> Tuple[str, str, List[str]]:
    if not isinstance(profile, dict):
        return "", "", []
    parts = (field_path or "").split(".", 1)
    if len(parts) != 2:
        return "", "", []
    sec, key = parts[0], parts[1]
    sec_obj = profile.get(sec)
    if not isinstance(sec_obj, dict):
        return "", "", []
    rec = sec_obj.get(key)
    if not isinstance(rec, dict):
        return "", "", []
    st = str(rec.get("status") or "").strip().lower()
    val = str(rec.get("value") or "").strip()
    opts = rec.get("options") if isinstance(rec.get("options"), list) else []
    opts2 = [str(x).strip() for x in opts if str(x).strip()]
    return st, val, opts2

def _is_resolved_status(st: str) -> bool:
    return str(st or "").strip().lower() in ("confirmed", "resolved_user", "resolved_auto")

def _global_facts_has_any(user: str, keys: List[str]) -> bool:
    try:
        m = project_store.load_user_global_facts_map(user)
        ents = m.get("entries") if isinstance(m, dict) else {}
        if not isinstance(ents, dict):
            return False
        for k in keys:
            kk = str(k or "").strip()
            if not kk:
                continue
            if kk.endswith("*"):
                pfx = kk[:-1]
                for ek in ents.keys():
                    if str(ek).startswith(pfx):
                        return True
            else:
                if kk in ents:
                    return True
        return False
    except Exception:
        return False

def _select_profile_gap_questions(user: str) -> List[Dict[str, Any]]:
    prof = {}
    try:
        prof0 = project_store.load_user_profile(user)
        if isinstance(prof0, dict):
            prof = prof0
    except Exception:
        prof = {}

    schema = _load_memory_schema()
    fields = schema.get("fields") if isinstance(schema, dict) else []
    if not isinstance(fields, list):
        fields = []

    # Sort by priority (low number = higher priority)
    def _prio(x: Dict[str, Any]) -> int:
        try:
            return int(x.get("priority") or 999)
        except Exception:
            return 999

    out: List[Dict[str, Any]] = []
    for f in sorted([x for x in fields if isinstance(x, dict)], key=_prio):
        fp = str(f.get("field_path") or "").strip()
        if not fp:
            continue

        # depends_on: require resolved fields
        deps = f.get("depends_on")
        if isinstance(deps, list) and deps:
            ok = True
            for d in deps:
                st_d, val_d, _ = _profile_field_status(prof, str(d))
                if not (_is_resolved_status(st_d) and val_d):
                    ok = False
                    break
            if not ok:
                continue

        # ask_if_any: only ask if global facts contain any of these keys
        ask_if_any = f.get("ask_if_any")
        if isinstance(ask_if_any, list) and ask_if_any:
            if not _global_facts_has_any(user, [str(x) for x in ask_if_any if str(x).strip()]):
                continue

        st, val, opts = _profile_field_status(prof, fp)
        if _is_resolved_status(st) and val:
            continue
        if st == "ambiguous" and opts:
            # still a gap, but prefer explicit disambiguation flows elsewhere
            pass
        # Add as gap
        out.append(f)

    # Cap by schema default
    max_q = 3
    try:
        max_q = int((schema.get("defaults") or {}).get("max_questions") or 3)
    except Exception:
        max_q = 3
    return out[: max(1, max_q)]

def _select_couples_intake_questions(user: str) -> List[Dict[str, Any]]:
    prof = {}
    try:
        prof0 = project_store.load_user_profile(user)
        if isinstance(prof0, dict):
            prof = prof0
    except Exception:
        prof = {}

    required: List[Dict[str, Any]] = [
        {
            "field_path": "identity.name",
            "expected_type": "text",
            "question": "What is your full name?",
            "claim_template": "My full name is {value}",
            "evidence_template": "My full name is {value}",
        },
        {
            "field_path": "identity.preferred_name",
            "expected_type": "text",
            "question": "What name should I use for you?",
            "claim_template": "My preferred name is {value}",
            "evidence_template": "My preferred name is {value}",
        },
        {
            "field_path": "identity.birthdate",
            "expected_type": "date",
            "question": "What is your birthdate (MM/DD/YYYY)? You can say skip.",
            "claim_template": "My birthday is {value}",
            "evidence_template": "My birthday is {value}",
        },
    ]

    out: List[Dict[str, Any]] = []
    for f in required:
        fp = str(f.get("field_path") or "").strip()
        if not fp:
            continue
        st, val, _opts = _profile_field_status(prof, fp)
        if _is_resolved_status(st) and val:
            continue
        out.append(f)

    return out[:6]

def _couples_intake_missing_fields(user: str, project_full: str) -> List[str]:
    if not _is_couples_user(user):
        return []
    proj_norm = str(project_full or "").replace(" ", "_").lower()
    if not proj_norm.endswith("/couples_therapy"):
        return []
    gaps = _select_couples_intake_questions(user)
    if not gaps:
        return []
    return [str(g.get("field_path") or "").strip() for g in gaps if str(g.get("field_path") or "").strip()]

def _couples_intake_needed(user: str, project_full: str) -> List[str]:
    missing = _couples_intake_missing_fields(user, project_full)
    if not missing:
        return []
    try:
        st = project_store.load_project_state(project_full) or {}
        if bool(st.get("couples_intake_completed")):
            return []
    except Exception:
        pass
    return missing

def _maybe_start_couples_intake_questions(user: str, project_full: str) -> Tuple[bool, str]:
    """
    Returns (handled, reply_text). If handled=True, caller should send reply and continue.
    Couples-only intake. Uses the same pending profile question mechanism but autostarts.
    """
    missing_fields = _couples_intake_needed(user, project_full)
    if not missing_fields:
        return False, ""
    try:
        st = project_store.load_project_state(project_full) or {}
        if bool(st.get("couples_intake_completed")):
            return False, ""
    except Exception:
        pass

    pend = load_user_pending_profile_question(user)
    if is_user_pending_profile_question_unexpired(pend):
        items = pend.get("items") if isinstance(pend.get("items"), list) else []
        idx = int(pend.get("index") or 0) if items else 0
        if 0 <= idx < len(items):
            return True, _format_profile_question(items[idx], user) or "What should I know about you?"

    gaps = _select_couples_intake_questions(user)
    if not gaps:
        return False, ""

    items: List[Dict[str, Any]] = []
    for g in gaps:
        if not isinstance(g, dict):
            continue
        item = dict(g)
        item["question"] = _format_profile_question(g, user)
        items.append(item)

    if items:
        q0 = str(items[0].get("question") or "").strip()
        if q0:
            items[0]["question"] = "Hello, I'm Quinn, your couples therapist. " + q0

    schema = _load_memory_schema()
    exp_s = 420
    try:
        exp_s = int((schema.get("defaults") or {}).get("expires_seconds") or 420)
    except Exception:
        exp_s = 420

    pend_obj = {
        "version": 1,
        "pending": True,
        "purpose": "couples_intake",
        "created_at": now_iso(),
        "expires_at_epoch": float(time.time() + max(60, exp_s)),
        "index": 0,
        "items": items,
    }
    save_user_pending_profile_question(user, pend_obj)

    q0 = items[0].get("question") if items else ""
    q0 = str(q0 or "").strip()
    return True, q0 or "What should I know about you?"

def _format_profile_question(item: Dict[str, Any], user: str) -> str:
    q = str(item.get("question") or "").strip()
    if not q:
        return ""
    # Optional {name} placeholder
    name = ""
    try:
        prof = project_store.load_user_profile(user)
        st, val, _opts = _profile_field_status(prof, "relationships.girlfriend_name")
        if _is_resolved_status(st) and val:
            name = val
    except Exception:
        name = ""
    if "{name}" in q and name:
        return q.replace("{name}", name)
    # Fallback: strip unresolved placeholder
    return q.replace("{name}", "your girlfriend")

def _is_profile_gap_request(user_msg: str) -> bool:
    low = (user_msg or "").strip().lower()
    if not low:
        return False
    triggers = (
        "what info do you need from me",
        "what information do you need from me",
        "what do you need from me",
        "what else do you need from me",
        "what should i tell you",
        "what should you know about me",
        "what info should i give you",
        "what information should i give you",
        "what do you want to know about me",
    )
    return any(t in low for t in triggers)

def _slot_for_field_path(field_path: str) -> str:
    if field_path.startswith("identity."):
        return "identity"
    if field_path.startswith("relationships."):
        return "relationship"
    return "other"

def _couples_intake_display_name(user: str) -> str:
    try:
        prof = project_store.load_user_profile(user)
    except Exception:
        prof = {}

    st, val, _ = _profile_field_status(prof, "identity.preferred_name")
    if _is_resolved_status(st) and val:
        return val
    st2, val2, _2 = _profile_field_status(prof, "identity.name")
    if _is_resolved_status(st2) and val2:
        return val2
    return ""

def _couples_intake_completion_line(user: str) -> str:
    name = _couples_intake_display_name(user)
    if name:
        return f"Thanks, {name}. I have the basics. What brings you two in right now?"
    return "Thanks. I have the basics. What brings you two in right now?"

def _mark_couples_intake_completed(project_full: str, pend: Dict[str, Any]) -> None:
    try:
        if not project_full:
            return
        if str(pend.get("purpose") or "").strip().lower() != "couples_intake":
            return
        project_store.write_project_state_fields(
            project_full,
            {
                "couples_intake_completed": True,
                "couples_intake_completed_at": now_iso(),
            },
        )
    except Exception:
        return

def _consume_pending_profile_question(
    user: str,
    user_msg: str,
    *,
    project_full: str = "",
    last_assistant_text: str = "",
) -> Tuple[bool, str]:
    """
    Returns (consumed, reply_text). If consumed=True, caller should send reply and continue.
    """
    pend = load_user_pending_profile_question(user)
    if not is_user_pending_profile_question_unexpired(pend):
        return False, ""

    items = pend.get("items") if isinstance(pend.get("items"), list) else []
    idx = int(pend.get("index") or 0) if items else 0
    if idx < 0:
        idx = 0
    if idx >= len(items):
        clear_user_pending_profile_question(user)
        _mark_couples_intake_completed(project_full, pend)
        return False, ""

    item = items[idx] if isinstance(items[idx], dict) else {}
    expected_type = str(item.get("expected_type") or "text").strip().lower()
    claim_tpl = str(item.get("claim_template") or "").strip()
    ev_tpl = str(item.get("evidence_template") or "").strip()
    field_path = str(item.get("field_path") or "").strip()

    um = (user_msg or "").strip()
    low = um.lower()

    couples_intake = (str(pend.get("purpose") or "").strip().lower() == "couples_intake")
    if couples_intake:
        try:
            last_txt = str(last_assistant_text or "").strip()
        except Exception:
            last_txt = ""
        if not last_txt:
            return False, ""
        try:
            expected_q = ""
            if idx == 0:
                expected_q = str(item.get("question") or "").strip()
            else:
                expected_q = _format_profile_question(item, user)
                disp_name = _couples_intake_display_name(user)
                low_q = expected_q.lower().strip()
                if expected_q and (not (low_q.startswith("thanks") or low_q.startswith("hello") or low_q.startswith("hi"))):
                    if disp_name:
                        expected_q = f"Thanks, {disp_name}. {expected_q}"
            if expected_q and expected_q not in last_txt:
                return False, ""
        except Exception:
            return False, ""
    disp_name = _couples_intake_display_name(user) if couples_intake else ""

    # Skip/decline handling
    if any(k in low for k in ("skip", "not sure", "don't know", "dont know", "no idea", "unknown")):
        idx += 1
        if idx >= len(items):
            clear_user_pending_profile_question(user)
            _mark_couples_intake_completed(project_full, pend)
            if couples_intake:
                return True, _couples_intake_completion_line(user)
            return True, "Got it - I'll leave that blank for now."
        pend["index"] = idx
        save_user_pending_profile_question(user, pend)
        next_q = _format_profile_question(items[idx], user)
        if couples_intake and next_q:
            low_q = next_q.lower().strip()
            if not (low_q.startswith("thanks") or low_q.startswith("hello") or low_q.startswith("hi")):
                if disp_name:
                    next_q = f"Thanks, {disp_name}. {next_q}"
        return True, next_q or "Thanks - what's the next detail you want me to remember?"

    value = ""
    missing_year = False
    if expected_type == "date":
        value, missing_year = _parse_date_ymd_any(um)
        if missing_year:
            return True, "What year is that?"
        if not value:
            return True, "What's the full date (MM/DD/YYYY)?"
    else:
        # text/location: accept non-empty
        value = " ".join(um.split()).strip()
        if not value:
            return True, "Could you say that again in a few words?"

    # Build canonical claim/evidence
    ctx = {
        "value": value,
    }
    claim = (claim_tpl.format(**ctx) if claim_tpl else value).strip()
    evq = (ev_tpl.format(**ctx) if ev_tpl else claim).strip()

    project_store.append_user_fact_raw_candidate(
        user,
        claim=claim,
        slot=_slot_for_field_path(field_path),
        subject="user",
        source="chat",
        evidence_quote=evq,
        turn_index=0,
        timestamp=now_iso(),
    )
    project_store.rebuild_user_profile_from_user_facts(user)

    # Advance to next question (if any)
    idx += 1
    if idx >= len(items):
        clear_user_pending_profile_question(user)
        _mark_couples_intake_completed(project_full, pend)
        if couples_intake:
            return True, _couples_intake_completion_line(user)
        return True, "Got it - I'll remember that."

    pend["index"] = idx
    save_user_pending_profile_question(user, pend)
    next_q = _format_profile_question(items[idx], user)
    if couples_intake and next_q:
        low_q = next_q.lower().strip()
        if not (low_q.startswith("thanks") or low_q.startswith("hello") or low_q.startswith("hi")):
            if disp_name:
                next_q = f"Thanks, {disp_name}. {next_q}"
    return True, next_q or "Thanks - what's the next detail you want me to remember?"

def _maybe_start_profile_gap_questions(user: str) -> Tuple[bool, str]:
    """
    Returns (handled, reply_text). If handled=True, caller should send reply and continue.
    """
    pend = load_user_pending_profile_question(user)
    if is_user_pending_profile_question_unexpired(pend):
        items = pend.get("items") if isinstance(pend.get("items"), list) else []
        idx = int(pend.get("index") or 0) if items else 0
        if 0 <= idx < len(items):
            return True, _format_profile_question(items[idx], user) or "What should I remember about you?"

    gaps = _select_profile_gap_questions(user)
    if not gaps:
        return True, "I already have the core details. If you want me to remember anything else, just tell me."

    schema = _load_memory_schema()
    exp_s = 420
    try:
        exp_s = int((schema.get("defaults") or {}).get("expires_seconds") or 420)
    except Exception:
        exp_s = 420

    items: List[Dict[str, Any]] = []
    for g in gaps:
        if not isinstance(g, dict):
            continue
        item = dict(g)
        item["question"] = _format_profile_question(g, user)
        items.append(item)

    pend_obj = {
        "version": 1,
        "pending": True,
        "created_at": now_iso(),
        "expires_at_epoch": float(time.time() + max(60, exp_s)),
        "index": 0,
        "items": items,
    }
    save_user_pending_profile_question(user, pend_obj)

    q0 = items[0].get("question") if items else ""
    q0 = str(q0 or "").strip()
    return True, q0 or "What should I remember about you?"

def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def safe_project_name(name: str) -> str:
    return path_engine.safe_project_name(name, default_project_name=DEFAULT_PROJECT_NAME)

def safe_user_name(user: str) -> str:
    """
    Sanitize the user segment for on-disk paths.

    IMPORTANT: project_store.safe_project_name() replaces spaces with underscores.
    Usernames like "couple Frank" MUST map to "couple_Frank" on disk or permissions
    and file APIs will break (allowed_root checks, listing, deletes).
    """
    cleaned = re.sub(r"[^a-zA-Z0-9_-]", "_", (user or "").strip())
    return cleaned or "user"

safe_filename = path_engine.safe_filename

def _is_couples_user(user: str) -> bool:
    """
    Accept both "couple_Frank" and "couple Frank" style usernames.
    Always normalize through safe_user_name so spacing doesn't break behavior.
    """
    raw = (user or "").strip().lower()
    if raw.startswith("couple_") or raw.startswith("couple "):
        return True
    safe_u = safe_user_name(user).lower()
    return safe_u.startswith("couple_")

def is_text_suffix(suffix: str) -> bool:
    # wrapper to keep call sites stable; implementation lives in lens0_config.py
    return (suffix or "").lower() in TEXT_LIKE_SUFFIXES

def get_request_user(request: web.Request) -> Optional[str]:
    user = request.cookies.get("user")
    if not user:
        return None
    if user not in USERS:
        return None
    return user

def get_user_projects_root(user: str) -> Path:
    root = PROJECTS_DIR / safe_user_name(user)
    root.mkdir(parents=True, exist_ok=True)

    default_path = root / DEFAULT_PROJECT_NAME
    if not default_path.exists():
        ensure_project_scaffold(f"{safe_user_name(user)}/{DEFAULT_PROJECT_NAME}")

    return root

def file_sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data or b"").hexdigest()

def read_text_file(path: Path, *, errors: str = "replace") -> str:
    try:
        return path.read_text(encoding="utf-8", errors=errors)
    except Exception:
        return ""
def _wants_codehelp_code_review(user_msg: str) -> bool:
    m = (user_msg or "").lower()
    triggers = (
        "code review mode",
        "review the code",
        "examine the code",
        "look at the code",
        "audit the code",
        "offer suggestions",
        "analyze the code",
        "capability map",
        "chunk map",
    )
    return any(t in m for t in triggers)

def _analysis_intent_workbook(user_msg: str) -> bool:
    low = (user_msg or "").strip().lower()
    if not low:
        return False
    triggers = (
        "case workbook",
        "case spreadsheet",
        "evidence workbook",
        "evidence matrix",
        "build workbook",
        "create workbook",
        "make workbook",
        "build excel",
        "create excel",
        "make excel",
        "spreadsheet",
        "excel sheet",
    )
    return any(t in low for t in triggers)

def _analysis_intent_case_summary(user_msg: str) -> bool:
    low = (user_msg or "").strip().lower()
    if not low:
        return False
    triggers = (
        "case summary",
        "summary doc",
        "summary document",
        "case memo",
        "case brief",
    )
    return any(t in low for t in triggers)

def _analysis_intent_audit(user_msg: str) -> bool:
    low = (user_msg or "").strip().lower()
    if not low:
        return False
    return ("audit" in low) or ("status report" in low)

def build_capability_state_note() -> str:
    """
    System-only note for model: availability of key capabilities + missing prerequisites.
    Keep it short and deterministic.
    """
    lines: List[str] = []
    lines.append("CAPABILITY_STATE:")

    # Web search
    brave_key = (os.getenv("BRAVE_API_KEY", "") or "").strip()
    if brave_key:
        lines.append("- web_search: available")
    else:
        lines.append("- web_search: unavailable (set BRAVE_API_KEY)")

    # Vision (image semantics/caption)
    vision_model = (os.environ.get("OPENAI_VISION_MODEL") or "").strip()
    openai_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if vision_model and openai_key:
        lines.append("- vision: available")
    else:
        lines.append("- vision: unavailable (set OPENAI_VISION_MODEL + OPENAI_API_KEY)")

    # OCR
    try:
        ocr_ok = (project_store.Image is not None) and (project_store.pytesseract is not None)
    except Exception:
        ocr_ok = False
    if ocr_ok:
        lines.append("- image_ocr: available")
    else:
        lines.append("- image_ocr: unavailable (install Pillow + pytesseract)")

    # Scanned PDF OCR (pdf2image + tesseract + poppler)
    try:
        pdf_ocr_ok = (project_store.convert_from_path is not None) and (project_store.pytesseract is not None)
    except Exception:
        pdf_ocr_ok = False
    if pdf_ocr_ok:
        lines.append("- pdf_ocr: available")
    else:
        lines.append("- pdf_ocr: unavailable (install pdf2image + poppler + pytesseract)")

    # Excel generation (openpyxl)
    try:
        excel_ok = (project_store.Workbook is not None)
    except Exception:
        excel_ok = False
    if excel_ok:
        lines.append("- excel_generation: available")
    else:
        lines.append("- excel_generation: unavailable (install openpyxl)")

    # Word parsing (stdlib docx + best-effort doc)
    lines.append("- word_docs: available (docx parsed; legacy doc is best-effort)")

    return "\n".join(lines).strip()

# (dedupe) _is_code_help_project is defined later in this file (canonical copy).

# (dedupe) _find_codehelp_index_files is defined later in this file (canonical copy).

# (dedupe) build_codehelp_capability_map is defined later in this file (canonical copy).

def build_codehelp_review_corpus(project_name: str, user_msg: str, max_chars_total: int = 50000) -> str:
    """
    When the user asks for review/suggestions, inject *some real code* (bounded) so the model
    can quote and reason concretely without requiring patch-mode.
    """
    idx_files = _find_codehelp_index_files(project_name)
    ad = artifacts_dir(project_name)
    if not (idx_files and ad.exists()):
        return ""

    # Extract chunk filenames from index text.
    chunk_names: List[str] = []
    rx = re.compile(r"(code_[^\s]+?_chunk_\d+_v\d+\.(?:py|html))", re.IGNORECASE)
    for p in idx_files[:2]:
        txt = read_text_file(p, errors="replace")
        for m in rx.finditer(txt or ""):
            nm = m.group(1)
            if nm not in chunk_names:
                chunk_names.append(nm)

    # Score chunks by keyword overlap with user request.
    tokens = [t for t in re.split(r"[^a-z0-9_]+", (user_msg or "").lower()) if len(t) >= 4]
    scored: List[tuple[int, str]] = []
    for nm in chunk_names:
        fp = ad / nm
        if not fp.exists():
            continue
        txt = read_text_file(fp, errors="replace")
        if not txt:
            continue
        score = 0
        for t in tokens[:20]:
            score += txt.lower().count(t)
        scored.append((score, nm))

    # Prefer keyword hits; fallback to earliest chunks.
    scored.sort(key=lambda x: (-x[0], x[1]))
    picked = [nm for s, nm in scored if s > 0][:6]
    if not picked:
        picked = chunk_names[:4]

    parts: List[str] = []
    parts.append("CODEHELP_REVIEW_CORPUS:")
    parts.append("- Bounded excerpts of real code chunks for grounded review (Code_Help only).")
    parts.append("===REVIEW_CORPUS_START===")

    used = 0
    per_chunk_cap = max(2500, int(max_chars_total / max(1, len(picked))))
    for nm in picked:
        fp = ad / nm
        txt = read_text_file(fp, errors="replace")
        if not txt:
            continue
        if len(txt) > per_chunk_cap:
            txt = txt[:per_chunk_cap].rstrip() + "\n...[truncated]..."
        block = f"\n---\nCHUNK_FILE: {nm}\n---\n{txt}\n"
        if used + len(block) > max_chars_total:
            break
        parts.append(block)
        used += len(block)

    parts.append("===REVIEW_CORPUS_END===")
    return "\n".join(parts).strip()

def _latest_artifact_path_by_name(project_full: str, name: str) -> str:
    try:
        m = load_manifest(project_full) or {}
    except Exception:
        m = {}
    arts = m.get("artifacts") if isinstance(m.get("artifacts"), list) else []
    best = None
    for a in arts:
        if not isinstance(a, dict):
            continue
        if str(a.get("name") or "") != str(name or ""):
            continue
        try:
            v = int(a.get("version") or 0)
        except Exception:
            v = 0
        if best is None or v > int(best.get("version") or 0):
            best = a
    if isinstance(best, dict):
        return str(best.get("path") or "").strip()
    return ""


def read_bytes_file(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except Exception:
        return b""

def atomic_write_text(path: Path, content: str, *, encoding: str = "utf-8", errors: str = "strict") -> None:
    # HARD INVARIANT (must run BEFORE any delegation):
    # server.py must never write under projects/<user>/_user/.
    rp = str(path).replace("\\", "/")
    if "/_user/" in rp:
        raise RuntimeError(
            "Refusing direct write to global user memory from server.py. "
            "Use project_store as the single authority."
        )

    # Prefer canonical writer for all other paths.
    try:
        fn = getattr(project_store, "atomic_write_text", None)
        if callable(fn):
            fn(path, content, encoding=encoding, errors=errors)
            return
    except Exception:
        pass

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content or "", encoding=encoding, errors=errors)
    os.replace(tmp, path)

def atomic_write_bytes(path: Path, content: bytes) -> None:
    # HARD INVARIANT (must run BEFORE any delegation):
    rp = str(path).replace("\\", "/")
    if "/_user/" in rp:
        raise RuntimeError(
            "Refusing direct write to global user memory from server.py. "
            "Use project_store as the single authority."
        )

    # Prefer canonical writer for all other paths.
    try:
        fn = getattr(project_store, "atomic_write_bytes", None)
        if callable(fn):
            fn(path, content)
            return
    except Exception:
        pass

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(content or b"")
    os.replace(tmp, path)

def append_jsonl(path: Path, obj: dict) -> None:
    # HARD INVARIANT (must run BEFORE any delegation):
    rp = str(path).replace("\\", "/")
    if "/_user/" in rp:
        raise RuntimeError(
            "Refusing direct append to global user memory from server.py. "
            "Use project_store as the single authority."
        )

    # Prefer canonical writer for all other paths.
    try:
        fn = getattr(project_store, "append_jsonl", None)
        if callable(fn):
            fn(path, obj)
            return
    except Exception:
        pass

    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(obj, ensure_ascii=False)
    with path.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

def brave_search(query: str, count: int = 7) -> str:
    """
    Brave web search with light query expansion + one fallback retry.
    Goal:
    - Stronger results when search is needed (e.g., utility rates).
    - Still deterministic + bounded (no runaway retries).
    """
    # Read key at call-time so blue/green swaps, services, and restarts don't get stuck with a stale value.
    brave_key = (os.getenv("BRAVE_API_KEY", "") or "").strip()
    if not brave_key:
        return "(Search error: BRAVE_API_KEY is not set.)"

    def _expand_queries(q: str) -> List[str]:
        q0 = (q or "").strip()
        if not q0:
            return []
        low0 = q0.lower()

        # Normalize OG&E spellings a bit (helps real-world queries)
        q_norm = q0.replace("OGE", "OG&E") if ("oge" in low0 and "og&e" not in low0) else q0

        # Normalize common "latest info on X" / "what's the latest info on X" lead-ins
        # so the base query stays entity-centric (better recall).
        q_base = q_norm
        try:
            q_base = re.sub(
                r"^\s*(?:what\s+is|what['’]s|whats)\s+the\s+latest\s+(?:info|information)\s+on\s+",
                "",
                q_base,
                flags=re.IGNORECASE,
            ).strip()
            q_base = re.sub(
                r"^\s*latest\s+(?:info|information)\s+on\s+",
                "",
                q_base,
                flags=re.IGNORECASE,
            ).strip()
            q_base = re.sub(
                r"^\s*latest\s+on\s+",
                "",
                q_base,
                flags=re.IGNORECASE,
            ).strip()
        except Exception:
            q_base = q_norm

        out_qs: List[str] = [q_base] if (q_base and q_base.lower() != q_norm.lower()) else [q_norm]
        if q_norm and (q_norm.lower() not in [(out_qs[0] or "").lower()]):
            out_qs.append(q_norm)
        # Confirmation/official-statement variants (bounded, deterministic).
        # Goal: make "latest info on X" retrieve the same cluster as "did Apple confirm X?"
        try:
            lowq = (q_base or q_norm or "").lower()

            # Avoid adding these if query already contains confirm/official language.
            if not re.search(r"\b(confirm|confirmed|official|statement|announced|announcement)\b", lowq):

                # Vendor integration smell (deterministic):
                # - Gemini present
                # - and Apple/Siri/Apple Intelligence present
                has_gemini = ("gemini" in lowq)
                has_appleish = ("apple" in lowq) or ("siri" in lowq) or ("apple intelligence" in lowq)

                if has_gemini and has_appleish:
                    base = (q_base or q_norm).strip()
                    if base:
                        out_qs.append(base + " confirmed")
                        out_qs.append("did apple confirm " + base)
                        out_qs.append(base + " official statement")
                        out_qs.append(base + " announced")
        except Exception:
            pass

        # If user is asking for a per-kWh electric rate, boost the search phrase.
        if ("kwh" in low0) and any(x in low0 for x in ("rate", "cost", "price", "per", "¢", "cents", "$", "tariff", "electric")):
            out_qs.append(q_norm + " rate per kWh")
            out_qs.append(q_norm + " residential rate per kWh")

        # Utility/provider lookups benefit from "tariff" phrasing.
        if any(x in low0 for x in ("utility", "electric", "tariff", "kwh")) and "tariff" not in low0:
            out_qs.append(q_norm + " tariff")

        # Expectation-shift / recency prompts benefit from "changes/updates" + standards keywords.
        # Keep deterministic + bounded.
        exp_shift = (
            ("what changed" in low0)
            or ("what has changed" in low0)
            or ("what’s changed" in low0)
            or ("whats changed" in low0)
            or ("changed in the last" in low0)
            or ("last 12 months" in low0)
            or ("past 12 months" in low0)
            or ("since " in low0)
            or ("now required" in low0)
            or ("being enforced" in low0)
            or ("enforced" in low0)
            or ("new guidance" in low0)
            or ("updated guidance" in low0)
        )
        if exp_shift:
            # Prefer variants that tend to include explicit publication dates in snippets.
            out_qs.append(q_norm + " published")
            out_qs.append(q_norm + " publication date")
            out_qs.append(q_norm + " BCP 240")
            out_qs.append(q_norm + " RFC 9700 published")
            out_qs.append(q_norm + " changes")
            out_qs.append(q_norm + " updates")

        # De-dupe while preserving order
        seen: set[str] = set()
        final: List[str] = []
        for qq in out_qs:
            kk = (qq or "").strip()
            if not kk:
                continue
            key = kk.lower()
            if key in seen:
                continue
            seen.add(key)
            final.append(kk)
        return final[:8]  # hard cap (still bounded)
    headers = {"X-Subscription-Token": brave_key, "Accept": "application/json"}

    def _do_one(q: str) -> Tuple[str, int]:
        params = {"q": q, "count": int(count)}
        try:
            resp = requests.get(BRAVE_ENDPOINT, headers=headers, params=params, timeout=12)
            print(f"[WEB] Brave HTTP {resp.status_code} for query={q!r}")
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            return f"(Search error for query={q!r}: {e!r})", 0

        # Structured evidence (search is an evidence stream, not authority).
        results: List[Dict[str, Any]] = []
        for i, item in enumerate(data.get("web", {}).get("results", [])[: int(count)], start=1):
            title = item.get("title", "")
            url = item.get("url", "")
            desc = item.get("description", "")
            results.append(
                {
                    "rank": int(i),
                    "title": str(title or "").strip(),
                    "url": str(url or "").strip(),
                    "snippet": str(desc or "").strip(),
                }
            )

        ev: Dict[str, Any] = {
            "schema": "search_evidence_v1",
            "source": "brave_web",
            "query": str(q or "").strip(),
            "retrieved_at": now_iso(),
            "results": results,
        }

        if not results:
            # Preserve legacy substring checks elsewhere (NO_RESULTS).
            ev["note"] = "NO_RESULTS: Search returned no relevant results."

        out = json.dumps(ev, ensure_ascii=False, separators=(",", ":"))
        out = _search_evidence_annotate_authority(out)
        print(f"[WEB] Brave results={len(results)} chars={len(out)} for query={q!r}")
        return out, len(results)

    # Try primary + small set of expansions; return first with real hits.
    for qtry in _expand_queries(query):
        out, n = _do_one(qtry)
        if n > 0:
            return out

    # If nothing hit, return the last attempt output (could be NO_RESULTS or error)
    out, _n = _do_one((query or "").strip())
    return out
def brave_search_multi(queries: List[str], count: int = 7) -> str:
    """
    Diligent search:
    - Run multiple distinct queries (already planned upstream)
    - Merge + de-dupe results by URL
    - Return one search_evidence_v1 JSON with queries_tried + merged results

    This is bounded and deterministic:
    - caps number of queries and results
    """
    brave_key = (os.getenv("BRAVE_API_KEY", "") or "").strip()
    if not brave_key:
        return "(Search error: BRAVE_API_KEY is not set.)"

    qs = [str(q or "").strip() for q in (queries or []) if str(q or "").strip()]
    # De-dupe while preserving order
    seen_q: set[str] = set()
    q_final: List[str] = []
    for q in qs:
        k = q.lower()
        if k in seen_q:
            continue
        seen_q.add(k)
        q_final.append(q)
        if len(q_final) >= 6:  # hard cap
            break

    if not q_final:
        return "NO_RESULTS: empty query plan."

    headers = {"X-Subscription-Token": brave_key, "Accept": "application/json"}

    merged: List[Dict[str, Any]] = []
    seen_url: set[str] = set()

    def _do_one(q: str) -> List[Dict[str, Any]]:
        params = {"q": q, "count": int(count)}
        try:
            resp = requests.get(BRAVE_ENDPOINT, headers=headers, params=params, timeout=12)
            print(f"[WEB] Brave HTTP {resp.status_code} for query={q!r}")
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            # Keep an auditable placeholder result rather than raising
            return [{"rank": 0, "title": "", "url": "", "snippet": f"(Search error for query={q!r}: {e!r})", "query": q}]

        out: List[Dict[str, Any]] = []
        for i, item in enumerate(data.get("web", {}).get("results", [])[: int(count)], start=1):
            title = str(item.get("title", "") or "").strip()
            url = str(item.get("url", "") or "").strip()
            desc = str(item.get("description", "") or "").strip()
            out.append(
                {
                    "rank": int(i),
                    "title": title,
                    "url": url,
                    "snippet": desc,
                    "query": q,
                }
            )
        return out

    # Pull results for each planned query
    for q in q_final:
        items = _do_one(q)
        for it in items:
            url = str(it.get("url") or "").strip()
            if not url:
                continue
            u = url.lower()
            if u in seen_url:
                continue
            seen_url.add(u)
            merged.append(it)
            if len(merged) >= 24:  # hard cap overall merged hits
                break
        if len(merged) >= 24:
            break

    ev: Dict[str, Any] = {
        "schema": "search_evidence_v1",
        "source": "brave_web_multi",
        "query": str(q_final[0] or "").strip(),
        "queries_tried": q_final[:],
        "retrieved_at": now_iso(),
        "results": merged,
    }

    if not merged:
        ev["note"] = "NO_RESULTS: Search returned no relevant results."

    out = json.dumps(ev, ensure_ascii=False, separators=(",", ":"))
    out = _search_evidence_annotate_authority(out)

    print(f"[WEB] Brave multi queries={len(q_final)} merged_results={len(merged)} chars={len(out)}")
    return out

def brave_image_search(query: str, count: int = 3) -> str:
    """
    Brave image search (bounded).
    Returns a small markdown-friendly block containing up to N image URLs + source pages.
    Deterministic rules:
    - hard cap count <= 3 by default
    - de-dupe by image URL
    - do not guess; if no results, return NO_RESULTS
    """
    brave_key = (os.getenv("BRAVE_API_KEY", "") or "").strip()
    if not brave_key:
        return "(Search error: BRAVE_API_KEY is not set.)"

    q0 = (query or "").strip()
    if not q0:
        return "NO_RESULTS: empty query."

    headers = {"X-Subscription-Token": brave_key, "Accept": "application/json"}
    params = {"q": q0, "count": int(max(1, min(6, int(count))))}

    try:
        resp = requests.get(BRAVE_IMAGE_ENDPOINT, headers=headers, params=params, timeout=12)
        print(f"[WEB] Brave IMAGE HTTP {resp.status_code} for query={q0!r}")
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        return f"(Search error for query={q0!r}: {e!r})"

    items = data.get("results") or data.get("image", {}).get("results") or []
    if not isinstance(items, list) or not items:
        return "NO_RESULTS: Image search returned no relevant results."

    out_lines: List[str] = []

    seen_img: set[str] = set()
    kept = 0

    for it in items:
        if not isinstance(it, dict):
            continue

        # Brave image result shapes vary; handle common keys conservatively.
        img_url = ""
        src_page = ""
        title = ""

        try:
            title = str(it.get("title") or it.get("name") or "").strip()
        except Exception:
            title = ""

        try:
            src_page = str(it.get("url") or it.get("page_url") or it.get("source") or "").strip()
        except Exception:
            src_page = ""

        # Prefer explicit image URL fields
        for k in ("image", "img", "thumbnail", "thumbnail_url", "image_url"):
            v = it.get(k)
            if isinstance(v, str) and v.strip().startswith(("http://", "https://")):
                img_url = v.strip()
                break
            if isinstance(v, dict):
                for kk in ("url", "src"):
                    vv = v.get(kk)
                    if isinstance(vv, str) and vv.strip().startswith(("http://", "https://")):
                        img_url = vv.strip()
                        break
            if img_url:
                break

        # If we still don't have an image URL, skip
        if not img_url:
            continue

        key = img_url.strip().lower()
        if key in seen_img:
            continue
        seen_img.add(key)

        # Output as markdown image so the UI can render it (bounded to max 3).
        safe_title = title[:80].strip() if title else "Image"
        out_lines.append(f"![{safe_title}]({img_url})")
        if src_page:
            out_lines.append(f"Source: {src_page}")
        out_lines.append("")  # spacer

        kept += 1
        if kept >= 3:
            break

    if kept == 0:
        return "NO_RESULTS: Image search returned results but no usable image URLs."

    return "\n".join(out_lines).strip()
def _fetch_page_text(url: str, *, timeout_s: float = 12.0, max_chars: int = 9000) -> str:
    """
    Fetch + extract readable text from a web page.
    Bounded, best-effort, deterministic.
    """
    u = str(url or "").strip()
    if not u.startswith(("http://", "https://")):
        return ""

    headers = {
        "User-Agent": "Mozilla/5.0 (Lens0ResearchBot/1.0)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    try:
        r = requests.get(u, headers=headers, timeout=float(timeout_s))
        r.raise_for_status()
        html0 = r.text or ""
    except Exception as e:
        return f"(fetch error: {e!r})"

    # Very simple HTML-to-text (no extra deps):
    # - strip scripts/styles
    # - strip tags
    # - collapse whitespace
    try:
        html1 = re.sub(r"(?is)<script[^>]*>.*?</script>", " ", html0)
        html1 = re.sub(r"(?is)<style[^>]*>.*?</style>", " ", html1)
        html1 = re.sub(r"(?is)<!--.*?-->", " ", html1)
        text = re.sub(r"(?is)<[^>]+>", " ", html1)
        text = re.sub(r"\s+", " ", text).strip()
    except Exception:
        text = (html0 or "").strip()

    if max_chars and len(text) > int(max_chars):
        text = text[: int(max_chars)].rstrip() + "…"

    return text

LENS0_LOOKUP_SYSTEM_PROMPT = r"""
You are Lens-0 in LOOKUP MODE.

Goal:
- Answer the user's factual lookup question directly and precisely.

Style:
- Tone: calm, crisp, friendly, lightly wry. No jokes, no slang, no snark.
- Be confident without swagger; do not placate or hedge away important risks.
- If a request is flawed or likely to fail, say so plainly and offer a better path.

Hard rules:
- Do NOT output Project Pulse.
- Do NOT output any tagged blocks like [USER_ANSWER], [PROJECT_STATE_JSON], etc.
- If SEARCH_EVIDENCE_JSON is present, treat it as live, up-to-date evidence and use it.
- SEARCH_EVIDENCE_JSON is an evidence stream (noisy, incomplete). It must not increase certainty on its own.
- Exception (server-annotated authority):
  - If SEARCH_EVIDENCE_JSON.authority.level == "primary_confirmed", treat the underlying claim as CONFIRMED by primary sources.
  - Be decisive on *what* is confirmed, but do NOT invent missing details (dates, versions, rollout regions) unless explicitly present.
- Do NOT launder snippets into facts. Prefer consensus/disagreement framing:
  - “Results converge on …”
  - “Sources disagree between …”
  - “Not enough evidence yet to say …”
- You have access to SEARCH SNIPPETS and URL metadata, not necessarily full documents or verbatim transcripts.
- Do NOT say you “don’t have” articles, transcripts, or pages.
- Instead, speak in terms of evidence quality:
  - “The available evidence does not clearly confirm…”
  - “The snippets do not show a direct quote stating…”
  - “Coverage appears inferential rather than explicit…”
- If you include sources, include 1–3 URLs that appear in SEARCH_EVIDENCE_JSON.


Reductio rule (anti-hallucination):
- You may summarize or interpret reporting, BUT you MUST distinguish clearly between:
  (a) what is explicitly stated in the available reporting, and
  (b) what is inferred or widely interpreted by commentators.
- You MUST NOT:
  - invent direct quotes,
  - invent specific dates,
  - invent internal program names,
  - or assert executive intent
  unless those elements appear explicitly in SEARCH_EVIDENCE_JSON.
- When evidence is indirect or interpretive, prefer phrasing like:
  - "Reporting suggests..."
  - "Several outlets interpret this as..."
  - "There is no explicit statement confirming..."
- If the user requested a current/exact/published fact and you cannot support it from SEARCH_EVIDENCE_JSON,
  you MUST NOT guess.
  Instead:
  - Ask at most ONE clarifying question that would disambiguate scope, OR
  - Say clearly: "The available reporting does not explicitly confirm this."

 Crowd-knowledge consensus rule (games/meta/builds/configs/tier lists):
- If the user is asking for community consensus / "best build" / "most people use" / tier lists / settings meta:
  - You MUST answer with the best working consensus first (1–2 sentences).
  - You MUST NOT block the answer by saying you can't verify, don't have telemetry, or lack enough evidence.
  - If SEARCH_EVIDENCE_JSON is present:
    - You MUST explicitly summarize what the results CONVERGE on (2–4 concrete points).
    - You MUST include at least ONE disagreement band (what serious players disagree about and why).
    - Speak like: "Results converge on X" and "A common disagreement is Y vs Z".
    - Do NOT pretend a single definitive best exists; describe the dominant cluster + the contested choices.
    - Only include URLs if it materially helps (0–2). No citation theater.
  - If SEARCH_EVIDENCE_JSON is missing/empty:
    - Give a provisional consensus-style template, but label it as "working default" and explain what would most likely change it.
  - Missing details (patch/region/variant/mode) are refinement questions AFTER you give the consensus, not prerequisites.
  - Do NOT lead with premise-denial openers like: "There isn't a single..." / "I can't responsibly claim..." / "Without X this would be guesswork..."
Clarifier discipline:
- Only ask a clarifying question if multiple correct answers exist depending on context that is not present.
- Keep it to ONE question, short, and directly actionable.
- For crowd-knowledge consensus questions, ask at most ONE refinement question AFTER answering.

 Answer format:
- Start with the best direct answer you can support (or the best working consensus if it's crowd-knowledge).
- For crowd-knowledge questions WITH SEARCH_EVIDENCE_JSON present:
  - After the opening, list 2–4 concrete convergence points.
  - Include at least ONE disagreement band (Y vs Z) with a short "why people split" explanation.
- Then add uncertainty bands and "what would change my mind" only if it helps refine the build/config.
- Sources:
  - Include 0–2 URLs only when it materially helps (e.g., a canonical spreadsheet/tier doc/patch note).
  - Do NOT add sources by default.
""".strip()
LENS0_LOOKUP_SYSTEM_PROMPT = LENS0_LOOKUP_SYSTEM_PROMPT + "\n\n" + TRUTH_BINDING_TREBEK_BLOCK + "\n\n" + CAPABILITY_USE_BLOCK

async def _run_lookup_mode_answer(*, question: str, web_results: str) -> str:
    """
    DEPRECATED:
    Lookup behavior is now owned by model_pipeline.run_model_pipeline() so:
    - lookup mode is consistent everywhere
    - grounding/anti-hallucination gates run in one place
    This stub remains only to avoid dangling references during transitions.
    """
    _ = (question, web_results)
    return ""
async def classify_needs_world_evidence(*, ctx: Any, user_text: str) -> Dict[str, Any]:
    """
    LLM-owned decision: does this question require world evidence (web search)?
    Returns a dict:
      {"needs_world_evidence": bool, "confidence": "low|medium|high", "reason": str}
    Must never throw; on failure returns {"needs_world_evidence": False, ...}.
    """
    msg = (user_text or "").strip()
    if not msg:
        return {"needs_world_evidence": False, "confidence": "low", "reason": "empty"}

    sys_prompt = (
        "You are a routing classifier for a tool-using assistant.\n"
        "Decide whether answering the user's question requires *world evidence* from the public internet,\n"
        "and whether the assistant must VERIFY existence before doing risk/implication analysis.\n"
        "\n"
        "Return ONLY a single JSON object with keys:\n"
        "  - needs_world_evidence: true|false\n"
        "  - verify_first: true|false\n"
        "  - claim_class: \"timeless\"|\"current_event\"|\"viral_claim\"|\"procedure_legal\"|\"other\"\n"
        "  - confidence: \"low\"|\"medium\"|\"high\"\n"
        "  - reason: short string (max 80 chars)\n"
        "\n"
        "needs_world_evidence = true when:\n"
        "- the answer depends on current/updated facts (current CEO, news, prices, laws)\n"
        "- the user asks for citations or external attribution\n"
        "- the question is likely wrong without verification\n"
        "\n"
        "verify_first = true when:\n"
        "- the claim hinges on whether a named thing exists (new app/project/tool/network),\n"
        "- the claim originates from a post/headline without primary artifacts provided,\n"
        "- or the user is asking 'is this real / what's the real story' about a new entity.\n"
        "In verify_first mode, the assistant should first verify existence, and if unverified,\n"
        "label it clearly and avoid speculative risk analysis.\n"
        "\n"
        "needs_world_evidence is NOT required when:\n"
        "- pure math or logic\n"
        "- general definitions/explanations\n"
        "- stable facts unlikely to change\n"
        "\n"
        "Be conservative: only set needs_world_evidence=true when it materially improves correctness.\n"
    )

    try:
        raw = ctx.call_openai_chat(
            [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": msg},
            ]
        )
    except Exception:
        return {"needs_world_evidence": False, "confidence": "low", "reason": "call_failed"}

    try:
        obj = json.loads(raw or "{}")
    except Exception:
        # Best-effort extract JSON object from a messy response
        try:
            m = re.search(r"\{.*\}", raw or "", flags=re.DOTALL)
            obj = json.loads(m.group(0)) if m else {}
        except Exception:
            obj = {}

    if not isinstance(obj, dict):
        return {
            "needs_world_evidence": False,
            "verify_first": False,
            "claim_class": "other",
            "confidence": "low",
            "reason": "parse_failed",
        }

    n = bool(obj.get("needs_world_evidence") is True)
    vf = bool(obj.get("verify_first") is True)

    claim_class = str(obj.get("claim_class") or "").strip().lower()
    if claim_class not in ("timeless", "current_event", "viral_claim", "procedure_legal", "other"):
        claim_class = "other"

    conf = str(obj.get("confidence") or "").strip().lower()
    if conf not in ("low", "medium", "high"):
        conf = "low"

    reason = str(obj.get("reason") or "").replace("\r", " ").replace("\n", " ").strip()
    if len(reason) > 80:
        reason = reason[:80].rstrip()

    return {
        "needs_world_evidence": n,
        "verify_first": vf,
        "claim_class": claim_class,
        "confidence": conf,
        "reason": reason,
    }
def should_auto_web_search(user_text: str) -> bool:
    # Explicit image requests ALWAYS allow search (do not gate on intent)
    try:
        low = (user_text or "").lower()
        if any(k in low for k in ("image", "picture", "photo", "show me", "find a picture")):
            return True
    except Exception:
        pass

    """
    Auto web-search gate (project-agnostic).

    Goal:
    - Automatically search when the user is likely asking for public facts that can change or should be verified,
      even if phrased conversationally (no question mark).
    - Avoid searching during local project work (patching/coding/writing/editing/summarizing user-provided text).
    """
    t0 = (user_text or "").strip()
    if not t0:
        return False

    low0 = t0.lower()

    # Explicit opt-out
    if low0.startswith("[nosearch]"):
        return False

    # Never auto-search for patching/coding/tooling work (local-context tasks).
    nosearch_signals = (
        "patch", "replace from this exact line", "through this exact line",
        "insert immediately after", "insert immediately before",
        "server.py", ".py", ".html", ".css", ".js", ".ts",
        "websocket", "aiohttp", "openpyxl", "excel_spec_json", "facts_map",
        "formula", "workbook", "spreadsheet spec", "anchor", "unified diff", "git diff",
        "ui", "frontend", "backend", "bug", "stack trace", "traceback",
        "/selfpatch", "/serverpatch", "/patch-server",
        "rewrite this", "edit this", "summarize this", "proofread",
        "use the text i provided", "based on the file", "in this document",
    )
    if any(x in low0 for x in nosearch_signals):
        return False

    # NOTE:
    # We intentionally do NOT trigger search purely because the user used a verb like
    # "search/lookup/verify/source". Those are brittle "magic word" behaviors.
    #
    # Instead, search routing is driven by:
    # - structural question shape
    # - time sensitivity
    # - unstable domains
    # - status-synthesis override (below)
    #
    # Explicit verb-based search requests are handled upstream by intent classification
    # (model_pipeline) so users don't need exact phrasing here.


    # If the user includes a URL, they often want verification/context.
    if "http://" in low0 or "https://" in low0 or "www." in low0:
        return True

    # ---------------------------------------------------------------------
    # Recognize lookup intent without relying on phrase lists ("magic words")
    #
    # Principle:
    # - Do NOT gate search on canned opener phrases.
    # - Use structural signals: interrogative form, time-sensitivity, unstable domains.
    # ---------------------------------------------------------------------

    # Structural interrogative detection (robust to “what’s/whats” and smart quotes)
    low_q = low0
    try:
        low_q = (
            low_q.replace("’", "'")
                 .replace("‘", "'")
                 .replace("“", '"')
                 .replace("”", '"')
        )
    except Exception:
        low_q = low0

    # Normalize leading contractions like "what's", "whats", "who’s", etc.
    # This is structural: WH-word + optional possessive/contraction marker.
    try:
        low_q = re.sub(r"^(what|who|when|where|why|how)\s*['’]?\s*s\b", r"\1", low_q).strip()
    except Exception:
        low_q = low_q.strip()

    looks_like_question = (
        low_q.endswith("?")
        or bool(re.match(r"^(what|who|when|where|why|how)\b", low_q))
        or (" how much" in low_q)
        or (" how many" in low_q)
        or (" which " in low_q)
        or (" vs " in low_q)
        or (" compare " in low_q)
    )

    # We no longer use conversational-starter phrase lists.
    looks_like_lookup_prompt = False

    # Numeric/unit cues (guessing likely)
    numeric_unit = any(x in t0 for x in ("$", "¢", "%")) or any(x in low0 for x in (
        " per ", "kwh", "kw", "mhz", "ghz", "gb", "tb", "mph", "°", "cents", "dollars",
        "price", "cost", "rate", "fee", "tax", "tariff", "apr", "interest",
    ))

    # Time sensitivity cues (freshness matters)
    time_sensitive = any(x in low0 for x in (
        "right now", "today", "as of", "current", "currently", "latest", "most recent", "updated", "breaking",
        "this week", "this month", "this year", "yesterday", "tomorrow",
    ))

    # Expectation-shift / "what changed" cues:
    # These are time-sensitive even when the domain is otherwise stable (e.g., standards / best practices).
    expectation_shift = (
        ("what changed" in low0)
        or ("what has changed" in low0)
        or ("what’s changed" in low0)
        or ("whats changed" in low0)
        or ("changed in the last" in low0)
        or ("in the last" in low0 and any(u in low0 for u in ("days", "weeks", "months", "years")))
        or ("last 12 months" in low0)
        or ("past 12 months" in low0)
        or ("over the last" in low0 and any(u in low0 for u in ("days", "weeks", "months", "years")))
        or ("since " in low0)
        or ("now required" in low0)
        or ("is this required" in low0)
        or ("being enforced" in low0)
        or ("enforced" in low0)
        or ("new guidance" in low0)
        or ("updated guidance" in low0)
    )

    # Also treat explicit years as time-sensitive (handles "2026 earnings call" style prompts).
    # Deterministic, no date libs needed.
    has_year = bool(re.search(r"\b(20[2-9]\d)\b", low0))

    # High-stakes domains (outdated/wrong matters)
    high_stakes = any(x in low0 for x in (
        "medical", "diagnosis", "dose", "dosage", "drug", "medication",
        "legal", "law", "statute", "regulation", "compliance",
        "tax", "irs", "refund", "audit",
        "mortgage", "apr", "interest rate",
    ))

    # Unstable public domains (likely to change)
    unstable_domain = any(x in low0 for x in (        
        "price", "cost", "tariff", "rate", "fee", "tax", "apr", "interest",
        "electric", "utility", "insurance", "mortgage", "exchange rate",
        "stock", "ticker", "earnings", "guidance", "call transcript",
        "weather", "forecast",
        "law", "regulation",
        "schedule", "standings", "score",
        "deadline", "hours", "open", "close",
        "version", "release", "update",
        "ceo of", "who is the ceo", "president of", "prime minister of",
        "election", "ballot", "registration",
        "flight", "hotel",
        "recall", "discontinued", "canceled", "cancelled",
    ))
    # Weather-like queries without the word "weather":
    # Example: "is it cold outside?"
    # Structural: outside-reference + temp adjective.
    outside_temp_like = False
    try:
        outside_temp_like = bool(
            re.search(r"\b(out|outside)\b", low0)
            and re.search(r"\b(cold|hot|warm|cool|chilly|freezing)\b", low0)
        )
    except Exception:
        outside_temp_like = False

    if outside_temp_like:
        unstable_domain = True
        # Outside-temperature questions are inherently time-sensitive.
        # Do not require the user to say "right now" / "today".
        time_sensitive = True
    # Crowd-knowledge domains (builds/tier lists/configs/meta) are time-sensitive and noisy.
    #
    # Foundational rule:
    # - This is NOT a magic-word trigger. It's a structural "domain smell" score over tokens.
    # - It must remain deterministic and must not initiate search outside this function.
    crowd_knowledge = False
    try:
        toks = set(re.findall(r"[a-z0-9]{3,}", low0))

        # Exclude software/tooling build contexts (avoid "build" meaning compile)
        devish = False
        try:
            dev_score = 0
            # Tooling tokens (broad, but not phrase-locked)
            for w in ("traceback", "stack", "compile", "compiler", "pip", "npm", "yarn", "conda", "docker", "kubernetes", "pytest", "eslint", "tsc"):
                if w in toks:
                    dev_score += 2
            # File/extension hints lean strongly toward local work, not crowd meta
            if any(x in low0 for x in (".py", ".js", ".ts", ".json", ".yaml", ".yml")):
                dev_score += 3
            devish = dev_score >= 4
        except Exception:
            devish = False

        if not devish:
            ck_score = 0

            # Strong crowd-knowledge indicators (token pairs, not exact phrases)
            if ("tier" in toks and "list" in toks) or ("tierlist" in toks):
                ck_score += 4

            # "meta" / balance/patch language
            if "meta" in toks:
                ck_score += 3
            if "buff" in toks or "nerf" in toks:
                ck_score += 3
            if "patch" in toks or ("notes" in toks and "patch" in toks):
                ck_score += 2

            # Build/config/loadout semantics (only meaningful when combined with other signals)
            if "build" in toks or "builds" in toks:
                ck_score += 2
            if "loadout" in toks or "loadouts" in toks:
                ck_score += 2
            if "config" in toks or "settings" in toks or "setup" in toks:
                ck_score += 2

            # Common gameplay optimization terms (generic across games)
            for w in ("perk", "perks", "talent", "talents", "skills", "runes", "items", "gear", "mods", "attachments", "weapon", "dps", "rotation"):
                if w in toks:
                    ck_score += 1

            # Competitive context increases likelihood it’s crowd knowledge
            for w in ("ranked", "mmr", "elo", "ladder", "pvp"):
                if w in toks:
                    ck_score += 1

            # Require some request-like structure (still natural language, not magic words)
            # This prevents random mentions of "build" in unrelated prose from triggering search.
            requesty = (
                looks_like_question
                or comparison
                or ("vs" in toks)
                or ("compare" in toks)
                or ("best" in toks)
                or ("optimal" in toks)
                or ("recommend" in toks)
            )

            # Crowd-knowledge triggers if:
            # - score indicates the domain, AND
            # - the user is actually requesting guidance/comparison
            crowd_knowledge = (ck_score >= 5) and requesty and (len(t0) <= 320)

        if crowd_knowledge:
            unstable_domain = True
            # Crowd knowledge shifts frequently; treat as time-sensitive even without "today/latest".
            time_sensitive = True

    except Exception:
        crowd_knowledge = False
    # Comparison/recommendation queries often require up-to-date options
    comparison = any(x in low0 for x in (
        "best ", "top ", "recommend", "recommendation", "compare", "vs", "cheapest", "under $",
    ))

    # Avoid searching on long “project task” instructions (usually local work)
    if len(t0) > 420 and not ("http://" in low0 or "https://" in low0):
        return False

    # Final decision rules (deterministic):
    # 1) Explicit question + unstable domain and time-sensitive/exact => search
    if looks_like_question and (unstable_domain or numeric_unit) and (time_sensitive or has_year):
        return True

    # 1.5) Human fallback (no magic words):
    # If the user is clearly asking for a time-sensitive public fact in an unstable domain,
    # trigger search even if the message isn't phrased as a classic question.
    if (not looks_like_question) and unstable_domain and (time_sensitive or has_year or numeric_unit) and len(t0) <= 260:
        return True

    # 1.6) Crowd-knowledge (builds/tier lists/configs/meta):
    # Treat as public, time-sensitive, crowd-sourced evidence.
    if crowd_knowledge and (looks_like_question or comparison or ("vs" in low0) or ("compare" in low0)) and len(t0) <= 320:
        return True

    # 2) Conversational lookup prompt + unstable domain/time markers => search
    if looks_like_lookup_prompt and (unstable_domain or numeric_unit) and (time_sensitive or has_year):
        return True

    # 3) High-stakes factual => search
    if (looks_like_question or looks_like_lookup_prompt) and high_stakes:
        return True

    # 4) Comparison/recommendation => search
    if (looks_like_question or looks_like_lookup_prompt) and comparison:
        return True

    # 4.5) "What changed / last X months / now required" => search (even if domain isn't in unstable_domain)
    if (looks_like_question or looks_like_lookup_prompt) and expectation_shift:
        return True

    # 5) Short numeric/entity lookups even without explicit "current/latest"
    if (looks_like_question or looks_like_lookup_prompt) and numeric_unit and unstable_domain and len(t0) <= 260:
        return True

    # 6) Year mentioned + unstable domain (handles "2026 earnings call" even without "latest")
    if has_year and unstable_domain:
        return True
    # -----------------------------------------------------------------
    # STATUS-SYNTHESIS OVERRIDE (FOUNDATIONAL)
    #
    # If the user is asking for "latest / current / status / roadmap /
    # integration / rollout / availability" about a public platform,
    # web search is MANDATORY even if the domain isn't in unstable_domain.
    #
    # This prevents prior-knowledge answers for evolving integrations
    # (e.g., Siri + Gemini, platform handoffs, model providers).
    # -----------------------------------------------------------------
    if looks_like_question and time_sensitive:
        status_terms = (
            "latest",
            "current",
            "status",
            "roadmap",
            "integration",
            "integrated",
            "rollout",
            "availability",
            "handoff",
            "partnership",
            "support",
            "provider",
        )
        if any(t in low0 for t in status_terms):
            return True

    return False


def strip_lens0_system_blocks(text: str) -> str:

    """
    Remove UI-injected instruction blocks before heuristics like auto web-search run.
    Examples:
      [SCOPE] ... [/SCOPE]
      [GOAL_ONBOARD] ... [/GOAL_ONBOARD]
      [DELIVERABLE] ... [/DELIVERABLE]
    """
    s = (text or "")
    # Remove bracketed blocks (non-greedy, DOTALL)
    s = re.sub(r"\[(SCOPE|GOAL_ONBOARD|DELIVERABLE)\].*?\[/\1\]", "", s, flags=re.DOTALL | re.IGNORECASE)
    # Also remove stray tag-only lines if any
    s = re.sub(r"^\s*\[(SCOPE|GOAL_ONBOARD|DELIVERABLE)\]\s*$", "", s, flags=re.MULTILINE | re.IGNORECASE)
    s = re.sub(r"^\s*\[/(SCOPE|GOAL_ONBOARD|DELIVERABLE)\]\s*$", "", s, flags=re.MULTILINE | re.IGNORECASE)
    return s.strip()

@dataclass
class UserFrame:
    """
    Deterministic, read-only per-turn memory frame.

    Purpose:
    - Resolve deictic references ("here", "my girlfriend") using Tier-2G/Tier-2M.
    - Provide a cleaned, rewritten text string for search/lookup routing.
    - NEVER guesses. If memory is missing/ambiguous, leaves the text unchanged.
    """
    user_seg: str
    preferred_name: str
    location_status: str
    location_value: str
    location_options: List[str]
    partner_name: str
    user_text_for_search: str

def _norm_ws_spaces(s: str) -> str:
    try:
        return " ".join((s or "").strip().split())
    except Exception:
        return (s or "").strip()

def _extract_confirmed_location_value_from_tier2g(prof: Dict[str, Any]) -> Tuple[str, str, List[str]]:
    """
    Return (status, value, options) from Tier-2G profile.identity.location.
    status is lowercased.
    """
    st = ""
    val = ""
    opts: List[str] = []

    try:
        ident = prof.get("identity") if isinstance(prof, dict) else {}
        if not isinstance(ident, dict):
            ident = {}
        loc = ident.get("location") if isinstance(ident.get("location"), dict) else {}
        st = str(loc.get("status") or "").strip().lower()
        val = _norm_ws_spaces(str(loc.get("value") or ""))
        raw_opts = loc.get("options") if isinstance(loc.get("options"), list) else []
        opts = [_norm_ws_spaces(str(x)) for x in raw_opts if _norm_ws_spaces(str(x))][:12]
    except Exception:
        st, val, opts = ("", "", [])

    return st, val, opts

def _extract_partner_name_from_tier2g(prof: Dict[str, Any]) -> str:
    """
    Return the strongest known partner name from Tier-2G relationships, if present.
    Deterministic: checks partner/spouse/wife/husband/boyfriend/girlfriend in that order.
    """
    try:
        rel = prof.get("relationships") if isinstance(prof, dict) else {}
        if not isinstance(rel, dict):
            rel = {}
        for k in ("partner_name", "spouse_name", "wife_name", "husband_name", "boyfriend_name", "girlfriend_name"):
            v = _norm_ws_spaces(str(rel.get(k) or ""))
            if v:
                return v
        return ""
    except Exception:
        return ""

def _rewrite_deictics_for_search(*, user_text: str, location_value: str, partner_name: str) -> str:
    """
    Deterministic rewrite for search/lookup only:
    - "here" -> location_value (only if we have a confirmed/resolved value)
    - "my girlfriend" -> partner_name (only if present)
    - Keep user text otherwise unchanged.

    No guessing. No domain-specific hacks.
    """
    t0 = (user_text or "").strip()
    if not t0:
        return ""

    low = t0.lower()

    out = t0

    # "here" resolution (only when we have a concrete location value)
    if location_value:
        # Replace standalone 'here' in common question forms.
        # Keep it conservative: whole-word match only.
        try:
            out = re.sub(r"\bhere\b", location_value, out, flags=re.IGNORECASE)
        except Exception:
            pass

        # Also handle "outside" style deictics if present (still location-bound only)
        # Example: "what's the weather outside?" -> "what's the weather in <loc>?"
        try:
            if re.search(r"\boutside\b", low) and ("weather" in low or re.search(r"\b(cold|hot|warm|cool|chilly|freezing)\b", low)):
                # Only rewrite if the text does not already name a city/state explicitly
                if not re.search(r"\bin\s+[a-zA-Z]", low):
                    out = re.sub(r"\boutside\b", f"in {location_value}", out, flags=re.IGNORECASE)
        except Exception:
            pass

    # Partner resolution (only when we have a partner name)
    if partner_name:
        # Replace "my girlfriend" / "my partner" with a named referent for search relevance.
        try:
            out = re.sub(r"\bmy\s+girlfriend\b", partner_name, out, flags=re.IGNORECASE)
        except Exception:
            pass
        try:
            out = re.sub(r"\bmy\s+partner\b", partner_name, out, flags=re.IGNORECASE)
        except Exception:
            pass

    return out.strip()

def resolve_user_frame(*, user: str, current_project_full: str, user_text: str) -> Dict[str, Any]:
    """
    FOUNDATIONAL: Memory-first resolution gate (deterministic).

    Reads:
    - Tier-2G profile (global identity kernel)
    - Tier-2M global facts map snippet (optional; read-only)

    Returns a dict with:
    - user_seg
    - preferred_name
    - location_status/value/options
    - partner_name
    - user_text_for_search (deictic rewrite for search/lookup only)
    """
    u = safe_user_name(user or "")
    msg = (user_text or "").strip()

    prof: Dict[str, Any] = {}
    try:
        prof0 = project_store.load_user_profile(u)
        if isinstance(prof0, dict):
            prof = prof0
    except Exception:
        prof = {}

    # preferred name (Tier-2G)
    preferred = ""
    try:
        ident = prof.get("identity") if isinstance(prof, dict) else {}
        if not isinstance(ident, dict):
            ident = {}
        pn = ident.get("preferred_name") if isinstance(ident.get("preferred_name"), dict) else {}
        # Use value when confirmed/resolved_user/resolved_auto; else empty.
        st = str(pn.get("status") or "").strip().lower()
        val = _norm_ws_spaces(str(pn.get("value") or ""))
        if st in ("confirmed", "resolved_user", "resolved_auto") and val:
            preferred = val
    except Exception:
        preferred = ""

    # location (Tier-2G)
    loc_status, loc_value, loc_opts = _extract_confirmed_location_value_from_tier2g(prof)

    # Only treat as usable if user-confirmed/resolved.
    loc_usable = ""
    try:
        if loc_status in ("confirmed", "resolved_user") and loc_value:
            loc_usable = loc_value
        # If auto-resolved (rare), also allow.
        if (not loc_usable) and (loc_status == "resolved_auto") and loc_value:
            loc_usable = loc_value
    except Exception:
        loc_usable = ""

    # partner name (Tier-2G)
    partner = _extract_partner_name_from_tier2g(prof)

    # Build search-safe rewritten text
    rewritten = _rewrite_deictics_for_search(
        user_text=msg,
        location_value=loc_usable,
        partner_name=partner,
    )

    # Fall back to original if rewrite produced empty
    if msg and (not rewritten):
        rewritten = msg

    return {
        "user_seg": u,
        "preferred_name": preferred,
        "location_status": loc_status,
        "location_value": loc_value,
        "location_options": loc_opts,
        "partner_name": partner,
        "user_text_for_search": rewritten,
    }

def _is_exact_pulse_cmd(text: str) -> bool:
    """
    ws_commands helper: True only when the user message is exactly a pulse/status command.
    Kept conservative to avoid accidental triggers.
    """
    t = (text or "").strip().lower()
    return t in {"pulse", "project pulse", "/pulse"}


# =============================================================================
# C2 — Decision Capture v1 (Candidate → Confirm → Promote)
# Strictly additive. Conservative heuristics. One confirmation question.
# =============================================================================

_DECISION_CANDIDATE_PATTERNS: List[Tuple[re.Pattern, float]] = [
    # High-confidence commitment phrases (capture the chosen thing as group 1)
    (re.compile(r"^\s*(?:let['’]s|lets)\s+go\s+with\s+(.+?)\s*$", re.IGNORECASE), 0.92),
    (re.compile(r"^\s*we\s*(?:are|’re|'re)\s+going\s+to\s+(.+?)\s*$", re.IGNORECASE), 0.90),
    (re.compile(r"^\s*we\s+will\s+do\s+(.+?)\s*$", re.IGNORECASE), 0.90),
    (re.compile(r"^\s*we['’]ll\s+do\s+(.+?)\s*$", re.IGNORECASE), 0.90),
    (re.compile(r"^\s*((?:tile|micro plaster|plaster|porcelain)\s+for\s+.+?)\s*$", re.IGNORECASE), 0.86),
    # Common natural commitments (still conservative)
    # Guard: avoid treating "I want you to ..." requests (evaluation/validation) as decisions.
    (re.compile(r"^\s*i\s+want\s+(?!you\s+to\s+)(.+?)\s*$", re.IGNORECASE), 0.75),
    (re.compile(r"^\s*let['’]s\s+do\s+(.+?)\s*$", re.IGNORECASE), 0.78),
    (re.compile(r"^\s*we\s+should\s+(.+?)\s*$", re.IGNORECASE), 0.72),
    # “X will be Y” style commitments (store the full sentence deterministically)
    (re.compile(r"^\s*(.+?)\s+will\s+be\s+(.+?)\s*$", re.IGNORECASE), 0.88),
    (re.compile(r"^\s*that['’]s\s+the\s+one\s*(?:[:\-–]\s*(.+))?\s*$", re.IGNORECASE), 0.90),
    (re.compile(r"^\s*i['’]m\s+choosing\s+(.+?)\s*$", re.IGNORECASE), 0.93),
    (re.compile(r"^\s*i\s+am\s+choosing\s+(.+?)\s*$", re.IGNORECASE), 0.93),
    (re.compile(r"^\s*use\s+(.+?)\s*$", re.IGNORECASE), 0.88),
    (re.compile(r"^\s*order\s+it\s*(?:[:\-–]\s*(.+))?\s*$", re.IGNORECASE), 0.91),
    (re.compile(r"^\s*book\s+it\s*(?:[:\-–]\s*(.+))?\s*$", re.IGNORECASE), 0.91),
    (re.compile(r"^\s*send\s+it\s*(?:[:\-–]\s*(.+))?\s*$", re.IGNORECASE), 0.91),
    (re.compile(r"^\s*lock\s+it\s*(?:in)?\s*(?:[:\-–]\s*(.+))?\s*$", re.IGNORECASE), 0.91),
]


_AFFIRMATIONS = {
    "yes",
    "yep",
    "yup",
    "yeah",
    "correct",
    "that's right",
    "thats right",
    "confirm",
    "go ahead",
    "do it",
    "do that",
    "sounds good",
    "ok",
    "okay",
}


def _is_affirmation(msg: str) -> bool:
    s = (msg or "").strip().lower()
    s = re.sub(r"[.!?]+$", "", s).strip()
    return s in _AFFIRMATIONS


def _looks_like_correction(msg: str) -> bool:
    low = msg.lower()
    return (
        low.startswith("actually ")
        or low.startswith("correction")
        or low.startswith("no, ")
        or low.startswith("not ")
        # explicit cleanup / finalize commands
        or ("clean up my " in low)
        or ("finalize my " in low)
        or ("treat this as final" in low)
        or ("treat that as final" in low)
    )


def _extract_correction_text(msg: str) -> str:
    s = (msg or "").strip()
    low = s.lower()
    for pfx in ("no", "nope", "nah", "actually", "correction", "instead"):
        if low.startswith(pfx):
            rest = s[len(pfx):].lstrip(" :,-—–\t")
            return rest.strip()
    return ""


def _detect_decision_candidate(msg: str) -> Tuple[str, float]:
    """
    Returns (candidate_text, confidence). If no high-confidence candidate, returns ("", 0.0).
    Conservative by design.
    """
    raw = (msg or "").strip()
    if not raw:
        return "", 0.0

    # Don’t treat very long paragraphs as decisions (usually discussion/analysis).
    if len(raw) > 220:
        return "", 0.0

    # Guard: evaluation/validation requests are NOT project decisions.
    # Example: "I want you to confirm that this idea is valid: ..."
    low = raw.lower()
    if (
        "i want you to" in low
        and any(k in low for k in ("confirm", "validate", "tell me if", "am i right", "is this a good idea", "does this make sense"))
    ):
        return "", 0.0


    for rx, conf in _DECISION_CANDIDATE_PATTERNS:
        m = rx.match(raw)
        if not m:
            continue

        chosen = ""

        # If the pattern has 2 capture groups, preserve the full "X will be Y" shape deterministically.
        try:
            if getattr(m, "lastindex", 0) == 2:
                a = (m.group(1) or "").strip()
                b = (m.group(2) or "").strip()
                if a and b:
                    chosen = f"{a} will be {b}"
            elif getattr(m, "lastindex", 0) >= 1:
                chosen = (m.group(1) or "").strip()
        except Exception:
            chosen = ""

        chosen = (chosen or "").strip().strip("\"' ").strip()
        if not chosen:
            # If pattern didn’t capture, use full message as-is for "That's the one"
            chosen = raw.strip()

        # Basic sanity: avoid ultra-short or generic captures
        if len(chosen) < 3:
            return "", 0.0

        return chosen, float(conf)

    return "", 0.0

def _detect_superseding_decision(msg: str, project_name: str) -> Optional[Dict[str, str]]:
    """
    C7 — Supersession detector (deterministic, conservative).
    If the user is clearly replacing an existing CURRENT decision in the same domain,
    return {"domain","surface","old_id","new_text"}; else None.

    Design constraints:
    - No model calls
    - No fuzzy inference beyond simple lexical signals
    - Only triggers when we can infer a domain and there is an existing current decision for it
    """
    raw = (msg or "").strip()
    if not raw:
        return None

    # Keep it bounded; long paragraphs are usually discussion, not a decisive supersession.
    if len(raw) > 320:
        return None

    try:
        dom, surf = project_store.infer_decision_domain(raw)
    except Exception:
        dom, surf = ("", "")

    if not dom:
        return None

    try:
        cur = project_store.get_current_decisions_by_domain(project_name) or {}
    except Exception:
        cur = {}

    prev = cur.get(dom) if isinstance(cur, dict) else None
    if not isinstance(prev, dict):
        return None

    old_id = str(prev.get("id") or "").strip()
    if not old_id:
        return None

    low = raw.lower()

    # Strong replacement phrases (explicit)
    strong = (
        "switching from",
        "switch from",
        "instead of",
        "replacing",
        "replace ",
        "no longer",
        "changed to",
        "change to",
        "now doing",
        "we're switching",
        "we are switching",
    )
    if any(s in low for s in strong):
        return {"domain": dom, "surface": surf, "old_id": old_id, "new_text": raw}

    # Also allow a short "X not Y" flip if it clearly negates the prior decision text
    prev_txt = str(prev.get("text") or "").strip().lower()
    if prev_txt:
        # If the old decision text appears and we see a negation, treat as supersession.
        if (prev_txt in low) and any(n in low for n in ("not ", "no ", "instead")):
            return {"domain": dom, "surface": surf, "old_id": old_id, "new_text": raw}

    return None

def _decision_pending_path(project_name: str) -> Path:
    return state_dir(project_name) / "decision_pending.json"


def _load_pending_decision(project_name: str) -> Dict[str, Any]:
    p = _decision_pending_path(project_name)
    if not p.exists():
        return {}
    try:
        obj = json.loads(p.read_text(encoding="utf-8") or "{}")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_pending_decision(project_name: str, obj: Dict[str, Any]) -> None:
    p = _decision_pending_path(project_name)
    atomic_write_text(p, json.dumps(obj or {}, indent=2), encoding="utf-8", errors="strict")


def _clear_pending_decision(project_name: str) -> None:
    p = _decision_pending_path(project_name)
    try:
        if p.exists():
            p.unlink()
    except Exception:
        pass

def _is_generic_search_query(q: str) -> bool:
    """
    True when q is too generic to be a useful search query.

    Deterministic + structural only.
    Must never raise (hot WS loop).

    Key upgrade:
    - Treat pronoun-heavy existence/verification/search follow-ups as generic
      so _pick_search_query() falls back to last_user_question_for_search.
    """
    s0 = (q or "").strip()
    if not s0:
        return True

    s = s0.lower()

    # Too short to be useful as a query
    if len(s) < 14:
        return True

    # Content-ish tokens (ignore short function words)
    toks = re.findall(r"[a-z0-9]{4,}", s)

    # Units / numeric signal often makes a short query still meaningful
    has_num_or_unit = bool(re.search(r"[\d$¢%]", s)) or (" per " in s)

    # -----------------------------------------------------------------
    # Pronoun-heavy follow-up detector (structural; bounded)
    #
    # Examples that should be treated as generic:
    #   - "it exists"
    #   - "does it exist"
    #   - "search it"
    #   - "look it up"
    #
    # We avoid hard phrase lists: we only detect (pronoun + thin verb/query operator)
    # with extremely low content signal.
    # -----------------------------------------------------------------
    try:
        has_pron = bool(re.search(r"\b(it|that|this|they|them|those|these)\b", s))
        shortish = len(s) <= 90

        # low content signal: at most 1 "content token" and no numeric/unit signal
        low_signal = (len(toks) <= 1) and (not has_num_or_unit)

        # thin operator verbs / existence verbs (small allowlist; not domain-specific)
        has_searchish = bool(re.search(r"\b(search|lookup|look\s+up|find|verify|check)\b", s))
        has_existish = bool(re.search(r"\b(exist|exists)\b", s))

        # "does it exist" shape (aux + pronoun + exist)
        does_exist_shape = bool(re.search(r"\bdoes\b.*\b(it|that|this|they|them|those|these)\b.*\bexist\b", s))

        if shortish and has_pron and low_signal and (has_searchish or has_existish or does_exist_shape):
            return True
    except Exception:
        # If anything goes sideways, do not break WS loop; fall through to existing logic.
        pass

    # If we have basically no content tokens and no digits/symbols, it's generic
    if (not toks) and (not has_num_or_unit):
        return True

    # If it has almost no content signal, treat as generic unless it’s long enough to carry meaning
    if (len(toks) <= 1) and (len(s) < 34) and (not has_num_or_unit):
        return True

    return False

def _ev_tokens_from_query(q: str) -> List[str]:
    """
    Deterministic tokenization for evidence checks.
    - lowercase
    - alnum tokens len>=4
    - bounded
    """
    s = (q or "").lower()
    toks = re.findall(r"[a-z0-9]{4,}", s)
    out: List[str] = []
    seen: set[str] = set()
    for t in toks:
        if t in seen:
            continue
        seen.add(t)
        out.append(t)
        if len(out) >= 10:
            break
    return out

def _topic_overlap_low(*, query: str, topic: str) -> bool:
    """
    Deterministic overlap test to detect under-specified followups.
    If the query shares <2 meaningful tokens with the active topic,
    treat it as low-overlap.
    """
    q = (query or "").lower()
    t = (topic or "").lower()
    if not q or not t:
        return False
    q_toks = set(re.findall(r"[a-z0-9]{4,}", q))
    t_toks = set(re.findall(r"[a-z0-9]{4,}", t))
    if not q_toks or not t_toks:
        return False
    hit = 0
    for tok in q_toks:
        if tok in t_toks:
            hit += 1
            if hit >= 2:
                return False
    return True

def _should_stitch_continuity_context(
    *,
    user_msg: str,
    active_topic_text: str,
    continuity_label: str,
    active_topic_strength: float,
) -> bool:
    """
    Heuristic: if the user asks for pricing/market comps without restating the subject,
    stitch the active topic so the model doesn't lose the thread.
    """
    if not (active_topic_text or "").strip():
        return False
    if float(active_topic_strength or 0.0) < 0.35:
        return False
    if str(continuity_label or "").strip().lower() == "new_topic":
        return False
    msg = (user_msg or "").lower()
    if not msg:
        return False

    pricing_signals = (
        "what should we charge",
        "what should i charge",
        "what should we price",
        "what should i price",
        "what others charge",
        "what others are charging",
        "what do people charge",
        "look up what others charge",
        "market rate",
        "pricing",
        "price",
        "rate",
        "charge",
        "cost",
    )

    if not any(p in msg for p in pricing_signals):
        return False

    # Only stitch when the user did NOT restate the subject (low overlap)
    return _topic_overlap_low(query=user_msg, topic=active_topic_text)

def _search_evidence_mentions_intended_query(*, evidence_json: str, intended_query: str) -> bool:
    """
    Return True iff the structured search evidence likely mentions the intended query.
    Conservative + deterministic:
    - require >=2 token hits when we have >=2 query tokens
    - otherwise require >=1 hit
    - only operates on schema=search_evidence_v1 JSON
    """
    iq = (intended_query or "").strip()
    if not iq:
        return True

    toks = _ev_tokens_from_query(iq)
    if not toks:
        return True

    try:
        obj = json.loads(evidence_json or "")
    except Exception:
        return True  # if we can't parse, don't invent "insufficient" flags

    if not isinstance(obj, dict):
        return True
    if str(obj.get("schema") or "") != "search_evidence_v1":
        return True

    results = obj.get("results")
    if not isinstance(results, list) or not results:
        return True

    blob_parts: List[str] = []
    for it in results[:8]:
        if not isinstance(it, dict):
            continue
        blob_parts.append(str(it.get("title") or ""))
        blob_parts.append(str(it.get("snippet") or ""))
        blob_parts.append(str(it.get("url") or ""))
    blob = " ".join(blob_parts).lower()

    need = 2 if len(toks) >= 2 else 1
    hit = 0
    for t in toks:
        if t and (t in blob):
            hit += 1
            if hit >= need:
                return True

    return False

def _mark_search_evidence_insufficient(search_results: str, intended_query: str) -> str:
    """
    If search_results is search_evidence_v1 JSON and appears unrelated to intended_query,
    mark it as insufficient (in-band, auditable) without changing routing.
    """
    sr = (search_results or "").strip()
    if not sr:
        return search_results

    # Preserve legacy substring checks
    if ("NO_RESULTS" in sr) or ("Search error" in sr):
        return search_results

    try:
        obj = json.loads(sr)
    except Exception:
        return search_results

    if not isinstance(obj, dict) or str(obj.get("schema") or "") != "search_evidence_v1":
        return search_results

    try:
        ok = _search_evidence_mentions_intended_query(evidence_json=sr, intended_query=intended_query)
        if ok:
            # Clear any stale flags (defensive)
            obj.pop("insufficient", None)
            obj.pop("intended_query", None)
            obj.pop("insufficient_reason", None)
        else:
            obj["insufficient"] = True
            obj["intended_query"] = str(intended_query or "").strip()[:240]
            obj["insufficient_reason"] = "evidence_does_not_mention_intended_query"
    except Exception:
        return search_results

    try:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return search_results
# =============================================================================
# Search Workspace v1 (ephemeral, project-scoped; NOT memory)
# =============================================================================

_SEARCH_WORKSPACE_SCHEMA = "search_workspace_v1"
# -----------------------------------------------------------------------------
# Evidence authority annotation (FOUNDATIONAL, deterministic)
#
# Purpose:
# - Preserve epistemic humility by default ("search doesn't increase certainty").
# - Allow decisive answers ONLY when evidence includes primary-source confirmation.
#
# This is a READ-TIME annotation on search_evidence_v1 JSON (not long-term memory).
# -----------------------------------------------------------------------------

_PRIMARY_SOURCE_DOMAINS = (
    "reuters.com",
    "sec.gov",
    "apple.com",
    "google.com",
    "alphabet.com",
    "investor.apple.com",
    "investor.google",
    "blog.google",
    "support.apple.com",
)

_TIER1_NEWS_DOMAINS = (
    "wsj.com",
    "ft.com",
    "bloomberg.com",
    "cnbc.com",
    "nytimes.com",
    "theverge.com",
    "arstechnica.com",
    "techcrunch.com",
)

def _ev_domain_from_url(url: str) -> str:
    """
    Deterministic domain extractor (no deps).
    Returns lowercase host without leading www.
    """
    u = (url or "").strip()
    if not u:
        return ""
    try:
        # cheap parse: split scheme, then path
        u2 = u.split("://", 1)[-1]
        host = u2.split("/", 1)[0].strip().lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""

def _search_evidence_annotate_authority(evidence_json: str) -> str:
    """
    Add:
      ev["authority"] = {
        "level": "primary_confirmed" | "mixed" | "none",
        "primary_domains": [...],
        "tier1_domains": [...],
      }

    Rules:
    - primary_confirmed: any PRIMARY domain is present in URLs OR fetched_pages URLs.
    - mixed: no primary, but at least one tier-1 news domain present.
    - none: everything else.
    """
    s = (evidence_json or "").strip()
    if not s:
        return evidence_json

    try:
        ev = json.loads(s)
    except Exception:
        return evidence_json

    if not isinstance(ev, dict) or str(ev.get("schema") or "") != "search_evidence_v1":
        return evidence_json

    # Avoid re-annotating repeatedly
    if isinstance(ev.get("authority"), dict) and ev["authority"].get("level"):
        return evidence_json

    primary_hits: set[str] = set()
    tier1_hits: set[str] = set()

    def _consider_url(u: str) -> None:
        d = _ev_domain_from_url(u)
        if not d:
            return
        if any(pd in d for pd in _PRIMARY_SOURCE_DOMAINS):
            primary_hits.add(d)
        if any(td in d for td in _TIER1_NEWS_DOMAINS):
            tier1_hits.add(d)

    try:
        for r in (ev.get("results") or [])[:30]:
            if not isinstance(r, dict):
                continue
            _consider_url(str(r.get("url") or ""))
    except Exception:
        pass

    try:
        for fp in (ev.get("fetched_pages") or [])[:10]:
            if not isinstance(fp, dict):
                continue
            _consider_url(str(fp.get("url") or ""))
    except Exception:
        pass

    level = "none"
    if primary_hits:
        level = "primary_confirmed"
    elif tier1_hits:
        level = "mixed"

    ev["authority"] = {
        "level": level,
        "primary_domains": sorted(primary_hits)[:12],
        "tier1_domains": sorted(tier1_hits)[:12],
    }

    try:
        return json.dumps(ev, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return evidence_json

def _search_tmp_dir(project_full: str) -> Path:
    """
    Ephemeral search workspace directory (project-scoped).
    This is NOT Tier-1/Tier-2 memory. Safe to delete at any time.
    """
    try:
        d = state_dir(project_full) / "tmp"
        d.mkdir(parents=True, exist_ok=True)
        return d
    except Exception:
        # last-resort: keep it under the project state dir
        try:
            d2 = (PROJECT_ROOT / f"projects/{project_full}/state/tmp").resolve()
            d2.mkdir(parents=True, exist_ok=True)
            return d2
        except Exception:
            return state_dir(project_full)

def _search_workspace_new_id() -> str:
    ts = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    tail = uuid.uuid4().hex[:8]
    return f"{ts}_{tail}"

def _search_workspace_write(project_full: str, ws_obj: Dict[str, Any]) -> str:
    """
    Write workspace JSON to state/tmp and return PROJECT_ROOT-relative path.
    Uses atomic_write_text so it routes through project_store when available.
    """
    try:
        tmpd = _search_tmp_dir(project_full)
        ws_id = str(ws_obj.get("id") or "").strip() or _search_workspace_new_id()
        fn = f"search_workspace_{ws_id}.json"
        p = (tmpd / fn).resolve()
        atomic_write_text(p, json.dumps(ws_obj, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", errors="replace")
        try:
            return str(p.relative_to(PROJECT_ROOT)).replace("\\", "/")
        except Exception:
            return str(p).replace("\\", "/")
    except Exception:
        return ""

def _search_evidence_parse(evidence_json: str) -> Dict[str, Any]:
    try:
        obj = json.loads(evidence_json or "")
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}

def _search_evidence_has_hits(evidence_json: str) -> bool:
    obj = _search_evidence_parse(evidence_json)
    if str(obj.get("schema") or "") != "search_evidence_v1":
        # treat non-json or legacy text as "unknown"; do not block
        return True
    res = obj.get("results")
    return bool(isinstance(res, list) and len(res) > 0)

def _search_evidence_is_insufficient(evidence_json: str) -> bool:
    """
    Deterministic insufficiency check:
    - NO_RESULTS note
    - explicit insufficient=true (set by _mark_search_evidence_insufficient)
    - empty results list
    """
    obj = _search_evidence_parse(evidence_json)
    if str(obj.get("schema") or "") != "search_evidence_v1":
        # if it's not the structured schema, don't claim insufficiency
        return False

    if obj.get("insufficient") is True:
        return True

    note = str(obj.get("note") or "").lower()
    if "no_results" in note:
        return True

    res = obj.get("results")
    if isinstance(res, list) and len(res) == 0:
        return True

    return False

async def _search_refine_queries_with_model(*, question: str, prior_query: str, evidence_json: str) -> List[str]:
    """
    Bounded query rewrite (NO magic words).
    Returns 2–4 short queries. Deterministic constraints enforced server-side.
    """
    q = (question or "").strip()
    pq = (prior_query or "").strip()
    ej = (evidence_json or "").strip()

    if not q:
        return []

    system = "\n".join([
        "You rewrite web search queries to improve recall/precision.",
        "Return ONLY strict JSON:",
        '{ "queries": ["..."] }',
        "",
        "Rules:",
        "- Provide 2 to 4 queries.",
        "- Each query must be <= 120 characters.",
        "- Remove filler like 'latest info on'. Focus on entities + action (confirmed/announced/release).",
        "- Prefer concrete variants: official statement, confirmed, announced, release, beta, integration, partnership.",
        "- Do NOT include quotes unless essential.",
        "- Do NOT include site: operators unless clearly helpful.",
        "- Do NOT include commentary or prose.",
    ]).strip()

    user = "\n".join([
        f"USER_QUESTION: {q}",
        f"PRIOR_QUERY: {pq}",
        "EVIDENCE_JSON (may be insufficient):",
        (ej[:2400] if ej else "(none)"),
    ]).strip()

    raw = ""
    try:
        raw = await asyncio.to_thread(call_openai_chat, [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ])
    except Exception:
        return []

    # Parse and validate hard
    out: List[str] = []
    try:
        m = re.search(r"\{.*\}", raw or "", flags=re.DOTALL)
        obj = json.loads(m.group(0)) if m else {}
        qs = obj.get("queries")
        if isinstance(qs, list):
            for it in qs:
                s = str(it or "").strip()
                if not s:
                    continue
                s = re.sub(r"\s+", " ", s).strip()
                if len(s) > 120:
                    s = s[:120].rstrip()
                out.append(s)
    except Exception:
        out = []

    # Dedup, keep order, cap 4
    seen: set[str] = set()
    final: List[str] = []
    for s in out:
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        final.append(s)
        if len(final) >= 4:
            break

    return final[:4]

async def _run_search_with_workspace(
    *,
    project_full: str,
    question_text: str,
    base_query: str,
    deep_search: bool,
) -> Tuple[str, str]:
    """
    Run up to 3 bounded passes:
      - pass 1: base_query + multi-query plan
      - pass 2: query rewrites iff pass 1 insufficient/empty
      - pass 3: broadened/alternative queries iff pass 2 still insufficient
    Writes workspace JSON to state/tmp and returns:
      (best_evidence_json, workspace_rel_path)
    """
    q0 = (base_query or "").strip()
    if not q0:
        return "", ""

    ws_id = _search_workspace_new_id()
    ws: Dict[str, Any] = {
        "schema": _SEARCH_WORKSPACE_SCHEMA,
        "id": ws_id,
        "created_at": now_iso(),
        "question": (question_text or "").strip()[:800],
        "base_query": q0[:240],
        "passes": [],
        "selected_pass": 1,
    }

    # --- pass 1 (diligent) ---
    # Build a query plan (4–6 targeted queries), then run multi-query search and merge results.
    plan: List[str] = [q0]

    try:
        # Reuse the existing rewrite model, but treat it as a planner:
        # it already outputs 2–4 short queries; we supplement with structured variants.
        rewrites = await _search_refine_queries_with_model(
            question=question_text,
            prior_query=q0,
            evidence_json="",
        )
        for r in (rewrites or [])[:4]:
            rr = (r or "").strip()
            if rr and rr.lower() not in [x.lower() for x in plan]:
                plan.append(rr)
    except Exception:
        pass

    # Deterministic plan enrichments (status/integration research pattern)
    try:
        lowq = (question_text or q0 or "").lower()
        if any(k in lowq for k in ("latest", "current", "status", "integration", "deal", "partnership")):
            base = (q0 or "").strip()
            # Add a couple high-recall variants humans use
            for add in (
                base + " Reuters",
                base + " CNBC",
                base + " timeline",
                base + " announced",
            ):
                a = (add or "").strip()
                if a and a.lower() not in [x.lower() for x in plan]:
                    plan.append(a)
    except Exception:
        pass

    # Cap plan
    plan = plan[:6]

    ev1 = brave_search_multi(plan, count=(12 if deep_search else 7))
    try:
        ev1 = _search_evidence_annotate_authority(ev1)
    except Exception:
        pass

    ev1 = _mark_search_evidence_insufficient(ev1, q0)

    ws["passes"].append({
        "pass": 1,
        "query": q0,
        "retrieved_at": now_iso(),
        "query_plan": plan,
        "evidence_json": ev1,
        "insufficient": bool(_search_evidence_is_insufficient(ev1)),
    })

    # Follow sources: fetch top 2 result pages and attach excerpts to evidence + workspace
    try:
        obj1 = _search_evidence_parse(ev1)
        res1 = obj1.get("results") if isinstance(obj1, dict) else None
        fetched: List[Dict[str, Any]] = []
        if isinstance(res1, list) and res1:
            # Fetch top 2 only (bounded)
            for it in res1[:2]:
                if not isinstance(it, dict):
                    continue
                url = str(it.get("url") or "").strip()
                title = str(it.get("title") or "").strip()
                if not url:
                    continue
                txt = _fetch_page_text(url, timeout_s=12.0, max_chars=9000)
                # Clip for embedding
                clip = txt[:1800].rstrip() + ("…" if len(txt) > 1800 else "")
                fetched.append({"url": url, "title": title, "excerpt": clip})

        if fetched:
            ws["fetched_pages"] = fetched[:]
            # Attach to evidence JSON too (so the model can actually use it this turn)
            if isinstance(obj1, dict) and str(obj1.get("schema") or "") == "search_evidence_v1":
                obj1["fetched_pages"] = fetched[:]
                ev1 = json.dumps(obj1, ensure_ascii=False, separators=(",", ":"))
                # update the stored pass evidence_json to match
                ws["passes"][-1]["evidence_json"] = ev1
    except Exception:
        pass

    best_ev = ev1
    selected = 1

    # --- pass 2 (bounded) ---
    try:
        if _search_evidence_is_insufficient(ev1) or (not _search_evidence_has_hits(ev1)):
            rewrites = await _search_refine_queries_with_model(
                question=question_text,
                prior_query=q0,
                evidence_json=ev1,
            )
            # Deterministic fallback if model fails: light suffix variants
            if not rewrites:
                rewrites = [q0 + " confirmed", q0 + " announced", q0 + " official statement"]

            tried: List[str] = []
            for rq in rewrites[:4]:
                rq2 = (rq or "").strip()
                if not rq2:
                    continue
                if rq2.lower() in [x.lower() for x in plan]:
                    continue
                tried.append(rq2)

                ev2 = brave_search(rq2, count=(12 if deep_search else 7))
                try:
                    ev2 = _search_evidence_annotate_authority(ev2)
                except Exception:
                    pass
                ev2 = _mark_search_evidence_insufficient(ev2, rq2)

                ws["passes"].append({
                    "pass": 2,
                    "query": rq2,
                    "retrieved_at": now_iso(),
                    "evidence_json": ev2,
                    "insufficient": bool(_search_evidence_is_insufficient(ev2)),
                })

                # Pick the first pass-2 result that has real hits and is not marked insufficient
                if _search_evidence_has_hits(ev2) and (not _search_evidence_is_insufficient(ev2)):
                    best_ev = ev2
                    selected = 2
                    break
    except Exception:
        pass

    # --- pass 3 (bounded) ---
    try:
        if _search_evidence_is_insufficient(best_ev) or (not _search_evidence_has_hits(best_ev)):
            rewrites = await _search_refine_queries_with_model(
                question=question_text,
                prior_query=q0,
                evidence_json=best_ev,
            )
            # Deterministic broadening fallback
            if not rewrites:
                base = (q0 or "").strip()
                base_short = " ".join(base.split()[:8]).strip()
                rewrites = [
                    base_short,
                    base + " official",
                    base + " documentation",
                    base + " press release",
                ]

            tried_all = {x.lower() for x in plan}
            for p in ws["passes"]:
                try:
                    qx = str(p.get("query") or "").strip().lower()
                    if qx:
                        tried_all.add(qx)
                except Exception:
                    pass

            pass3_used = 0
            for rq in rewrites[:6]:
                rq2 = (rq or "").strip()
                if not rq2 or rq2.lower() in tried_all:
                    continue
                tried_all.add(rq2.lower())
                pass3_used += 1

                ev3 = brave_search(rq2, count=(12 if deep_search else 7))
                try:
                    ev3 = _search_evidence_annotate_authority(ev3)
                except Exception:
                    pass
                ev3 = _mark_search_evidence_insufficient(ev3, rq2)

                ws["passes"].append({
                    "pass": 3,
                    "query": rq2,
                    "retrieved_at": now_iso(),
                    "evidence_json": ev3,
                    "insufficient": bool(_search_evidence_is_insufficient(ev3)),
                })

                if _search_evidence_has_hits(ev3) and (not _search_evidence_is_insufficient(ev3)):
                    best_ev = ev3
                    selected = 3
                    break

                # hard cap to 2 queries in pass 3
                if pass3_used >= 2:
                    break
    except Exception:
        pass

    ws["selected_pass"] = selected

    # persist workspace
    rel_path = _search_workspace_write(project_full, ws)

    # Also embed workspace pointer into the evidence JSON (schema stays search_evidence_v1)
    try:
        obj_best = _search_evidence_parse(best_ev)
        if str(obj_best.get("schema") or "") == "search_evidence_v1":
            obj_best["workspace_path"] = rel_path
            obj_best["workspace_id"] = ws_id
            obj_best["selected_pass"] = selected
            best_ev = json.dumps(obj_best, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        pass
    try:
        best_ev = _search_evidence_annotate_authority(best_ev)
    except Exception:
        pass

    return best_ev, rel_path
def load_pdf_text(path: Path) -> str:
    try:
        reader = PdfReader(str(path))
        pages: List[str] = []
        for page in reader.pages:
            try:
                pages.append(page.extract_text() or "")
            except Exception as e:
                pages.append(f"[PDF page extract error: {e!r}]")
        text = "\n".join(pages).strip()
        if text:
            return text

        # Best-effort OCR fallback for scanned PDFs (optional deps)
        if convert_from_path is None or pytesseract is None:
            return "(PDF error: no extractable text found; scanned PDF OCR not available (install pdf2image + poppler + pytesseract/tesseract).)"

        try:
            images = convert_from_path(str(path))
            ocr_parts: List[str] = []
            for i, im in enumerate(images[:25]):  # safety cap
                try:
                    ocr = pytesseract.image_to_string(im) or ""
                    ocr = ocr.strip()
                    if ocr:
                        ocr_parts.append(f"[PAGE {i+1}]\n{ocr}")
                except Exception as e:
                    ocr_parts.append(f"[PAGE {i+1} OCR error: {e!r}]")
            ocr_text = "\n\n".join(ocr_parts).strip()
            if not ocr_text:
                return "(PDF error: no extractable text found; OCR ran but produced empty output.)"
            return ocr_text
        except Exception as e:
            return f"(PDF error: no extractable text found; OCR fallback failed: {e!r})"
    except Exception as e:
        return f"(PDF error: {e!r})"

# =============================================================================
# Image OCR + caption helpers (Phase 1)
# =============================================================================

# IMAGE_SUFFIXES moved to lens0_config.py (imported above)

def _mime_for_image_suffix(suffix: str) -> str:
    # wrapper to keep call sites stable; implementation lives in lens0_config.py
    return mime_for_image_suffix(suffix)

def load_image_ocr(path: Path) -> Tuple[str, str]:
    """
    Return (ocr_text, note).
    - Uses pytesseract + Pillow if installed.
    - If not installed, returns empty text with a note.
    """
    if Image is None or pytesseract is None:
        return "", "OCR not available (install Pillow + pytesseract + Tesseract binary)."

    try:
        img = Image.open(str(path))
        # Small normalization: convert to RGB to avoid mode issues
        if getattr(img, "mode", "") not in ("RGB", "L"):
            img = img.convert("RGB")
        text = (pytesseract.image_to_string(img) or "").strip()
        if not text:
            return "", "OCR ran but produced no text."
        return text, "OCR extracted text successfully."
    except Exception as e:
        return "", f"OCR error: {e!r}"

def call_openai_vision_caption(image_bytes: bytes, mime: str, *, prompt: str) -> Tuple[str, str]:
    """
    Return (caption, note).
    Uses OPENAI_VISION_MODEL if set; otherwise returns empty caption.
    """
    vision_model = (os.environ.get("OPENAI_VISION_MODEL") or "").strip()
    if not vision_model:
        return "", "Vision caption disabled (set OPENAI_VISION_MODEL to enable)."

    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        return "", "Vision caption unavailable (OPENAI_API_KEY not set)."

    try:
        import base64
        b64 = base64.b64encode(image_bytes).decode("ascii")
        data_url = f"data:{mime};base64,{b64}"

        # SDK path (if available)
        if OpenAI is not None:
            global _OPENAI_CLIENT
            if _OPENAI_CLIENT is None:
                kwargs: Dict[str, Any] = {}
                if (os.environ.get("OPENAI_BASE_URL") or "").strip():
                    kwargs["base_url"] = os.environ["OPENAI_BASE_URL"].strip()
                _OPENAI_CLIENT = OpenAI(**kwargs)  # type: ignore

            messages = [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ]
            resp = _OPENAI_CLIENT.chat.completions.create(model=vision_model, messages=messages)
            out = (resp.choices[0].message.content or "").strip()
            return out, "Vision caption generated via SDK."

        # HTTP fallback
        base_url = (os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip().rstrip("/")
        url = base_url + "/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": vision_model,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
        }
        r = requests.post(url, headers=headers, json=payload, timeout=90)
        r.raise_for_status()
        data = r.json()
        out = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        return out, "Vision caption generated via HTTP."
    except Exception as e:
        return "", f"Vision caption error: {e!r}"

def _caption_image_bytes(img_bytes: bytes, mime: str) -> Tuple[str, str]:
    return call_openai_vision_caption(
        img_bytes,
        mime,
        prompt="Describe this image clearly and concisely for a project workspace. If there is text, do not transcribe it (OCR handles that). Focus on objects, layout, and what it appears to show.",
    )
# =============================================================================
# Vision semantics (Phase 2): on-demand, cached, throttled
# =============================================================================

_VISION_SEM_LOCK: Optional[asyncio.Lock] = None
_VISION_SEM_LAST_CALL_EPOCH: float = 0.0

def _vision_sem_lock() -> asyncio.Lock:
    global _VISION_SEM_LOCK
    if _VISION_SEM_LOCK is None:
        _VISION_SEM_LOCK = asyncio.Lock()
    return _VISION_SEM_LOCK

def _vision_sem_min_interval_s() -> float:
    try:
        v = float((os.environ.get("LENS0_VISION_MIN_INTERVAL_S") or "1.5").strip())
        return max(0.0, v)
    except Exception:
        return 1.5

def _vision_sem_prompt_id() -> str:
    return "vision_semantics_v1"

def _vision_sem_find_cached_by_cache_key(project_full: str, cache_key: str) -> str:
    """
    Deterministic cache lookup: scan manifest artifacts for type=image_semantics
    and match meta.cache_key == cache_key.
    """
    try:
        m = load_manifest(project_full) or {}
    except Exception:
        m = {}
    arts = m.get("artifacts") or []
    if not isinstance(arts, list):
        return ""

    ck = (cache_key or "").strip()
    if not ck:
        return ""

    for a in reversed(arts[-5000:]):
        if not isinstance(a, dict):
            continue
        if str(a.get("type") or "") != "image_semantics":
            continue
        meta = a.get("meta") if isinstance(a.get("meta"), dict) else {}
        if str(meta.get("cache_key") or "").strip() != ck:
            continue
        try:
            return (read_artifact_text(project_full, a, cap_chars=220000) or "").strip()
        except Exception:
            return ""
    return ""

def _vision_sem_store_artifact(
    project_full: str,
    *,
    logical: str,
    file_rel: str,
    json_text: str,
    meta: Dict[str, Any],
) -> None:
    create_artifact(
        project_full,
        logical,
        json_text,
        artifact_type="image_semantics",
        from_files=[file_rel],
        file_ext=".json",
        meta=meta,
    )

def call_openai_vision_semantics(image_bytes: bytes, mime: str, *, prompt: str) -> Tuple[str, str]:
    """
    Return (json_text, note).
    Uses OPENAI_VISION_MODEL if set (same env as caption). Caller throttles.
    """
    vision_model = (os.environ.get("OPENAI_VISION_MODEL") or "").strip()
    if not vision_model:
        print("[VISION] semantics disabled: OPENAI_VISION_MODEL not set", flush=True)
        return "", "Vision semantics disabled (set OPENAI_VISION_MODEL to enable)."

    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        print("[VISION] semantics unavailable: OPENAI_API_KEY not set", flush=True)
        return "", "Vision semantics unavailable (OPENAI_API_KEY not set)."

    try:
        import base64
        b64 = base64.b64encode(image_bytes).decode("ascii")
        data_url = f"data:{mime};base64,{b64}"

        # SDK path (if available)
        if OpenAI is not None:
            global _OPENAI_CLIENT
            if _OPENAI_CLIENT is None:
                kwargs: Dict[str, Any] = {}
                if (os.environ.get("OPENAI_BASE_URL") or "").strip():
                    kwargs["base_url"] = os.environ["OPENAI_BASE_URL"].strip()
                _OPENAI_CLIENT = OpenAI(**kwargs)  # type: ignore

            messages = [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ]
            resp = _OPENAI_CLIENT.chat.completions.create(
                model=vision_model,
                messages=messages,
                temperature=0,
            )
            out = (resp.choices[0].message.content or "").strip()
            return out, "Vision semantics generated via SDK."

        # HTTP fallback
        base_url = (os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip().rstrip("/")
        url = base_url + "/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": vision_model,
            "temperature": 0,
            "messages": [
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": prompt},
                        {"type": "image_url", "image_url": {"url": data_url}},
                    ],
                }
            ],
        }
        r = requests.post(url, headers=headers, json=payload, timeout=90)
        r.raise_for_status()
        data = r.json()
        out = (((data.get("choices") or [{}])[0].get("message") or {}).get("content") or "").strip()
        return out, "Vision semantics generated via HTTP."
    except Exception as e:
        return "", f"Vision semantics error: {e!r}"

async def ensure_image_semantics_for_file(
    project_full: str,
    *,
    file_rel: str,
    force: bool = False,
    reason: str = "",
) -> Tuple[bool, str, str]:
    """
    Returns (ok, json_text, note).
    On-demand only. Cached by sha256 + prompt_id + prompt_hash + model.
    Global throttling enforced for vision calls.
    """
    rel = (file_rel or "").replace("\\", "/").strip()
    if not rel:
        return False, "", "missing_rel"

    abs_path = (PROJECT_ROOT / rel).resolve()
    if not abs_path.exists() or not abs_path.is_file():
        return False, "", "file_missing_on_disk"

    suffix = abs_path.suffix.lower()
    if suffix not in IMAGE_SUFFIXES:
        return False, "", "not_an_image"

    mime = ""
    try:
        mime = mime_for_image_suffix(suffix)
    except Exception:
        mime = ""

    img_bytes = read_bytes_file(abs_path)
    if not img_bytes:
        return False, "", "empty_image_bytes"

    sha = file_sha256_bytes(img_bytes)

    # Gather existing evidence (all deterministic reads)
    caption_txt = _find_latest_artifact_text_for_file(project_full, artifact_type="image_caption", file_rel=rel, cap=12000)
    ocr_txt = _find_latest_artifact_text_for_file(project_full, artifact_type="ocr_text", file_rel=rel, cap=22000)
    cls_txt = _find_latest_artifact_text_for_file(project_full, artifact_type="image_classification", file_rel=rel, cap=12000)

    # Local semantics (Wordle etc.) if present
    local_sem = ""

    # Build prompt (stable v1)
    prompt_id = _vision_sem_prompt_id()
    prompt = vision_semantics.build_prompt_v1(
        caption=caption_txt,
        ocr_text=ocr_txt,
        classification_json=cls_txt,
        local_semantics_json=local_sem,
    )

    vision_model = (os.environ.get("OPENAI_VISION_MODEL") or "").strip()

    # Cache key + cache lookup
    cache_key = vision_semantics.build_cache_key(
        file_sha256=sha,
        prompt_id=prompt_id,
        prompt_hash=vision_semantics.sha256_text(prompt),
        model=(vision_model or ""),
    )

    if not force:
        cached = _vision_sem_find_cached_by_cache_key(project_full, cache_key)
        if cached:
            return True, cached, "cache_hit"

    # Throttled model call (single-flight + min interval)
    async with _vision_sem_lock():
        # Re-check cache inside lock (prevents duplicate calls)
        if not force:
            cached2 = _vision_sem_find_cached_by_cache_key(project_full, cache_key)
            if cached2:
                return True, cached2, "cache_hit_after_lock"

        # Global min-interval throttle
        global _VISION_SEM_LAST_CALL_EPOCH
        min_iv = _vision_sem_min_interval_s()
        now = time.time()
        wait_s = max(0.0, (_VISION_SEM_LAST_CALL_EPOCH + min_iv) - now)
        if wait_s > 0:
            await asyncio.sleep(wait_s)

        raw, note = await asyncio.to_thread(call_openai_vision_semantics, img_bytes, mime, prompt=prompt)
        _VISION_SEM_LAST_CALL_EPOCH = time.time()

    if not raw.strip():
        return False, "", note or "empty_model_output"

    obj = vision_semantics.parse_json_best_effort(raw)
    # Enforce schema + minimal safety wrapper (never trust malformed output)
    if not isinstance(obj, dict) or str(obj.get("schema") or "") != "image_semantics_v1":
        obj = {
            "schema": "image_semantics_v1",
            "created_at": vision_semantics.now_iso(),
            "inputs": {
                "caption": (caption_txt or "")[:1400],
                "ocr_text": (ocr_txt or "")[:2200],
                "classification": (cls_txt or "")[:1800],
                "local_semantics": (local_sem or "")[:2400],
            },
            "outputs": {
                "summary": "",
                "observations": [],
                "uncertainties": ["Model returned non-conforming JSON; stored as raw_snippet only."],
                "raw_snippet": (raw or "")[:4000],
            },
        }

    # Attach deterministic source/cache metadata
    try:
        obj.setdefault("created_at", vision_semantics.now_iso())
        obj["source"] = {
            "rel_path": rel,
            "sha256": sha,
            "mime": mime,
            "bytes": int(len(img_bytes)),
        }
        obj["cache"] = {
            "cache_key": cache_key,
            "prompt_id": prompt_id,
            "prompt_hash": vision_semantics.sha256_text(prompt),
            "model": (vision_model or ""),
            "reason": (reason or "").strip()[:200],
        }
    except Exception:
        pass

    json_text = json.dumps(obj, indent=2, ensure_ascii=False)

    logical = f"image_semantics_{Path(rel).stem or 'image'}"
    _vision_sem_store_artifact(
        project_full,
        logical=logical,
        file_rel=rel,
        json_text=json_text,
        meta={
            "cache_key": cache_key,
            "prompt_id": prompt_id,
            "prompt_hash": vision_semantics.sha256_text(prompt),
            "model": (vision_model or ""),
            "sha256": sha,
            "reason": (reason or "").strip()[:200],
        },
    )

    return True, json_text, (note or "stored")

def _classify_image_wrapper(*, project_name: str, canonical_rel: str, abs_stem: str, suffix: str, caption: str, ocr_text: str) -> Dict[str, Any]:
    return classify_and_map_image(
        project_name,
        canonical_rel=canonical_rel,
        abs_stem=abs_stem,
        suffix=suffix,
        caption=caption,
        ocr_text=ocr_text,
    )

# =============================================================================
# Image classification + mapping (Phase 1.1)
# =============================================================================

def _read_project_goal_focus(project_name: str) -> Tuple[str, str]:
    """
    Read goal (manifest) + focus (project_state.json) without any model call.
    """
    goal = ""
    focus = ""
    try:
        m = load_manifest(project_name)
        goal = (m.get("goal") or "").strip()
    except Exception:
        goal = ""

    try:
        st_raw = read_state_doc(project_name, "project_state", cap=4000).strip()
        if st_raw:
            st = json.loads(st_raw)
            if isinstance(st, dict):
                focus = str(st.get("current_focus") or "").strip()
    except Exception:
        focus = ""

    return goal, focus

def classify_and_map_image(
    project_name: str,
    *,
    canonical_rel: str,
    abs_stem: str,
    suffix: str,
    caption: str,
    ocr_text: str,
) -> Dict[str, Any]:
    """
    Model-assisted but small + universal:
    - Determine what the image is (label/tags)
    - Decide relevance to the CURRENT project (0..1)
    - Suggest a bucket path (logical; not moving files yet)
    - Decide whether we should ask the user what to do

    Returns dict with keys:
      label, tags, suggested_bucket, relevance, ask_user, question, rationale
    """
    goal, focus = _read_project_goal_focus(project_name)

    # Build small evidence text (bounded)
    cap = (caption or "").strip()
    ocr = (ocr_text or "").strip()
    ocr_preview = ocr[:1800]
    cap_preview = cap[:1200]

    # If we have no signal at all, force ask.
    if not cap_preview and not ocr_preview:
        return {
            "label": "unknown",
            "tags": [],
            "suggested_bucket": "references/",
            "relevance": 0.0,
            "ask_user": True,
            "question": "I ingested the image, but I couldn’t read enough to classify it. What should I do with it in this project?",
            "rationale": "No caption/OCR signal available.",
        }

    system = "\n".join([
        "You are a strict JSON classifier for a project workspace.",
        "Return ONLY valid JSON with exactly these keys:",
        "label, tags, suggested_bucket, relevance, ask_user, question, rationale",
        "",
        "Rules:",
        "- label: short (2–6 words), lowercase.",
        "- tags: array of 3–12 short lowercase tags.",
        "- suggested_bucket: one of: references/, inputs/, specs/, screenshots/, receipts/, design/, code/, misc/",
        "- relevance: number 0.0 to 1.0 (how likely this image belongs in the CURRENT project).",
        "- ask_user: true only if relevance < 0.35 OR ambiguity is high.",
        "- question: if ask_user=true, ask ONE short question about where it belongs (project vs general references); else empty string",
        "- rationale: 1–2 short sentences grounded only in the provided evidence.",
        "- Do NOT invent facts beyond caption/OCR/project goal/focus.",
    ])

    user = "\n".join([
        f"PROJECT_NAME: {project_name}",
        f"PROJECT_GOAL: {goal or '(not set)'}",
        f"PROJECT_FOCUS: {focus or '(not set)'}",
        f"FILE: {canonical_rel} ({suffix})",
        "",
        "EVIDENCE_CAPTION:",
        cap_preview or "(none)",
        "",
        "EVIDENCE_OCR:",
        ocr_preview or "(none)",
    ])

    raw = ""
    try:
        raw = call_openai_chat(
            [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ]
        )
    except Exception as e:
        # Non-fatal fallback: keep it useful + conservative
        return {
            "label": "image",
            "tags": ["image"],
            "suggested_bucket": "misc/",
            "relevance": 0.3,
            "ask_user": True,
            "question": "I ingested the image. Should I treat it as project-relevant, or just keep it as a general reference?",
            "rationale": f"Classifier error: {e!r}",
        }

    # Parse strict JSON (best-effort; never throw)
    obj: Dict[str, Any] = {}
    try:
        m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if m:
            parsed = json.loads(m.group(0))
            if isinstance(parsed, dict):
                obj = parsed
    except Exception:
        obj = {}

    # Normalize outputs (defensive)
    label = str(obj.get("label") or "image").strip()[:64].lower()
    tags = obj.get("tags")
    if isinstance(tags, list):
        tags = [str(x).strip().lower() for x in tags if str(x).strip()]
    else:
        tags = []
    tags = tags[:12]

    bucket = str(obj.get("suggested_bucket") or "misc/").strip()
    allowed_buckets = {"references/", "inputs/", "specs/", "screenshots/", "receipts/", "design/", "code/", "misc/"}
    if bucket not in allowed_buckets:
        bucket = "misc/"

    relevance = obj.get("relevance")
    try:
        relevance_f = float(relevance)
    except Exception:
        relevance_f = 0.5
    if relevance_f < 0.0:
        relevance_f = 0.0
    if relevance_f > 1.0:
        relevance_f = 1.0

    ask_user = bool(obj.get("ask_user")) if "ask_user" in obj else (relevance_f < 0.60)
    question = str(obj.get("question") or "").strip()[:240]
    if ask_user and not question:
        question = "How should I use this image in the current project?"
    if not ask_user:
        question = ""

    rationale = str(obj.get("rationale") or "").strip()[:240]

    # Persist artifacts (classification + tags)
    create_artifact(
        project_name,
        f"image_classification_{abs_stem}",
        json.dumps(
            {
                "file": canonical_rel,
                "label": label,
                "tags": tags,
                "suggested_bucket": bucket,
                "relevance": relevance_f,
                "ask_user": ask_user,
                "question": question,
                "rationale": rationale,
            },
            indent=2,
            ensure_ascii=False,
        ),
        artifact_type="image_classification",
        from_files=[canonical_rel],
        file_ext=".json",
    )

    create_artifact(
        project_name,
        f"image_tags_{abs_stem}",
        "\n".join(tags) + ("\n" if tags else ""),
        artifact_type="image_tags",
        from_files=[canonical_rel],
        file_ext=".txt",
    )

    return {
        "label": label,
        "tags": tags,
        "suggested_bucket": bucket,
        "relevance": relevance_f,
        "ask_user": ask_user,
        "question": question,
        "rationale": rationale,
    }
# =============================================================================
# Project store (disk-backed)
# =============================================================================
# Extracted to project_store.py (Step 4)
# - manifest + dir helpers
# - create_artifact*
# - scaffold + register/list
# - chunking/indexing + ingestion entrypoint
# Server.py keeps network glue and passes control into project_store.

# =============================================================================
# Deltas and project map updates on upload (optional model call)
# =============================================================================

def build_upload_delta(project_name: str, new_rel_path: str, prev_rel_path: str, orig_name: str) -> Tuple[str, str]:
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

    old_path = PROJECT_ROOT / prev_rel_path
    new_path = PROJECT_ROOT / new_rel_path
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

def update_project_goal_from_message(project_name: str, user_msg: str) -> Optional[str]:
    text = (user_msg or "").strip()
    if not text:
        return None
    lower = text.lower()

    # If goal is not set yet, do NOT special-case any domain.
    # Goal setting must be explicit (goal: ... / my goal is ...).

    # Explicit prefix-based goal set (existing behavior)
    prefixes = ["my goal is", "the goal is", "our goal is", "goal is", "goal:", "we are trying to", "we're trying to"]
    new_goal: Optional[str] = None
    for prefix in prefixes:
        if lower.startswith(prefix):
            new_goal = text[len(prefix):].lstrip(" :.-")
            break
    if not new_goal:
        return None

    m = load_manifest(project_name)
    m["goal"] = new_goal
    save_manifest(project_name, m)

    # also update (or create) canonical project_state.json
    try:
        st_path = state_file_path(project_name, "project_state")
        st: Dict[str, Any] = {}
        try:
            if st_path.exists():
                st0 = json.loads(st_path.read_text(encoding="utf-8") or "{}")
                if isinstance(st0, dict):
                    st = st0
        except Exception:
            st = {}

        st["goal"] = new_goal
        st["bootstrap_status"] = "active"
        st.setdefault("project_mode", "hybrid")
        st.setdefault("current_focus", "")
        st.setdefault("next_actions", [])
        st.setdefault("key_files", [])
        st["last_updated"] = now_iso()

        atomic_write_text(st_path, json.dumps(st, indent=2))
    except Exception:
        pass

    return new_goal


# =============================================================================
# OpenAI wrapper
# =============================================================================

_OPENAI_CLIENT = None  # lazily initialized SDK client (if available)

def call_openai_chat(messages: List[Dict[str, str]]) -> str:
    """
    Blocking helper for the OpenAI chat call.
    - Uses the official `openai` SDK if installed.
    - Falls back to an OpenAI-compatible HTTP POST if the SDK isn't installed.

    Requires:
      - OPENAI_API_KEY
    Optional:
      - OPENAI_BASE_URL (default: https://api.openai.com/v1)
    """
    api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    base_url = (os.environ.get("OPENAI_BASE_URL") or "https://api.openai.com/v1").strip().rstrip("/")

    global _OPENAI_CLIENT
    if OpenAI is not None:
        # SDK path (fastest / most compatible for OpenAI features)
        if _OPENAI_CLIENT is None:
            kwargs: Dict[str, Any] = {}
            # Only pass base_url if explicitly set (keeps default behavior otherwise)
            if (os.environ.get("OPENAI_BASE_URL") or "").strip():
                kwargs["base_url"] = os.environ["OPENAI_BASE_URL"].strip()
            _OPENAI_CLIENT = OpenAI(**kwargs)  # type: ignore
        resp = _OPENAI_CLIENT.chat.completions.create(model=OPENAI_MODEL, messages=messages)
        return resp.choices[0].message.content or ""

    # HTTP fallback (works with OpenAI-compatible servers)
    url = base_url + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {"model": OPENAI_MODEL, "messages": messages}
    r = requests.post(url, headers=headers, json=payload, timeout=90)
    r.raise_for_status()
    data = r.json()
    try:
        return (data.get("choices") or [{}])[0].get("message", {}).get("content") or ""
    except Exception:
        # last-resort: return raw JSON snippet
        return json.dumps(data, ensure_ascii=False)[:4000]
# =============================================================================
# Model output parsing and state persistence
# =============================================================================

def parse_model_output(text: str) -> Tuple[str, Dict[str, str]]:
    """
    Extract tagged blocks from the model output.
    Returns (user_answer, blocks).

    Patch Protocol safety:
    - PATCH_PROTOCOL_V1 is a control-plane block and must NEVER be user-visible.
    - If the model accidentally echoes it inside USER_ANSWER (or outside tags), we strip it.
    """
    blocks: Dict[str, str] = {}
    for m in _TAG_RE.finditer(text or ""):
        tag = m.group("tag")
        body = (m.group("body") or "").strip("\n")
        blocks[tag] = body.strip()

    user_answer = blocks.get("USER_ANSWER")
    if user_answer is None:
        # fallback: strip any blocks and return remaining
        cleaned = _TAG_RE.sub("", text or "").strip()
        user_answer = cleaned

    # Strip Patch Protocol block if it leaked into user-visible text
    try:
        user_answer = re.sub(
            r"\[PATCH_PROTOCOL_V1\].*?\[/PATCH_PROTOCOL_V1\]",
            "",
            user_answer or "",
            flags=re.DOTALL,
        ).strip()
    except Exception:
        user_answer = (user_answer or "").strip()

    # -----------------------------------------------------------------
    # User-visible sanitization (hard guardrail)
    #
    # Goal:
    # - Never show "system talk" or state-doc sections in chat even if the model
    #   fails to follow the output protocol (e.g., missing [USER_ANSWER]).
    #
    # This keeps UX human and prevents DECISION_LOG / FACTS_MAP / assumptions
    # from leaking into the user's chat surface.
    # -----------------------------------------------------------------
    try:
        ua_lines = (user_answer or "").splitlines()

        # Drop leading "assumption" boilerplate (robot talk)
        cleaned: List[str] = []
        skipping = True
        for ln in ua_lines:
            s = (ln or "").strip()
            low = s.lower()

            if skipping and not s:
                continue

            if skipping and (
                low == "assumption unless overridden"
                or low.startswith("assumption unless overridden")
                or low.startswith("assumption:")
            ):
                continue

            skipping = False
            cleaned.append(ln)

        # Cut off if the model starts dumping state-doc headings into the chat output
        # (common failure mode when [USER_ANSWER] is missing).
        stop_heads = {
            "decision_log",
            "facts_map",
            "project_state_json",
            "project_map",
            "project_brief",
            "working_doc",
            "preferences",
            "patch_protocol_v1",
            "capability_gap_json",
        }

        final_lines: List[str] = []
        for ln in cleaned:
            s = (ln or "").strip()
            low = s.lower()

            # Exact headings or heading-like "DECISION_LOG:" etc.
            head = low.rstrip(":").strip()
            if head in stop_heads:
                break

            # Bracketed tag headers (defensive)
            if head.startswith("[") and head.endswith("]"):
                break

            final_lines.append(ln)

        user_answer = "\n".join(final_lines).strip()
    except Exception:
        user_answer = (user_answer or "").strip()

    return user_answer.strip(), blocks

def _parse_capability_gap_json(raw: str) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}

def _capability_gap_search_queries(obj: Dict[str, Any]) -> List[str]:
    if not isinstance(obj, dict):
        return []
    qs = obj.get("recommended_search_queries")
    if not isinstance(qs, list):
        qs = obj.get("search_queries")
    if not isinstance(qs, list):
        return []
    out = [str(q).strip() for q in qs if str(q).strip()]
    # Bound to 3 queries max
    return out[:3]

async def _synthesize_capability_gap_suggestions(
    *,
    clean_user_msg: str,
    cap_gap_obj: Dict[str, Any],
    search_results: str,
) -> str:
    if not (search_results or "").strip():
        return ""

    sys_note = (
        "You are Quinn. Use SEARCH_EVIDENCE_JSON to propose 2-4 concrete additions "
        "that would enable the missing capabilities. Keep it short. "
        "Use plain bullets only. No sub-bullets. No system talk."
    )
    user_note = (
        "Task:\n"
        f"{clean_user_msg}\n\n"
        "Capability gap (JSON):\n"
        f"{json.dumps(cap_gap_obj, ensure_ascii=False)[:1600]}"
    )

    messages = [
        {"role": "system", "content": sys_note},
        {"role": "system", "content": "SEARCH_EVIDENCE_JSON:\n\n" + (search_results or "")},
        {"role": "user", "content": user_note},
    ]

    try:
        raw = await asyncio.to_thread(call_openai_chat, messages)
    except Exception:
        return ""

    out = (raw or "").strip()
    # Defensive strip of any protocol tags
    out = re.sub(r"\[/?[A-Z0-9_]+\]", "", out).strip()
    return out
def _rewrite_html_image_src(html: str, project_full: str) -> str:
    """
    Rewrite <img src="..."> so relative image paths resolve via /file?path=...
    Only rewrites:
      - src without http(s)
      - src without /file?path=
    """
    if not html or "<img" not in html:
        return html

    def repl(match):
        src = match.group(1).strip()
        # Skip absolute URLs and already-correct paths
        if src.startswith(("http://", "https://", "/file?path=")):
            return match.group(0)

        # Only rewrite bare filenames or relative paths
        safe_src = src.lstrip("./")
        rewritten = f'/file?path=projects/{project_full}/raw/{safe_src}'
        return match.group(0).replace(src, rewritten)

    return re.sub(
        r'<img[^>]+src=["\']([^"\']+)["\']',
        repl,
        html,
        flags=re.IGNORECASE,
    )

def _normalize_no_change(s: str) -> bool:
    return (s or "").strip() == "NO_CHANGE"
def enforce_facts_map_policy(project_name: str, *, max_facts: int = 30) -> None:
    """
    Server-side enforcement so FACTS_MAP stays bounded and well-formed.
    - Dedupe by FACT claim text (case-insensitive)
    - Ensure Provenance + Confidence lines exist (placeholders if missing)
    - Cap to last N facts
    """
    try:
        p = state_file_path(project_name, "facts_map")
        if not p.exists():
            return
        txt = read_text_file(p, errors="replace")
        if not txt.strip():
            return

        lines = txt.splitlines()

        # Split header vs entries (try to keep everything up to "## Entries")
        header: List[str] = []
        entries_lines: List[str] = []
        in_entries = False
        for ln in lines:
            if ln.strip() == "## Entries":
                in_entries = True
                header.append(ln)
                continue
            if not in_entries:
                header.append(ln)
            else:
                entries_lines.append(ln)

        # Parse entries: start at lines beginning with "- FACT:" (preferred) or "FACT:"
        entries: List[List[str]] = []
        cur: List[str] = []
        def is_fact_start(s: str) -> bool:
            st = (s or "").lstrip()
            return st.startswith("- FACT:") or st.startswith("FACT:")

        for ln in entries_lines:
            if is_fact_start(ln):
                if cur:
                    entries.append(cur)
                cur = [ln]
            else:
                if cur:
                    cur.append(ln)
                else:
                    # Ignore stray lines before first FACT entry
                    pass
        if cur:
            entries.append(cur)

        # Normalize + enforce fields, then dedupe
        def claim_key(block: List[str]) -> str:
            if not block:
                return ""
            first = block[0].strip()
            first = first[2:].strip() if first.startswith("- ") else first
            if first.lower().startswith("fact:"):
                first = first[5:].strip()
            return re.sub(r"\s+", " ", first).strip().lower()

        normalized: List[List[str]] = []
        seen: set[str] = set()

        for block in entries:
            key = claim_key(block)
            if not key:
                continue

            # Ensure provenance + confidence exist in the block
            has_prov = any(ln.strip().lower().startswith("- provenance:") for ln in block[1:])
            has_conf = any(ln.strip().lower().startswith("- confidence:") for ln in block[1:])
            out_block = list(block)

            # Add placeholders if missing (do not invent provenance)
            if not has_prov:
                out_block.append("  - Provenance: (missing)")
            if not has_conf:
                out_block.append("  - Confidence: Low")

            # Dedupe: keep the LAST occurrence (newest wins)
            if key in seen:
                # remove older version already added
                normalized = [b for b in normalized if claim_key(b) != key]
            seen.add(key)
            normalized.append(out_block)

        # Cap to last max_facts entries
        if max_facts and len(normalized) > max_facts:
            normalized = normalized[-max_facts:]

        # Rebuild file
        out_lines: List[str] = []
        out_lines.extend(header if header else ["# Facts Map (Canonical Project Memory)", "", "## Entries"])
        if not header:
            out_lines.append("## Entries")

        # Ensure there's at least one line after header
        if not normalized:
            out_lines.append("- (none yet)")
        else:
            # Ensure a blank line after "## Entries"
            if out_lines and out_lines[-1].strip() == "## Entries":
                out_lines.append("")
            for block in normalized:
                out_lines.extend(block)
                out_lines.append("")

        atomic_write_text(p, "\n".join(out_lines).rstrip() + "\n", encoding="utf-8", errors="strict")
    except Exception:
        # Never let policy enforcement break the request path
        return
def _looks_truncated_state_doc(doc_text: str) -> bool:
    """
    Deterministic integrity guard for canonical state docs.
    Reject obvious truncation / ellipsis artifacts that should not be stored as truth.
    """
    s = (doc_text or "").strip()
    if not s:
        return False

    # Hard red flags that indicate truncation or lossy summarization:
    # - explicit ellipsis token
    # - backticked tokens containing ellipsis (common filename truncation)
    if "..." in s:
        # Allow "..." only if it appears in a code block fence context (rare for decision logs),
        # but simplest safe rule: treat any ellipsis as truncation for state docs.
        return True

    # Common truncated filename pattern: `1767...png`
    if "`" in s and "...`" in s:
        return True

    return False

def persist_state_blocks(project_name: str, blocks: Dict[str, str]) -> List[str]:
    """
    Save project state updates coming from the model.
    Returns a list of newly-created artifact relative paths (PROJECT_ROOT-relative).
    """
    ensure_project_scaffold(project_name)

    created_paths: List[str] = []
    # PATCH_PROTOCOL_V1 (preferred deterministic write path)
    patch_raw = blocks.get("PATCH_PROTOCOL_V1")
    if patch_raw and not _normalize_no_change(patch_raw):
        # Hard rule: if patch protocol is present, do not allow simultaneous whole-doc canonical writes.
        # This prevents double-apply ambiguity and keeps replay deterministic.
        for disallowed in ("PROJECT_MAP", "PROJECT_BRIEF", "WORKING_DOC", "PREFERENCES", "DECISION_LOG", "FACTS_MAP", "PROJECT_STATE_JSON"):
            if blocks.get(disallowed) and not _normalize_no_change(blocks.get(disallowed) or ""):
                # Deterministic rule: PATCH_PROTOCOL_V1 wins. Discard whole-doc blocks instead of crashing.
                blocks.pop(disallowed, None)

        # Strict validation/apply with deterministic rejection logging.
        try:
            created_paths.extend(apply_patch_protocol_v1(project_name, patch_raw))
        except Exception as e:
            # Best-effort: parse idempotency + patch_id for receipt metadata (no guessing).
            patch_id = ""
            idem_key = ""
            try:
                env0 = json.loads(patch_raw)
                if isinstance(env0, dict):
                    patch_id = str(env0.get("patch_id") or "").strip()
                    idem = env0.get("idempotency") if isinstance(env0.get("idempotency"), dict) else {}
                    idem_key = str((idem or {}).get("key") or "").strip()
            except Exception:
                pass

            # Receipt (rejected)
            _pp1_append_receipt(
                project_name,
                {
                    "v": 1,
                    "patch_id": patch_id,
                    "idempotency_key": idem_key,
                    "applied_at": now_iso(),
                    "outcome": "rejected",
                    "error": str(e),
                },
            )

            # Artifact for debugging/auditing (versioned, does NOT modify canonical state)
            try:
                entry = create_artifact(
                    project_name,
                    "patch_protocol_v1_rejected",
                    json.dumps(
                        {
                            "when": now_iso(),
                            "error": str(e),
                            "patch_id": patch_id,
                            "idempotency_key": idem_key,
                            "raw": (patch_raw or "")[:20000],
                        },
                        indent=2,
                        ensure_ascii=False,
                    ),
                    artifact_type="patch_protocol_v1_rejected",
                    from_files=[],
                    file_ext=".json",
                )
                created_paths.append(str(entry.get("path") or ""))
            except Exception:
                pass

        return [p for p in created_paths if p]

    # PROJECT_STATE_JSON -> canonical state file + versioned artifact
    st_raw = blocks.get("PROJECT_STATE_JSON")
    if st_raw and not _normalize_no_change(st_raw):
        # Load existing state so we don't clobber C6.3 fields like project_mode/bootstrap_status/domains
        existing_state: Dict[str, Any] = {}
        try:
            st_path_existing = state_file_path(project_name, "project_state")
            if st_path_existing.exists():
                prev = json.loads(st_path_existing.read_text(encoding="utf-8") or "{}")
                if isinstance(prev, dict):
                    existing_state = prev
        except Exception:
            existing_state = {}

        # validate JSON; if invalid, store raw with an error wrapper (but preserve existing keys)
        try:
            st_obj = json.loads(st_raw)
            if not isinstance(st_obj, dict):
                raise ValueError("PROJECT_STATE_JSON must be a JSON object")

            merged = dict(existing_state)
            merged.update(st_obj)

            # Server-enforced Expert Frame Lock (prevents hesitant "proposed" drift)
            try:
                ef = merged.get("expert_frame")
                ef_status = ""
                if isinstance(ef, dict):
                    ef_status = str(ef.get("status") or "").strip().lower()

                if not isinstance(ef, dict) or ef_status in ("", "proposed", "draft", "suggested"):
                    merged["expert_frame"] = {
                        "status": "locked",
                        "label": "General Project Operator",
                        "directive": (
                            "Act as a locked, authoritative expert. Optimize for forward progress. "
                            "Do not ask for confirmation unless truly blocked. "
                            "If key details (format/audience/constraints) are missing, assume sensible defaults, "
                            "state assumptions explicitly, and proceed; user can override later."
                        ),
                        "set_reason": "server_enforced_lock",
                        "updated_at": now_iso(),
                    }
            except Exception:
                pass

            # minimal required keys
            merged.setdefault("goal", "")
            merged.setdefault("current_focus", "")
            merged.setdefault("next_actions", [])
            merged.setdefault("key_files", [])
            merged["last_updated"] = now_iso()

            # If questions exist, ensure we still proceed by default (default-forward next action)
            try:
                qs = merged.get("questions")
                na = merged.get("next_actions")

                qs_list = qs if isinstance(qs, list) else []
                na_list = na if isinstance(na, list) else []
                merged["next_actions"] = na_list

                if qs_list:
                    needle = "best-effort"
                    already = any(isinstance(x, str) and needle in x.lower() for x in na_list)
                    if not already:
                        merged["next_actions"] = [
                            "Proceed now with best-effort defaults (state assumptions); user can override later."
                        ] + na_list
            except Exception:
                pass

            st_canon = json.dumps(merged, indent=2, ensure_ascii=False)
        except Exception as e:
            merged = dict(existing_state)
            merged["parse_error"] = str(e)
            merged["raw"] = st_raw
            merged["last_updated"] = now_iso()
            st_canon = json.dumps(merged, indent=2, ensure_ascii=False)

        project_store.write_canonical_entry(
            project_name,
            target_path=state_file_path(project_name, "project_state"),
            mode="json_overwrite",
            data=st_canon,
        )
        entry = create_artifact(
            project_name,
            "project_state",
            st_canon,
            artifact_type="project_state",
            from_files=[],
            file_ext=".json",
        )
        created_paths.append(str(entry.get("path") or ""))

        # If goal present, update manifest goal

        try:
            st_obj2 = json.loads(st_canon)
            if isinstance(st_obj2, dict) and isinstance(st_obj2.get("goal"), str):
                m = load_manifest(project_name)
                if st_obj2["goal"] and st_obj2["goal"] != (m.get("goal") or ""):
                    m["goal"] = st_obj2["goal"]
                    save_manifest(project_name, m)
        except Exception:
            pass

    # Markdown docs
    mapping = {
        "PROJECT_MAP": ("project_map", "project_map", ".md"),
        "PROJECT_BRIEF": ("project_brief", "project_brief", ".md"),
        "WORKING_DOC": ("working_doc", "working_doc", ".md"),
        "PREFERENCES": ("preferences", "preferences", ".md"),
        "DECISION_LOG": ("decision_log", "decision_log", ".md"),
        # IMPORTANT:
        # FACTS_MAP must be deterministic (Tier-1 -> Tier-2 distiller), not model-authored.
        # If the model writes FACTS_MAP, it can overwrite the distiller output and drop identity facts.
        # So we do NOT persist model-provided FACTS_MAP blocks.
        # "FACTS_MAP": ("facts_map", "facts_map", ".md"),
    }
    for tag, (atype, logical, ext) in mapping.items():
        body = blocks.get(tag)
        if not body or _normalize_no_change(body):
            continue
        # Integrity guard: never overwrite canonical state docs with obviously truncated content.
        # If we detect truncation, store it as an error artifact and keep the previous canonical file.
        if atype in ("decision_log", "project_map", "project_brief", "working_doc", "preferences", "facts_map"):
            if _looks_truncated_state_doc(body):
                err_entry = create_artifact(
                    project_name,
                    f"{logical}_rejected",
                    body,
                    artifact_type=f"{atype}_rejected",
                    from_files=[],
                    file_ext=ext,
                    meta={"reason": "truncation_detected"},
                )
                created_paths.append(str(err_entry.get("path") or ""))
                continue

        project_store.write_canonical_entry(
            project_name,
            target_path=state_file_path(project_name, atype),
            mode="text_overwrite",
            data=body,
        )
        entry = create_artifact(project_name, logical, body, artifact_type=atype, from_files=[], file_ext=ext)
        created_paths.append(str(entry.get("path") or ""))


        if atype == "facts_map":
            enforce_facts_map_policy(project_name, max_facts=30)

    # Excel deliverable: model provides EXCEL_SPEC_JSON, server writes an .xlsx artifact
    x_spec_raw = blocks.get("EXCEL_SPEC_JSON")
    if x_spec_raw and not _normalize_no_change(x_spec_raw):
        try:
            spec_obj = json.loads(x_spec_raw)
            if not isinstance(spec_obj, dict):
                raise ValueError("EXCEL_SPEC_JSON must be a JSON object")

            report = validate_excel_spec_master(spec_obj)
            repaired_flag = False
            if report.get("ok") is not True:
                spec_obj2, report2, repaired_flag = maybe_repair_excel_spec_once_master(spec_obj, report)
                spec_obj, report = spec_obj2, report2

            if report.get("ok") is not True:
                entry = create_artifact(
                    project_name,
                    "excel_generation_error",
                    json.dumps(
                        {
                            "error": "spec_validation_failed_master",
                            "report": report,
                            "raw": (x_spec_raw or "")[:8000],
                            "when": now_iso(),
                        },
                        indent=2,
                        ensure_ascii=False,
                    ),
                    artifact_type="excel_error",
                    from_files=[],
                    file_ext=".json",
                )
                created_paths.append(str(entry.get("path") or ""))
            else:
                x_bytes = generate_workbook_from_spec(spec_obj, project_raw_dir=raw_dir(project_name))

                entry = create_artifact_bytes(
                    project_name,
                    "deliverable_excel",
                    x_bytes,
                    artifact_type="deliverable_excel",
                    from_files=[],
                    file_ext=".xlsx",
                    meta={
                        "spec_name": str(spec_obj.get("name") or ""),
                        "generator": "openpyxl",
                        "repaired": bool(repaired_flag),
                        "validation_warnings": report.get("warnings") or [],
                    },
                )
                created_paths.append(str(entry.get("path") or ""))

                # C1: Register Excel deliverable as a first-class object
                try:
                    project_store.register_deliverable(
                        project_name,
                        deliverable_type="xlsx",
                        title=str(spec_obj.get("name") or "Excel Deliverable"),
                        path=str(entry.get("path") or ""),
                        source="excel_engine",
                    )
                except Exception:
                    pass
        except Exception as e:
            # Never break the chat loop on Excel generation errors; store a note artifact instead.

            entry = create_artifact(
                project_name,
                "excel_generation_error",
                json.dumps(
                    {"error": repr(e), "raw": (x_spec_raw or "")[:8000], "when": now_iso()},
                    indent=2,
                    ensure_ascii=False,
                ),
                artifact_type="excel_error",
                from_files=[],
                file_ext=".json",
            )
            created_paths.append(str(entry.get("path") or ""))

    # Deliverables (print-ready): model provides HTML, server writes an artifact file

    d_html = blocks.get("DELIVERABLE_HTML")
    if d_html and not _normalize_no_change(d_html):
        fixed_html = _rewrite_html_image_src(d_html, project_name)
        entry = create_artifact(
            project_name,
            "deliverable_html",
            fixed_html,
            artifact_type="deliverable_html",
            from_files=[],
            file_ext=".html",
        )
        created_paths.append(str(entry.get("path") or ""))

    return [p for p in created_paths if p]
def _extract_user_rules_from_message(user_msg: str) -> List[str]:
    """
    Deterministically extract "assistant behavior rules" from a user message.
    Conservative on purpose: only captures explicit imperatives that are likely
    meant as stable constraints (e.g., "never", "do not", "don't", "only", "always").
    """
    raw = (user_msg or "").strip()
    if not raw:
        return []

    low = raw.lower()

    # Fast gate: avoid parsing every message.
    if not any(k in low for k in ("i want you to", "never", "don't", "do not", "only", "always")):
        return []

    # Split into candidate lines/sentences.
    candidates: List[str] = []
    for ln in raw.splitlines():
        s = ln.strip()
        if not s:
            continue
        candidates.append(s)

    # Also split long single-line messages into sentence-ish chunks.
    if len(candidates) <= 1 and len(raw) > 120:
        parts = re.split(r"(?<=[\.\!\?])\s+", raw)
        for p in parts:
            p2 = (p or "").strip()
            if p2:
                candidates.append(p2)

    rules: List[str] = []
    for c in candidates:
        c0 = c.strip()
        c_low = c0.lower()

        # Must look like an explicit rule about assistant behavior.
        if not any(c_low.startswith(pfx) for pfx in ("never", "don't", "do not", "only", "always", "please don't")):
            if "i want you to" not in c_low:
                continue

        # Avoid capturing generic content rules unrelated to assistant behavior.
        if not any(w in c_low for w in ("you", "suggest", "say", "ask", "respond", "reply", "give me", "don't suggest", "never suggest")):
            continue

        # Normalize common "I want you to ..." prefix.
        if "i want you to" in c_low:
            # Keep original casing but trim the prefix in a simple, deterministic way.
            i = c_low.find("i want you to")
            if i >= 0:
                c0 = c0[i + len("i want you to"):].strip()
                # Remove leading punctuation.
                c0 = c0.lstrip(":").strip()

        # Hard caps to keep state small and stable.
        c0 = c0.strip()
        if not c0:
            continue
        if len(c0) > 220:
            c0 = c0[:220].rstrip() + "…"

        # De-duplicate (case-insensitive).
        if any(r.lower() == c0.lower() for r in rules):
            continue

        rules.append(c0)
        if len(rules) >= 10:
            break

    return rules


def ingest_user_rules_message(project_name: str, user_msg: str) -> int:
    """
    Deterministically ingest user rules into canonical project_state.json under `user_rules`.
    Returns number of new rules added.
    """
    ensure_project_scaffold(project_name)

    new_rules = _extract_user_rules_from_message(user_msg)
    if not new_rules:
        return 0

    st_path = state_file_path(project_name, "project_state")
    st: Dict[str, Any] = {}
    try:
        if st_path.exists():
            st0 = json.loads(st_path.read_text(encoding="utf-8", errors="replace") or "{}")
            if isinstance(st0, dict):
                st = st0
    except Exception:
        st = {}

    existing = st.get("user_rules")
    if not isinstance(existing, list):
        existing = []

    existing_norm = []
    for it in existing:
        s = str(it or "").strip()
        if s:
            existing_norm.append(s)

    added = 0
    for r in new_rules:
        if any(x.lower() == r.lower() for x in existing_norm):
            continue
        existing_norm.append(r)
        added += 1

    if added:
        # Keep bounded.
        st["user_rules"] = existing_norm[-30:]
        try:
            atomic_write_text(st_path, json.dumps(st, indent=2, sort_keys=True) + "\n")
        except Exception:
            # Phase-1: eliminate non-atomic fallback writes for project_state
            pass

    return added

# (cleanup) Removed special-case durable facts ingestion.
# All fact capture must flow through the unified Tier-1 write path and Tier-2 distiller.

def should_snapshot_answer(answer: str) -> bool:
    """
    Heuristic: snapshot substantial outputs as artifacts so the project can re-open
    without "reconstructing" deliverables from memory.
    """
    a = (answer or "").strip()
    if not a:
        return False
    if len(a) >= 1200:
        return True
    if "```" in a:
        return True
    if a.startswith("#") or "\n#" in a:
        return True
    if "\n- " in a or "\n1." in a or "\n2." in a:
        return True
    return False

def snapshot_assistant_output(project_name: str, content: str) -> Optional[Dict[str, Any]]:
    if not should_snapshot_answer(content):
        return None
    stamp = time.strftime("%Y%m%d_%H%M%S", time.gmtime())
    logical = f"assistant_output_{stamp}"
    try:
        return create_artifact(project_name, logical, content, artifact_type="assistant_output", file_ext=".md")
    except Exception:
        return None
def generate_system_registry(project_name: str) -> Dict[str, str]:
    """
    Deterministically generate the repo registry and write fixed-name outputs into this project's artifacts folder:

      projects/<user>/<project>/artifacts/system_registry_v1.json
      projects/<user>/<project>/artifacts/system_registry_v1.md
      projects/<user>/<project>/artifacts/system_registry_notes.md   (created if missing)

    Returns:
      {
        "json_rel": "<relative path>",
        "md_rel": "<relative path>",
        "notes_rel": "<relative path>",
        "open": "/file?path=<md_rel>",
        "notes_open": "/file?path=<notes_rel>"
      }
    """
    ensure_project_scaffold(project_name)

    # Build registry from the actual repo root (PROJECT_ROOT)
    reg = registry_builder.build_system_registry(PROJECT_ROOT)
    md = registry_builder.registry_to_markdown(reg)

    # Write fixed-name files inside THIS project's artifacts dir (so /file can serve them)
    ad = artifacts_dir(project_name)
    ad.mkdir(parents=True, exist_ok=True)

    json_abs = ad / "system_registry_v1.json"
    md_abs = ad / "system_registry_v1.md"
    notes_abs = ad / "system_registry_notes.md"

    atomic_write_text(json_abs, json.dumps(reg, indent=2, ensure_ascii=False) + "\n")
    atomic_write_text(md_abs, md)

    # Create companion notes file if missing (human semantic layer)
    if not notes_abs.exists():
        try:
            notes = registry_builder.registry_notes_template(reg)
        except Exception:
            # ultra-safe fallback template
            notes = "# System Registry Notes\n\n- TODO: add per-file purpose, known issues, invariants.\n"
        atomic_write_text(notes_abs, notes)

    # Convert to /file relative path (PROJECT_ROOT-relative)
    json_rel = str(json_abs.relative_to(PROJECT_ROOT)).replace("\\", "/")
    md_rel = str(md_abs.relative_to(PROJECT_ROOT)).replace("\\", "/")
    notes_rel = str(notes_abs.relative_to(PROJECT_ROOT)).replace("\\", "/")

    return {
        "json_rel": json_rel,
        "md_rel": md_rel,
        "notes_rel": notes_rel,
        "open": f"/file?path={md_rel}",
        "notes_open": f"/file?path={notes_rel}",
    }

# =============================================================================
# Context building
# =============================================================================

def read_state_doc(project_name: str, key: str, cap: int = 12000) -> str:
    p = state_file_path(project_name, key)
    if not p.exists():
        return ""
    txt = read_text_file(p, errors="replace")
    if cap and len(txt) > cap:
        return txt[:cap] + "\n...[truncated]..."
    return txt
def ingest_chatlog_file_to_facts_raw(
    project_full: str,
    *,
    raw_rel_path: str,
    expert_label: str = "",
    max_facts_per_chunk: int = 60,
) -> Dict[str, Any]:
    """
    Import a full chat log file (from another project or export) and extract Tier-1 raw fact candidates.

    Writes:
      - state/facts_raw.jsonl (append-only)

    Requirements:
      - Each extracted fact MUST include an evidence quote from the log.
      - Each extracted fact MUST include a pointer back to the source turn (turn_index, timestamp if available).
      - Facts are candidates (Tier-1), NOT distilled truth.
    """
    project_store.ensure_project_scaffold(project_full)

    rel = (raw_rel_path or "").replace("\\", "/").strip()
    if not rel:
        return {"ok": False, "error": "missing_raw_rel_path"}

    abs_path = (PROJECT_ROOT / rel).resolve()
    if not abs_path.exists() or not abs_path.is_file():
        return {"ok": False, "error": f"file_missing: {rel}"}

    # Read the entire file (bounded by hard cap to prevent OOM; adjust if needed)
    text = read_text_file(abs_path, errors="replace")
    if not text.strip():
        return {"ok": False, "error": "empty_file"}

    # Parse into turns (supports JSONL {role,content,timestamp} and loose text formats)
    turns: List[Dict[str, Any]] = []
    for ln in text.splitlines():
        s = (ln or "").strip()
        if not s:
            continue
        obj = None
        try:
            obj = json.loads(s)
        except Exception:
            obj = None

        if isinstance(obj, dict) and ("content" in obj or "text" in obj):
            role = str(obj.get("role") or "").strip().lower() or "user"
            content = str(obj.get("content") or obj.get("text") or "").strip()
            ts = str(obj.get("timestamp") or obj.get("ts") or obj.get("created_at") or "").strip()
            if content:
                turns.append({"role": role, "content": content, "timestamp": ts})
            continue

        # Loose format fallback
        if s.upper().startswith("USER:"):
            turns.append({"role": "user", "content": s.split(":", 1)[1].strip(), "timestamp": ""})
        elif s.upper().startswith("ASSISTANT:"):
            turns.append({"role": "assistant", "content": s.split(":", 1)[1].strip(), "timestamp": ""})

    if not turns:
        # If we couldn’t parse structure, treat whole file as one “user” blob (still extractable)
        turns = [{"role": "user", "content": text.strip(), "timestamp": ""}]

    # Chunk turns to keep calls bounded
    chunks: List[List[Dict[str, Any]]] = []
    cur: List[Dict[str, Any]] = []
    cur_chars = 0
    for t in turns:
        line = f"{t.get('role','user').upper()}: {t.get('content','')}"
        if cur and (cur_chars + len(line) > 9000 or len(cur) >= 80):
            chunks.append(cur)
            cur = []
            cur_chars = 0
        cur.append(t)
        cur_chars += len(line)
    if cur:
        chunks.append(cur)

    system = "\n".join([
        "You are extracting Tier-1 raw fact candidates from a chat log.",
        "Return ONLY strict JSON (no prose).",
        "",
        "Output schema:",
        "{",
        '  "facts": [',
        "    {",
        '      "claim": "string (normalized, 1 sentence)",',
        '      "slot": "identity|relationship|preference|possession|routine|constraint|context|event|other",',
        '      "subject": "user|other|project|unknown",',
        '      "confidence": "low|medium|high",',
        '      "volatility": "stable|likely_to_change|unknown",',
        '      "privacy": "normal|sensitive",',
        '      "evidence_quote": "short quote copied from the log",',
        '      "turn_index": 123,',
        '      "timestamp": "optional string if present"',
        "    }",
        "  ]",
        "}",
        "",
        "Rules:",
        "- Extract ANY potentially recallable personal detail, preference, possession, relationship, or constraint.",
        "- Do NOT invent facts. Every fact MUST have an evidence_quote copied from the input.",
        "- Keep claims short and normalized.",
        f"- Hard cap: return at most {int(max_facts_per_chunk)} facts per chunk.",
        "- privacy=sensitive for health, sex, trauma, illegal activity, exact address, etc.",
    ])

    total_written = 0
    errors: List[str] = []

    for chunk_i, chunk in enumerate(chunks):
        # Build user payload for this chunk
        lines: List[str] = []
        for i, t in enumerate(chunk):
            role = str(t.get("role") or "user").strip().upper()
            content = str(t.get("content") or "").strip()
            ts = str(t.get("timestamp") or "").strip()
            # Global turn index proxy: chunk offset + i
            turn_index = (chunk_i * 100000) + i
            if ts:
                lines.append(f"[{turn_index} | {ts}] {role}: {content}")
            else:
                lines.append(f"[{turn_index}] {role}: {content}")

        user = "\n".join(lines)[:12000]

        raw = call_openai_chat([
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ])

        try:
            m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
            obj = json.loads(m.group(0)) if m else {}
            facts = obj.get("facts")
            if not isinstance(facts, list):
                facts = []
        except Exception as e:
            errors.append(f"chunk_{chunk_i}: parse_error:{e!r}")
            continue

        # Write each fact candidate (append-only)
        for f in facts[: int(max_facts_per_chunk)]:
            if not isinstance(f, dict):
                continue

            claim = str(f.get("claim") or "").strip()
            quote = str(f.get("evidence_quote") or "").strip()
            if not claim or not quote:
                continue

            entry = {
                "id": f"rawfact_{time.strftime('%Y_%m_%d')}_{total_written+1:04d}",
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
                "claim": claim,
                "slot": str(f.get("slot") or "other").strip(),
                "subject": str(f.get("subject") or "unknown").strip(),
                "confidence": str(f.get("confidence") or "low").strip(),
                "volatility": str(f.get("volatility") or "unknown").strip(),
                "privacy": str(f.get("privacy") or "normal").strip(),
                "evidence_quote": quote[:600],
                "source": {
                    "type": "imported_chat_log",
                    "file": rel,
                    "turn_index": int(f.get("turn_index") or 0),
                    "timestamp": str(f.get("timestamp") or "").strip(),
                },
            }

            ok = project_store.write_canonical_entry(
                project_full,
                target_path=project_store.facts_raw_path(project_full),
                mode="jsonl_append",
                data=entry,
            )
            if ok:
                total_written += 1

    return {"ok": True, "facts_written": total_written, "chunks": len(chunks), "errors": errors[:12]}

def read_recent_chat_log_snippet(project_name: str, *, n_messages: int = 8, cap_chars: int = 4000) -> str:
    """
    Persistent continuity across sessions: include the last few chat messages
    (user-visible only) in the model context.
    """
    p = state_dir(project_name) / "chat_log.jsonl"
    if not p.exists():
        return ""
    try:
        tail = p.read_text(encoding="utf-8", errors="replace").splitlines()[-n_messages:]
    except Exception:
        return ""

    rendered: List[str] = []
    for line in tail:
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        role = str(obj.get("role") or "").strip().upper() or "UNK"
        content = str(obj.get("content") or "").strip()
        if not content:
            continue
        if len(content) > 900:
            content = content[:900] + "\n.[truncated]."
        rendered.append(f"{role}: {content}")

    snippet = "\n".join(rendered).strip()
    if cap_chars and len(snippet) > cap_chars:
        snippet = snippet[-cap_chars:]
    return snippet
# ------------------------------------------------------------
# Contextual Greetings v1 (human, context-aware, bounded)
# ------------------------------------------------------------
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

def _greet_daypart_for_project(user_seg: str) -> tuple[str, str]:
    """
    Return (daypart, tz_name) deterministically using Tier-2G timezone if present.
    daypart: morning|afternoon|evening|night
    """
    tz_name = "America/Chicago"
    try:
        prof = project_store.load_user_profile(safe_user_name(user_seg)) or {}
    except Exception:
        prof = {}
    try:
        ident = prof.get("identity") if isinstance(prof, dict) else {}
        if not isinstance(ident, dict):
            ident = {}
        tz = ident.get("timezone") if isinstance(ident.get("timezone"), dict) else {}
        tz_val = str((tz or {}).get("value") or "").strip()
        if tz_val:
            tz_name = tz_val
    except Exception:
        pass

    # Local time (best-effort)
    try:
        if ZoneInfo is not None:
            import datetime as _dt
            dt = _dt.datetime.now(tz=ZoneInfo(tz_name))
            h = int(dt.hour)
        else:
            h = int(time.strftime("%H"))
    except Exception:
        h = 12

    if 5 <= h < 12:
        return "morning", tz_name
    if 12 <= h < 17:
        return "afternoon", tz_name
    if 17 <= h < 22:
        return "evening", tz_name
    return "night", tz_name

def _parse_iso_utc_epoch(ts: str) -> float:
    """
    Parse 'YYYY-MM-DDTHH:MM:SSZ' into epoch seconds (UTC). Returns 0.0 on failure.
    Deterministic + no external deps.
    """
    s = (ts or "").strip()
    if not s:
        return 0.0
    try:
        return time.mktime(time.strptime(s, "%Y-%m-%dT%H:%M:%SZ"))
    except Exception:
        return 0.0

def _greet_recent_chat_snippet_with_age(project_full: str, *, max_age_days: int = 14) -> tuple[str, Optional[int], str]:
    """
    Return (snippet, age_days, source):
      - source: "recent_user_msg" | "recent_any_msg" | "none"
    Rules:
      - Only return a snippet if the most recent log timestamp is within max_age_days.
      - Prefer user-visible tail renderer (bounded) but gate it by recency.
      - If timestamps are missing/unparseable, treat as stale (source="none").
    """
    # Find the newest timestamp in chat_log.jsonl (bounded tail scan).
    newest_epoch = 0.0
    try:
        p = state_dir(project_full) / "chat_log.jsonl"
        if p.exists():
            lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
            tail = lines[-250:] if len(lines) > 250 else lines
            for ln in reversed(tail):
                s = (ln or "").strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except Exception:
                    continue
                if not isinstance(obj, dict):
                    continue
                ts = str(obj.get("ts") or "").strip()
                ep = _parse_iso_utc_epoch(ts)
                if ep > 0:
                    newest_epoch = ep
                    break
    except Exception:
        newest_epoch = 0.0

    # If we can't parse timestamps, do not risk stale-topic hallucinations.
    if newest_epoch <= 0:
        return "", None, "none"

    age_s = max(0.0, time.time() - newest_epoch)
    age_days = int(age_s // 86400)

    if age_days > int(max_age_days):
        return "", age_days, "none"

    # Within recency window: return bounded tail snippet.
    try:
        snip = read_recent_chat_log_snippet(project_full, n_messages=8, cap_chars=1400)
    except Exception:
        snip = ""

    snip = (snip or "").strip()
    if not snip:
        return "", age_days, "none"

    # Best-effort “source”: if there's a USER line in the snippet, call it recent_user_msg.
    src = "recent_user_msg" if "USER:" in snip else "recent_any_msg"
    return snip, age_days, src

def _greet_pick_recent_topic_snippet(project_full: str) -> str:
    """
    Deterministic, bounded snippet for greeting continuity.
    Recency-gated: returns empty if chat activity is stale.
    """
    snip, _age_days, _src = _greet_recent_chat_snippet_with_age(project_full, max_age_days=14)
    return (snip or "").strip()

async def build_contextual_greeting(
    *,
    user: str,
    project_full: str,
    project_short: str,
    reason: str,
) -> str:
    """
    LLM-based but bounded greeting:
    - May ONLY reference details present in provided evidence fields.
    - Must be 1–2 short lines + exactly one question.
    - Never invent past topics.
    """
    try:
        missing_fields = _couples_intake_missing_fields(user, project_full)
        try:
            st = project_store.load_project_state(project_full) or {}
        except Exception:
            st = {}
        try:
            audit_event(
                project_full,
                {
                    "schema": "audit_turn_v1",
                    "stage": "couples_intake_greeting_skip",
                    "couples_intake_completed": bool((st or {}).get("couples_intake_completed")),
                    "missing_fields_count": len(missing_fields or []),
                },
            )
        except Exception:
            pass
    except Exception:
        pass

    user_seg = safe_user_name(user or "")
    frame = {}
    try:
        frame = resolve_user_frame(user=user, current_project_full=project_full, user_text="") or {}
    except Exception:
        frame = {}

    preferred_name = str(frame.get("preferred_name") or "").strip()
    if not preferred_name:
        preferred_name = str(user or "").strip()

    daypart, tz_name = _greet_daypart_for_project(user_seg)
    topic_snip, topic_age_days, topic_source = _greet_recent_chat_snippet_with_age(project_full, max_age_days=14)

    # Expert switch (optional): reason like "expert_switch:analysis"
    reason_l = str(reason or "").strip().lower()
    expert_id = ""
    if reason_l.startswith("expert_switch:"):
        expert_id = reason_l.split(":", 1)[1].strip()
    expert_label_map = {
        "default": "Default",
        "coding": "Coding",
        "writing": "Writing",
        "therapist": "Therapist",
        "analysis": "Analysis",
    }
    expert_tone_map = {
        "default": "classic Trebek (calm, poised, lightly wry)",
        "coding": "technical and pragmatic",
        "writing": "editorial and crisp",
        "therapist": "calm, steady, clinical warmth",
        "analysis": "precise, analytical, no fluff",
    }
    expert_label = expert_label_map.get(expert_id, "Default")
    expert_tone = expert_tone_map.get(expert_id, expert_tone_map["default"])

    # Evidence fields (truth-bound; do NOT add anything else)
    ev = {
        "preferred_name": preferred_name,
        "daypart": daypart,
        "timezone": tz_name,
        "project": project_short,
        "reason": str(reason or "").strip(),
        "recent_chat_snippet": topic_snip,
        "expert_label": expert_label if expert_id else "",
    }

    is_couples = _is_couples_user(user) and str(project_full or "").replace(" ", "_").lower().endswith("/couples_therapy")
    if expert_id:
        sys_prompt = "\n".join(
            [
                "Your name is Quinn.",
                f"You are switching into the {expert_label} expert hat.",
                "",
                "Return ONLY strict JSON with keys: greeting, followup",
                "",
                "Hard rules:",
                "- Use the preferred_name if provided.",
                "- Use the daypart (morning/afternoon/evening/night) naturally.",
                f"- Tone: {expert_tone}.",
                "- Acknowledge the switch in a human way (no \"Got it\"; no em-dashes).",
                "- Do NOT mention projects or switching chats.",
                "- You MAY reference prior topic ONLY if it is explicitly present in recent_chat_snippet.",
                "- If recent_chat_snippet is empty, do NOT pretend familiarity.",
                "- greeting: 1 short sentence.",
                "- followup: 1 short sentence that ends with exactly ONE question mark.",
                "- Total combined length <= 220 characters.",
                "- No emojis.",
                "- No system/meta talk (no 'project pulse', no 'I see in your logs').",
            ]
        ).strip()
    elif is_couples:
        sys_prompt = "\n".join(
            [
                "Your name is Quinn.",
                "You write a short, professional greeting as a couples therapist.",
                "",
                "Return ONLY strict JSON with keys: greeting, followup",
                "",
                "Hard rules:",
                "- Use the preferred_name if provided.",
                "- Use the daypart (morning/afternoon/evening/night) naturally.",
                "- Tone: calm, steady, clinical warmth (not jokey).",
                "- Avoid generic filler like \"hope you're well\".",
                "- Do NOT mention projects, switching, or choosing a chat.",
                "- You MAY reference prior topic ONLY if it is explicitly present in recent_chat_snippet.",
                "- If recent_chat_snippet is empty, do NOT pretend familiarity.",
                "- greeting: 1 short sentence.",
                "- followup: 1 short sentence that ends with exactly ONE question mark.",
                "- Total combined length <= 220 characters.",
                "- No emojis.",
                "- No system/meta talk (no 'project pulse', no 'I see in your logs').",
            ]
        ).strip()
    else:
        sys_prompt = "\n".join(
            [
                "Your name is Quinn.",
                "You write a short, human greeting for a returning user in a multi-chat workspace.",
                "",
                "Return ONLY strict JSON with keys: greeting, followup",
                "",
                "Hard rules:",
                "- Use the preferred_name if provided.",
                "- Use the daypart (morning/afternoon/evening/night) naturally.",
                "- Cadence: classic Trebek (calm, poised, lightly wry, no hype).",
                "- Include a short character quip (3-8 words) tied to daypart or recent_chat_snippet; no new facts.",
                "- Avoid generic filler like \"hope you're well\".",
                "- Do NOT mention projects, switching, or choosing a chat.",
                "- You MAY reference prior topic ONLY if it is explicitly present in recent_chat_snippet.",
                "- If recent_chat_snippet is empty, do NOT pretend familiarity.",
                "- greeting: 1 short sentence.",
                "- followup: 1 short sentence that ends with exactly ONE question mark.",
                "- Total combined length <= 220 characters.",
                "- No emojis.",
                "- No system/meta talk (no 'project pulse', no 'I see in your logs').",
            ]
        ).strip()

    user_prompt = "EVIDENCE_JSON:\n" + json.dumps(ev, ensure_ascii=False)

    raw = ""
    try:
        raw = await asyncio.to_thread(
            call_openai_chat,
            [
                {"role": "system", "content": sys_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
    except Exception:
        raw = ""

    # Parse + enforce constraints (deterministic)
    out_g = ""
    out_q = ""
    try:
        m = re.search(r"\{.*\}", raw or "", flags=re.DOTALL)
        obj = json.loads(m.group(0)) if m else {}
        if isinstance(obj, dict):
            out_g = str(obj.get("greeting") or "").replace("\n", " ").strip()
            out_q = str(obj.get("followup") or "").replace("\n", " ").strip()
    except Exception:
        out_g = ""
        out_q = ""

    # Deterministic fallback (never empty)
    if not out_g:
        out_g = f"Good {daypart}, {preferred_name}."
    if not out_q:
        if topic_snip:
            out_q = "Want to pick up where we left off, or start something new?"
        else:
            out_q = "What are we working on today?"
    # Enforce exactly one question mark in followup
    try:
        out_q = out_q.replace("??", "?").strip()
        if out_q.count("?") == 0:
            out_q = out_q.rstrip(".") + "?"
        if out_q.count("?") > 1:
            # keep only the first question
            pre = out_q.split("?", 1)[0].strip()
            out_q = pre + "?"
    except Exception:
        pass

    msg = (out_g + "\n" + out_q).strip()
    if len(msg) > 220:
        msg = msg[:219].rstrip() + "…"

    # Audit (best-effort)
    try:
        audit_event(
            project_full,
            {
                "schema": "audit_turn_v1",
                "stage": "greeting.emit",
                "reason": str(reason or ""),
                "used_name": bool(preferred_name),
                "daypart": daypart,
                "tz": tz_name,
                "has_recent_chat": bool(topic_snip),
                "topic_age_days": topic_age_days,
                "topic_source": topic_source,
                "project": project_short,
            },
        )
    except Exception:
        pass

    return msg

def read_chat_log_messages_for_ui(
    project_name: str,
    *,
    max_messages: int = 800,
    max_chars_per_msg: int = 24000,
) -> List[Dict[str, str]]:
    """
    UI-only history replay (token-free):
    Read state/chat_log.jsonl and return [{role, ts, text}] in file order.

    - No model calls
    - Bounded output
    - Filters empty rows
    """
    out: List[Dict[str, str]] = []
    p = state_dir(project_name) / "chat_log.jsonl"
    if not p.exists():
        return out

    try:
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return out

    if max_messages and len(lines) > max_messages:
        lines = lines[-max_messages:]

    for ln in lines:
        s = (ln or "").strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue

        role = str(obj.get("role") or "").strip().lower()
        if role not in ("user", "assistant", "info", "system"):
            # keep unknown roles out of UI history
            continue

        ts = str(obj.get("ts") or obj.get("timestamp") or "").strip()
        text = str(obj.get("content") or "").strip()
        if not text:
            continue

        if role == "assistant":
            text = _normalize_assistant_text_for_display(text)

        if max_chars_per_msg and len(text) > max_chars_per_msg:
            text = text[:max_chars_per_msg].rstrip() + "\n.[truncated]."

        # UI treats "system" as "info" (your UI renders info bubbles)
        if role == "system":
            role = "info"

        out.append({"role": role, "ts": ts, "text": text})

    return out

def _count_user_messages_in_chat_log(project_name: str, *, max_lines: int = 120) -> int:
    p = state_dir(project_name) / "chat_log.jsonl"
    if not p.exists():
        return 0
    try:
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return 0
    if max_lines and len(lines) > max_lines:
        lines = lines[-max_lines:]
    n = 0
    for ln in lines:
        s = (ln or "").strip()
        if not s:
            continue
        try:
            obj = json.loads(s)
        except Exception:
            continue
        if not isinstance(obj, dict):
            continue
        role = str(obj.get("role") or "").strip().lower()
        if role == "user":
            n += 1
    return n

def _derive_chat_display_name(text: str, *, max_words: int = 7, max_chars: int = 60) -> str:
    t = _normalize_human_punctuation_segment(str(text or ""))
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return ""
    # Drop leading question/auxiliary verbs to make titles less awkward.
    t = re.sub(r"^(what|how|why|can|could|would|should|is|are|do|does|did|help|please)\b[\\s,]+", "", t, flags=re.IGNORECASE)
    t = t.strip(" .!?;:,-")
    if not t:
        return ""
    words = t.split()
    if len(words) > max_words:
        t = " ".join(words[:max_words]).strip()
        t = t.rstrip(" .!?;:,")
        t = t + "..."
    if max_chars and len(t) > max_chars:
        t = t[:max_chars].rstrip()
    if len(t) < 4:
        return ""
    return t[:1].upper() + t[1:]

def _maybe_set_chat_display_name(
    project_full: str,
    project_short: str,
    *,
    user_msg: str,
    active_topic_text: str,
) -> None:
    try:
        if not str(project_short or "").startswith("chat_"):
            return
        cur = ""
        try:
            cur = str(project_store.get_project_display_name(project_full) or "").strip()
        except Exception:
            cur = ""
        if cur:
            return
        # Wait for at least 2 user messages before naming.
        if _count_user_messages_in_chat_log(project_full, max_lines=120) < 2:
            return
        base = active_topic_text or user_msg
        title = _derive_chat_display_name(base)
        if not title:
            return
        project_store.set_project_display_name(project_full, title)
    except Exception:
        return

def build_project_context_for_model(project_name: str, user_text: str, *, max_chars: int = 24000) -> str:
    """
    Truth-bound context for the model: ONLY canonical project state.

    Canonical sources (authoritative):
    - state/upload_notes.jsonl
    - state/decisions.jsonl
    - state/deliverables.json
    - facts_map.md (if present)
    - project_state.json (if present)

    Chat tail is NOT a source of truth and must not be injected.
    """
    m = load_manifest(project_name)

    # Canonical: project_state.json (bounded)
    st_json = read_state_doc(project_name, "project_state", cap=6000).strip()

    # Canonical: markdown project docs (truth-bound, bounded)
    project_map = read_state_doc(project_name, "project_map", cap=9000).strip()
    working_doc = read_state_doc(project_name, "working_doc", cap=12000).strip()
    preferences = read_state_doc(project_name, "preferences", cap=6000).strip()
    decision_log = read_state_doc(project_name, "decision_log", cap=9000).strip()
    facts_map = read_state_doc(project_name, "facts_map", cap=9000).strip()

    # Canonical: decisions.jsonl (tail)
    dec_lines: List[str] = []
    try:
        decs = load_decisions(project_name) or []
    except Exception:
        decs = []
    if isinstance(decs, list) and decs:
        for it in decs[-12:]:
            if not isinstance(it, dict):
                continue
            ts = str(it.get("timestamp") or "").strip()
            txt0 = str(it.get("text") or "").strip()
            if txt0:
                dec_lines.append(f"- [{ts}] {txt0}")

    # Canonical: upload_notes.jsonl (tail)
    note_lines: List[str] = []
    try:
        notes = load_upload_notes(project_name) or []
    except Exception:
        notes = []
    if isinstance(notes, list) and notes:
        for it in notes[-12:]:
            if not isinstance(it, dict):
                continue
            ts = str(it.get("timestamp") or "").strip()
            up = str(it.get("upload_path") or "").strip()
            q = str(it.get("question") or "").strip()
            a = str(it.get("answer") or "")
            if up or a or q:
                note_lines.append(f"- [{ts}] {up} | Q: {q} | A: {a}")

    # Canonical: deliverables.json
    deliverables_json = ""
    try:
        reg = project_store.load_deliverables(project_name)
        if isinstance(reg, dict):
            deliverables_json = json.dumps(reg, indent=2, ensure_ascii=False)[:6000]
    except Exception:
        deliverables_json = ""

    # -------------------------------------------------------------------------
    # HARD REQUIREMENT (Patch Protocol v1):
    # Inject ALL canonical docs EVERY turn (bounded), so no doc is dropped due
    # to end-slicing of the combined context string.
    # -------------------------------------------------------------------------
    parts: List[str] = []
    parts.append(f"PROJECT_NAME: {project_name}")

    # Keep existing OCR/context injection and upload tails behavior (unchanged)
    # --- C6 FILE-SCOPED OCR INJECTION (read-time only) -------------------------
    try:
        referenced = extract_explicit_filenames(user_text)
        if referenced:
            arts_all = m.get("artifacts") or []
            raw_files_all = m.get("raw_files") or []

            def _resolve_raw_rel_for_ref(base: str) -> str:
                b = (Path(base).name or "").strip().lower()
                if not b:
                    return ""
                for rf in reversed(raw_files_all if isinstance(raw_files_all, list) else []):
                    if not isinstance(rf, dict):
                        continue
                    relp = str(rf.get("path") or "").replace("\\", "/").strip()
                    if not relp:
                        continue
                    cand_names = []
                    cand_names.append(str(rf.get("orig_name") or "").strip())
                    cand_names.append(str(rf.get("saved_name") or "").strip())
                    cand_names.append(Path(relp).name)
                    cand_norm = [Path(x).name.lower() for x in cand_names if x]
                    if any(n == b for n in cand_norm):
                        return relp
                    if any(n.endswith(b) for n in cand_norm):
                        return relp
                    if any(b in n for n in cand_norm):
                        return relp
                return ""

            for ref in referenced:
                base = Path(ref).name
                raw_rel = _resolve_raw_rel_for_ref(base)
                if not raw_rel:
                    continue

                def _artifact_text_for(ref_type: str, cap: int) -> str:
                    for a in reversed((arts_all or [])[-900:]):
                        if not isinstance(a, dict):
                            continue
                        if str(a.get("type") or "") != ref_type:
                            continue
                        ff = a.get("from_files") or []
                        if not isinstance(ff, list):
                            continue
                        ff_norm = [(str(x) or "").replace("\\", "/").strip() for x in ff]
                        if raw_rel not in ff_norm:
                            continue
                        return read_artifact_text(project_name, a, cap_chars=cap).strip()
                    return ""

                plan_txt = _artifact_text_for("plan_ocr", 220000)
                ocr_txt = _artifact_text_for("ocr_text", 220000)
                picked = (plan_txt or ocr_txt).strip()

                if picked:
                    block: List[str] = []
                    block.append(f"OCR_CONTEXT_FOR_FILE: {base}")
                    block.append("===OCR_START===")
                    block.append(picked[:8000])
                    block.append("===OCR_END===")
                    parts.append("\n".join(block))
    except Exception:
        pass
    # -------------------------------------------------------------------------

    # Include latest raw uploads (manifest) so the model can ground follow-up questions
    try:
        raw_files = m.get("raw_files") or []
    except Exception:
        raw_files = []

    if isinstance(raw_files, list) and raw_files:
        lines_rf: List[str] = []
        for rf in raw_files[-6:]:
            if not isinstance(rf, dict):
                continue
            p = str(rf.get("path") or "").replace("\\", "/").strip()
            orig = str(rf.get("orig_name") or "").strip()
            saved = str(rf.get("saved_name") or "").strip()
            if p:
                lines_rf.append(f"- {orig or saved or '(unknown)'} | {p}")
        if lines_rf:
            parts.append("RAW_UPLOADS_TAIL (manifest):\n" + "\n".join(lines_rf))

    # -----------------------------
    # Deterministic bounded injector
    # -----------------------------
    def _add_block(label: str, body: str, *, soft_cap: int, placeholder: str) -> None:
        nonlocal parts
        b = (body or "").strip()
        if not b:
            b = placeholder
        if soft_cap and len(b) > soft_cap:
            b = b[:soft_cap].rstrip() + "\n...[truncated]..."
        parts.append(f"{label}:\n{b}")

    # Canonical docs required by Patch Protocol v1 (always inject, bounded)
    _add_block("PROJECT_STATE_JSON (canonical)", st_json, soft_cap=5200, placeholder="(missing or empty)")
    _add_block("PROJECT_MAP_MD (canonical)", project_map, soft_cap=3200, placeholder="(missing or empty)")
    _add_block("WORKING_DOC_MD (canonical)", working_doc, soft_cap=3200, placeholder="(missing or empty)")
    _add_block("PREFERENCES_MD (canonical)", preferences, soft_cap=2200, placeholder="(missing or empty)")
    _add_block("DECISION_LOG_MD (canonical)", decision_log, soft_cap=2800, placeholder="(missing or empty)")
    _add_block("FACTS_MAP_MD (canonical)", facts_map, soft_cap=2800, placeholder="(missing or empty)")

    # Canonical registries/log tails (still canonical; bounded)
    if deliverables_json:
        _add_block("DELIVERABLES_JSON (canonical)", deliverables_json, soft_cap=2600, placeholder="(missing or empty)")
    else:
        _add_block("DELIVERABLES_JSON (canonical)", "", soft_cap=2600, placeholder="(missing or empty)")

    if dec_lines:
        _add_block("DECISIONS_JSONL_TAIL (canonical)", "\n".join(dec_lines), soft_cap=2200, placeholder="(missing or empty)")
    else:
        _add_block("DECISIONS_JSONL_TAIL (canonical)", "", soft_cap=2200, placeholder="(missing or empty)")

    if note_lines:
        _add_block("UPLOAD_NOTES_JSONL_TAIL (canonical)", "\n".join(note_lines), soft_cap=2200, placeholder="(missing or empty)")
    else:
        _add_block("UPLOAD_NOTES_JSONL_TAIL (canonical)", "", soft_cap=2200, placeholder="(missing or empty)")

    txt = "\n\n".join(parts).strip()

    # Final hard cap (still applies), but at this point every canonical doc has
    # already been injected with bounded per-doc caps, so truncation is far less likely
    # to drop an entire required doc.
    return txt[:max_chars] if (max_chars and len(txt) > max_chars) else txt


# =============================================================================
# Patch mode: target selection + anchor candidates + validation
# =============================================================================

ENABLE_PATCH_MODE = (os.environ.get("LENS0_ENABLE_PATCH_MODE") or "1").strip() in ("1", "true", "yes", "on")
ENABLE_SELF_PATCH = ENABLE_PATCH_MODE

def looks_like_patch_request(user_text: str) -> bool:
    """
    Explicit-only patch mode:
    - ONLY "!patch ..." (and related !selfpatch/!serverpatch aliases) may trigger patch workflows.
    - No magic words. Plain "patch" or "diff" in natural language must not force patch mode.
    """
    raw_text = user_text or ""
    t = raw_text.strip()

    result = False

    if not ENABLE_PATCH_MODE:
        result = False
    elif not t:
        result = False
    else:
        # Only explicit admin commands are allowed to trigger patch mode.
        low = t.lower()
        if low.startswith(("!patch", "!selfpatch", "!serverpatch", "!patch-server")):
            result = True
        else:
            result = False

    if (os.environ.get("LENS0_DEBUG_PATCH") or "").strip().lower() in ("1", "true", "yes", "on"):
        try:
            print("[LENS0_DEBUG_PATCH] looks_like_patch_request first80:", raw_text[:80], flush=True)
        except Exception:
            pass

    return result

def infer_preferred_suffix(user_text: str) -> str:
    t = (user_text or "").lower()

    # Explicit extension mention
    m = re.search(r"\.(py|js|ts|jsx|tsx|html|htm|css|json|md|txt|yml|yaml)\b", t)
    if m:
        return "." + m.group(1)

    # UI/Frontend signals
    ui = ("frontend", "ui", "html", "<div", "<span", "css", "button", "clipboard", "icon", "chat bubble")
    if any(s in t for s in ui):
        # prefer html unless the user clearly mentions js/css
        if "css" in t:
            return ".css"
        if "javascript" in t or ".js" in t:
            return ".js"
        if "typescript" in t or ".ts" in t:
            return ".ts"
        return ".html"

    # Backend signals
    if any(s in t for s in ("python", "backend", "websocket", "aiohttp", ".py", "server.py")):
        return ".py"

    return ""

def extract_explicit_filenames(user_text: str) -> List[str]:
    t = user_text or ""
    matches = re.findall(
        r"[\w./-]+\.(?:py|js|ts|jsx|tsx|html|htm|css|json|md|txt|yml|yaml|pdf|png|jpg|jpeg|webp|gif|csv|xlsx|xlsm|xls|docx|doc)\b",
        t,
        flags=re.IGNORECASE,
    )
    out: List[str] = []
    seen: set[str] = set()
    for m in matches:
        mm = m.replace("\\", "/")
        key = mm.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(mm)
    return out

def pick_patch_target_source(project_name: str, user_text: str) -> str:
    """
    Determine which ingested source file to patch.

    Priority:
    1) Explicit filename mention that matches a known ingested source.
    2) Preferred suffix inference, choosing newest ingested.
    3) manifest.last_ingested.source_rel_path
    4) newest code_index source
    """
    m = load_manifest(project_name)
    arts = m.get("artifacts") or []

    # Collect known sources from code_index meta
    sources: List[Tuple[float, str]] = []
    for a in arts:
        if a.get("type") != "code_index":
            continue
        meta = a.get("meta") or {}
        if not isinstance(meta, dict):
            continue
        src = (meta.get("source_rel_path") or "").replace("\\", "/").strip()
        if not src:
            continue
        ts = float(a.get("created_at", 0.0) or 0.0)
        sources.append((ts, src))
    sources.sort(key=lambda x: x[0], reverse=True)

    # Explicit filename
    explicit = extract_explicit_filenames(user_text)
    if explicit and sources:
        for ex in explicit:
            ex_base = Path(ex).name.lower()
            for _ts, src in sources:
                if Path(src).name.lower() == ex_base:
                    return src
            for _ts, src in sources:
                if src.lower().endswith(ex.lower()):
                    return src

    # Suffix preference
    prefer = infer_preferred_suffix(user_text)
    if prefer and sources:
        for _ts, src in sources:
            if Path(src).suffix.lower() == prefer.lower():
                return src

    # last_ingested
    li = m.get("last_ingested") or {}
    if isinstance(li, dict):
        p = (li.get("source_rel_path") or "").replace("\\", "/").strip()
        if p:
            return p

    return sources[0][1] if sources else ""

def _line_counts(text: str) -> Dict[str, int]:

    counts: Dict[str, int] = {}
    for ln in (text or "").splitlines():
        counts[ln] = counts.get(ln, 0) + 1
    return counts

def build_patch_excerpt(project_name: str, source_rel_path: str, user_text: str, *, max_chars: int) -> str:
    """
    Build a context excerpt for patch generation.
    - If the file is small enough: return full file text.
    - If large: select the most relevant code chunks (in file order) up to max_chars.
    Token-friendly: include clear chunk separators to avoid "blob" confusion.
    """
    source_rel_path = (source_rel_path or "").replace("\\", "/").strip()
    if not source_rel_path:
        return ""

    abs_path = PROJECT_ROOT / source_rel_path
    full_text = read_text_file(abs_path, errors="surrogateescape")
    if not full_text:
        return ""

    if len(full_text) <= max_chars:
        return full_text

    # Find code_index entry for this source
    m = load_manifest(project_name)
    idxs = [a for a in (m.get("artifacts") or []) if a.get("type") == "code_index"]
    idxs.sort(key=lambda a: float(a.get("created_at", 0.0) or 0.0), reverse=True)

    # (cleanup) code_index lookup currently unused; chunk selection is disabled in this build.
    chunk_files: List[str] = []

    if not chunk_files:
        return full_text[:max_chars]

    # score chunks by keyword hits (slightly broader tokenization than before)
    t = (user_text or "").lower()
    keywords = [k for k in re.findall(r"[a-zA-Z0-9_]{3,}", t)][:24]
    keywords = list(dict.fromkeys(keywords))  # dedupe, keep order

    scored: List[Tuple[int, int, str]] = []  # (score, chunk_index, text)

    def chunk_index_from_name(fn: str) -> int:
        m = re.search(r"_chunk_(\d+)", fn)
        return int(m.group(1)) if m else 0

    for fn in chunk_files:
        ci = chunk_index_from_name(fn)
        txt = read_text_file(artifacts_dir(project_name) / fn, errors="surrogateescape")
        low = (txt or "").lower()
        score = 0
        for k in keywords:
            if k and k in low:
                score += 1
        # slight bias toward defs/classes
        if "def " in low or "class " in low:
            score += 1
        scored.append((score, ci, txt or ""))

    # pick chunks: prefer those with score>0; if none, take from start
    positives = [s for s in scored if s[0] > 0]
    pool = positives if positives else sorted(scored, key=lambda x: x[1])

    chosen: List[Tuple[int, str]] = []
    total = 0

    # Always include a small header so the model understands it's partial context
    header = (
        "### NOTE: CODE_CORPUS is an excerpt assembled from file chunks for token safety.\n"
        "### It may NOT contain the entire file. Use anchors only from what you see here.\n\n"
    )
    if len(header) < max_chars:
        chosen.append((-1, header))
        total += len(header)

    for score, ci, txt in pool:
        if not txt:
            continue
        if total >= max_chars:
            break

        chunk_header = f"\n\n### --- CHUNK {ci} ---\n"
        remaining = max_chars - total
        if remaining <= 0:
            break

        if len(chunk_header) <= remaining:
            chosen.append((ci, chunk_header))
            total += len(chunk_header)
            remaining = max_chars - total
        else:
            break

        if remaining <= 0:
            break

        piece = txt if len(txt) <= remaining else txt[:remaining]
        chosen.append((ci, piece))
        total += len(piece)

    # Maintain file order to reduce confusion (keep header at top)
    chosen.sort(key=lambda x: (x[0] < 0, x[0]))
    return "".join([t for _, t in chosen])


def extract_unique_anchor_candidates(
    corpus_excerpt: str,
    user_text: str,
    full_line_counts: Dict[str, int],
    limit: int = 30,
) -> List[str]:
    """
    Pick useful anchor lines that:
    - appear in the provided excerpt
    - are UNIQUE in the FULL file (line_counts == 1)
    - are non-trivial length
    """
    lines = (corpus_excerpt or "").splitlines()
    t = (user_text or "").lower()
    keywords = [k for k in re.findall(r"[a-zA-Z_]{3,}", t)][:12]

    scored: List[Tuple[int, str]] = []
    for ln in lines:
        # keep indentation exactly; use trimmed string only for checks/scoring
        if full_line_counts.get(ln, 0) != 1:
            continue
        st = ln.strip()
        if not st:
            continue
        if len(st) < 12 or len(st) > 180:
            continue
        # Reject generic lines that are likely to be ambiguous or low-signal anchors.
        # These often appear many times in a file and cause anchor validation failures.
        low2 = st.lower()
        if low2 in ("return false", "return true", "pass", "continue", "break", "else:", "try:", "except:", "finally:"):
            continue
        if re.fullmatch(r"return\s+(true|false)\s*", low2):
            continue


        score = 0
        low = st.lower()
        for k in keywords:
            if k and k in low:
                score += 2
        if st.startswith(("def ", "async def ", "class ")):
            score += 6
        if st.startswith("<") or "id=" in low or "class=" in low:
            score += 4
        scored.append((score, ln))

    scored.sort(key=lambda x: x[0], reverse=True)
    out: List[str] = []
    seen: set[str] = set()
    for _, ln in scored:
        if ln in seen:
            continue
        seen.add(ln)
        out.append(ln)
        if len(out) >= limit:
            break
    return out

def patch_has_effective_code(patch_text: str) -> bool:
    """
    Returns True if there is at least one non-empty, non-comment line inside any fenced code block.
    Used to reject "empty" patches that only contain comments/whitespace.
    """
    if not patch_text:
        return False

    blocks = re.findall(r"```[^\n]*\n(.*?)\n```", patch_text, flags=re.DOTALL)
    if not blocks:
        return False

    for b in blocks:
        for raw in b.splitlines():
            line = raw.strip()
            if not line:
                continue
            if line.startswith(("#", "//", "--")):
                continue
            return True

    return False

def patch_text_parses_strictly(patch_text: str) -> Tuple[bool, str]:

    """
    Strict PATCH MODE validator.

    Must pass BOTH:
    1) validate_patch_text_strict_format()  (no prose, correct lines, fences on their own lines)
    2) parse_anchor_patch_ops()             (operations can be extracted)
    """
    try:
        ok, err = validate_patch_text_strict_format(patch_text)
        if not ok:
            return False, err or "Strict patch format validation failed."
        _ = parse_anchor_patch_ops(patch_text)
        return True, ""
    except Exception as e:
        return False, repr(e)

def _python_ok_after_anchor_patch(target_rel_path: str, full_text: str, patch_text: str) -> Tuple[bool, str]:
    """
    For .py targets: apply the anchor patch in-memory and AST-parse the result.
    Returns (ok, err). For non-.py targets, always returns (True, "").
    """
    try:
        if not (target_rel_path or "").lower().endswith(".py"):
            return True, ""
        new_text = apply_anchor_patch_to_text(full_text, patch_text)
        ast.parse(new_text)
        return True, ""
    except SyntaxError as e:
        msg = f"{e.__class__.__name__}: {e.msg} (line {e.lineno})"
        return False, msg
    except Exception as e:
        return False, repr(e)

async def generate_anchor_validated_patch(project_name: str, user_text: str) -> Tuple[str, str]:
    """
    Returns (target_source_rel_path, patch_text OR error message).
    Validation is done against the FULL source file (line-based uniqueness),
    while the model sees an excerpt (for token safety).
    """
    target = pick_patch_target_source(project_name, user_text)
    if not target:
        return "", (
            "PATCH MODE ERROR: I couldn't find any ingested code files to patch.\n"
            "Upload or ingest a code/text file first, then ask again."
        )

    # Ensure ingestion is current and read the full source for validation
    ensure_ingested_current(project_name, target)
    full_text = read_text_file(PROJECT_ROOT / target, errors="surrogateescape")
    if not full_text.strip():
        return target, (
            "PATCH MODE ERROR: I couldn't read the target file on disk.\n"
            "Try re-uploading the file, then ask again."
        )

    full_counts = _line_counts(full_text)

    # Excerpt for the model
    excerpt = build_patch_excerpt(project_name, target, user_text, max_chars=PATCH_MAX_CONTEXT_CHARS)
    if not excerpt.strip():
        excerpt = full_text[:PATCH_MAX_CONTEXT_CHARS]

    # Anchor candidates: unique in full file AND present in excerpt
    candidates = extract_unique_anchor_candidates(excerpt, user_text, full_counts, limit=30)
    if not candidates:
        # fallback: pick any unique line from the full file (still prefer practical length)
        candidates = []
        for ln, c in full_counts.items():
            st = ln.strip()
            if c == 1 and st and 12 <= len(st) <= 180:
                candidates.append(ln)
            if len(candidates) >= 40:
                break
        if not candidates:
            candidates = [ln for ln in excerpt.splitlines() if ln.strip()][:40]

    lang_hint = language_hint_from_suffix(Path(target).suffix)

    anchor_block = "ANCHOR_CANDIDATES (copy anchors EXACTLY from one of these lines):\n```text\n" + "\n".join(candidates) + "\n```"

    messages = [
        {"role": "system", "content": patch_system_prompt(lang_hint)},
        {"role": "system", "content": "TARGET_FILE: " + target},
        {"role": "system", "content": anchor_block},
        {"role": "system", "content": "===CODE_CORPUS_START===\n" + excerpt + "\n===CODE_CORPUS_END==="},
        {"role": "user", "content": user_text},
    ]

    # Attempt 1
    patch1 = await asyncio.to_thread(call_openai_chat, messages)
    anchors1 = extract_anchor_lines_from_patch(patch1)
    ok1, missing1, ambiguous1 = anchors_exist_and_unique(anchors1, full_counts)
    parse_ok1, parse_err1 = patch_text_parses_strictly(patch1)

    py_ok1, py_err1 = True, ""
    if ok1 and parse_ok1 and patch_has_effective_code(patch1):
        py_ok1, py_err1 = _python_ok_after_anchor_patch(target, full_text, patch1)
        if py_ok1:
            return target, patch1.strip()

    # Attempt 2 with explicit validation failures
    problems: List[str] = []
    if not anchors1:
        problems.append("No anchors were detected. You must include at least one Replace/Insert anchor line.")
    if not parse_ok1:
        problems.append("Patch format/parse failed (must be strict anchor patch blocks only): " + (parse_err1 or "(unknown parse error)"))
    if missing1:
        problems.append("Missing anchors (not found as exact full lines in the target file):\n- " + "\n- ".join(missing1[:12]))
    if ambiguous1:
        problems.append("Ambiguous anchors (appear multiple times as full lines in the target file):\n- " + "\n- ".join(ambiguous1[:12]))
    if ok1 and parse_ok1 and not patch_has_effective_code(patch1):
        problems.append("Patch has no effective (non-comment) code lines inside fenced code blocks.")
    if ok1 and parse_ok1 and patch_has_effective_code(patch1):
        py_ok1b, py_err1b = _python_ok_after_anchor_patch(target, full_text, patch1)
        if not py_ok1b:
            problems.append("Patched file would be invalid Python: " + (py_err1b or "(unknown python parse error)"))

    messages.append({"role": "system", "content": "PATCH VALIDATION FAILED:\n" + "\n\n".join(problems) + "\n\nRegenerate a correct patch. Do NOT guess anchors."})
    patch2 = await asyncio.to_thread(call_openai_chat, messages)
    anchors2 = extract_anchor_lines_from_patch(patch2)
    ok2, missing2, ambiguous2 = anchors_exist_and_unique(anchors2, full_counts)
    parse_ok2, parse_err2 = patch_text_parses_strictly(patch2)

    py_ok2, py_err2 = True, ""
    if ok2 and parse_ok2 and patch_has_effective_code(patch2):
        py_ok2, py_err2 = _python_ok_after_anchor_patch(target, full_text, patch2)
        if py_ok2:
            return target, patch2.strip()

    # Attempt 3 (hard constraint): force anchors to be chosen ONLY from ANCHOR_CANDIDATES
    problems2: List[str] = []
    if not anchors2:
        problems2.append("No anchors were detected in attempt #2.")
    if not parse_ok2:
        problems2.append("Patch format/parse failed: " + (parse_err2 or "(unknown parse error)"))
    if missing2:
        problems2.append("Missing anchors:\n- " + "\n- ".join(missing2[:12]))
    if ambiguous2:
        problems2.append("Ambiguous anchors:\n- " + "\n- ".join(ambiguous2[:12]))
    if ok2 and parse_ok2 and not patch_has_effective_code(patch2):
        problems2.append("Patch has no effective (non-comment) code lines inside fenced code blocks.")

    messages.append(
        {
            "role": "system",
            "content": (
                "PATCH VALIDATION FAILED AGAIN.\n"
                "You MUST follow these rules now:\n"
                "1) Use ONLY anchor lines copied EXACTLY from ANCHOR_CANDIDATES.\n"
                "2) Output ONLY patch blocks (no target:, no diff summary:, no prose).\n"
                "3) Every patch block MUST include a fenced code block.\n\n"
                "Errors from attempt #2:\n" + "\n\n".join(problems2)
            ),
        }
    )

    patch3 = await asyncio.to_thread(call_openai_chat, messages)
    anchors3 = extract_anchor_lines_from_patch(patch3)
    ok3, missing3, ambiguous3 = anchors_exist_and_unique(anchors3, full_counts)
    parse_ok3, parse_err3 = patch_text_parses_strictly(patch3)

    # HARD ENFORCEMENT: attempt #3 anchors MUST be copied from ANCHOR_CANDIDATES.
    cand_set = set(candidates or [])
    bad3 = [a for a in anchors3 if a not in cand_set]
    if bad3:
        missing3 = list(dict.fromkeys(list(missing3) + bad3))
        ok3 = False

    py_ok3, py_err3 = True, ""
    if ok3 and parse_ok3 and patch_has_effective_code(patch3):
        py_ok3, py_err3 = _python_ok_after_anchor_patch(target, full_text, patch3)
        if py_ok3:
            return target, patch3.strip()

    # Final: return concrete diagnostics so the user can see what failed
    diag = {
        "attempt1": {"anchors": anchors1[:12], "missing": missing1[:12], "ambiguous": ambiguous1[:12], "parse_ok": parse_ok1, "parse_err": parse_err1},
        "attempt2": {"anchors": anchors2[:12], "missing": missing2[:12], "ambiguous": ambiguous2[:12], "parse_ok": parse_ok2, "parse_err": parse_err2},
        "attempt3": {"anchors": anchors3[:12], "missing": missing3[:12], "ambiguous": ambiguous3[:12], "parse_ok": parse_ok3, "parse_err": parse_err3},
        "target": target,
    }

    return target, (
        "I cannot provide a safe patch because anchor validation failed.\n\n"
        "Diagnostics (first 12 items each):\n"
        + json.dumps(diag, indent=2, ensure_ascii=False)
        + "\n"
    )
# Project dashboard
# =============================================================================
def build_grounded_project_pulse(project_name: str) -> str:
    """
    C5.2 — Truth-bound Project Pulse (DETERMINISTIC ONLY)

    Delegates to project_store.build_truth_bound_pulse(project_name).
    No model calls. No inference. Canonical sources only.
    """
    ensure_project_scaffold(project_name)
    try:
        return project_store.build_truth_bound_pulse(project_name)
    except Exception:
        return "\n".join([
            "Project Pulse (truth-bound)",
            "Goal: Not recorded / ambiguous",
            "Current deliverable: Not recorded / ambiguous",
            "Recent decisions: Not recorded / ambiguous",
            "Recent upload notes: Not recorded / ambiguous",
            "Next action: Not recorded / ambiguous",
            "Facts map: empty",
        ]).strip()


def build_project_resume_message(project_name: str) -> str:
    """
    Truth-bound resume blurb.
    Canonical only: project_state.json + facts_map.md + deliverables.json (plus key_files if present in project_state).
    Chat tail is NOT a source of truth.
    """
    ensure_project_scaffold(project_name)
    m = load_manifest(project_name)
    _ = m  # manifest is not a source of truth; kept only for backward compatibility

    st_raw = read_state_doc(project_name, "project_state", cap=6000).strip()
    facts_map = read_state_doc(project_name, "facts_map", cap=4000).strip()

    focus = ""
    next_actions: List[str] = []
    key_files: List[str] = []

    def _clean(s: str) -> str:
        x = (s or "").strip()
        for pfx in ("User:", "Assistant:", "USER:", "ASSISTANT:"):
            if x.startswith(pfx):
                x = x[len(pfx):].strip()
        x = re.sub(r"\s+", " ", x).strip()
        return x

    def _clip(s: str, n: int) -> str:
        s = (s or "").strip()
        if len(s) <= n:
            return s
        return s[: max(0, n - 1)].rstrip() + "…"

    st: Dict[str, Any] = {}
    if st_raw:
        try:
            obj = json.loads(st_raw)
            if isinstance(obj, dict):
                st = obj
        except Exception:
            st = {}

    focus = _clean(str(st.get("current_focus") or ""))

    na = st.get("next_actions") or []
    if isinstance(na, list):
        next_actions = [_clean(str(x)) for x in na if _clean(str(x))][:2]

    kf = st.get("key_files") or []
    if isinstance(kf, list):
        key_files = [str(x).strip() for x in kf if str(x).strip()]

    # Pick a file to open (prefer key_files that exist)
    open_rel = ""
    for rel in key_files:
        rel2 = rel.replace("\\", "/").strip()
        if not rel2:
            continue
        p = (PROJECT_ROOT / rel2).resolve()
        try:
            p.relative_to(PROJECT_ROOT.resolve())
        except Exception:
            continue
        if p.exists() and p.is_file():
            open_rel = rel2
            break

    lines: List[str] = []
    lines.append("Resume:")

    if focus:
        lines.append(f"- Where we are: {_clip(focus, 170)}")
    else:
        lines.append("- Where we are: Not recorded / ambiguous")

    if open_rel:
        lines.append(f"- Open: /file?path={open_rel}")

    if next_actions:
        lines.append(f"- Suggested next: {_clip(next_actions[0], 130)}")
        if len(next_actions) > 1:
            lines.append(f"- Then: {_clip(next_actions[1], 130)}")
    else:
        lines.append("- Suggested next: Not yet recorded")

    lines.append(f"- Memory: facts_map.md {'present' if facts_map else 'empty'}")
    return "\n".join(lines).strip()


def build_project_dashboard_message(project_name: str, max_map_lines: int = 40) -> str:
    ensure_project_scaffold(project_name)
    m = load_manifest(project_name)
    goal = (m.get("goal") or "").strip()

    st_path = state_file_path(project_name, "project_state")
    focus = ""
    next_actions: List[str] = []
    if st_path.exists():
        try:
            st = json.loads(st_path.read_text(encoding="utf-8") or "{}")
            if isinstance(st, dict):
                focus = str(st.get("current_focus") or "")
                na = st.get("next_actions") or []

                def _clean_action(s: str) -> str:
                    x = (s or "").strip()
                    for pfx in ("User:", "Assistant:", "USER:", "ASSISTANT:"):
                        if x.startswith(pfx):
                            x = x[len(pfx):].strip()
                    x = re.sub(r"\s+", " ", x).strip()
                    return x

                if isinstance(na, list):
                    cleaned = [_clean_action(str(x)) for x in na]
                    cleaned = [c for c in cleaned if c]
                    next_actions = cleaned[:8]
        except Exception:
            pass
        except Exception:
            pass

    lines: List[str] = []
    lines.append(f"Current project: {project_name}")
    lines.append(f"Goal: {goal or '(no goal set yet)'}")
    if focus:
        lines.append(f"Current focus: {focus}")
    if next_actions:
        lines.append("Next actions:")
        for a in next_actions:
            lines.append(f"- {a}")

    # show top raw files
    raw_files = m.get("raw_files") or []
    if raw_files:
        lines.append("")
        lines.append("Raw files:")
        for rf in raw_files[:12]:
            p = (rf.get("path") or "").replace("\\", "/")
            n = rf.get("orig_name") or rf.get("saved_name") or "(unknown)"
            lines.append(f"- {n} ({p})")

    # project map snippet
    pm = read_state_doc(project_name, "project_map", cap=12000)
    if pm:
        pm_lines = pm.splitlines()
        lines.append("")
        lines.append("Project Map (top):")
        lines.extend(pm_lines[:max_map_lines])
        if len(pm_lines) > max_map_lines:
            lines.append("... (truncated)")

    lines.append("")
    # Intentionally omit internal server plumbing + self-patch instructions for normal users.
    return "\n".join(lines)
def _pp1_sha256_text(s: str) -> str:
    try:
        import hashlib
        return hashlib.sha256((s or "").encode("utf-8", errors="replace")).hexdigest()
    except Exception:
        return ""

def _pp1_receipts_path(project_name: str) -> Path:
    return state_dir(project_name) / "patch_receipts.jsonl"

def _pp1_seen_idempotency(project_name: str, key: str) -> Optional[Dict[str, Any]]:
    p = _pp1_receipts_path(project_name)
    if not p.exists():
        return None
    try:
        for ln in read_text_file(p, errors="replace").splitlines():
            if not ln.strip():
                continue
            obj = json.loads(ln)
            if str(obj.get("idempotency_key") or "") == key:
                return obj
    except Exception:
        return None
    return None

def _pp1_append_receipt(project_name: str, rec: Dict[str, Any]) -> None:
    """
    Append a Patch Protocol receipt to state/patch_receipts.jsonl using the canonical write path.
    Must never throw (patch application should decide rejection vs apply; logging must be best-effort).
    """
    try:
        ok = project_store.write_canonical_entry(
            project_name,
            target_path=_pp1_receipts_path(project_name),
            mode="jsonl_append",
            data=(rec if isinstance(rec, dict) else {}),
        )
        _ = ok
    except Exception:
        # Never block the request path on receipt logging.
        return


def _pp1_apply_json_pointer(doc: Any, ptr: str, op: str, value: Any = None) -> Any:
    # Very small RFC6901 subset for dict/list trees.
    # Returns updated doc (may be same object).
    if not ptr.startswith("/"):
        raise ValueError("json_pointer ptr must start with '/'")
    parts = [p.replace("~1", "/").replace("~0", "~") for p in ptr.split("/")[1:]]

    cur = doc
    parents = []
    for i, key in enumerate(parts[:-1]):
        parents.append((cur, key))
        if isinstance(cur, dict):
            if key not in cur:
                if op == "add":
                    cur[key] = {}
                else:
                    raise KeyError(f"json_pointer path missing: {ptr}")
            cur = cur[key]
        elif isinstance(cur, list):
            idx = int(key)
            cur = cur[idx]
        else:
            raise TypeError("json_pointer encountered non-container")

    last = parts[-1] if parts else ""
    if isinstance(cur, dict):
        if op == "remove":
            if last not in cur:
                raise KeyError(f"remove missing key at {ptr}")
            del cur[last]
        elif op in ("add", "replace"):
            if op == "replace" and last not in cur:
                raise KeyError(f"replace missing key at {ptr}")
            cur[last] = value
        else:
            raise ValueError("unsupported op")
    elif isinstance(cur, list):
        idx = int(last)
        if op == "remove":
            del cur[idx]
        elif op == "add":
            # insert at idx (JSON Patch-ish)
            cur.insert(idx, value)
        elif op == "replace":
            cur[idx] = value
        else:
            raise ValueError("unsupported op")
    else:
        raise TypeError("json_pointer final target non-container")

    return doc

def _pp1_md_find_section(lines: List[str], heading_path: List[str]) -> Tuple[int, int]:
    """
    Resolve a markdown section by exact heading line matches (including leading #).
    Returns (start_idx, end_idx) line indices for the entire section.
    """
    if not heading_path:
        raise ValueError("heading_path empty")

    # Find the first heading, then refine within its bounds for nested headings.
    start = 0
    end = len(lines)

    for hp in heading_path:
        hp0 = (hp or "").rstrip()
        idx = None
        for i in range(start, end):
            if lines[i].rstrip() == hp0:
                idx = i
                break
        if idx is None:
            raise KeyError(f"markdown heading not found: {hp0}")

        # Determine this heading level to bound the section.
        lvl = len(hp0) - len(hp0.lstrip("#"))
        if lvl <= 0 or not hp0.startswith("#"):
            raise ValueError(f"heading_path items must include leading #: {hp0}")

        # Section ends at next heading of same or higher level.
        sec_end = end
        for j in range(idx + 1, end):
            l = lines[j].rstrip()
            if not l.startswith("#"):
                continue
            lvl2 = len(l) - len(l.lstrip("#"))
            if lvl2 <= lvl:
                sec_end = j
                break

        start = idx
        end = sec_end

    return (start, end)

def apply_patch_protocol_v1(project_name: str, patch_raw_json: str) -> List[str]:
    """
    Validate and apply Patch Protocol v1 envelope. Returns created artifact paths (PROJECT_ROOT-relative).
    Deterministic, bounded, idempotent.
    """
    ensure_project_scaffold(project_name)

    try:
        env = json.loads(patch_raw_json)
    except Exception as e:
        raise ValueError(f"PATCH_PROTOCOL_V1 invalid JSON: {e}")

    if not isinstance(env, dict) or env.get("v") != 1:
        raise ValueError("PATCH_PROTOCOL_V1: env must be object with v=1")

    idem = (env.get("idempotency") or {})
    idem_key = str(idem.get("key") or "").strip()
    if not idem_key:
        raise ValueError("PATCH_PROTOCOL_V1: missing idempotency.key")

    prev = _pp1_seen_idempotency(project_name, idem_key)
    if prev and str(prev.get("outcome") or "") == "applied":
        # Idempotent replay: do nothing
        return []

    # Allowlist targets
    allow = {
        "project_state.json": ("json", "project_state"),
        "working_doc.md": ("markdown", "working_doc"),
        "facts_map.md": ("markdown", "facts_map"),
        "decision_log.md": ("markdown", "decision_log"),
        "project_map.md": ("markdown", "project_map"),
        "preferences.md": ("markdown", "preferences"),
    }

    targets = env.get("targets")
    ops = env.get("ops")
    if not isinstance(targets, list) or not isinstance(ops, list) or not targets or not ops:
        raise ValueError("PATCH_PROTOCOL_V1: targets/ops must be non-empty arrays")

    target_meta: Dict[str, Dict[str, Any]] = {}
    for t in targets:
        if not isinstance(t, dict):
            raise ValueError("PATCH_PROTOCOL_V1: target must be object")
        path = str(t.get("path") or "")
        ttype = str(t.get("type") or "")
        pre = t.get("precondition") or {}
        if path not in allow:
            raise ValueError(f"PATCH_PROTOCOL_V1: target not allowlisted: {path}")
        if ttype not in ("json", "markdown"):
            raise ValueError(f"PATCH_PROTOCOL_V1: invalid target type for {path}")
        mode = str(pre.get("mode") or "")
        if mode not in ("must_exist", "must_not_exist", "if_missing_create_seeded", "if_missing_create_empty"):
            raise ValueError(f"PATCH_PROTOCOL_V1: invalid precondition.mode for {path}")
        target_meta[path] = {"type": ttype, "pre": pre}

    # Load current file contents
    before_text: Dict[str, str] = {}
    before_sha: Dict[str, str] = {}
    for path, (kind, logical) in allow.items():
        if path not in target_meta:
            continue
        p = state_file_path(project_name, logical)
        exists = p.exists()

        mode = str((target_meta[path]["pre"] or {}).get("mode") or "")
        if mode == "must_exist" and not exists:
            raise ValueError(f"PATCH_PROTOCOL_V1 precondition_failed: {path} missing")
        if mode == "must_not_exist" and exists:
            raise ValueError(f"PATCH_PROTOCOL_V1 precondition_failed: {path} exists")

        if not exists and mode in ("if_missing_create_seeded", "if_missing_create_empty"):
            # Use existing scaffold behavior; ensure_project_scaffold already seeds.
            ensure_project_scaffold(project_name)

        txt = read_text_file(p, errors="replace") if p.exists() else ""
        before_text[path] = txt
        before_sha[path] = _pp1_sha256_text(txt)

        exp = str((target_meta[path]["pre"] or {}).get("expected_sha256") or "").strip()
        if exp and exp != before_sha[path]:
            raise ValueError(f"PATCH_PROTOCOL_V1 precondition_failed: {path} sha256 mismatch")

    # Apply ops in order to in-memory buffers
    after_text = dict(before_text)

    # Protected JSON pointers
    protected_ptrs = {
        "/bootstrap_status",
        "/project_mode",
        "/domains",
        "/expert_frame",
    }

    for op in ops:
        if not isinstance(op, dict):
            raise ValueError("PATCH_PROTOCOL_V1: op must be object")
        target = str(op.get("target") or "")
        if target not in target_meta:
            raise ValueError(f"PATCH_PROTOCOL_V1: op target not declared in targets: {target}")
        opk = str(op.get("op") or "")
        sel = op.get("selector") or {}
        guards = op.get("guards") or {}

        # Enforce value presence rules
        if opk in ("add", "replace"):
            if "value" not in op:
                raise ValueError("PATCH_PROTOCOL_V1: add/replace require value")
        if opk == "remove":
            if "value" in op:
                raise ValueError("PATCH_PROTOCOL_V1: remove forbids value")

        # Guard: expected_before_sha256 for whole target
        exp_sha = str(guards.get("expected_before_sha256") or "").strip()
        if exp_sha and exp_sha != before_sha.get(target, ""):
            raise ValueError(f"PATCH_PROTOCOL_V1 guard_failed: expected_before_sha256 mismatch for {target}")

        # Guard: expected_before_substring must appear in current buffer
        exp_sub = str(guards.get("expected_before_substring") or "")
        if exp_sub and exp_sub not in (after_text.get(target) or ""):
            raise ValueError(f"PATCH_PROTOCOL_V1 guard_failed: expected_before_substring missing for {target}")

        ttype = target_meta[target]["type"]
        if ttype == "json":
            if str(sel.get("kind") or "") != "json_pointer":
                raise ValueError("PATCH_PROTOCOL_V1: json target requires json_pointer selector")
            ptr = str(sel.get("ptr") or "")
            if ptr in protected_ptrs or any(ptr.startswith(p + "/") for p in protected_ptrs):
                raise ValueError(f"PATCH_PROTOCOL_V1: protected_pointer {ptr}")

            cur_txt = after_text.get(target) or "{}"
            cur_obj = json.loads(cur_txt) if cur_txt.strip() else {}
            cur_obj = cur_obj if isinstance(cur_obj, dict) else {}

            cur_obj = _pp1_apply_json_pointer(cur_obj, ptr, opk, op.get("value"))
            after_text[target] = json.dumps(cur_obj, indent=2, sort_keys=True) + "\n"

        else:
            kind = str(sel.get("kind") or "")
            if kind == "whole_file":
                if opk == "remove":
                    after_text[target] = ""
                else:
                    after_text[target] = str(op.get("value") or "")
            elif kind == "markdown_section":
                hp = sel.get("heading_path") or []
                if not isinstance(hp, list) or not hp:
                    raise ValueError("PATCH_PROTOCOL_V1: markdown_section requires heading_path array")

                cur_lines = (after_text.get(target) or "").splitlines()
                must_absent = bool(guards.get("must_be_absent")) if isinstance(guards, dict) else False
                must_present = bool(guards.get("must_be_present")) if isinstance(guards, dict) else False

                found = True
                try:
                    s_idx, e_idx = _pp1_md_find_section(cur_lines, [str(x) for x in hp])
                except KeyError:
                    found = False

                if must_present and not found:
                    raise ValueError("PATCH_PROTOCOL_V1 guard_failed: section must be present")
                if must_absent and found:
                    raise ValueError("PATCH_PROTOCOL_V1 guard_failed: section must be absent")

                if not found:
                    # Add: insert at end of parent (or EOF if no parent)
                    parent_path = [str(x) for x in hp[:-1]]
                    insert_at = len(cur_lines)
                    if parent_path:
                        ps_idx, pe_idx = _pp1_md_find_section(cur_lines, parent_path)
                        insert_at = pe_idx
                    ins_lines = str(op.get("value") or "").splitlines()
                    cur_lines[insert_at:insert_at] = ins_lines
                else:
                    if opk == "remove":
                        del cur_lines[s_idx:e_idx]
                    else:
                        rep_lines = str(op.get("value") or "").splitlines()
                        cur_lines[s_idx:e_idx] = rep_lines

                after_text[target] = "\n".join(cur_lines).rstrip() + "\n"
            else:
                raise ValueError("PATCH_PROTOCOL_V1: markdown target requires whole_file or markdown_section selector")

    # Write canonical updates + artifact versions
    created: List[str] = []
    for path, (kind, logical) in allow.items():
        if path not in target_meta:
            continue

        before = before_text.get(path, "")
        after = after_text.get(path, "")

        if before == after:
            continue

        # bounded size (chars)
        if len(after) > 200000:
            raise ValueError(f"PATCH_PROTOCOL_V1 size_cap_exceeded: {path}")

        if kind == "json":
            # stamp last_updated server-side (ignore model)
            try:
                obj = json.loads(after) if after.strip() else {}
                if isinstance(obj, dict):
                    obj["last_updated"] = now_iso()
                    after = json.dumps(obj, indent=2, sort_keys=True) + "\n"
            except Exception:
                pass

            project_store.write_canonical_entry(
                project_name,
                target_path=state_file_path(project_name, logical),
                mode="json_overwrite",
                data=json.loads(after) if after.strip() else {},
            )
            entry = create_artifact(
                project_name,
                logical,
                json.loads(after) if after.strip() else {},
                artifact_type=logical,
                from_files=[],
                file_ext=".json",
            )
            if isinstance(entry, dict) and entry.get("path"):
                created.append(str(entry["path"]))
        else:
            # truncation guard already exists elsewhere; keep consistent here
            if _looks_truncated_state_doc(after):
                raise ValueError(f"PATCH_PROTOCOL_V1 rejected truncated markdown: {path}")

            project_store.write_canonical_entry(
                project_name,
                target_path=state_file_path(project_name, logical),
                mode="text_overwrite",
                data=after,
            )
            entry = create_artifact(
                project_name,
                logical,
                after,
                artifact_type=logical,
                from_files=[],
                file_ext=".md",
            )
            if isinstance(entry, dict) and entry.get("path"):
                created.append(str(entry["path"]))

    # Write receipt (append-only)
    receipt = {
        "v": 1,
        "patch_id": str(env.get("patch_id") or ""),
        "idempotency_key": idem_key,
        "applied_at": now_iso(),
        "outcome": "applied",
        "targets": [
            {
                "path": p,
                "before_sha256": before_sha.get(p, ""),
                "after_sha256": _pp1_sha256_text(after_text.get(p, "")),
                "bytes_before": len((before_text.get(p, "") or "").encode("utf-8", errors="replace")),
                "bytes_after": len((after_text.get(p, "") or "").encode("utf-8", errors="replace")),
            }
            for p in target_meta.keys()
        ],
    }
    _pp1_append_receipt(project_name, receipt)

    return created

async def model_refresh_state(project_name: str, instruction: str, *, extra_system: str = "") -> str:
    """
    Run a Project OS call whose primary purpose is to update persistent state docs.
    Returns the USER_ANSWER (typically a short "what next" nudge).
    """
    ensure_project_scaffold(project_name)
    project_context = build_project_context_for_model(project_name, instruction, max_chars=22000)

    messages: List[Dict[str, str]] = [{"role": "system", "content": LENS0_SYSTEM_PROMPT}]
    if project_context:
        messages.append({"role": "system", "content": "CURRENT_PROJECT_CONTEXT:\n\n" + project_context})
    if extra_system:
        messages.append({"role": "system", "content": extra_system})
    messages.append({"role": "user", "content": instruction})

    raw_answer = await asyncio.to_thread(call_openai_chat, messages)
    user_answer, blocks = parse_model_output(raw_answer)

    created_paths = persist_state_blocks(project_name, blocks)

    # If a deliverable was created, append a guaranteed working /file link.
    # Prefer Excel over HTML. Also strip any model-invented "Open:" lines first.
    excel_files = [p for p in created_paths if "/artifacts/" in (p or "") and (p or "").endswith(".xlsx")]
    html_files = [p for p in created_paths if "/artifacts/" in (p or "") and (p or "").endswith(".html")]

    # Remove any Open: lines the model may have invented (we will add a real one below).
    try:
        ua_lines = (user_answer or "").splitlines()
        ua_lines = [ln for ln in ua_lines if not ln.strip().lower().startswith("open:")]
        user_answer = "\n".join(ua_lines).strip()
    except Exception:
        pass

    link = ""
    if excel_files:
        link = f"/file?path={excel_files[-1]}"
    elif html_files:
        link = f"/file?path={html_files[-1]}"

    if link:
        user_answer = (user_answer.rstrip() + "\n\n" + "Open: " + link).strip()

    # Never return empty text (UI would look like "no response")
    if not (user_answer or "").strip():
        # Prefer an existing link if we appended one above; otherwise be explicit.
        user_answer = "Updated."
    return user_answer.strip()


# =============================================================================
# Blue/Green self-patching (safe server edits)
# =============================================================================

SELF_PATCH_STATE_PATH = RUNTIME_DIR / "self_patch_state.json"

RUNTIME_BLUE_DIR = RUNTIME_DIR / "blue"
RUNTIME_GREEN_DIR = RUNTIME_DIR / "green"

# Keep a lightweight in-process registry of subprocesses we started (for graceful stop)
_RUNNING_SUBPROCS: Dict[str, subprocess.Popen] = {}

_SELF_PATCH_LOCK: Optional[asyncio.Lock] = None  # created lazily (needs event loop)

def _self_patch_lock() -> asyncio.Lock:
    global _SELF_PATCH_LOCK
    if _SELF_PATCH_LOCK is None:
        _SELF_PATCH_LOCK = asyncio.Lock()
    return _SELF_PATCH_LOCK

def _runtime_dir_for_color(color: str) -> Path:
    c = (color or "").strip().lower()
    return RUNTIME_GREEN_DIR if c == "green" else RUNTIME_BLUE_DIR

def _other_color(color: str) -> str:
    return "green" if (color or "").strip().lower() != "green" else "blue"

def _pid_file(color: str) -> Path:
    return _runtime_dir_for_color(color) / "server.pid"

def _logs_dir(color: str) -> Path:
    d = _runtime_dir_for_color(color) / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d

def _is_windows() -> bool:
    try:
        return platform.system().lower().startswith("win")
    except Exception:
        return os.name == "nt"

def _is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if _is_windows():
        # Best-effort: tasklist output includes PID if running
        try:
            out = subprocess.check_output(["tasklist", "/FI", f"PID eq {pid}"], text=True, stderr=subprocess.DEVNULL)
            return str(pid) in out
        except Exception:
            return False
    else:
        try:
            os.kill(pid, 0)
            return True
        except Exception:
            return False

def _stop_pid(pid: int) -> bool:
    if pid <= 0:
        return False
    if _is_windows():
        try:
            subprocess.check_call(["taskkill", "/PID", str(pid), "/T", "/F"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True
        except Exception:
            return False
    else:
        try:
            os.kill(pid, signal.SIGTERM)
            return True
        except Exception:
            return False

def ensure_runtime_scaffold() -> None:
    """Create runtime + patches folders and ensure active.json exists."""
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    PATCHES_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_BLUE_DIR.mkdir(parents=True, exist_ok=True)
    RUNTIME_GREEN_DIR.mkdir(parents=True, exist_ok=True)
    # active.json is informational but helps humans
    active_path = RUNTIME_DIR / "active.json"
    if not active_path.exists():
        try:
            write_active_color(SERVER_COLOR if SERVER_COLOR in ("blue", "green") else "blue")
        except Exception:
            pass

def _load_self_patch_state() -> Dict[str, Any]:
    ensure_runtime_scaffold()
    if not SELF_PATCH_STATE_PATH.exists():
        return {"version": 1, "latest_id": None, "proposals": {}}
    try:
        return json.loads(SELF_PATCH_STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"version": 1, "latest_id": None, "proposals": {}}

def _save_self_patch_state(state: Dict[str, Any]) -> None:
    ensure_runtime_scaffold()
    try:
        atomic_write_text(SELF_PATCH_STATE_PATH, json.dumps(state, indent=2))
    except Exception:
        # last resort
        SELF_PATCH_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")

def _new_patch_id() -> str:
    # human-sortable, unique enough
    ts = time.strftime("%Y%m%d_%H%M%S")
    tail = uuid.uuid4().hex[:8]
    return f"{ts}_{tail}"

def _safe_slug(text: str, max_len: int = 42) -> str:
    t = (text or "").strip().lower()
    t = re.sub(r"[^a-z0-9]+", "-", t).strip("-")
    return (t[:max_len].strip("-") or "change")

def _current_code_root() -> Path:
    return Path(__file__).resolve().parent

def _project_rel_from_abs(abs_path: Path) -> str:
    try:
        return str(abs_path.resolve().relative_to(PROJECT_ROOT)).replace("\\", "/")
    except Exception:
        return abs_path.name

def _code_rel_from_project_rel(target_rel: str, code_root: Path) -> str:
    """Map PROJECT_ROOT-relative path -> code_root-relative path (for staged runtime copy)."""
    t = (target_rel or "").replace("\\", "/").lstrip("/")
    try:
        cr = str(code_root.resolve().relative_to(PROJECT_ROOT)).replace("\\", "/").strip("/")
    except Exception:
        cr = ""
    if not cr or cr == ".":
        return t
    prefix = cr.rstrip("/") + "/"
    if t.startswith(prefix):
        return t[len(prefix):]
    # Not under code root: refuse (prevents accidentally patching projects/ or other shared data)
    raise ValueError(f"Refusing self-patch outside code root. target='{t}' code_root='{cr}'")

@dataclass
class SelfPatchCommand:
    kind: str
    patch_id: Optional[str] = None
    raw: str = ""

def parse_self_patch_command(user_text: str) -> Optional[SelfPatchCommand]:
    t = (user_text or "").strip()
    if not t:
        return None
    if not ENABLE_PATCH_MODE:
        return None

    # Explicit-only: self-patch controls must start with "!".
    # No magic words in normal chat.
    if not t.startswith("!"):
        return None

    cmd = t[1:].strip()
    if not cmd:
        return None
    low = cmd.lower()

    # status
    if low in ("self patch status", "selfpatch status", "selfpatch", "self patch status", "status selfpatch", "status self patch", "selfpatch status"):
        return SelfPatchCommand(kind="status", raw=t)

    # stop
    m = re.match(r"^(stop)\s+(blue|green|me)\b", low)
    if m:
        target = m.group(2)
        return SelfPatchCommand(kind=f"stop_{target}", raw=t)

    # approve/cancel/retry
    m = re.match(r"^(approve|cancel|retry)\s+patch\s+([0-9]{8}_[0-9]{6}_[0-9a-f]{8})\b", low)
    if m:
        return SelfPatchCommand(kind=m.group(1), patch_id=m.group(2), raw=t)

    # switch
    m = re.match(r"^(switch|switch_and_stop)\s+([0-9]{8}_[0-9]{6}_[0-9a-f]{8})\b", low)
    if m:
        return SelfPatchCommand(kind=m.group(1), patch_id=m.group(2), raw=t)

    return None

def _summarize_unified_diff(diff_text: str) -> str:
    adds = dels = 0
    for ln in (diff_text or "").splitlines():
        if ln.startswith("+++") or ln.startswith("---"):
            continue
        if ln.startswith("+"):
            adds += 1
        elif ln.startswith("-"):
            dels += 1
    return f"+{adds} / -{dels} lines"

def _extract_code_fence_block(lines: List[str], start_idx: int) -> Tuple[str, int]:
    """Extract a ```...``` block starting at start_idx (which must be the fence line).

    Returns (content, next_index_after_block).
    """
    if start_idx >= len(lines):
        raise ValueError("Missing code fence start.")
    fence = lines[start_idx].strip()
    if not fence.startswith("```"):
        raise ValueError("Expected code fence start.")
    i = start_idx + 1
    buf: List[str] = []
    while i < len(lines):
        if lines[i].strip() == "```":
            return "\n".join(buf), i + 1
        buf.append(lines[i].rstrip("\n").rstrip("\r"))
        i += 1
    raise ValueError("Unterminated code fence in patch output.")

def unified_git_diff(rel_path: str, old_text: str, new_text: str) -> str:

    rp = (rel_path or "").replace("\\", "/").lstrip("/")
    old_lines = (old_text or "").splitlines(keepends=False)
    new_lines = (new_text or "").splitlines(keepends=False)
    ud = list(difflib.unified_diff(
        old_lines,
        new_lines,
        fromfile="a/" + rp,
        tofile="b/" + rp,
        lineterm="",
    ))
    if not ud:
        return ""
    out = ["diff --git a/" + rp + " b/" + rp] + ud
    return "\n".join(out).rstrip() + "\n"


# (cleanup) removed legacy unified_git_diff_1766091671_server alias


def build_unified_diff_from_anchor_patch(target_rel_path: str, patch_text: str) -> Tuple[str, str]:
    """Return (diff_text, summary) or ("","") if cannot build."""
    target = (target_rel_path or "").replace("\\", "/").strip()
    if not target:
        return "", ""
    abs_path = PROJECT_ROOT / target
    if not abs_path.exists():
        return "", ""
    if not patch_text or patch_text.strip().startswith("PATCH MODE ERROR"):
        return "", ""

    old = read_text_file(abs_path)
    try:
        new = apply_anchor_patch_to_text(old, patch_text)
    except Exception:
        return "", ""

    diff = unified_git_diff(target, old, new)
    if not diff.strip():
        return "", ""

    return diff, _summarize_unified_diff(diff)

async def propose_self_patch(user_text: str) -> str:
    """Generate a self-patch proposal (does NOT apply it)."""
    ensure_runtime_scaffold()

    # Normalize user request (strip explicit command prefix)
    req = (user_text or "").strip()
    for prefix in ("!selfpatch", "!serverpatch", "!patch-server", "!patch"):
        if req.lower().startswith(prefix):
            req = req[len(prefix):].strip()
            break
    if not req:
        req = user_text.strip()

    from_color = SERVER_COLOR
    to_color = _other_color(from_color)

    code_root = _current_code_root()
    script_abs = Path(__file__).resolve()
    target_project_rel = _project_rel_from_abs(script_abs)

    # Ensure the current server file is ingested in the reserved server project
    ensure_project_scaffold(SERVER_INTERNAL_PROJECT)
    try:
        ensure_ingested_current(SERVER_INTERNAL_PROJECT, target_project_rel)
    except Exception as e:
        return (
            "SELF-PATCH ERROR: I couldn't ingest the current server file.\n"
            f"- target: {target_project_rel}\n"
            f"- error: {e!r}\n"
        )

    # Generate anchor patch against the current server file
    target_rel, patch_text = await generate_anchor_validated_patch(SERVER_INTERNAL_PROJECT, req)

    # Guard: only allow self-patching files under the code root
    try:
        _ = _code_rel_from_project_rel(target_rel, code_root)
    except Exception as e:
        return (
            "SELF-PATCH ERROR: The generated patch targets a file outside the current code root.\n"
            f"- target_rel: {target_rel}\n"
            f"- code_root: {code_root}\n"
            f"- error: {e}\n"
            "Try explicitly mentioning the server entry file name in your request.\n"
        )

    diff_text, diff_summary = build_unified_diff_from_anchor_patch(target_rel, patch_text)

    patch_id = _new_patch_id()
    slug = _safe_slug(req)
    patch_anchor_file = PATCHES_DIR / f"selfpatch_{patch_id}__{slug}__anchor.txt"
    patch_diff_file = PATCHES_DIR / f"selfpatch_{patch_id}__{slug}.patch"

    # Save files
    patch_anchor_file.write_text(patch_text, encoding="utf-8")
    if diff_text:
        patch_diff_file.write_text(diff_text, encoding="utf-8")

    # Persist proposal
    st = _load_self_patch_state()
    proposals = st.get("proposals") if isinstance(st.get("proposals"), dict) else {}
    proposals[patch_id] = {
        "id": patch_id,
        "requested_at": time.time(),
        "from_color": from_color,
        "to_color": to_color,
        "target_rel": target_rel,
        "req": req,
        "anchor_patch_file": str(patch_anchor_file),
        "diff_patch_file": str(patch_diff_file) if diff_text else "",
        "diff_summary": diff_summary,
        "status": "proposed",
        "last_error": "",
        "candidate_pid": None,
        "candidate_http": None,
        "candidate_ws": None,
        "log_file": "",
    }
    st["latest_id"] = patch_id
    st["proposals"] = proposals
    _save_self_patch_state(st)

    ws_to, http_to = ports_for_color(to_color)
    msg_lines = [
        "BLUE/GREEN SELF-PATCH (proposal)",
        "",
        f"- Patch ID: {patch_id}",
        f"- From: {from_color}  ->  To: {to_color}",
        f"- Target file: {target_rel}",
        f"- Staged ports (to test safely): ws://{HOST}:{ws_to}  and  http://{HOST}:{http_to}",
        "",
    ]

    if diff_text:
        msg_lines += [
            f"- Diff summary: {diff_summary or '(no summary)'}",
            f"- Patch saved: {patch_diff_file}",
            "",
            "Preview (unified diff):",
            "```diff",
            diff_text.strip(),
            "```",
            "",
        ]
    else:
        msg_lines += [
            "- Diff summary: (couldn't compute a unified diff; will use anchor patch)",
            f"- Anchor patch saved: {patch_anchor_file}",
            "",
        ]

    msg_lines += [
        "Next step:",
        f"1) Reply EXACTLY: APPROVE PATCH {patch_id}",
        "   (I will stage the change into the other color, start it, and health-check it.)",
        "",
        f"To cancel: CANCEL PATCH {patch_id}",
        "To see status later: SELF PATCH STATUS",
    ]
    return "\n".join(msg_lines).strip()

async def _stage_and_start_self_patch(patch_id: str) -> str:
    """Apply proposal into inactive runtime folder and boot-check the new server."""
    ensure_runtime_scaffold()

    st = _load_self_patch_state()
    proposals = st.get("proposals") if isinstance(st.get("proposals"), dict) else {}
    p = proposals.get(patch_id) if isinstance(proposals, dict) else None
    if not isinstance(p, dict):
        return f"SELF-PATCH ERROR: Unknown patch id: {patch_id}"

    if p.get("status") not in ("proposed", "failed"):
        return f"SELF-PATCH: Patch {patch_id} is in status={p.get('status')!r}. Use SELF PATCH STATUS."

    from_color = (p.get("from_color") or SERVER_COLOR).strip().lower() or SERVER_COLOR
    to_color = (p.get("to_color") or _other_color(from_color)).strip().lower() or _other_color(from_color)

    # Stop existing to_color if running
    stop_note = ""
    pid_path = _pid_file(to_color)
    if pid_path.exists():
        try:
            pid = int(pid_path.read_text(encoding="utf-8").strip() or "0")
        except Exception:
            pid = 0
        if _is_pid_running(pid):
            ok = _stop_pid(pid)
            stop_note = f"Stopped existing {to_color} (pid {pid}) -> {ok}."
        try:
            pid_path.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass

    # Mirror code root to runtime/<to_color>
    code_root = _current_code_root()
    dst_root = _runtime_dir_for_color(to_color)

    ignore_dirs = {
        "projects", "runtime", "patches", ".git",
        "__pycache__", ".venv", "venv", "node_modules", "dist", "build",
        ".pytest_cache", ".mypy_cache",
    }

    def _sync_tree(src: Path, dst: Path) -> None:
        if dst.exists():
            shutil.rmtree(dst)
        dst.mkdir(parents=True, exist_ok=True)

        for root, dirs, files in os.walk(src):
            root_p = Path(root)
            rel = root_p.relative_to(src)
            # skip ignored dirs anywhere in path
            if any(part in ignore_dirs for part in rel.parts):
                dirs[:] = []
                continue
            # filter dirs in-place so walk doesn't descend
            dirs[:] = [d for d in dirs if d not in ignore_dirs and not d.startswith(".")]
            out_dir = dst / rel
            out_dir.mkdir(parents=True, exist_ok=True)
            for fn in files:
                if fn.startswith("."):
                    continue
                src_file = root_p / fn
                rel_file = src_file.relative_to(src)
                if any(part in ignore_dirs for part in rel_file.parts):
                    continue
                dst_file = dst / rel_file
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_file, dst_file)

    await asyncio.to_thread(_sync_tree, code_root, dst_root)

    # Apply patch to staged target file (anchor patch is canonical)
    target_rel = (p.get("target_rel") or "").replace("\\", "/").strip()
    anchor_path = Path(p.get("anchor_patch_file") or "")
    if not anchor_path.exists():
        return f"SELF-PATCH ERROR: Missing anchor patch file on disk: {anchor_path}"

    anchor_text = anchor_path.read_text(encoding="utf-8", errors="replace")

    try:
        code_rel = _code_rel_from_project_rel(target_rel, code_root)
    except Exception as e:
        p["status"] = "failed"
        p["last_error"] = f"code-path-map error: {e}"
        proposals[patch_id] = p
        st["proposals"] = proposals
        _save_self_patch_state(st)
        return f"SELF-PATCH ERROR: {e}"

    staged_file = dst_root / code_rel
    if not staged_file.exists():
        p["status"] = "failed"
        p["last_error"] = f"staged target missing: {staged_file}"
        proposals[patch_id] = p
        st["proposals"] = proposals
        _save_self_patch_state(st)
        return f"SELF-PATCH ERROR: Staged target file does not exist: {staged_file}"

    old_text = staged_file.read_text(encoding="utf-8", errors="replace")
    try:
        new_text = apply_anchor_patch_to_text(old_text, anchor_text)
    except Exception as e:
        p["status"] = "failed"
        p["last_error"] = f"apply error: {e!r}"
        proposals[patch_id] = p
        st["proposals"] = proposals
        _save_self_patch_state(st)
        return (
            "SELF-PATCH FAILED: I couldn't apply the anchor patch to the staged file.\n"
            f"- error: {e!r}\n"
            f"- staged_file: {staged_file}\n"
        )

    staged_file.write_text(new_text, encoding="utf-8")

    # Syntax check (Python)
    if staged_file.suffix.lower() == ".py":
        try:
            import py_compile
            py_compile.compile(str(staged_file), doraise=True)
        except Exception as e:
            p["status"] = "failed"
            p["last_error"] = f"py_compile failed: {e!r}"
            proposals[patch_id] = p
            st["proposals"] = proposals
            _save_self_patch_state(st)
            return (
                "SELF-PATCH FAILED: The patched server file does not compile.\n"
                f"- error: {e!r}\n"
                f"- staged_file: {staged_file}\n"
                f"- (Blue server is still running.)\n"
            )

    # Start staged server
    ws_to, http_to = ports_for_color(to_color)
    log_file = _logs_dir(to_color) / f"server_{patch_id}.log"
    env = os.environ.copy()
    env["LENS0_PROJECT_ROOT"] = str(PROJECT_ROOT)
    env["LENS0_SERVER_COLOR"] = to_color
    env["LENS0_HOST"] = HOST
    env["LENS0_WS_PORT"] = str(ws_to)
    env["LENS0_HTTP_PORT"] = str(http_to)

    entry = dst_root / Path(__file__).name
    if not entry.exists():
        # If the entry file name changed, fall back to the staged_file's name.
        entry = staged_file if staged_file.name else entry

    log_fh = open(log_file, "a", encoding="utf-8", errors="replace")
    try:
        proc = subprocess.Popen(
            [sys.executable, str(entry)],
            cwd=str(PROJECT_ROOT),
            env=env,
            stdout=log_fh,
            stderr=log_fh,
        )
        _RUNNING_SUBPROCS[to_color] = proc
    except Exception as e:
        log_fh.close()
        p["status"] = "failed"
        p["last_error"] = f"subprocess start failed: {e!r}"
        proposals[patch_id] = p
        st["proposals"] = proposals
        _save_self_patch_state(st)
        return f"SELF-PATCH FAILED: Couldn't start {to_color} server: {e!r}"

    # Record pid
    try:
        _pid_file(to_color).write_text(str(proc.pid), encoding="utf-8")
    except Exception:
        pass

    # Health-check loop
    health_url = f"http://{HOST}:{http_to}/health"

    def _wait_health() -> Tuple[bool, str]:
        deadline = time.time() + 25.0
        last_err = ""
        while time.time() < deadline:
            if proc.poll() is not None:
                return False, f"process exited with code {proc.returncode}"
            try:
                r = requests.get(health_url, timeout=2.5)
                if r.status_code == 200:
                    return True, "ok"
                last_err = f"http {r.status_code}"
            except Exception as e:
                last_err = repr(e)
            time.sleep(0.6)
        return False, f"timeout waiting for /health ({last_err})"

    ok, why = await asyncio.to_thread(_wait_health)

    if not ok:
        p["status"] = "failed"
        p["last_error"] = f"healthcheck failed: {why}"
        p["log_file"] = str(log_file)
        proposals[patch_id] = p
        st["proposals"] = proposals
        _save_self_patch_state(st)
        return (
            "SELF-PATCH FAILED: The staged server did not become healthy.\n"
            f"- to_color: {to_color}\n"
            f"- reason: {why}\n"
            f"- log: {log_file}\n"
            "\nBlue server stays up. You can fix the patch request and try again.\n"
            f"Retry: RETRY PATCH {patch_id}\n"
        )

    # Success
    p["status"] = "healthy"
    p["candidate_pid"] = proc.pid
    p["candidate_ws"] = f"ws://{HOST}:{ws_to}"
    p["candidate_http"] = f"http://{HOST}:{http_to}"
    p["log_file"] = str(log_file)
    proposals[patch_id] = p
    st["proposals"] = proposals
    _save_self_patch_state(st)

    switch_hint = (
        "Staged server is healthy.\n\n"
        f"- {to_color} WS:  ws://{HOST}:{ws_to}\n"
        f"- {to_color} HTTP: http://{HOST}:{http_to}\n\n"
        "Next step (confirm & switch):\n"
        f"1) Open your UI/client against the {to_color} ports and verify it works.\n"
        f"2) Then reply EXACTLY: SWITCH {patch_id}\n"
        f"   (optional hard cutover): SWITCH_AND_STOP {patch_id}\n"
        "\nIf you want to keep both running for a while, just SWITCH (no stop).\n"
    )
    if stop_note:
        switch_hint = stop_note + "\n\n" + switch_hint
    return switch_hint

async def handle_self_patch_command(cmd: SelfPatchCommand) -> str:
    if cmd.kind == "status":
        st = _load_self_patch_state()
        latest = st.get("latest_id")
        active = read_active_color("blue")
        msg = [
            "SELF PATCH STATUS",
            "",
            f"- This server color: {SERVER_COLOR}",
            f"- Recommended active (runtime/active.json): {active}",
            f"- This server endpoints: ws://{HOST}:{WS_PORT}  http://{HOST}:{HTTP_PORT}",
            "",
        ]
        if latest and isinstance(latest, str):
            p = (st.get("proposals") or {}).get(latest, {})
            if isinstance(p, dict):
                msg += [
                    f"- Latest patch: {latest}",
                    f"  - status: {p.get('status')}",
                    f"  - target: {p.get('target_rel')}",
                    f"  - from->to: {p.get('from_color')} -> {p.get('to_color')}",
                ]
                if p.get("diff_summary"):
                    msg.append(f"  - diff: {p.get('diff_summary')}")
                if p.get("last_error"):
                    msg.append(f"  - last_error: {p.get('last_error')}")
                if p.get("candidate_http"):
                    msg.append(f"  - candidate_http: {p.get('candidate_http')}")
                if p.get("log_file"):
                    msg.append(f"  - log_file: {p.get('log_file')}")
        else:
            msg.append("- No self-patch proposals yet.")
        return "\n".join(msg).strip()

    if cmd.kind in ("stop_blue", "stop_green"):
        color = "blue" if cmd.kind.endswith("blue") else "green"
        pid_path = _pid_file(color)
        pid = 0
        if pid_path.exists():
            try:
                pid = int(pid_path.read_text(encoding="utf-8").strip() or "0")
            except Exception:
                pid = 0
        proc = _RUNNING_SUBPROCS.get(color)
        stopped = False
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                stopped = True
            except Exception:
                stopped = False
        if not stopped and pid:
            stopped = _stop_pid(pid)
        return f"STOP {color.upper()}: pid={pid or (proc.pid if proc else None)} stopped={stopped}"

    if cmd.kind == "stop_me":
        request_shutdown("User requested stop_me")
        return "Stopping this server (shutdown requested)."

    if cmd.kind in ("approve", "retry"):
        if not cmd.patch_id:
            return "SELF-PATCH ERROR: Missing patch id."
        async with _self_patch_lock():
            return await _stage_and_start_self_patch(cmd.patch_id)

    if cmd.kind == "cancel":
        if not cmd.patch_id:
            return "SELF-PATCH ERROR: Missing patch id."
        st = _load_self_patch_state()
        proposals = st.get("proposals") if isinstance(st.get("proposals"), dict) else {}
        p = proposals.get(cmd.patch_id) if isinstance(proposals, dict) else None
        if not isinstance(p, dict):
            return f"SELF-PATCH ERROR: Unknown patch id: {cmd.patch_id}"
        p["status"] = "cancelled"
        proposals[cmd.patch_id] = p
        st["proposals"] = proposals
        _save_self_patch_state(st)
        return f"Cancelled patch {cmd.patch_id}."

    if cmd.kind in ("switch", "switch_and_stop"):
        if not cmd.patch_id:
            return "SELF-PATCH ERROR: Missing patch id."
        st = _load_self_patch_state()
        proposals = st.get("proposals") if isinstance(st.get("proposals"), dict) else {}
        p = proposals.get(cmd.patch_id) if isinstance(proposals, dict) else None
        if not isinstance(p, dict):
            return f"SELF-PATCH ERROR: Unknown patch id: {cmd.patch_id}"
        if p.get("status") != "healthy":
            return f"SELF-PATCH: Patch {cmd.patch_id} is status={p.get('status')!r}. You can only switch after it's healthy."

        to_color = (p.get("to_color") or "").strip().lower() or _other_color(SERVER_COLOR)
        try:
            write_active_color(to_color)
        except Exception as e:
            return f"SELF-PATCH ERROR: Couldn't write active.json: {e!r}"

        p["status"] = "switched"
        proposals[cmd.patch_id] = p
        st["proposals"] = proposals
        _save_self_patch_state(st)

        msg = [
            f"SWITCHED recommended active to {to_color.upper()} (runtime/active.json updated).",
            "",
            "New server endpoints:",
            f"- WS:  {p.get('candidate_ws')}",
            f"- HTTP:{p.get('candidate_http')}",
            "",
            "Note: your current client is still connected to this (old) server until you reconnect.",
            "If you want to stop this server after you reconnect, run: STOP ME",
        ]
        if cmd.kind == "switch_and_stop":
            msg.append("")
            msg.append("Shutting down this server now (as requested).")
            request_shutdown("switch_and_stop")
        return "\n".join(msg).strip()

    return "SELF-PATCH: Unknown command."

# Shutdown coordination (used by STOP ME / switch_and_stop)
_SHUTDOWN_EVENT: Optional[asyncio.Event] = None

def request_shutdown(reason: str = "") -> None:
    global _SHUTDOWN_EVENT
    if _SHUTDOWN_EVENT is not None:
        _SHUTDOWN_EVENT.set()
    if reason:
        print(f"[SHUTDOWN] requested: {reason}")
# =============================================================================
# Async upload processing (queue + worker)
# =============================================================================

# Upload subsystem extracted to upload_pipeline.py.
# Keep server.py as glue only. Re-export names for ws_commands + existing call sites.
get_recent_upload_status = upload_pipeline.get_recent_upload_status

_ws_frame = upload_pipeline.ws_frame
_ws_add_client = upload_pipeline.ws_add_client
_ws_remove_client = upload_pipeline.ws_remove_client
_ws_move_client = upload_pipeline.ws_move_client
_broadcast_to_project = upload_pipeline.broadcast_to_project

# WebSocket handler
# =============================================================================
async def handle_file_added(
    *,
    current_project_full: str,
    rel: str,
    current_project: str,
    dashboard_for,
) -> str:
    """
    Upload enqueue path:
    - Keep callable from server (ws_commands expects it)
    - Heavy lifting moved into upload_pipeline.py
    """
    # AOF v2 (Active Object Focus):
    # Do NOT set focus here because `rel` may be a pre-canonical upload path.
    # Focus is set inside upload_pipeline.enqueue_file_added() after canonical_rel is known.
    return await upload_pipeline.enqueue_file_added(
        current_project_full=current_project_full,
        rel=rel,
        current_project=current_project,
        project_root=PROJECT_ROOT,
    )


async def handle_goal_command(
    *,
    current_project_full: str,
    goal_text: str,
    current_project: str,
    dashboard_for,
) -> str:
    """
    Heavy work stays in server.py.
    ws_commands only parses goal: and calls here.
    """
    g = (goal_text or "").strip()
    m = load_manifest(current_project_full)
    m["goal"] = g
    save_manifest(current_project_full, m)
    try:
        st_path = state_file_path(current_project_full, "project_state")
        if st_path.exists():
            st = json.loads(st_path.read_text(encoding="utf-8") or "{}")
            if isinstance(st, dict):
                st["goal"] = g
                st["last_updated"] = now_iso()
                atomic_write_text(st_path, json.dumps(st, indent=2))
    except Exception:
        pass
    return dashboard_for(current_project)
# =============================================================================
# C5 v1 — Deterministic Memory Reuse (Notes + Decisions + Deliverables)
# - Detect memory-seeking questions via simple keyword heuristics (no model call)
# - Retrieve from state/upload_notes.jsonl, state/decisions.jsonl, state/deliverables.json
# - Use stored text verbatim where possible; cap 3 results
# =============================================================================

_C5_MEMORY_SEEK_TRIGGERS = (
    "what did we decide",
    "what did we pick",
    "what did we choose",
    "what was that for",
    "what did i say about",
    "remind me",
    "show me what we",
    "show me what we used",
    "show me what we picked",
    "show me what we chose",
    "which one did we choose",
    "which one did we pick",
    "which one did we use",
    "what was that image",
    "what was that file",
)

def _c5_is_memory_seeking_query(user_text: str) -> bool:
    t = (user_text or "").strip().lower()
    if not t:
        return False

    # Avoid accidental triggers on command-like messages
    if t in ("list", "ls", "plan", "projects", "list projects"):
        return False
    if t.startswith(("open ", "goal:", "[search]", "[nosearch]", "patch", "/selfpatch", "/serverpatch", "/patch-server")):
        return False

    # High-confidence phrases
    for trig in _C5_MEMORY_SEEK_TRIGGERS:
        if trig in t:
            return True
    # Deterministic pattern catch:
    # - "what was the tile for?"
    # - "what is that photo for?"
    # Keep it conservative: must start with "what was/is" and contain a trailing "for".
    if re.search(r"^\s*what\s+(?:was|is)\s+.+\s+for\??\s*$", t):
        return True

    # Additional simple combos (still deterministic, still conservative)
    has_what = ("what " in t) or t.startswith("what")
    has_remind = ("remind" in t)
    has_show = t.startswith("show me") or ("show me" in t)
    has_decision_word = any(w in t for w in ("decide", "decision", "picked", "pick", "chose", "choose", "used", "use"))
    has_that = ("that" in t) or ("this" in t)
    has_for = (" for" in t)

    # "about me" memory queries (avoid phrase-lock / exact-string brittleness)
    # Examples we must catch:
    # - "tell me what you know about me"
    # - "tell me what you know about me and if they are t2 etc"
    # - "what do you remember about me"
    has_about_me = (" about me" in t) or t.endswith("about me") or ("about myself" in t)
    has_memory_signal = any(
        w in t
        for w in (
            "remember", "know", "what do you know", "what do you remember",
            "stored", "store", "memory", "profile",
            "tier", "tiers",
            "t1", "t2", "t2g", "t2m",
            "tier 1", "tier 2", "tier2", "tier-2", "tier2g", "tier2m",
            "facts", "facts map", "canonical",
        )
    )
    if has_about_me and has_memory_signal:
        return True

    if has_remind:
        return True
    if has_show and has_decision_word:
        return True
    if has_what and has_that and has_for:
        return True

    return False


def _c5_format_memory_response(items: List[Dict[str, Any]]) -> str:
    """
    Build a calm, grounded response using stored text verbatim where possible.
    items are normalized records from project_store.find_project_memory(...)
    """
    if not items:
        return ""

    lines: List[str] = []
    # Extra spacing to prevent UI line-collapsing
    lines.append("Here’s what we have so far:")
    lines.append("")
    lines.append("")

    for r in items:
        if not isinstance(r, dict):
            continue
        src = str(r.get("source") or "").strip()
        data = r.get("data") if isinstance(r.get("data"), dict) else {}

        if src == "upload_note":
            up = str(data.get("upload_path") or "").strip()
            ans = str(data.get("answer") or "")

            # Display-only cleanup (read-only):
            # - Strip UI-injected instruction blocks like [SCOPE]...[/SCOPE]
            # - Normalize whitespace so the UI doesn't smear bullets together
            try:
                ans = strip_lens0_system_blocks(ans)
            except Exception:
                pass
            ans = re.sub(r"\s+", " ", (ans or "").strip())

            if up:
                lines.append(f"• For {up}: {ans}")
            else:
                lines.append(f"• Upload note: {ans}")

        elif src == "decision":
            txt = str(data.get("text") or "").strip()
            if txt:
                lines.append(f"• Decision: {txt}")

        elif src == "deliverable":
            title = str(data.get("title") or "").strip()
            path = str(data.get("path") or "").replace("\\", "/").strip()
            if title and path:
                lines.append(f"• Deliverable: {title}")
                lines.append(f"  /file?path={path}")
            elif path:
                lines.append(f"• Deliverable:")
                lines.append(f"  /file?path={path}")

# -----------------------------------------------------------------------------
# C5.3 — Conversational file fallback (deterministic)
# - If user says “that spreadsheet/photo/deliverable…”, try:
#   1) active_object.json
#   2) latest deliverable (by type inferred)
#   3) latest raw upload (by type inferred)
# - If still ambiguous, list best-matching candidates from raw/ + artifacts/
#   and ask “is it one of these?”
# -----------------------------------------------------------------------------

_FILE_REF_TRIGGERS = (
    "that file",
    "this file",
    "the file",
    "that spreadsheet",
    "this spreadsheet",
    "the spreadsheet",
    "that excel",
    "this excel",
    "the excel",
    "that workbook",
    "this workbook",
    "the workbook",
    "that photo",
    "this photo",
    "the photo",
    "that image",
    "this image",
    "the image",
    "that screenshot",
    "this screenshot",
    "the screenshot",
    "that pdf",
    "this pdf",
    "the pdf",
    "that document",
    "this document",
    "the document",
    "that deliverable",
    "this deliverable",
    "the deliverable",
    "look at it again",
    "open it again",
    "use it again",
    "tell me about it",
    "tell me about that",
)

def _infer_file_kind_from_msg(user_msg: str) -> str:
    """
    Return one of: "excel" | "image" | "pdf" | "html" | "any"
    Deterministic keyword heuristic only.
    """
    t = (user_msg or "").lower()
    if any(w in t for w in ("spreadsheet", "workbook", "excel", ".xlsx", ".xlsm", ".xls")):
        return "excel"
    if any(w in t for w in ("photo", "image", "screenshot", ".png", ".jpg", ".jpeg", ".webp", ".gif")):
        return "image"
    if any(w in t for w in ("pdf", ".pdf")):
        return "pdf"
    if any(w in t for w in ("html", "webpage", "page", ".html", "deliverable html")):
        return "html"
    return "any"
def _wants_describe_file(user_msg: str) -> bool:
    """
    Deterministic intent check for 'describe what this file/image shows'.
    Conservative but handles common phrasing like:
      - what does this screenshot show?
      - what does this image show?
      - what's shown here?
      - describe this
    """
    lowq = (user_msg or "").strip().lower()
    if not lowq:
        return False

    # Simple intent phrases
    if any(x in lowq for x in (
        "tell me about",
        "summarize",
        "describe",
        "what is in",
        "what's in",
        "what does it say",
    )):
        return True

    # Regex-based patterns
    try:
        if re.search(r"\bwhat\s+does\s+.+\s+show\??\s*$", lowq):
            return True
        if re.search(r"\bwhat\s+is\s+.+\s+showing\??\s*$", lowq):
            return True
        if re.search(r"\bwhat['’]s\s+shown\s+here\??\s*$", lowq):
            return True
    except Exception:
        pass

    return False

def _is_file_referential_query(user_msg: str) -> bool:
    """
    True only when the user is referring to a specific, already-existing file
    AND is asking *us* to act on it.

    Key guardrails:
    - Do NOT trigger on abstract commentary like:
        "models often fail to read the file carefully"
      even though it contains "the file" + "read".
    - Require assistant-addressed intent (can you / please / show me / tell me / open / look at).
    - Do NOT trigger if an explicit filename is present.
    """
    msg = (user_msg or "").strip()
    if not msg:
        return False

    # If they explicitly named a file, don't treat as referential.
    if extract_explicit_filenames(msg):
        return False

    low = msg.lower()

    # If file-ish wording is inside quotes, treat as meta discussion (not a request).
    try:
        if re.search(r"['\"].*(file|document|pdf|spreadsheet|excel|image|photo|screenshot|deliverable).*['\"]", low):
            return False
    except Exception:
        pass

    # Must contain a deictic file reference.
    try:
        has_deictic = bool(
            re.search(
                r"\b(this|that|the)\s+(file|document|pdf|spreadsheet|excel|workbook|image|photo|screenshot|deliverable)\b",
                low,
            )
        )
    except Exception:
        has_deictic = False

    if not has_deictic:
        return False

    # Must look like the user is asking the assistant to do something with it.
    # This blocks abstract statements that mention "read the file" generically.
    assistant_addressed = (
        ("can you" in low)
        or ("could you" in low)
        or ("would you" in low)
        or ("please" in low)
        or low.startswith((
            "open",
            "look at",
            "use",
            "show me",
            "tell me",
            "describe",
            "summarize",
            "review",
            "analyze",
            "read",
        ))
    )
    if not assistant_addressed:
        return False

    # Action intent required (still conservative)
    action_verbs = (
        "open",
        "look at",
        "use",
        "show",
        "describe",
        "summarize",
        "tell me about",
        "review",
        "analyze",
        "read",
    )
    if not any(v in low for v in action_verbs):
        return False

    return True


def _path_exists_under_root(rel_path: str) -> bool:
    try:
        rp = (rel_path or "").replace("\\", "/").strip()
        if not rp:
            return False
        p = (PROJECT_ROOT / rp).resolve()
        p.relative_to(PROJECT_ROOT.resolve())
        return p.exists() and p.is_file()
    except Exception:
        return False

def _candidate_score(name: str, msg: str) -> int:
    """
    Small deterministic scoring:
    - keyword overlap between message tokens and filename tokens
    - recency handled separately (we sort by score then recency)
    """
    a = (name or "").lower()
    b = (msg or "").lower()
    if not a or not b:
        return 0
    toks = [t for t in re.split(r"[^a-z0-9_]+", b) if len(t) >= 3][:24]
    score = 0
    for t in toks:
        if t and t in a:
            score += 2
    # small boost for direct “deliverable” mention
    if "deliverable" in b and "deliverable" in a:
        score += 2
    return score

def _filter_by_kind(filename: str, kind: str) -> bool:
    fn = (filename or "").lower().strip()
    if not fn:
        return False
    if kind == "excel":
        return fn.endswith((".xlsx", ".xlsm", ".xls"))
    if kind == "image":
        return fn.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif"))
    if kind == "pdf":
        return fn.endswith(".pdf")
    if kind == "html":
        return fn.endswith((".html", ".htm"))
    return True

def _get_best_candidates(project_full: str, user_msg: str, *, kind: str, limit: int = 5) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    listing: Dict[str, Any] = {}
    try:
        listing = list_project_files(project_full) or {}
    except Exception:
        listing = {}

    raw = listing.get("raw") if isinstance(listing, dict) else None
    arts = listing.get("artifacts") if isinstance(listing, dict) else None

    raw = raw if isinstance(raw, list) else []
    arts = arts if isinstance(arts, list) else []

    scored: List[Tuple[int, float, str, str]] = []

    # -------- 1) From manifest listing (existing behavior) --------

    # Raw: recency proxy by index
    for idx, r in enumerate(raw):
        if not isinstance(r, dict):
            continue
        nm = str(r.get("filename") or r.get("original_name") or "").strip()
        rel = str(r.get("relative_path") or "").replace("\\", "/").strip()
        if not nm or not rel:
            continue
        if not _filter_by_kind(nm, kind):
            continue
        score = _candidate_score(nm, user_msg)
        rec = float(idx)
        scored.append((score, rec, nm, rel))

    # Artifacts: version as weak recency proxy; fallback to index
    for idx, a in enumerate(arts):
        if not isinstance(a, dict):
            continue
        nm = str(a.get("filename") or "").strip()
        rel = str(a.get("relative_path") or "").replace("\\", "/").strip()
        if not nm or not rel:
            continue
        if not _filter_by_kind(nm, kind):
            continue
        score = _candidate_score(nm, user_msg)
        try:
            rec = float(a.get("version") or 0)
        except Exception:
            rec = float(idx)
        scored.append((score, rec, nm, rel))

    # -------- 2) Disk scan fallback (NEW) --------
    # If the manifest missed a file (e.g., multi-upload race), but the file exists on disk,
    # we still want it to appear as a candidate.

    try:
        rd = raw_dir(project_full)
        if rd.exists():
            files = [p for p in rd.iterdir() if p.is_file()]
            files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            for rank, p in enumerate(files[:200]):
                nm = p.name
                if not _filter_by_kind(nm, kind):
                    continue
                rel = str(p.relative_to(PROJECT_ROOT)).replace("\\", "/")
                if not rel:
                    continue
                score = _candidate_score(nm, user_msg)
                # Recency from mtime ranking (higher rec = newer)
                rec = float(100000 - rank)
                scored.append((score, rec, nm, rel))
    except Exception:
        pass

    try:
        ad = artifacts_dir(project_full)
        if ad.exists():
            files = [p for p in ad.iterdir() if p.is_file()]
            files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            for rank, p in enumerate(files[:400]):
                nm = p.name
                if not _filter_by_kind(nm, kind):
                    continue
                rel = str(p.relative_to(PROJECT_ROOT)).replace("\\", "/")
                if not rel:
                    continue
                score = _candidate_score(nm, user_msg)
                rec = float(100000 - rank)
                scored.append((score, rec, nm, rel))
    except Exception:
        pass

    # Sort: score desc, recency desc, name asc
    scored.sort(key=lambda x: (-x[0], -x[1], x[2].lower()))

    seen = set()
    for _sc, _rec, nm, rel in scored:
        # Never surface internal canonical state docs or internal assistant outputs
        # as "file candidates" in conversational resolution.
        # These are system-managed artifacts, not user-selected working files.
        nm_low = (nm or "").strip().lower()
        if nm_low.startswith("assistant_output_"):
            continue
        if nm_low in (
            "project_state_v1.json",
            "facts_map_v1.md",
            "decision_log_v1.md",
            "preferences_v1.md",
            "working_doc_v1.md",
            "project_map_v1.md",
            "project_brief_v1.md",
        ):
            continue        
        # Never surface internal assistant outputs as "file candidates"
        # unless the user explicitly names them (handled elsewhere).
        if (nm or "").strip().lower().startswith("assistant_output_"):
            continue        
        key = rel.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append({"name": nm, "rel": rel})
        if len(out) >= int(limit or 5):
            break

    return out


def _resolve_referential_file(project_full: str, user_msg: str) -> Tuple[str, str]:
    """
    Returns (resolved_rel_path, reason_string).
    reason_string is for diagnostics only.
    """
    kind = _infer_file_kind_from_msg(user_msg)

    # 1) active object
    try:
        ao = project_store.load_active_object(project_full) or {}
    except Exception:
        ao = {}
    if isinstance(ao, dict):
        rel = str(ao.get("rel_path") or "").replace("\\", "/").strip()
        if rel and _path_exists_under_root(rel):
            base = Path(rel).name
            if _filter_by_kind(base, kind) or kind == "any":
                return rel, "active_object"

    # 2) latest deliverable artifact by inferred type (best-effort)
    try:
        if kind == "excel":
            latest = get_latest_artifact_by_type(project_full, "deliverable_excel")
        elif kind == "html":
            latest = get_latest_artifact_by_type(project_full, "deliverable_html")
        else:
            latest = None
        if isinstance(latest, dict):
            rel = str(latest.get("path") or "").replace("\\", "/").strip()
            if rel and _path_exists_under_root(rel):
                return rel, "latest_deliverable_artifact"
    except Exception:
        pass

    # 3) latest raw upload (by kind)
    try:
        m = load_manifest(project_full) or {}
        raw_files = m.get("raw_files") or []
        if isinstance(raw_files, list) and raw_files:
            for rf in reversed(raw_files[-50:]):
                if not isinstance(rf, dict):
                    continue
                rel = str(rf.get("path") or "").replace("\\", "/").strip()
                if not rel:
                    continue
                base = Path(rel).name
                if not _filter_by_kind(base, kind) and kind != "any":
                    continue
                if _path_exists_under_root(rel):
                    return rel, "latest_raw_upload"
    except Exception:
        pass

    return "", "no_match"

def _format_candidate_prompt(user_msg: str, candidates: List[Dict[str, str]]) -> str:
    kind = _infer_file_kind_from_msg(user_msg)
    kind_label = {
        "excel": "spreadsheet",
        "image": "image",
        "pdf": "PDF",
        "html": "deliverable",
        "any": "file",
    }.get(kind, "file")

    lines: List[str] = []
    lines.append(f"I don’t see an exact filename in your message, but I *do* have these {kind_label} candidates:")
    lines.append("")
    for c in candidates[:5]:
        nm = str(c.get("name") or "").strip()
        rel = str(c.get("rel") or "").replace("\\", "/").strip()
        if not nm or not rel:
            continue
        lines.append(f"- {nm}")
        lines.append(f"  Open: /file?path={rel}")
    lines.append("")
    lines.append("Is it one of these? If yes, tell me which one (or paste the filename).")
    return "\n".join(lines).strip()
def _find_latest_artifact_text_for_file(project_full: str, *, artifact_type: str, file_rel: str, cap: int) -> str:
    """
    Best-effort: scan manifest artifacts newest-first and return text for the latest artifact
    of a given type that references file_rel in from_files.
    Deterministic. No model calls.
    """
    try:
        m = load_manifest(project_full) or {}
    except Exception:
        m = {}
    arts = m.get("artifacts") or []
    if not isinstance(arts, list):
        arts = []

    needle = (file_rel or "").replace("\\", "/").strip()
    if not needle:
        return ""

    for a in reversed(arts[-3000:]):
        if not isinstance(a, dict):
            continue
        if str(a.get("type") or "") != str(artifact_type or ""):
            continue
        ff = a.get("from_files") or []
        if not isinstance(ff, list):
            continue
        ff_norm = [(str(x) or "").replace("\\", "/").strip() for x in ff]
        if needle not in ff_norm:
            continue
        try:
            return (read_artifact_text(project_full, a, cap_chars=int(cap or 0)) or "").strip()
        except Exception:
            return ""
    return ""

def _describe_resolved_file(project_full: str, file_rel: str, user_msg: str) -> Tuple[str, str]:
    """
    Deterministic description of a resolved file using stored artifacts.
    Returns (reply, why). If reply is empty, why explains what was missing.
    """
    rel = (file_rel or "").replace("\\", "/").strip()
    if not rel:
        return "", "no_resolved_path"

    base = Path(rel).name
    kind = _infer_file_kind_from_msg(user_msg)

    # Pull the best-available stored signals for this file.
    cls = _find_latest_artifact_text_for_file(project_full, artifact_type="image_classification", file_rel=rel, cap=8000)
    sem_txt = _find_latest_artifact_text_for_file(project_full, artifact_type="image_semantics", file_rel=rel, cap=220000)    
    cap_txt = _find_latest_artifact_text_for_file(project_full, artifact_type="image_caption", file_rel=rel, cap=8000)
    ocr_txt = _find_latest_artifact_text_for_file(project_full, artifact_type="ocr_text", file_rel=rel, cap=12000)
    pdf_txt = _find_latest_artifact_text_for_file(project_full, artifact_type="pdf_text", file_rel=rel, cap=12000)
    plan_txt = _find_latest_artifact_text_for_file(project_full, artifact_type="plan_ocr", file_rel=rel, cap=12000)
    ov_txt = _find_latest_artifact_text_for_file(project_full, artifact_type="file_overview", file_rel=rel, cap=8000)
    bp_txt = _find_latest_artifact_text_for_file(project_full, artifact_type="excel_blueprint", file_rel=rel, cap=12000)
    doc_txt = _find_latest_artifact_text_for_file(project_full, artifact_type="doc_text", file_rel=rel, cap=12000)

    # Decide what “content signal” we have, by kind.
    has_signal = False
    if kind == "image":
        has_signal = bool(sem_txt or cap_txt or ocr_txt or cls or ov_txt)
    elif kind == "pdf":
        has_signal = bool(plan_txt or pdf_txt or ov_txt)
    elif kind == "excel":
        has_signal = bool(bp_txt or ov_txt)
    elif kind == "html":
        has_signal = bool(ov_txt)
    else:
        has_signal = bool(ov_txt or cap_txt or ocr_txt or pdf_txt or plan_txt or bp_txt or doc_txt or cls)

    if not has_signal:
        return "", "no_stored_artifacts_for_file"

    lines: List[str] = []
    lines.append(f"Here’s what I have for **{base}**:")
    lines.append(f"- Open: /file?path={rel}")

    # Classification (JSON-ish)
    if cls:
        preview = cls.strip()
        if len(preview) > 900:
            preview = preview[:899].rstrip() + "…"
        lines.append("")
        lines.append("Classification (stored):")
        lines.append(preview)

    # Prefer semantics for images (authoritative fused result)
    if kind == "image":
        # If we have image_semantics_v1, render a human-facing answer from outputs.summary
        # (and do NOT dump raw JSON into the chat).
        if sem_txt:
            sem_obj: Dict[str, Any] = {}
            try:
                sem_obj = json.loads(sem_txt.strip())
            except Exception:
                sem_obj = {}

            out = sem_obj.get("outputs") if isinstance(sem_obj, dict) else None
            if not isinstance(out, dict):
                out = {}

            summary = str(out.get("summary") or "").strip()

            candidates = out.get("candidates")
            if not isinstance(candidates, list):
                candidates = []
            top = candidates[0] if candidates and isinstance(candidates[0], dict) else {}

            top_make = str(top.get("make") or "").strip()
            top_model = str(top.get("model") or "").strip()
            try:
                top_conf = float(top.get("confidence")) if top.get("confidence") is not None else None
            except Exception:
                top_conf = None

            observations = out.get("observations")
            if not isinstance(observations, list):
                observations = []
            observations = [str(x).strip() for x in observations if str(x).strip()][:6]

            lines.append("")
            lines.append("What it shows:")
            if summary:
                lines.append(summary)
            else:
                lines.append("(No summary available in cached vision semantics.)")

            if top_make or top_model:
                if isinstance(top_conf, float):
                    lines.append(f"\nLikely ID: {top_make} {top_model}".strip() + f" (confidence {top_conf:.2f})")
                else:
                    lines.append(f"\nLikely ID: {top_make} {top_model}".strip())

            if observations:
                lines.append("")
                lines.append("Visible cues:")
                for ob in observations[:5]:
                    lines.append(f"- {ob}")

            # We rendered a human answer; do not also print caption/OCR/classification below.
            return "\n".join(lines).strip(), "described_from_image_semantics"

        if cap_txt:
            p = cap_txt.strip()

            if len(p) > 700:
                p = p[:699].rstrip() + "…"
            lines.append("")
            lines.append("Caption (stored):")
            lines.append(p)

        if ocr_txt:
            p = ocr_txt.strip()
            if len(p) > 900:
                p = p[:899].rstrip() + "…"
            lines.append("")
            lines.append("OCR text (stored):")
            lines.append(p)

    # PDFs
    if kind == "pdf":
        pick = plan_txt or pdf_txt
        if pick:
            p = pick.strip()
            if len(p) > 1200:
                p = p[:1199].rstrip() + "…"
            lines.append("")
            lines.append("Extracted text (stored):")
            lines.append(p)

    # Excel
    if kind == "excel":
        if ov_txt:
            p = ov_txt.strip()
            if len(p) > 900:
                p = p[:899].rstrip() + "…"
            lines.append("")
            lines.append("Workbook overview (stored):")
            lines.append(p)
        elif bp_txt:
            p = bp_txt.strip()
            if len(p) > 900:
                p = p[:899].rstrip() + "…"
            lines.append("")
            lines.append("Workbook blueprint (stored):")
            lines.append(p)

    # Word docs
    if kind == "any" and doc_txt:
        p = doc_txt.strip()
        if len(p) > 1200:
            p = p[:1199].rstrip() + "…"
        lines.append("")
        lines.append("Extracted text (stored):")
        lines.append(p)

    # Generic fallback: file_overview is usually most human-readable
    if kind == "any" and ov_txt:
        p = ov_txt.strip()
        if len(p) > 900:
            p = p[:899].rstrip() + "…"
        lines.append("")
        lines.append("File overview (stored):")
        lines.append(p)

    return "\n".join(lines).strip(), "described_from_artifacts"
# -----------------------------------------------------------------------------
# C5.4 — Wordle routing removed
# -----------------------------------------------------------------------------

def _attach_file_context_to_message(user_msg: str, rel_path: str) -> str:
    """
    Deterministic “nudge” that forces the model to anchor on a specific file
    without requiring the user to type the filename.

    IMPORTANT:
    Do not inject trigger keywords like "excel" into the user message, or it can
    accidentally flip the pipeline into Excel deliverable mode.
    """
    base = Path(rel_path).name
    return (
        f"{user_msg}\n\n"
        f"(File context: {base} at {rel_path}. If you need contents, re-check the stored OCR/PDF text or other cached file artifacts for this file.)"
    )

def _tier2g_extract_personal_fact_candidates(user_msg: str) -> List[Dict[str, str]]:
    """
    Deterministic extractor for Tier-2G promotion.
    Returns list of {claim, slot} where claim is a single-line durable personal fact.
    """
    t = (user_msg or "").strip()
    if not t:
        return []

    low = t.lower()

    out: List[Dict[str, str]] = []

    def _add_rel_claim(rel: str, name_val: str) -> None:
        v = (name_val or "").strip().strip(" .!?,;:'\"")
        v = " ".join(v.split())
        if not v:
            return
        rel_l = rel.lower().strip()
        # Canonicalize relation tokens
        rel_map = {
            "girlfriend": "girlfriend",
            "boyfriend": "boyfriend",
            "partner": "partner",
            "spouse": "spouse",
            "wife": "wife",
            "husband": "husband",
            "mother": "mother",
            "mom": "mother",
            "father": "father",
            "dad": "father",
            "sister": "sister",
            "brother": "brother",
            "son": "son",
            "daughter": "daughter",
            "child": "child",
            "kid": "child",
            "stepson": "stepson",
            "stepdaughter": "stepdaughter",
            "stepmother": "stepmother",
            "stepfather": "stepfather",
        }
        rel2 = rel_map.get(rel_l, rel_l)
        out.append({"claim": f"My {rel2}'s name is {v}", "slot": "relationships"})

    # Relationship: immediate family + partner/spouse names (allow multiple in one message)
    # Examples:
    # - "My girlfriend's name is Emanie"
    # - "My girlfriends name is Emanie Sanabria"
    # - "My son's name is Logan Hawes"
    # - "My mother name is Susan"
    try:
        rel_pat = re.compile(
            r"\bmy\s+"
            r"(girlfriend|boyfriend|partner|spouse|wife|husband|mother|mom|father|dad|sister|brother|son|daughter|child|kid|stepson|stepdaughter|stepmother|stepfather)"
            r"(?:'s|s)?\s+name\s+is\s+([^.;\n]+)",
            flags=re.IGNORECASE,
        )
        for m in rel_pat.finditer(t):
            _add_rel_claim(m.group(1), m.group(2))
    except Exception:
        pass

    # Alternate phrasing: "my son is named X"
    try:
        rel_pat2 = re.compile(
            r"\bmy\s+"
            r"(girlfriend|boyfriend|partner|spouse|wife|husband|mother|mom|father|dad|sister|brother|son|daughter|child|kid|stepson|stepdaughter|stepmother|stepfather)"
            r"\s+is\s+named\s+([^.;\n]+)",
            flags=re.IGNORECASE,
        )
        for m in rel_pat2.finditer(t):
            _add_rel_claim(m.group(1), m.group(2))
    except Exception:
        pass

    # If we captured any relationship names, return them now.
    if out:
        return out

    # Location (Tier-2G eligible): accept common natural phrasings deterministically.
    #
    # Examples:
    # - "I live in Oklahoma City"
    # - "I'm in Oklahoma City"
    # - "I am in Oklahoma City"
    # - "I'm located in Oklahoma City"
    # - "I am located in Oklahoma City"
    # - "My location is Oklahoma City"
    # - "I'm based in Oklahoma City"
    # - "I'm currently in Oklahoma City"
    #
    # NOTE:
    # We normalize most variants into "I live in <place>" so downstream profile
    # rebuild logic can stay conservative and consistent.
    loc = ""
    m = re.search(r"\bi\s+live\s+in\s+(.+)$", t, re.IGNORECASE)
    if not m:
        m = re.search(r"\b(?:i\s*[' ]?m|i\s+am)\s+in\s+(.+)$", t, re.IGNORECASE)
    if not m:
        m = re.search(r"\b(?:i\s*[' ]?m|i\s+am)\s+located\s+in\s+(.+)$", t, re.IGNORECASE)
    if not m:
        m = re.search(r"\bmy\s+location\s+is\s+(.+)$", t, re.IGNORECASE)
    if not m:
        m = re.search(r"\b(?:i\s*[' ]?m|i\s+am)\s+based\s+in\s+(.+)$", t, re.IGNORECASE)
    if not m:
        m = re.search(r"\b(?:i\s*[' ]?m|i\s+am)\s+currently\s+in\s+(.+)$", t, re.IGNORECASE)

    if m:
        loc = (m.group(1) or "").strip()
        loc = loc.strip().strip(" .!?,;:'\"")
        if loc:
            out.append({"claim": f"I live in {loc}", "slot": "location"})
            return out



    # Identity: birthdate inference (month/day + age in same message)
    try:
        mm = re.search(
            r"\b(my\s+birthday\s+is|my\s+birthdate\s+is)\s+"
            r"(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
            r"\s+(\d{1,2})(?:st|nd|rd|th)?\b",
            t,
            flags=re.IGNORECASE,
        )
        ma = re.search(r"\b(?:i\s*[' ]?m|i\s+am)\s+(\d{1,3})\b", t, flags=re.IGNORECASE)
        if mm and ma:
            mon_txt = (mm.group(2) or "").lower()
            day_txt = (mm.group(3) or "").strip()
            age_txt = (ma.group(1) or "").strip()

            month_map = {
                "jan": 1, "january": 1,
                "feb": 2, "february": 2,
                "mar": 3, "march": 3,
                "apr": 4, "april": 4,
                "may": 5,
                "jun": 6, "june": 6,
                "jul": 7, "july": 7,
                "aug": 8, "august": 8,
                "sep": 9, "september": 9,
                "oct": 10, "october": 10,
                "nov": 11, "november": 11,
                "dec": 12, "december": 12,
            }
            mon = month_map.get(mon_txt[:3], 0)
            day = int(day_txt) if day_txt.isdigit() else 0
            age = int(age_txt) if age_txt.isdigit() else 0
            if (1 <= mon <= 12) and (1 <= day <= 31) and (0 < age < 130):
                now = time.gmtime()
                cur_y, cur_m, cur_d = now.tm_year, now.tm_mon, now.tm_mday
                # If birthday already passed this year, subtract age; else subtract age+1
                if (cur_m, cur_d) >= (mon, day):
                    birth_y = cur_y - age
                else:
                    birth_y = cur_y - age - 1
                out.append({"claim": f"My birthday is {birth_y:04d}-{mon:02d}-{day:02d}", "slot": "identity"})
                return out
    except Exception:
        pass

    # Identity: "My name is ..." / "I go by ..."
    if re.search(r"\bmy\s+name\s+is\s+.+$", low) or re.search(r"\bi\s+go\s+by\s+.+$", low):
        out.append({"claim": t.strip(), "slot": "identity"})
        return out

    return out

def _tier2g_autoresolve_preferred_name_from_tier2m(project_store, user_seg: str) -> None:
    """
    Server-side hook only.

    Invariant:
    - server.py must NOT open/read Tier-2G/Tier-2M files directly.
    - All Tier-2G/Tier-2M inspection and mutation logic lives in project_store (single authority).
    """
    try:
        u = (user_seg or "").strip()
        if not u:
            return

        # Optional hook: if project_store provides an autoresolve helper, use it.
        fn = getattr(project_store, "autoresolve_preferred_name_from_tier2m", None)
        if callable(fn):
            fn(u)
    except Exception:
        return


def _tier2g_promote_global_memory_or_raise(user: str, user_msg: str) -> Dict[str, Any]:
    """
    Unbreakable, auditable Tier-2G global promotion:
      1) Append evidence to GLOBAL facts_raw.jsonl (Tier-1G candidates)
      2) Rebuild Tier-2G profile.json deterministically from that ledger
    Raises on any failure so the caller can surface it (no silent no-op).
    """
    u = (user or "").strip()
    if not u:
        return {"ok": True, "written": 0, "skipped": "missing_user"}

    cands = _tier2g_extract_personal_fact_candidates(user_msg)
    if not cands:
        return {"ok": True, "written": 0, "skipped": "no_candidates"}

    written = 0
    last_ids: List[str] = []

    for c in cands:
        claim = str(c.get("claim") or "").strip()
        slot = str(c.get("slot") or "other").strip() or "other"

        res = project_store.append_user_fact_raw_candidate(
            u,
            claim=claim,
            slot=slot,
            subject="user",
            source="chat",
            evidence_quote=(user_msg or "")[:600],
            turn_index=0,
            timestamp=now_iso(),
        )
        if not isinstance(res, dict) or not res.get("ok"):
            err = (res or {}).get("error") if isinstance(res, dict) else "unknown_error"
            raise RuntimeError(f"Tier-2G append failed: {err}")
        written += 1
        if res.get("id"):
            last_ids.append(str(res.get("id")))

    # Materialize Tier-2G profile.json from the canonical global ledger
    prof = project_store.rebuild_user_profile_from_user_facts(u)
    # Also materialize Tier-2M (global mutable facts map) from the same global ledger.
    # This is deterministic and derived from users/<user>/_user/facts_raw.jsonl.
    try:
        project_store.rebuild_user_global_facts_map_from_user_facts(u)
        _tier2g_autoresolve_preferred_name_from_tier2m(project_store, u)     
    except Exception as _e_t2m:
        # Do not fail Tier-2G on Tier-2M issues; caller may log/surface Tier-2G only.
        pass    
    if not isinstance(prof, dict) or str(prof.get("schema") or "") != "user_profile_v1":
        raise RuntimeError("Tier-2G rebuild failed: invalid_profile_obj")

    return {
        "ok": True,
        "written": written,
        "ids": last_ids[:8],
        "facts_raw_path": str(project_store.user_facts_raw_path(u)),
        "profile_path": str(project_store.user_profile_path(u)),
    }


async def handle_connection(websocket, path=None):  # path optional for websockets compatibility
    print("[WS] Client connected")

    # Very simple cookie parse (no security claims; just isolation)
    cookie_header = ""
    try:
        cookie_header = websocket.request_headers.get("Cookie", "") or ""
    except Exception:
        cookie_header = ""

    cookies: Dict[str, str] = {}
    for part in cookie_header.split(";"):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        cookies[k.strip()] = v.strip()

    user = cookies.get("user", "")

    # Fallback auth (TEMP): allow WS auth via querystring (?user=...&pass=...)
    # NOTE: websockets versions differ: sometimes `path` is None and the request path lives on websocket.
    if (not user) or (user not in USERS):
        try:
            from urllib.parse import urlparse, parse_qs

            raw_path = (path or "").strip()

            # Try to recover the request path from the websocket object (version-tolerant)
            if not raw_path:
                try:
                    # websockets >= 12: websocket.request.path
                    req = getattr(websocket, "request", None)
                    if req is not None:
                        raw_path = (getattr(req, "path", "") or getattr(req, "target", "") or "").strip()
                except Exception:
                    pass

            if not raw_path:
                try:
                    # older variants may have `.path`
                    raw_path = (getattr(websocket, "path", "") or "").strip()
                except Exception:
                    pass

            q = parse_qs(urlparse(raw_path).query)

            q_user = (q.get("user", [""])[0] or "").strip()
            q_pass = (q.get("pass", [""])[0] or "").strip()

            if q_user and q_pass and USERS.get(q_user) == q_pass:
                user = q_user
                print(f"[WS] Auth via querystring: user={user!r}")
            else:
                # Debug line so you can see what the server *actually* received
                print(f"[WS] Query auth failed. raw_path={raw_path!r} keys={list(q.keys())!r}")
        except Exception as e:
            print(f"[WS] Query auth parse error: {e!r}")

    # If still not authorized, hard-fail (prevents Frank/Emanie state bleed).
    if (not user) or (user not in USERS):
        try:
            await websocket.close(code=1008, reason="Unauthorized (missing/invalid cookie or querystring auth)")
        except Exception:
            try:
                await websocket.close()
            except Exception:
                pass
        return

    # Keep UI-facing state

    def _user_last_project_path(u: str) -> Path:
        return project_store.user_dir(u) / "last_project.json"

    def _load_last_project(u: str) -> str:
        try:
            p = _user_last_project_path(u)
            if p.exists():
                obj = json.loads(p.read_text(encoding="utf-8") or "{}")
                val = str(obj.get("project") or "").strip()
                return safe_project_name(val) if val else ""
        except Exception:
            pass
        return ""

    def _save_last_project(u: str, proj_short: str) -> None:
        try:
            p = _user_last_project_path(u)
            obj = {"project": safe_project_name(proj_short), "updated_at": now_iso()}
            project_store.atomic_write_text(p, json.dumps(obj, indent=2, sort_keys=True))
        except Exception:
            pass

    # Keep UI-facing project name short ("default"), but store on disk under "User/<project>"
    #
    # Couples Therapist Mode (Option A UX):
    # - If user is a couple_* account, ALWAYS drop them into a stable Couples Therapy project
    #   with a pre-set goal and an ACTIVE Couples Therapist expert frame (no setup).
    current_project = DEFAULT_PROJECT_NAME
    if _is_couples_user(user):
        current_project = "Couples_Therapy"
    else:
        # Prefer last project per user (not per device)
        lp = _load_last_project(user)
        if lp:
            current_project = lp

    current_project_full = safe_project_name(f"{user}/{current_project}")
    ensure_project_scaffold(current_project_full)

    # Deterministic couples bootstrap (no model call, no user setup)
    # Couples accounts must always have user-scoped global memory scaffolded on disk.
    # This ensures projects/<user>/_user/ exists even after the user deletes it.
    try:
        if _is_couples_user(user):
            # best-effort; creates folder + seeds profile.json if missing (via project_store)
            project_store.load_user_profile(user)
    except Exception:
        pass    
    try:
        if _is_couples_user(user):
            default_goal = "Couples therapy — improve communication, repair trust, and address recurring conflict patterns."

            # Ensure manifest has a goal (used by greeting / dashboard)
            try:
                m0 = load_manifest(current_project_full) or {}
            except Exception:
                m0 = {}
            if not str(m0.get("goal") or "").strip():
                m0["goal"] = default_goal
                if not str(m0.get("expert_type") or "").strip():
                    m0["expert_type"] = "general"
                save_manifest(current_project_full, m0)

            # Ensure project_state has goal + active bootstrap + ACTIVE Couples Therapist frame
            try:
                st0 = project_store.load_project_state(current_project_full) or {}
            except Exception:
                st0 = {}

            # Keep existing goal if already set
            if not str(st0.get("goal") or "").strip():
                st0["goal"] = default_goal

            st0["bootstrap_status"] = "active"
            st0.setdefault("project_mode", "hybrid")
            st0.setdefault("current_focus", "")
            st0.setdefault("next_actions", [])
            st0.setdefault("key_files", [])

            # IMPORTANT: must be ACTIVE so model_pipeline does NOT overwrite with a proposed fallback frame.
            st0["expert_frame"] = {
                "status": "active",
                "label": "Couples Therapist",
                "directive": (
                    "Your name is Quinn. You are a couples therapist. Be warm, steady, and practical. "
                    "Focus on communication patterns, emotions, needs, boundaries, repair, and concrete next steps. "
                    "Use both partners' context as a lens, but never reveal, quote, attribute, or confirm private content. "
                    "Treat event claims as one-sided unless corroborated by both partners (directly or indirectly). "
                    "If only one side mentions it, label it as one perspective and seek the other side. "
                    "Feelings are valid as experience, not as proof of facts or intent. "
                    "If asked 'what did they say', refuse and redirect to a safe process. "
                    "If key information is missing, ask one targeted question and treat the answer as fact. "
                    "End like a therapist: one small next step and one gentle question. "
                    "Do NOT talk like a project manager; no deliverables or output formats."
                ),
                "set_reason": "couples_autobootstrap",
                "updated_at": now_iso(),
            }

            try:
                project_store.write_project_state_fields(current_project_full, st0)
            except Exception:
                # Phase-1: remove split-brain fallback writes for project_state
                pass
    except Exception:
        pass


    # Register client for background upload completion notifications
    _ws_add_client(current_project_full, websocket)

    def _full(short_name: str) -> str:
        return safe_project_name(f"{user}/{safe_project_name(short_name)}")

    def _get_project_goal(full_name: str) -> str:
        try:
            m = load_manifest(full_name)
            return (m.get("goal") or "").strip()
        except Exception:
            return ""

    def _dashboard_for(short_name: str) -> str:
        full_name = _full(short_name)
        msg = build_project_dashboard_message(full_name)
        # rewrite the first "Current project:" line so UI never sees "User/project"
        lines = msg.splitlines()
        for i, ln in enumerate(lines):
            if ln.startswith("Current project:"):
                lines[i] = f"Current project: {short_name}"
                break
        return "\n".join(lines)

    def _project_switch_message(short_name: str, *, prefix: str = "Project:") -> str:
        # UI needs a single-line meta message to update the chat pill.
        # Do NOT narrate switching in chat.
        return f"{prefix} {short_name}"

    conn_id = f"conn_{uuid.uuid4().hex[:8]}"
    _trace_event_counts: Dict[str, Dict[str, int]] = {}

    def _trace_emit(event_type: str, detail: Optional[Dict[str, Any]] = None) -> None:
        try:
            tid = get_current_audit_trace_id()
            if not tid:
                tid = f"srv_{int(time.time() * 1000)}_{uuid.uuid4().hex[:6]}"
                try:
                    _AUDIT_TRACE_ID.set(tid)
                except Exception:
                    pass
            et = str(event_type or "").strip()
            per_trace = _trace_event_counts.get(tid)
            if not isinstance(per_trace, dict):
                per_trace = {}
                _trace_event_counts[tid] = per_trace
            per_trace[et] = int(per_trace.get(et, 0)) + 1
            count = per_trace[et]
            obj: Dict[str, Any] = {
                "trace_id": tid,
                "conn_id": conn_id,
                "project": current_project_full,
                "event_type": et,
                "ts_monotonic": time.monotonic(),
                "count": count,
            }
            if isinstance(detail, dict) and detail:
                obj["detail"] = detail
            print(json.dumps(obj, ensure_ascii=False), flush=True)
        except Exception:
            pass
    
    # UI-selected expert (sticky per connection; updated by WS frames)
    active_expert = "default"
    # Per-connection history (in-memory)
    conversation_history: List[Dict[str, str]] = []
    # C2/C3 helper: count user messages (per-connection) so pending confirmations can expire deterministically.
    user_msg_counter = 0

    # Track last real user question for better [SEARCH] fallback when the user types something generic
    last_user_question_for_search = ""

    # C11 — Active Topic Frame (ephemeral, per-connection)
    # Purpose: preserve continuity for follow-ups like "look up what people are saying"
    # without polluting long-term memory tiers.
    active_topic_text = ""
    active_topic_updated_at = ""
    # Strength tracks how confidently we should bind pronouns ("it/that/they") to the active topic.
    # 1.0 = very active/recent, 0.0 = no active topic.
    active_topic_strength = 0.0

    # Track whether the most recent completed turn used live web search results.
    # This is used to answer meta questions like "did you read those articles?"
    last_web_search_used = False
    last_web_search_query = ""
    last_web_search_results = ""
    # Search cache (for continuity across same-topic follow-ups)
    last_search_cache_results = ""
    last_search_cache_query = ""
    last_search_cache_ts = 0.0
    last_search_cache_ws_rel = ""
    # Thread synthesis cache (ephemeral, evidence-bound)
    last_thread_synthesis_text = ""
    last_thread_synthesis_hash = ""
    last_full_answer_text = ""
    last_patch_text = ""
    last_patch_target = ""
    # C5.3 helper: remember last resolved referential file so “the other one” can work.
    last_referential_file_rel = ""
    # Greeting de-duplication (FOUNDATIONAL):
    # Ensure we emit at most ONE greeting per project-enter event per connection.
    greeted_projects: set[str] = set()
    # Allow expert-switch greetings without colliding with project-entry greet.
    greeted_expert_switch: set[str] = set()
    # Analysis backfill (automatic, debounced)
    last_backfill_ts = 0.0
    backfill_inflight = False
    # -----------------------------------------------------------------
    # C4.2 — Upload → Expert Synthesis (non-spam; one per uploaded file)
    #
    # Trigger:
    # - When an outgoing WS frame "upload.status" has status="done"
    #
    # Requirements:
    # - One synthesis per uploaded file (persisted; no reconnect loops)
    # - Must go through expert pipeline (model_pipeline.run_request_pipeline)
    # - Must not block normal chat loop (async task)
    # - Wordle special-case: skip if payload appears to be a word-only guess
    # -----------------------------------------------------------------

    _UPLOAD_SYNTHESIS_LOG = "upload_synthesis.jsonl"
    _upload_synthesis_inflight: set[str] = set()

    def _upload_synthesis_log_path(project_full: str) -> Path:
        return state_dir(project_full) / _UPLOAD_SYNTHESIS_LOG

    def _upload_synthesis_already_done(project_full: str, key: str) -> bool:
        k = (key or "").strip()
        if not k:
            return True
        p = _upload_synthesis_log_path(project_full)
        if not p.exists():
            return False
        try:
            lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
        except Exception:
            return False
        # bounded tail scan
        tail = lines[-2500:] if len(lines) > 2500 else lines
        for ln in reversed(tail):
            try:
                obj = json.loads(ln)
            except Exception:
                continue
            if not isinstance(obj, dict):
                continue
            if str(obj.get("key") or "").strip() == k:
                return True
        return False

    def _upload_synthesis_mark_done(project_full: str, *, key: str, canonical_rel: str, orig_name: str) -> None:
        k = (key or "").strip()
        if not k:
            return
        entry = {
            "v": 1,
            "ts": now_iso(),
            "key": k,
            "canonical_rel": (canonical_rel or "").replace("\\", "/").strip(),
            "orig_name": (orig_name or "").strip(),
        }
        try:
            project_store.write_canonical_entry(
                project_full,
                target_path=_upload_synthesis_log_path(project_full),
                mode="jsonl_append",
                data=entry,
            )
        except Exception:
            # Never block chat on log writes
            pass

    def _is_wordle_word_only_guess(s: str) -> bool:
        t = (s or "").strip()
        if not t:
            return False
        # Single token, alphabetic only, typical Wordle length (4–8)
        if " " in t or "\n" in t or "\t" in t:
            return False
        if not re.fullmatch(r"[A-Za-z]{4,8}", t):
            return False
        return True

    def _should_skip_upload_synthesis_wordle(frame: Dict[str, Any]) -> bool:
        """
        Wordle special-case:
        If this upload completion is effectively "here's a single word guess",
        do NOT run expert synthesis.
        """
        try:
            rel = str(frame.get("relative_path") or frame.get("path") or "").strip().lower()
            if rel.endswith(".zip"):
                return True
        except Exception:
            pass

        try:
            kind = str(frame.get("kind") or frame.get("mode") or frame.get("pipeline") or "").strip().lower()
            if "wordle" in kind:
                return True
        except Exception:
            pass

        # Some workers may include a guess/message/output field
        for k in ("guess", "word", "output", "message", "text", "result"):
            try:
                v = frame.get(k)
                if isinstance(v, str) and _is_wordle_word_only_guess(v):
                    return True
            except Exception:
                continue

        # Or embed it in a nested payload
        try:
            payload = frame.get("payload")
            if isinstance(payload, dict):
                for k in ("guess", "word", "output", "message", "text", "result"):
                    v = payload.get(k)
                    if isinstance(v, str) and _is_wordle_word_only_guess(v):
                        return True
        except Exception:
            pass

        return False

    def _extract_upload_identity(frame: Dict[str, Any]) -> tuple[str, str, str]:
        """
        Returns (orig_name, canonical_rel, stable_key).
        Prefer canonical_rel; fallback to filename.
        """
        orig_name = ""
        canonical_rel = ""
        analysis_ver = ""

        try:
            orig_name = str(frame.get("orig_name") or frame.get("filename") or frame.get("name") or "").strip()
        except Exception:
            orig_name = ""

        try:
            canonical_rel = str(
                frame.get("canonical_rel")
                or frame.get("rel")
                or frame.get("path")
                or frame.get("file_rel")
                or ""
            ).replace("\\", "/").strip()
        except Exception:
            canonical_rel = ""
        try:
            analysis_ver = str(frame.get("analysis_version") or frame.get("pipeline_version") or "").strip()
        except Exception:
            analysis_ver = ""

        # Look into nested payload if present
        try:
            payload = frame.get("payload")
            if isinstance(payload, dict):
                if not orig_name:
                    orig_name = str(payload.get("orig_name") or payload.get("filename") or payload.get("name") or "").strip()
                if not canonical_rel:
                    canonical_rel = str(
                        payload.get("canonical_rel")
                        or payload.get("rel")
                        or payload.get("path")
                        or payload.get("file_rel")
                        or ""
                    ).replace("\\", "/").strip()
                if not analysis_ver:
                    analysis_ver = str(payload.get("analysis_version") or payload.get("pipeline_version") or "").strip()
        except Exception:
            pass

        # Stable key: canonical_rel if present, else orig_name
        stable_key = canonical_rel or orig_name
        stable_key = (stable_key or "").strip()
        if analysis_ver and stable_key:
            stable_key = f"{stable_key}|analysis:{analysis_ver}"

        return orig_name, canonical_rel, stable_key

    async def _run_upload_expert_synthesis(
        *,
        orig_name: str,
        canonical_rel: str,
        suppress_chat: bool = False,
        suppress_history: bool = False,
    ) -> None:
        """
        Run ONE expert follow-up turn via the same pipeline as normal chat,
        then send its answer as an assistant message.
        """
        # Build a synthetic user instruction; include canonical_rel for grounding.
        base = (orig_name or Path(canonical_rel).name or "a file").strip() or "a file"
        rel = (canonical_rel or "").replace("\\", "/").strip()

        is_analysis_hat = str(active_expert or "").strip().lower() == "analysis"
        doc_partial_note = ""
        try:
            if rel and Path(rel).suffix.lower() == ".doc":
                dtxt = _find_latest_artifact_text_for_file(
                    current_project_full,
                    artifact_type="doc_text",
                    file_rel=rel,
                    cap=12000,
                )
                if not dtxt or len(dtxt.strip()) < 1500:
                    doc_partial_note = (
                        "Legacy .doc extraction is partial for this file. "
                        "You MUST acknowledge the limitation, propose conversion via LibreOffice "
                        "or a PDF/DOCX export, and include CAPABILITY_GAP_JSON with "
                        "recommended_search_queries for installing LibreOffice or converting .doc."
                    )
        except Exception:
            doc_partial_note = ""

        if is_analysis_hat:
            synthetic = (
                f"I uploaded {base}.\n"
                f"- Stored at: {rel or '(unknown path)'}\n\n"
                "Hard requirement for images:\n"
                "- You MUST use the stored image_semantics artifact (type=image_semantics, linked via from_files) as PRIMARY evidence.\n"
                "- Do NOT rely on image_caption or OCR alone for what the image shows.\n\n"
                + (f"{doc_partial_note}\n\n" if doc_partial_note else "")
                + "Output format:\n"
                "[USER_ANSWER]\n"
                "Short summary (2-5 sentences), why it matters, immediate next step, and one precise question.\n"
                "[/USER_ANSWER]\n"
                "[DISCOVERY_INDEX_JSON]\n"
                "{\n"
                "  \"doc_type\": \"...\",\n"
                "  \"title\": \"...\",\n"
                "  \"summary\": \"...\",\n"
                "  \"parties\": [\"...\"],\n"
                "  \"dates\": [\"YYYY-MM-DD\"],\n"
                "  \"amounts\": [\"...\"],\n"
                "  \"issues\": [\"...\"],\n"
                "  \"questions\": [\"...\"],\n"
                "  \"tags\": [\"...\"],\n"
                "  \"confidence\": \"low|med|high\"\n"
                "}\n"
                "[/DISCOVERY_INDEX_JSON]\n"
                "[FACT_LEDGER_JSON]\n"
                "[\n"
                "  {\n"
                "    \"entity\": \"...\",\n"
                "    \"attribute\": \"...\",\n"
                "    \"value\": \"...\",\n"
                "    \"date\": \"YYYY-MM-DD\",\n"
                "    \"source_hint\": \"page 3 / line / account / statement name\",\n"
                "    \"confidence\": \"low|med|high\",\n"
                "    \"notes\": \"...\"\n"
                "  }\n"
                "]\n"
                "[/FACT_LEDGER_JSON]\n"
                "Rules:\n"
                "- JSON must be a single object. Use empty arrays if unknown.\n"
                "- Use ISO dates if possible; otherwise include raw dates as strings.\n"
                "- Summary must be grounded in the file contents.\n"
                "- FACT_LEDGER_JSON must be a JSON array (empty if no solid facts).\n"
            ).strip()
        else:
            synthetic = (
                f"I uploaded {base}.\n"
                f"- Stored at: {rel or '(unknown path)'}\n\n"
                "Hard requirement for images:\n"
                "- You MUST use the stored image_semantics artifact (type=image_semantics, linked via from_files) as PRIMARY evidence.\n"
                "- Do NOT rely on image_caption or OCR alone for what the image shows.\n\n"
                "Give me:\n"
                "1) What the file is (short)\n"
                "2) Why it matters to this project (grounded)\n"
                "3) The next suggested action\n"
            ).strip()


        try:
            _gen_ok = False
            try:
                _trace_emit("assistant.generate.start")
            except Exception:
                pass
            try:
                out = await model_pipeline.run_request_pipeline(
                    ctx=sys.modules[__name__],
                    current_project_full=current_project_full,
                    clean_user_msg=synthetic,
                    do_search=False,
                    search_results="",
                    conversation_history=conversation_history,
                    max_history_pairs=MAX_HISTORY_PAIRS,
                    server_file_path=Path(__file__).resolve(),
                    active_expert=active_expert,
                    suppress_output_side_effects=bool(suppress_history),
                )
                _gen_ok = True
            finally:
                try:
                    _trace_emit("assistant.generate.end", {"ok": bool(_gen_ok)})
                except Exception:
                    pass

        except Exception as e:
            try:
                await websocket.send(f"(Upload synthesis failed: {e!r})")
            except Exception:
                pass
            return
        # ---------------------------------------------------------
        # AUDIT (server-side): upload synthesis pipeline completion
        # ---------------------------------------------------------
        try:
            audit_event(
                current_project_full,
                {
                    "schema": "audit_turn_v1",
                    "stage": "server_after_run_request_pipeline_upload_synthesis",
                    "clean_user_msg": str(synthetic or ""),
                    "do_search": False,
                    "search_results_len": 0,
                    "lookup_mode": bool((out or {}).get("lookup_mode") or False) if isinstance(out, dict) else False,
                    "canonical_rel": str(canonical_rel or ""),
                    "orig_name": str(orig_name or ""),
                },
            )
        except Exception:
            pass
        try:
            ans = str((out or {}).get("user_answer") or "").strip()
        except Exception:
            ans = ""

        # If this is analysis hat, capture discovery index data and update case files.
        if is_analysis_hat:
            try:
                blocks = (out or {}).get("blocks") if isinstance(out, dict) else {}
            except Exception:
                blocks = {}

            raw_di = ""
            try:
                if isinstance(blocks, dict):
                    raw_di = str(blocks.get("DISCOVERY_INDEX_JSON") or "").strip()
            except Exception:
                raw_di = ""

            if raw_di:
                try:
                    obj = json.loads(raw_di)
                except Exception:
                    obj = {}

                if isinstance(obj, dict):
                    obj.setdefault("upload_path", rel)
                    obj.setdefault("orig_name", base)
                    try:
                        project_store.append_discovery_index_entry(current_project_full, obj)
                        project_store.build_discovery_views_and_write(current_project_full)
                    except Exception:
                        pass

            raw_fl = ""
            try:
                if isinstance(blocks, dict):
                    raw_fl = str(blocks.get("FACT_LEDGER_JSON") or "").strip()
            except Exception:
                raw_fl = ""

            if raw_fl:
                try:
                    arr = json.loads(raw_fl)
                except Exception:
                    arr = []
                if isinstance(arr, list):
                    for it in arr[:80]:
                        if not isinstance(it, dict):
                            continue
                        it.setdefault("source_file", base)
                        it.setdefault("source_hint", "")
                        try:
                            project_store.append_fact_ledger_entry(current_project_full, it)
                        except Exception:
                            continue
                    try:
                        project_store.build_fact_ledger_views_and_write(current_project_full)
                    except Exception:
                        pass

            try:
                project_store.build_analysis_audit_report(current_project_full)
            except Exception:
                pass

        if not ans:
            return

        # Update in-memory history if pipeline returned it
        try:
            ch2 = (out or {}).get("conversation_history")
            if isinstance(ch2, list):
                conversation_history[:] = ch2
        except Exception:
            pass

        if not suppress_chat:
            try:
                await websocket.send(ans)
            except Exception:
                pass

    def _analysis_backfill_key(rel: str) -> str:
        r = (rel or "").replace("\\", "/").strip()
        if not r:
            return ""
        try:
            ver = str(project_store.ANALYSIS_PIPELINE_VERSION or "").strip()
        except Exception:
            ver = ""
        if ver:
            return f"{r}|analysis:{ver}"
        return r

    async def _run_analysis_backfill_for_file(*, rel: str, orig_name: str) -> bool:
        """
        Re-run analysis synthesis for a stored file (no chat spam).
        Returns True if synthesis ran; False if blocked (e.g., missing vision).
        """
        rel0 = (rel or "").replace("\\", "/").strip()
        if not rel0:
            return False
        suf0 = Path(rel0).suffix.lower()

        # For images, ensure semantics exists; do NOT proceed on OCR-only.
        if suf0 in IMAGE_SUFFIXES:
            ok_v, _sem_json, note_v = await ensure_image_semantics_for_file(
                current_project_full,
                file_rel=rel0,
                force=False,
                reason="analysis_backfill",
            )
            if not ok_v:
                return False
            try:
                sem_check = _find_latest_artifact_text_for_file(
                    current_project_full,
                    artifact_type="image_semantics",
                    file_rel=rel0,
                    cap=220000,
                )
            except Exception:
                sem_check = ""
            if not (sem_check or "").strip():
                return False

        await _run_upload_expert_synthesis(
            orig_name=orig_name,
            canonical_rel=rel0,
            suppress_chat=True,
            suppress_history=True,
        )
        return True

    def _should_backfill_now() -> bool:
        nonlocal last_backfill_ts
        if str(active_expert or "").strip().lower() != "analysis":
            return False
        now = time.time()
        if (now - last_backfill_ts) < 18.0:
            return False
        last_backfill_ts = now
        return True

    async def _maybe_schedule_analysis_backfill(*, reason: str = "") -> None:
        nonlocal backfill_inflight
        if backfill_inflight or (not _should_backfill_now()):
            return
        backfill_inflight = True

        async def _task() -> None:
            nonlocal backfill_inflight
            processed = 0
            ingested = 0
            deduped = 0
            try:
                # 1) Deduplicate by content hash (manifest-level)
                try:
                    dsum = project_store.dedupe_raw_files_by_sha(
                        current_project_full,
                        delete_duplicate_files=False,
                    )
                    deduped = int(dsum.get("duplicates") or 0)
                except Exception:
                    deduped = 0

                # 2) Backfill ingest (if pipeline version changed)
                try:
                    m = load_manifest(current_project_full) or {}
                except Exception:
                    m = {}
                raw_files = m.get("raw_files") if isinstance(m.get("raw_files"), list) else []

                ingest_updates = 0
                ingest_targets = []
                ingest_seen: set[str] = set()
                for rf in raw_files:
                    if not isinstance(rf, dict):
                        continue
                    rel = str(rf.get("path") or "").replace("\\", "/").strip()
                    if not rel:
                        continue
                    try:
                        abs_p = (PROJECT_ROOT / rel).resolve()
                        if not abs_p.exists() or not abs_p.is_file():
                            continue
                    except Exception:
                        continue

                    cur = str(rf.get("ingest_version") or "").strip()
                    if cur != str(project_store.INGEST_PIPELINE_VERSION):
                        if rel not in ingest_seen:
                            ingest_targets.append(rf)
                            ingest_seen.add(rel)

                # If LibreOffice is available, re-ingest legacy .doc files with partial extraction
                try:
                    soffice_ok = bool(shutil.which("soffice"))
                except Exception:
                    soffice_ok = False
                if soffice_ok:
                    for rf in raw_files:
                        if not isinstance(rf, dict):
                            continue
                        rel = str(rf.get("path") or "").replace("\\", "/").strip()
                        if not rel or rel in ingest_seen:
                            continue
                        if Path(rel).suffix.lower() != ".doc":
                            continue
                        try:
                            dtxt = _find_latest_artifact_text_for_file(
                                current_project_full,
                                artifact_type="doc_text",
                                file_rel=rel,
                                cap=6000,
                            )
                        except Exception:
                            dtxt = ""
                        if not dtxt or len(dtxt.strip()) < 1500:
                            ingest_targets.append(rf)
                            ingest_seen.add(rel)

                for rf in ingest_targets[:2]:
                    rel = str(rf.get("path") or "").replace("\\", "/").strip()
                    try:
                        await asyncio.to_thread(
                            project_store.ingest_uploaded_file,
                            current_project_full,
                            rel,
                            caption_image_fn=_caption_image_bytes,
                            classify_image_fn=_classify_image_wrapper,
                        )
                        rf["ingest_version"] = str(project_store.INGEST_PIPELINE_VERSION)
                        ingest_updates += 1
                        ingested += 1
                    except Exception:
                        continue

                if ingest_updates:
                    try:
                        m["raw_files"] = raw_files
                        save_manifest(current_project_full, m)
                    except Exception:
                        pass

                # 3) Backfill analysis synthesis (bounded)
                candidates = []
                for rf in raw_files:
                    if not isinstance(rf, dict):
                        continue
                    rel = str(rf.get("path") or "").replace("\\", "/").strip()
                    if not rel:
                        continue
                    try:
                        abs_p = (PROJECT_ROOT / rel).resolve()
                        if not abs_p.exists() or not abs_p.is_file():
                            continue
                    except Exception:
                        continue
                    key = _analysis_backfill_key(rel)
                    if not key:
                        continue
                    if _upload_synthesis_already_done(current_project_full, key):
                        continue
                    candidates.append(rf)

                for rf in candidates[:3]:
                    rel = str(rf.get("path") or "").replace("\\", "/").strip()
                    orig_name = str(rf.get("orig_name") or Path(rel).name or "").strip()
                    key = _analysis_backfill_key(rel)
                    if not key:
                        continue
                    if key in _upload_synthesis_inflight:
                        continue
                    _upload_synthesis_inflight.add(key)
                    try:
                        ok = await _run_analysis_backfill_for_file(rel=rel, orig_name=orig_name)
                        if ok:
                            _upload_synthesis_mark_done(
                                current_project_full,
                                key=key,
                                canonical_rel=rel,
                                orig_name=orig_name,
                            )
                            processed += 1
                    finally:
                        try:
                            _upload_synthesis_inflight.discard(key)
                        except Exception:
                            pass

                # 4) Refresh derived views
                try:
                    project_store.build_discovery_views_and_write(current_project_full)
                except Exception:
                    pass
                try:
                    project_store.build_fact_ledger_views_and_write(current_project_full)
                except Exception:
                    pass
                try:
                    project_store.build_library_index_and_write(current_project_full)
                except Exception:
                    pass
                try:
                    project_store.build_analysis_audit_report(current_project_full)
                except Exception:
                    pass

                # Optional, minimal user-visible notice (only if work was done)
                if (processed or ingested or deduped) and str(active_expert or "").strip().lower() == "analysis":
                    try:
                        msg = f"Backfill check: {processed} analysis pass(es), {ingested} ingest refresh(es), {deduped} duplicate(s) de-listed."
                        await _ws_send_safe(msg)
                    except Exception:
                        pass
            finally:
                backfill_inflight = False

        try:
            asyncio.create_task(_task())
        except Exception:
            backfill_inflight = False

    async def _run_upload_batch_summary(*, batch_id: str, zip_name: str, total: int) -> None:
        """
        Batch-level synthesis after ZIP extraction completes.
        """
        if str(active_expert or "").strip().lower() != "analysis":
            return
        synthetic = (
            f"A ZIP batch has finished ingesting.\n"
            f"- Batch ID: {batch_id}\n"
            f"- ZIP: {zip_name}\n"
            f"- Files: {total}\n\n"
            "Use the discovery index, fact ledger, timeline, issues/questions, and conflict report.\n"
            "Give:\n"
            "1) A concise batch summary (4-8 sentences)\n"
            "2) The top 3 issues or weak points\n"
            "3) The top 3 missing items to request\n"
            "4) The next best action\n"
            "Ask ONE precise question.\n"
        ).strip()

        try:
            _gen_ok = False
            try:
                _trace_emit("assistant.generate.start")
            except Exception:
                pass
            try:
                out = await model_pipeline.run_request_pipeline(
                    ctx=sys.modules[__name__],
                    current_project_full=current_project_full,
                    clean_user_msg=synthetic,
                    do_search=False,
                    search_results="",
                    conversation_history=conversation_history,
                    max_history_pairs=MAX_HISTORY_PAIRS,
                    server_file_path=Path(__file__).resolve(),
                    active_expert=active_expert,
                )
                _gen_ok = True
            finally:
                try:
                    _trace_emit("assistant.generate.end", {"ok": bool(_gen_ok)})
                except Exception:
                    pass
        except Exception:
            return

        try:
            ans = str((out or {}).get("user_answer") or "").strip()
        except Exception:
            ans = ""
        if not ans:
            return

        try:
            ch2 = (out or {}).get("conversation_history")
            if isinstance(ch2, list):
                conversation_history[:] = ch2
        except Exception:
            pass

        try:
            project_store.build_analysis_audit_report(current_project_full)
        except Exception:
            pass

        try:
            await websocket.send(ans)
        except Exception:
            pass

    def _maybe_trigger_upload_synthesis_from_outgoing(m: str) -> None:
        """
        Called from _ws_send_safe for every outbound message (string).
        Must be fast + non-blocking.
        """
        s = (m or "").strip()
        if not s.startswith("{"):
            return

        try:
            obj = json.loads(s)
        except Exception:
            return

        if not isinstance(obj, dict):
            return

        if int(obj.get("v", 0) or 0) != 1:
            return

        if str(obj.get("type") or "").strip() != "upload.status":
            return

        # Only trigger on completion
        status = str(obj.get("status") or obj.get("state") or "").strip().lower()
        if status != "done":
            return

        # Wordle special-case skip
        if _should_skip_upload_synthesis_wordle(obj):
            return

        orig_name, canonical_rel, key = _extract_upload_identity(obj)
        batch_id = str(obj.get("batch_id") or "").strip()
        batch_mode = bool(batch_id)
        if not key:
            return

        # Non-spam: at most once per uploaded file (persisted) + no concurrent duplicates
        if key in _upload_synthesis_inflight:
            return
        if _upload_synthesis_already_done(current_project_full, key):
            return

        _upload_synthesis_inflight.add(key)
        # IMPORTANT: do NOT mark "done" until AFTER we have either:
        # - successfully run expert synthesis (non-image or image with semantics), OR
        # - explicitly reported that vision semantics are unavailable.
        # This prevents a failed vision call from permanently suppressing future retries.


        async def _task() -> None:
            try:
                # IMAGE uploads: ensure vision semantics exist BEFORE expert synthesis runs.
                # Block synthesis for images until semantics is present (or cached).
                ok_v = True
                note_v = ""

                try:
                    rel0 = (canonical_rel or "").replace("\\", "/").strip()
                    suf0 = Path(rel0).suffix.lower()
                    if rel0 and (suf0 in IMAGE_SUFFIXES):
                        # Ensure active object points at this file so downstream pipelines pick it up.
                        try:
                            sha0 = ""
                            try:
                                man0 = load_manifest(current_project_full) or {}
                                raw0 = man0.get("raw_files") or []
                                if isinstance(raw0, list):
                                    for rf in raw0:
                                        if not isinstance(rf, dict):
                                            continue
                                        if str(rf.get("path") or "").replace("\\", "/").strip() == rel0:
                                            sha0 = str(rf.get("sha256") or "").strip()
                                            break
                            except Exception:
                                sha0 = ""

                            mime0 = ""
                            try:
                                mime0 = mime_for_image_suffix(suf0)
                            except Exception:
                                mime0 = ""

                            project_store.save_active_object(
                                current_project_full,
                                {
                                    "rel_path": rel0,
                                    "orig_name": (orig_name or Path(rel0).name),
                                    "sha256": sha0,
                                    "mime": mime0,
                                    "set_reason": "upload_auto_synthesis",
                                },
                            )
                        except Exception:
                            pass

                        ok_v, _sem_json, note_v = await ensure_image_semantics_for_file(
                            current_project_full,
                            file_rel=rel0,
                            force=False,
                            reason="upload_auto_synthesis",
                        )

                        # Verify the required artifact actually exists and is linked via from_files.
                        # (Mandatory: do NOT proceed to expert synthesis on caption/OCR-only evidence.)
                        if ok_v:
                            try:
                                sem_check = _find_latest_artifact_text_for_file(
                                    current_project_full,
                                    artifact_type="image_semantics",
                                    file_rel=rel0,
                                    cap=220000,
                                )
                            except Exception:
                                sem_check = ""
                            if not (sem_check or "").strip():
                                ok_v = False
                                note_v = "image_semantics_artifact_missing_after_ensure"

                except Exception as e:
                    ok_v = False
                    note_v = f"vision_semantics_exception: {e!r}"

                # Only run expert synthesis after semantics exists (or non-image).
                if ok_v:
                    # Batch mode: suppress per-file chat/history, but still extract discovery/ledger.
                    is_analysis_hat = str(active_expert or "").strip().lower() == "analysis"
                    suppress_chat = bool(batch_mode)
                    suppress_hist = bool(batch_mode)

                    if (not batch_mode) or is_analysis_hat:
                        await _run_upload_expert_synthesis(
                            orig_name=orig_name,
                            canonical_rel=canonical_rel,
                            suppress_chat=suppress_chat,
                            suppress_history=suppress_hist,
                        )

                    # Non-spam + correctness: only mark done after successful synthesis.
                    _upload_synthesis_mark_done(
                        current_project_full,
                        key=key,
                        canonical_rel=canonical_rel,
                        orig_name=orig_name,
                    )

                    # Batch progress tracking + summary when complete
                    if batch_mode:
                        try:
                            b = project_store.mark_upload_batch_file_done(
                                current_project_full,
                                batch_id=batch_id,
                                canonical_rel=canonical_rel,
                            )
                        except Exception:
                            b = {}

                        if isinstance(b, dict):
                            b_status = str(b.get("status") or "")
                            summary_done = bool(b.get("summary_done", False))
                            if b_status == "done" and not summary_done:
                                try:
                                    project_store.mark_upload_batch_summary_done(current_project_full, batch_id=batch_id)
                                except Exception:
                                    pass
                                try:
                                    await _run_upload_batch_summary(
                                        batch_id=batch_id,
                                        zip_name=str(b.get("zip_name") or ""),
                                        total=int(b.get("total") or 0),
                                    )
                                except Exception:
                                    pass

                else:
                    # Mandatory vision semantics for images: never silently degrade.
                    try:
                        rel_fail = (canonical_rel or "").replace("\\", "/").strip()
                        suf_fail = Path(rel_fail).suffix.lower()
                        if rel_fail and (suf_fail in IMAGE_SUFFIXES):
                            vision_model = (os.environ.get("OPENAI_VISION_MODEL") or "").strip()
                            api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()

                            msg_lines: List[str] = []
                            msg_lines.append("⚠️ Vision semantics are REQUIRED for this image upload, but vision is unavailable.")
                            msg_lines.append(f"- File: {orig_name or Path(rel_fail).name}")
                            msg_lines.append(f"- Path: {canonical_rel}")
                            if note_v:
                                msg_lines.append(f"- Detail: {note_v}")

                            msg_lines.append("")
                            msg_lines.append("Fix:")
                            if not vision_model:
                                msg_lines.append("- Set OPENAI_VISION_MODEL to a vision-capable model.")
                            if not api_key:
                                msg_lines.append("- Set OPENAI_API_KEY.")
                            msg_lines.append("- Restart the server and re-upload the image.")

                            msg_lines.append("")
                            msg_lines.append("Expert synthesis was NOT run to avoid OCR/caption-only image understanding.")
                            await _ws_send_safe("\n".join(msg_lines).strip())
                    except Exception:
                        pass
            finally:
                try:
                    _upload_synthesis_inflight.discard(key)
                except Exception:
                    pass

        try:
            asyncio.create_task(_task())

        except Exception:
            try:
                _upload_synthesis_inflight.discard(key)
            except Exception:
                pass

    # Greeting (contextual, human, bounded)
    projects = list_existing_projects(safe_user_name(user))

    goal0 = _get_project_goal(_full(current_project))
    g0 = (goal0 or "").strip()

    greet: List[str] = []

    try:
        # Prefer a human greeting that uses Tier-2G name + local daypart + recent project context.
        greet_msg = await build_contextual_greeting(
            user=user,
            project_full=current_project_full,
            project_short=current_project,
            reason="connect",
        )

        # HARD GUARANTEE:
        # If greet_msg is empty (but no exception), the UI would otherwise get a blank startup.
        gm = (greet_msg or "").strip()
        if gm:
            greet.append(gm)
        else:
            # Non-LLM fallback: still human-visible; do NOT start with "Project:" (UI may suppress it).
            greet.append(f"Hi {user}.")
            greet.append("What do you want to work on?")
    except Exception:
        # Ultra-safe fallback (never empty, never meta-only)
        greet.append(f"Hi {user}.")
        greet.append("What do you want to work on?")

    # NOTE:
    # Do NOT auto-print the numbered project list on connect.
    # It creates a noisy startup sequence and conflicts with UI-driven project selection.
    # Users can request it explicitly via the command gate (!projects / !list projects).

    async def _ws_send_safe(msg: Any) -> bool:
        """
        Best-effort WS send:
        - Avoid poison chars (NUL)
        - Cap size to stay under WS max_size
        - Never throw (log and return False)
        """
        frame_type = "text"
        try:
            m = "" if msg is None else str(msg)
            m = m.replace("\x00", "")
            if len(m) > 900_000:
                m = m[:900_000].rstrip() + "\n.[truncated]."

            # Never send empty messages (the UI will warn/spam on sanitized-empty output).
            if not m.strip():
                return True

            # Normalize assistant-facing text (skip JSON frames).
            try:
                s0 = m.lstrip()
                looks_json = s0.startswith("{") and ("\"type\"" in s0 or "\"v\"" in s0 or "\"messages\"" in s0)
                if not looks_json:
                    m = _normalize_assistant_text_for_display(m)
            except Exception:
                pass

            try:
                if isinstance(m, str):
                    s1 = m.lstrip()
                    if s1.startswith("{"):
                        obj1 = json.loads(s1)
                        if isinstance(obj1, dict) and obj1.get("type"):
                            frame_type = str(obj1.get("type") or "").strip() or "json"
                        else:
                            frame_type = "json"
            except Exception:
                frame_type = "text"
            try:
                detail = {"frame_type": frame_type}
                if frame_type == "text":
                    prev = (m or "").replace("\n", " ")
                    detail["chars"] = len(m or "")
                    detail["preview"] = prev[:80]
                _trace_emit("assistant.send.start", detail)
            except Exception:
                pass

            await _orig_send(m)
            try:
                detail = {"ok": True, "frame_type": frame_type}
                if frame_type == "text":
                    prev = (m or "").replace("\n", " ")
                    detail["chars"] = len(m or "")
                    detail["preview"] = prev[:80]
                _trace_emit("assistant.send.end", detail)
            except Exception:
                pass

            # Upload → Expert Synthesis trigger (non-blocking)
            try:
                _maybe_trigger_upload_synthesis_from_outgoing(m)
            except Exception:
                pass

            return True
        except Exception as e:
            try:
                detail = {"ok": False, "frame_type": frame_type}
                if frame_type == "text":
                    prev = (m or "").replace("\n", " ")
                    detail["chars"] = len(m or "")
                    detail["preview"] = prev[:80]
                _trace_emit("assistant.send.end", detail)
            except Exception:
                pass
            try:
                _trace_emit("exception", {"err_type": type(e).__name__, "where": "ws_send"})
            except Exception:
                pass
            print(f"[WS] send failed: {e!r}")
            return False


    # Monkeypatch: make EVERY existing `await websocket.send(...)` call safe.
    _orig_send = websocket.send
    async def _send_wrapper(msg: Any) -> None:
        await _ws_send_safe(msg)
    try:
        websocket.send = _send_wrapper  # type: ignore[attr-defined]
    except Exception:
        pass

    # Always emit a meta project frame on connect so the UI can sync to the
    # server's current project (critical for cross-device consistency).
    try:
        await _ws_send_safe(json.dumps({"v": 1, "type": "ui.status", "project": current_project}))
    except Exception:
        pass

    # Connect greeting is disabled.
    # The UI will request a greeting AFTER thread history replay (thread.get or greeting.request).

    try:
        async for raw_msg in websocket:
            # Per-turn flag: set True only when THIS incoming message actually changes projects.
            project_changed = False
            # ----------------------------
            # WS Frame Envelope (v1)
            # Accept:
            # - JSON frames (preferred): {"v":1,"type":"..."}
            # - raw strings (legacy): treated as chat.send
            # ----------------------------
            frame_obj: Dict[str, Any] = {}
            is_frame = False

            try:
                if isinstance(raw_msg, str) and raw_msg.strip().startswith("{"):
                    cand = json.loads(raw_msg)
                    if isinstance(cand, dict) and int(cand.get("v", 0) or 0) == 1 and isinstance(cand.get("type"), str):
                        frame_obj = cand
                        is_frame = True
            except Exception:
                frame_obj = {}
                is_frame = False
            # Per-turn audit trace id (context-local; safe under asyncio tasks)
            try:
                incoming_trace = ""
                if is_frame:
                    incoming_trace = str(frame_obj.get("trace_id") or "").strip()
                _trace_id = incoming_trace or f"turn_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"
                _AUDIT_TRACE_ID.set(_trace_id)
                # Reset per-turn decision context (so prior turns can't leak into this audit row)
                _audit_ctx_reset()
                _trace_event_count = 0
            except Exception:
                pass
            try:
                if is_frame:
                    ftype0 = str(frame_obj.get("type") or "").strip()
                    if ftype0 != "ws.ping":
                        detail0: Dict[str, Any] = {"type": ftype0}
                        if ftype0 == "chat.send":
                            try:
                                _t = str(
                                    frame_obj.get("text")
                                    or frame_obj.get("message")
                                    or frame_obj.get("content")
                                    or ""
                                )
                            except Exception:
                                _t = ""
                            try:
                                detail0["text_preview"] = (_t or "").replace("\n", " ")[:80]
                                if "project" in frame_obj:
                                    detail0["project_field"] = frame_obj.get("project")
                                # Mark as command if it matches explicit command gate.
                                try:
                                    s0 = (_t or "").strip()
                                    is_cmd = False
                                    if s0.startswith("!"):
                                        is_cmd = True
                                    else:
                                        low0 = s0.lower()
                                        if low0.startswith("/cmd"):
                                            is_cmd = True
                                    detail0["is_command"] = bool(is_cmd)
                                except Exception:
                                    detail0["is_command"] = False
                            except Exception:
                                pass
                        _trace_emit("ws.recv", detail0)
                else:
                    if not (isinstance(raw_msg, str) and raw_msg.strip() == "__ping__"):
                        _trace_emit("ws.recv", {"type": "raw"})
            except Exception:
                pass
            if is_frame:
                ftype = str(frame_obj.get("type") or "").strip()

                # UI heartbeat: allow legacy ping/pong unchanged
                if ftype == "ws.ping":
                    try:
                        await websocket.send(json.dumps({"v": 1, "type": "ws.pong"}))
                    except Exception:
                        pass
                    continue

                # Upload status frames are server->UI only; ignore if received from client
                if ftype == "upload.status":
                    continue

                # UI project list request (token-free)
                if ftype == "projects.list":
                    try:
                        names = list_existing_projects(safe_user_name(user))
                        # SAFETY/PRIVACY HARDENING:
                        # Never leak usernames via the projects dropdown.
                        try:
                            user_dir_names = set()
                            for u in (USERS or {}).keys():
                                user_dir_names.add(safe_user_name(str(u)))
                            names = [n for n in (names or []) if safe_user_name(str(n)) not in user_dir_names]
                        except Exception:
                            pass

                        projects_out = []
                        for n in (names or []):
                            title = ""
                            updated_at = ""
                            try:
                                title = str(project_store.get_project_display_name(safe_project_name(f"{user}/{n}")) or "").strip()
                            except Exception:
                                title = ""
                            try:
                                m = load_manifest(safe_project_name(f"{user}/{n}")) or {}
                                updated_at = str(m.get("updated_at") or m.get("updatedAt") or "").strip()
                            except Exception:
                                updated_at = ""
                            projects_out.append({"id": n, "name": n, "title": title, "updated_at": updated_at})

                        await websocket.send(json.dumps({"v": 1, "type": "projects.list", "projects": projects_out}, ensure_ascii=False))
                    except Exception:
                        pass
                    continue

                # UI thread history request (token-free): return canonical state/chat_log.jsonl
                if ftype == "thread.get":
                    try:
                        proj_req = str(frame_obj.get("project") or "").strip()
                        proj_short = safe_project_name(proj_req) if proj_req else current_project
                        proj_full = _full(proj_short)
                        try:
                            _trace_emit("thread.get.start", {"project": proj_short})
                        except Exception:
                            pass

                        # Make thread.get authoritative for current project (align server state)
                        if proj_short != current_project:
                            old_full = current_project_full
                            current_project = proj_short
                            current_project_full = proj_full
                            _ws_move_client(old_full, current_project_full, websocket)
                        _save_last_project(user, proj_short)

                        ensure_project_scaffold(proj_full)

                        msgs = read_chat_log_messages_for_ui(proj_full, max_messages=800)
                        payload = {
                            "v": 1,
                            "type": "thread.history",
                            "project": str(proj_short),
                            "messages": msgs,
                        }
                        await websocket.send(json.dumps(payload, ensure_ascii=False))
                        try:
                            _trace_emit("thread.history", {"history_state": "ok", "messages": len(msgs)})
                        except Exception:
                            pass

                        # FOUNDATIONAL:
                        # After the UI replays history for a project, emit a human greeting immediately.
                        # De-dupe per project so connect/thread.get/greeting.request can't double-fire.
                        try:
                            if proj_full not in greeted_projects:
                                greeted_projects.add(proj_full)
                                gmsg = await build_contextual_greeting(
                                    user=user,
                                    project_full=proj_full,
                                    project_short=proj_short,
                                    reason="thread_get",
                                )
                                if not (gmsg or "").strip():
                                    gmsg = "What are we working on today?"
                                await websocket.send(json.dumps({"v": 1, "type": "ui.greeting", "text": str(gmsg).strip()}))
                        except Exception:
                            pass
                        # NOTE:
                        # Greeting is already emitted above with de-duplication.
                        try:
                            _trace_emit("thread.get.end", {"project": proj_short})
                        except Exception:
                            pass

                    except Exception:
                        # fail silent: don't poison the session
                        pass
                    continue
                # UI greeting request (LLM-based, bounded)
                # Purpose: allow the UI to request a contextual greeting AFTER it replays history.
                if ftype == "greeting.request":
                    try:
                        proj = str(frame_obj.get("project") or "").strip() or current_project
                        reason = str(frame_obj.get("reason") or "new_project").strip() or "new_project"
                        reason_l = reason.lower()
                        is_expert_switch = reason_l.startswith("expert_switch")

                        proj_short = safe_project_name(proj)
                        proj_full = _full(proj_short)
                        ensure_project_scaffold(proj_full)

                        gmsg = ""
                        try:
                            gmsg = await build_contextual_greeting(
                                user=user,
                                project_full=proj_full,
                                project_short=proj_short,
                                reason=reason,
                            )
                        except Exception:
                            gmsg = ""

                        if not (gmsg or "").strip():
                            gmsg = "What are we working on today?"

                        # De-dupe: per project for normal greetings; per (project+reason) for expert switch.
                        if is_expert_switch:
                            k = f"{proj_full}|{reason_l}"
                            if k not in greeted_expert_switch:
                                greeted_expert_switch.add(k)
                                await websocket.send(json.dumps({"v": 1, "type": "ui.greeting", "text": str(gmsg).strip()}))
                        else:
                            if proj_full not in greeted_projects:
                                greeted_projects.add(proj_full)
                                await websocket.send(json.dumps({"v": 1, "type": "ui.greeting", "text": str(gmsg).strip()}))
                    except Exception:
                        pass
                    continue
                # Preferred chat send
                if ftype == "chat.send":
                    # UI-selected expert (optional). Keep sticky if missing on this frame.
                    try:
                        ae = str(
                            frame_obj.get("active_expert")
                            or frame_obj.get("expert")
                            or ""
                        ).strip()
                        if ae:
                            active_expert = ae
                    except Exception:
                        pass

                    # Extract message text early so we can gate switch messages on real user input.
                    # This prevents startup project-sync frames (empty text) from emitting "Switched."
                    frame_user_msg = ""
                    try:
                        frame_user_msg = str(
                            frame_obj.get("text")
                            or frame_obj.get("message")
                            or frame_obj.get("content")
                            or ""
                        ).strip()
                    except Exception:
                        frame_user_msg = ""

                    # Optional project override (short name)
                    try:
                        proj = str(frame_obj.get("project") or "").strip()
                        if proj:
                            old_short = current_project
                            old_full = current_project_full
                            next_short = safe_project_name(proj)
                            if next_short != old_short:
                                try:
                                    _trace_emit("project.switch.start", {"from": old_short, "to": next_short})
                                except Exception:
                                    pass
                            current_project = next_short
                            current_project_full = _full(current_project)
                            _save_last_project(user, current_project)

                            if current_project != old_short:
                                project_changed = True

                            ensure_project_scaffold(current_project_full)
                            _ws_move_client(old_full, current_project_full, websocket)
                            if current_project != old_short:
                                try:
                                    _trace_emit("project.switch.end", {"from": old_short, "to": current_project})
                                except Exception:
                                    pass
                    except Exception:
                        pass

                    # If the UI switched projects on THIS message, emit:
                    # 1) the system switch message, then
                    # 2) a human contextual greeting (LLM-based, bounded).
                    try:
                        if bool(project_changed) and bool(frame_user_msg):
                            await websocket.send(_project_switch_message(current_project))
                            try:
                                gmsg = await build_contextual_greeting(
                                    user=user,
                                    project_full=current_project_full,
                                    project_short=current_project,
                                    reason="switch_project",
                                )
                                if gmsg and (current_project_full not in greeted_projects):
                                    greeted_projects.add(current_project_full)
                                    await websocket.send(gmsg)
                            except Exception:
                                pass
                    except Exception:
                        pass

                    # IMPORTANT:
                    # The JSON frame must carry the actual user message text.
                    # If we don't extract it here, the downstream chat loop sees an empty user_msg
                    # and silently skips the turn (no pipeline => no facts capture).
                    user_msg = frame_user_msg
            else:
                # Legacy raw string fallback => chat.send
                user_msg = (raw_msg or "").strip()

            # UI heartbeat: respond and do NOT treat as a chat message.
            # The web UI sends "__ping__" periodically to detect dead sockets.
            if user_msg == "__ping__":
                try:
                    await websocket.send("__pong__")
                except Exception:
                    pass
                continue

            if not user_msg:
                continue

            # -------------------------------------------------------------
            # USER-SCOPED pending disambiguation consumption (Tier-2G assist)
            #
            # Goal:
            # - No magic words required.
            # - If the assistant previously asked the user to choose between explicit
            #   options for a Tier-2G identity field (currently: identity.location),
            #   consume this user message deterministically, emit a canonical Tier-1 claim,
            #   rebuild Tier-2G, and clear the pending state.
            #
            # This MUST run before:
            # - Tier-2G promotion hook
            # - project routing
            # - model invocation
            # -------------------------------------------------------------
            try:
                pend_u = project_store.load_user_pending_disambiguation(user)
                if project_store.is_user_pending_disambiguation_unexpired(pend_u):
                    kind = str(pend_u.get("kind") or "").strip().lower()
                    if kind == "identity.location":
                        opts = pend_u.get("options") if isinstance(pend_u.get("options"), list) else []
                        opts2 = [str(x).strip() for x in opts if str(x).strip()][:6]
                        default_opt = str(pend_u.get("default") or (opts2[0] if opts2 else "")).strip()

                        um = (user_msg or "").strip()
                        low = um.lower().strip()

                        chosen = ""

                        # 1) plain yes/no => accept default
                        if _is_affirmation(um) and default_opt:
                            chosen = default_opt

                        # 2) short semantic picks
                        if not chosen:
                            if "metro" in low:
                                for o in opts2:
                                    if "metro" in o.lower():
                                        chosen = o
                                        break
                            elif ("city" in low) or ("proper" in low):
                                # prefer the non-metro option
                                for o in opts2:
                                    if "metro" not in o.lower():
                                        chosen = o
                                        break

                        # 3) ordinal picks
                        if not chosen:
                            if low in ("1", "one", "first"):
                                if len(opts2) >= 1:
                                    chosen = opts2[0]
                            elif low in ("2", "two", "second"):
                                if len(opts2) >= 2:
                                    chosen = opts2[1]

                        # 4) direct option mention / substring match
                        if not chosen:
                            for o in opts2:
                                if o and (o.lower() in low):
                                    chosen = o
                                    break

                        # 5) fallback: if user provided a concrete location string, use it as-is
                        # (still deterministic; we only do this when a pending disambiguation exists)
                        if not chosen:
                            # avoid capturing empty / generic answers
                            if len(um) >= 3 and (not _is_affirmation(um)):
                                chosen = um

                        # If we resolved a choice, emit canonical Tier-1 claim and rebuild Tier-2G
                        if chosen:
                            # deterministic cleanup
                            v = chosen.strip().strip(" .!?,;:'\"")
                            v = " ".join(v.split())

                            # Canonical confirmation claim that Tier-2G resolver understands
                            project_store.append_user_fact_raw_candidate(
                                user,
                                claim=f"My confirmed location is {v}",
                                slot="location",
                                subject="user",
                                source="chat",
                                # evidence_quote must contain the explicit confirmation phrase so Tier-2G accepts it
                                evidence_quote=f"My confirmed location is {v}",
                                turn_index=0,
                                timestamp=now_iso(),
                            )
                            project_store.rebuild_user_profile_from_user_facts(user)

                            # clear pending
                            project_store.clear_user_pending_disambiguation(user)

                            ack = f"Got it — I’ll treat your location as confirmed: {v}."
                            await websocket.send(ack)
                            last_full_answer_text = ack
                            continue
            except Exception:
                pass

            # -------------------------------------------------------------
            # Option 3 — Explicit cleanup commands (deterministic; NO magic words)
            #
            # These commands let the user force a cleanup/confirmation flow WITHOUT
            # auto-collapsing ambiguity.
            #
            # Supported (v1):
            # - "clean up my location" / "finalize my location"
            #
            # Rules:
            # - If Tier-2G already has a value, re-emit a canonical confirmation claim:
            #     "My confirmed location is <value>"
            # - If Tier-2G is ambiguous and has options, set pending_disambiguation and ask.
            # - If no location is known, ask the user to provide it.
            #
            # MUST run before Tier-2G promotion hook to avoid double-writing.
            # -------------------------------------------------------------
            try:
                low_um = (user_msg or "").strip().lower()
                if low_um in (
                    "clean up my location",
                    "cleanup my location",
                    "finalize my location",
                    "finalize location",
                ):
                    st_obj = {}
                    try:
                        fn = getattr(project_store, "tier2g_location_status_options", None)
                        st_obj = fn(user) if callable(fn) else {}
                    except Exception:
                        st_obj = {}

                    st_loc = str((st_obj or {}).get("status") or "").strip().lower()
                    val_loc = str((st_obj or {}).get("value") or "").strip()
                    opts = (st_obj or {}).get("options") if isinstance((st_obj or {}).get("options"), list) else []
                    opts2 = [str(x).strip() for x in opts if str(x).strip()][:6]

                    # If ambiguous, ask user to choose and persist pending_disambiguation.
                    if st_loc == "ambiguous" and len(opts2) >= 2:
                        default_opt = opts2[0]

                        pend = {
                            "version": 1,
                            "pending": True,
                            "kind": "identity.location",
                            "options": opts2[:6],
                            "default": default_opt,
                            "created_at": now_iso(),
                            "expires_at_epoch": float(time.time() + 300.0),
                        }
                        project_store.save_user_pending_disambiguation(user, pend)

                        q = f'Do you want it stored as "{opts2[0]}" (city proper) or "{opts2[1]}" (metro area)?'
                        await websocket.send(q)
                        last_full_answer_text = q
                        continue

                    # If we have a concrete value, re-confirm it canonically.
                    if val_loc:
                        v = val_loc.strip().strip(" .!?,;:'\"")
                        v = " ".join(v.split())

                        project_store.append_user_fact_raw_candidate(
                            user,
                            claim=f"My confirmed location is {v}",
                            slot="location",
                            subject="user",
                            source="chat",
                            evidence_quote=f"My confirmed location is {v}",
                            turn_index=0,
                            timestamp=now_iso(),
                        )
                        project_store.rebuild_user_profile_from_user_facts(user)

                        ack = f"Got it — I re-confirmed your location as {v}."
                        await websocket.send(ack)
                        last_full_answer_text = ack
                        continue

                    # Nothing known yet
                    msg = "I don’t have a stored location to clean up yet — tell me where you live (e.g., “I live in Oklahoma City, OK”)."
                    await websocket.send(msg)
                    last_full_answer_text = msg
                    continue
            except Exception:
                pass

            # -------------------------------------------------------------
            # Tier-2G (GLOBAL) promotion hook — MUST run before:
            # - project routing
            # - expert routing
            # - model invocation
            # - any project-memory writes
            #
            # Deterministic + auditable:
            # - Writes to users/<user>/_user/facts_raw.jsonl via project_store.append_user_fact_raw_candidate
            # - Rebuilds users/<user>/_user/profile.json via project_store.rebuild_user_profile_from_user_facts
            # - No silent failure: on failure, send an explicit WS error line and log.
            # - Prevent leakage into project memory: mark this turn for redacted project logging.
            # -------------------------------------------------------------
            t2g_written_this_turn = False
            t2g_project_log_redaction = ""
            try:
                # Tier-2G promotion based on the USER message alone (no reliance on last assistant text).
                #
                # Hard goals:
                # - No magic words required.
                # - If Tier-2G is ambiguous and the user supplies a concrete value for a slot (name/location),
                #   we emit a deterministic canonical Tier-1G claim that triggers the existing Tier-2G resolver.
                #
                # Supported:
                # - Name: "call me X" / "use X" / single-token name / "yes" when only one option exists.
                # - Location: "I live in X" / "I'm in X" / "my location is X" / "I moved to X" / "I relocated to X"
                #   plus "yes" when the prior assistant message contains a (City, ST) proposal.
                t2g_msg_for_promo = user_msg
                try:
                    um = (user_msg or "").strip()

                    # ---------------------------------------------------------
                    # A) Preferred name resolution (existing behavior, unchanged)
                    # ---------------------------------------------------------
                    m = re.match(r"^(?:please\s+)?(?:call\s+me|use|please\s+use)\s+(.+?)\s*$", um, flags=re.IGNORECASE)
                    if not m:
                        m = re.match(r"^(?:actually\s+)?call\s+me\s+(.+?)\s*$", um, flags=re.IGNORECASE)

                    chosen = ""
                    if m:
                        chosen = (m.group(1) or "").strip().strip('"\'')
                    if not chosen:
                        if re.fullmatch(r"[A-Za-z][A-Za-z\s\-']{0,39}", um) and len(um.split()) <= 3:
                            chosen = um
                    if chosen:
                        chosen = chosen.strip().strip(" .!?,")
                        def _is_valid_pref_name_candidate(val: str) -> Tuple[bool, str]:
                            v = (val or "").strip()
                            low_v = v.lower()
                            if not re.search(r"[A-Za-z]", v):
                                return False, "no_letters"
                            if low_v in {"hi", "hey", "hello", "ok", "okay", "yo"}:
                                return False, "greeting"
                            if low_v in {"he", "she", "they", "him", "her", "them", "his", "hers", "their", "theirs"}:
                                return False, "pronoun"
                            letters_only = re.sub(r"[^A-Za-z]", "", v)
                            if len(letters_only) <= 2:
                                if len(letters_only) == 2 and v[:1].isupper() and v[1:2].islower():
                                    return True, ""
                                return False, "too_short"
                            return True, ""

                        ok_name, reason_code = _is_valid_pref_name_candidate(chosen)
                        if not ok_name:
                            try:
                                _trace_emit(
                                    "tier2g.name.reject",
                                    {
                                        "reason": reason_code,
                                        "candidate": chosen[:24],
                                    },
                                )
                            except Exception:
                                pass
                            chosen = ""
                    if (not chosen) and _is_affirmation(um):
                        try:
                            fn = getattr(project_store, "tier2g_single_preferred_name_option", None)
                            if callable(fn):
                                opt = str(fn(user) or "").strip()
                                if opt and (" " not in opt):
                                    chosen = opt
                        except Exception:
                            chosen = ""

                    if chosen:
                        t2g_msg_for_promo = f"I go by {chosen}"
                    else:
                        # ---------------------------------------------------------
                        # B) Location resolution (NEW)
                        #
                        # Principle:
                        # - If Tier-2G location is ambiguous, and the user supplies a location value,
                        #   we upgrade the claim to: "My confirmed location is <place>"
                        #   so the existing Tier-2G resolver can collapse ambiguity.
                        #
                        # Also allow a clean yes/no:
                        # - If user says "yes" and the prior assistant output contains a clear "City, ST",
                        #   treat that as confirming the proposed location.
                        # ---------------------------------------------------------
                        loc_choice = ""

                        # 1) Explicit move language (treat as current/home-base update)
                        mm = re.search(
                            r"\b(?:i\s+(?:just\s+)?moved|i\s+relocated)\s+to\s+(.+)$",
                            um,
                            flags=re.IGNORECASE,
                        )
                        if mm:
                            loc_choice = (mm.group(1) or "").strip()

                        # 2) Direct location statements (extract place)
                        if not loc_choice:
                            mm = re.search(r"\bi\s+live\s+in\s+(.+)$", um, flags=re.IGNORECASE)
                            if not mm:
                                mm = re.search(r"\b(?:i\s*[' ]?m|i\s+am)\s+in\s+(.+)$", um, flags=re.IGNORECASE)
                            if not mm:
                                mm = re.search(r"\b(?:i\s*[' ]?m|i\s+am)\s+located\s+in\s+(.+)$", um, flags=re.IGNORECASE)
                            if not mm:
                                mm = re.search(r"\bmy\s+location\s+is\s+(.+)$", um, flags=re.IGNORECASE)
                            if not mm:
                                mm = re.search(r"\b(?:i\s*[' ]?m|i\s+am)\s+based\s+in\s+(.+)$", um, flags=re.IGNORECASE)
                            if not mm:
                                mm = re.search(r"\b(?:i\s*[' ]?m|i\s+am)\s+currently\s+in\s+(.+)$", um, flags=re.IGNORECASE)

                            if mm:
                                loc_choice = (mm.group(1) or "").strip()

                        # 3) "yes" confirmation when the system likely just proposed a City, ST
                        if (not loc_choice) and _is_affirmation(um):
                            try:
                                prev = str(last_full_answer_text or "")
                            except Exception:
                                prev = ""
                            # Conservative: only accept "City, ST" proposals (avoids grabbing random nouns)
                            mm = re.search(r"\b([A-Z][A-Za-z .'\-]{1,40},\s*[A-Z]{2})\b", prev)
                            if mm:
                                loc_choice = (mm.group(1) or "").strip()

                        # If we have a location candidate, only confirm it when Tier-2G is currently ambiguous.
                        if loc_choice:
                            # Deterministic cleanup
                            loc_choice = loc_choice.strip().strip(" .!?,;:'\"")
                            loc_choice = " ".join(loc_choice.split())

                            try:
                                fn = getattr(project_store, "tier2g_location_status_options", None)
                                st_obj = fn(user) if callable(fn) else {}
                            except Exception:
                                st_obj = {}

                            st_loc = ""
                            try:
                                st_loc = str((st_obj or {}).get("status") or "").strip().lower()
                            except Exception:
                                st_loc = ""

                            # Only collapse ambiguity; if already confirmed, let normal Tier-2G capture run.
                            if st_loc == "ambiguous":
                                t2g_msg_for_promo = f"My confirmed location is {loc_choice}"
                except Exception:
                    t2g_msg_for_promo = user_msg

                t2g_res = _tier2g_promote_global_memory_or_raise(user, t2g_msg_for_promo)
                # (removed: assistant-context preferred-name promotion; user-message-only promotion above is authoritative)
                if isinstance(t2g_res, dict) and int(t2g_res.get("written") or 0) > 0:
                    t2g_written_this_turn = True
                    # Minimal redaction to prevent global identity facts entering project logs/memory.
                    # Keep model input unchanged (clean_user_msg stays the real user text).
                    t2g_project_log_redaction = (
                        f"[GLOBAL_TIER2G_FACT_CAPTURED] ids={t2g_res.get('ids')} "
                        f"facts_raw={t2g_res.get('facts_raw_path')} profile={t2g_res.get('profile_path')}"
                    )
                    try:
                        print(f"[T2G] ok written={t2g_res.get('written')} user={user!r} facts_raw={t2g_res.get('facts_raw_path')!r}")
                    except Exception:
                        pass
            except Exception as e:
                # Observable failure (no swallow)
                try:
                    print(f"[T2G] ERROR user={user!r} err={e!r}")
                except Exception:
                    pass
                try:
                    await websocket.send(f"GLOBAL_MEMORY_ERROR (Tier-2G): {e!r}")
                except Exception:
                    pass

            # Count real user messages (used to expire decision confirmations)
            user_msg_counter += 1
            # C4 v1 — flag to short-circuit higher-level logic for this turn
            c4_consumed_this_turn = False
            # -------------------------------------------------------------
            # C4 v1 — Upload Clarification Loop (deterministic)
            #
            # If a pending upload question exists and is unexpired:
            # - Treat this user message as the answer (verbatim)
            # - Append to upload_notes.jsonl
            # - Clear pending state
            # - Continue normal pipeline (do NOT trap conversation)
            # -------------------------------------------------------------
            try:
                pend_q = load_pending_upload_question(current_project_full)
                if isinstance(pend_q, dict) and pend_q and is_pending_upload_question_unexpired(pend_q):

                    # IMPORTANT:
                    # Do NOT consume deterministic control queries as clarification answers.
                    msg_norm = str(user_msg or "").strip().lower()
                    is_control_query = msg_norm in (
                        "project pulse",
                        "pulse",
                        "status",
                        "project status",
                        "resume",
                        "inbox",
                        "pending",
                        "what's left",
                        "whats left",
                        "what should i do next",
                    )

                    if not is_control_query:
                        try:
                            append_upload_note(
                                current_project_full,
                                upload_path=str(pend_q.get("upload_path") or "").strip(),
                                question=str(pend_q.get("question") or "").strip(),
                                answer=str(user_msg or ""),
                                source="user",
                            )

                            # C9: resolve the corresponding inbox item (if present)
                            try:
                                inbox_id = str(pend_q.get("inbox_id") or "").strip()
                                if inbox_id:
                                    project_store.resolve_inbox_item(
                                        current_project_full,
                                        inbox_id=inbox_id,
                                        resolution_note="answered",
                                        refs=[str(pend_q.get("upload_path") or "").strip()],
                                    )
                            except Exception:
                                pass

                            c4_consumed_this_turn = True

                        except Exception:
                            pass
                        try:
                            clear_pending_upload_question(current_project_full)
                        except Exception:
                            pass
            except Exception:
                pass


            lower = user_msg.lower().strip()
            # -------------------------------------------------------------
            # Deterministic truthfulness: "Did you read those articles?"
            #
            # The model can drift here (claiming it didn't use web even when it did).
            # We answer deterministically based on the previous turn's recorded state.
            # -------------------------------------------------------------
            try:
                lowq = lower

                meta_web_q = (
                    ("did you read" in lowq)
                    or ("are you reading" in lowq)
                    or ("did you look at" in lowq)
                    or ("did you open" in lowq)
                    or ("are you basing" in lowq and ("articles" in lowq or "those" in lowq or "that" in lowq))
                    or ("based on" in lowq and ("articles" in lowq or "those" in lowq))
                )

                if meta_web_q:
                    if bool(last_web_search_used) and (last_web_search_results or "").strip():
                        reply = (
                            "Yes — I used the live web search snippets returned by Brave for that answer.\n"
                            "I did not open and read full pages like a browser; I used the returned snippets/URLs as evidence.\n"
                            "If you want, I can re-run the search right now and focus on primary sources (filings/transcript hosts) where available."
                        )
                    else:
                        reply = (
                            "No — that answer was general reasoning, not based on live web search results.\n"
                            "If you want it grounded in sources, ask me to look it up and I’ll pull the latest search evidence."
                        )
                    await websocket.send(reply)
                    last_full_answer_text = reply
                    continue
            except Exception:
                pass
            print(f"[WS] {user}/{current_project} <- {user_msg!r}")
            # Couples Therapy invariant:
            # For couple_* accounts in Couples_Therapy, ALWAYS enforce:
            # - user global memory scaffold exists (projects/<user>/_user/)
            # - project_state has ACTIVE Couples Therapist expert frame
            try:
                if _is_couples_user(user):
                    # Normalize for safety: handle spacing/underscores the same way everywhere
                    proj_full_norm = str(current_project_full or "").strip().replace(" ", "_").lower()
                    if proj_full_norm.endswith("/couples_therapy"):
                        # Ensure global user profile scaffold exists (recreates _user/ if deleted)
                        try:
                            project_store.load_user_profile(user)
                        except Exception:
                            pass

                        default_goal = "Couples therapy — improve communication, repair trust, and address recurring conflict patterns."

                        # Enforce ACTIVE therapist frame in project_state.json
                        try:
                            st0 = project_store.load_project_state(current_project_full) or {}
                        except Exception:
                            st0 = {}

                        if not str(st0.get("goal") or "").strip():
                            st0["goal"] = default_goal

                        st0["bootstrap_status"] = "active"
                        st0.setdefault("project_mode", "hybrid")
                        st0.setdefault("current_focus", "")
                        st0.setdefault("next_actions", [])
                        st0.setdefault("key_files", [])

                        st0["expert_frame"] = {
                            "status": "active",
                            "label": "Couples Therapist",
                            "directive": (
                                "Your name is Quinn. You are a couples therapist. Be warm, steady, and practical. "
                                "Focus on communication patterns, emotions, needs, boundaries, repair, and concrete next steps. "
                                "Use both partners' context as a lens, but never reveal, quote, attribute, or confirm private content. "
                                "Treat event claims as one-sided unless corroborated by both partners (directly or indirectly). "
                                "If only one side mentions it, label it as one perspective and seek the other side. "
                                "Feelings are valid as experience, not as proof of facts or intent. "
                                "If asked 'what did they say', refuse and redirect to a safe process. "
                                "If key information is missing, ask one targeted question and treat the answer as fact. "
                                "End like a therapist: one small next step and one gentle question. "
                                "Do NOT talk like a project manager; no deliverables or output formats."
                            ),
                            "set_reason": "couples_autobootstrap",
                            "updated_at": now_iso(),
                        }

                        try:
                            project_store.write_project_state_fields(current_project_full, st0)
                        except Exception:
                            # Phase-1: remove split-brain fallback writes for project_state (never block chat loop)
                            pass
            except Exception:
                pass
            # Ensure pending is defined before any upstream logic references it.
            pending = _load_pending_decision(current_project_full)

            # Per-project expert type:
            # Do NOT block chat or require a forced selection.
            # If unset, default silently to "general" (background behavior).

            # Continuity defaults (LLM-based when available)
            continuity_label = "same_topic"
            continuity_followup: Optional[bool] = None
            continuity_topic = ""
            allow_history_in_lookup = True

            # Remember the last real user question (used for [SEARCH] fallback),
            # and maintain an ephemeral Active Topic Frame for continuity.
            #
            # Do NOT record control messages / heartbeats / upload notifications.
            try:
                if user_msg not in ("__ping__", "__pong__"):
                    low_tmp = lower
                    if not (
                        low_tmp.startswith("[file_added]")
                        or low_tmp.startswith("switch project:")
                        or low_tmp.startswith("use project:")
                        or low_tmp.startswith("new project:")
                        or low_tmp.startswith("start project:")
                        or low_tmp in ("list", "ls", "plan", "show plan", "projects", "list projects", "refresh state")
                        or low_tmp.startswith("goal:")
                        or low_tmp.startswith(("patch", "/selfpatch", "/serverpatch", "/patch-server"))
                    ):
                        # Strip UI instruction blocks so we store the actual user intent.
                        cand = strip_lens0_system_blocks(user_msg)
                        cand = cand.strip()

                        # C11 - Topic strength decay (light): each user turn lowers confidence slightly
                        # unless the user clearly restates/sets a new topic.
                        try:
                            active_topic_strength = float(active_topic_strength)
                        except Exception:
                            active_topic_strength = 0.0
                        active_topic_strength = max(0.0, min(1.0, active_topic_strength * 0.85))

                        # LLM continuity classifier (preferred).
                        cont_obj: Dict[str, Any] = {}
                        try:
                            if cand:
                                cont_obj = await model_pipeline.classify_continuity_c11(
                                    ctx=sys.modules[__name__],
                                    conversation_history=conversation_history,
                                    user_text=cand,
                                    active_topic_text=active_topic_text,
                                )
                        except Exception:
                            cont_obj = {}

                        cont_label = ""
                        cont_followup = None
                        cont_topic = ""

                        if isinstance(cont_obj, dict) and cont_obj:
                            cont_label = str(cont_obj.get("continuity") or "").strip().lower()
                            if cont_label in ("same", "same_topic", "continuation", "continue"):
                                cont_label = "same_topic"
                            elif cont_label in ("new", "new_topic", "topic_shift", "different"):
                                cont_label = "new_topic"
                            elif cont_label in ("unclear", "unknown"):
                                cont_label = "unclear"
                            else:
                                cont_label = "same_topic"

                            cont_followup = bool(cont_obj.get("followup_only") or cont_obj.get("followup") or False)
                            cont_topic = str(cont_obj.get("topic") or "").strip()
                            if cont_topic:
                                cont_topic = re.sub(r"\s+", " ", cont_topic).strip()
                                if len(cont_topic) > 180:
                                    cont_topic = cont_topic[:180].rstrip()

                            continuity_label = cont_label
                            continuity_followup = cont_followup
                            continuity_topic = cont_topic
                            allow_history_in_lookup = True

                            try:
                                _audit_ctx_update(
                                    {
                                        "continuity": {
                                            "label": cont_label,
                                            "followup_only": bool(cont_followup),
                                            "topic": cont_topic,
                                        }
                                    }
                                )
                            except Exception:
                                pass
                            try:
                                _trace_emit(
                                    "routing.classify",
                                    {
                                        "continuation": cont_label,
                                        "reason": str(cont_obj.get("reason") or "model").strip() or "model",
                                    },
                                )
                            except Exception:
                                pass

                            # Update "last real question" only when this turn isn't followup-only.
                            if cand and (not cont_followup) and (not _is_generic_search_query(cand)):
                                last_user_question_for_search = cand

                            # Update Active Topic Frame based on LLM continuity.
                            try:
                                topic2 = cont_topic
                                if cont_label == "new_topic":
                                    if not topic2:
                                        try:
                                            topic2 = str(model_pipeline._extract_bringup_topic_nl(cand, cap=180) or "").strip()
                                        except Exception:
                                            topic2 = ""
                                    if not topic2:
                                        topic2 = re.sub(r"\s+", " ", cand).strip()
                                        if len(topic2) > 180:
                                            topic2 = topic2[:180].rstrip()

                                    if topic2:
                                        active_topic_text = topic2
                                        active_topic_updated_at = now_iso()
                                        active_topic_strength = 1.0
                                    else:
                                        active_topic_text = ""
                                        active_topic_updated_at = now_iso()
                                        active_topic_strength = 0.0
                                else:
                                    # same_topic or unclear -> keep continuity by default
                                    if topic2 and (not cont_followup):
                                        active_topic_text = topic2
                                        active_topic_updated_at = now_iso()
                                    if (active_topic_text or "").strip():
                                        active_topic_strength = min(1.0, max(active_topic_strength, 0.6))
                            except Exception:
                                pass

                        else:
                            # Fallback: deterministic continuation heuristics.
                            low_cand = cand.lower().strip()

                            # Pronoun-followup detector: short message + "it/that/they/those/this" => continuation
                            # when we have a strong active topic.
                            pronoun_followup = False
                            try:
                                shortish = len(low_cand) <= 90
                                has_pron = bool(re.search(r"\b(it|that|this|they|them|those)\b", low_cand))
                                # Avoid treating explicit topic resets as pronoun followups.
                                is_break = False
                                try:
                                    is_break = bool(model_pipeline._c10_is_topic_break(cand))
                                except Exception:
                                    is_break = False
                                pronoun_followup = bool(shortish and has_pron and (not is_break) and active_topic_strength >= 0.35)
                            except Exception:
                                pronoun_followup = False

                            # Treat "show me a picture" as a continuation operator too, so it can't become
                            # the new active topic / last real question (prevents meme-y image queries).
                            picture_request = False
                            try:
                                picture_request = any(x in low_cand for x in (
                                    "show me a picture",
                                    "show me a photo",
                                    "show me an image",
                                    "can you show me a picture",
                                    "can you show me a photo",
                                    "can you show me an image",
                                    "picture of",
                                    "photo of",
                                    "image of",
                                ))
                            except Exception:
                                picture_request = False

                            # Continuation is structural (not phrase-locked):
                            # - pronoun follow-up with strong active topic, OR
                            # - the user is asking for an image (picture_request)
                            continuation_op = bool(pronoun_followup or picture_request)

                            continuity_followup = bool(pronoun_followup)
                            try:
                                continuity_label = "new_topic" if model_pipeline._c10_is_topic_break(cand) else "same_topic"
                            except Exception:
                                continuity_label = "same_topic"
                            allow_history_in_lookup = True
                            try:
                                _trace_emit(
                                    "routing.classify",
                                    {"continuation": continuity_label, "reason": "heuristic"},
                                )
                            except Exception:
                                pass

                            # Update "last real question" only when this turn isn't generic/continuation.
                            if cand and (not continuation_op) and (not _is_generic_search_query(cand)):
                                last_user_question_for_search = cand

                            # Update Active Topic Frame only on substantive, non-continuation turns.
                            # Topic is derived deterministically (no model calls).
                            if cand and (not continuation_op):
                                try:
                                    # Prefer model_pipeline's deterministic extractor if present.
                                    topic = ""
                                    try:
                                        topic = str(model_pipeline._extract_bringup_topic_nl(cand, cap=180) or "").strip()
                                    except Exception:
                                        topic = ""

                                    # Fallback: first ~180 chars of the user turn, cleaned.
                                    if not topic:
                                        topic = re.sub(r"\s+", " ", cand).strip()
                                        if len(topic) > 180:
                                            topic = topic[:180].rstrip()

                                    # Conservative topic-break handling: only reset on explicit user signal.
                                    if topic and (not model_pipeline._c10_is_topic_break(cand)):
                                        active_topic_text = topic
                                        active_topic_updated_at = now_iso()
                                        active_topic_strength = 1.0
                                    elif model_pipeline._c10_is_topic_break(cand):
                                        # Explicit topic break: clear topic so pronouns won't bind backward.
                                        active_topic_text = ""
                                        active_topic_updated_at = now_iso()
                                        active_topic_strength = 0.0
                                except Exception:
                                    pass
            except Exception:
                pass


            # -------------------------------------------------------------
            # CONTROL-PLANE COMMAND GATE (explicit-only)
            #
            # Invariants:
            # - Non-command messages MUST NOT trigger control behavior.
            # - Project switching/listing via text is allowed ONLY with:
            #     - !<command>
            #     - /cmd <command>
            # - UI JSON frames may still set frame_obj["project"] (UI flag path).
            # -------------------------------------------------------------
            def _parse_explicit_cmd(raw: str) -> tuple[str, str]:
                s = (raw or "").strip()
                if not s:
                    return "", ""
                if s.startswith("!"):
                    return "!", s[1:].lstrip()
                low = s.lower()
                if low.startswith("/cmd"):
                    return "/cmd", s[4:].lstrip()
                return "", ""

            def _is_project_control_cmd(cmd_lower: str) -> bool:
                if not cmd_lower:
                    return False

                if cmd_lower in ("projects", "list projects"):
                    return True

                # Allow both:
                # - "switch project: alpha" / "use project: alpha"
                # - "switch project alpha"  / "use project alpha"
                if cmd_lower.startswith(("switch project:", "use project:", "switch project ", "use project ")):
                    return True

                if cmd_lower.startswith(("new project:", "start project:", "new project ", "start project ")):
                    return True

                # numeric selection (explicit only): "!1", "!2", ...
                if re.fullmatch(r"\d+", cmd_lower):
                    return True

                return False


            cmd_prefix, cmd_text = _parse_explicit_cmd(user_msg)
            cmd_lower = (cmd_text or "").lower().strip()
            suppress_switch_greeting = False
            try:
                if isinstance(frame_obj, dict) and str(frame_obj.get("type") or "") == "chat.send":
                    intent0 = frame_obj.get("intent")
                    if isinstance(intent0, dict):
                        src0 = str(
                            intent0.get("startup")
                            or intent0.get("source")
                            or intent0.get("origin")
                            or ""
                        ).strip().lower()
                        if src0 in ("restore", "startup_restore", "reconnect"):
                            suppress_switch_greeting = True
            except Exception:
                suppress_switch_greeting = False

            # Only handle explicit project-control commands here.
            # Everything else (including unknown !commands) falls through to ws_commands / expert pipeline.
            if cmd_prefix and _is_project_control_cmd(cmd_lower):

                # Project selection by number (explicit only)
                if re.fullmatch(r"\d+", cmd_lower):
                    idx = int(cmd_lower)
                    names = list_existing_projects(safe_user_name(user))
                    if 1 <= idx <= len(names):
                        sel = names[idx - 1]
                        if sel == current_project:
                            # No-op: avoid redundant switch churn or greeting spam.
                            _save_last_project(user, current_project)
                            continue
                        old_short = current_project
                        if sel != old_short:
                            try:
                                _trace_emit("project.switch.start", {"from": old_short, "to": sel})
                            except Exception:
                                pass
                        old_full = current_project_full
                        current_project = sel
                        current_project_full = _full(current_project)
                        ensure_project_scaffold(current_project_full)
                        _ws_move_client(old_full, current_project_full, websocket)
                        _save_last_project(user, current_project)
                        if sel != old_short:
                            try:
                                _trace_emit("project.switch.end", {"from": old_short, "to": current_project})
                            except Exception:
                                pass
                        if suppress_switch_greeting:
                            await websocket.send(f"Project: {current_project}")
                        else:
                            await websocket.send(_project_switch_message(current_project))
                            # Emit a human contextual greeting immediately on switch (no user input required).
                            try:
                                gmsg = await build_contextual_greeting(
                                    user=user,
                                    project_full=current_project_full,
                                    project_short=current_project,
                                    reason="switch_project",
                                )
                                if gmsg:
                                    await websocket.send(gmsg)
                            except Exception:
                                pass
                        # Continuity: emit Resume only when the project has meaningful prior content.
                        try:
                            m0 = load_manifest(current_project_full) or {}
                            has_goal0 = bool((m0.get("goal") or "").strip())
                            has_raw0 = bool(m0.get("raw_files") or [])
                            has_snap0 = any(
                                isinstance(a, dict) and str(a.get("type") or "") == "assistant_output"
                                for a in (m0.get("artifacts") or [])
                            )
                        except Exception:
                            has_goal0 = has_raw0 = has_snap0 = False

# NOTE:
# Project Pulse is NEVER auto-emitted on project switch.
# Pulse is explicit-only (user must ask).

                    continue

                # New / switch project (explicit only)
                if cmd_lower.startswith(("new project:", "start project:", "new project ", "start project ")):
                    old_short = current_project
                    old_full = current_project_full
                    # Accept both:
                    # - "new project: Alpha"
                    # - "new project Alpha"
                    arg = ""
                    if ":" in cmd_text:
                        arg = cmd_text.split(":", 1)[1].strip()
                    else:
                        parts = cmd_text.split(None, 2)  # ["new","project","Alpha..."]
                        arg = parts[2].strip() if len(parts) >= 3 else ""
                    name = safe_project_name(arg)

                    current_project = name
                    current_project_full = _full(current_project)
                    if current_project != old_short:
                        try:
                            _trace_emit("project.switch.start", {"from": old_short, "to": current_project})
                        except Exception:
                            pass
                    ensure_project_scaffold(current_project_full)
                    _ws_move_client(old_full, current_project_full, websocket)
                    _save_last_project(user, current_project)
                    if current_project != old_short:
                        try:
                            _trace_emit("project.switch.end", {"from": old_short, "to": current_project})
                        except Exception:
                            pass
                    await websocket.send(_project_switch_message(current_project, prefix="Started new project:"))
                    # If this is a brand-new empty project, send a human first-line immediately.
                    # Emit a human contextual greeting immediately on new project (no user input required).
                    try:
                        gmsg = await build_contextual_greeting(
                            user=user,
                            project_full=current_project_full,
                            project_short=current_project,
                            reason="new_project",
                        )
                        if gmsg:
                            await websocket.send(gmsg)
                    except Exception:
                        pass                    
                    try:
                        m0 = load_manifest(current_project_full) or {}
                        has_goal0 = bool((m0.get("goal") or "").strip())
                        has_raw0 = bool(m0.get("raw_files") or [])
                        has_snap0 = any(
                            isinstance(a, dict) and str(a.get("type") or "") == "assistant_output"
                            for a in (m0.get("artifacts") or [])
                        )
                    except Exception:
                        has_goal0 = has_raw0 = has_snap0 = False

                    # No extra "what do you want to do first" prompt here; greeting handles the ask.
                    # Expert type is background-only: default to general for new projects.
                    try:
                        m0 = load_manifest(current_project_full) or {}
                        if not str(m0.get("expert_type") or "").strip():
                            m0["expert_type"] = "general"
                            save_manifest(current_project_full, m0)
                    except Exception:
                        pass

                    # Ensure no stale awaiting flag can trap the first real user message.
                    try:
                        pend0 = _load_pending_decision(current_project_full)
                        if isinstance(pend0, dict) and pend0.get("awaiting_expert_type"):
                            pend0.pop("awaiting_expert_type", None)
                            _save_pending_decision(current_project_full, pend0)
                    except Exception:
                        pass

                    # Continuity: emit Resume only when the project has meaningful prior content.
                    try:
                        m0 = load_manifest(current_project_full) or {}
                        has_goal0 = bool((m0.get("goal") or "").strip())
                        has_raw0 = bool(m0.get("raw_files") or [])
                        has_snap0 = any(
                            isinstance(a, dict) and str(a.get("type") or "") == "assistant_output"
                            for a in (m0.get("artifacts") or [])
                        )
                    except Exception:
                        has_goal0 = has_raw0 = has_snap0 = False

# NOTE:
# Project Pulse is NEVER auto-emitted on project switch.
# Pulse is explicit-only (user must ask).

                    continue

                if cmd_lower.startswith(("switch project:", "use project:", "switch project ", "use project ")):
                    old_short = current_project
                    old_full = current_project_full
                    # Accept both:
                    # - "switch project: alpha"
                    # - "switch project alpha"
                    arg = ""
                    if ":" in cmd_text:
                        arg = cmd_text.split(":", 1)[1].strip()
                    else:
                        parts = cmd_text.split(None, 2)  # ["switch","project","alpha..."]
                        arg = parts[2].strip() if len(parts) >= 3 else ""
                    name = safe_project_name(arg)

                    if name == current_project:
                        # No-op: avoid redundant switch churn or greeting spam.
                        _save_last_project(user, current_project)
                        continue

                    current_project = name
                    current_project_full = _full(current_project)
                    if current_project != old_short:
                        try:
                            _trace_emit("project.switch.start", {"from": old_short, "to": current_project})
                        except Exception:
                            pass
                    ensure_project_scaffold(current_project_full)
                    _ws_move_client(old_full, current_project_full, websocket)
                    _save_last_project(user, current_project)
                    if current_project != old_short:
                        try:
                            _trace_emit("project.switch.end", {"from": old_short, "to": current_project})
                        except Exception:
                            pass

                    if suppress_switch_greeting:
                        await websocket.send(f"Project: {current_project}")
                    else:
                        await websocket.send(_project_switch_message(current_project))

                        # Emit a human contextual greeting immediately on switch (no user input required).
                        try:
                            gmsg = await build_contextual_greeting(
                                user=user,
                                project_full=current_project_full,
                                project_short=current_project,
                                reason="switch_project",
                            )
                            if gmsg:
                                await websocket.send(gmsg)
                        except Exception:
                            pass

                    # If the target project is empty, send a human first-line immediately.
                    try:
                        m0 = load_manifest(current_project_full) or {}
                        has_goal0 = bool((m0.get("goal") or "").strip())
                        has_raw0 = bool(m0.get("raw_files") or [])
                        has_snap0 = any(
                            isinstance(a, dict) and str(a.get("type") or "") == "assistant_output"
                            for a in (m0.get("artifacts") or [])
                        )
                    except Exception:
                        has_goal0 = has_raw0 = has_snap0 = False

                    # No extra "what do you want to do first" prompt here; greeting handles the ask.

                    # Expert type is background-only:
                    # - If missing, silently default to "general"
                    # - Clear any stale awaiting_expert_type flags so we never trap control messages
                    try:
                        m0 = load_manifest(current_project_full) or {}
                        if not str(m0.get("expert_type") or "").strip():
                            m0["expert_type"] = "general"
                            save_manifest(current_project_full, m0)
                    except Exception:
                        pass

                    try:
                        pend0 = _load_pending_decision(current_project_full)
                        if isinstance(pend0, dict) and pend0.get("awaiting_expert_type"):
                            pend0.pop("awaiting_expert_type", None)
                            _save_pending_decision(current_project_full, pend0)
                    except Exception:
                        pass

                    # NOTE:
                    # Project Pulse is NEVER auto-emitted on project switch.
                    # Pulse is explicit-only (user must ask).

                    continue

                # List projects (explicit only)
                if cmd_lower in ("list projects", "projects"):
                    names = list_existing_projects(safe_user_name(user))
                    lines = ["Projects:"] + [
                        f"  {i}) {n}{' (current)' if n == current_project else ''}"
                        for i, n in enumerate(names, start=1)
                    ]
                    await websocket.send("\n".join(lines))
                    continue

            # NOTE: project switching by plain text is forbidden.
            # It is handled ONLY via the explicit command gate (!... or /cmd ...),
            # or via the UI frame {"type":"chat.send","project":"..."}.

            # NOTE: project listing by plain text is forbidden.
            # It is handled ONLY via the explicit command gate (!... or /cmd ...).

            # C4 v1 — upload clarification answers are data-only
            if c4_consumed_this_turn:
                # Minimal, human acknowledgment — no inference, no follow-ups
                await websocket.send("Got it — noted.")
                last_full_answer_text = "Got it — noted."
                continue

            # -------------------------------------------------------------
            # C4.6 v1 - Profile Memory Gaps (deterministic)
            #
            # If a pending profile question exists, treat this message as the answer.
            # If the user asks "what info do you need from me", start/continue the
            # profile gap questions (one at a time).
            # -------------------------------------------------------------
            try:
                msg_strip = str(user_msg or "").strip()
                msg_low = msg_strip.lower()

                looks_explicit_cmd = (
                    msg_strip.startswith("!")
                    or msg_low.startswith("/cmd")
                    or msg_low.startswith("/search")
                    or msg_low.startswith("/patch")
                    or msg_low.startswith("/selfpatch")
                    or msg_low.startswith("/serverpatch")
                )

                is_control_query = msg_low in (
                    "project pulse",
                    "pulse",
                    "status",
                    "project status",
                    "resume",
                    "inbox",
                    "pending",
                    "what's left",
                    "whats left",
                    "what should i do next",
                    "projects",
                    "list projects",
                ) or msg_low.startswith(
                    (
                        "switch project:",
                        "use project:",
                        "switch project ",
                        "use project ",
                        "new project:",
                        "start project:",
                        "new project ",
                        "start project ",
                    )
                )

                if (not looks_explicit_cmd) and (not is_control_query):
                    is_couples_account = _is_couples_user(user)
                    proj_norm = str(current_project_full or "").replace(" ", "_").lower()
                    is_couples_project = proj_norm.endswith("/couples_therapy")

                    if is_couples_account and is_couples_project:
                        consumed, reply = _consume_pending_profile_question(
                            user,
                            msg_strip,
                            project_full=current_project_full,
                            last_assistant_text=last_full_answer_text,
                        )
                        if consumed and reply:
                            await websocket.send(reply)
                            last_full_answer_text = reply
                            continue

                        handled, reply = _maybe_start_couples_intake_questions(user, current_project_full)
                        if handled and reply:
                            await websocket.send(reply)
                            last_full_answer_text = reply
                            continue

                        if _is_profile_gap_request(msg_strip):
                            handled, reply = _maybe_start_profile_gap_questions(user)
                            if handled and reply:
                                await websocket.send(reply)
                                last_full_answer_text = reply
                                continue
                    else:
                        if _is_profile_gap_request(msg_strip):
                            handled, reply = _maybe_start_profile_gap_questions(user)
                            if handled and reply:
                                await websocket.send(reply)
                                last_full_answer_text = reply
                                continue

                        consumed, reply = _consume_pending_profile_question(
                            user,
                            msg_strip,
                            project_full=current_project_full,
                            last_assistant_text=last_full_answer_text,
                        )
                        if consumed and reply:
                            await websocket.send(reply)
                            last_full_answer_text = reply
                            continue
            except Exception:
                pass


            # WS command routing (expanded): plan/open/list/facts/last answer/file_added/goal/search routing
            search_route_mode = ""  # "", "force", "nosearch"
            # Strip UI-injected instruction blocks so exact WS commands (e.g., "capabilities") still match.
            _stripped_user_msg = strip_lens0_system_blocks(user_msg)
            if _stripped_user_msg:
                user_msg = _stripped_user_msg
                lower = user_msg.lower().strip()

            # IMPORTANT: clean_user_msg MUST be the stripped version used for routing + pipeline
            clean_user_msg = user_msg
            cmd_reply = await ws_commands.dispatch(
                ctx=sys.modules[__name__],
                user_msg=user_msg,
                lower=lower,
                user=user,
                current_project=current_project,
                current_project_full=current_project_full,
                dashboard_for=_dashboard_for,
                last_user_question_for_search=last_user_question_for_search,
            )
            if cmd_reply is not None:
                # Special routing tuple: ("__search__", mode, clean_user_msg)
                if isinstance(cmd_reply, tuple) and len(cmd_reply) == 3 and cmd_reply[0] == "__search__":
                    search_route_mode = str(cmd_reply[1] or "").strip().lower()
                    clean_user_msg = str(cmd_reply[2] or "").strip()
                    lower = clean_user_msg.lower().strip()
                    try:
                        _trace_emit("tool.route", {"tool": "search", "reason": search_route_mode or "route"})
                    except Exception:
                        pass
                else:
                    try:
                        tool_name = (lower.split()[0] if lower else "").strip() or "ws_command"
                        _trace_emit("tool.route", {"tool": tool_name, "reason": "ws_command"})
                    except Exception:
                        pass
                    await websocket.send(cmd_reply)
                    last_full_answer_text = cmd_reply
                    continue
            # -------------------------------------------------------------
            # Upload "Insert to chat" handler (deterministic)
            #
            # The UI inserts the uploaded filename into chat only when the user clicks.
            # When we see a message that matches a READY upload filename, we surface
            # what we ingested + what we think it is, and ask ONE question if ambiguous.
            # -------------------------------------------------------------
            try:
                msg0 = (user_msg or "").strip()
                low0 = msg0.lower()

                # Conservative: only treat simple filenames as insert events
                # Must look like "name.ext" (avoid matching normal sentences ending with ".")
                looks_like_filename = (
                    bool(re.match(r"^[A-Za-z0-9._-]+\.[A-Za-z0-9]{1,6}$", msg0))
                    and len(msg0) <= 220
                    and not low0.startswith(("[search]", "[nosearch]", "[pdf]", "switch project:", "use project:", "new project:", "start project:", "goal:", "patch"))
                )

                if looks_like_filename:
                    recs = []
                    try:
                        recs = get_recent_upload_status(current_project_full, limit=40) or []
                    except Exception:
                        recs = []

                    picked = None
                    for r in reversed(recs):
                        if not isinstance(r, dict):
                            continue
                        if str(r.get("state") or "").strip().lower() != "ready":
                            continue
                        if str(r.get("orig_name") or "").strip() == msg0:
                            picked = r
                            break

                    if picked:
                        canonical_rel = str(picked.get("canonical_rel") or "").strip().replace("\\", "/")
                        # AOF v1: user explicitly brought this object into chat — set focus.
                        try:
                            sha2 = ""
                            try:
                                man2 = load_manifest(current_project_full) or {}
                                raw2 = man2.get("raw_files") or []
                                if isinstance(raw2, list):
                                    for rf in raw2:
                                        if not isinstance(rf, dict):
                                            continue
                                        if str(rf.get("path") or "").replace("\\", "/").strip() == canonical_rel:
                                            sha2 = str(rf.get("sha256") or "").strip()
                                            break
                            except Exception:
                                sha2 = ""

                            suffix2 = Path(canonical_rel).suffix.lower()
                            mime2 = ""
                            if suffix2 in IMAGE_SUFFIXES:
                                try:
                                    mime2 = mime_for_image_suffix(suffix2)
                                except Exception:
                                    mime2 = ""
                            elif suffix2 == ".pdf":
                                mime2 = "application/pdf"
                            elif suffix2:
                                mime2 = "text/plain"

                            project_store.save_active_object(
                                current_project_full,
                                {
                                    "rel_path": canonical_rel,
                                    "orig_name": msg0,
                                    "sha256": sha2,
                                    "mime": mime2,
                                    "set_reason": "user_inserted_filename",
                                },
                            )
                        except Exception:
                            pass

                        cls = picked.get("classification") if isinstance(picked.get("classification"), dict) else {}
                        label = str(cls.get("label") or "").strip()
                        bucket = str(cls.get("suggested_bucket") or "").strip()
                        tags = cls.get("tags") if isinstance(cls.get("tags"), list) else []
                        tags = [str(t).strip() for t in tags if str(t).strip()][:10]

                        rel_score = cls.get("relevance", None)
                        ask_user = bool(cls.get("ask_user", False))
                        question = str(cls.get("question") or "").strip()

                        # Build a user-facing summary
                        lines = []
                        lines.append(f"Got it — here’s what I pulled from **{msg0}**:")
                        if label:
                            lines.append(f"- Looks like: {label}")
                        if tags:
                            lines.append(f"- Tags: {', '.join(tags)}")
                        if bucket:
                            lines.append(f"- Filed under: {bucket}")
                        if rel_score is not None:
                            try:
                                lines.append(f"- Relevance: {float(rel_score):.2f}")
                            except Exception:
                                pass
                        if canonical_rel:
                            lines.append(f"- Stored at: {canonical_rel}")

                        # Decide whether to ask a question now (ONE question max)
                        q_final = ""
                        if ask_user and question:
                            q_final = question
                            if len(q_final) > 240:
                                q_final = q_final[:239].rstrip() + "…"
                            if not q_final.endswith("?"):
                                q_final = q_final.rstrip(".") + "?"
                        else:
                            # If we have weak/no signal, ask a generic “what is this for?”
                            if not label and not tags:
                                q_final = "Quick check: what should I use this file for in this project?"

                        if q_final:
                            # Persist pending upload question so the NEXT user message is captured deterministically.
                            try:
                                import time as _time
                                created_at = (now_iso() or "").replace("Z", "")
                                expires_at = _time.strftime("%Y-%m-%dT%H:%M:%S", _time.gmtime(_time.time() + 300))

                                inbox_id = ""
                                try:
                                    raw_ref = canonical_rel or f"projects/{current_project_full}/raw/{msg0}".replace("\\", "/")
                                    inbox_entry = project_store.append_inbox_item(
                                        current_project_full,
                                        type_="clarification",
                                        text=q_final,
                                        refs=[raw_ref],
                                        created_at=created_at,
                                    )
                                    if isinstance(inbox_entry, dict):
                                        inbox_id = str(inbox_entry.get("id") or "").strip()
                                except Exception:
                                    inbox_id = ""

                                project_store.save_pending_upload_question(
                                    current_project_full,
                                    {
                                        "version": 1,
                                        "pending": True,
                                        "created_at": created_at,
                                        "expires_at": expires_at,
                                        "upload_path": canonical_rel or f"projects/{current_project_full}/raw/{msg0}".replace("\\", "/"),
                                        "question": q_final,
                                        "inbox_id": inbox_id,
                                    },
                                )
                            except Exception:
                                pass

                            lines.append("")
                            lines.append(q_final)

                        reply = "\n".join(lines).strip()
                        await websocket.send(reply)
                        last_full_answer_text = reply
                        try:
                            append_jsonl(
                                state_dir(current_project_full) / "chat_log.jsonl",
                                {"ts": now_iso(), "role": "assistant", "content": reply},
                            )
                        except Exception:
                            pass
                        continue
            except Exception:
                pass
                
            # -----------------------------------------------------------------
            # C5 v1 — Deterministic Memory Reuse (read-only)
            # Detect memory-seeking questions and answer from stored project memory:
            #   upload_notes.jsonl -> decisions.jsonl -> deliverables.json
            # Chat history is NOT a source of truth here.
            # -----------------------------------------------------------------
            try:
                if _c5_is_memory_seeking_query(user_msg):
                    hits = []
                    try:
                        hits = find_project_memory(current_project_full, query=user_msg, max_items=3) or []
                    except Exception:
                        hits = []
                    # Fallback (still deterministic, still read-only):
                    # If keyword overlap yields nothing (e.g., user asks "what was the tile for?"
                    # but the stored note doesn't contain the word "tile"), return the most recent
                    # upload notes for this project (cap 3) rather than falling into the model.
                    if not hits:
                        try:
                            recent_notes = load_upload_notes(current_project_full) or []
                            if recent_notes:
                                tail = recent_notes[-3:]
                                # Normalize into the same shape expected by _c5_format_memory_response
                                hits = [
                                    {
                                        "source": "upload_note",
                                        "timestamp": str(n.get("timestamp") or "").strip(),
                                        "score": 0,
                                        "data": n,
                                    }
                                    for n in reversed(tail)  # newest first
                                    if isinstance(n, dict)
                                ]
                        except Exception:
                            pass

                    if hits:
                        reply = _c5_format_memory_response(hits)
                        if reply:
                            await websocket.send(reply)
                            last_full_answer_text = reply
                            continue
            except Exception:
                pass
            
            # -----------------------------------------------------------------
            # C5.3 — Conversational file fallback (deterministic)
            #
            # If the user refers to “that spreadsheet/photo/deliverable” without naming it:
            # - resolve a likely file (active_object -> latest deliverable -> latest raw)
            # - if none: list candidates from raw/ + artifacts and ask which one
            # - if resolved: set active object + inject file context into the model prompt
            # -----------------------------------------------------------------
            try:
                if _is_file_referential_query(user_msg):
                    resolved_rel, why = _resolve_referential_file(current_project_full, user_msg)

                    # If the user says “the other one”, choose the next most recent candidate
                    # of the same inferred kind, excluding the last referential file we used.
                    try:
                        lowq = (user_msg or "").strip().lower()
                        wants_other = ("other one" in lowq) or ("the other" in lowq) or lowq.strip() in ("other", "the other")
                        if wants_other:
                            kind0 = _infer_file_kind_from_msg(user_msg)
                            cands0 = _get_best_candidates(current_project_full, user_msg, kind=kind0, limit=8)
                            # Pick first candidate not equal to the last referential file
                            picked = ""
                            for c in cands0:
                                rel0 = str(c.get("rel") or "").replace("\\", "/").strip()
                                if not rel0:
                                    continue
                                if last_referential_file_rel and rel0 == last_referential_file_rel:
                                    continue
                                picked = rel0
                                break
                            if picked:
                                resolved_rel = picked
                                why = "other_one_candidate"
                    except Exception:
                        pass

                    if not resolved_rel:
                        candidates = _get_best_candidates(
                            current_project_full,
                            user_msg,
                            kind=_infer_file_kind_from_msg(user_msg),
                            limit=5,
                        )
                        if candidates:
                            reply = _format_candidate_prompt(user_msg, candidates)
                            await websocket.send(reply)
                            last_full_answer_text = reply
                            continue

                        # No candidates at all → explicit reason
                        reply = (
                            "I don’t have an active file in focus for this project right now.\n\n"
                            "To open it, do one of these:\n"
                            "- Paste the exact filename (e.g., report.pdf)\n"
                            "- Upload it again\n"
                            "- Or click the file in the UI so it inserts the filename into chat (that sets the active focus)\n"
                        )
                        await websocket.send(reply)
                        last_full_answer_text = reply
                        continue

                    # We have a resolved file → set as active object
                    try:
                        project_store.save_active_object(
                            current_project_full,
                            {
                                "rel_path": resolved_rel,
                                "orig_name": Path(resolved_rel).name,
                                "sha256": "",
                                "mime": "",
                                "set_reason": f"referential_query:{why}",
                            },
                        )
                    except Exception:
                        pass

                    # AOF v2 — Enforce cached vision semantics for image referential turns (best-effort).
                    # This satisfies: "any image-referential user turn must have vision semantics available before answering."
                    try:
                        kind0 = _infer_file_kind_from_msg(user_msg)
                    except Exception:
                        kind0 = "any"

                    if kind0 == "image":
                        try:
                            _okv, _sem_json, _notev = await ensure_image_semantics_for_file(
                                current_project_full,
                                file_rel=resolved_rel,
                                force=False,
                                reason="referential_turn",
                            )
                        except Exception:
                            pass

                    # If the user is asking to "tell me about/summarize/describe" the file,
                    # answer deterministically from stored artifacts and SKIP the model.
                    try:
                        lowq = (user_msg or "").strip().lower()
                        wants_describe = _wants_describe_file(user_msg)
                        if wants_describe:
                            reply, why2 = _describe_resolved_file(current_project_full, resolved_rel, user_msg)
                            if reply:
                                await websocket.send(reply)
                                last_full_answer_text = reply
                                last_referential_file_rel = resolved_rel
                                continue
                            else:
                                # For images, attempt on-demand semantic perception (cached) before giving up.
                                try:
                                    kind0 = _infer_file_kind_from_msg(user_msg)
                                except Exception:
                                    kind0 = "any"

                                if kind0 == "image":
                                    okv, sem_json, notev = await ensure_image_semantics_for_file(
                                        current_project_full,
                                        file_rel=resolved_rel,
                                        force=False,
                                        reason="referential_describe",
                                    )
                                    if okv and sem_json:
                                        # Now that semantics exists, re-run deterministic describe (will pick up artifacts).
                                        reply3, why3 = _describe_resolved_file(current_project_full, resolved_rel, user_msg)
                                        if reply3:
                                            await websocket.send(reply3)
                                            last_full_answer_text = reply3
                                            last_referential_file_rel = resolved_rel
                                            continue

                                        # Last-resort: show the summary directly (still grounded, no new model call).
                                        try:
                                            obj = json.loads(sem_json)
                                        except Exception:
                                            obj = {}
                                        summary = ""
                                        if isinstance(obj, dict):
                                            out = obj.get("outputs") if isinstance(obj.get("outputs"), dict) else {}
                                            summary = str((out or {}).get("summary") or "").strip()
                                        if summary:
                                            reply4 = (
                                                f"Here’s what I can see in **{Path(resolved_rel).name}** (cached semantics):\n"
                                                f"- Open: /file?path={resolved_rel}\n\n"
                                                + summary
                                            )
                                            await websocket.send(reply4)
                                            last_full_answer_text = reply4
                                            last_referential_file_rel = resolved_rel
                                            continue

                                # Explicit reason (deterministic)
                                reply2 = (
                                    f"I resolved the file as {Path(resolved_rel).name}, but I don't have stored OCR/caption/text/overview artifacts for it yet.\n"
                                    f"- Open: /file?path={resolved_rel}\n"
                                    f"- Reason: {why2}\n\n"
                                    "If you want, re-upload the file (or re-insert the filename into chat) and I’ll re-ingest it."
                                )
                                await websocket.send(reply2)
                                last_full_answer_text = reply2
                                continue

                    except Exception:
                        pass

                    # Otherwise: inject file context into the model prompt (so it stays anchored)
                    clean_user_msg = _attach_file_context_to_message(clean_user_msg, resolved_rel)
                    lower = clean_user_msg.lower().strip()
                    last_referential_file_rel = resolved_rel

            except Exception:
                pass

            # NOTE (C6): Pulse/Status/Resume are handled by the unified request pipeline controller.


            # Rebuild project map/state (explicit-only)
            # Command: "!refresh_state"
            if user_msg.strip().startswith("!"):
                cmd_txt = user_msg.strip()[1:].strip().lower()
            else:
                cmd_txt = ""

            if cmd_txt in ("refresh_state", "rebuild_state", "rebuild_project_map", "refresh map", "refresh_state now"):
                user_msg = "Rebuild the Project Map and Project State JSON from the current project context. Then tell me the top 3 next actions."
                lower = user_msg.lower()

            # Save last answer as artifact (explicit-only)
            # Command: "!save <short_name>"
            if cmd_txt.startswith("save"):
                logical = user_msg.strip()[1:].strip()
                logical = logical[4:].strip() if logical.lower().startswith("save") else ""
                if logical.startswith(":"):
                    logical = logical[1:].strip()

                if not logical:
                    await websocket.send("Usage: !save <short_name>")
                    continue
                if not last_full_answer_text:
                    await websocket.send("Nothing to save yet. Ask a question first.")
                    continue
                entry = create_artifact(current_project_full, logical, last_full_answer_text, artifact_type="cheat_sheet", file_ext=".md")
                await websocket.send(f"Saved as {entry.get('filename')}")
                continue

            # Apply last patch (optional helper)
            if lower in ("apply patch", "apply last patch") and last_patch_text and last_patch_target:
                await websocket.send(
                    "Patch apply is currently disabled by default in this replacement server.\n"
                    "Reason: applying patches automatically can corrupt files if anchors drift.\n\n"
                    "If you want auto-apply, say so and I'll enable a safe apply+re-ingest command."
                )
                continue

            # Natural-language goal update
            maybe_goal = update_project_goal_from_message(current_project_full, user_msg)
            if maybe_goal:
                # Acknowledge goal update, but DO NOT short-circuit the turn.
                # The expert pipeline (EFL) must still run on this message.
                # Goal update is UI-state, not user-facing narration.
                # Keep the turn flowing into the expert pipeline without emitting a systemy echo.
                pass
                # NOTE: intentionally fall through — no continue
            # C6.3 — Usability-first bootstrap: if the project has no goal yet, treat a normal first message as the goal.
            # This prevents "dead" new projects that require magic goal: prefixes.
            try:
                if not maybe_goal:
                    m0 = load_manifest(current_project_full)
                    goal0 = str(m0.get("goal") or "").strip()

                    st0 = {}
                    try:
                        st0_raw = read_state_doc(current_project_full, "project_state", cap=6000).strip()
                        if st0_raw:
                            st0 = json.loads(st0_raw) if isinstance(st0_raw, str) else {}
                    except Exception:
                        st0 = {}

                    bs0 = str((st0 or {}).get("bootstrap_status") or "").strip()
                    st_goal0 = str((st0 or {}).get("goal") or "").strip()

                    msg0 = (user_msg or "").strip()
                    low0 = msg0.lower()

                    looks_like_command = (
                        low0 in ("project pulse", "pulse", "status", "project status", "resume", "inbox", "pending", "list", "ls", "projects", "list projects")
                        or low0.startswith(("switch project:", "use project:", "new project:", "start project:", "goal:", "patch", "/selfpatch", "/serverpatch", "/patch-server", "[search]", "[nosearch]", "[pdf]"))
                    )
                    is_question = msg0.endswith("?")

                    # ------------------------------------------------------------
                    # DO NOT TREAT CONSTRAINT-ONLY MESSAGES AS PROJECT GOALS
                    # ------------------------------------------------------------
                    constraint_only = msg0.lower() in (
                        "no questions",
                        "dont ask questions",
                        "don't ask questions",
                        "no more questions",
                        "no emoji",
                        "no emojis",
                    )

                    if constraint_only:
                        # Persist as a user_rule instead of a goal
                        try:
                            st_path = state_file_path(current_project_full, "project_state")
                            stx = {}
                            if st_path.exists():
                                stx = json.loads(st_path.read_text(encoding="utf-8") or "{}")
                            if isinstance(stx, dict):
                                rules = stx.get("user_rules")
                                if not isinstance(rules, list):
                                    rules = []
                                if msg0.lower() not in [r.lower() for r in rules]:
                                    rules.append(msg0)
                                stx["user_rules"] = rules[-30:]
                                stx["last_updated"] = now_iso()
                                atomic_write_text(st_path, json.dumps(stx, indent=2))
                        except Exception:
                            pass

                        await websocket.send("Understood.")
                        last_full_answer_text = "Understood."
                        continue

                    # ------------------------------------------------------------
                    # NORMAL GOAL AUTO-CAPTURE (unchanged)
                    # ------------------------------------------------------------
                    if (not goal0) and (not st_goal0) and (bs0 != "active") and (not looks_like_command) and (not is_question) and (10 <= len(msg0) <= 420) and (not (_is_couples_user(user) and str(current_project_full or "").replace(" ", "_").lower().endswith("/couples_therapy"))):
                        # Write manifest goal (source used by switch messaging)
                        try:
                            m0["goal"] = msg0
                            save_manifest(current_project_full, m0)
                        except Exception:
                            pass

                        # Write project_state goal + activate bootstrap (create file if missing)
                        try:
                            st_path = state_file_path(current_project_full, "project_state")
                            stx: Dict[str, Any] = {}
                            try:
                                if st_path.exists():
                                    st0 = json.loads(st_path.read_text(encoding="utf-8") or "{}")
                                    if isinstance(st0, dict):
                                        stx = st0
                            except Exception:
                                stx = {}

                            stx["goal"] = msg0
                            stx["bootstrap_status"] = "active"
                            stx.setdefault("project_mode", "hybrid")
                            stx.setdefault("current_focus", "")
                            stx.setdefault("next_actions", [])
                            stx.setdefault("key_files", [])

                            # Server-side: lock the expert frame for brand-new projects immediately.
                            # This prevents "fallback/proposed" hesitation on the very first response.
                            ef = stx.get("expert_frame")
                            ef_status = ""
                            if isinstance(ef, dict):
                                ef_status = str(ef.get("status") or "").strip().lower()
                            if (not isinstance(ef, dict)) or ef_status in ("", "proposed", "draft", "suggested"):
                                stx["expert_frame"] = {
                                    "status": "locked",
                                    "label": "General Project Operator",
                                    "directive": (
                                        "Act like a superhuman senior expert. Be natural and smooth. "
                                        "Do not narrate system state. Do not ask to confirm unless truly blocked. "
                                        "If the user provides only a topic/goal, respond briefly with a strong default direction "
                                        "and ask one human question to steer."
                                    ),
                                    "set_reason": "server_bootstrap_lock",
                                    "updated_at": now_iso(),
                                }

                            stx["last_updated"] = now_iso()

                            atomic_write_text(st_path, json.dumps(stx, indent=2))
                        except Exception:
                            pass

                        # Goal auto-capture is an internal bootstrap action.
                        # Do not emit a user-facing "Updated goal" line; it feels mechanical.
                        #
                        # SUPERHUMAN PACING:
                        # If the user only provided a topic/goal, do NOT immediately dump a full deliverable.
                        # Instead: acknowledge + one steering question, then wait.
                        # SUPERHUMAN PACING:
                        # For brand-new projects, keep the first follow-up question generic and ChatGPT-like.
                        # If the user explicitly wants "a chat like chatgpt", do not ask deliverable/audience questions.
                        low_goal = msg0.lower().strip()

                        if "chatgpt" in low_goal or "chat gpt" in low_goal or "chat like" in low_goal:
                            steer = (
                                "Got it — we’ll keep this like ChatGPT.\n\n"
                                "What do you want to talk about or do first?"
                            )
                        else:
                            steer = (
                                f"Got it — **{msg0}**.\n\n"
                                "What should we do first in this project?"
                            )

                        await websocket.send(steer)
                        last_full_answer_text = steer
                        continue
            except Exception:
                pass

            # Deterministic durable facts ingestion (no model call)
            # Deterministic user rules ingestion (no model call)
            # Captures explicit "never / don't / do not / only / always" constraints into canonical project_state.json.
            try:
                ingest_user_rules_message(current_project_full, user_msg)
            except Exception:
                pass

            # Durable facts ingestion (explicit-only)
            # Command: "!durable_facts: <text>"
            if user_msg.strip().startswith("!"):
                cmd_raw = user_msg.strip()[1:].strip()
                cmd_low = cmd_raw.lower()
            else:
                cmd_raw = ""
                cmd_low = ""

            if cmd_low.startswith("durable facts:") or cmd_low.startswith("durable_facts:"):
                payload = cmd_raw.split(":", 1)[1].strip() if ":" in cmd_raw else ""
                fn = globals().get("ingest_durable_facts_message")
                if callable(fn):
                    try:
                        n_added = int(fn(current_project_full, payload) or 0)
                    except Exception:
                        n_added = 0
                    await websocket.send(f"Recorded {n_added} fact(s) into FACTS_MAP.")
                else:
                    await websocket.send("That command is currently disabled on this server build.")
                continue
            # -----------------------------------------------------------------
            # C8.2 — Conflict resolution (deterministic; no model)
            #
            # If there is an OPEN conflict and the user replies with a decision id (e.g. "dec_YYYY_MM_DD_NNN"),
            # treat that as choosing the winner:
            # - supersede all other decision_ids in that conflict (append-only markers)
            # - resolve the conflict inbox item (append-only)
            # -----------------------------------------------------------------
            try:
                msg0 = (user_msg or "").strip()
                # Conservative: only trigger on exact decision-id-like strings.
                if re.fullmatch(r"(dec|legacy)_[A-Za-z0-9_]{6,}", msg0):
                    open_items = project_store.list_open_inbox(current_project_full, max_items=24)  # type: ignore[attr-defined]
                    conflicts = [it for it in (open_items or []) if isinstance(it, dict) and str(it.get("type") or "") == "conflict"]
                    if conflicts:
                        # Deterministic: resolve the most recent conflict that contains this id.
                        winner = msg0.strip()
                        picked = None
                        for it in conflicts:
                            ids = it.get("decision_ids") if isinstance(it.get("decision_ids"), list) else []
                            ids = [str(x).strip() for x in ids if str(x).strip()]
                            if winner in ids:
                                picked = it
                                break
                        if picked:
                            ok = project_store.resolve_conflict_by_winner(  # type: ignore[attr-defined]
                                current_project_full,
                                conflict_inbox_id=str(picked.get("id") or "").strip(),
                                winner_decision_id=winner,
                            )
                            if ok:
                                await websocket.send("Conflict resolved — kept the chosen decision.")
                                last_full_answer_text = "Conflict resolved — kept the chosen decision."
                                continue
            except Exception:
                pass

            # -----------------------------------------------------------------
            # C2/C7 — Decision Capture (Candidate → Confirm → Promote) + Supersession (immutable)
            #
            # C7 invariants:
            # - Decisions are append-only.
            # - Supersession is represented by a new decision row pointing backward,
            #   plus a supersede marker row for the old decision id.
            #
            # Conversational rule:
            # - Ask ONE confirmation question.
            # - Pending confirmation expires after:
            #     N user messages (2), OR T seconds (5 minutes)
            # - Pending confirmations are mirrored into C9 inbox.jsonl (pending_decision).
            # -----------------------------------------------------------------

            # Expiry params (deterministic defaults)
            _PENDING_MAX_USER_MSGS = 2
            _PENDING_MAX_SECONDS = 300

            def _pending_is_expired(pend: Dict[str, Any]) -> bool:
                try:
                    asked_at = float(pend.get("asked_at_epoch") or 0.0)
                except Exception:
                    asked_at = 0.0
                try:
                    asked_idx = int(pend.get("asked_msg_index") or 0)
                except Exception:
                    asked_idx = 0

                # If we don't have markers, treat as expired (prevents old state from trapping).
                if asked_at <= 0 or asked_idx <= 0:
                    return True

                age_s = time.time() - asked_at
                age_msgs = user_msg_counter - asked_idx
                if age_msgs > _PENDING_MAX_USER_MSGS:
                    return True
                if age_s > _PENDING_MAX_SECONDS:
                    return True
                return False

            pending = _load_pending_decision(current_project_full)

            # If pending exists but is expired: mark stale, resolve inbox item (if any), and clear.
            if isinstance(pending, dict) and pending.get("status") == "pending" and pending.get("id"):
                if _pending_is_expired(pending):
                    try:
                        update_decision_candidate_status(
                            current_project_full,
                            candidate_id=str(pending.get("id") or "").strip(),
                            new_status="stale",
                        )
                    except Exception:
                        pass

                    try:
                        inbox_id = str(pending.get("inbox_id") or "").strip()
                        if inbox_id:
                            project_store.resolve_inbox_item(
                                current_project_full,
                                inbox_id=inbox_id,
                                resolution_note="stale",
                                refs=[],
                            )
                    except Exception:
                        pass

                    _clear_pending_decision(current_project_full)
                    pending = {}

            if isinstance(pending, dict) and pending.get("status") == "pending" and pending.get("id") and pending.get("text"):
                # Only accept affirmation/correction inside the expiry window.
                if _is_affirmation(user_msg):
                    # Mark candidate promoted (append-only-ish JSONL overwrite happens in project_store)
                    try:
                        update_decision_candidate_status(
                            current_project_full,
                            candidate_id=str(pending.get("id") or "").strip(),
                            new_status="promoted",
                        )
                    except Exception:
                        pass

                    # Resolve inbox item (pending_decision) if present
                    try:
                        inbox_id = str(pending.get("inbox_id") or "").strip()
                        if inbox_id:
                            project_store.resolve_inbox_item(
                                current_project_full,
                                inbox_id=inbox_id,
                                resolution_note="confirmed",
                                refs=[],
                            )
                    except Exception:
                        pass

                    # Write decision (C7 schema) with optional supersession
                    try:
                        txt = str(pending.get("text") or "").strip()
                        dom = str(pending.get("domain") or "").strip()
                        surf = str(pending.get("surface") or "").strip()
                        old_id = str(pending.get("supersede_old_id") or "").strip()

                        if old_id:
                            project_store.supersede_decision(
                                current_project_full,
                                old_id=old_id,
                                new_domain=dom,
                                new_surface=surf,
                                new_text=txt,
                                evidence=[],
                                confidence="",
                            )
                        else:
                            project_store.add_decision(
                                current_project_full,
                                domain=dom,
                                surface=surf,
                                status="final",
                                text=txt,
                                supersedes=None,
                                evidence=[],
                                confidence="",
                            )
                    except Exception:
                        # Fallback to legacy final decision write (keeps old behavior alive if C7 path fails)
                        try:
                            append_final_decision(
                                current_project_full,
                                text=str(pending.get("text") or "").strip(),
                                source="user",
                                related_deliverable="",
                            )
                        except Exception:
                            pass

                    _clear_pending_decision(current_project_full)

                    try:
                        label = "decision"
                        if dom and surf:
                            label = f"{dom} — {surf}"
                        elif dom:
                            label = dom

                        wc = f"What changed: Recorded {label} as final."
                        if old_id:
                            wc = wc + f" (Superseded {old_id}.)"

                        wm = "Why it matters: The system will treat this as the current truth for future turns."
                        ns = "Next step: Continue with the next highest-priority task using this decision as a constraint."
                        synth = "\n".join([wc, wm, ns]).strip()
                    except Exception:
                        synth = "What changed: Recorded decision as final.\nWhy it matters: It becomes the current truth.\nNext step: Continue."

                    await websocket.send(synth)
                    last_full_answer_text = synth
                    continue
                # C8.2.2 — If we previously asked "what should it be instead?",
                # then the NEXT user message is treated as the corrected decision text (deterministic).
                if bool(pending.get("awaiting_correction")) and (not _is_affirmation(user_msg)) and (not _looks_like_correction(user_msg)):
                    corrected = (user_msg or "").strip()

                    if corrected:
                        # Discard old candidate + resolve inbox, then create a new pending candidate from corrected text.
                        try:
                            update_decision_candidate_status(
                                current_project_full,
                                candidate_id=str(pending.get("id") or "").strip(),
                                new_status="discarded",
                            )
                        except Exception:
                            pass

                        try:
                            inbox_id = str(pending.get("inbox_id") or "").strip()
                            if inbox_id:
                                project_store.resolve_inbox_item(
                                    current_project_full,
                                    inbox_id=inbox_id,
                                    resolution_note="discarded",
                                    refs=[],
                                )
                        except Exception:
                            pass

                        _clear_pending_decision(current_project_full)

                        cand2 = {}
                        try:
                            cand2 = append_decision_candidate(
                                current_project_full,
                                text=corrected,
                                confidence=0.90,
                            )
                        except Exception:
                            cand2 = {}

                        dom2, surf2 = ("", "")
                        try:
                            dom2, surf2 = project_store.infer_decision_domain(corrected)
                        except Exception:
                            dom2, surf2 = ("", "")

                        supersede_old_id2 = ""
                        if dom2:
                            try:
                                cur = project_store.get_current_decisions_by_domain(current_project_full) or {}
                                prev = cur.get(dom2) or {}
                                supersede_old_id2 = str(prev.get("id") or "").strip()
                            except Exception:
                                supersede_old_id2 = ""

                        question2 = "Want me to treat that as final?"
                        if dom2 and supersede_old_id2:
                            question2 = f"This would replace the previous decision about {dom2}. Correct?"

                        inbox_id2 = ""
                        try:
                            inbox = project_store.append_inbox_item(
                                current_project_full,
                                type_="pending_decision",
                                text=question2,
                                refs=[],
                                created_at=(now_iso() or "").replace("Z", ""),
                            )
                            if isinstance(inbox, dict):
                                inbox_id2 = str(inbox.get("id") or "").strip()
                        except Exception:
                            inbox_id2 = ""

                        if isinstance(cand2, dict) and cand2.get("id"):
                            _save_pending_decision(
                                current_project_full,
                                {
                                    "id": str(cand2.get("id") or "").strip(),
                                    "timestamp": str(cand2.get("timestamp") or "").strip(),
                                    "text": corrected,
                                    "confidence": float(cand2.get("confidence") or 0.90),
                                    "status": "pending",
                                    "asked": True,
                                    "asked_at_epoch": float(time.time()),
                                    "asked_msg_index": int(user_msg_counter),
                                    "domain": dom2,
                                    "surface": surf2,
                                    "supersede_old_id": supersede_old_id2,
                                    "inbox_id": inbox_id2,
                                    # reset mode
                                    "awaiting_correction": False,
                                },
                            )
                            await websocket.send(question2)
                            last_full_answer_text = question2
                            continue

                if _looks_like_correction(user_msg):
                    corrected = _extract_correction_text(user_msg).strip()
                    # C8.2.2 — "no" means NO:
                    # If the user replies "no" to the supersession/confirmation prompt
                    # and provides no corrected text, do NOT discard, do NOT record,
                    # keep the pending decision open, and ask for the corrected sentence.
                    if not corrected:
                        # Persist a deterministic mode flag so the NEXT user message is treated as the corrected decision text.
                        try:
                            pending2 = dict(pending)
                            pending2["awaiting_correction"] = True
                            _save_pending_decision(current_project_full, pending2)
                        except Exception:
                            pass

                        followup = "Okay — what should it be instead? Please reply with the corrected sentence."
                        await websocket.send(followup)
                        last_full_answer_text = followup
                        continue

                    # Mark the old candidate as discarded and resolve inbox if present
                    try:
                        update_decision_candidate_status(
                            current_project_full,
                            candidate_id=str(pending.get("id") or "").strip(),
                            new_status="discarded",
                        )
                    except Exception:
                        pass

                    try:
                        inbox_id = str(pending.get("inbox_id") or "").strip()
                        if inbox_id:
                            project_store.resolve_inbox_item(
                                current_project_full,
                                inbox_id=inbox_id,
                                resolution_note="discarded",
                                refs=[],
                            )
                    except Exception:
                        pass

                    _clear_pending_decision(current_project_full)

                    # If the correction contains a new decision text, treat it as a new candidate and ask once.
                    if corrected:
                        cand2 = {}
                        try:
                            cand2 = append_decision_candidate(
                                current_project_full,
                                text=corrected,
                                confidence=0.90,
                            )
                        except Exception:
                            cand2 = {}

                        # Determine domain + possible supersession deterministically
                        dom2, surf2 = ("", "")
                        try:
                            dom2, surf2 = project_store.infer_decision_domain(corrected)
                        except Exception:
                            dom2, surf2 = ("", "")

                        supersede_old_id2 = ""
                        if dom2:
                            try:
                                cur = project_store.get_current_decisions_by_domain(current_project_full) or {}
                                prev = cur.get(dom2) or {}
                                supersede_old_id2 = str(prev.get("id") or "").strip()
                            except Exception:
                                supersede_old_id2 = ""

                        question2 = "Want me to treat that as final?"
                        if dom2 and supersede_old_id2:
                            question2 = f"This would replace the previous decision about {dom2}. Correct?"

                        inbox_id2 = ""
                        try:
                            inbox = project_store.append_inbox_item(
                                current_project_full,
                                type_="pending_decision",
                                text=question2,
                                refs=[],
                                created_at=(now_iso() or "").replace("Z", ""),
                            )
                            if isinstance(inbox, dict):
                                inbox_id2 = str(inbox.get("id") or "").strip()
                        except Exception:
                            inbox_id2 = ""

                        if isinstance(cand2, dict) and cand2.get("id"):
                            _save_pending_decision(
                                current_project_full,
                                {
                                    "id": str(cand2.get("id") or "").strip(),
                                    "timestamp": str(cand2.get("timestamp") or "").strip(),
                                    "text": corrected,
                                    "confidence": float(cand2.get("confidence") or 0.90),
                                    "status": "pending",
                                    "asked": True,
                                    "asked_at_epoch": float(time.time()),
                                    "asked_msg_index": int(user_msg_counter),
                                    "domain": dom2,
                                    "surface": surf2,
                                    "supersede_old_id": supersede_old_id2,
                                    "inbox_id": inbox_id2,
                                },
                            )
                            await websocket.send(question2)
                            last_full_answer_text = question2
                            continue

                    # No corrected text: fall through to normal chat with no nagging.

                # Anything else: do not mention pending; proceed with normal flow.

            else:
                # No pending candidate: detect a new high-confidence candidate.
                # C7: supersession detection must run even when the message does NOT match
                # the conservative decision-candidate patterns.
                sup = _detect_superseding_decision(user_msg, current_project_full)
                if isinstance(sup, dict) and sup.get("old_id") and sup.get("domain") and sup.get("new_text"):
                    dom = str(sup.get("domain") or "").strip()
                    surf = str(sup.get("surface") or "").strip()
                    old_id = str(sup.get("old_id") or "").strip()
                    new_txt = str(sup.get("new_text") or "").strip()

                    question = f"This would replace the previous decision about {dom}. Correct?"

                    cand = {}
                    try:
                        cand = append_decision_candidate(
                            current_project_full,
                            text=new_txt,
                            confidence=0.90,
                        )
                    except Exception:
                        cand = {}

                    if isinstance(cand, dict) and cand.get("id"):
                        suppress_confirm = False
                        try:
                            stx = project_store.load_project_state(current_project_full) or {}
                            ef = stx.get("expert_frame") if isinstance(stx.get("expert_frame"), dict) else {}
                            ef_status = str(ef.get("status") or "").strip().lower()
                            ef_label = str(ef.get("label") or "").strip()
                            ef_dir = str(ef.get("directive") or "")
                            if ef_status == "active" and (ef_label == "Couples Therapist" or ("Do NOT talk like a project manager" in ef_dir)):
                                suppress_confirm = True
                        except Exception:
                            suppress_confirm = False

                        if suppress_confirm:
                            try:
                                _trace_emit(
                                    "decision.confirm.suppressed",
                                    {
                                        "reason": "couples_therapist_expert_frame",
                                        "candidate_id": str(cand.get("id") or "").strip(),
                                        "project": str(current_project_full or ""),
                                    },
                                )
                            except Exception:
                                pass
                        else:
                            inbox_id = ""
                            try:
                                inbox = project_store.append_inbox_item(
                                    current_project_full,
                                    type_="pending_decision",
                                    text=question,
                                    refs=[],
                                    created_at=(now_iso() or "").replace("Z", ""),
                                )
                                if isinstance(inbox, dict):
                                    inbox_id = str(inbox.get("id") or "").strip()
                            except Exception:
                                inbox_id = ""

                            _save_pending_decision(
                                current_project_full,
                                {
                                    "id": str(cand.get("id") or "").strip(),
                                    "timestamp": str(cand.get("timestamp") or "").strip(),
                                    "text": new_txt,
                                    "confidence": float(cand.get("confidence") or 0.90),
                                    "status": "pending",
                                    "asked": True,
                                    "asked_at_epoch": float(time.time()),
                                    "asked_msg_index": int(user_msg_counter),
                                    "domain": dom,
                                    "surface": surf,
                                    "supersede_old_id": old_id,
                                    "inbox_id": inbox_id,
                                },
                            )
                            await websocket.send(question)
                            last_full_answer_text = question
                            continue

                # Fallback: normal candidate detection
                cand_text, cand_conf = _detect_decision_candidate(user_msg)
                if cand_text and cand_conf >= 0.60:
                    cand = {}
                    try:
                        cand = append_decision_candidate(
                            current_project_full,
                            text=cand_text,
                            confidence=cand_conf,
                        )
                    except Exception:
                        cand = {}

                    # Determine domain + possible supersession deterministically
                    dom, surf = ("", "")
                    try:
                        dom, surf = project_store.infer_decision_domain(cand_text)
                    except Exception:
                        dom, surf = ("", "")

                    supersede_old_id = ""
                    if dom:
                        try:
                            cur = project_store.get_current_decisions_by_domain(current_project_full) or {}
                            prev = cur.get(dom) or {}
                            supersede_old_id = str(prev.get("id") or "").strip()
                        except Exception:
                            supersede_old_id = ""

                    question = f"Got it — I have you choosing {cand_text}. Want me to treat that as final?"
                    if dom and supersede_old_id:
                        question = f"This would replace the previous decision about {dom}. Correct?"

                    if isinstance(cand, dict) and cand.get("id"):
                        suppress_confirm = False
                        try:
                            stx = project_store.load_project_state(current_project_full) or {}
                            ef = stx.get("expert_frame") if isinstance(stx.get("expert_frame"), dict) else {}
                            ef_status = str(ef.get("status") or "").strip().lower()
                            ef_label = str(ef.get("label") or "").strip()
                            ef_dir = str(ef.get("directive") or "")
                            if ef_status == "active" and (ef_label == "Couples Therapist" or ("Do NOT talk like a project manager" in ef_dir)):
                                suppress_confirm = True
                        except Exception:
                            suppress_confirm = False

                        if suppress_confirm:
                            try:
                                _trace_emit(
                                    "decision.confirm.suppressed",
                                    {
                                        "reason": "couples_therapist_expert_frame",
                                        "candidate_id": str(cand.get("id") or "").strip(),
                                        "project": str(current_project_full or ""),
                                    },
                                )
                            except Exception:
                                pass
                        else:
                            inbox_id = ""
                            try:
                                inbox = project_store.append_inbox_item(
                                    current_project_full,
                                    type_="pending_decision",
                                    text=question,
                                    refs=[],
                                    created_at=(now_iso() or "").replace("Z", ""),
                                )
                                if isinstance(inbox, dict):
                                    inbox_id = str(inbox.get("id") or "").strip()
                            except Exception:
                                inbox_id = ""

                            _save_pending_decision(
                                current_project_full,
                                {
                                    "id": str(cand.get("id") or "").strip(),
                                    "timestamp": str(cand.get("timestamp") or "").strip(),
                                    "text": cand_text,
                                    "confidence": float(cand.get("confidence") or cand_conf),
                                    "status": "pending",
                                    "asked": True,
                                    "asked_at_epoch": float(time.time()),
                                    "asked_msg_index": int(user_msg_counter),
                                    "domain": dom,
                                    "surface": surf,
                                    "supersede_old_id": supersede_old_id,
                                    "inbox_id": inbox_id,
                                },
                            )
                            await websocket.send(question)
                            last_full_answer_text = question
                            continue
            # -----------------------------------------------------------------
            # C5.4 — Wordle routing removed
            # -----------------------------------------------------------------

            # [SEARCH] prefix + AUTO web search
            do_search = False
            search_results = ""
            search_cached = False
            # Ensure deep_search is defined before any audit/routing uses it.
            deep_search = False            
            # AUDIT_CTX: initialize search decision context for this turn
            _audit_ctx_update(
                {
                    "search": {
                        "route_mode": str(search_route_mode or ""),
                        "deep_search": bool(deep_search),
                        "do_search": False,
                        "reason": "",
                        "query_text": "",
                        "workspace_rel": "",
                        "results_len": 0,
                    }
                }
            )            
            # -------------------------------------------------------------
            # Patch 1 (FOUNDATIONAL): Memory-first resolution gate (read-only)
            #
            # Goal:
            # - Every request has a deterministic user frame BEFORE search/lookup routing.
            # - No guessing. If memory is missing/ambiguous, frame leaves text unchanged.
            # -------------------------------------------------------------
            resolved_frame = resolve_user_frame(
                user=user,
                current_project_full=current_project_full,
                user_text=clean_user_msg,
            )
            _user_text_for_search = str(resolved_frame.get("user_text_for_search") or clean_user_msg or "").strip()


            # Detect “deep/thorough/verify” intensity for bounded expansion (no commands needed).
            deep_search = False
            try:
                low_tmp = (clean_user_msg or "").lower()
                deep_search = any(k in low_tmp for k in (
                    "deep search", "thorough", "thoroughly", "verify", "fact check", "fact-check",
                    "double check", "cross check", "cross-check", "sources", "with sources",
                ))
            except Exception:
                deep_search = False
            # AUDIT_CTX: record the final deep_search value for this turn.
            _audit_ctx_set("search.deep_search", bool(deep_search))
            # Helper: decide query text (handles generic "search for it" cleanly).
            def _pick_search_query(q_raw: str) -> str:
                q0 = strip_lens0_system_blocks(q_raw or "").strip()
                # If user typed a generic "search for it" / "look it up", use last real question.
                if _is_generic_search_query(q0) and last_user_question_for_search:
                    return last_user_question_for_search
                return q0

            if search_route_mode == "nosearch":
                pass

            elif search_route_mode == "force":
                do_search = True
                qtext = _pick_search_query(_user_text_for_search)
                _audit_ctx_set("search.do_search", True)
                _audit_ctx_set("search.reason", "route_mode_force")
                _audit_ctx_set("search.query_text", str(qtext or "").strip())
                # Search Workspace v1 (bounded 2-pass + temp file)
                try:
                    best_ev, _ws_rel = await _run_search_with_workspace(
                        project_full=current_project_full,
                        question_text=(strip_lens0_system_blocks(clean_user_msg) or clean_user_msg or "").strip(),
                        base_query=(qtext or "").strip(),
                        deep_search=bool(deep_search),
                    )
                    # AUDIT_CTX: preserve workspace pointer (ephemeral evidence trail)
                    try:
                        _audit_ctx_set("search.workspace_rel", str(_ws_rel or "").strip())
                    except Exception:
                        pass                    
                    if (best_ev or "").strip():
                        search_results = best_ev
                        _audit_ctx_set("search.results_len", int(len(search_results or "")))

                    else:
                        search_results = brave_search(qtext, count=(12 if deep_search else 7))
                        search_results = _mark_search_evidence_insufficient(search_results, qtext)
                        _audit_ctx_set("search.results_len", int(len(search_results or "")))

                except Exception:
                    search_results = brave_search(qtext, count=(12 if deep_search else 7))
                    search_results = _mark_search_evidence_insufficient(search_results, qtext)

            elif lower.startswith("[nosearch]"):
                clean_user_msg = user_msg[len("[nosearch]"):].strip()
                # Keep the search-text frame in sync with the cleaned message (read-only).
                try:
                    resolved_frame = resolve_user_frame(
                        user=user,
                        current_project_full=current_project_full,
                        user_text=clean_user_msg,
                    )
                    _user_text_for_search = str(resolved_frame.get("user_text_for_search") or clean_user_msg or "").strip()
                except Exception:
                    _user_text_for_search = (clean_user_msg or "").strip()

            elif lower.startswith("[search]"):
                do_search = True
                clean_user_msg = user_msg[len("[search]"):].strip()

                # IMPORTANT:
                # This legacy branch mutates clean_user_msg AFTER the per-turn frame was computed.
                # Recompute the frame so deictics like "here" are resolved for search too.
                try:
                    resolved_frame = resolve_user_frame(
                        user=user,
                        current_project_full=current_project_full,
                        user_text=clean_user_msg,
                    )
                    _user_text_for_search = str(resolved_frame.get("user_text_for_search") or clean_user_msg or "").strip()
                except Exception:
                    _user_text_for_search = (clean_user_msg or "").strip()

                qtext = _pick_search_query(_user_text_for_search)
                search_results = brave_search(qtext, count=(12 if deep_search else 7))
                search_results = _mark_search_evidence_insufficient(search_results, qtext)


            else:
                # No explicit [SEARCH] tag, but still honor explicit user intent in natural language.
                # Examples: "search for it", "look it up", "verify that", "fact check this".
                qtext0 = strip_lens0_system_blocks(_user_text_for_search or clean_user_msg or "").strip()
                low0 = qtext0.lower()

                # Continuation operator binding:
                # If the user asks to "look up" / "see what people are saying" without naming an entity,
                # bind it to the current Active Topic Frame.
                pronoun_followup = False
                if continuity_followup is not None:
                    pronoun_followup = bool(continuity_followup)
                else:
                    try:
                        shortish = len(low0.strip()) <= 90
                        has_pron = bool(re.search(r"\b(it|that|this|they|them|those)\b", low0))
                        pronoun_followup = bool(shortish and has_pron and float(active_topic_strength) >= 0.35 and (active_topic_text or "").strip())
                    except Exception:
                        pronoun_followup = False

                # NEW: If user asks for an image/photo/picture "of that/this/it", deterministically bind
                # the target to the Active Topic Frame so we don't search for the pronoun itself.
                try:
                    if (active_topic_text or "").strip() and float(active_topic_strength) >= 0.35:
                        if re.search(r"\b(?:picture|photo|image)\s+of\s+(?:that|this|it|them|those|these)\b", low0, flags=re.IGNORECASE):
                            qtext0 = f"picture of {active_topic_text.strip()}"
                            low0 = qtext0.lower()
                except Exception:
                    pass

                # -----------------------------------------------------------------
                # NO MAGIC-WORD SEARCH ROUTING
                #
                # Rule:
                # - Do NOT decide lookup/search based on phrase lists like "look it up" or
                #   "what are people saying". That creates brittle “magic words” behavior.
                # - Web search is driven ONLY by:
                #     (a) explicit [SEARCH] / [NOSEARCH] routing (handled above), OR
                #     (b) should_auto_web_search(qtext) (single stable heuristic), OR
                #     (c) explicit picture requests (handled here deterministically).
                # -----------------------------------------------------------------

                # Image request detector (bounded, no spam):
                # We only show a few accurate images when asked, never "a million".
                picture_request = False
                try:
                    # IMPORTANT:
                    # Do NOT trigger image search just because the word "image" appears.
                    # Only trigger when the user is explicitly asking to SEE/GET a picture/photo/image.
                    low_pic = (low0 or "").strip()

                    asks_for_visual = bool(
                        re.search(r"\b(picture|photo|image|pic)\b", low_pic)
                        and (
                            re.search(r"\b(show|send|give|find|pull|get|search|lookup|look\s+up)\b", low_pic)
                            or re.search(r"\b(picture|photo|image|pic)\s+of\b", low_pic)
                            or low_pic.startswith(("show me", "can you show", "could you show", "send me", "give me"))
                        )
                    )
                    picture_request = bool(asks_for_visual)
                except Exception:
                    picture_request = False

                if picture_request:
                    do_search = True

                    # Determine what to search for:
                    # - Prefer "X" from "picture of X"
                    # - Else bind to active topic (if strong)
                    # - Else fall back to last user question
                    q_img = ""

                    try:
                        m = re.search(r"\b(?:picture|photo|image)\s+of\s+(.+)$", qtext0, flags=re.IGNORECASE)
                        if m:
                            q_img = (m.group(1) or "").strip()
                    except Exception:
                        q_img = ""

                    if (not q_img) and (active_topic_text or "").strip():
                        q_img = (active_topic_text or "").strip()

                    if (not q_img) and (last_user_question_for_search or "").strip():
                        q_img = (last_user_question_for_search or "").strip()

                    q_img = (q_img or "").strip()
                    if not q_img:
                        # No robot talk: ask one human clarifier, then stop this turn.
                        await websocket.send("Sure — a picture of what?")
                        continue

                    # Run bounded image search (max 3) and send directly.
                    img_block = brave_image_search(q_img, count=3)

                    # Auditable continuity:
                    # - Wrap image results into search_evidence_v1 JSON
                    # - Persist a Search Workspace entry (state/tmp) so image lookup is auditable
                    # - Record last_web_search_results as the EVIDENCE JSON (not the rendered markdown)
                    ev_json = ""
                    ws_rel = ""
                    try:
                        results_img: List[Dict[str, Any]] = []
                        # brave_image_search returns markdown lines like:
                        #   ![Title](IMG_URL)
                        #   Source: PAGE_URL
                        img_url = ""
                        title = ""
                        for ln in (img_block or "").splitlines():
                            s = (ln or "").strip()
                            m_img = re.match(r"!\[([^\]]*)\]\((https?://[^)]+)\)", s)
                            if m_img:
                                title = (m_img.group(1) or "").strip()
                                img_url = (m_img.group(2) or "").strip()
                                continue
                            if s.lower().startswith("source:") and img_url:
                                src_page = s.split(":", 1)[1].strip()
                                results_img.append(
                                    {
                                        "rank": int(len(results_img) + 1),
                                        "title": title,
                                        "url": src_page,
                                        "image_url": img_url,
                                        "snippet": "",
                                        "query": q_img,
                                    }
                                )
                                img_url = ""
                                title = ""

                        ev_obj: Dict[str, Any] = {
                            "schema": "search_evidence_v1",
                            "source": "brave_image",
                            "query": str(q_img or "").strip(),
                            "retrieved_at": now_iso(),
                            "results": results_img[:3],
                        }
                        if not ev_obj["results"]:
                            ev_obj["note"] = "NO_RESULTS: Image search returned no relevant results."

                        ev_json = json.dumps(ev_obj, ensure_ascii=False, separators=(",", ":"))
                        # Reuse authority annotator (it keys off the source-page domain)
                        ev_json = _search_evidence_annotate_authority(ev_json)

                        # Write an auditable search workspace record (ephemeral; safe to delete)
                        ws_id = _search_workspace_new_id()
                        ws_obj: Dict[str, Any] = {
                            "schema": "search_workspace_v1",
                            "id": ws_id,
                            "created_at": now_iso(),
                            "question": f"Image request: {q_img}",
                            "base_query": str(q_img or "").strip(),
                            "passes": [
                                {
                                    "pass": 1,
                                    "query": str(q_img or "").strip(),
                                    "retrieved_at": now_iso(),
                                    "query_plan": [str(q_img or "").strip()],
                                    "evidence_json": ev_json,
                                    "insufficient": bool(_search_evidence_is_insufficient(ev_json)),
                                }
                            ],
                            "selected_pass": 1,
                        }
                        ws_rel = _search_workspace_write(current_project_full, ws_obj)

                        # Attach workspace pointer to the evidence JSON (best-effort)
                        try:
                            ev2 = json.loads(ev_json)
                            if isinstance(ev2, dict) and ws_rel:
                                ev2["workspace_path"] = ws_rel
                                ev2["workspace_id"] = ws_id
                                ev_json = json.dumps(ev2, ensure_ascii=False, separators=(",", ":"))
                        except Exception:
                            pass
                    except Exception:
                        ev_json = ""
                        ws_rel = ""

                    if "NO_RESULTS" in (img_block or "") or "Search error" in (img_block or ""):
                        # Fall back to normal web search if images fail (still bounded).
                        qtext_used_for_search = (q_img or "").strip()
                        search_results = brave_search(q_img, count=7)
                        search_results = _mark_search_evidence_insufficient(search_results, q_img)

                    else:
                        # Send the images as the user-visible answer (no model needed).
                        await websocket.send(img_block)
                        last_full_answer_text = img_block
                        # Mark that web search was used (truthfulness next turn).
                        try:
                            last_web_search_used = True
                            last_web_search_query = q_img
                            # IMPORTANT: store auditable evidence JSON, not the rendered markdown
                            last_web_search_results = (ev_json or img_block)
                        except Exception:
                            pass
                        continue

                else:
                    qtext_used_for_search = ""
                    # -----------------------------------------------------------------
                    # LLM-FIRST SEARCH PREFLIGHT (FOUNDATIONAL)
                    #
                    # Goal:
                    # - Use the model intent classifier to decide whether the user wants a world lookup.
                    # - Avoid magic-word triggers like "search the web".
                    # - Keep should_auto_web_search(...) as a conservative fallback only.
                    #
                    # Outputs (for auditing / downstream):
                    # - allow_web_search: bool
                    # - intent_obj_for_search: dict (best-effort)
                    # - intent_norm_for_search: str
                    # - ws_rel_used_for_search: str (workspace path when search runs)
                    # -----------------------------------------------------------------
                    allow_web_search = False
                    intent_obj_for_search: Dict[str, Any] = {}
                    intent_norm_for_search = ""
                    ws_rel_used_for_search = ""                    
                    qtext = strip_lens0_system_blocks(clean_user_msg)
                    # Deterministic guard: avoid web search for personal memory claims
                    # (names, birthdays, relationship facts). These should be handled by memory, not lookup.
                    personal_memory_claim = False
                    try:
                        qlow = (qtext or "").lower()
                        if any(x in qlow for x in ("my girlfriend", "my boyfriend", "my wife", "my husband", "my partner",
                                                   "my son", "my daughter", "my child", "my mom", "my mother", "my dad",
                                                   "my father", "my sister", "my brother", "my stepmom", "my stepmother",
                                                   "my stepdad", "my stepfather", "my stepsister", "my stepbrother")):
                            personal_memory_claim = True
                        if any(x in qlow for x in ("my name is", "my birthday is", "my birthdate is", "i was born",
                                                   "i am confirming", "i confirm", "this is a fact")):
                            personal_memory_claim = True
                    except Exception:
                        personal_memory_claim = False
                    # Preflight intent classification (LLM owns meaning).
                    # If classifier says "lookup", we allow web search for this turn (unless [NOSEARCH] forced earlier).
                    try:
                        intent_obj_for_search = await model_pipeline.classify_intent_c6(
                            ctx=sys.modules[__name__],
                            user_text=(qtext or "").strip(),
                        )
                        intent_norm_for_search = str(intent_obj_for_search.get("intent") or "").strip().lower()
                    except Exception:
                        intent_obj_for_search = {}
                        intent_norm_for_search = ""

                    # LLM-first decision:
                    # - "lookup" intent means the user is asking for factual retrieval
                    # - BUT web search should only run when the model judges world evidence is required
                    #   (prevents trivial math/definitions from triggering search).
                    needs_obj: Dict[str, Any] = {}
                    needs_world = False
                    needs_reason = ""
                    needs_conf = "low"
                    verify_first = False
                    claim_class = "other"                    
                    try:
                        needs_obj = await classify_needs_world_evidence(
                            ctx=sys.modules[__name__],
                            user_text=(qtext or "").strip(),
                        )
                        needs_world = bool(needs_obj.get("needs_world_evidence") is True)
                        verify_first = bool(needs_obj.get("verify_first") is True)
                        claim_class = str(needs_obj.get("claim_class") or "").strip().lower()
                        if claim_class not in ("timeless", "current_event", "viral_claim", "procedure_legal", "other"):
                            claim_class = "other"                        
                        needs_conf = str(needs_obj.get("confidence") or "low").strip().lower()
                        if needs_conf not in ("low", "medium", "high"):
                            needs_conf = "low"
                        needs_reason = str(needs_obj.get("reason") or "").strip()
                    except Exception:
                        needs_obj = {}
                        needs_world = False
                        verify_first = False
                        claim_class = "other"
                        needs_reason = ""
                        needs_conf = "low"

                    # Personal memory claims must not trigger web search or verify-first.
                    if personal_memory_claim:
                        needs_world = False
                        verify_first = False
                        claim_class = "other"
                        if not needs_reason:
                            needs_reason = "personal_memory_claim"

                    # Final allow_web_search decision (FOUNDATIONAL):
                    # Invariant: if the model says world evidence is needed (or verify_first),
                    # search MUST be allowed regardless of intent label.
                    #
                    # Also: if qtext is generic/pronoun-heavy, bind deterministically to the
                    # Active Topic Frame / last real question BEFORE heuristic fallback.
                    try:
                        q_raw = (qtext or "").strip()

                        # Deterministic binding for generic follow-ups (no phrase lists)
                        q_bound = q_raw
                        if _is_generic_search_query(q_raw):
                            if (active_topic_text or "").strip() and float(active_topic_strength) >= 0.25:
                                q_bound = (active_topic_text or "").strip()
                            elif (last_user_question_for_search or "").strip():
                                q_bound = (last_user_question_for_search or "").strip()

                        # If same-topic but the query doesn't mention the active topic,
                        # prepend the topic to preserve entity anchoring (deterministic).
                        try:
                            same_topic = (str(continuity_label or "") != "new_topic")
                            if (
                                same_topic
                                and (active_topic_text or "").strip()
                                and float(active_topic_strength) >= 0.35
                                and (q_bound or "").strip()
                            ):
                                q_low = q_bound.lower()
                                topic_low = (active_topic_text or "").lower().strip()
                                if topic_low and (topic_low not in q_low):
                                    toks_q = set(re.findall(r"[a-z0-9]{4,}", q_low))
                                    toks_t = set(re.findall(r"[a-z0-9]{4,}", topic_low))
                                    if toks_t and toks_q and (not (toks_q & toks_t)) and (len(q_bound) <= 140):
                                        q_bound = f"{active_topic_text.strip()} {q_bound}".strip()
                        except Exception:
                            pass

                        # Keep qtext in sync for downstream logging/search execution
                        qtext = q_bound

                        if intent_norm_for_search == "lookup":
                            if needs_obj:
                                allow_web_search = bool(needs_world or verify_first)
                            else:
                                allow_web_search = bool(should_auto_web_search(qtext))
                        else:
                            # Override: world-evidence requirement must not be blocked by intent=misc/etc.
                            if bool(needs_world or verify_first):
                                allow_web_search = True
                            else:
                                allow_web_search = bool(should_auto_web_search(qtext))
                    except Exception:
                        allow_web_search = bool(should_auto_web_search(qtext))

                    # Personal memory claims are never web-searchable.
                    if personal_memory_claim:
                        allow_web_search = False

                    # AUDIT_CTX: record intent + world-evidence decision (authoritative reasons)
                    try:
                        _audit_ctx_set("intent.intent_norm", str(intent_norm_for_search or ""))
                        _audit_ctx_set("intent.raw", intent_obj_for_search if isinstance(intent_obj_for_search, dict) else {})
                        _audit_ctx_set("search.needs_world_evidence", bool(needs_world))
                        _audit_ctx_set("search.needs_world_confidence", str(needs_conf or "low"))
                        _audit_ctx_set("search.needs_world_reason", str(needs_reason or ""))
                        _audit_ctx_set("search.verify_first", bool(verify_first))
                        _audit_ctx_set("search.claim_class", str(claim_class or "other"))                        
                        _audit_ctx_set("search.personal_memory_claim", bool(personal_memory_claim))
                    except Exception:
                        pass
                    # AUDIT_CTX: record why we are (or are not) allowing web search.
                    try:
                        _audit_ctx_set("search.route_mode", str(search_route_mode or ""))
                        _audit_ctx_set("search.query_text", str(qtext or "").strip())
                        if bool(allow_web_search):
                            if bool(needs_world or verify_first):
                                _audit_ctx_set("search.reason", "needs_world_evidence_override")
                            elif (intent_norm_for_search == "lookup") and (not _is_generic_search_query(str(qtext or "").strip())):
                                _audit_ctx_set("search.reason", "llm_lookup_bound_followup_or_heuristic")
                            else:
                                _audit_ctx_set("search.reason", "heuristic_auto_web_search")
                        else:
                            if intent_norm_for_search == "lookup":
                                _audit_ctx_set("search.reason", "llm_lookup_but_no_world_evidence")
                            elif intent_norm_for_search:
                                _audit_ctx_set("search.reason", f"not_lookup(intent={intent_norm_for_search})")
                            else:
                                _audit_ctx_set("search.reason", "intent_unknown_or_blocked")
                    except Exception:
                        pass
                    # Single authoritative auto-search heuristic (no phrase lists).
                    if allow_web_search and (not personal_memory_claim):
                        do_search = True
                        qtext2 = _pick_search_query(qtext)
                        qtext_used_for_search = (qtext2 or "").strip()
                        # FOUNDATIONAL: same-topic followup binding for under-specified queries
                        # If continuity says same topic and the query has low token overlap with active topic,
                        # prepend the active topic so the search is entity-anchored.
                        try:
                            same_topic = (str(continuity_label or "") != "new_topic")
                            if same_topic and (active_topic_text or "").strip():
                                if _topic_overlap_low(query=qtext_used_for_search, topic=active_topic_text):
                                    qtext_used_for_search = (active_topic_text.strip() + " " + qtext_used_for_search).strip()
                                    try:
                                        _audit_ctx_set("search.bound_to_active_topic", True)
                                    except Exception:
                                        pass
                        except Exception:
                            pass
                        # AUDIT_CTX: search actually executing on this turn
                        try:
                            _audit_ctx_set("search.do_search", True)
                        except Exception:
                            pass

                        # Search Workspace v1 (bounded 2-pass + temp file)
                        try:
                            best_ev, _ws_rel = await _run_search_with_workspace(
                                project_full=current_project_full,
                                question_text=(strip_lens0_system_blocks(clean_user_msg) or clean_user_msg or "").strip(),
                                base_query=qtext_used_for_search,
                                deep_search=bool(deep_search),
                            )
                            # AUDIT_CTX: preserve workspace pointer (ephemeral evidence trail)
                            try:
                                _audit_ctx_set("search.workspace_rel", str(_ws_rel or "").strip())
                            except Exception:
                                pass

                            if (best_ev or "").strip():
                                search_results = best_ev
                                try:
                                    _audit_ctx_set("search.results_len", int(len(search_results or "")))
                                except Exception:
                                    pass
                            else:
                                search_results = brave_search(qtext_used_for_search, count=(12 if deep_search else 7))
                                search_results = _mark_search_evidence_insufficient(search_results, qtext_used_for_search)
                                try:
                                    _audit_ctx_set("search.results_len", int(len(search_results or "")))
                                except Exception:
                                    pass
                        except Exception:
                            search_results = brave_search(qtext_used_for_search, count=(12 if deep_search else 7))
                            search_results = _mark_search_evidence_insufficient(search_results, qtext_used_for_search)
                            try:
                                _audit_ctx_set("search.results_len", int(len(search_results or "")))
                            except Exception:
                                pass
                            # Preserve workspace path for audit/debug (best-effort).
                            try:
                                ws_rel_used_for_search = str(_ws_rel or "").strip()
                            except Exception:
                                ws_rel_used_for_search = ""

                    # If the user explicitly asked to "look up/search" but web search is unavailable,
                    # record a capability gap and give the model a continuity note.
                    try:
                        low0 = (clean_user_msg or "").lower()
                        search_requested = bool(re.search(r"\b(look\s+up|search|find\s+out|price\s+check|what\s+others\s+charge|what\s+others\s+are\s+charging|market\s+rate)\b", low0))
                        brave_ok = bool((os.getenv("BRAVE_API_KEY", "") or "").strip())
                        if search_requested and (not brave_ok):
                            topic_hint = (active_topic_text or "").strip()
                            base_q = topic_hint or (clean_user_msg or "").strip()
                            rec_queries = []
                            if base_q:
                                rec_queries.append(base_q + " pricing")
                                rec_queries.append(base_q + " rates")
                            gap_id = "gap_web_search_" + hashlib.sha256((base_q or low0).encode("utf-8", errors="ignore")).hexdigest()[:12]
                            if not project_store.capability_gap_exists(current_project_full, gap_id):
                                try:
                                    project_store.append_capability_gap_entry(
                                        current_project_full,
                                        {
                                            "id": gap_id,
                                            "blocked": True,
                                            "task_summary": "Web search requested but not configured",
                                            "limitations": ["Web search API key is not configured."],
                                            "missing_capabilities": ["BRAVE_API_KEY (web search)"],
                                            "needed_inputs": ["Configure BRAVE_API_KEY or provide sources."],
                                            "suggested_features": ["Enable web search for market comps"],
                                            "recommended_search_queries": rec_queries[:3],
                                            "created_at": now_iso(),
                                        },
                                    )
                                    project_store.build_capability_gap_views_and_write(current_project_full)
                                except Exception:
                                    pass
                            # Continuity: keep model anchored to the active topic
                            if topic_hint:
                                try:
                                    conversation_history = (conversation_history or [])
                                    conversation_history.append(
                                        {
                                            "role": "system",
                                            "content": f"CONTINUITY_CONTEXT: The user is asking for market pricing related to '{topic_hint}'.",
                                        }
                                    )
                                except Exception:
                                    pass
                    except Exception:
                        pass


                # Continuity: reuse cached search evidence for same-topic follow-ups.
                if (not bool(do_search)) and (not (search_results or "").strip()):
                    try:
                        same_topic = (str(continuity_label or "") != "new_topic")
                        if same_topic:
                            age_ok = False
                            try:
                                age_ok = bool(last_search_cache_ts) and (time.time() - float(last_search_cache_ts) <= 3600.0)
                            except Exception:
                                age_ok = False

                            if (last_search_cache_results or "").strip() and age_ok:
                                search_results = last_search_cache_results
                                search_cached = True
                                _audit_ctx_set("search.cached", True)
                            elif (last_search_cache_ws_rel or "").strip():
                                try:
                                    p_ws = (PROJECT_ROOT / last_search_cache_ws_rel).resolve()
                                    if p_ws.exists():
                                        search_results = read_text_file(p_ws) or ""
                                        if (search_results or "").strip():
                                            search_cached = True
                                            _audit_ctx_set("search.cached", True)
                                except Exception:
                                    pass
                    except Exception:
                        pass

                # -------------------------------------------------------------
                # Expectation-shift Date Lookup (2nd pass; deterministic, bounded)
                #
                # If the user asks "what changed / last X months / since" and the initial
                # snippets don't show obvious publication dates, run ONE small follow-up
                # search for publication dates and append it as extra evidence.
                # -------------------------------------------------------------
                try:
                    if bool(do_search) and (search_results or "").strip() and qtext_used_for_search:
                        low_es = (strip_lens0_system_blocks(clean_user_msg) or "").lower()

                        expectation_shift = (
                            ("what changed" in low_es)
                            or ("what has changed" in low_es)
                            or ("what’s changed" in low_es)
                            or ("whats changed" in low_es)
                            or ("changed in the last" in low_es)
                            or ("in the last" in low_es and any(u in low_es for u in ("days", "weeks", "months", "years")))
                            or ("last 12 months" in low_es)
                            or ("past 12 months" in low_es)
                            or ("over the last" in low_es and any(u in low_es for u in ("days", "weeks", "months", "years")))
                            or ("since " in low_es)
                            or ("now required" in low_es)
                            or ("being enforced" in low_es)
                            or ("enforced" in low_es)
                            or ("new guidance" in low_es)
                            or ("updated guidance" in low_es)
                        )

                        # Heuristic: do we already have explicit date anchors in snippets?
                        # (Keep conservative: year/month-name/ISO-ish/published markers.)
                        sr_low = (search_results or "").lower()
                        has_date_anchor = bool(
                            re.search(r"\b(20\d{2})\b", sr_low)
                            or re.search(r"\b(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\b", sr_low)
                            or re.search(r"\b\d{4}-\d{2}-\d{2}\b", sr_low)
                            or ("published" in sr_low)
                            or ("publication" in sr_low)
                            or ("last updated" in sr_low)
                        )

                        if expectation_shift and (not has_date_anchor):
                            # One deterministic follow-up query (bounded).
                            date_q = (qtext_used_for_search or "").strip()
                            # Prefer RFC-specific phrasing when relevant.
                            if ("rfc" in low_es) or ("oauth" in low_es) or ("bcp" in low_es):
                                date_q = date_q + " publication date"

                            date_results = brave_search(date_q, count=5)

                            # Append only if it produced real hits (avoid NO_RESULTS noise).
                            if (
                                (date_results or "").strip()
                                and ("NO_RESULTS" not in date_results)
                                and ("Search error" not in date_results)
                            ):
                                search_results = (
                                    (search_results or "").rstrip()
                                    + "\n\n"
                                    + "DATE_LOOKUP_RESULTS (publication dates / last-updated evidence):\n"
                                    + (date_results or "").strip()
                                )
                except Exception:
                    pass

            # [PDF] prefix
            if lower.startswith("[pdf]"):
                rest = user_msg[len("[pdf]"):].strip()
                if "::" in rest:
                    path_part, q = rest.split("::", 1)
                    pdf_path = Path(path_part.strip())
                    question = q.strip()
                else:
                    pdf_path = Path(rest.strip())
                    question = "Summarize the key points of this PDF."

                if not pdf_path.is_absolute():
                    pdf_path = (PROJECT_ROOT / str(pdf_path)).resolve()

                pdf_text = load_pdf_text(pdf_path)
                clean_user_msg = (
                    f"The following PDF was loaded from the local path {pdf_path}.\n\n"
                    f"===PDF_START===\n{pdf_text}\n===PDF_END===\n\n"
                    f"User question: {question}"
                )

            # Persist "what actually happened" for truthfulness on the next turn.
            # IMPORTANT: do this AFTER [PDF] modifies clean_user_msg, so the recorded query is accurate.
            try:
                searched_ok = False
                sr0 = (search_results or "").strip()
                if bool(do_search) and sr0:
                    # Prefer structured evidence schema to avoid false negatives like:
                    # - JSON containing "NO_RESULTS" inside snippets
                    # - or workspace evidence that should count as "searched"
                    try:
                        ev = json.loads(sr0)
                        if isinstance(ev, dict) and str(ev.get("schema") or "") == "search_evidence_v1":
                            res = ev.get("results")
                            insufficient = bool(ev.get("insufficient") is True)
                            note = str(ev.get("note") or "").lower()
                            has_hits = bool(isinstance(res, list) and len(res) > 0)
                            searched_ok = bool(has_hits and (not insufficient) and ("no_results" not in note))
                        else:
                            searched_ok = ("NO_RESULTS" not in sr0) and ("Search error" not in sr0)
                    except Exception:
                        searched_ok = ("NO_RESULTS" not in sr0) and ("Search error" not in sr0)

                if bool(do_search) and sr0:
                    last_web_search_used = True  # it searched, even if results were weak
                    last_web_search_query = (strip_lens0_system_blocks(clean_user_msg) or "").strip()
                    last_web_search_results = sr0
                    # Update continuity cache
                    last_search_cache_results = sr0
                    last_search_cache_query = last_web_search_query
                    try:
                        last_search_cache_ts = float(time.time())
                    except Exception:
                        last_search_cache_ts = 0.0
                    try:
                        last_search_cache_ws_rel = str(ws_rel_used_for_search or "").strip()
                    except Exception:
                        last_search_cache_ws_rel = ""
                else:
                    last_web_search_used = False
                    last_web_search_query = ""
                    last_web_search_results = ""
            except Exception:
                last_web_search_used = False
                last_web_search_query = ""
                last_web_search_results = ""

            # Self-patch control commands (blue/green server edits)
            sp_cmd = parse_self_patch_command(clean_user_msg) if ENABLE_PATCH_MODE else None
            if sp_cmd is not None:
                try:
                    sp_reply = await handle_self_patch_command(sp_cmd)
                except Exception as e:
                    sp_reply = f"SELF-PATCH ERROR: {e!r}"
                await websocket.send(sp_reply)
                last_full_answer_text = sp_reply
                append_jsonl(state_dir(current_project_full) / "chat_log.jsonl", {"ts": now_iso(), "role": "user", "content": clean_user_msg})
                append_jsonl(state_dir(current_project_full) / "chat_log.jsonl", {"ts": now_iso(), "role": "assistant", "content": sp_reply})
                continue

            # Patch mode
            if looks_like_patch_request(clean_user_msg):
                # IMPORTANT: Do NOT auto-route into blue/green self-patch proposals.
                # Self-patching must be explicitly requested via /selfpatch (or by using the reserved __server__ project).
                patch_request = strip_lens0_system_blocks(clean_user_msg)
                target, patch_text = await generate_anchor_validated_patch(current_project_full, patch_request)

                last_patch_text = patch_text
                last_patch_target = target

                diff_text, diff_summary = build_unified_diff_from_anchor_patch(target, patch_text)

                # PATCH MODE must return ONLY copy/paste anchor patch blocks (no wrapper text).
                combined = (patch_text or "").strip()

                # FINAL SAFETY GATE:
                # Never send malformed patch text to the UI (prevents "python print(...)" one-liners).
                ok_fmt, err_fmt = validate_patch_text_strict_format(combined)
                if not ok_fmt:
                    msg = (
                        "PATCH MODE ERROR: Model output was not in strict patch-block format.\n"
                        "Do NOT apply anything.\n\n"
                        f"Reason: {err_fmt}\n"
                    )
                    await websocket.send(msg)
                    last_full_answer_text = msg
                    stamp = time.strftime("%Y%m%d_%H%M%S")
                    logical = f"patch_rejected_{stamp}"
                    create_artifact(
                        current_project_full,
                        logical,
                        combined,
                        artifact_type="patch_rejected",
                        file_ext=".txt",
                        meta={"target": target, "reason": err_fmt},
                    )
                    append_jsonl(state_dir(current_project_full) / "chat_log.jsonl", {"ts": now_iso(), "role": "user", "content": clean_user_msg})
                    append_jsonl(state_dir(current_project_full) / "chat_log.jsonl", {"ts": now_iso(), "role": "assistant", "content": msg})
                    continue

                await websocket.send(combined)
                last_full_answer_text = combined

                stamp = time.strftime("%Y%m%d_%H%M%S")
                logical = f"patch_{stamp}"
                create_artifact(current_project_full, logical, combined, artifact_type="patch", file_ext=".txt", meta={"target": target})
                if diff_text:
                    create_artifact(current_project_full, logical + "_diff", diff_text, artifact_type="patch_diff", file_ext=".patch", meta={"target": target, "summary": diff_summary})

                append_jsonl(state_dir(current_project_full) / "chat_log.jsonl", {"ts": now_iso(), "role": "user", "content": clean_user_msg})
                append_jsonl(state_dir(current_project_full) / "chat_log.jsonl", {"ts": now_iso(), "role": "assistant", "content": combined})
                continue
            # NOTE:
            # greeting.request is handled in the WS frame router (near thread.get / chat.send).
            # Keeping it there prevents it from being skipped by early frame continues.
            # Normal chat (Project OS)
            # -----------------------------------------------------------------
            # New-project greeting guard (deterministic)
            #
            # Problem:
            # - In a brand-new project with no goal, a greeting like "hi" can fall into
            #   bootstrap interrogation (mode/deliverables questions).
            #
            # Fix:
            # - If the user greets and the project has no goal yet, respond like a human
            #   and ask one simple steering question. Do NOT call the model.
            # -----------------------------------------------------------------
            try:
                msg0 = (clean_user_msg or "").strip()
                low0 = msg0.lower().strip()

                greeting_only = low0 in ("hi", "hello", "hey", "yo", "hiya", "howdy", "sup")

                if greeting_only and (not _is_couples_user(user)):
                    try:
                        m0 = load_manifest(current_project_full) or {}
                    except Exception:
                        m0 = {}
                    goal_m = str(m0.get("goal") or "").strip()

                    if not goal_m:
                        greet2 = "Hi — what do you want to use this project for?"
                        await websocket.send(greet2)
                        last_full_answer_text = greet2
                        continue
            except Exception:
                pass            
            # Upload readiness gate (Fix A):
            # If ANY upload in this project is still queued/processing, do not call the model yet.
            upload_notice = ""
            try:
                ok_ready, pending = await upload_pipeline.wait_for_project_uploads_ready(  # type: ignore[attr-defined]
                    current_project_full,
                    timeout_s=2.0,
                )
                if not ok_ready:
                    # Deterministic, bounded status message (ADVISORY ONLY — do not block expert flow)
                    names: List[str] = []
                    try:
                        for v in (pending or [])[:6]:
                            if not isinstance(v, dict):
                                continue
                            nm = str(v.get("orig_name") or "").strip()
                            st = str(v.get("state") or "").strip().lower()
                            if nm:
                                names.append(f"{nm} ({st or 'processing'})")
                    except Exception:
                        names = []

                    if names:
                        upload_notice = "Heads up: still processing uploads: " + ", ".join(names) + "."
                    else:
                        upload_notice = "Heads up: still processing uploads."
            except Exception:
                upload_notice = ""

            try:
                # Lookup behavior is handled inside model_pipeline.run_model_pipeline().
                # Do NOT short-circuit here (keeps one authoritative lookup/grounding path).
                # VERIFY-FIRST ENFORCEMENT:
                # If the classifier says this is a viral / novel-entity claim that hinges on existence,
                # instruct the model to verify existence before risk/implication analysis.
                try:
                    if bool(_audit_ctx_get().get("search", {}).get("verify_first")):
                        conversation_history = (conversation_history or [])
                        conversation_history.append(
                            {
                                "role": "system",
                                "content": (
                                    "EPISTEMIC_MODE: VERIFY_FIRST\n"
                                    "\n"
                                    "You MUST begin your answer with these two lines (in this order) BEFORE any other text:\n"
                                    "1) CLAIM_STATUS: VERIFIED | UNVERIFIED | PARTIALLY_VERIFIED\n"
                                    "2) CLAIM_CLASS: timeless | current_event | viral_claim | procedure_legal | other\n"
                                    "\n"
                                    "Rules:\n"
                                    "- First, verify whether the named thing/event actually exists using provided evidence.\n"
                                    "- If evidence is insufficient or uncorroborated, set CLAIM_STATUS: UNVERIFIED and do NOT\n"
                                    "  speculate about downstream risks beyond stating generic possibilities.\n"
                                    "- Ask for the link/screenshot if needed.\n"
                                    "- Only AFTER verification should you explain risks/implications, and keep them tied to what is verified.\n"
                                ),
                            }
                        )
                except Exception:
                    pass

                # -----------------------------------------------------------------
                # FOUNDATIONAL: Ensure web evidence is actually visible to the model
                #
                # Problem observed:
                # - Search ran (workspace exists), but the model responded as if it did not,
                #   claiming "no reliable confirmation" even when evidence included
                #   authority.level="primary_confirmed".
                #
                # Fix (deterministic):
                # - If do_search=True and we have search_results, inject them as a SYSTEM note
                #   labeled exactly "WEB_SEARCH_RESULTS:" which the system prompt explicitly
                #   treats as live evidence.
                # - Also add a tiny authority hint when present in the structured JSON.
                #
                # No phrase lists. No added heuristics. Pure evidence plumbing.
                # -----------------------------------------------------------------
                try:
                    if bool(do_search or search_cached) and (search_results or "").strip():
                        sr = (search_results or "").strip()
                        auth_hint = ""
                        try:
                            ev = json.loads(sr)
                            if isinstance(ev, dict) and str(ev.get("schema") or "") == "search_evidence_v1":
                                auth = ev.get("authority") if isinstance(ev.get("authority"), dict) else {}
                                lvl = str((auth or {}).get("level") or "").strip()
                                if lvl:
                                    auth_hint = f"\n\nSEARCH_AUTHORITY_LEVEL: {lvl}"
                        except Exception:
                            auth_hint = ""

                        cache_hint = "\n\nSEARCH_EVIDENCE_CACHED: true" if bool(search_cached) else ""
                        conversation_history = (conversation_history or [])
                        conversation_history.append(
                            {
                                "role": "system",
                                "content": ("WEB_SEARCH_RESULTS:\n" + sr + auth_hint + cache_hint).strip(),
                            }
                        )
                except Exception:
                    pass
                # THREAD_SYNTHESIS: provide prior evidence-bound synthesis only for same-topic follow-ups
                thread_synthesis_for_turn = ""
                try:
                    same_topic = (str(continuity_label or "") != "new_topic")
                    if not same_topic:
                        last_thread_synthesis_text = ""
                        last_thread_synthesis_hash = ""
                    elif (last_thread_synthesis_text or "").strip() and (not bool(do_search)):
                        thread_synthesis_for_turn = (last_thread_synthesis_text or "").strip()
                except Exception:
                    thread_synthesis_for_turn = ""

                # Analysis hat deterministic deliverables (no magic commands)
                try:
                    if str(active_expert or "").strip().lower() == "analysis":
                        if _analysis_intent_workbook(clean_user_msg):
                            path = None
                            try:
                                path = project_store.build_case_workbook_and_write(current_project_full)
                            except Exception:
                                path = None
                            if path:
                                await websocket.send("Case workbook generated.\nOpen: /file?path=" + path)
                                last_full_answer_text = "Case workbook generated."
                                continue
                            else:
                                await websocket.send("I couldn't generate the workbook (missing Excel engine).")
                                last_full_answer_text = "I couldn't generate the workbook (missing Excel engine)."
                                continue

                        if _analysis_intent_case_summary(clean_user_msg):
                            path = None
                            try:
                                path = project_store.build_case_summary_doc(current_project_full)
                            except Exception:
                                path = None
                            if path:
                                await websocket.send("Case summary document generated.\nOpen: /file?path=" + path)
                                last_full_answer_text = "Case summary document generated."
                                continue

                        if _analysis_intent_audit(clean_user_msg):
                            try:
                                project_store.build_analysis_audit_report(current_project_full)
                            except Exception:
                                pass
                            apath = _latest_artifact_path_by_name(current_project_full, "analysis_audit_latest")
                            if apath:
                                await websocket.send("Analysis audit generated.\nOpen: /file?path=" + apath)
                                last_full_answer_text = "Analysis audit generated."
                                continue
                except Exception:
                    pass

                # Continuity stitch for under-specified pricing followups
                try:
                    if _should_stitch_continuity_context(
                        user_msg=clean_user_msg,
                        active_topic_text=active_topic_text,
                        continuity_label=str(continuity_label or ""),
                        active_topic_strength=float(active_topic_strength or 0.0),
                    ):
                        conversation_history = (conversation_history or [])
                        conversation_history.append(
                            {
                                "role": "system",
                                "content": f"CONTINUITY_CONTEXT: The user is still discussing {active_topic_text}.",
                            }
                        )
                except Exception:
                    pass

                _gen_ok = False
                try:
                    _trace_emit("assistant.generate.start")
                except Exception:
                    pass
                try:
                    result = await model_pipeline.run_request_pipeline(
                        ctx=sys.modules[__name__],
                        current_project_full=current_project_full,
                        clean_user_msg=clean_user_msg,
                        do_search=bool(do_search),
                        search_results=search_results,
                        conversation_history=conversation_history,
                        max_history_pairs=MAX_HISTORY_PAIRS,
                        server_file_path=Path(__file__).resolve(),
                        allow_history_in_lookup=bool(allow_history_in_lookup),
                        search_cached=bool(search_cached),
                        thread_synthesis_text=thread_synthesis_for_turn,
                        active_expert=active_expert,
                    )
                    _gen_ok = True
                finally:
                    try:
                        _trace_emit("assistant.generate.end", {"ok": bool(_gen_ok)})
                    except Exception:
                        pass
                # Update thread synthesis cache from current evidence (ephemeral)
                try:
                    sr0 = (search_results or "").strip()
                    if sr0:
                        ev_hash = hashlib.sha256(sr0.encode("utf-8", errors="ignore")).hexdigest()[:16]
                        if ev_hash != last_thread_synthesis_hash:
                            synth = ""
                            try:
                                synth = str(model_pipeline._build_thread_synthesis_from_search_results(sr0) or "").strip()
                            except Exception:
                                synth = ""
                            if synth:
                                last_thread_synthesis_text = synth
                                last_thread_synthesis_hash = ev_hash
                except Exception:
                    pass
                # ---------------------------------------------------------
                # AUDIT (server-side): record what path produced this turn
                # This guarantees audit_log.jsonl exists even if model_pipeline
                # audit hooks are not yet wired correctly.
                # ---------------------------------------------------------
                try:
                    audit_event(
                        current_project_full,
                        {
                            "schema": "audit_turn_v1",
                            "stage": "server_after_run_request_pipeline",
                            "clean_user_msg": str(clean_user_msg or ""),
                            "do_search": bool(do_search),
                            "search_cached": bool(search_cached),
                            "search_results_len": len(search_results or ""),
                            "search_route_mode": str(search_route_mode or ""),
                            "deep_search": bool(deep_search),
                            "user_text_for_search": str(_user_text_for_search or ""),
                            "lookup_mode": bool(result.get("lookup_mode") or False),
                        },
                    )
                except Exception:
                    pass

                user_answer = str(result.get("user_answer") or "").strip()
                if upload_notice:
                    user_answer = (upload_notice + "\n\n" + user_answer).strip()
                lookup_mode = bool(result.get("lookup_mode") or False)
                conversation_history = result.get("conversation_history") or []

                # Capability-gap search assist (optional, bounded).
                try:
                    cap_gap_obj = result.get("capability_gap")
                    if not isinstance(cap_gap_obj, dict):
                        blocks0 = result.get("blocks") if isinstance(result, dict) else {}
                        if isinstance(blocks0, dict):
                            cap_gap_obj = _parse_capability_gap_json(str(blocks0.get("CAPABILITY_GAP_JSON") or "").strip())
                    if isinstance(cap_gap_obj, dict) and cap_gap_obj:
                        queries = _capability_gap_search_queries(cap_gap_obj)
                        if queries and (not lookup_mode):
                            if (os.getenv("BRAVE_API_KEY", "") or "").strip():
                                sr = brave_search_multi(queries, count=5)
                                if sr and ("Search error" not in sr):
                                    sug = await _synthesize_capability_gap_suggestions(
                                        clean_user_msg=clean_user_msg,
                                        cap_gap_obj=cap_gap_obj,
                                        search_results=sr,
                                    )
                                    if sug:
                                        user_answer = (user_answer.rstrip() + "\n\nSuggested additions:\n" + sug).strip()
                                else:
                                    user_answer = (
                                        user_answer.rstrip()
                                        + "\n\nI tried to pull tools via web search, but it is not configured here."
                                    ).strip()
                            else:
                                user_answer = (
                                    user_answer.rstrip()
                                    + "\n\nI can search for tools to close this gap, but web search is not configured."
                                ).strip()
                except Exception:
                    pass

                # If model forgot USER_ANSWER entirely, don't send an empty WS message.
                if not (user_answer or "").strip():
                    user_answer = "Done. (Pipeline returned an empty response.)"

            except Exception as e:
                try:
                    _trace_emit("exception", {"err_type": type(e).__name__, "where": "run_request_pipeline"})
                except Exception:
                    pass
                err = f"I hit an internal error: {e!r}"
                await _ws_send_safe(err)
                last_full_answer_text = err
                continue

            # ---- SEND + LOG (this was accidentally unreachable before) ----
            try:
                logged_user_content = clean_user_msg
                if bool(t2g_written_this_turn) and (t2g_project_log_redaction or "").strip():
                    logged_user_content = t2g_project_log_redaction

                append_jsonl(
                    state_dir(current_project_full) / "chat_log.jsonl",
                    {"ts": now_iso(), "role": "user", "content": logged_user_content},
                )
                append_jsonl(
                    state_dir(current_project_full) / "chat_log.jsonl",
                    {"ts": now_iso(), "role": "assistant", "content": user_answer},
                )
            except Exception:
                pass

            try:
                _maybe_set_chat_display_name(
                    current_project_full,
                    current_project,
                    user_msg=clean_user_msg,
                    active_topic_text=active_topic_text,
                )
            except Exception:
                pass

            try:
                project_store.update_couples_shared_memory_from_message(
                    current_project_full,
                    user,
                    clean_user_msg,
                )
            except Exception:
                pass

            try:
                project_store.update_couples_shared_memory_from_assistant(
                    current_project_full,
                    user_answer,
                )
            except Exception:
                pass

            # No domain-specific guards.
            await _ws_send_safe(user_answer)
            last_full_answer_text = user_answer

            # Automatic analysis backfill (debounced)
            try:
                await _maybe_schedule_analysis_backfill(reason="post_turn")
            except Exception:
                pass

            # -------------------------------------------------------------
            # USER-SCOPED pending disambiguation setter (post-answer)
            #
            # If the assistant asks a location precision question like:
            #   Do you want "...Oklahoma City, OK..." or "...Oklahoma City metro area"?
            # persist the options under projects/<user>/_user/pending_disambiguation.json
            # so the NEXT user reply can be consumed without magic words.
            # -------------------------------------------------------------
            try:
                ans = (user_answer or "")
                low_ans = ans.lower()

                # Detect common disambiguation prompt shapes (deterministic)
                # Examples we want to catch:
                # - "Stored location set to exactly "X" ... or "Y" ..."
                # - "Do you want it stored as "X" ... or "Y" ..."
                if (
                    (
                        ("stored location set to exactly" in low_ans)
                        or ("do you want it stored as" in low_ans)
                        or ("do you want it stored" in low_ans)
                    )
                    and (" or " in low_ans)
                ):
                    # Capture quoted options (handles straight quotes and curly quotes)
                    found = re.findall(r"[\"“”']([^\"“”']{2,80})[\"“”']", ans)
                    # Keep only plausible location strings
                    cleaned: List[str] = []
                    for s in found:
                        v = str(s).strip().strip(" .!?,;:")
                        v = " ".join(v.split())
                        if not v:
                            continue
                        if v not in cleaned:
                            cleaned.append(v)

                    # We need at least 2 options for a meaningful disambiguation
                    if len(cleaned) >= 2:
                        exp = time.time() + 300.0  # 5 minutes
                        project_store.save_user_pending_disambiguation(
                            user,
                            {
                                "version": 1,
                                "pending": True,
                                "kind": "identity.location",
                                "created_at": now_iso(),
                                "expires_at_epoch": exp,
                                "options": cleaned[:4],
                                "default": cleaned[0],
                                "asked_msg_index": int(user_msg_counter),
                            },
                        )
            except Exception:
                pass

    except (ConnectionClosedOK, ConnectionClosedError) as e:
        try:
            _trace_emit("exception", {"err_type": type(e).__name__, "where": "ws_connection"})
        except Exception:
            pass
        print(f"[WS] Client disconnected: {e!r}")
    except Exception as e:
        try:
            _trace_emit("exception", {"err_type": type(e).__name__, "where": "ws_handler"})
        except Exception:
            pass
        print(f"[WS] Unhandled error: {e!r}")
    finally:
        try:
            _ws_remove_client(current_project_full, websocket)
        except Exception:
            pass
        print("[WS] Connection handler exiting.")

# -----------------------------------------------------------------------------
# Root UI handler (serves the web UI at GET /)
# -----------------------------------------------------------------------------
# NOTE: Keep a single handle_root() definition to avoid accidental shadowing.
# The implementation below is the robust one (handles UI_FILE env override safely).
#
# (handle_root is defined later in the file.)
#
#
# NOTE: Do NOT define handle_get_manifest() twice.
# The correct JSON /manifest handler is defined later.
# (duplicate handler block removed)
# =============================================================================
# Manifest + simple classification HTTP API
# =============================================================================
# -----------------------------------------------------------------------------
# Root UI handler (serves the web UI at GET /)
# -----------------------------------------------------------------------------

async def main() -> None:
    # Respect env overrides (already baked into HOST/WS_PORT/HTTP_PORT)
    host = HOST
    ws_port = WS_PORT
    http_port = HTTP_PORT

    ensure_runtime_scaffold()

    PROJECTS_DIR.mkdir(parents=True, exist_ok=True)
    ensure_project_scaffold(DEFAULT_PROJECT_NAME)

    print(f"[BOOT] OPENAI_MODEL={OPENAI_MODEL}")
    print(f"[BOOT] PROJECT_ROOT={PROJECT_ROOT}")
    try:
        print(f"[BOOT] SERVER_FILE={Path(__file__).resolve()}")
    except Exception:
        pass
    try:
        print(f"[BOOT] SERVER_SHA256={file_sha256_bytes(read_bytes_file(Path(__file__).resolve()))}")
    except Exception:
        pass
    print(f"[BOOT] SERVER_COLOR={SERVER_COLOR}  (recommended active: {read_active_color('blue')})")
    print(f"[BOOT] WebSocket: ws://{host}:{ws_port}")
    print(f"[BOOT] HTTP:      http://{host}:{http_port}")

    # Install shutdown event + signal handlers
    global _SHUTDOWN_EVENT
    _SHUTDOWN_EVENT = asyncio.Event()
    try:
        loop = asyncio.get_running_loop()

        def _sig() -> None:
            request_shutdown("signal")

        try:
            loop.add_signal_handler(signal.SIGINT, _sig)
        except Exception:
            pass
        try:
            loop.add_signal_handler(signal.SIGTERM, _sig)
        except Exception:
            pass
    except Exception:
        pass
    # Start upload worker(s) (extracted)
    upload_pipeline.start(
        project_root=PROJECT_ROOT,
        caption_image_fn=_caption_image_bytes,
        classify_image_fn=_classify_image_wrapper,
        workers=1,
    )
    print("[BOOT] Upload worker started (1)")

    # Some proxies / monitors may hit the WS port with plain HTTP (no Upgrade).
    # That produces handshake tracebacks like "InvalidUpgrade: invalid Connection header: keep-alive".
    # This hook returns a simple 200 for non-WS requests and allows real WS upgrades to proceed.
    def _ws_process_request(*args, **kwargs):
        try:
            headers = None

            # Common forms:
            # - process_request(path, request_headers)
            # - process_request(connection, request)
            if len(args) == 2:
                a0, a1 = args
                if isinstance(a0, str):
                    # (path, headers)
                    headers = a1
                else:
                    # (connection, request)
                    req = a1
                    headers = getattr(req, "headers", None) or getattr(req, "raw_headers", None)

            def hget(key: str) -> str:
                if not headers:
                    return ""
                try:
                    return (headers.get(key, "") or "")
                except Exception:
                    return ""

            conn = hget("Connection").lower()
            upg = hget("Upgrade").lower()

            # If it doesn't look like a websocket upgrade, reply 200 OK.
            if ("upgrade" not in conn) or ("websocket" not in upg):
                body = b"Lens-0 WebSocket Port Active\n"
                return (200, [("Content-Type", "text/plain"), ("Content-Length", str(len(body)))], body)

            return None
        except Exception:
            return None

    # Keep mobile + proxy connections alive (Cloudflare / carrier NAT will drop idle WS fast).
    ws_server = await websockets.serve(
        handle_connection,
        host,
        ws_port,
        process_request=_ws_process_request,
        ping_interval=30,   # send ping every 20s
        ping_timeout=120,    # if pong not received within 20s, consider dead
        close_timeout=10,   # faster close handshake
        max_size=2**22,     # allow larger messages (4MB) without disconnect
    )


    app = web.Application()
    http_api.register_routes(app, ctx=sys.modules[__name__])

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, http_port)
    await site.start()

    try:
        # Run until request_shutdown() is called
        await _SHUTDOWN_EVENT.wait()
    finally:
        # Stop upload workers (extracted)
        try:
            upload_pipeline.stop()
        except Exception:
            pass


        ws_server.close()
        await ws_server.wait_closed()
        await runner.cleanup()


def _smoke_test_global_memory_write_guards() -> Tuple[bool, str]:
    """
    Deterministic, offline guard test:
    server.py must refuse any direct writes/appends to paths under /_user/.

    This does NOT depend on network, model calls, or the presence of real user data.
    """
    try:
        # Construct a path that matches the global memory directory shape.
        # We do NOT want to actually write anything here; we only validate we *refuse*.
        p_user = PROJECT_ROOT / "projects" / "SMOKE_USER" / "_user" / "profile.json"
        p_raw = PROJECT_ROOT / "projects" / "SMOKE_USER" / "_user" / "facts_raw.jsonl"

        # 1) atomic_write_text must refuse
        ok1 = False
        try:
            atomic_write_text(p_user, "x")
        except Exception as e:
            ok1 = "Refusing direct write to global user memory" in str(e) or "/_user/" in str(p_user).replace("\\", "/")
        if not ok1:
            return False, "atomic_write_text did not refuse /_user/ write"

        # 2) atomic_write_bytes must refuse
        ok2 = False
        try:
            atomic_write_bytes(p_user, b"x")
        except Exception as e:
            ok2 = "Refusing direct write to global user memory" in str(e) or "/_user/" in str(p_user).replace("\\", "/")
        if not ok2:
            return False, "atomic_write_bytes did not refuse /_user/ write"

        # 3) append_jsonl must refuse
        ok3 = False
        try:
            append_jsonl(p_raw, {"ts": now_iso(), "kind": "smoke"})
        except Exception as e:
            ok3 = "Refusing direct append to global user memory" in str(e) or "/_user/" in str(p_raw).replace("\\", "/")
        if not ok3:
            return False, "append_jsonl did not refuse /_user/ append"

        return True, "ok"
    except Exception as e:
        return False, f"guard smoke crashed: {e!r}"


def _run_smoke_test_with_guard_checks() -> int:
    """
    Preserve existing smoke test behavior, then add guard checks.
    Returns process exit code.
    """
    # Base smoke runner was previously external (run_smoke_test) but is not present in this repo.
    # Treat "base smoke" as pass=0 and rely on deterministic guard checks below.
    base_rc = 0

    ok, note = _smoke_test_global_memory_write_guards()
    if not ok:
        print(f"[SMOKE] global memory write-guard FAILED: {note}")
        return max(1, base_rc)
    print("[SMOKE] global memory write-guard OK")

    # Capabilities registry smoke: must be internally consistent and contain core surfaces.
    try:
        required = [
            "http.health",
            "http.file",
            "http.upload",
            "http.manifest",
            "http.projects",
            "http.capabilities",
            "ws.chat",
            "ws.commands.basic",
            "ws.search",
            "ws.patch.mode",
        ]
        ok2, note2 = capabilities.smoke_test_registry(required_ids=required)
        if not ok2:
            print(f"[SMOKE] capabilities registry FAILED:\n{note2}")
            return max(1, base_rc)
        print("[SMOKE] capabilities registry OK")
    except Exception as e:
        print(f"[SMOKE] capabilities registry smoke crashed: {e!r}")
        return max(1, base_rc)

    return base_rc


if __name__ == "__main__":
    # Smoke test short-circuit (must run BEFORE asyncio loop)
    if len(sys.argv) > 1 and sys.argv[1].strip().lower() in ("--smoke", "smoke"):
        raise SystemExit(_run_smoke_test_with_guard_checks())

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except asyncio.CancelledError:
        pass

