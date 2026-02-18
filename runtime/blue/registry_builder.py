# -*- coding: utf-8 -*-
"""
registry_builder.py

Deterministic stdlib-only codebase registry generator.

Outputs are intended to be written by the caller (e.g., server.py) into a project's artifacts folder.

Scans:
- Python modules (*.py)

Extracts:
- module purpose (first line of module docstring)
- top-level functions/classes with signatures (best-effort)
- module-level imports
- approximate outbound call names (AST Call nodes)
- inbound reference counts across the repo (token scan)
- entry points (heuristics: __main__, server WS handler, HTTP route registrations)
- dead suspects (symbols never referenced outside their defining file)
"""

from __future__ import annotations

import ast
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# -----------------------------
# Deterministic repo walk config
# -----------------------------

_IGNORE_DIRS = {
    "projects",
    # NOTE: do NOT ignore "runtime" — the active server code lives under runtime/blue|green.
    # We still ignore heavy/non-code subfolders via the more specific rules below.
    "patches",
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    "node_modules",
    "dist",
    "build",
    ".pytest_cache",
    ".mypy_cache",
}

# Additional prunes anywhere in path (keeps runtime lean while still scanning runtime/blue code)
_IGNORE_PATH_PARTS = {
    "logs",
}

_PY_SUFFIX = ".py"

# UI / non-Python entrypoints (explicit allowlist; deterministic + low-noise)
_UI_ENTRYPOINT_FILENAMES = {
    "Project Web 2 - BLUE.html",
}
def _scan_ui_js_function_defs(text: str, *, cap: int = 60) -> Dict[str, Any]:
    """
    Deterministically detect duplicate JS function definitions inside the UI entrypoint.

    Why:
    - Duplicate defs silently override behavior and commonly cause "startup weird loads".
    """
    t = text or ""

    # function foo(...) { ... }
    rx_fn = re.compile(r"(?m)^\s*function\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*\(")
    # const foo = (...) => { ... } / let foo = function(...) { ... }
    rx_assign = re.compile(
        r"(?m)^\s*(?:const|let|var)\s+([A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*(?:async\s*)?(?:function\s*\(|\([^)]*\)\s*=>)"
    )

    counts: Dict[str, int] = {}
    for m in rx_fn.finditer(t):
        name = (m.group(1) or "").strip()
        if name:
            counts[name] = counts.get(name, 0) + 1
    for m in rx_assign.finditer(t):
        name = (m.group(1) or "").strip()
        if name:
            counts[name] = counts.get(name, 0) + 1

    dups = sorted([k for k, v in counts.items() if int(v) >= 2])[: int(cap or 60)]
    return {
        "js_function_def_counts": {k: int(v) for (k, v) in sorted(counts.items())},
        "js_duplicate_function_defs": dups,
    }


def _scan_ui_startup_flow_risks(text: str, *, cap: int = 30) -> List[Dict[str, Any]]:
    """
    Deterministically flag UI startup churn patterns:
    - multiple thread.get sends/calls inside ws.onopen window
    - restore switch + thread.get both present in ws.onopen window
    - multiple ws.onopen assignments
    """
    t = text or ""
    out: List[Dict[str, Any]] = []

    # Multiple ws.onopen assignments
    onopen_hits = len(re.findall(r"(?m)^\s*ws\.onopen\s*=\s*\(\)\s*=>\s*\{", t))
    if onopen_hits >= 2:
        out.append(
            _warn_record(
                rel="(ui)",
                code="ui_ws_onopen_multiple_assignments",
                severity="high",
                where="coarse_match",
                evidence=f"ws.onopen assigned {onopen_hits} times",
            )
        )

    # Bounded slice around first ws.onopen (no JS parse; deterministic)
    m = re.search(r"(?m)^\s*ws\.onopen\s*=\s*\(\)\s*=>\s*\{", t)
    if m:
        window = t[m.start(): m.start() + 6500]  # bounded

        tg_json = len(re.findall(r'"type"\s*:\s*"thread\.get"', window))
        tg_call = len(re.findall(r"\brequestThreadHistory(?:Once)?\s*\(", window))
        sw = len(re.findall(r"ws\.send\(\s*['\"]!switch project:\s*", window))
        last_ans = len(re.findall(r"\brequestLatestAssistantOutput\s*\(", window))

        if (tg_json + tg_call) >= 2:
            out.append(
                _warn_record(
                    rel="(ui)",
                    code="ui_startup_multiple_thread_get",
                    severity="high",
                    where="ws.onopen_window",
                    evidence=f"thread.get signals in ws.onopen window: json={tg_json}, calls={tg_call}",
                )
            )

        if sw >= 1 and (tg_json + tg_call) >= 1:
            out.append(
                _warn_record(
                    rel="(ui)",
                    code="ui_startup_switch_plus_thread_get",
                    severity="high",
                    where="ws.onopen_window",
                    evidence=f"restore switch present (count={sw}) and thread.get present (json={tg_json}, calls={tg_call})",
                )
            )

        if last_ans >= 2:
            out.append(
                _warn_record(
                    rel="(ui)",
                    code="ui_startup_multiple_last_answer",
                    severity="medium",
                    where="ws.onopen_window",
                    evidence=f"requestLatestAssistantOutput calls in ws.onopen window: {last_ans}",
                )
            )

    return out[: int(cap or 30)]
_UI_SUFFIXES = {".html", ".htm"}

def _scan_ui_emit_sites(text: str, *, cap: int = 80) -> Dict[str, Any]:
    """
    Deterministically index UI "emit sites" that cause user-visible churn or server actions.

    Emits we care about:
    - ws.send("...")  (control-plane + legacy sends)
    - JSON ws.send({type:"thread.get"/"greeting.request"/...})
    - requestThreadHistory / requestThreadHistoryOnce
    - requestLatestAssistantOutput
    - updateProjectName (often triggers history fetch)
    - connect() / scheduleReconnect() (connection churn)

    Additionally, we tag which emits occur inside the FIRST ws.onopen window.
    This makes startup double-firing diagnosable via the registry alone.
    """
    t = text or ""
    lines = t.splitlines()

    # Patterns (kept conservative; deterministic; bounded)
    rx_ws_send = re.compile(r"""ws\.send\(""")
    rx_thread_get = re.compile(r"""type\s*:\s*['"]thread\.get['"]""")
    rx_greet_req = re.compile(r"""type\s*:\s*['"]greeting\.request['"]""")
    rx_last_ans = re.compile(r"""\brequestLatestAssistantOutput\s*\(""")
    rx_req_hist = re.compile(r"""\brequestThreadHistory(?:Once)?\s*\(""")
    rx_switch = re.compile(r"""ws\.send\(\s*['"]!switch project:\s*""")
    rx_connect = re.compile(r"""\bconnect\s*\(""")
    rx_sched_reco = re.compile(r"""\bscheduleReconnect\s*\(""")
    rx_update_pn = re.compile(r"""\bupdateProjectName\s*\(""")

    def _emit(kind: str, i: int, s: str) -> Dict[str, Any]:
        return {
            "kind": kind,
            "line": int(i + 1),
            "evidence": (s or "").strip()[:220],
        }

    emit_sites: List[Dict[str, Any]] = []
    for i, ln in enumerate(lines):
        s = (ln or "").strip()
        if not s:
            continue

        # Order matters a bit: more specific tags first
        if rx_thread_get.search(s):
            emit_sites.append(_emit("ws_frame:thread.get", i, s))
        if rx_greet_req.search(s):
            emit_sites.append(_emit("ws_frame:greeting.request", i, s))
        if rx_switch.search(s):
            emit_sites.append(_emit("ws_send:!switch_project", i, s))
        elif rx_ws_send.search(s):
            emit_sites.append(_emit("ws_send:any", i, s))

        if rx_req_hist.search(s):
            emit_sites.append(_emit("call:requestThreadHistory", i, s))
        if rx_last_ans.search(s):
            emit_sites.append(_emit("call:requestLatestAssistantOutput", i, s))
        if rx_update_pn.search(s):
            emit_sites.append(_emit("call:updateProjectName", i, s))
        if rx_sched_reco.search(s):
            emit_sites.append(_emit("call:scheduleReconnect", i, s))
        if rx_connect.search(s):
            # Avoid tagging obvious function defs like "function connect("
            if not s.startswith(("function connect", "async function connect", "def connect")):
                emit_sites.append(_emit("call:connect", i, s))

        if len(emit_sites) >= int(cap or 80):
            break

    # Tag emits inside FIRST ws.onopen window (bounded slice, no JS parsing)
    onopen_emit_sites: List[Dict[str, Any]] = []
    m = re.search(r"(?m)^\s*ws\.onopen\s*=\s*\(\)\s*=>\s*\{", t)
    if m:
        window = t[m.start(): m.start() + 6500]  # bounded window
        wlines = window.splitlines()

        # Re-run the same scan but on the window text; record line numbers relative to file
        base_line = int(t[: m.start()].count("\n"))  # 0-index offset for line numbers
        for j, ln in enumerate(wlines):
            s = (ln or "").strip()
            if not s:
                continue

            if rx_thread_get.search(s):
                onopen_emit_sites.append(_emit("ws_frame:thread.get", base_line + j, s))
            if rx_greet_req.search(s):
                onopen_emit_sites.append(_emit("ws_frame:greeting.request", base_line + j, s))
            if rx_switch.search(s):
                onopen_emit_sites.append(_emit("ws_send:!switch_project", base_line + j, s))
            elif rx_ws_send.search(s):
                onopen_emit_sites.append(_emit("ws_send:any", base_line + j, s))

            if rx_req_hist.search(s):
                onopen_emit_sites.append(_emit("call:requestThreadHistory", base_line + j, s))
            if rx_last_ans.search(s):
                onopen_emit_sites.append(_emit("call:requestLatestAssistantOutput", base_line + j, s))
            if rx_update_pn.search(s):
                onopen_emit_sites.append(_emit("call:updateProjectName", base_line + j, s))
            if rx_sched_reco.search(s):
                onopen_emit_sites.append(_emit("call:scheduleReconnect", base_line + j, s))
            if rx_connect.search(s):
                if not s.startswith(("function connect", "async function connect", "def connect")):
                    onopen_emit_sites.append(_emit("call:connect", base_line + j, s))

            if len(onopen_emit_sites) >= 60:
                break

    return {
        "ui_emit_sites": emit_sites[: int(cap or 80)],
        "ui_emit_sites_ws_onopen": onopen_emit_sites[:60],
    }
def _walk_ui_files(root: Path) -> List[Path]:
    """
    Deterministically locate UI entrypoint files by explicit filename allowlist.

    We intentionally do NOT scan all *.html to avoid pulling in unrelated assets.
    """
    out: List[Path] = []
    root = root.resolve()

    allow_lower = {n.lower() for n in _UI_ENTRYPOINT_FILENAMES}

    for dirpath, dirnames, filenames in os.walk(root):
        dpath = Path(dirpath)
        rel_parts = _safe_rel(root, dpath).split("/")

        if any(part in _IGNORE_DIRS for part in rel_parts):
            dirnames[:] = []
            continue
        if any(part in _IGNORE_PATH_PARTS for part in rel_parts):
            dirnames[:] = []
            continue

        dirnames[:] = [
            d for d in dirnames
            if d not in _IGNORE_DIRS
            and d not in _IGNORE_PATH_PARTS
            and not d.startswith(".")
        ]

        for fn in filenames:
            if fn.startswith("."):
                continue
            if fn.lower() not in allow_lower:
                continue
            if Path(fn).suffix.lower() not in _UI_SUFFIXES:
                continue
            out.append(dpath / fn)

    out.sort(key=lambda p: _safe_rel(root, p))
    return out

# (dedup) NOTE: removed duplicate definition of _scan_ui_js_function_defs.
# Keep the earlier canonical definition (cap=60) above.


# (dedup) NOTE: removed duplicate definition of _scan_ui_startup_flow_risks.
# Keep the earlier canonical definition above.
def _summarize_html_ui_contract(text: str) -> Dict[str, Any]:
    """
    Best-effort, regex-based extraction of UI contract surfaces:
    - HTTP endpoints referenced via `${API_ORIGIN}/...`
    - ws.send("...") string commands
    - WS framing hints (v=1, type="chat.send")
    - heartbeat tokens (__ping__/__pong__)
    """
    t = text or ""

    # Endpoints referenced via `${API_ORIGIN}/...`
    ep_pat = re.compile(r"""\$\{API_ORIGIN\}(/[^`"' \)\}]+)""")
    endpoints = {m.group(1) for m in ep_pat.finditer(t) if m.group(1)}

    # Common plain-string endpoints (rare, but keep deterministic)
    ep2_pat = re.compile(r"""fetch\(\s*['"](/[^'"]+)['"]""")
    for m in ep2_pat.finditer(t):
        if m.group(1):
            endpoints.add(m.group(1))

    # ws.send("...") commands (string-literal only; ignores templates)
    ws_send_pat = re.compile(r"""ws\.send\(\s*(['"])(.*?)\1\s*\)""")
    ws_cmds = []
    for m in ws_send_pat.finditer(t):
        s = (m.group(2) or "").strip()
        if not s:
            continue
        ws_cmds.append(s)

    # Deterministic unique ordering
    ws_cmds_unique = sorted(set(ws_cmds))

    # WS command patterns (dynamic sends).
    # We only capture obvious prefix-literal patterns like:
    #   ws.send("switch project: " + name)
    #   ws.send("[FILE_ADDED]" + path)
    #   ws.send(`switch project: ${name}`)
    ws_patterns: List[str] = []

    # "prefix" + something
    ws_send_prefix_plus_pat = re.compile(r"""ws\.send\(\s*(['"])([^'"]+?)\1\s*\+""")
    for m in ws_send_prefix_plus_pat.finditer(t):
        prefix = (m.group(2) or "")
        prefix = prefix.strip()
        if prefix:
            # Preserve exact prefix intent; normalize to "<dynamic>" marker.
            # Example: "switch project:" or "switch project: " both become "switch project: <dynamic>"
            if prefix.endswith(":"):
                ws_patterns.append(prefix + " <dynamic>")
            elif prefix.endswith(": "):
                ws_patterns.append(prefix.rstrip() + " <dynamic>")
            else:
                ws_patterns.append(prefix + "<dynamic>" if prefix.endswith("]") else (prefix + " <dynamic>"))

    # `prefix ${...}` template literal
    ws_send_tpl_pat = re.compile(r"""ws\.send\(\s*`([^`]*?)\$\{""")
    for m in ws_send_tpl_pat.finditer(t):
        prefix = (m.group(1) or "").strip()
        if prefix:
            if prefix.endswith(":"):
                ws_patterns.append(prefix + " <dynamic>")
            elif prefix.endswith(": "):
                ws_patterns.append(prefix.rstrip() + " <dynamic>")
            else:
                ws_patterns.append(prefix + "<dynamic>" if prefix.endswith("]") else (prefix + " <dynamic>"))

    ws_patterns_unique = sorted(set(ws_patterns))

    # Framing hint
    has_frame_v1 = bool(re.search(r"""\bv\s*:\s*1\b""", t))
    has_chat_send = bool(re.search(r"""type\s*:\s*['"]chat\.send['"]""", t))

    # Heartbeat tokens
    ping = "__ping__" if "__ping__" in t else ""
    pong = "__pong__" if "__pong__" in t else ""

    # Extra UI debug surfaces
    ui_defs = _scan_ui_js_function_defs(t, cap=60)

    return {
        "http_endpoints_used": sorted(endpoints),
        "ws_commands_seen": ws_cmds_unique,
        "ws_command_patterns": ws_patterns_unique,
        "ws_contract": {
            "frame_version": 1 if (has_frame_v1 and has_chat_send) else 0,
            "has_chat_send_frame": bool(has_chat_send),
            "ping": ping,
            "pong": pong,
        },
        # Debug surfaces for UI drift / startup churn
        "ui_js_function_def_counts": ui_defs.get("js_function_def_counts") or {},
        "ui_js_duplicate_function_defs": ui_defs.get("js_duplicate_function_defs") or [],
    }


def _ui_entrypoint_record(root: Path, path: Path) -> Dict[str, Any]:
    rel = _safe_rel(root, path)
    text = _read_text(path)

    rec: Dict[str, Any] = {
        "path": rel,
        "type": "html_ui",
    }
    rec.update(_summarize_html_ui_contract(text))

    # Emit-site indexing (diagnostic)
    try:
        em = _scan_ui_emit_sites(text, cap=120)
        rec["ui_emit_sites"] = em.get("ui_emit_sites") or []
        rec["ui_emit_sites_ws_onopen"] = em.get("ui_emit_sites_ws_onopen") or []
    except Exception:
        rec["ui_emit_sites"] = []
        rec["ui_emit_sites_ws_onopen"] = []

    return rec

# -----------------------------
# Registry warnings (v1)
# -----------------------------
#
# Goal:
# - Deterministically flag “foundational drift” issues:
#   - unreachable code after return
#   - duplicated logic blocks inside a function
#   - Tier-2G (profile.json) writes outside project_store.py (dual-writer risk)
#
# Hard rules:
# - stdlib only
# - bounded output (cap warnings per file and total)
#

def _warn_record(*, rel: str, code: str, severity: str, where: str, evidence: str) -> Dict[str, Any]:
    return {
        "file": rel,
        "code": code,
        "severity": severity,
        "where": where,
        "evidence": (evidence or "").strip()[:220],
    }

def _scan_unreachable_after_return(rel: str, text: str, *, cap: int = 12) -> List[Dict[str, Any]]:
    """
    Deterministic "real unreachable only" heuristic (v2):

    We ONLY flag cases that are truly unreachable without needing control-flow analysis:
    - A `return` occurs at the function's BASE BODY indent (not nested inside if/try/for/with),
      and later we see another non-empty, non-comment statement at that same base indent
      before the function ends.

    This intentionally ignores:
    - returns inside nested blocks (indented > base body)
    - guard returns (inside `if ...:` blocks)
    - multi-line `return (` ... `)` formatting
    """
    lines = (text or "").splitlines()
    out: List[Dict[str, Any]] = []

    def _indent(s: str) -> int:
        return len(s) - len(s.lstrip(" "))

    def _is_closer_only(s: str) -> bool:
        # allow common multi-line return closers at base indent without flagging
        st = (s or "").strip()
        return st in (")", "]", "}", "),", "],", "},") or st.startswith((")", "]", "}")) and st.rstrip(",") in (")", "]", "}")

    in_def = False
    def_indent = 0
    body_indent: Optional[int] = None

    saw_base_return = False
    base_return_line = ""
    base_return_lineno = 0
    base_return_multiline = False

    for i, raw in enumerate(lines):
        ln = raw.rstrip("\n").rstrip("\r")
        st = ln.strip()

        # Function start
        if st.startswith(("def ", "async def ")):
            in_def = True
            def_indent = _indent(ln)
            body_indent = None
            saw_base_return = False
            base_return_line = ""
            base_return_lineno = 0
            base_return_multiline = False
            continue

        if not in_def:
            continue

        ind = _indent(ln)

        # Function end
        if st and ind <= def_indent and not st.startswith(("#", "@")):
            in_def = False
            body_indent = None
            saw_base_return = False
            base_return_line = ""
            base_return_lineno = 0
            base_return_multiline = False
            continue

        # Skip blanks/comments
        if not st or st.startswith("#"):
            continue

        # Determine base body indent (first real statement in body)
        if body_indent is None:
            if ind > def_indent:
                body_indent = ind
            else:
                # Defensive: if formatting is odd, assume 4-space indent
                body_indent = def_indent + 4

        # Only consider returns at BASE BODY indent as candidates for "truly unreachable"
        if ind == body_indent and (st == "return" or st.startswith("return ")):
            saw_base_return = True
            base_return_line = st
            base_return_lineno = i + 1
            # Multi-line return expression (return ( / return [ / return { or trailing opener)
            st2 = st.replace(" ", "")
            base_return_multiline = st2.endswith(("(", "[", "{")) or st2 in ("return(", "return[", "return{")
            continue

        # If we previously saw a base return, any later statement at base indent is unreachable.
        if saw_base_return and ind == body_indent:
            # Allow pure closers for multi-line return formatting
            if base_return_multiline and _is_closer_only(st):
                continue

            # Ignore structural keywords that could appear in rare formatting edge cases
            if st.startswith(("elif ", "else:", "except", "finally:")):
                continue

            out.append(
                _warn_record(
                    rel=rel,
                    code="unreachable_after_return",
                    severity="high",
                    where=f"line_{i+1}",
                    evidence=f"return@{base_return_lineno}='{base_return_line}' then '{st}' at base indent",
                )
            )
            if len(out) >= cap:
                break

    return out

def _scan_unreachable_after_return_ast(rel: str, text: str, *, cap: int = 12) -> List[Dict[str, Any]]:
    """
    True unreachable-after-return detection using AST only.

    Flags ONLY this case:
    - A Return statement occurs in a function body
    - A later sibling statement exists in that same body block

    No indentation heuristics. No multiline special-casing. No false positives.
    """
    out: List[Dict[str, Any]] = []

    try:
        tree = ast.parse(text or "")
    except Exception:
        return out

    class V(ast.NodeVisitor):
        def visit_FunctionDef(self, node: ast.FunctionDef):
            self._check_body(node.body, node.name)
            self.generic_visit(node)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
            self._check_body(node.body, node.name)
            self.generic_visit(node)

        def _check_body(self, body: List[ast.stmt], fn_name: str):
            saw_return = False
            for stmt in body:
                if saw_return:
                    lineno = getattr(stmt, "lineno", None)
                    if lineno:
                        out.append(
                            _warn_record(
                                rel=rel,
                                code="unreachable_after_return",
                                severity="high",
                                where=f"line_{lineno}",
                                evidence=f"statement after return in function '{fn_name}'",
                            )
                        )
                        if len(out) >= cap:
                            return
                if isinstance(stmt, ast.Return):
                    saw_return = True

    V().visit(tree)
    return out

def _scan_duplicate_blocks_in_functions(rel: str, text: str, *, cap: int = 10) -> List[Dict[str, Any]]:
    """
    Detect duplicated “chunks” inside a function body:
    - Split the function body into blank-line-separated chunks
    - Normalize whitespace
    - Hash and find repeats
    Deterministic and bounded.
    """
    lines = (text or "").splitlines()
    out: List[Dict[str, Any]] = []

    def _indent(s: str) -> int:
        return len(s) - len(s.lstrip(" "))

    i = 0
    while i < len(lines):
        st = lines[i].strip()
        if not st.startswith(("def ", "async def ")):
            i += 1
            continue

        def_start = i
        def_indent = _indent(lines[i])
        i += 1

        body_lines: List[str] = []
        while i < len(lines):
            ln = lines[i]
            s2 = ln.strip()
            if s2 and _indent(ln) <= def_indent and not s2.startswith(("#", "@")):
                break
            body_lines.append(ln)
            i += 1

        # Chunk by blank lines
        chunks: List[str] = []
        cur: List[str] = []
        for ln in body_lines:
            if not ln.strip():
                if cur:
                    chunks.append("\n".join(cur))
                    cur = []
                continue
            cur.append(ln.rstrip())
        if cur:
            chunks.append("\n".join(cur))

        seen: Dict[str, int] = {}
        for idx, ch in enumerate(chunks):
            # Normalize: strip trailing spaces; collapse internal whitespace
            norm = "\n".join([" ".join(l.strip().split()) for l in ch.splitlines() if l.strip()])
            if len(norm) < 80:
                continue
            key = norm
            if key in seen:
                out.append(
                    _warn_record(
                        rel=rel,
                        code="duplicate_block_in_function",
                        severity="medium",
                        where=f"def_line_{def_start+1}",
                        evidence=f"chunk_{seen[key]} repeats at chunk_{idx}",
                    )
                )
                if len(out) >= cap:
                    break
            else:
                seen[key] = idx

        if len(out) >= cap:
            break

    return out

def _scan_tier2g_writes_outside_project_store(rel: str, text: str, *, cap: int = 6) -> List[Dict[str, Any]]:
    """
    Detect suspicious Tier-2G writes outside project_store.py.

    Note: We deliberately skip scanner modules to avoid self-matching on signal tables.

    v2 improvements:
    - Broader write detection (write_text / write_bytes / open(...,"w") / json.dump / .write(...))
    - Windowed correlation: Tier-2G signal and a write signal may be on adjacent lines
    - Emits precise evidence when possible; falls back to a single coarse warning
    """
    out: List[Dict[str, Any]] = []
    norm_rel = rel.replace("\\", "/")
    if norm_rel.endswith("project_store.py"):
        return out
    # Avoid self-matching scanners/patch tooling (signal tables contain strings like "profile.json", "open(", etc.)
    if norm_rel.endswith("registry_builder.py") or norm_rel.endswith("patch_engine.py"):
        return out

    lines = (text or "").splitlines()
    low = (text or "").lower()

    # Tier-2G signals (strong + weak)
    t2g_signals = ("profile.json", "user_profile_path", "/_user/", "\\_user\\")
    has_t2g_signal = any(s in low for s in t2g_signals)
    if not has_t2g_signal:
        return out

    # Delegation-only exemption:
    # If this module only *references* Tier-2G via project_store helpers and does not
    # appear to write into _user/ at all, do not flag it.
    #
    # This prevents false positives in orchestrators like server.py that:
    # - call project_store.user_profile_path(...)
    # - call rebuild_user_profile_from_user_facts(...)
    # but never write profile.json directly.
    #
    # Deterministic + conservative: only exempt when there is no explicit _user path usage.
    if ("user_profile_path" in low) and ("/_user/" not in low) and ("\\_user\\" not in low) and ("profile.json" not in low):
        # Still allow warning if there are clear direct writes (handled below).
        pass

    # Write primitives (broader than write_text)
    write_signals = (
        ".write_text(",
        ".write_bytes(",
        "atomic_write_text",
        "atomic_write_bytes",
        "open(",
        ".write(",
        "json.dump(",
    )
    # Pre-gate: require at least one *likely write* indicator (avoid flagging read-only code).
    has_write = (
        (".write_text(" in low)
        or (".write_bytes(" in low)
        or ("atomic_write_text" in low)
        or ("atomic_write_bytes" in low)
        or ("json.dump(" in low)
        or (".write(" in low)
        # open(...) only counts if it looks like write-mode somewhere in the file
        or ("open(" in low and ("\"w" in low or "'w" in low or "mode=" in low))
    )
    if not has_write:
        return out

    def _is_t2g_line(s: str) -> bool:
        sl = s.lower()
        return ("profile.json" in sl) or ("user_profile_path" in sl) or ("/_user/" in sl) or ("\\_user\\" in sl)

    def _is_strong_t2g_line(s: str) -> bool:
        """
        Strong Tier-2G signal: direct path targeting the global identity store.
        Delegation-only references (e.g., calling project_store.user_profile_path) are NOT strong.
        """
        sl = s.lower()
        return ("/_user/" in sl) or ("\\_user\\" in sl) or ("profile.json" in sl)

    def _is_write_line(s: str) -> bool:
        sl = s.lower()
        if "write_text" in sl or "write_bytes" in sl or "atomic_write_" in sl:
            return True
        if "json.dump" in sl or "json.dumps" in sl:
            return True
        # open(..., "w"/"wb"/"wt") / open(..., mode="w")
        if "open(" in sl and ("\"w" in sl or "'w" in sl or "mode=" in sl):
            return True
        # raw fileobj.write(...)
        if ".write(" in sl:
            return True
        return False

    # Windowed correlation: find any t2g signal within +/- 8 lines of a write
    window = 8
    emitted = 0
    for i, ln in enumerate(lines):
        s = ln.strip()
        if not s:
            continue

        if not (_is_t2g_line(s) or _is_write_line(s)):
            continue

        lo = max(0, i - window)
        hi = min(len(lines) - 1, i + window)
        t2g_at = None
        write_at = None
        for j in range(lo, hi + 1):
            sj = (lines[j] or "").strip()
            if not sj:
                continue
            if t2g_at is None and _is_t2g_line(sj):
                t2g_at = j
            if write_at is None and _is_write_line(sj):
                write_at = j
            if t2g_at is not None and write_at is not None:
                break

        if t2g_at is not None and write_at is not None:
            t2g_line = lines[t2g_at].strip()
            w_line = lines[write_at].strip()
            evidence = f"t2g@{t2g_at+1}: {t2g_line} | write@{write_at+1}: {w_line}"

            # Classify:
            # - If Tier-2G evidence is only delegation (user_profile_path) and not a direct _user/profile.json path,
            #   emit a LOW-severity note (keeps everyone honest without crying wolf).
            # - If direct _user/profile.json is present near a write, emit the HIGH-severity dual-writer warning.
            if _is_strong_t2g_line(t2g_line):
                code = "tier2g_write_outside_project_store"
                sev = "high"
            else:
                code = "tier2g_delegation_present"
                sev = "low"

            out.append(
                _warn_record(
                    rel=rel,
                    code=code,
                    severity=sev,
                    where=f"lines_{t2g_at+1}_{write_at+1}",
                    evidence=evidence,
                )
            )
            emitted += 1
            if emitted >= cap:
                break

    if not out:
        # Coarse fallback: keep it honest, but distinguish delegation-only from likely direct write risk.
        if ("/_user/" in low) or ("\\_user\\" in low) or ("profile.json" in low):
            out.append(
                _warn_record(
                    rel=rel,
                    code="tier2g_write_outside_project_store",
                    severity="high",
                    where="coarse_match",
                    evidence="Detected Tier-2G signals (profile.json/_user) near write primitives (write/open/json.dump) in this module.",
                )
            )
        else:
            out.append(
                _warn_record(
                    rel=rel,
                    code="tier2g_delegation_present",
                    severity="low",
                    where="coarse_match",
                    evidence="Detected Tier-2G-related delegation signals (e.g., user_profile_path) near write primitives in this module; no direct _user/profile.json path observed.",
                )
            )

    return out[:cap]

def _scan_phrase_locked_heuristics(rel: str, text: str, *, cap: int = 10) -> List[Dict[str, Any]]:
    """
    Detect "exact-phrase" heuristics over user text (common source of 'cheating' fixes):
    - re.search(r"...", low)
    - "..." in low
    - low.startswith("...")
    Emits bounded line-level evidence.
    """
    out: List[Dict[str, Any]] = []
    lines = (text or "").splitlines()

    # Heuristic: if it looks like natural-language phrase matching, flag it.
    rx_re_search_low = re.compile(r"re\.search\(\s*r?(['\"])(?P<pat>.+?)\1\s*,\s*low\b")
    rx_in_low = re.compile(r"(['\"])(?P<pat>.+?)\1\s+in\s+low\b")
    rx_startswith_low = re.compile(r"low\.startswith\(\s*(['\"])(?P<pat>.+?)\1")

    def _looks_like_phrase(p: str) -> bool:
        pl = (p or "").lower()
        if "\\b" in pl or "\\s" in pl:
            return True
        if " my " in f" {pl} " or " i " in f" {pl} ":
            return True
        if "name" in pl or "live" in pl or "birthday" in pl or "girlfriend" in pl or "boyfriend" in pl or "partner" in pl:
            return True
        # spaces are a strong indicator of phrase matching
        if " " in pl:
            return True
        return False

    for i, ln in enumerate(lines):
        s = ln.strip()
        if not s:
            continue

        m = rx_re_search_low.search(s)
        if m and _looks_like_phrase(m.group("pat")):
            out.append(_warn_record(rel=rel, code="phrase_locked_heuristic", severity="high", where=f"line_{i+1}", evidence=s))
            if len(out) >= cap:
                break
            continue

        m = rx_in_low.search(s)
        if m and _looks_like_phrase(m.group("pat")):
            out.append(_warn_record(rel=rel, code="phrase_locked_heuristic", severity="high", where=f"line_{i+1}", evidence=s))
            if len(out) >= cap:
                break
            continue

        m = rx_startswith_low.search(s)
        if m and _looks_like_phrase(m.group("pat")):
            out.append(_warn_record(rel=rel, code="phrase_locked_heuristic", severity="high", where=f"line_{i+1}", evidence=s))
            if len(out) >= cap:
                break
            continue

    return out[:cap]

def _scan_magic_word_intent_gates(rel: str, text: str, *, cap: int = 10) -> List[Dict[str, Any]]:
    """
    Upgrade a subset of phrase locks into a clearer category:
    - cases where normal requests are gated on polite/magic phrases ("can you", "could you", "i want you to", ...)
    This is the 'magic word syndrome' you want to stamp out.
    """
    out: List[Dict[str, Any]] = []
    lines = (text or "").splitlines()

    for i, ln in enumerate(lines):
        s = (ln or "").strip()
        if not s:
            continue
        low = s.lower()
        if (" in low" in low) and any(k in low for k in ("can you", "could you", "would you", "i want you to", "i need you to")):
            out.append(_warn_record(rel=rel, code="magic_word_intent_gate", severity="high", where=f"line_{i+1}", evidence=s))
            if len(out) >= cap:
                break

    return out[:cap]


def _scan_bloat_signals(rel: str, text: str, *, cap: int = 10) -> List[Dict[str, Any]]:
    """
    Flag common bloat patterns:
    - very long regex patterns
    - very large string tuples/lists on one line
    - long chains of ("... in low") conditions
    """
    out: List[Dict[str, Any]] = []
    lines = (text or "").splitlines()

    # long regex compile patterns
    rx_compile = re.compile(r"re\.compile\(\s*r?(['\"])(?P<pat>.+?)\1")
    for i, ln in enumerate(lines):
        s = (ln or "").strip()
        if not s:
            continue
        m = rx_compile.search(s)
        if m and len(m.group("pat") or "") >= 180:
            out.append(_warn_record(rel=rel, code="bloat_regex", severity="medium", where=f"line_{i+1}", evidence=s))
            if len(out) >= cap:
                return out[:cap]

    # large inline tuple/list of strings (cheap heuristic)
    for i, ln in enumerate(lines):
        s = (ln or "").strip()
        if len(s) < 60:
            continue
        # rough item count via quote occurrences
        items = (s.count("'") // 2) + (s.count('"') // 2)
        if items >= 20 and ("=(" in s.replace(" ", "") or "=[" in s.replace(" ", "")):
            out.append(_warn_record(rel=rel, code="bloat_phrase_list", severity="medium", where=f"line_{i+1}", evidence=s))
            if len(out) >= cap:
                return out[:cap]

    # long condition chains
    for i, ln in enumerate(lines):
        low = (ln or "").strip().lower()
        if " in low" in low and (" or " in low or " and " in low):
            if low.count(" in low") >= 6:
                out.append(_warn_record(rel=rel, code="bloat_condition_chain", severity="low", where=f"line_{i+1}", evidence=(ln or "").strip()))
                if len(out) >= cap:
                    return out[:cap]

    return out[:cap]


# (dedup) Removed earlier duplicate definitions of:
# - _scan_canonical_write_bypass
# - _scan_shadowed_entrypoints
# - _scan_duplicate_function_names_across_files
# Keep the later, canonical definitions below.

def _scan_exception_swallow_hotspots(rel: str, text: str, *, cap: int = 14) -> List[Dict[str, Any]]:
    """
    Flag silent exception swallowing hotspots.

    Why this matters:
    - Silent `except Exception: pass` is one of the highest drivers of "it failed and we don't know why".
    - We treat these as pointers for debugging and future cleanup.

    Deterministic (regex + indentation heuristics), bounded.
    """
    out: List[Dict[str, Any]] = []
    lines = (text or "").splitlines()

    # Skip scanning the registry generator itself for false positives? No:
    # registry_builder should also be held to this standard.
    # (We still cap output heavily.)

    # Match: except Exception [as e]:
    rx_except = re.compile(r"^\s*except\s+Exception(?:\s+as\s+\w+)?\s*:\s*$")

    def _indent(s: str) -> int:
        return len(s) - len(s.lstrip(" "))

    def _is_comment_or_blank(s: str) -> bool:
        st = (s or "").strip()
        return (not st) or st.startswith("#")

    # Heuristic scan:
    # When we see an `except Exception:` line, look forward for the first
    # nonblank/noncomment statement in that block and classify it.
    i = 0
    while i < len(lines):
        ln = lines[i]
        if not rx_except.match(ln):
            i += 1
            continue

        ex_lineno = i + 1
        ex_indent = _indent(ln)

        # Scan forward to find first statement inside the except block
        j = i + 1
        first_stmt = ""
        first_stmt_lineno = 0
        while j < len(lines):
            ln2 = lines[j]
            st2 = ln2.strip()
            if _is_comment_or_blank(ln2):
                j += 1
                continue

            ind2 = _indent(ln2)
            # Block ends when indent returns to <= except indent
            if ind2 <= ex_indent:
                break

            first_stmt = st2
            first_stmt_lineno = j + 1
            break

        sev = ""
        code = "exception_swallow_hotspot"
        if first_stmt in ("pass",):
            sev = "high"
            code = "exception_swallow_pass"
        elif first_stmt.startswith(("return", "continue", "break")):
            sev = "medium"
            code = "exception_swallow_control_flow"
        else:
            # If the handler neither logs nor raises, it's still a potential hotspot.
            # We treat obvious logging/raise/retry markers as less urgent.
            low = first_stmt.lower()
            if any(k in low for k in ("print(", "logger.", "logging.", "raise", "traceback", "audit_", "_audit_", "return _ws_send_safe", "await _ws_send_safe")):
                sev = "low"
                code = "exception_handled_non_silent"
            else:
                sev = "medium"
                code = "exception_swallow_non_logging"

        ev = f"except@{ex_lineno} then stmt@{first_stmt_lineno or ex_lineno}: {first_stmt or '(empty)'}"
        out.append(_warn_record(rel=rel, code=code, severity=sev, where=f"line_{ex_lineno}", evidence=ev))
        if len(out) >= cap:
            break

        i = j if j > i else (i + 1)

    return out[:cap]
def _scan_user_emit_sites(rel: str, text: str, *, cap: int = 18) -> List[Dict[str, Any]]:
    """
    Emit Site Map:
    Deterministically find user-facing "send" sites (WS/HTTP/message broadcast).

    This helps answer:
    - "Where did this greeting/dashboard message come from?"
    - "Where does project switching emit text?"
    """
    out: List[Dict[str, Any]] = []
    lines = (text or "").splitlines()

    # Keep patterns conservative and deterministic; these are internal APIs, not user text.
    pat = re.compile(
        r"""(?P<sig>websocket\.send\(|_ws_send_safe\(|broadcast_to_project\(|ws\.send\(|web\.Response\(|web\.json_response\()"""
    )

    for i, ln in enumerate(lines):
        s = (ln or "").strip()
        if not s:
            continue
        if pat.search(s):
            sig = pat.search(s).group("sig") if pat.search(s) else ""
            out.append(
                _warn_record(
                    rel=rel,
                    code="user_emit_site",
                    severity="low",
                    where=f"line_{i+1}",
                    evidence=f"{sig} {s}",
                )
            )
            if len(out) >= cap:
                break

    return out[:cap]


def _scan_decision_surface_keys(rel: str, text: str, *, cap: int = 24) -> List[Dict[str, Any]]:
    """
    Decision Surface Index:
    Deterministically find which internal keys can change behavior or audit interpretation.

    We record:
    - _audit_ctx_set("path.key", ...)
    - _audit_ctx_update({ ... }) top-level keys (best-effort)
    """
    out: List[Dict[str, Any]] = []
    lines = (text or "").splitlines()

    rx_set = re.compile(r"""_audit_ctx_set\(\s*(['"])(?P<key>[^'"]+)\1\s*,\s*""")
    rx_update = re.compile(r"""_audit_ctx_update\(\s*\{""")

    seen: set[str] = set()

    for i, ln in enumerate(lines):
        s = (ln or "").strip()
        if not s:
            continue

        m = rx_set.search(s)
        if m:
            k = (m.group("key") or "").strip()
            if k and k not in seen:
                seen.add(k)
                out.append(
                    _warn_record(
                        rel=rel,
                        code="decision_surface_key",
                        severity="low",
                        where=f"line_{i+1}",
                        evidence=f"_audit_ctx_set('{k}', ...)",
                    )
                )
                if len(out) >= cap:
                    break
            continue

        # Best-effort: note presence of an update block (we do not try to parse the dict fully here)
        if rx_update.search(s):
            tag = "audit_ctx_update_block"
            if tag not in seen:
                seen.add(tag)
                out.append(
                    _warn_record(
                        rel=rel,
                        code="decision_surface_key",
                        severity="low",
                        where=f"line_{i+1}",
                        evidence="_audit_ctx_update({ ... }) block present",
                    )
                )
                if len(out) >= cap:
                    break

    return out[:cap]
def _scan_multi_return_routing_risk(rel: str, text: str, *, cap: int = 8) -> List[Dict[str, Any]]:
    """
    Flag routing/context functions with many returns (often indicates dead paths / bypassed logic).
    Deterministic and bounded; intended as a pointer, not a proof.
    """
    out: List[Dict[str, Any]] = []
    lines = (text or "").splitlines()

    def _indent(s: str) -> int:
        return len(s) - len(s.lstrip(" "))

    suspect_tokens = ("build", "context", "route", "dispatch", "handle", "select", "router", "memory", "pipeline")

    i = 0
    while i < len(lines):
        st = lines[i].strip()
        if not st.startswith(("def ", "async def ")):
            i += 1
            continue

        header = st
        m = re.match(r"(async\s+def|def)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", header)
        fn = m.group(2) if m else ""
        fn_low = fn.lower()

        def_indent = _indent(lines[i])
        def_start = i
        i += 1

        body: List[str] = []
        while i < len(lines):
            ln = lines[i]
            s2 = ln.strip()
            if s2 and _indent(ln) <= def_indent and not s2.startswith(("#", "@")):
                break
            body.append(ln)
            i += 1

        if not fn or not any(t in fn_low for t in suspect_tokens):
            continue

        returns = 0
        for ln in body:
            s3 = ln.strip()
            if s3.startswith("return") or s3 == "return":
                returns += 1

        if returns >= 5:
            out.append(
                _warn_record(
                    rel=rel,
                    code="multi_return_routing_risk",
                    severity="medium",
                    where=f"def_line_{def_start+1}",
                    evidence=f"{fn} has return_count={returns}",
                )
            )
            if len(out) >= cap:
                break

    return out[:cap]

def _scan_duplicate_function_bodies_across_files(file_texts: Dict[str, str], *, cap: int = 60) -> List[Dict[str, Any]]:
    """
    Detect duplicated function bodies across different files.
    This catches the 'patched the wrong copy' failure mode.

    Approach:
    - Extract each def/async def body (indent-based)
    - Normalize (drop blank lines + comment-only lines, collapse whitespace)
    - Hash normalized body and find collisions across files
    """
    out: List[Dict[str, Any]] = []

    def _indent(s: str) -> int:
        return len(s) - len(s.lstrip(" "))

    def _strip_docstring(block_lines: List[str]) -> List[str]:
        # Remove leading docstring if present (best-effort; deterministic)
        bl = list(block_lines)
        # find first nonblank
        k = 0
        while k < len(bl) and not bl[k].strip():
            k += 1
        if k >= len(bl):
            return bl
        s0 = bl[k].strip()
        if s0.startswith(('"""', "'''")):
            q = s0[:3]
            # single-line docstring
            if s0.count(q) >= 2 and len(s0) > 5:
                return bl[:k] + bl[k+1:]
            # multi-line: skip until closing triple
            k2 = k + 1
            while k2 < len(bl):
                if q in bl[k2]:
                    return bl[:k] + bl[k2+1:]
                k2 += 1
            return bl  # unmatched; keep as-is
        return bl

    def _normalize(block_lines: List[str]) -> str:
        keep: List[str] = []
        for ln in _strip_docstring(block_lines):
            s = ln.strip()
            if not s:
                continue
            if s.startswith("#"):
                continue
            keep.append(" ".join(s.split()))
        return "\n".join(keep)

    # hash -> list of (rel, def_line, fn_name)
    seen: Dict[str, List[Tuple[str, int, str]]] = {}

    for rel in sorted(file_texts.keys()):
        text = file_texts.get(rel) or ""
        lines = text.splitlines()
        i = 0
        while i < len(lines):
            st = lines[i].strip()
            if not st.startswith(("def ", "async def ")):
                i += 1
                continue

            def_start = i
            def_indent = _indent(lines[i])
            m = re.match(r"(async\s+def|def)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", st)
            fn = m.group(2) if m else ""

            i += 1
            body_lines: List[str] = []
            while i < len(lines):
                ln = lines[i]
                s2 = ln.strip()
                if s2 and _indent(ln) <= def_indent and not s2.startswith(("#", "@")):
                    break
                body_lines.append(ln)
                i += 1

            norm = _normalize(body_lines)
            if len(norm) < 320:
                continue

            key = str(hash(norm))
            seen.setdefault(key, []).append((rel, def_start + 1, fn))

    # Emit collisions across distinct files
    emitted = 0
    for key in sorted(seen.keys()):
        occ = seen[key]
        files = sorted(set([r for (r, _, _) in occ]))
        if len(files) <= 1:
            continue

        # Build a short evidence list
        occ_sorted = sorted(occ, key=lambda t: (t[0], t[1], t[2]))
        evidence_bits = [f"{r}:{ln}:{fn or '<?>'}" for (r, ln, fn) in occ_sorted[:6]]
        evidence = "shared_body_hash across: " + ", ".join(evidence_bits)
        for (r, ln, fn) in occ_sorted:
            out.append(
                _warn_record(
                    rel=r,
                    code="duplicate_function_body_across_files",
                    severity="high",
                    where=f"def_line_{ln}",
                    evidence=evidence,
                )
            )
            emitted += 1
            if emitted >= cap:
                return out[:cap]

    return out[:cap]


def _scan_duplicate_function_names_across_files(file_texts: Dict[str, str], *, cap: int = 80) -> List[Dict[str, Any]]:
    """
    Detect same function name defined in multiple files (even if bodies drift).
    This catches 'near duplicates' where hashing won't match, but maintenance drift is guaranteed.
    """
    out: List[Dict[str, Any]] = []
    name_map: Dict[str, List[Tuple[str, int]]] = {}

    rx = re.compile(r"^\s*(async\s+def|def)\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.MULTILINE)

    for rel in sorted(file_texts.keys()):
        text = file_texts.get(rel) or ""
        for m in rx.finditer(text):
            fn = (m.group(2) or "").strip()
            if not fn or fn.startswith("__"):
                continue
            # best-effort line number
            line_no = int((text[: m.start()].count("\n") + 1) if text else 1)
            name_map.setdefault(fn, []).append((rel, line_no))

    emitted = 0
    for fn in sorted(name_map.keys()):
        occ = name_map[fn]
        files = sorted(set([r for (r, _ln) in occ]))
        if len(files) <= 1:
            continue

        # skip very common tiny names that may be intentionally repeated
        if fn in ("now_iso", "read_text_file", "read_bytes_file"):
            continue

        occ_sorted = sorted(occ, key=lambda t: (t[0], t[1]))
        evidence = "name defined in multiple files: " + ", ".join([f"{r}:{ln}" for (r, ln) in occ_sorted[:8]])
        for (r, ln) in occ_sorted[: min(len(occ_sorted), 12)]:
            out.append(
                _warn_record(
                    rel=r,
                    code="duplicate_function_name_across_files",
                    severity="medium",
                    where=f"def_line_{ln}",
                    evidence=f"{fn} — {evidence}",
                )
            )
            emitted += 1
            if emitted >= cap:
                return out[:cap]

    return out[:cap]


# (dedup) _scan_bloat_phrase_lists removed; use _scan_bloat_signals instead.


def _scan_shadowed_entrypoints(file_texts: Dict[str, str], *, cap: int = 6) -> List[Dict[str, Any]]:
    """
    Warn when a shim entrypoint exists alongside an active runtime server, a common patch trap.
    """
    out: List[Dict[str, Any]] = []
    keys = set(file_texts.keys())

    has_root = "server.py" in keys
    has_runtime = ("runtime/blue/server.py" in keys) or ("runtime/green/server.py" in keys)

    if not (has_root and has_runtime):
        return out

    root_text = file_texts.get("server.py") or ""
    low = root_text.lower()
    if "runpy.run_path" in low or "runpy" in low:
        out.append(
            _warn_record(
                rel="server.py",
                code="shadowed_entrypoint",
                severity="high",
                where="coarse_match",
                evidence="Root server.py appears to forward execution (runpy). Patch runtime/*/server.py for behavior changes.",
            )
        )
    else:
        out.append(
            _warn_record(
                rel="server.py",
                code="shadowed_entrypoint",
                severity="high",
                where="coarse_match",
                evidence="Both root server.py and runtime/*/server.py exist. Confirm which entrypoint is active before patching.",
            )
        )

    return out[:cap]


def _scan_canonical_write_bypass(rel: str, text: str, *, cap: int = 8) -> List[Dict[str, Any]]:
    """
    Detect direct writes into canonical truth stores (state/ and _user/) outside project_store.

    Notes:
    - Skip scanner/patch tooling to avoid self-matching on signal tables.
    """
    out: List[Dict[str, Any]] = []
    norm_rel = rel.replace("\\", "/")
    if norm_rel.endswith("project_store.py"):
        return out
    if norm_rel.endswith("registry_builder.py") or norm_rel.endswith("patch_engine.py"):
        return out

    lines = (text or "").splitlines()
    low = (text or "").lower()

    canon_signals = ("/_user/", "\\_user\\", "/state/", "\\state\\")
    if not any(s in low for s in canon_signals):
        return out

    write_signals = (
        ".write_text(",
        ".write_bytes(",
        "atomic_write_text",
        "atomic_write_bytes",
        "open(",
        ".write(",
        "json.dump(",
    )
    if not any(s in low for s in write_signals):
        return out

    def _is_canon_line(s: str) -> bool:
        sl = s.lower()
        return ("/_user/" in sl) or ("\\_user\\" in sl) or ("/state/" in sl) or ("\\state\\" in sl)

    def _is_write_line(s: str) -> bool:
        sl = s.lower()
        if "write_text" in sl or "write_bytes" in sl or "atomic_write_" in sl:
            return True
        if "json.dump" in sl or "json.dumps" in sl:
            return True
        if "open(" in sl and ("\"w" in sl or "'w" in sl or "mode=" in sl):
            return True
        if ".write(" in sl:
            return True
        return False

    window = 8
    emitted = 0
    for i, ln in enumerate(lines):
        s = (ln or "").strip()
        if not s:
            continue
        if not (_is_canon_line(s) or _is_write_line(s)):
            continue

        lo = max(0, i - window)
        hi = min(len(lines) - 1, i + window)
        canon_at = None
        write_at = None
        for j in range(lo, hi + 1):
            sj = (lines[j] or "").strip()
            if not sj:
                continue
            if canon_at is None and _is_canon_line(sj):
                canon_at = j
            if write_at is None and _is_write_line(sj):
                write_at = j
            if canon_at is not None and write_at is not None:
                break

        if canon_at is not None and write_at is not None:
            ev = f"canon@{canon_at+1}: {lines[canon_at].strip()} | write@{write_at+1}: {lines[write_at].strip()}"
            out.append(_warn_record(rel=rel, code="canonical_write_bypass", severity="high", where=f"lines_{canon_at+1}_{write_at+1}", evidence=ev))
            emitted += 1
            if emitted >= cap:
                break

    return out[:cap]


def _classify_phrase_lock(evidence: str, rel: str = "") -> str:
    """
    Classify phrase-locked heuristics into actionable buckets.
    This is intentionally blunt and deterministic (no model).
    """
    ev = (evidence or "").lower()
    r = (rel or "").lower()

    # control plane / exact commands
    if "ws.send" in ev or "switch project:" in ev or ev.strip().startswith(("!", "/")):
        return "ok_exact_control_plane"

    # memory gating (worst)
    if any(k in ev for k in ("my name is", "i go by", "my preferred name is", "i live in", "girlfriend", "boyfriend", "partner", "wife", "husband", "birthday")):
        return "risky_memory_gate"

    # intent gating (bad UX)
    if any(k in ev for k in ("can you", "could you", "would you", "i want you to", "i need you to")):
        return "risky_intent_gate"

    # safety/constraints (review, but sometimes acceptable)
    if any(k in r for k in ("constraint", "safety")) or any(k in ev for k in ("forbidden", "disallow", "blocked", "refuse")):
        return "ok_safety_gate"

    return "unknown"


def _prioritize_warning(code: str, severity: str) -> int:
    """
    Lower is more urgent. Order reflects your philosophy:
    1) dead code / unreachable
    2) canonical truth bypass (Tier-2G + state writes)
    3) magic-word syndrome (phrase locks)
    4) duplication drift
    5) bloat
    """
    c = (code or "").strip()
    if c == "unreachable_after_return":
        return 0
    if c in ("tier2g_write_outside_project_store", "canonical_write_bypass"):
        return 1
    if c == "tier2g_delegation_present":
        return 4
    if c == "magic_word_intent_gate":
        return 2
    if c == "phrase_locked_heuristic":
        return 3
    if c in ("exception_swallow_pass", "exception_swallow_control_flow", "exception_swallow_non_logging"):
        return 2    
    if c in ("duplicate_function_body_across_files", "duplicate_function_name_across_files"):
        return 3
    if c in ("bloat_phrase_list", "bloat_regex", "bloat_condition_chain"):
        return 4
    if c == "shadowed_entrypoint":
        return 5
    # everything else
    sev = (severity or "").lower()
    if sev == "high":
        return 6
    if sev == "medium":
        return 7
    return 8


def _build_registry_warnings(file_texts: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Aggregate warnings across repo files. Deterministic ordering and caps.
    """
    all_warns: List[Dict[str, Any]] = []

    # Per-file scans (stable ordering)
    for rel in sorted(file_texts.keys()):
        txt = file_texts.get(rel) or ""

        all_warns.extend(_scan_unreachable_after_return_ast(rel, txt, cap=12))
        all_warns.extend(_scan_duplicate_blocks_in_functions(rel, txt, cap=10))

        # Canonical truth bypass risks
        all_warns.extend(_scan_tier2g_writes_outside_project_store(rel, txt, cap=6))
        all_warns.extend(_scan_canonical_write_bypass(rel, txt, cap=8))

        # Magic-word syndrome (explicit)
        all_warns.extend(_scan_magic_word_intent_gates(rel, txt, cap=10))

        # Phrase locks (broader)
        all_warns.extend(_scan_phrase_locked_heuristics(rel, txt, cap=10))

        # Silent exception swallowing hotspots (debuggability killer)
        all_warns.extend(_scan_exception_swallow_hotspots(rel, txt, cap=14))

        # Continuity risk pointers
        all_warns.extend(_scan_multi_return_routing_risk(rel, txt, cap=8))

        # Bloat signals
        all_warns.extend(_scan_bloat_signals(rel, txt, cap=10))

        # Hard cap to keep output stable
        if len(all_warns) > 240:
            all_warns = all_warns[:240]
            break

    # Cross-file scans (only if we still have budget)
    if len(all_warns) < 240:
        remaining = 240 - len(all_warns)
        all_warns.extend(_scan_duplicate_function_bodies_across_files(file_texts, cap=min(60, remaining)))

    if len(all_warns) < 240:
        remaining = 240 - len(all_warns)
        all_warns.extend(_scan_duplicate_function_names_across_files(file_texts, cap=min(80, remaining)))

    if len(all_warns) < 240:
        remaining = 240 - len(all_warns)
        all_warns.extend(_scan_shadowed_entrypoints(file_texts, cap=min(6, remaining)))

    # Add phrase-lock classification (in-place; deterministic)
    for w in all_warns:
        try:
            if str(w.get("code") or "") == "phrase_locked_heuristic":
                w["phrase_lock_kind"] = _classify_phrase_lock(str(w.get("evidence") or ""), str(w.get("file") or ""))
        except Exception:
            pass

    # Deterministic sort: priority -> severity -> file -> code -> where
    sev_rank = {"high": 0, "medium": 1, "low": 2}
    all_warns.sort(
        key=lambda d: (
            _prioritize_warning(str(d.get("code") or ""), str(d.get("severity") or "")),
            sev_rank.get(str(d.get("severity") or "low"), 9),
            str(d.get("file") or ""),
            str(d.get("code") or ""),
            str(d.get("where") or ""),
        )
    )
    return all_warns[:240]

_TOKEN_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
VERSION_STAMP = "registry_builder:2026-01-29C"


def _read_text(path: Path) -> str:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        return f.read()


def _safe_rel(root: Path, p: Path) -> str:
    try:
        return str(p.resolve().relative_to(root.resolve())).replace("\\", "/")
    except Exception:
        return p.name


def _walk_py_files(root: Path) -> List[Path]:
    out: List[Path] = []
    root = root.resolve()

    for dirpath, dirnames, filenames in os.walk(root):
        dpath = Path(dirpath)
        rel_parts = _safe_rel(root, dpath).split("/")
        # prune ignored dirs anywhere in path
        if any(part in _IGNORE_DIRS for part in rel_parts):
            dirnames[:] = []
            continue

        # prune specific noisy path parts (e.g., runtime/**/logs)
        if any(part in _IGNORE_PATH_PARTS for part in rel_parts):
            dirnames[:] = []
            continue

        # prune in-place so os.walk doesn't descend
        dirnames[:] = [
            d for d in dirnames
            if d not in _IGNORE_DIRS
            and d not in _IGNORE_PATH_PARTS
            and not d.startswith(".")
        ]

        for fn in filenames:
            if fn.startswith("."):
                continue
            if not fn.lower().endswith(_PY_SUFFIX):
                continue
            out.append(dpath / fn)

    out.sort(key=lambda p: _safe_rel(root, p))
    return out


# -----------------------------
# Signature formatting (best-effort)
# -----------------------------

def _unparse(node: Optional[ast.AST]) -> str:
    if node is None:
        return ""
    try:
        # Python 3.9+
        return ast.unparse(node)  # type: ignore[attr-defined]
    except Exception:
        return ""


def _fmt_arg(arg: ast.arg) -> str:
    name = arg.arg
    ann = _unparse(arg.annotation)
    return f"{name}: {ann}" if ann else name


def _fmt_args(args: ast.arguments) -> str:
    parts: List[str] = []

    # positional-only (py3.8+)
    posonly = getattr(args, "posonlyargs", []) or []
    for a in posonly:
        parts.append(_fmt_arg(a))
    if posonly:
        parts.append("/")

    # regular args
    for a in args.args:
        parts.append(_fmt_arg(a))

    # vararg
    if args.vararg is not None:
        va = _fmt_arg(args.vararg)
        parts.append(f"*{va}")
    elif args.kwonlyargs:
        parts.append("*")

    # kw-only
    for a, default in zip(args.kwonlyargs, args.kw_defaults):
        s = _fmt_arg(a)
        if default is not None:
            s += f"={_unparse(default) or '...'}"
        parts.append(s)

    # kwarg
    if args.kwarg is not None:
        ka = _fmt_arg(args.kwarg)
        parts.append(f"**{ka}")

    # defaults for regular args (align to last N)
    if args.defaults:
        n = len(args.defaults)
        # find the slice of parts that corresponds to args.args (not posonly)
        # We patch defaults by walking backwards over args.args entries only.
        arg_names = [a.arg for a in args.args]
        # indexes in `parts` where those names appear (in order)
        idxs: List[int] = []
        for i, token in enumerate(parts):
            if token in arg_names:
                idxs.append(i)
        # apply defaults to last n args
        for j in range(1, n + 1):
            di = idxs[-j] if j <= len(idxs) else None
            if di is None:
                continue
            dnode = args.defaults[-j]
            dv = _unparse(dnode) or "..."
            parts[di] = parts[di] + f"={dv}"

    return ", ".join([p for p in parts if p])


# -----------------------------
# AST extraction per module
# -----------------------------

@dataclass
class TopSymbol:
    kind: str  # "function" | "async_function" | "class"
    name: str
    signature: str
    lineno: int


def _module_purpose(tree: ast.Module) -> str:
    doc = ast.get_docstring(tree) or ""
    first = (doc.splitlines()[0] if doc.splitlines() else "").strip()
    return first


def _extract_imports(tree: ast.Module) -> List[str]:
    out: List[str] = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.asname:
                    out.append(f"import {a.name} as {a.asname}")
                else:
                    out.append(f"import {a.name}")
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            lvl = int(getattr(node, "level", 0) or 0)
            dots = "." * lvl
            names = []
            for a in node.names:
                if a.asname:
                    names.append(f"{a.name} as {a.asname}")
                else:
                    names.append(a.name)
            out.append(f"from {dots}{mod} import {', '.join(names)}".strip())
    return out


def _extract_top_symbols(tree: ast.Module) -> List[TopSymbol]:
    out: List[TopSymbol] = []
    for node in tree.body:
        if isinstance(node, ast.FunctionDef):
            sig = f"({ _fmt_args(node.args) })"
            out.append(TopSymbol("function", node.name, sig, int(node.lineno or 0)))
        elif isinstance(node, ast.AsyncFunctionDef):
            sig = f"({ _fmt_args(node.args) })"
            out.append(TopSymbol("async_function", node.name, sig, int(node.lineno or 0)))
        elif isinstance(node, ast.ClassDef):
            # no attempt to infer __init__; just show class name
            out.append(TopSymbol("class", node.name, "", int(node.lineno or 0)))
    return out


def _extract_outbound_calls(tree: ast.Module, *, cap: int = 80) -> List[str]:
    calls: Dict[str, int] = {}

    class V(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call) -> Any:
            name = ""
            fn = node.func
            if isinstance(fn, ast.Name):
                name = fn.id
            elif isinstance(fn, ast.Attribute):
                # best-effort dotted chain
                parts: List[str] = []
                cur: ast.AST = fn
                while isinstance(cur, ast.Attribute):
                    parts.append(cur.attr)
                    cur = cur.value
                if isinstance(cur, ast.Name):
                    parts.append(cur.id)
                parts.reverse()
                name = ".".join(parts)
            if name:
                calls[name] = calls.get(name, 0) + 1
            self.generic_visit(node)

    V().visit(tree)

    # deterministic order: freq desc, name asc
    items = sorted(calls.items(), key=lambda kv: (-kv[1], kv[0]))
    return [k for k, _v in items[: int(cap or 80)]]


def _has_dunder_main(text: str) -> bool:
    return "__name__" in text and "__main__" in text and "if" in text

def _extract_aiohttp_routes(text: str) -> List[Dict[str, str]]:
    """
    Best-effort extraction of aiohttp route registrations like:
      app.router.add_get("/path", handler)
      app.router.add_post("/path", handler)

    This is regex-based to stay deterministic and stdlib-only.
    """
    out: List[Dict[str, str]] = []
    if not text:
        return out

    route_pat = re.compile(
        r"""app\.router\.add_(get|post|put|delete|patch|head|options)\(\s*(['"])(.*?)\2\s*,\s*([A-Za-z_][A-Za-z0-9_]*)""",
        re.IGNORECASE,
    )

    for m in route_pat.finditer(text):
        method = (m.group(1) or "").upper()
        path = m.group(3) or ""
        handler = m.group(4) or ""
        out.append({"method": method, "path": path, "handler": handler})

    # deterministic ordering
    out.sort(key=lambda r: (r.get("path", ""), r.get("method", ""), r.get("handler", "")))
    return out

def _detect_entry_points(rel_path: str, text: str) -> List[str]:
    entries: List[str] = []

    if _has_dunder_main(text):
        entries.append("__main__")

    low = (text or "").lower()

    # WS handler heuristic:
    # Require that the module both defines handle_connection AND passes it to websockets.serve(...)
    if "websockets.serve" in low and "handle_connection" in low:
        # extra guard: avoid false positives from registry_builder itself
        if "async def handle_connection" in low or "def handle_connection" in low:
            entries.append("ws_handler:handle_connection")

    # HTTP route registration heuristic:
    # Require a real register_routes() function AND router additions.
    if "def register_routes" in low and "app.router.add_" in low:
        entries.append("http_routes:register_routes")

    # WS command router
    if rel_path.endswith("ws_commands.py"):
        entries.append("ws_command_router:dispatch")

    return entries



# -----------------------------
# Inbound reference counting
# -----------------------------

def _token_counts(text: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for tok in _TOKEN_RE.findall(text or ""):
        counts[tok] = counts.get(tok, 0) + 1
    return counts


def _internal_name_refs_via_ast(tree: ast.Module) -> Dict[str, int]:
    """
    Count references to names within the same module, excluding the defining occurrence
    of top-level function/class names.

    This is deliberately conservative and stdlib-only.
    """
    defined_top: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if getattr(node, "name", None):
                defined_top.add(node.name)

    refs: Dict[str, int] = {}

    class V(ast.NodeVisitor):
        def visit_Name(self, node: ast.Name) -> Any:
            if node.id:
                refs[node.id] = refs.get(node.id, 0) + 1
            self.generic_visit(node)

        def visit_Attribute(self, node: ast.Attribute) -> Any:
            # Count attribute leaf as a "name-like" ref too (best-effort).
            # Example: foo.bar() counts "bar"
            if node.attr:
                refs[node.attr] = refs.get(node.attr, 0) + 1
            self.generic_visit(node)

        def visit_FunctionDef(self, node: ast.FunctionDef) -> Any:
            # Don't count the def name itself as a reference
            for dec in node.decorator_list:
                self.visit(dec)
            for d in node.args.defaults:
                self.visit(d)
            for d in node.args.kw_defaults:
                if d is not None:
                    self.visit(d)
            if node.returns is not None:
                self.visit(node.returns)
            # Visit body normally
            for b in node.body:
                self.visit(b)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> Any:
            return self.visit_FunctionDef(node)  # type: ignore[misc]

        def visit_ClassDef(self, node: ast.ClassDef) -> Any:
            # Don't count the class name itself as a reference
            for b in node.bases:
                self.visit(b)
            for kw in node.keywords:
                self.visit(kw)
            for dec in node.decorator_list:
                self.visit(dec)
            for b in node.body:
                self.visit(b)

    V().visit(tree)

    # Subtract one for each top-level definition to remove the "def NAME" / "class NAME" token hit
    for name in defined_top:
        if name in refs:
            refs[name] = max(0, int(refs.get(name, 0)) - 1)

    return refs


def build_system_registry(project_root: Path) -> Dict[str, Any]:
    """
    Returns a JSON-serializable registry dict.
    Deterministic for a given filesystem state.
    """
    root = Path(project_root).resolve()
    py_files = _walk_py_files(root)
    ui_files = _walk_ui_files(root)

    modules: Dict[str, Any] = {}
    symbols_index: Dict[str, Dict[str, Any]] = {}  # key: "rel_path::symbol" -> info
    file_defined_symbols: Dict[str, List[str]] = {}  # rel_path -> [symbol names]

    # Keep full file text for deterministic warning scans (bounded by repo walk already).
    file_texts: Dict[str, str] = {}

    # Pass 1: per-file AST extraction
    internal_ref_cache: Dict[str, Dict[str, int]] = {}

    for p in py_files:
        rel = _safe_rel(root, p)
        text = _read_text(p)
        file_texts[rel] = text or ""
        try:
            tree = ast.parse(text or "", filename=str(p))
        except Exception:
            tree = ast.parse("", filename=str(p))

        # Internal refs (within this same file), excluding the definition token itself
        internal_ref_cache[rel] = _internal_name_refs_via_ast(tree)

        purpose = _module_purpose(tree)
        imports = _extract_imports(tree)
        top = _extract_top_symbols(tree)
        outbound = _extract_outbound_calls(tree, cap=80)
        entry_points = _detect_entry_points(rel, text)
        aiohttp_routes = _extract_aiohttp_routes(text)

        file_defined_symbols[rel] = [s.name for s in top if s.name]

        modules[rel] = {
            "path": rel,
            "purpose": purpose,
            "imports": imports,
            "aiohttp_routes": aiohttp_routes,
            "top_level": [
                {
                    "kind": s.kind,
                    "name": s.name,
                    "signature": s.signature,
                    "lineno": s.lineno,
                }
                for s in top
            ],
            "outbound_calls_approx": outbound,
            "entry_points": entry_points,
        }

        for s in top:
            key = f"{rel}::{s.name}"
            symbols_index[key] = {
                "file": rel,
                "name": s.name,
                "kind": s.kind,
                "signature": s.signature,
                "lineno": s.lineno,
                "inbound_refs": 0,  # filled later
            }

    # Pass 2: refs split into internal + external
    token_cache: Dict[str, Dict[str, int]] = {}
    for p in py_files:
        rel = _safe_rel(root, p)
        token_cache[rel] = _token_counts(_read_text(p))

    for _sym_key, meta in symbols_index.items():
        sym = str(meta.get("name") or "")
        defining_file = str(meta.get("file") or "")

        # Internal refs: prefer AST-based counts; fallback to token-scan within same file.
        internal_ast = 0
        try:
            internal_ast = int((internal_ref_cache.get(defining_file) or {}).get(sym, 0))
        except Exception:
            internal_ast = 0

        internal_tok = 0
        try:
            # token count includes the definition token, so subtract 1 for the def/class itself
            internal_tok = max(0, int((token_cache.get(defining_file) or {}).get(sym, 0)) - 1)
        except Exception:
            internal_tok = 0

        internal_refs = internal_ast if internal_ast > 0 else internal_tok

        external_refs = 0
        for rel, counts in token_cache.items():
            if rel == defining_file:
                continue
            external_refs += int(counts.get(sym, 0))

        meta["internal_refs"] = internal_refs
        meta["external_refs"] = external_refs
        meta["inbound_refs"] = external_refs  # backward compatible field name
        meta["total_refs"] = int(internal_refs + external_refs)

    # file-level rollup (sum of TOTAL refs across symbols in that file)
    file_inbound: Dict[str, int] = {}
    for _sym_key, meta in symbols_index.items():
        f = str(meta.get("file") or "")
        file_inbound[f] = file_inbound.get(f, 0) + int(meta.get("total_refs") or 0)

    # dead suspects: symbols never referenced anywhere (internal + external), excluding:
    # - obvious entrypoints
    # - private helpers (leading "_") because token-scan false positives are common
    # - intentional dormant public APIs (kept for wiring/back-compat)
    dead: List[Dict[str, Any]] = []

    DEAD_EXCLUDE_NAMES = {
        # entrypoint-ish
        "main",
        "handle_connection",

        # intentional dormant/back-compat helpers (keep; not necessarily "dead")
        "get_registry",
        "retrieve_canonical_snippets",
        "links_for_decision",
        "links_for_upload",
        "links_for_deliverable",
        "summarize_links_for_domain",

        # legacy conflict helper (kept even if currently unused; safe to keep)
        "detect_decision_conflicts",
    }

    for _sym_key, meta in symbols_index.items():
        total = int(meta.get("total_refs") or 0)
        name = str(meta.get("name") or "")
        kind = str(meta.get("kind") or "")

        if total != 0:
            continue

        # heuristics: ignore dunder + common entrypoint-ish names
        if name.startswith("__") and name.endswith("__"):
            continue

        # Private helpers: do not mark as dead suspects (too many false positives)
        if name.startswith("_"):
            continue

        # Known intentionally-kept APIs
        if name in DEAD_EXCLUDE_NAMES:
            continue

        # Exception types
        if kind == "class" and name.endswith("Error"):
            continue

        dead.append(
            {
                "file": meta.get("file"),
                "name": name,
                "kind": meta.get("kind"),
                "signature": meta.get("signature"),
                "lineno": meta.get("lineno"),
            }
        )

    # deterministic ordering
    dead.sort(key=lambda d: (str(d.get("file") or ""), str(d.get("name") or "")))

    # entrypoints rollup
    entry_points: List[Dict[str, Any]] = []
    for rel, m in modules.items():
        eps = m.get("entry_points") or []
        if eps:
            entry_points.append({"file": rel, "entry_points": list(eps)})

    entry_points.sort(key=lambda x: str(x.get("file") or ""))

    ui_warnings: List[Dict[str, Any]] = []
    ui_entry_points: List[Dict[str, Any]] = []
    for p in ui_files:
        rec = _ui_entrypoint_record(root, p)
        ui_entry_points.append(rec)

        # UI warnings: startup churn + duplicate defs (deterministic)
        try:
            ui_text = _read_text(p)
            rel_ui = str(rec.get("path") or _safe_rel(root, p))

            # Startup flow risks
            for w in _scan_ui_startup_flow_risks(ui_text, cap=30):
                w["file"] = rel_ui  # attribute to real UI file
                ui_warnings.append(w)

            # Duplicate JS function defs
            dups = rec.get("ui_js_duplicate_function_defs") if isinstance(rec.get("ui_js_duplicate_function_defs"), list) else []
            if dups:
                ui_warnings.append(
                    _warn_record(
                        rel=rel_ui,
                        code="ui_duplicate_js_function_defs",
                        severity="high",
                        where="coarse_match",
                        evidence="duplicate function defs: " + ", ".join([str(x) for x in dups[:24]]),
                    )
                )
        except Exception:
            pass

    ui_entry_points.sort(key=lambda d: str(d.get("path") or ""))
    # Flatten UI emit sites (diagnostic)
    ui_emit_sites: List[Dict[str, Any]] = []
    ui_emit_sites_ws_onopen: List[Dict[str, Any]] = []
    try:
        for it in ui_entry_points:
            p = str(it.get("path") or "")
            for e in (it.get("ui_emit_sites") if isinstance(it.get("ui_emit_sites"), list) else [])[:120]:
                rec = dict(e)
                rec["file"] = p
                ui_emit_sites.append(rec)
            for e in (it.get("ui_emit_sites_ws_onopen") if isinstance(it.get("ui_emit_sites_ws_onopen"), list) else [])[:80]:
                rec = dict(e)
                rec["file"] = p
                ui_emit_sites_ws_onopen.append(rec)

        # Hard caps
        if len(ui_emit_sites) > 180:
            ui_emit_sites = ui_emit_sites[:180]
        if len(ui_emit_sites_ws_onopen) > 140:
            ui_emit_sites_ws_onopen = ui_emit_sites_ws_onopen[:140]
    except Exception:
        ui_emit_sites = ui_emit_sites[:180]
        ui_emit_sites_ws_onopen = ui_emit_sites_ws_onopen[:140]
    # warnings (deterministic)
    warnings: List[Dict[str, Any]] = []
    try:
        warnings = _build_registry_warnings(file_texts)
    except Exception:
        warnings = []

    # Merge UI warnings (startup churn + duplicate defs) into the same actionable stream
    try:
        if ui_warnings:
            warnings.extend(ui_warnings)
    except Exception:
        pass

    # ------------------------------------------------------------
    # Emit Site Map + Decision Surface Index (deterministic)
    # These are NOT "warnings"; they are debug surfaces.
    # ------------------------------------------------------------
    user_emit_sites: List[Dict[str, Any]] = []
    decision_surface_keys: List[Dict[str, Any]] = []

    try:
        for rel in sorted(file_texts.keys()):
            txt = file_texts.get(rel) or ""
            user_emit_sites.extend(_scan_user_emit_sites(rel, txt, cap=18))
            decision_surface_keys.extend(_scan_decision_surface_keys(rel, txt, cap=24))

            # Hard caps to keep registry stable
            if len(user_emit_sites) > 120:
                user_emit_sites = user_emit_sites[:120]
            if len(decision_surface_keys) > 160:
                decision_surface_keys = decision_surface_keys[:160]
    except Exception:
        user_emit_sites = user_emit_sites[:120]
        decision_surface_keys = decision_surface_keys[:160]

    # registry
    # Include debug surfaces (bounded)    
    registry: Dict[str, Any] = {
        "version": 1,
        "generator": VERSION_STAMP,
        "project_root": str(root),
        "module_count": len(modules),
        "modules": modules,
        "ui_entry_points": ui_entry_points,
        "symbols": symbols_index,
        "inbound_refs_by_file": file_inbound,
        "entry_points": entry_points,
        "dead_suspects": dead[:400],  # cap for size
        "warnings": warnings,
        "user_emit_sites": user_emit_sites[:120],
        "ui_emit_sites": ui_emit_sites[:180],
        "ui_emit_sites_ws_onopen": ui_emit_sites_ws_onopen[:140],        
        "decision_surface_keys": decision_surface_keys[:160],        
    }
    return registry

def registry_notes_template(reg: Dict[str, Any]) -> str:
    """
    Human-maintained companion file template.
    This is the semantic layer that explains intent, invariants, and known issues.
    """
    modules = reg.get("modules") if isinstance(reg.get("modules"), dict) else {}
    entry_points = reg.get("entry_points") if isinstance(reg.get("entry_points"), list) else []

    lines: List[str] = []
    lines.append("# System Registry Notes")
    lines.append("")
    lines.append("This file is **human-maintained** and is meant to make any new chat immediately effective.")
    lines.append("")
    lines.append("## How to use")
    lines.append("- Keep this short and blunt.")
    lines.append("- If a bug recurs, add it under the owning file with: **symptom -> likely cause -> where to look first**.")
    lines.append("- Record invariants (things that must never change) and why.")
    lines.append("")
    lines.append("## Global invariants")
    lines.append("- (add invariants here)")
    lines.append("")
    lines.append("## Entry points (from registry)")
    if not entry_points:
        lines.append("- (none detected)")
    else:
        for ep in entry_points:
            f = str(ep.get("file") or "")
            eps = ep.get("entry_points") or []
            lines.append(f"- **{f}**: " + ", ".join([str(x) for x in eps if str(x)]))
    lines.append("")

    lines.append("## Per-file notes")
    for rel in sorted(modules.keys()):
        m = modules.get(rel) or {}
        purpose = str(m.get("purpose") or "").strip()
        lines.append(f"### {rel}")
        lines.append(f"- Purpose (human): TODO")
        lines.append(f"- Registry purpose (docstring first line): {purpose or '(missing)'}")
        lines.append("- Owns:")
        lines.append("  - TODO")
        lines.append("- Debug first:")
        lines.append("  - TODO (function names / routes / ws commands)")
        lines.append("- Known issues:")
        lines.append("  - TODO (symptom -> likely cause -> where to look)")
        lines.append("- Invariants / do-not-break:")
        lines.append("  - TODO")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"

def registry_to_markdown(reg: Dict[str, Any]) -> str:
    modules = reg.get("modules") if isinstance(reg.get("modules"), dict) else {}
    dead = reg.get("dead_suspects") if isinstance(reg.get("dead_suspects"), list) else []
    entry_points = reg.get("entry_points") if isinstance(reg.get("entry_points"), list) else []
    inbound_by_file = reg.get("inbound_refs_by_file") if isinstance(reg.get("inbound_refs_by_file"), dict) else {}
    warnings = reg.get("warnings") if isinstance(reg.get("warnings"), list) else []

    lines: List[str] = []
    lines.append("# System Registry (v1)")
    lines.append("")
    lines.append(f"- Modules: {int(reg.get('module_count') or 0)}")
    lines.append(f"- Dead suspects (cap 400): {len(dead)}")
    pr = str(reg.get("project_root") or "").strip()
    if pr:
        lines.append(f"- Scan root: `{pr}`")

    missing_doc = []
    for rel in sorted(modules.keys()):
        m = modules.get(rel) or {}
        if not str(m.get("purpose") or "").strip():
            missing_doc.append(rel)
    if missing_doc:
        lines.append(f"- Missing module docstrings: {len(missing_doc)}")

    lines.append("")
    if missing_doc:
        lines.append("## Missing module docstrings")
        for rel in missing_doc[:80]:
            lines.append(f"- {rel}")
        lines.append("")
    # Warnings (actionable hygiene)
    lines.append("## Warnings")
    if not warnings:
        lines.append("- (none detected)")
    else:
        # show top warnings first; cap in the markdown too
        for w in warnings[:120]:
            f = str(w.get("file") or "")
            code = str(w.get("code") or "")
            sev = str(w.get("severity") or "")
            where = str(w.get("where") or "")
            ev = str(w.get("evidence") or "")
            kind = str(w.get("phrase_lock_kind") or "")

            suffix = f" (kind={kind})" if (code == "phrase_locked_heuristic" and kind) else ""
            lines.append(f"- [{sev}] `{code}` — {f}:{where}{suffix}")
            if ev:
                lines.append(f"  - Evidence: {ev}")
    lines.append("")

    # Auto-ranked patch plan
    lines.append("## Patch Plan (auto-ranked)")
    if not warnings:
        lines.append("- (no warnings; nothing to patch)")
    else:
        buckets = [
            ("unreachable_after_return", "Fix unreachable-after-return in core flows first (removes phantom logic)."),

            # UI startup / duplicate firing diagnostics (high leverage for your current issue)
            ("ui_duplicate_js_function_defs", "Fix duplicate JS function definitions in UI (silent override → double firing)."),
            ("ui_startup_multiple_thread_get", "Fix UI startup churn: multiple thread.get triggers in ws.onopen window."),
            ("ui_startup_switch_plus_thread_get", "Fix UI startup churn: restore switch + thread.get overlap (double-load/greeting churn)."),
            ("ui_ws_onopen_multiple_assignments", "Fix UI startup churn: ws.onopen assigned multiple times."),

            ("tier2g_write_outside_project_store", "Remove Tier-2G dual-writer paths (route through project_store)."),
            ("canonical_write_bypass", "Eliminate direct writes into _user/ or state/ outside canonical writers."),
            ("magic_word_intent_gate", "Remove magic-word gates on normal requests (intent must not require polite phrases)."),
            ("phrase_locked_heuristic", "Replace phrase-lock gates with real heuristics / slot extraction."),
            ("duplicate_function_body_across_files", "Deduplicate identical bodies across files (patch-the-wrong-copy trap)."),
            ("duplicate_function_name_across_files", "Deduplicate or consolidate same-named defs across files (drift trap)."),
            ("bloat_phrase_list", "Reduce large phrase lists/tuples; consolidate or refactor."),
            ("bloat_regex", "Split or simplify overlong regexes."),
            ("bloat_condition_chain", "Collapse long condition chains into data-driven structures."),
            ("shadowed_entrypoint", "Confirm active entrypoint; patch runtime/*/server.py not root shim."),
        ]
        # Count by code
        counts: Dict[str, int] = {}
        for w in warnings:
            c = str(w.get("code") or "")
            counts[c] = counts.get(c, 0) + 1

        shown = 0
        for code, note in buckets:
            n = int(counts.get(code, 0) or 0)
            if n <= 0:
                continue
            lines.append(f"- **{code}**: {n} findings — {note}")
            shown += 1
            if shown >= 12:
                break
        if shown == 0:
            lines.append("- (no recognized patch buckets)")
    lines.append("")

    # Patchability index (per-file quick triage)
    lines.append("## Patchability index (per file)")
    if not warnings:
        lines.append("- (no warnings; patchability is clean)")
    else:
        # file -> counts by warning
        per: Dict[str, Dict[str, int]] = {}
        for w in warnings:
            f = str(w.get("file") or "")
            c = str(w.get("code") or "")
            per.setdefault(f, {})
            per[f][c] = per[f].get(c, 0) + 1

        # score: unreachable*5 + canonical*4 + phrase*3 + dup*2 + bloat*1
        def _score(d: Dict[str, int]) -> int:
            return (
                5 * int(d.get("unreachable_after_return", 0) or 0)
                + 4 * (int(d.get("tier2g_write_outside_project_store", 0) or 0) + int(d.get("canonical_write_bypass", 0) or 0))
                + 3 * (int(d.get("magic_word_intent_gate", 0) or 0) + int(d.get("phrase_locked_heuristic", 0) or 0))
                + 2 * (int(d.get("duplicate_function_body_across_files", 0) or 0) + int(d.get("duplicate_function_name_across_files", 0) or 0))
                + 1 * (int(d.get("bloat_phrase_list", 0) or 0) + int(d.get("bloat_regex", 0) or 0) + int(d.get("bloat_condition_chain", 0) or 0))
            )

        items = sorted(per.items(), key=lambda kv: (-_score(kv[1]), kv[0]))[:40]

        lines.append("| file | score | unreachable | canonical bypass | phrase locks | duplicates | bloat |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        for f, d in items:
            score = _score(d)
            unreachable = int(d.get("unreachable_after_return", 0) or 0)
            canonical = int(d.get("tier2g_write_outside_project_store", 0) or 0) + int(d.get("canonical_write_bypass", 0) or 0)
            phrase = int(d.get("phrase_locked_heuristic", 0) or 0)
            dup = int(d.get("duplicate_function_body_across_files", 0) or 0) + int(d.get("duplicate_function_name_across_files", 0) or 0)
            bloat = int(d.get("bloat_phrase_list", 0) or 0) + int(d.get("bloat_regex", 0) or 0) + int(d.get("bloat_condition_chain", 0) or 0)
            lines.append(f"| {f} | {score} | {unreachable} | {canonical} | {phrase} | {dup} | {bloat} |")
    lines.append("")

    lines.append("## Companion notes")
    lines.append("- `system_registry_notes.md` lives alongside this registry in the same artifacts folder.")
    lines.append("- Keep it updated with per-file purpose, known issues, invariants, and debug-first pointers.")
    lines.append("")
    # Emit sites (debug map)
    emit_sites = reg.get("user_emit_sites") if isinstance(reg.get("user_emit_sites"), list) else []
    lines.append("## Emit site map (where user-facing text is sent)")
    if not emit_sites:
        lines.append("- (none detected)")
    else:
        for it in emit_sites[:120]:
            f = str(it.get("file") or "")
            where = str(it.get("where") or "")
            ev = str(it.get("evidence") or "")
            lines.append(f"- `{f}:{where}` — {ev}")
    lines.append("")

    # Decision surface keys (debug map)
    dkeys = reg.get("decision_surface_keys") if isinstance(reg.get("decision_surface_keys"), list) else []
    lines.append("## Decision surface index (audit ctx keys that can affect behavior)")
    if not dkeys:
        lines.append("- (none detected)")
    else:
        for it in dkeys[:160]:
            f = str(it.get("file") or "")
            where = str(it.get("where") or "")
            ev = str(it.get("evidence") or "")
            lines.append(f"- `{f}:{where}` — {ev}")
    lines.append("")

    # UI entry points
    ui_eps = reg.get("ui_entry_points") if isinstance(reg.get("ui_entry_points"), list) else []
    lines.append("## UI entry points")
    if not ui_eps:
        lines.append("- (none detected)")
    else:
        for it in ui_eps:
            p = str(it.get("path") or "")
            lines.append(f"- **{p}**")
            ws_contract = it.get("ws_contract") if isinstance(it.get("ws_contract"), dict) else {}
            if ws_contract:
                fv = int(ws_contract.get("frame_version") or 0)
                ping = str(ws_contract.get("ping") or "")
                pong = str(ws_contract.get("pong") or "")
                lines.append(f"  - WS framing: v={fv}" if fv else "  - WS framing: (unknown)")
                if ping or pong:
                    lines.append(f"  - Heartbeat: {ping or '(none)'} / {pong or '(none)'}")
            cmds = it.get("ws_commands_seen") if isinstance(it.get("ws_commands_seen"), list) else []
            if cmds:
                lines.append("  - WS commands seen:")
                for c in cmds[:40]:
                    lines.append(f"    - `{c}`")

            pats = it.get("ws_command_patterns") if isinstance(it.get("ws_command_patterns"), list) else []
            if pats:
                lines.append("  - WS command patterns:")
                for p in pats[:40]:
                    lines.append(f"    - `{p}`")

            eps = it.get("http_endpoints_used") if isinstance(it.get("http_endpoints_used"), list) else []
            if eps:
                lines.append("  - HTTP endpoints used:")
                for e in eps[:60]:
                    lines.append(f"    - `{e}`")
    lines.append("")
    # UI emit sites (diagnostic)
    ui_es = reg.get("ui_emit_sites") if isinstance(reg.get("ui_emit_sites"), list) else []
    ui_es_on = reg.get("ui_emit_sites_ws_onopen") if isinstance(reg.get("ui_emit_sites_ws_onopen"), list) else []

    lines.append("## UI emit sites (diagnostic)")
    if not ui_es:
        lines.append("- (none detected)")
    else:
        for it in ui_es[:180]:
            f = str(it.get("file") or "")
            ln = int(it.get("line") or 0)
            kind = str(it.get("kind") or "")
            ev = str(it.get("evidence") or "")
            lines.append(f"- `{f}:line_{ln}` — {kind} — {ev}")
    lines.append("")

    lines.append("## UI emit sites in ws.onopen window (startup)")
    if not ui_es_on:
        lines.append("- (none detected)")
    else:
        for it in ui_es_on[:140]:
            f = str(it.get("file") or "")
            ln = int(it.get("line") or 0)
            kind = str(it.get("kind") or "")
            ev = str(it.get("evidence") or "")
            lines.append(f"- `{f}:line_{ln}` — {kind} — {ev}")
    lines.append("")
    # Entry points
    lines.append("## Entry points (heuristic)")
    if not entry_points:
        lines.append("- (none detected)")
    else:
        for ep in entry_points:
            f = str(ep.get("file") or "")
            eps = ep.get("entry_points") or []
            lines.append(f"- **{f}**: " + ", ".join([str(x) for x in eps if str(x)]))
    lines.append("")

    # Modules summary
    lines.append("## Modules")
    for rel in sorted(modules.keys()):
        m = modules.get(rel) or {}
        purpose = str(m.get("purpose") or "").strip()
        lines.append(f"### {rel}")
        lines.append(f"- Purpose: {purpose or '(missing docstring)'}")
        lines.append(f"- Inbound refs (all symbols): {int(inbound_by_file.get(rel, 0) or 0)}")

        # Imports
        imps = m.get("imports") or []
        if imps:
            lines.append("- Imports:")
            for imp in imps[:40]:
                lines.append(f"  - `{imp}`")
        else:
            lines.append("- Imports: (none)")
        # Routes (aiohttp)
        routes = m.get("aiohttp_routes") or []
        if routes:
            lines.append("- HTTP routes (aiohttp, extracted):")
            for r in routes[:60]:
                meth = str(r.get("method") or "").upper()
                path = str(r.get("path") or "")
                h = str(r.get("handler") or "")
                lines.append(f"  - `{meth} {path}` -> `{h}`")
        else:
            lines.append("- HTTP routes (aiohttp): (none detected)")

        # Top-level
        top = m.get("top_level") or []
        if top:
            lines.append("- Top-level:")
            for s in top:
                kind = str(s.get("kind") or "")
                name = str(s.get("name") or "")
                sig = str(s.get("signature") or "")
                ln = int(s.get("lineno") or 0)
                if sig:
                    lines.append(f"  - `{kind} {name}{sig}` (line {ln})")
                else:
                    lines.append(f"  - `{kind} {name}` (line {ln})")
        else:
            lines.append("- Top-level: (none)")

        # Outbound calls
        oc = m.get("outbound_calls_approx") or []
        if oc:
            lines.append("- Outbound calls (approx, top 25):")
            for c in oc[:25]:
                lines.append(f"  - `{c}`")
        else:
            lines.append("- Outbound calls (approx): (none)")

        lines.append("")

    # Dead suspects
    lines.append("## Dead suspects (never referenced anywhere: internal + external)")
    if not dead:
        lines.append("- (none)")
    else:
        # If we have symbol-level ref stats, show them for auditability.
        sym_index = reg.get("symbols") if isinstance(reg.get("symbols"), dict) else {}
        for d in dead[:200]:
            f = str(d.get("file") or "")
            name = str(d.get("name") or "")
            kind = str(d.get("kind") or "")
            sig = str(d.get("signature") or "")
            ln = int(d.get("lineno") or 0)

            internal = external = total = None
            try:
                meta = sym_index.get(f"{f}::{name}") if isinstance(sym_index, dict) else None
                if isinstance(meta, dict):
                    internal = meta.get("internal_refs")
                    external = meta.get("external_refs")
                    total = meta.get("total_refs")
            except Exception:
                pass

            if internal is not None or external is not None or total is not None:
                lines.append(
                    f"- `{kind} {name}{sig}` — {f}:{ln} (internal={int(internal or 0)}, external={int(external or 0)}, total={int(total or 0)})"
                )
            else:
                lines.append(f"- `{kind} {name}{sig}` — {f}:{ln}")

    lines.append("")
    return "\n".join(lines).rstrip() + "\n"


if __name__ == "__main__":
    # CLI usage (optional, deterministic):
    #   python registry_builder.py <project_root> <out_json> <out_md>
    import sys

    root = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else Path(os.getcwd()).resolve()
    out_json = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else (root / "system_registry_v1.json")
    out_md = Path(sys.argv[3]).resolve() if len(sys.argv) > 3 else (root / "system_registry_v1.md")

    reg = build_system_registry(root)
    md = registry_to_markdown(reg)

    out_json.write_text(json.dumps(reg, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    out_md.write_text(md, encoding="utf-8")

    print(str(out_json))
    print(str(out_md))
