import argparse
import ast
import hashlib
import json
import os
import re

import registry_builder

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
NOTES_DIR = os.path.join(ROOT, "notes")
METRICS_PATH = os.path.join(NOTES_DIR, "METRICS.json")
METRICS_DIFF_PATH = os.path.join(NOTES_DIR, "METRICS.diff.md")
DIAGNOSIS_PATH = os.path.join(NOTES_DIR, "DIAGNOSIS.md")
GUARDRAILS_PATH = os.path.join(NOTES_DIR, "GUARDRAILS.md")
REMOVAL_CANDIDATES_PATH = os.path.join(NOTES_DIR, "REMOVAL_CANDIDATES.md")
PATCH_PLAN_PATH = os.path.join(NOTES_DIR, "PATCH_PLAN.md")


IGNORE_DIRS = {
    ".git",
    "__pycache__",
    ".venv",
    "venv",
    ".pytest_cache",
    ".mypy_cache",
    "node_modules",
    "dist",
    "build",
    "logs",
    "artifacts",
    "benchmarks",
    "lens0_data",
    "PDF",
    "uploads",
    "maxims",
    "patches",
    "notes",
}


MAGIC_GATE_PATTERNS = [
    re.compile(r"\bcan you\b", re.IGNORECASE),
    re.compile(r"\bi want you to\b", re.IGNORECASE),
    re.compile(r"\bcould you\b", re.IGNORECASE),
]


USER_PATH_PATTERN = re.compile(r"[\\/]_user[\\/]", re.IGNORECASE)
USER_LITERAL_PATTERN = re.compile(r"_user", re.IGNORECASE)
DEF_PATTERN = re.compile(r"(?m)^\s*def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(")
CLASS_PATTERN = re.compile(r"(?m)^\s*class\s+([A-Za-z_][A-Za-z0-9_]*)\s*[\(:]")
CONDITION_HINTS = ("if ", "elif ", "match ", "re.search", "re.match", " in ")
STRONG_ROUTING_MARKERS = (
    "if ",
    "elif ",
    "re.search",
    "re.match",
    "intent",
    "route",
    "continuation",
    "classify",
)
PROTECTED_FILES = ("server.py", "project_store.py", "model_pipeline.py")


def _ensure_notes_dir() -> None:
    if not os.path.isdir(NOTES_DIR):
        os.makedirs(NOTES_DIR, exist_ok=True)


def _iter_files(root: str) -> list:
    files = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in IGNORE_DIRS]
        for name in filenames:
            if name.endswith(".pyc"):
                continue
            files.append(os.path.join(dirpath, name))
    return files


_read_text = registry_builder._read_text


def _file_loc(text: str) -> int:
    if not text:
        return 0
    return len(text.splitlines())


def _hash_text(text: str) -> str:
    h = hashlib.sha256()
    h.update((text or "").encode("utf-8", errors="ignore"))
    return h.hexdigest()


def _scan_metrics(_prev_metrics: dict | None = None) -> dict:
    files = _iter_files(ROOT)
    metrics = {
        "root": ROOT,
        "files": {},
        "protected_files": {},
        "notes": {},
        "top_10_largest": [],
        "risk": {
            "except_pass": {},
            "direct_user_writes": {},
            "magic_word_gates": {},
            "duplicate_functions": {},
            "parse_errors": {},
        },
    }

    file_sizes = []
    def_index = {}

    for path in files:
        rel = os.path.relpath(path, ROOT)
        try:
            size = os.path.getsize(path)
        except OSError:
            size = 0

        text = _read_text(path)
        parsed = None
        loc = _file_loc(text)
        metrics["files"][rel] = {
            "loc": int(loc),
            "size": int(size),
            "sha256": _hash_text(text),
        }
        file_sizes.append((rel, size))

        if path.endswith(".py"):
            parsed = _parse_python(text, path)
            if parsed is None:
                metrics["risk"]["parse_errors"][rel] = "ast.parse failed"
            else:
                except_hits = _detect_except_pass_ast(parsed, text)
                if except_hits:
                    metrics["risk"]["except_pass"][rel] = except_hits

                write_hits = _detect_user_writes_ast(parsed, text)
                if write_hits:
                    metrics["risk"]["direct_user_writes"][rel] = write_hits

                defs = _collect_top_level_defs(parsed)
                if defs:
                    for name, line in defs:
                        def_index.setdefault(name, []).append(
                            {"file": rel, "line": int(line)}
                        )

        magic_hits = _detect_magic_gates(path, text, parsed)
        if magic_hits:
            metrics["risk"]["magic_word_gates"][rel] = magic_hits

    file_sizes.sort(key=lambda x: x[1], reverse=True)
    metrics["top_10_largest"] = [
        {"path": rel, "size": int(size)} for rel, size in file_sizes[:10]
    ]
    metrics["risk"]["duplicate_functions"] = _build_duplicate_defs(def_index)
    for fname in PROTECTED_FILES:
        if fname in metrics["files"]:
            metrics["protected_files"][fname] = metrics["files"][fname]
    metrics["notes"]["patch_plan"] = _scan_patch_plan_meta()
    return metrics


def _detect_magic_gates(path: str, text: str, parsed: ast.AST | None) -> list:
    # Detector: flags intent-gate phrases only when used in routing conditions (AST),
    # and a secondary text heuristic for conditional/routing lines outside docstrings/comments.
    hits = []
    if parsed is not None and path.endswith(".py"):
        hits.extend(_detect_magic_gates_ast(parsed, text))
        doc_ranges = _get_docstring_ranges(parsed)
        comment_lines = _get_comment_lines(text)
        hits.extend(
            _detect_magic_gates_text(text, doc_ranges, comment_lines, condition_only=True)
        )
    else:
        hits.extend(_detect_magic_gates_text(text, [], set(), condition_only=True))
    return hits


def _detect_magic_gates_ast(tree: ast.AST, text: str) -> list:
    # Detector: flags If/While test expressions containing the target phrases;
    # does not scan docstrings, templates, or non-conditional text.
    hits = []
    lines = text.splitlines()

    def _match_phrase(value: str) -> bool:
        return any(rx.search(value or "") for rx in MAGIC_GATE_PATTERNS)

    def _extract_strings(expr: ast.AST) -> list[str]:
        out = []
        for node in ast.walk(expr):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                out.append(node.value)
        return out

    def _test_has_phrase(expr: ast.AST) -> bool:
        for s in _extract_strings(expr):
            if _match_phrase(s):
                return True
        return False

    for node in ast.walk(tree):
        if isinstance(node, (ast.If, ast.While)):
            test = node.test
            if _test_has_phrase(test):
                lineno = getattr(node, "lineno", 0)
                evidence = lines[lineno - 1].strip()[:200] if 1 <= lineno <= len(lines) else ""
                hits.append(
                    {
                        "line": int(lineno),
                        "kind": "ast-condition",
                        "reason": "condition contains intent-gate phrase",
                        "evidence": evidence,
                        "strong": True,
                    }
                )
    return hits


def _detect_magic_gates_text(
    text: str,
    doc_ranges: list[tuple[int, int]],
    comment_lines: set[int],
    condition_only: bool,
) -> list:
    # Detector: text scan for target phrases on routing-like lines; ignores comments/docstrings
    # and does not treat plain narrative text as a gate.
    hits = []
    if not text:
        return hits
    lines = text.splitlines()
    for i, line in enumerate(lines, start=1):
        if i in comment_lines or _line_in_ranges(i, doc_ranges):
            continue
        if condition_only and not _line_has_condition_hint(line):
            continue
        for rx in MAGIC_GATE_PATTERNS:
            if rx.search(line):
                strong = _line_has_strong_routing_marker(line)
                hits.append(
                    {
                        "line": int(i),
                        "kind": "text-heuristic",
                        "reason": "routing marker present" if strong else "weak routing marker",
                        "evidence": line.strip()[:200],
                        "strong": bool(strong),
                    }
                )
                break
    return hits


def _parse_python(text: str, path: str) -> ast.AST | None:
    try:
        return ast.parse(text, filename=path)
    except SyntaxError:
        return None


def _detect_except_pass_ast(tree: ast.AST, text: str) -> list:
    # Detector: AST scan for ExceptHandler nodes whose body is exactly [ast.Pass].
    hits = []
    lines = text.splitlines()
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler):
            if len(node.body) == 1 and isinstance(node.body[0], ast.Pass):
                lineno = getattr(node, "lineno", None) or 0
                evidence = ""
                if 1 <= lineno <= len(lines):
                    evidence = lines[lineno - 1].strip()[:200]
                hits.append({"line": int(lineno), "evidence": evidence})
    return hits


def _collect_top_level_defs(tree: ast.AST) -> list:
    # Detector: AST scan of module body for top-level defs/async defs only.
    defs = []
    if isinstance(tree, ast.Module):
        for node in tree.body:
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                defs.append((node.name, getattr(node, "lineno", 0)))
    return defs


def _build_duplicate_defs(def_index: dict) -> dict:
    # Detector: global index across files; keep names appearing in 2+ distinct files.
    dupes = {}
    for name, entries in sorted(def_index.items()):
        files = {e["file"] for e in entries}
        if len(files) >= 2:
            dupes[name] = entries
    return dupes


def _detect_user_writes_ast(tree: ast.AST, text: str) -> list:
    # Detector: AST scan for open(path, mode) write calls targeting /_user/ via literal,
    # f-string, concat, join/Path args, or scope-local variable propagation (no cross-function flow).
    hits = []
    lines = text.splitlines()

    def _is_user_literal(value: str) -> bool:
        return bool(USER_PATH_PATTERN.search(value or ""))

    def _is_user_arg_literal(value: str) -> bool:
        return bool(USER_LITERAL_PATTERN.search(value or "")) or bool(
            USER_PATH_PATTERN.search(value or "")
        )

    def _get_const_str(node: ast.AST) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        return None

    def _is_os_path_join_call(node: ast.AST) -> bool:
        if not isinstance(node, ast.Call):
            return False
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "join":
            if isinstance(func.value, ast.Attribute) and func.value.attr == "path":
                if isinstance(func.value.value, ast.Name) and func.value.value.id == "os":
                    return True
        return False

    def _is_pathlib_path_call(node: ast.AST) -> bool:
        if not isinstance(node, ast.Call):
            return False
        func = node.func
        if isinstance(func, ast.Name) and func.id == "Path":
            return True
        if isinstance(func, ast.Attribute) and func.attr == "Path":
            return True
        return False

    def _expr_has_user_literal(expr: ast.AST, assigned_user_paths: set[str]) -> bool:
        if isinstance(expr, ast.Name):
            return expr.id in assigned_user_paths
        if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
            return _is_user_literal(expr.value)
        if isinstance(expr, ast.JoinedStr):
            for part in expr.values:
                if _expr_has_user_literal(part, assigned_user_paths):
                    return True
            return False
        if isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Add):
            return _expr_has_user_literal(expr.left, assigned_user_paths) or _expr_has_user_literal(
                expr.right, assigned_user_paths
            )
        if isinstance(expr, (ast.Attribute, ast.Subscript)):
            value = getattr(expr, "value", None)
            return bool(value) and _expr_has_user_literal(value, assigned_user_paths)
        if isinstance(expr, ast.Call):
            for arg in expr.args:
                if _expr_has_user_literal(arg, assigned_user_paths):
                    return True
                s_arg = _get_const_str(arg)
                if s_arg is not None and _is_user_arg_literal(s_arg):
                    return True
            for kw in expr.keywords or []:
                if _expr_has_user_literal(kw.value, assigned_user_paths):
                    return True
                s_kw = _get_const_str(kw.value)
                if s_kw is not None and _is_user_arg_literal(s_kw):
                    return True
        return False

    def _user_path_kind(expr: ast.AST, assigned_user_paths: set[str]) -> str:
        if isinstance(expr, ast.Name) and expr.id in assigned_user_paths:
            return "variable"
        if isinstance(expr, ast.Constant) and isinstance(expr.value, str) and _is_user_literal(expr.value):
            return "literal"
        if isinstance(expr, ast.JoinedStr):
            return "fstring"
        if isinstance(expr, ast.BinOp) and isinstance(expr.op, ast.Add):
            return "concat"
        if isinstance(expr, ast.Call):
            if _is_os_path_join_call(expr):
                return "join"
            if _is_pathlib_path_call(expr):
                return "path"
            return "call"
        if isinstance(expr, (ast.Attribute, ast.Subscript)):
            return "attribute"
        return "unknown"

    def _open_write_mode(call: ast.Call) -> bool:
        mode_node = None
        if len(call.args) >= 2:
            mode_node = call.args[1]
        for kw in call.keywords or []:
            if kw.arg == "mode":
                mode_node = kw.value
        if mode_node is None:
            return False
        mode_val = _get_const_str(mode_node)
        if mode_val is None:
            return False
        return any(ch in mode_val for ch in ("w", "a", "x"))

    def _call_is_open_write(call: ast.Call) -> bool:
        return isinstance(call.func, ast.Name) and call.func.id == "open" and _open_write_mode(call)

    def _scan_scope(body: list[ast.stmt], assigned_user_paths: set[str]) -> None:
        for stmt in body:
            for node in ast.walk(stmt):
                if isinstance(node, ast.Call) and _call_is_open_write(node):
                    if not node.args:
                        continue
                    path_expr = node.args[0]
                    reason = ""
                    kind = ""
                    if isinstance(path_expr, ast.Name) and path_expr.id in assigned_user_paths:
                        reason = f"open() uses user-path variable '{path_expr.id}'"
                        kind = "variable"
                    elif _expr_has_user_literal(path_expr, assigned_user_paths):
                        kind = _user_path_kind(path_expr, assigned_user_paths)
                        reason = "open() uses /_user/ via expression"
                    if reason:
                        lineno = getattr(node, "lineno", 0)
                        evidence = ""
                        if 1 <= lineno <= len(lines):
                            evidence = lines[lineno - 1].strip()[:200]
                        hits.append(
                            {
                                "line": int(lineno),
                                "kind": kind or "unknown",
                                "reason": reason,
                                "evidence": evidence,
                            }
                        )

            if isinstance(stmt, ast.Assign):
                value = stmt.value
                is_user_path = _expr_has_user_literal(value, assigned_user_paths)
                if is_user_path:
                    for target in stmt.targets:
                        if isinstance(target, ast.Name):
                            assigned_user_paths.add(target.id)
            elif isinstance(stmt, ast.AnnAssign):
                value = stmt.value
                if value is not None:
                    is_user_path = _expr_has_user_literal(value, assigned_user_paths)
                    if is_user_path and isinstance(stmt.target, ast.Name):
                        assigned_user_paths.add(stmt.target.id)

            if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef)):
                _scan_scope(stmt.body, set())

    if isinstance(tree, ast.Module):
        _scan_scope(tree.body, set())
    return hits


def _render_metrics_diff(old: dict, new: dict) -> str:
    old_files = set((old or {}).get("files", {}).keys())
    new_files = set((new or {}).get("files", {}).keys())
    added = sorted(list(new_files - old_files))
    removed = sorted(list(old_files - new_files))

    def _sum(key: str, data: dict) -> int:
        return sum(int(v.get(key, 0)) for v in (data.get("files") or {}).values())

    old_total_loc = _sum("loc", old or {})
    new_total_loc = _sum("loc", new or {})
    old_total_size = _sum("size", old or {})
    new_total_size = _sum("size", new or {})

    lines = []
    lines.append("# METRICS.diff")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- total_files: {len(old_files)} -> {len(new_files)}")
    lines.append(f"- total_loc: {old_total_loc} -> {new_total_loc}")
    lines.append(f"- total_size: {old_total_size} -> {new_total_size}")
    lines.append("")
    lines.append("## Added Files")
    if added:
        for path in added[:200]:
            lines.append(f"- {path}")
    else:
        lines.append("- (none)")
    lines.append("")
    lines.append("## Removed Files")
    if removed:
        for path in removed[:200]:
            lines.append(f"- {path}")
    else:
        lines.append("- (none)")
    lines.append("")
    return "\n".join(lines).strip() + "\n"


def _write_metrics(metrics: dict, prev_metrics: dict | None) -> None:
    _ensure_notes_dir()
    with open(METRICS_PATH, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2, sort_keys=True)

    if prev_metrics is not None:
        diff_text = _render_metrics_diff(prev_metrics, metrics)
        with open(METRICS_DIFF_PATH, "w", encoding="utf-8") as f:
            f.write(diff_text)


def _write_guardrails() -> None:
    _ensure_notes_dir()
    content = "\n".join(
        [
            "# Guardrails",
            "",
            "- Replacement only, no layering. Future patches must replace or remove old code.",
            "- No silent failures. `except: pass` is forbidden.",
            "- Single canonical atomic writer for all paths under `/_user/` (project_store.py).",
            "- No wording-dependent routing or magic-word gates.",
            "",
        ]
    )
    with open(GUARDRAILS_PATH, "w", encoding="utf-8") as f:
        f.write(content)


def _write_diagnosis(metrics: dict, prev_metrics: dict | None) -> None:
    _ensure_notes_dir()
    risk = metrics.get("risk", {})
    except_pass = risk.get("except_pass", {})
    user_writes = risk.get("direct_user_writes", {})
    magic_gates = risk.get("magic_word_gates", {})
    dupes = risk.get("duplicate_functions", {})
    bloat = _compute_bloat_deltas(metrics, prev_metrics)
    protected_changed = _protected_file_changed(metrics, prev_metrics)
    patch_plan_changed = _patch_plan_changed(metrics, prev_metrics)

    lines = []
    lines.append("# Diagnosis")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- files_scanned: {len(metrics.get('files', {}))}")
    lines.append(f"- except_pass_files: {len(except_pass)}")
    lines.append(f"- direct_user_write_files: {len(user_writes)}")
    lines.append(f"- magic_gate_files: {len(magic_gates)}")
    lines.append(f"- duplicate_function_files: {len(dupes)}")
    lines.append(f"- protected_changed_count: {len(protected_changed)}")
    lines.append(f"- patch_plan_changed: {'yes' if patch_plan_changed else 'no'}")
    if bloat:
        lines.append(f"- protected_bloat_flags: {len(bloat)}")
    lines.append("")
    lines.append("## Top 10 Largest Files")
    for item in metrics.get("top_10_largest", []):
        lines.append(f"- {item.get('path')} ({item.get('size')} bytes)")
    if bloat:
        lines.append("")
        lines.append("## Protected Bloat Delta")
        for item in bloat:
            lines.append(
                f"- {item['file']}: loc {item['old_loc']} -> {item['new_loc']} ({item['delta_pct']:.1f}%)"
            )
    lines.append("")
    lines.append("## Decisions")
    lines.append("- Cleanup-first workflow enforced via notes and verification gates.")
    lines.append("- No edits to server.py, project_store.py, or model_pipeline.py in phase 1.")
    lines.append("- All future changes must be replacement-only (no layering).")
    lines.append("")
    with open(DIAGNOSIS_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")


def _write_candidates(metrics: dict) -> None:
    _ensure_notes_dir()
    risk = metrics.get("risk", {})
    lines = []
    lines.append("# Removal Candidates")
    lines.append("")
    lines.append("## except: pass")
    ep = risk.get("except_pass", {})
    if ep:
        for path, hits in sorted(ep.items()):
            for hit in hits:
                lines.append(
                    f"- {path}:{hit.get('line')} except-pass {hit.get('evidence')}".rstrip()
                )
    else:
        lines.append("- (none)")
    lines.append("")
    lines.append("## Direct Writes to /_user/")
    dw = risk.get("direct_user_writes", {})
    if dw:
        for path, hits in sorted(dw.items()):
            for hit in hits:
                reason = hit.get("reason", "")
                kind = hit.get("kind", "unknown")
                lines.append(
                    f"- {path}:{hit.get('line')} user-write ({kind}) {reason} {hit.get('evidence')}".rstrip()
                )
    else:
        lines.append("- (none)")
    lines.append("")
    lines.append("## Magic-Word Gates")
    mg = risk.get("magic_word_gates", {})
    if mg:
        for path, hits in sorted(mg.items()):
            for hit in hits:
                kind = hit.get("kind", "text-heuristic")
                reason = hit.get("reason", "")
                lines.append(
                    f"- {path}:{hit.get('line')} magic-gate ({kind}) {reason} {hit.get('evidence')}".rstrip()
                )
    else:
        lines.append("- (none)")
    lines.append("")
    lines.append("## Duplicate Functions")
    df = risk.get("duplicate_functions", {})
    if df:
        for name, entries in sorted(df.items()):
            locations = ", ".join(f"{e['file']}:{e['line']}" for e in entries)
            lines.append(f"- {name}: {locations}")
    else:
        lines.append("- (none)")
    lines.append("")
    with open(REMOVAL_CANDIDATES_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")


def _write_plan(metrics: dict) -> None:
    _ensure_notes_dir()
    lines = []
    lines.append("# Patch Plan (Cleanup-First)")
    lines.append("")
    lines.append("1. Eliminate any `except: pass` instances in protected files.")
    lines.append("2. Route all `/_user/` writes through the canonical writer in project_store.py.")
    lines.append("3. Remove wording-dependent routing gates (magic-word commands).")
    lines.append("4. Remove or consolidate duplicate function definitions.")
    lines.append("5. Re-scan and verify with cleanup.py.")
    lines.append("")
    with open(PATCH_PLAN_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines).rstrip() + "\n")


def _map_file(path: str) -> str:
    if not os.path.isfile(path):
        rel = os.path.join(ROOT, path)
        if os.path.isfile(rel):
            path = rel
        else:
            raise FileNotFoundError(path)

    text = _read_text(path)
    rel = os.path.relpath(path, ROOT)
    parsed = _parse_python(text, path) if path.endswith(".py") else None

    docstring = ""
    defs = []
    if parsed is not None:
        docstring = ast.get_docstring(parsed) or ""
        defs = _collect_top_level_defs(parsed)

    magic_hits = _detect_magic_gates(path, text, parsed)
    except_hits = _detect_except_pass_ast(parsed, text) if parsed is not None else []
    write_hits = _detect_user_writes_ast(parsed, text) if parsed is not None else []

    out = []
    out.append(f"# MAP: {rel}")
    out.append("")
    out.append("## Module Docstring")
    out.append(docstring if docstring else "(none)")
    out.append("")
    out.append("## Top-Level Defs")
    if defs:
        for name, line in defs:
            out.append(f"- def {name} @ line {int(line)}")
    else:
        out.append("- (none)")
    out.append("")
    out.append("## Suspicious Markers")
    out.append(f"- except_pass_hits: {len(except_hits)}")
    out.append(f"- open_write_hits: {len(write_hits)}")
    out.append(f"- magic_word_hits: {len(magic_hits)}")
    out.append("")
    for hit in except_hits:
        out.append(f"- except_pass: line {hit.get('line')} {hit.get('evidence')}".rstrip())
    for hit in write_hits:
        out.append(
            f"- open_write: line {hit.get('line')} {hit.get('reason')} {hit.get('evidence')}".rstrip()
        )
    for hit in magic_hits:
        out.append(
            f"- magic_word: line {hit.get('line')} ({hit.get('kind')}) {hit.get('evidence')}".rstrip()
        )
    out.append("")

    _ensure_notes_dir()
    map_name = f"MAP_{os.path.basename(path)}.md"
    map_path = os.path.join(NOTES_DIR, map_name)
    with open(map_path, "w", encoding="utf-8") as f:
        f.write("\n".join(out).rstrip() + "\n")

    summary = (
        f"MAP {rel}: defs={len(defs)}, except_pass={len(except_hits)}, "
        f"open_write={len(write_hits)}, magic_words={len(magic_hits)}"
    )
    return summary


def _verify(metrics: dict, prev_metrics: dict | None) -> None:
    failures = []

    def _fail(msg: str) -> None:
        failures.append(msg)

    except_pass = metrics.get("risk", {}).get("except_pass", {})
    if except_pass:
        _fail(f"except: pass detected in {len(except_pass)} file(s)")
    for fname in PROTECTED_FILES:
        hits = except_pass.get(fname, [])
        if hits:
            _fail(f"{fname} has {len(hits)} occurrences of except-pass")

    for rel, hits in metrics.get("risk", {}).get("direct_user_writes", {}).items():
        if os.path.basename(rel) != "project_store.py":
            _fail(f"Direct /_user/ write hits in {rel} ({len(hits)} hits)")

    magic = metrics.get("risk", {}).get("magic_word_gates", {})
    if magic:
        for rel, hits in magic.items():
            base = os.path.basename(rel)
            if base not in PROTECTED_FILES:
                continue
            for hit in hits:
                kind = hit.get("kind", "text-heuristic")
                strong = bool(hit.get("strong")) or (kind == "ast-condition")
                if kind == "ast-condition" or strong:
                    _fail(f"Magic-word routing gate in {rel} ({kind})")

    dupes = metrics.get("risk", {}).get("duplicate_functions", {})
    protected_dupes = _protected_duplicate_hits(dupes)
    if protected_dupes:
        _fail(
            "Duplicate function names involving protected files: "
            + ", ".join(sorted(protected_dupes))
        )

    parse_errors = metrics.get("risk", {}).get("parse_errors", {})
    for rel in parse_errors.keys():
        if os.path.basename(rel) in PROTECTED_FILES:
            _fail(f"AST parse failed for protected file {rel}")

    bloat = _compute_bloat_deltas(metrics, prev_metrics)
    for item in bloat:
        if _has_removal_evidence(item["file"]):
            continue
        _fail(
            f"Protected file bloat without removal evidence: {item['file']} "
            f"(loc {item['old_loc']} -> {item['new_loc']}, "
            f"threshold: >2% or >200 LOC)"
        )

    protected_changed = _protected_file_changed(metrics, prev_metrics)
    if prev_metrics and protected_changed:
        patch_plan_changed = _patch_plan_changed(metrics, prev_metrics)
        ack_ok, missing = _has_plan_acknowledgment(protected_changed)
        if not patch_plan_changed and not ack_ok:
            missing_list = ", ".join(missing) if missing else ", ".join(protected_changed)
            _fail(
                "Protected file changed without PATCH_PLAN update: "
                + missing_list
                + " (update notes/PATCH_PLAN.md or add ACK/REMOVED)"
            )

    if failures:
        _ensure_notes_dir()
        with open(DIAGNOSIS_PATH, "a", encoding="utf-8") as f:
            f.write("\n## Verification Failures\n")
            for msg in failures:
                f.write(f"- {msg}\n")
        raise SystemExit(1)


def cmd_scan(_args: argparse.Namespace) -> None:
    prev_metrics = _load_previous_metrics()
    metrics = _scan_metrics()
    _write_metrics(metrics, prev_metrics)
    _write_guardrails()
    _write_diagnosis(metrics, prev_metrics)
    _write_candidates(metrics)


def cmd_verify(_args: argparse.Namespace) -> None:
    prev_metrics = _load_previous_metrics()
    metrics = _scan_metrics()
    _write_metrics(metrics, prev_metrics)
    _write_guardrails()
    _write_diagnosis(metrics, prev_metrics)
    _write_candidates(metrics)
    _verify(metrics, prev_metrics)


def cmd_map(args: argparse.Namespace) -> None:
    output = _map_file(args.file)
    print(output)


def cmd_candidates(_args: argparse.Namespace) -> None:
    metrics = _scan_metrics()
    _write_candidates(metrics)


def cmd_plan(_args: argparse.Namespace) -> None:
    prev_metrics = _load_previous_metrics()
    metrics = _scan_metrics()
    _write_metrics(metrics, prev_metrics)
    _write_diagnosis(metrics, prev_metrics)
    _write_candidates(metrics)
    _write_plan(metrics)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cleanup.py")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("scan")
    sub.add_parser("verify")
    map_p = sub.add_parser("map")
    map_p.add_argument("--file", required=True)
    sub.add_parser("candidates")
    sub.add_parser("plan")
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.cmd == "scan":
        cmd_scan(args)
    elif args.cmd == "verify":
        cmd_verify(args)
    elif args.cmd == "map":
        cmd_map(args)
    elif args.cmd == "candidates":
        cmd_candidates(args)
    elif args.cmd == "plan":
        cmd_plan(args)
    else:
        raise SystemExit(2)


def _get_docstring_ranges(tree: ast.AST) -> list[tuple[int, int]]:
    # Detector support: collect docstring line ranges to ignore during text scans.
    ranges: list[tuple[int, int]] = []

    def _add_doc(node: ast.AST) -> None:
        if hasattr(node, "body") and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                if isinstance(first.value.value, str):
                    start = getattr(first, "lineno", None)
                    end = getattr(first, "end_lineno", None) or start
                    if start is not None:
                        ranges.append((int(start), int(end or start)))

    if isinstance(tree, ast.Module):
        _add_doc(tree)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            _add_doc(node)
    return ranges


def _line_in_ranges(line_no: int, ranges: list[tuple[int, int]]) -> bool:
    for start, end in ranges:
        if start <= line_no <= end:
            return True
    return False


def _get_comment_lines(text: str) -> set[int]:
    # Detector support: mark comment-only lines using tokenize to avoid false positives.
    import io
    import tokenize

    comment_lines: set[int] = set()
    try:
        tokens = tokenize.generate_tokens(io.StringIO(text).readline)
        for tok_type, _tok_str, start, _end, _line in tokens:
            if tok_type == tokenize.COMMENT:
                comment_lines.add(int(start[0]))
    except tokenize.TokenError:
        pass
    return comment_lines


def _line_has_condition_hint(line: str) -> bool:
    l = line.strip()
    return any(hint in l for hint in CONDITION_HINTS)


def _line_has_strong_routing_marker(line: str) -> bool:
    l = line.strip().lower()
    return any(hint in l for hint in STRONG_ROUTING_MARKERS)


def _scan_patch_plan_meta() -> dict:
    # Notes: record PATCH_PLAN.md metadata for auditable protected-change tracking.
    if not os.path.isfile(PATCH_PLAN_PATH):
        return {"missing": True}
    try:
        text = _read_text(PATCH_PLAN_PATH)
    except OSError:
        return {"missing": True}
    try:
        size = os.path.getsize(PATCH_PLAN_PATH)
    except OSError:
        size = 0
    return {
        "sha256": _hash_text(text),
        "loc": int(_file_loc(text)),
        "size": int(size),
    }


def _protected_file_changed(metrics: dict, prev_metrics: dict | None) -> list[str]:
    # Guardrail: report protected files whose sha256 changed vs previous metrics.
    if not prev_metrics:
        return []
    changed: list[str] = []
    prev_files = prev_metrics.get("files", {}) or {}
    for rel, meta in (metrics.get("files") or {}).items():
        base = os.path.basename(rel)
        if base not in PROTECTED_FILES:
            continue
        prev_meta = prev_files.get(rel)
        if not prev_meta or prev_meta.get("sha256") != meta.get("sha256"):
            changed.append(base)
    return sorted(set(changed))


def _patch_plan_changed(metrics: dict, prev_metrics: dict | None) -> bool:
    # Guardrail: detect PATCH_PLAN.md content change via stored sha256.
    if not prev_metrics:
        return False
    prev_pp = (prev_metrics.get("notes") or {}).get("patch_plan", {})
    curr_pp = (metrics.get("notes") or {}).get("patch_plan", {})
    prev_missing = bool(prev_pp.get("missing"))
    curr_missing = bool(curr_pp.get("missing"))
    if prev_missing and curr_missing:
        return False
    if prev_missing != curr_missing:
        return True
    return prev_pp.get("sha256") != curr_pp.get("sha256")


def _has_plan_acknowledgment(changed_files: list[str]) -> tuple[bool, list[str]]:
    # Guardrail: require explicit ACK/REMOVED markers when protected files change.
    if not changed_files:
        return True, []
    if not os.path.isfile(PATCH_PLAN_PATH):
        return False, changed_files
    try:
        text = _read_text(PATCH_PLAN_PATH)
    except OSError:
        return False, changed_files
    if "ACK: protected change" in text:
        return True, []
    missing = []
    for fname in changed_files:
        if f"ACK: {fname}" in text or f"REMOVED: {fname}" in text:
            continue
        missing.append(fname)
    return len(missing) == 0, missing


def _protected_duplicate_hits(dupes: dict) -> set[str]:
    # Detector support: only flag duplicates that include protected files and another file.
    protected = set(PROTECTED_FILES)
    hits: set[str] = set()
    for name, entries in dupes.items():
        files = {e["file"] for e in entries}
        if files & protected and len(files) >= 2:
            hits.add(name)
    return hits


def _load_previous_metrics() -> dict | None:
    if not os.path.isfile(METRICS_PATH):
        return None
    try:
        with open(METRICS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _compute_bloat_deltas(metrics: dict, prev_metrics: dict | None) -> list[dict]:
    # Detector: compare LOC for protected files vs previous metrics (2% or 200 LOC threshold);
    # does not attempt to infer bloat when there is no prior snapshot.
    if not prev_metrics:
        return []
    deltas = []
    for fname in PROTECTED_FILES:
        old = (prev_metrics.get("files") or {}).get(fname, {})
        new = (metrics.get("files") or {}).get(fname, {})
        old_loc = int(old.get("loc", 0))
        new_loc = int(new.get("loc", 0))
        if old_loc == 0:
            continue
        delta = new_loc - old_loc
        delta_pct = (delta / float(old_loc)) * 100.0
        if delta > 200 or delta_pct > 2.0:
            deltas.append(
                {
                    "file": fname,
                    "old_loc": old_loc,
                    "new_loc": new_loc,
                    "delta_pct": delta_pct,
                }
            )
    return deltas


def _has_removal_evidence(filename: str) -> bool:
    # Detector support: allow bloat if notes include explicit REMOVED evidence.
    needles = [f"REMOVED: {filename}"]
    for path in (PATCH_PLAN_PATH, DIAGNOSIS_PATH):
        if not os.path.isfile(path):
            continue
        try:
            text = _read_text(path)
        except OSError:
            continue
        for needle in needles:
            if needle in text:
                return True
    return False


if __name__ == "__main__":
    main()
