"""
Strict anchor-based patch engine (stdlib-only).

Responsibilities:
- build_patch_mode_system_prompt()
- parse/validate patch blocks
- apply_anchor_patch_to_text()
- build_unified_diff()

Constraints:
- No imports from server.py (prevents circular imports).
- Deterministic behavior.
"""


from __future__ import annotations

import difflib
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

# ---- Patch directives (must match your existing patch format) ----

REPLACE_FROM = "Replace from this exact line:"
THROUGH = "Through this exact line:"
INSERT_AFTER = "Insert immediately AFTER this exact line:"
INSERT_BEFORE = "Insert immediately BEFORE this exact line:"
WITH_THIS_CODE = "With this code:"


# ---- Prompt builders ----

def build_patch_mode_system_prompt(language_hint: str = "") -> str:
    lang = (language_hint or "").strip()
    fence = f"```{lang}".rstrip()

    return (
        "You are in PATCH MODE.\n\n"
        "Your ONLY job is to output patch instructions in this exact format, and nothing else:\n\n"
        f"{REPLACE_FROM} <verbatim line>\n"
        f"{THROUGH} <verbatim line>\n"
        f"{WITH_THIS_CODE}\n"
        f"{fence}\n"
        "<replacement code>\n"
        "```\n\n"
        "OR you may use:\n"
        f"{INSERT_AFTER} <verbatim line>\n"
        f"{WITH_THIS_CODE}\n"
        f"{fence}\n"
        "<insert code>\n"
        "```\n\n"
        "OR:\n"
        f"{INSERT_BEFORE} <verbatim line>\n"
        f"{WITH_THIS_CODE}\n"
        f"{fence}\n"
        "<insert code>\n"
        "```\n\n"
        "Rules (strict):\n"
        "- Anchor lines MUST be copied verbatim from the provided CODE_CORPUS (character-for-character).\n"
        "- Anchors MUST be UNIQUE in the codebase (the server will reject ambiguous anchors).\n"
        "- Do NOT output any prose, explanation, headings, or extra formatting.\n"
        "- Output only patch blocks.\n"
    )


def language_hint_from_suffix(suffix: str) -> str:
    s = (suffix or "").lower()
    mapping = {
        ".py": "python",
        ".js": "javascript",
        ".ts": "typescript",
        ".jsx": "javascript",
        ".tsx": "typescript",
        ".html": "html",
        ".htm": "html",
        ".css": "css",
        ".json": "json",
        ".md": "markdown",
        ".yml": "yaml",
        ".yaml": "yaml",
        ".txt": "",
    }
    return mapping.get(s, "")


# ---- Core parsing / validation ----

@dataclass(frozen=True)
class PatchOp:
    op: str  # "replace" | "insert_after" | "insert_before"
    start: Optional[str] = None
    end: Optional[str] = None
    anchor: Optional[str] = None
    code: str = ""


def line_counts(full_text: str) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for ln in (full_text or "").splitlines():
        counts[ln] = counts.get(ln, 0) + 1
    return counts


def extract_anchor_lines_from_patch(patch_text: str) -> List[str]:
    anchors: List[str] = []
    for raw in (patch_text or "").splitlines():
        line = raw.rstrip("\n").rstrip("\r")
        for prefix in (REPLACE_FROM, THROUGH, INSERT_AFTER, INSERT_BEFORE):
            if line.startswith(prefix):
                a = line[len(prefix):]
                if a.startswith(" "):
                    a = a[1:]
                a = a.rstrip()
                if a:
                    anchors.append(a)
    return anchors


def anchors_exist_and_unique(
    anchors: List[str],
    full_line_counts: Dict[str, int],
) -> Tuple[bool, List[str], List[str]]:
    missing: List[str] = []
    ambiguous: List[str] = []
    for a in anchors:
        c = full_line_counts.get(a, 0)
        if c == 0:
            missing.append(a)
        elif c != 1:
            ambiguous.append(a)
    ok = (not missing) and (not ambiguous)
    return ok, missing, ambiguous


def validate_patch_text_strict_format(patch_text: str) -> Tuple[bool, str]:
    """
    Strictly enforces:
      - Only patch blocks, no extra prose
      - For replace: REPLACE_FROM + THROUGH + WITH_THIS_CODE + fenced code
      - For insert: INSERT_AFTER/INSERT_BEFORE + WITH_THIS_CODE + fenced code
      - Fences must be proper ```... and close with ```
    """
    lines = (patch_text or "").splitlines()
    i = 0

    def skip_blanks(idx: int) -> int:
        while idx < len(lines) and not (lines[idx] or "").strip():
            idx += 1
        return idx

    def expect_exact(idx: int, expected: str) -> Tuple[bool, int, str]:
        if idx >= len(lines):
            return False, idx, f"Expected {expected!r}, got EOF."
        if (lines[idx] or "").strip() != expected:
            return False, idx, f"Expected {expected!r}, got {(lines[idx] or '')!r}"
        return True, idx + 1, ""

    def parse_anchor(idx: int, prefix: str) -> Tuple[bool, int, str, str]:
        if idx >= len(lines):
            return False, idx, "", f"Missing {prefix!r}"
        raw = (lines[idx] or "").rstrip("\n").rstrip("\r")
        if not raw.startswith(prefix):
            return False, idx, "", f"Expected line starting with {prefix!r}, got {raw!r}"
        a = raw[len(prefix):]
        if a.startswith(" "):
            a = a[1:]
        a = a.rstrip()
        if not a:
            return False, idx, "", f"Empty anchor for {prefix!r}"
        return True, idx + 1, a, ""

    def parse_fence(idx: int) -> Tuple[bool, int, str]:
        if idx >= len(lines):
            return False, idx, "Missing code fence start."
        fence = (lines[idx] or "").strip()
        if not fence.startswith("```"):
            return False, idx, f"Missing code fence start at line {idx+1}."
        idx += 1
        while idx < len(lines):
            if (lines[idx] or "").strip() == "```":
                return True, idx + 1, ""
            idx += 1
        return False, idx, "Unterminated code fence (missing closing ```)."

    i = skip_blanks(i)
    if i >= len(lines):
        return False, "Empty patch output."

    while True:
        i = skip_blanks(i)
        if i >= len(lines):
            break

        line = (lines[i] or "").rstrip("\n").rstrip("\r")

        if line.startswith(REPLACE_FROM):
            ok, i, _a1, err = parse_anchor(i, REPLACE_FROM)
            if not ok:
                return False, err

            ok, i, _a2, err = parse_anchor(i, THROUGH)
            if not ok:
                return False, err

            ok, i, err = expect_exact(i, WITH_THIS_CODE)
            if not ok:
                return False, err

            ok, i, err = parse_fence(i)
            if not ok:
                return False, err

            continue

        if line.startswith(INSERT_AFTER):
            ok, i, _a, err = parse_anchor(i, INSERT_AFTER)
            if not ok:
                return False, err

            ok, i, err = expect_exact(i, WITH_THIS_CODE)
            if not ok:
                return False, err

            ok, i, err = parse_fence(i)
            if not ok:
                return False, err

            continue

        if line.startswith(INSERT_BEFORE):
            ok, i, _a, err = parse_anchor(i, INSERT_BEFORE)
            if not ok:
                return False, err

            ok, i, err = expect_exact(i, WITH_THIS_CODE)
            if not ok:
                return False, err

            ok, i, err = parse_fence(i)
            if not ok:
                return False, err

            continue

        # Anything else is illegal text outside patch blocks
        if (line or "").strip():
            return False, f"Unexpected text outside patch blocks at line {i+1}: {line!r}"
        i += 1

    return True, ""


# ---- Patch operation parsing ----

_CODE_FENCE_RE = re.compile(r"^```.*?$", re.MULTILINE)


def _extract_fenced_code(lines: List[str], idx: int) -> Tuple[str, int]:
    if idx >= len(lines):
        raise ValueError("Missing code fence start.")
    if not (lines[idx] or "").strip().startswith("```"):
        raise ValueError("Expected code fence start.")
    idx += 1
    buf: List[str] = []
    while idx < len(lines):
        if (lines[idx] or "").strip() == "```":
            return "\n".join(buf), idx + 1
        buf.append((lines[idx] or "").rstrip("\n").rstrip("\r"))
        idx += 1
    raise ValueError("Unterminated code fence in patch output.")


def parse_anchor_patch_ops(patch_text: str) -> List[PatchOp]:
    ops: List[PatchOp] = []
    lines = (patch_text or "").splitlines()
    i = 0

    def strip_prefix(line: str, prefix: str) -> str:
        s = line[len(prefix):]
        if s.startswith(" "):
            s = s[1:]
        return s.rstrip()

    while i < len(lines):
        raw = (lines[i] or "").rstrip("\n").rstrip("\r")
        if not raw.strip():
            i += 1
            continue

        if raw.startswith(REPLACE_FROM):
            start = strip_prefix(raw, REPLACE_FROM)
            i += 1
            if i >= len(lines) or not (lines[i] or "").startswith(THROUGH):
                raise ValueError("Patch missing 'Through this exact line:' after replace start.")
            end = strip_prefix(lines[i], THROUGH)
            i += 1

            # Seek "With this code:"
            while i < len(lines) and (lines[i] or "").strip() != WITH_THIS_CODE:
                i += 1
            if i >= len(lines):
                raise ValueError("Patch missing 'With this code:' line.")
            i += 1

            code, i = _extract_fenced_code(lines, i)
            ops.append(PatchOp(op="replace", start=start, end=end, code=code))
            continue

        if raw.startswith(INSERT_AFTER):
            anchor = strip_prefix(raw, INSERT_AFTER)
            i += 1
            while i < len(lines) and (lines[i] or "").strip() != WITH_THIS_CODE:
                i += 1
            if i >= len(lines):
                raise ValueError("Patch missing 'With this code:' line for insert-after.")
            i += 1

            code, i = _extract_fenced_code(lines, i)
            ops.append(PatchOp(op="insert_after", anchor=anchor, code=code))
            continue

        if raw.startswith(INSERT_BEFORE):
            anchor = strip_prefix(raw, INSERT_BEFORE)
            i += 1
            while i < len(lines) and (lines[i] or "").strip() != WITH_THIS_CODE:
                i += 1
            if i >= len(lines):
                raise ValueError("Patch missing 'With this code:' line for insert-before.")
            i += 1

            code, i = _extract_fenced_code(lines, i)
            ops.append(PatchOp(op="insert_before", anchor=anchor, code=code))
            continue

        raise ValueError(f"Unexpected text outside patch directives at line {i+1}: {raw!r}")

    if not ops:
        raise ValueError("No patch operations found.")
    return ops


# ---- Patch application ----

def apply_anchor_patch_to_text(original_text: str, patch_text: str) -> str:
    ops = parse_anchor_patch_ops(patch_text)

    had_trailing_newline = (original_text or "").endswith("\n")
    lines = (original_text or "").splitlines()

    def find_unique_line(target: str) -> int:
        idxs = [k for k, ln in enumerate(lines) if ln == target]
        if len(idxs) != 1:
            raise ValueError(f"Expected 1 match for anchor line, got {len(idxs)}: {target!r}")
        return idxs[0]

    for op in ops:
        code_lines = (op.code or "").splitlines()

        if op.op == "replace":
            if op.start is None or op.end is None:
                raise ValueError("Replace op missing start/end anchors.")
            s_idx = find_unique_line(op.start)
            e_idx = find_unique_line(op.end)
            if e_idx < s_idx:
                raise ValueError("End anchor occurs before start anchor.")
            lines = lines[:s_idx] + code_lines + lines[e_idx + 1:]
            continue

        if op.op == "insert_after":
            if op.anchor is None:
                raise ValueError("Insert-after op missing anchor.")
            a_idx = find_unique_line(op.anchor)
            lines = lines[:a_idx + 1] + code_lines + lines[a_idx + 1:]
            continue

        if op.op == "insert_before":
            if op.anchor is None:
                raise ValueError("Insert-before op missing anchor.")
            a_idx = find_unique_line(op.anchor)
            lines = lines[:a_idx] + code_lines + lines[a_idx:]
            continue

        raise ValueError(f"Unknown patch operation: {op.op!r}")

    out = "\n".join(lines)
    if had_trailing_newline:
        out += "\n"
    return out


# ---- Unified diff builder ----

def build_unified_diff(
    before_text: str,
    after_text: str,
    fromfile: str = "before",
    tofile: str = "after",
    context: int = 3,
) -> str:
    before_lines = (before_text or "").splitlines(keepends=True)
    after_lines = (after_text or "").splitlines(keepends=True)

    diff_iter = difflib.unified_diff(
        before_lines,
        after_lines,
        fromfile=fromfile,
        tofile=tofile,
        n=context,
    )
    return "".join(diff_iter)
