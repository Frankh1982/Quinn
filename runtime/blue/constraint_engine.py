# -*- coding: utf-8 -*-
"""
constraint_engine.py

Generic, deterministic constraint compilation + enforcement.

Purpose:
- Turn "rules as text" (project_state.user_rules + user directives) into
  a machine-checkable constraint object.
- Validate candidate assistant outputs against constraints BEFORE they reach the user.
- Provide a bounded, deterministic retry prompt when violations occur.

Non-goals:
- Domain-specific solvers (Wordle, etc.)
- Freeform policy inference (no model calls here)
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple


# Basic emoji range heuristic (covers most pictographic emojis).
_EMOJI_RE = re.compile(r"[\U0001F300-\U0001FAFF]")

# Conservative “question” detector: any '?'.
_QMARK_RE = re.compile(r"\?")

# Common “hedge / low-authority” patterns (optional; only used when user asked).
_HEDGE_RE = re.compile(r"\b(i think|maybe|probably|might be|not sure|i guess)\b", re.IGNORECASE)
_DEFAULT_FORBIDDEN_SUBSTRINGS = [
    # Hard anti-sycophancy / anti-ChatGPT-default guardrails (case-insensitive contains)
    "great question",
    "you’re absolutely right",
    "you're absolutely right",
    "you’re so right",
    "you're so right",
    "totally valid",
    "completely valid",
    "as an ai",
    "as a language model",
    "i'm happy to help",
    "happy to help",
    "glad to help",
    "you're brilliant",
    "you are brilliant",
]

def compile_constraints(*, project_state: Dict[str, Any], user_msg: str, active_expert: str = "") -> Dict[str, Any]:
    """
    Deterministically compile constraints from:
    - project_state.user_rules (list[str])
    - user_msg (this turn only; explicit directives)

    Returns:
      {
        "max_questions": int|None,
        "max_lines": int|None,
        "forbid_emoji": bool,
        "forbid_hedging": bool,
        "forbidden_substrings": [str...],   # case-insensitive contains check
      }
    """
    st = project_state if isinstance(project_state, dict) else {}
    rules = st.get("user_rules")
    if not isinstance(rules, list):
        rules = []
    rules_norm = [str(x).strip() for x in rules if str(x).strip()]

    msg = (user_msg or "").strip()
    msg_low = msg.lower()

    # Defaults: permissive unless user has explicitly imposed constraints.
    out: Dict[str, Any] = {
        "max_questions": None,
        "max_lines": None,
        "forbid_emoji": False,
        "forbid_hedging": False,
        "forbidden_substrings": [],
    }

    # Default expert hardening (anti-sycophancy / no-emoji / low-hedge).
    ae = (active_expert or "").strip().lower()
    ae_is_default = (ae in ("", "default"))

    if ae_is_default:
        out["forbid_emoji"] = True
        out["forbid_hedging"] = True


    # Helper: search in project rules + user msg
    hay = "\n".join(rules_norm + ([msg] if msg else [])).lower()

    # No questions
    if ("no questions" in hay) or ("do not ask" in hay) or ("don't ask" in hay):
        out["max_questions"] = 0

    # One-line / minimal verbosity
    if ("word only" in hay) or ("one word" in hay) or ("single word" in hay):
        out["max_lines"] = 1
    if ("no explanations" in hay) or ("do not explain" in hay) or ("don't explain" in hay):
        # Keep it strict but not absurd: 1–2 short lines
        out["max_lines"] = 2

    # No fluff / no emoji
    if ("no emoji" in hay) or ("no emojis" in hay):
        out["forbid_emoji"] = True

    # Authority / decisiveness (optional)
    if ("be decisive" in hay) or ("stop hedging" in hay) or ("no hedging" in hay):
        out["forbid_hedging"] = True

    # User-provided “never say X” / “do not say X” (substring-based; deterministic)
    forbidden: List[str] = (list(_DEFAULT_FORBIDDEN_SUBSTRINGS) if ae_is_default else [])

    rx = re.compile(r"^\s*(never|do not|don't)\s+say\s+(.+?)\s*$", re.IGNORECASE)
    for line in rules_norm:
        m = rx.match(line.strip())
        if not m:
            continue
        frag = (m.group(2) or "").strip().strip("\"'").strip()
        if frag:
            forbidden.append(frag)

    # Also allow user_msg to add a one-off forbidden phrase (rare but useful):
    # e.g., "Don't say 'great question'".
    m2 = re.search(r"don't\s+say\s+[\"'](.+?)[\"']", msg_low)
    if m2:
        frag2 = (m2.group(1) or "").strip()
        if frag2:
            forbidden.append(frag2)

    # Dedupe (case-insensitive)
    seen = set()
    dedup = []
    for f in forbidden:
        k = f.lower()
        if k in seen:
            continue
        seen.add(k)
        dedup.append(f)

    out["forbidden_substrings"] = dedup[:24]
    return out


def validate_output(*, output_text: str, constraints: Dict[str, Any]) -> List[str]:
    """
    Return a list of violation strings. Empty list => OK.
    Deterministic only.
    """
    c = constraints if isinstance(constraints, dict) else {}
    s = (output_text or "").strip()
    v: List[str] = []

    if not s:
        v.append("empty_output")
        return v

    # max_lines
    ml = c.get("max_lines", None)
    if isinstance(ml, int) and ml > 0:
        lines = [ln for ln in s.splitlines() if ln.strip()]
        if len(lines) > ml:
            v.append(f"too_many_lines (max_lines={ml})")

    # max_questions
    mq = c.get("max_questions", None)
    if isinstance(mq, int) and mq >= 0:
        qcount = len(_QMARK_RE.findall(s))
        if qcount > mq:
            v.append(f"too_many_questions (max_questions={mq})")

    # emoji
    if bool(c.get("forbid_emoji", False)):
        if _EMOJI_RE.search(s):
            v.append("emoji_forbidden")

    # hedging
    if bool(c.get("forbid_hedging", False)):
        if _HEDGE_RE.search(s):
            v.append("hedging_forbidden")

    # forbidden substrings (case-insensitive contains)
    subs = c.get("forbidden_substrings", [])
    if isinstance(subs, list):
        low = s.lower()
        for frag in subs:
            f = str(frag or "").strip()
            if not f:
                continue
            if f.lower() in low:
                v.append(f"forbidden_phrase: {f}")
                # cap noise
                if len(v) >= 8:
                    break

    return v


def build_retry_system_note(*, constraints: Dict[str, Any], violations: List[str]) -> str:
    """
    Build a deterministic system message telling the model to regenerate compliantly.
    """
    c = constraints if isinstance(constraints, dict) else {}
    vio = violations if isinstance(violations, list) else []
    parts: List[str] = []
    parts.append("CONSTRAINT ENFORCEMENT:")
    parts.append("- The previous draft violated hard constraints. Regenerate a compliant answer.")
    parts.append("- Do NOT mention constraints or violations in the user-visible output.")
    parts.append("")
    parts.append("Constraints:")
    parts.append(f"- max_questions: {c.get('max_questions', None)}")
    parts.append(f"- max_lines: {c.get('max_lines', None)}")
    parts.append(f"- forbid_emoji: {bool(c.get('forbid_emoji', False))}")
    parts.append(f"- forbid_hedging: {bool(c.get('forbid_hedging', False))}")
    subs = c.get("forbidden_substrings", [])
    if isinstance(subs, list) and subs:
        parts.append("- forbidden_phrases: " + ", ".join([str(x) for x in subs[:10]]))
    else:
        parts.append("- forbidden_phrases: (none)")
    parts.append("")
    parts.append("Violations detected:")
    for x in vio[:12]:
        parts.append(f"- {x}")
    return "\n".join(parts).strip()
