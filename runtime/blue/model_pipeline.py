# -*- coding: utf-8 -*-
"""
model_pipeline.py

Extracted “model pipeline” from server.py.

Constraints:
- No websocket/aiohttp imports.
- No behavior changes: keep logic/strings/ordering identical to server.py pipeline.
- Server passes a ctx module (server.py) that provides all existing helpers/constants.

Pipeline responsibilities:
- decide lookup vs Project OS vs excel vs deliverable
- build LLM messages
- call call_openai_chat
- parse tagged blocks
- persist state blocks / create deliverables
- append real Open: /file?path=... lines
- snapshot assistant output
"""

from __future__ import annotations

import asyncio
import time
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import json
from datetime import datetime
try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None  # type: ignore

from openpyxl import Workbook

import constraint_engine

# ------------------------------------------------------------
# Time anchors v1 (deterministic, bounded, project-scoped)
# ------------------------------------------------------------
_TIME_ANCHOR_KEY = "time_anchors_v1"

def _time_parse_iso_noz(s: str, tz_name: str) -> Optional[datetime]:
    """
    Parse ISO-like timestamps written by this file.
    Stored as local wall-time "YYYY-MM-DDTHH:MM:SS" (no Z).
    """
    v = (s or "").strip()
    dt: Optional[datetime] = None

    if v:
        try:
            v2 = v.replace(" ", "T")
            if v2.endswith("Z"):
                v2 = v2[:-1]
            dt = datetime.fromisoformat(v2)
            if ZoneInfo is not None and dt.tzinfo is None:
                try:
                    dt = dt.replace(tzinfo=ZoneInfo(tz_name))
                except Exception:
                    pass
        except Exception:
            dt = None

    return dt

def _time_anchor_label_from_text(s: str) -> str:
    t = " ".join((s or "").strip().split())
    t = re.sub(r"[\r\n\t]+", " ", t).strip()
    t = t.rstrip(".!?")
    if len(t) > 80:
        t = t[:80].rstrip()
    return t

def _time_maybe_extract_anchor_label(user_msg: str) -> str:
    """
    Conservative deterministic anchor extractor.
    We only anchor when the user asserts a concrete event that implies a start-time.
    """
    raw = (user_msg or "").strip()
    label = ""

    if raw:
        low = raw.lower()

        # Strong cue: "I just ..."
        m = re.search(
            r"\b(i\s+just\s+(?:put|started|began|set|placed|turned|took|left|arrived|threw))\s+(.+)$",
            low,
        )
        if m:
            tail = raw[m.start(2):].strip()
            tail = re.split(r"[.!?]\s+", tail, maxsplit=1)[0].strip()
            label = _time_anchor_label_from_text(tail)

        if not label:
            # Still useful: "I put/started/left..."
            m2 = re.search(
                r"\b(i\s+(?:put|started|began|set|placed|turned|took|left|arrived))\s+(.+)$",
                low,
            )
            if m2:
                tail = raw[m2.start(2):].strip()
                tail = re.split(r"[.!?]\s+", tail, maxsplit=1)[0].strip()
                # Avoid ultra-generic anchors like "I started"
                if len(tail.split()) >= 2:
                    label = _time_anchor_label_from_text(tail)

        if not label:
            # Explicit timer language
            if ("timer" in low) and (("start" in low) or ("started" in low) or ("set" in low)):
                label = _time_anchor_label_from_text(raw)

    return label

def _time_update_project_anchors(ctx: Any, project_full: str, *, label: str, now_dt: datetime, tz_name: str) -> None:
    """
    Append a time anchor to project_state[_TIME_ANCHOR_KEY], bounded + de-duped.
    """
    lbl = str(label or "").strip()

    # Registry-scan stability: avoid early return; gate the body instead.
    if lbl:
        try:
            st = ctx.project_store.load_project_state(project_full) or {}
        except Exception:
            st = {}
        if not isinstance(st, dict):
            st = {}

        anchors = st.get(_TIME_ANCHOR_KEY)
        if not isinstance(anchors, list):
            anchors = []

        should_skip = False

        # De-dupe: skip if same label within 2 minutes of last entry
        try:
            if anchors:
                last = anchors[-1] if isinstance(anchors[-1], dict) else {}
                last_label = str(last.get("label") or "").strip().lower()
                if last_label == lbl.lower():
                    last_ts = _time_parse_iso_noz(str(last.get("ts") or ""), tz_name)
                    if last_ts is not None and abs((now_dt - last_ts).total_seconds()) <= 120:
                        should_skip = True
        except Exception:
            should_skip = False

        if not should_skip:
            rec = {
                "label": lbl,
                "ts": now_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                "tz": tz_name,
            }
            anchors.append(rec)

            # Hard bound
            if len(anchors) > 8:
                anchors = anchors[-8:]

            try:
                ctx.project_store.write_project_state_fields(project_full, {_TIME_ANCHOR_KEY: anchors})
            except Exception:
                pass

    return

def _time_note_from_project_anchors(ctx: Any, project_full: str, *, now_dt: datetime, tz_name: str) -> str:
    """
    Build a tiny TIME_ANCHORS: line (system-only). Empty string if none.
    """
    out = ""
    st: Any = {}

    try:
        st = ctx.project_store.load_project_state(project_full) or {}
    except Exception:
        st = {}

    anchors: Any = []
    if isinstance(st, dict):
        anchors = st.get(_TIME_ANCHOR_KEY)

    if isinstance(anchors, list) and anchors:
        parts: List[str] = []
        for rec in anchors[-3:]:
            if not isinstance(rec, dict):
                continue
            lbl = str(rec.get("label") or "").strip()
            ts = str(rec.get("ts") or "").strip()
            tz0 = str(rec.get("tz") or tz_name).strip() or tz_name
            if not lbl or not ts:
                continue

            dt0 = _time_parse_iso_noz(ts, tz0) or _time_parse_iso_noz(ts, tz_name)
            if dt0 is None:
                continue

            mins = int(max(0, round((now_dt - dt0).total_seconds() / 60.0)))
            if mins < 60:
                ago = f"{mins}m ago"
            else:
                hrs = int(mins // 60)
                rem = int(mins % 60)
                ago = f"{hrs}h{rem:02d}m ago"

            parts.append(f"{lbl} ({ago})")

        if parts:
            out = "TIME_ANCHORS: " + "; ".join(parts)

    return out

def _time_context_system_note(ctx: Any, current_project_full: str) -> str:
    """
    Small always-on time awareness (system-only).
    Goal: feel human without bloating tokens or leaking timestamps into chat unless asked.
    """
    # Default (matches your product expectation)
    tz_name = "America/Chicago"

    # Best-effort: read user timezone + birthdate from Tier-2G profile (if present)
    user_seg = ""
    try:
        cp = str(current_project_full or "")
        user_seg = cp.split("/", 1)[0].strip() if "/" in cp else cp.strip()
    except Exception:
        user_seg = ""

    prof: Dict[str, Any] = {}
    try:
        if user_seg and hasattr(ctx, "project_store") and hasattr(ctx.project_store, "load_user_profile"):
            prof0 = ctx.project_store.load_user_profile(user_seg)
            if isinstance(prof0, dict):
                prof = prof0
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

    # Local time rendering
    dt = None
    abbr = ""
    try:
        if ZoneInfo is not None:
            z = ZoneInfo(tz_name)
            dt = datetime.now(tz=z)
            try:
                abbr = dt.tzname() or ""
            except Exception:
                abbr = ""
        else:
            dt = datetime.now()
    except Exception:
        dt = datetime.now()

    # Daypart (deterministic)
    h = 12
    try:
        h = int(getattr(dt, "hour", 12))
    except Exception:
        h = 12

    if 5 <= h < 12:
        daypart = "morning"
    elif 12 <= h < 17:
        daypart = "afternoon"
    elif 17 <= h < 22:
        daypart = "evening"
    else:
        daypart = "night"

    # Birthday flag (ONLY when true)
    birthday_today = False
    try:
        ident = prof.get("identity") if isinstance(prof, dict) else {}
        bd = ident.get("birthdate") if isinstance(ident.get("birthdate"), dict) else {}
        bd_val = str((bd or {}).get("value") or "").strip()  # expected ISO YYYY-MM-DD
        if bd_val and re.fullmatch(r"\d{4}-\d{2}-\d{2}", bd_val):
            mmdd = bd_val[5:]
            today_mmdd = dt.strftime("%m-%d")
            if mmdd == today_mmdd:
                birthday_today = True
    except Exception:
        birthday_today = False

    # Keep it tiny. One note + optional flag.
    try:
        stamp = dt.strftime("%Y-%m-%d %a %H:%M")
    except Exception:
        stamp = ""

    # TIME_RULE is the important behavioral fix (stops "I can't access your clock" refusals).
    lines: List[str] = []
    lines.append("TIME_RULE: If TIME_CONTEXT is present, you may answer current local time/date questions. Do NOT claim you lack access to the clock.")
    lines.append(f"TIME_CONTEXT: {stamp} {abbr}".strip() + f" ({tz_name}) • daypart={daypart}")
    if birthday_today:
        lines.append("TIME_FLAG: birthday_today=true")

    # Optional time anchors (ONLY when present; still system-only)
    try:
        anchor_note = _time_note_from_project_anchors(ctx, current_project_full, now_dt=dt, tz_name=tz_name)
    except Exception:
        anchor_note = ""
    if anchor_note:
        lines.append(anchor_note)

    return "\n".join(lines).strip()

def _read_user_global_facts_map_compact(ctx: Any, user_seg: str, cap_chars: int = 16000) -> str:
    """
    Deterministic, bounded read of GLOBAL Tier-2M facts for prompt injection.

    Preferred source (authoritative + compact):
      - ctx.project_store.render_user_global_facts_snippet_tier2m(user, max_items=...)

    Fallback (deterministic):
      - ctx.project_store.user_global_facts_map_path(user) -> read bounded JSON text
    """
    out = ""

    try:
        u = (user_seg or "").strip()

        if u:
            # Preferred: server-side renderer (compact, dedup-friendly)
            try:
                if hasattr(ctx, "project_store") and hasattr(ctx.project_store, "render_user_global_facts_snippet_tier2m"):
                    txt2 = ctx.project_store.render_user_global_facts_snippet_tier2m(u, max_items=18)
                    txt2 = (txt2 or "").strip()
                    if txt2:
                        out = txt2
            except Exception:
                pass

            # Fallback: deterministic Tier-2M path (avoid Tier-2G profile-path signals)
            if (not out) and hasattr(ctx, "project_store") and hasattr(ctx.project_store, "user_global_facts_map_path"):
                try:
                    p = ctx.project_store.user_global_facts_map_path(u)
                    if p and p.exists():
                        txt = p.read_text(encoding="utf-8", errors="replace") or ""
                        txt = txt.strip()
                        if txt:
                            out = txt
                except Exception:
                    pass

        if out and (len(out) > cap_chars):
            out = out[:cap_chars].rstrip() + "\n…(truncated)"

    except Exception:
        out = ""

    return out



def _build_lookup_memory_context(ctx: Any, current_project_full: str, user_text: str) -> str:
    """
    Bounded global memory context for lookup mode (Tier-2G + Tier-2M subset).
    Read-only, policy-aware; avoids injecting full project docs in lookup mode.
    """
    out: List[str] = []

    try:
        if hasattr(ctx, "project_store") and hasattr(ctx.project_store, "build_canonical_snippets"):
            snippets = ctx.project_store.build_canonical_snippets(
                current_project_full,
                intent="lookup",
                entities=[],
                user_text=(user_text or ""),
            ) or []
        else:
            snippets = []
    except Exception:
        snippets = []

    for s in snippets:
        if not isinstance(s, str):
            continue
        if s.startswith("[user_profile_global.json (Tier-2G)"):
            out.append(s)
            continue
        if s.startswith("[user_global_facts_map.json (Tier-2M subset)"):
            out.append(s)
            continue

    return "\n\n".join([x for x in out if x.strip()]).strip()


def _inject_global_user_memory_into_canonical(ctx: Any, current_project_full: str, canonical_snippets: List[str]) -> List[str]:
    """
    Seamless continuity: ensure Tier-2M global memory is available to the model for recall/status queries.
    Inject as an extra canonical snippet, bounded, deterministic.
    """
    try:
        cp = str(current_project_full or "")
        user_seg = cp.split("/", 1)[0].strip() if "/" in cp else cp.strip()
    except Exception:
        user_seg = ""

    try:
        blob = _read_user_global_facts_map_compact(ctx, user_seg, cap_chars=16000)
    except Exception:
        blob = ""

    if not blob:
        return canonical_snippets

    out = list(canonical_snippets or [])
    out.append("GLOBAL_FACTS_MAP_JSON (Tier-2M; derived from facts_raw):\n" + blob)
    return out

def _auto_resolve_user_preferred_name(ctx, user_seg: str) -> None:
    """
    DEPRECATED (foundational hygiene):
    Tier-2G identity kernel writes must be performed by deterministic server-side memory promotion,
    not by the request pipeline.

    This function is intentionally a no-op to prevent dual-writer drift.
    """
    return



_INTENT_CLASSIFIER_SYSTEM = """
You are an intent classifier for a project-centric assistant.

Return ONLY strict JSON, no prose:

{
  "intent": "recall | status | plan | execute | lookup | misc",
  "entities": ["..."],
  "scope": "current_project"
}

Rules:
- Greetings and small talk (e.g., a short hello) MUST be intent="misc".
- Questions about what the assistant/system can do (capabilities/usage) MUST be intent="misc" (not lookup).
- Requests to draft/write/send an email/message MUST be intent="execute" or "misc" (not lookup).
- "status" is ONLY for explicit commands like:
  "project pulse", "status", "project status", "resume"
- Vague progress questions like:
  "how’s it going", "how’s it coming along"
  MUST be intent="misc"
- Default scope is "current_project"
- Scope may change ONLY if the user explicitly switches projects
- entities: 0–8 short noun phrases from the user message
- Use intent="lookup" ONLY for general world knowledge

""".strip()

_CONTINUITY_CLASSIFIER_SYSTEM = """
You are a conversation continuity classifier for a project-centric assistant.

Return ONLY strict JSON:
{
  "continuity": "same_topic | new_topic | unclear",
  "followup_only": true | false,
  "topic": "short topic summary (<=12 words; empty if unclear)"
}

Rules:
- Default to continuity="same_topic" unless the latest user message clearly introduces a different topic.
- If the user explicitly signals a topic switch/unrelated/new topic, set continuity="new_topic".
- followup_only=true ONLY when the latest user message depends on prior context and does not add a new topic or substantive new subject.
- If continuity="new_topic", topic MUST summarize the new topic in a short phrase.
- If continuity="same_topic" and the user adds a new sub-question, topic MAY summarize the current topic.
- If uncertain, choose continuity="same_topic" and followup_only=true.
""".strip()
_GROUNDED_RESPONSE_SYSTEM = """
Your name is Quinn.
You are generating a grounded response using:
- CANONICAL_SNIPPETS as the ONLY source of truth for project facts (goals/decisions/status/constraints).
- RECENT_CHAT_TAIL ONLY for conversational continuity (pronouns, "how?", "that one", "do it").

Hard rules:
- You may NOT add project facts beyond CANONICAL_SNIPPETS.
- You MAY extract and restate values that are literally present in CANONICAL_SNIPPETS
  (names, ages, places, relationships), even if the question uses different wording.
  Example: if snippets say "You and Emanie actively work on your relationship", and the user asks
  "what's my girlfriend's name?", you may answer "Emanie" because the name is present in the snippets.
- If the needed value is not present in CANONICAL_SNIPPETS, say: "I don't have that yet."
- Do NOT write or invent Project Pulse, goals, or next actions unless they appear in CANONICAL_SNIPPETS.
- Do NOT claim you recorded/stored/logged/updated any decisions, facts, or project state.
  (Only deterministic server code can write canonical state.)
- If the user asks about something outside the current project scope, say it is outside scope and ask them to switch projects.
Return plain text only (no tags).
""".strip()


_HYBRID_GROUNDED_RESPONSE_SYSTEM = """
Your name is Quinn.
You are in HYBRID MODE.

You MAY use general knowledge for explanation.
But ANY project-specific facts (goal, decisions, constraints, project status) MUST come ONLY from CANONICAL_SNIPPETS.
You MAY use RECENT_CHAT_TAIL ONLY for conversational continuity (pronouns, short follow-ups).

Hard rules:
- If a project fact is not present in CANONICAL_SNIPPETS, say: "I don't have that yet."
- Do NOT claim you updated/recorded project state. Deterministic server code is the only writer.
Return plain text only (no tags).
""".strip()
_DEFAULT_EXPERT_SYSTEM = """
Your name is Quinn.
You are a capable human expert having a real conversation.

Operating principles:
- Be accurate, direct, and useful.
- Prioritize forward progress over sounding agreeable.
- If the user is wrong, say so plainly and respectfully.
- If information is missing, say so clearly rather than guessing.

Truth discipline:
- Do not invent facts.
- Distinguish clearly between what you know, what you infer, and what you don’t know.
- When offering an opinion or interpretation, mark it as such.
- When the user asks “is this worth it / is this a good idea / should I do this”, take a clear position based on what is known so far (not vibes).
  - Use language like: “Based on what you’ve told me so far…” / “Given what I know right now…”.
  - If you’re recommending against something *for now*, do not leak permission with phrases like “it can work” unless you immediately constrain it to a reversible test.
  - To keep it conversational and educational, end with ONE question in this form: “What would change my mind is X — do you have that?”

Identity continuity:
- If CANONICAL_SNIPPETS already contains a non-empty preferred name for the user (e.g., identity.preferred_name.value), use it without asking for confirmation.
- Only ask what name to use if no preferred name is available or the user explicitly indicates they want to change it.

Communication style:
- Speak like a person, not a specification.
- Do not explain your rules, roles, or internal processes.
Formatting rule:
- If you use a numbered list, number items sequentially (1, 2, 3).
- Do not repeat the same number for multiple items.
- Avoid frameworks, rubrics, or enumerated lists unless they materially improve clarity or the user asked for structure.
- Use lists only when they help; otherwise prefer short paragraphs.

Tone constraints:
- No emojis.
- No therapy or motivational language.
- No “as an AI” phrasing.
List usage rule:
- Do not use numbered or bulleted lists by default.
- Use lists only when:
  a) the user explicitly asks for steps, structure, or an outline, OR
  b) the content is procedural and ambiguous without ordering.

Return plain text only (no tags).
""".strip()

_CONVERSATIONAL_EXPERT_SYSTEM = """
Your name is Quinn.
You are the active project expert having a natural conversation.

Use CANONICAL_SNIPPETS as persistent background context:
- Treat CANONICAL_SNIPPETS as "my notes about this person/project".
- You may freely speak and reason like a human expert (therapist/programmer/architect/etc.), using those notes.

Truth / memory rules (important):
- Do NOT invent or fabricate facts that are not in CANONICAL_SNIPPETS.

Identity continuity (important):
- If CANONICAL_SNIPPETS already contains a non-empty preferred name for the user (e.g., identity.preferred_name.value), use it without asking for confirmation.
- Only ask what name to use if no preferred name is available or the user explicitly indicates they want to change it.
- You MAY make interpretations, hypotheses, and opinions, but you MUST clearly mark them as such
  (e.g., "my guess is...", "it sounds like...", "one possibility is...", "in my opinion...").
- Do NOT say (or imply) that an interpretation/opinion is a stored/confirmed project fact.
- If the user asks for a specific factual recall about them/the project and it is not in CANONICAL_SNIPPETS,
  say you don't have that recorded yet and ask one clarifying question.

Conversation quality:
- Be human and helpful. Do not default to refusal language.
List usage rule:
- Avoid numbered or bulleted lists unless they materially improve clarity.
- Prefer short paragraphs for normal conversational responses.

- For therapy-style questions (feelings, relationships, motives), prefer reflective responses.
- Ask at most one question unless the user explicitly requests an interview-style intake.

Return plain text only (no tags).
""".strip()

_COUPLES_THERAPIST_SYSTEM = """
Your name is Quinn.
You are a couples therapist helping two partners improve communication, reduce recurring conflict patterns, and repair ruptures.

You may have access to private notes from each partner (via CANONICAL_SNIPPETS). HARD RULE: NEVER reveal, quote, attribute, or confirm any private content.
- Do NOT say or imply: "she said", "he told me", "I read", "from your partner's notes", "I know because", or anything that reveals private provenance.
- If asked to reveal private content, refuse briefly and redirect to a safe process (themes, needs, consent-based conversation prompts).

Privacy / non-disclosure (hard):
- No direct quotes from either partner’s private space.
- No identifying “who said what.”
- Convert private info into neutral themes and permission-based invitations.
  Example: "A theme that may be worth exploring is feeling unheard around planning. Would it be okay to talk about that?"

Non-weaponization (hard):
- Do not take sides. Do not arbitrate who is right/wrong.
- Do not help one partner “win.” Do not draft attacks, ultimatums, or manipulative scripts.
- Validate emotions without endorsing accusations as facts. Avoid mind-reading.

Epistemic discipline (couples):
- Treat event claims as ONE-SIDED unless corroborated by BOTH partners (directly or indirectly).
- Indirect corroboration counts if both describe the same event in different words (e.g., "you yelled" vs "I raised my voice").
- If only one side mentions an event, label it as one perspective and seek the other side before treating it as fact.
- Feelings are valid as experience; they do not establish facts about intent or events.
- Use COUPLES_SHARED_MEMORY for shared agreements:
  - "corroborated" agreements can be treated as shared facts.
  - "proposed" agreements are tentative; confirm before treating as fact.

Clinical approach (explicit disciplines):
- Primary: Integrative couples therapy using (1) Gottman-style skills (conflict management, repair attempts, soft start-ups) and (2) EFT-style cycle framing (attachment needs; pursue/withdraw patterns).
Formulation style (first-person mirroring — REQUIRED):
- When describing patterns or protective moves, speak in first-person mirroring rather than abstract role labels.
- Prefer language like:
  - “When you push for clarity, it’s often because you’re trying to protect…”
  - “When you pull back, it’s often because you’re trying to protect…”
- Avoid labels such as “the pursuing partner,” “the withdrawing side,” “the hurt/angry side,” or similar role abstractions.
- Keep formulations grounded in lived experience and felt intent, not categories.
- This is not casual language; it is deliberate clinical mirroring to increase felt understanding.
- Toolbelt (use as needed): Motivational Interviewing (reduce resistance, evoke change talk), CBT-style reframes (interpretations vs facts; attribution errors), NVC-style clean requests (feelings/needs/requests without blame), DBT interpersonal effectiveness (clear asks + boundaries), Solution-Focused micro-steps (small wins this week).

Tone baseline (anti-sycophantic):
- Kind, steady, reality-based. Do NOT sugarcoat. Avoid flattery and excessive validation.
- Be direct about patterns that make things worse (criticism, defensiveness, stonewalling, contempt, escalation, mind-reading) and offer healthier alternatives.
- Keep warmth without pep-talks. Respectfully challenge distortions and unhelpful strategies.

Default session loop (strong default):
1) Reflect the emotional truth briefly (1–2 lines). No flattery.
2) Before naming a pattern, add one brief epistemic softener (clinical humility),
   such as: “Tell me if this fits,” “Based on what you’ve shared so far,” or
   “Here’s what I think may be happening—correct me if I’m off.”
   This is not deference; it is professional calibration.
3) Identify the interaction cycle/pattern and the likely protective moves on both sides.
4) Choose ONE intervention from the toolbelt and explain it plainly.
5) Give ONE small concrete next step (10–15 minutes max).
6) Ask ONE gentle, high-leverage question.

Intake behavior (when key basics are missing from CANONICAL_SNIPPETS):
- Ask one starter question at a time, like a real therapist.
- Examples: preferred names, relationship length, main conflict themes, what “better” would look like.
- Do not interrogate; ask only what is necessary to proceed.

Truth-bound:
- Any project-specific facts must come from CANONICAL_SNIPPETS.
- If something is not in CANONICAL_SNIPPETS, say you don't have it recorded yet and ask a single targeted question to fill the gap.

Output:
- Return plain text only.
- End like a therapist: one small next step + one question (no “deliverables”, no format selection).
"""

_TREBEK_CADENCE_BLOCK = """
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

_CAPABILITY_USE_BLOCK = """
Capability awareness (required):
- You know you can search the web, read uploads, write artifacts (docs/excel/html), and ask one clarifying question.
- If a capability would materially improve accuracy or clarity, either use it automatically (for search)
  or propose it briefly in one sentence and proceed unless blocked.
- Example: if the user wants calculations/structured comparisons, offer an Excel sheet.
- Example: if they want something printable, offer a clean document deliverable.
- Keep capability suggestions human and brief (no system talk).
""".strip()

_ANALYSIS_HAT_LEGAL_OPS_NOTE = """
ANALYSIS_HAT_LEGAL_OPS:
- Act like a senior legal-ops team on a complex case. Be rigorous, practical, and specific.
- Treat uploads as discovery: summarize, classify, extract entities/dates/amounts, and track issues/weakpoints.
- Maintain a living case file: discovery index, timeline, issues/questions, evidence matrix, fact ledger, and conflict report.
- Ask targeted questions when key facts are missing; avoid generic intake.
- Use structured output when it improves clarity (tables, short lists).
""".strip()

_GROUNDED_RESPONSE_SYSTEM = _GROUNDED_RESPONSE_SYSTEM + "\n\n" + _TREBEK_CADENCE_BLOCK
_HYBRID_GROUNDED_RESPONSE_SYSTEM = _HYBRID_GROUNDED_RESPONSE_SYSTEM + "\n\n" + _TREBEK_CADENCE_BLOCK
_DEFAULT_EXPERT_SYSTEM = _DEFAULT_EXPERT_SYSTEM + "\n\n" + _TREBEK_CADENCE_BLOCK
_CONVERSATIONAL_EXPERT_SYSTEM = _CONVERSATIONAL_EXPERT_SYSTEM + "\n\n" + _TREBEK_CADENCE_BLOCK
_COUPLES_THERAPIST_SYSTEM = _COUPLES_THERAPIST_SYSTEM.strip() + "\n\n" + _TREBEK_CADENCE_BLOCK

_GROUNDED_RESPONSE_SYSTEM = _GROUNDED_RESPONSE_SYSTEM + "\n\n" + _CAPABILITY_USE_BLOCK
_HYBRID_GROUNDED_RESPONSE_SYSTEM = _HYBRID_GROUNDED_RESPONSE_SYSTEM + "\n\n" + _CAPABILITY_USE_BLOCK
_DEFAULT_EXPERT_SYSTEM = _DEFAULT_EXPERT_SYSTEM + "\n\n" + _CAPABILITY_USE_BLOCK
_CONVERSATIONAL_EXPERT_SYSTEM = _CONVERSATIONAL_EXPERT_SYSTEM + "\n\n" + _CAPABILITY_USE_BLOCK
_COUPLES_THERAPIST_SYSTEM = _COUPLES_THERAPIST_SYSTEM + "\n\n" + _CAPABILITY_USE_BLOCK

_BOOTSTRAP_INTENT_SYSTEM = """
You are synthesizing project bootstrap intent.

Return ONLY strict JSON, no prose:

{
  "candidate_goal": "<1 sentence>",
  "candidate_project_mode": "open_world | closed_world | hybrid",
  "candidate_domains": ["..."],
  "questions": ["..."]
}

Rules:
- Max 2 questions total.
- candidate_goal must be <= 1 sentence (no newlines).
- candidate_project_mode must be exactly one of: open_world, closed_world, hybrid.
- candidate_domains: 0-8 short tags (snake_case preferred).
- If unclear, ask 1-2 clarification questions, otherwise questions=[].
""".strip()


def _is_affirmation_bootstrap(msg: str) -> bool:
    s = (msg or "").strip().lower()
    s = re.sub(r"[.!?]+$", "", s).strip()
    return s in ("yes", "y", "yep", "yup", "yeah", "confirm", "confirmed", "correct", "ok", "okay", "sounds good", "do it", "go ahead")


def _is_negative_bootstrap(msg: str) -> bool:
    s = (msg or "").strip().lower()
    return s.startswith(("no", "nope", "nah"))
# (dedupe) Expert Frame Lock (EFL v1) — deterministic inference helpers removed.
# Canonical EFL implementation is defined below.

# ---------------------------------------------------------------------------
# Expert Frame Lock (EFL v1) — deterministic inference + command parsing
# ---------------------------------------------------------------------------

_EFL_ALLOWED_STATUSES = ("proposed", "active")

def _efl_normalize_frame(obj: Any) -> Dict[str, str]:
    ef = obj if isinstance(obj, dict) else {}
    st = str(ef.get("status") or "").strip().lower()
    if st not in _EFL_ALLOWED_STATUSES:
        st = ""
    return {
        "status": st,
        "label": str(ef.get("label") or "").strip(),
        "directive": str(ef.get("directive") or "").strip(),
        "set_reason": str(ef.get("set_reason") or "").strip(),
        "updated_at": str(ef.get("updated_at") or "").strip(),
    }

def _efl_parse_set_cmd(user_msg: str) -> str:
    """
    Deterministic set-frame command:
      - "expert frame: <label>"
      - "set expert frame: <label>"
      - "change expert frame to <label>"
      - "switch frame to <label>"
    Returns label or "".
    """
    t = (user_msg or "").strip()
    low = t.lower()

    prefixes = (
        "expert frame:",
        "set expert frame:",
        "set frame:",
        "change expert frame to ",
        "switch expert frame to ",
        "change frame to ",
        "switch frame to ",
    )

    for p in prefixes:
        if low.startswith(p):
            out = t[len(p):].strip()
            # Keep it single-line and bounded
            out = re.sub(r"[\r\n]+", " ", out).strip()
            if len(out) > 80:
                out = out[:80].rstrip() + "…"
            return out

    return ""

def _efl_infer_candidate(
    *,
    project_full: str,
    project_state: Dict[str, Any],
    user_msg: str,
) -> Dict[str, str]:
    """
    Deterministic, bounded inference.
    Returns a normalized candidate frame dict (status unset here).
    """
    goal = str((project_state or {}).get("goal") or "").lower()
    doms = (project_state or {}).get("domains")
    if not isinstance(doms, list):
        doms = []
    dom_blob = " ".join([str(x) for x in doms]).lower()

    msg = (user_msg or "").lower()
    blob = " ".join([project_full.lower(), goal, dom_blob, msg])

    # Small, deterministic frame library (v1)
    # Keep directives as "how to optimize WIN", not roleplay persona.
    if any(k in blob for k in ("prompt", "retrieval", "pipeline", "memory", "canonical", "project_store", "model_pipeline", "state")):
        return {
            "label": "Systems Architect (stateful assistants)",
            "directive": "Optimize for durable, inspectable state; minimize drift; prefer deterministic routing and bounded retrieval; take action first then ask one high-leverage question; never invent project facts.",
            "set_reason": "keyword_match:systems_assistant_build",
        }

    if any(k in blob for k in ("renovation", "tile", "floor", "shower", "contractor", "estimate", "bid", "material")):
        return {
            "label": "Renovation PM (scope/cost/decisions)",
            "directive": "Optimize for clear decisions, cost/lead-time tradeoffs, and de-risking unknowns; extract options; propose next steps; keep a tight decision log; ask one clarifier only when it unblocks ordering or spend.",
            "set_reason": "keyword_match:renovation",
        }

    if any(k in blob for k in ("spreadsheet", "excel", "workbook", "ledger", "formula", "pivot")):
        return {
            "label": "Data Analyst (spreadsheets/logic)",
            "directive": "Optimize for correctness and traceability; preserve structure; compute and reconcile; highlight assumptions; produce artifacts that are easy to audit; ask one clarifier only when it affects computation.",
            "set_reason": "keyword_match:excel",
        }

    if any(k in blob for k in ("write", "rewrite", "draft", "tone", "copy", "report", "deliverable")):
        return {
            "label": "Editor (clarity and outcomes)",
            "directive": "Optimize for clarity, structure, and persuasive framing; propose a strong default; keep scope tight; ask one clarifier only when it changes audience or objective.",
            "set_reason": "keyword_match:writing",
        }

    return {
        "label": "General Project Operator",
        "directive": "Optimize for forward progress; ground all project facts in canonical snippets; propose a concrete next action; ask one high-leverage question only when blocked.",
        "set_reason": "fallback",
    }


def _safe_json_extract(s: str) -> Dict[str, Any]:
    raw = (s or "").strip()
    try:
        # best-effort: first {...} block
        m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if m:
            obj = json.loads(m.group(0))
            return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}
    try:
        obj2 = json.loads(raw)
        return obj2 if isinstance(obj2, dict) else {}
    except Exception:
        return {}


def _capability_gap_meaningful(obj: Dict[str, Any]) -> bool:
    if not isinstance(obj, dict):
        return False
    if bool(obj.get("blocked")):
        return True
    for k in ("limitations", "missing_capabilities", "suggested_features", "needed_inputs"):
        v = obj.get(k)
        if isinstance(v, list) and v:
            return True
    return False


def _render_capability_gap_note(obj: Dict[str, Any]) -> str:
    if not isinstance(obj, dict):
        return ""
    lims = obj.get("limitations") if isinstance(obj.get("limitations"), list) else []
    feats = obj.get("suggested_features") if isinstance(obj.get("suggested_features"), list) else []
    needs = obj.get("needed_inputs") if isinstance(obj.get("needed_inputs"), list) else []

    lim = str(lims[0] if lims else "").strip()
    feat = str(feats[0] if feats else "").strip()
    need = str(needs[0] if needs else "").strip()

    lines: List[str] = []
    if lim:
        lines.append(f"Limits right now: {lim}.")
    if feat:
        lines.append(f"What would make this production-ready: {feat}.")
    if (not feat) and need:
        lines.append(f"What would make this production-ready: {need}.")
    return " ".join([ln for ln in lines if ln]).strip()


def _normalize_exact_cmd(text: str) -> str:

    """
    Deterministic normalization for exact command matching:
    - case-insensitive
    - whitespace-insensitive
    """
    return " ".join((text or "").strip().lower().split())


_PULSE_EXACT_CMDS = {
    "project pulse",
    "pulse",
    "status",
    "project status",
    "resume",
    "show status",
    "show project status",
    "project resume",
}

def _is_exact_pulse_cmd(text: str) -> bool:
    """
    Deterministic pulse/status command matcher (FOUNDATIONAL).

    HARD RULE:
    - Project Pulse may ONLY trigger on explicit, short, meta-level requests.
    - Long narrative sentences must NEVER trigger pulse, even if they contain 'status'.
    """
    t = _normalize_exact_cmd(text)
    if not t:
        return False

    # Guard 1: length — pulse is a command, not a paragraph
    if len(t) > 60:
        return False

    # Exact short forms
    if t in _PULSE_EXACT_CMDS:
        return True

    # Require an explicit meta verb
    has_meta_verb = any(v in t for v in (
        "what",
        "show",
        "give",
        "get",
        "resume",
    ))

    if not has_meta_verb:
        return False

    # Require explicit project framing
    if not ("project" in t or "here" in t or "this" in t):
        return False

    # Explicit pulse/status mention
    if re.search(r"\b(pulse|status)\b", t):
        # Exclude common non-project meanings
        if re.search(r"\b(out of status|immigration status|legal status)\b", t):
            return False
        return True

    return False


def _is_inbox_query(text: str) -> bool:
    """
    Deterministic matcher for inbox/pending/remaining work.
    No model call. Conservative but less "magic word" brittle.
    """
    t = _normalize_exact_cmd(text)
    if not t:
        return False

    # Hard exclusions: keep conversational prompts out of inbox routing
    if "what should i do next" in t or "what should i do" in t:
        return False

    # Exact short forms
    if t in ("inbox", "pending", "todo", "to do", "to-do", "backlog", "queue"):
        return True

    # Remaining-work phrasing
    if any(p in t for p in (
        "what's left",
        "whats left",
        "what is left",
        "anything left",
        "anything pending",
        "anything open",
        "remaining items",
        "pending items",
        "open items",
        "left to do",
        "left to complete",
    )):
        return True

    # Verb + object variants
    if re.search(r"\b(show|list|summarize|review)\b", t) and re.search(r"\b(inbox|pending|open items|remaining)\b", t):
        return True

    # Prefix forms
    if t.startswith(("inbox ", "pending ", "backlog ", "queue ")):
        return True

    return False


def _looks_like_image_referential_turn(user_msg: str) -> bool:
    """
    Conservative deterministic check:
    - If the user did NOT explicitly name a file, but uses referential language,
      treat as referring to the active object (if it is an image).
    """
    t = (user_msg or "").strip().lower()
    if not t:
        return False

    # If the user explicitly named a file, we treat that as explicit (not purely referential).
    try:
        if re.search(r"[\w./-]+\.(?:pdf|png|jpg|jpeg|webp|gif|csv|xlsx|xlsm|xls|docx|doc)\b", t, flags=re.IGNORECASE):
            return False
    except Exception:
        pass

    triggers = (
        "this image",
        "that image",
        "this screenshot",
        "that screenshot",
        "this photo",
        "that photo",
        "this picture",
        "that picture",
        "this one",
        "that one",
        "look at this",
        "look at that",
        "what does this show",
        "what's shown",
        "whats shown",
        "describe this",
        "summarize this",
        "tell me about this",
        "what is this",
        "what is in this",
        "what's in this",
        "whats in this",
        "what does it say",
        "can you read this",
        "can you read that",
    )
    return any(x in t for x in triggers)
# ---------------------------------------------------------------------------
# CCG — Context Commitment Gate (FOUNDATIONAL, deterministic, read-time only)
#
# Goal:
# - Once a user has committed a "domain + target + goal" frame in the conversation,
#   NEVER ask scope-resetting questions like:
#     "a build of what?" / "which game?" / "what platform?"
# - Instead: produce a best-effort answer first, then ask ONE refinement question.
#
# Hard rules:
# - No model calls
# - No canonical writes
# - No magic words / brittle exact phrases
# - Works across domains (games, configs, tiers, optimization, etc.)
# ---------------------------------------------------------------------------

def _ccg_norm(s: str) -> str:
    try:
        return " ".join((s or "").strip().split())
    except Exception:
        return (s or "").strip()

def _ccg_extract_committed_frame(
    conversation_history: List[Dict[str, str]],
    clean_user_msg: str,
) -> Dict[str, str]:
    """
    Return a minimal committed frame:
      { "domain": "...", "target": "...", "goal": "..." }
    Any field may be "" when not reliably inferred.

    Deterministic heuristics:
    - Domain: look for "I'm playing <X>" / "I’m playing <X>" / "playing <X>" patterns in recent USER turns
    - Target: look for a proper-noun-ish referent in recent USER turns when they say "build <X>" or "the <X>"
      (bounded; no global NER)
    - Goal: detect optimization intent like max damage / DPS / best / optimal / tier list / config
    """
    domain = ""
    target = ""
    goal = ""

    msg = _ccg_norm(clean_user_msg)
    low_msg = msg.lower()

    # Build a small recent window (bounded)
    recent_user: List[str] = []
    try:
        for m in (conversation_history or [])[-18:]:
            if isinstance(m, dict) and str(m.get("role") or "") == "user":
                t = _ccg_norm(str(m.get("content") or ""))
                if t:
                    recent_user.append(t)
    except Exception:
        recent_user = []

    # Always include current message in the scan
    if msg:
        recent_user.append(msg)

    # --- Goal detection (broad, non-phrase-locked; token scoring) ---
    try:
        toks = set(re.findall(r"[a-z0-9]{3,}", low_msg))
        g_score = 0

        # Optimization intent signals (generic across domains)
        for w in ("best", "optimal", "maximize", "maximum", "dps", "damage", "tier", "meta", "build", "loadout", "config", "settings"):
            if w in toks:
                g_score += 1

        # "tier list" combined signal
        if ("tier" in toks and "list" in toks) or ("tierlist" in toks):
            g_score += 2

        # Treat as goal if there is meaningful optimization signal
        if g_score >= 2:
            goal = "optimization"
    except Exception:
        goal = ""

    # --- Domain extraction (recent "I'm playing ..." patterns) ---
    # We keep this conservative and bounded: only pull the tail as the domain string.
    try:
        rx = re.compile(r"\b(i['’]m|i am)\s+playing\s+(.+)$", re.IGNORECASE)
        for t in reversed(recent_user[-10:]):
            m = rx.search(t)
            if m:
                tail = (t[m.start(2):] or "").strip()
                # clip at sentence boundary
                tail = re.split(r"[.!?]\s+", tail, maxsplit=1)[0].strip()
                # keep it bounded
                tail = _ccg_norm(tail)
                if 3 <= len(tail) <= 80:
                    domain = tail
                    break
    except Exception:
        domain = ""

    # --- Target extraction (bounded "build X" / "the X" in recent turns) ---
    # We avoid brittle phrase lists by using a structural "noun tail" capture after common build referents.
    try:
        rx2 = re.compile(r"\b(?:build|building)\s+([A-Za-z0-9][A-Za-z0-9\s'’:_-]{1,60})$", re.IGNORECASE)
        for t in reversed(recent_user[-10:]):
            m = rx2.search(t)
            if m:
                tail = _ccg_norm(m.group(1))
                tail = re.split(r"[.!?]\s+", tail, maxsplit=1)[0].strip()
                # prune generic tails
                low_tail = tail.lower()
                if low_tail in ("him", "her", "it", "this", "that"):
                    continue
                if 2 <= len(tail) <= 64:
                    target = tail
                    break
    except Exception:
        target = ""

    # If target still empty, try "the <X>" style (common after prior context)
    if not target:
        try:
            rx3 = re.compile(r"\bthe\s+([A-Za-z0-9][A-Za-z0-9\s'’:_-]{2,60})\s*(?:that|who)?\s*(?:was\s+just\s+released|new|latest)?\b", re.IGNORECASE)
            for t in reversed(recent_user[-10:]):
                m = rx3.search(t)
                if m:
                    tail = _ccg_norm(m.group(1))
                    low_tail = tail.lower()
                    if low_tail in ("build", "game", "platform"):
                        continue
                    if 2 <= len(tail) <= 64:
                        target = tail
                        break
        except Exception:
            target = ""

    return {"domain": domain, "target": target, "goal": goal}

def _ccg_system_note(conversation_history: List[Dict[str, str]], clean_user_msg: str) -> str:
    """
    System-only note:
    - Prevents scope reset once context is committed.
    - Enforces "answer first, then one refinement question" in crowd-knowledge optimization.
    """
    fr = _ccg_extract_committed_frame(conversation_history, clean_user_msg)
    domain = str(fr.get("domain") or "").strip()
    target = str(fr.get("target") or "").strip()
    goal = str(fr.get("goal") or "").strip()

    # Consider context "committed" if we have at least (domain OR target) and an optimization goal.
    committed = bool(goal == "optimization" and (domain or target))

    if not committed:
        return ""

    # Keep it short; no lists unless necessary.
    parts: List[str] = []
    parts.append("CCG_CONTEXT_COMMITMENT (FOUNDATIONAL):")
    if domain:
        parts.append(f"- Domain is already established: {domain}")
    if target:
        parts.append(f"- Target is already established: {target}")
    parts.append("- Do NOT ask scope-resetting questions (e.g., 'a build of what?' / 'which game/platform?').")
    parts.append("- Give a best-effort answer first (provisional consensus is allowed), then ask ONE refinement question.")
    parts.append("- Treat crowd-knowledge as noisy: summarize consensus + disagreement; do not launder into certainty.")
    return "\n".join(parts).strip()
def _ccg_consensus_opening_note(conversation_history: List[Dict[str, str]], clean_user_msg: str) -> str:
    """
    FOUNDATIONAL: When the user asks for crowd-knowledge optimization ("best/meta/tier/build/config"),
    require: lead with the working consensus first, then qualify.
    This prevents academic preambles ("can't state confidently...") that reduce usefulness.
    """
    fr = _ccg_extract_committed_frame(conversation_history, clean_user_msg)
    domain = str(fr.get("domain") or "").strip()
    target = str(fr.get("target") or "").strip()
    goal = str(fr.get("goal") or "").strip()

    committed = bool(goal == "optimization" and (domain or target))
    if not committed:
        return ""

    # Keep it short and behavioral (no new modes).
    return "\n".join([
        "CONSENSUS_FIRST_OPENING (FOUNDATIONAL):",
        "- Start with the best working consensus answer immediately (1–2 sentences).",
        "- THEN add uncertainty/disagreement bands and what would change your mind.",
        "- Do NOT lead with academic disclaimers (e.g., 'can't be stated confidently without telemetry/surveys').",
        "- In crowd-knowledge domains, missing perfect evidence is not a blocker; treat it as refinement.",
    ]).strip()
def _cksg_is_crowd_knowledge_query(user_msg: str) -> bool:
    """
    Deterministic crowd-knowledge / optimization intent detector.
    Not a magic-word gate: uses token scoring with broad signals.
    """
    msg = (user_msg or "").strip().lower()
    if not msg:
        return False

    toks = set(re.findall(r"[a-z0-9]{3,}", msg))
    score = 0

    # Optimization / consensus asks (broad)
    for w in ("best", "optimal", "maximize", "maximum", "meta", "tier", "build", "loadout", "config", "settings", "consensus", "most"):
        if w in toks:
            score += 1

    if ("tier" in toks and "list" in toks) or ("tierlist" in toks):
        score += 2

    # Enough signal = treat as crowd-knowledge query.
    return score >= 2
def _requires_status_synthesis(user_msg: str) -> bool:
    """
    FOUNDATIONAL, deterministic.

    Purpose:
    - Prevent "defensive collapse" answers for ANY domain where the user is asking for
      the *current state of the world* (latest/current/status/what happened/confirmed?).
    - This is not vendor-specific and not a one-off fix.

    Trigger principle (structural, not brittle):
    - The user is asking for *recency / status / confirmation / announcement / partnership / integration*
      AND the message looks like a world-fact query (short-ish, interrogative or status wording).
    """
    raw = (user_msg or "").strip()
    if not raw:
        return False

    # Keep bounded: long multi-part prompts usually need normal handling.
    if len(raw) > 420:
        return False

    low = raw.lower()

    # Recency / status / confirmation intent (broad, non-domain-specific)
    statusish = (
        ("latest" in low)
        or ("current" in low)
        or ("right now" in low)
        or ("as of" in low)
        or ("status" in low)
        or ("update" in low)
        or ("news" in low)
        or ("what happened" in low)
        or ("what’s going on" in low)
        or ("whats going on" in low)
        or ("did " in low and "confirm" in low)   # confirmation-shaped questions
        or ("announce" in low)                    # announced/announcement
        or ("confirmed" in low)
        or ("official" in low)
    )

    if not statusish:
        return False

    # World-fact query shape: interrogative, or short declarative ask.
    looks_like_question = raw.endswith("?") or bool(re.match(r"^\s*(what|who|when|where|why|how|did|is|are|has|have)\b", low))

    # Integration/partnership/product-state smell (broad)
    # NOTE: This is not a one-off list; it's general nouns for how product status is discussed.
    integrationish = any(k in low for k in (
        "integration", "integrated", "partnership", "partner", "deal",
        "support", "supported", "rollout", "release", "beta", "availability",
        "feature", "launch", "timeline",
        "in talks", "talks", "negotiations",
        "option", "toggle", "handoff", "routing",
    ))

    # If it’s explicitly a status/recency ask and it looks like a question, synthesize.
    if looks_like_question:
        return True

    # If not phrased as a question, require some integration/product-state smell.
    if integrationish and len(raw) <= 260:
        return True

    return False


def _status_synthesis_system_note() -> str:
    """
    FOUNDATIONAL: enforce epistemically-complete answers for "current status" queries.

    This is not a special-case for any topic. It applies anywhere the user asks for
    the latest/current state of an external reality claim.
    """
    return "\n".join([
        "STATUS_SYNTHESIS_OBLIGATION (FOUNDATIONAL):",
        "- The user is asking for the CURRENT / LATEST state of a real-world claim.",
        "- You MUST distinguish between:",
        "  (A) CONFIRMED BY CREDIBLE REPORTING (named, dated outlets stating the fact), and",
        "  (B) OFFICIALLY ANNOUNCED BY PRIMARY SOURCES (e.g., Apple/Google press, docs).",
        "- CONFIRMED BY CREDIBLE REPORTING is sufficient to state a fact as confirmed.",
        "- OFFICIAL ANNOUNCEMENT is a stronger category, but NOT required to state confirmation.",
        "",
        "- You MUST structure the answer into these buckets when applicable:",
        "  1) CONFIRMED (credible reporting): what reputable outlets explicitly state, with dates.",
        "  2) OFFICIALLY ANNOUNCED (primary source): what the company itself has formally announced, if any.",
        "  3) REPORTED / DETAILS: timelines, scope, rollout specifics from reporting.",
        "  4) UNKNOWN: what remains unclear or not yet specified.",
        "",
        "- You MUST NOT say 'no evidence' or 'no snippets' if SEARCH_EVIDENCE_EXCERPT or fetched_pages are present.",
        "- If credible reporting explicitly states a deal/integration exists, you MUST say so.",
        "- Do NOT downgrade explicit reporting to 'inference' solely because a press release is absent.",
        "- End with at most ONE refinement question only if it materially changes the answer.",
    ]).strip()

def _build_thread_synthesis_from_search_results(search_results: str, max_items: int = 4) -> str:
    """
    Deterministic, evidence-bound thread synthesis.
    Uses ONLY SEARCH_EVIDENCE_JSON content (no model calls).
    """
    try:
        obj = _safe_json_extract(search_results or "")
    except Exception:
        obj = {}

    if not isinstance(obj, dict) or str(obj.get("schema") or "") != "search_evidence_v1":
        return ""

    results = obj.get("results")
    if not isinstance(results, list) or not results:
        return ""

    lines: List[str] = []

    try:
        auth = obj.get("authority") if isinstance(obj.get("authority"), dict) else {}
        level = str((auth or {}).get("level") or "").strip()
        if level:
            lines.append(f"Authority level: {level}")
    except Exception:
        pass

    try:
        if obj.get("insufficient") is True:
            lines.append("Evidence marked insufficient for the intended query.")
    except Exception:
        pass

    kept = 0
    for it in results:
        if kept >= int(max_items):
            break
        if not isinstance(it, dict):
            continue
        rank = it.get("rank")
        title = str(it.get("title") or "").strip()
        snippet = str(it.get("snippet") or it.get("description") or "").strip()

        if not (title or snippet):
            continue

        if snippet and len(snippet) > 240:
            snippet = snippet[:240].rstrip() + "..."

        label = f"[r{rank}]" if rank is not None else "[r?]"
        if title and snippet:
            line = f"{label} {title} — {snippet}"
        elif title:
            line = f"{label} {title}"
        else:
            line = f"{label} {snippet}"

        lines.append(line)
        kept += 1

    if not lines:
        return ""

    return "\n".join(f"- {ln}" for ln in lines).strip()


def _cksg_stall_reason(*, committed: bool, user_msg: str, output_text: str) -> str:
    """
    FOUNDATIONAL: Detect the forbidden failure mode:
    - Context is committed
    - User asked for consensus/best/meta/build/config
    - Model leads with 'can't responsibly / can't verify / no telemetry / need more info'
      instead of giving a provisional consensus answer first.

    Returns "" if OK, else a short reason key.
    """
    if not committed:
        return ""

    if not _cksg_is_crowd_knowledge_query(user_msg):
        return ""

    out = (output_text or "").strip()
    if not out:
        return "empty"

    low = out.lower()

    # Detect "stall" phrasing anywhere (these are answer-blockers, not uncertainty qualifiers)
    stall_markers = (
        "i can't responsibly",
        "i cant responsibly",
        "i can't safely",
        "i cant safely",
        "i can't verify",
        "i cant verify",
        "cannot verify",
        "can't be stated confidently",
        "cant be stated confidently",
        "without telemetry",
        "without current telemetry",
        "without survey",
        "no live patch notes",
        "weren't provided",
        "were not provided",
        "aren’t present",
        "arent present",
        "not present here",
        "i need one detail",
        "i need one piece of context",
        "i need more info",
        "i need more information",
        "before i can",
        "so i can't",
        "so i cant",
        "i can’t",
        "i cant",
    )

    if any(m in low for m in stall_markers):
        return "stall_language"

    # Also block "hedge-first" openings (first ~220 chars)
    head = low[:220]
    hedge_first = (
        "there isn't a single" in head
        or "there isnt a single" in head
        or "cannot be stated" in head
        or "can't be stated" in head
        or "cant be stated" in head
    )
    if hedge_first:
        return "hedge_first_opening"

    return ""
def _ckcl_committed_and_crowd(*, conversation_history: List[Dict[str, str]], clean_user_msg: str) -> bool:
    """
    Deterministic activation for CKCL:
    - Context is committed via CCG frame extraction
    - User is asking for crowd-knowledge optimization/consensus
    """
    try:
        fr = _ccg_extract_committed_frame(conversation_history or [], clean_user_msg or "")
        domain = str(fr.get("domain") or "").strip()
        target = str(fr.get("target") or "").strip()
        goal = str(fr.get("goal") or "").strip()
        committed = bool(goal == "optimization" and (domain or target))
        if not committed:
            return False
        return bool(_cksg_is_crowd_knowledge_query(clean_user_msg or ""))
    except Exception:
        return False

def _ckcl_system_lock_note() -> str:
    """
    HARD constraint injected BEFORE generation when CKCL is active.
    This is not style guidance — it forbids refusal-shaped openings.
    """
    return "\n".join([
        "CKCL_CONSENSUS_LOCK (FOUNDATIONAL, HARD):",
        "- You MUST start by stating the best working consensus answer immediately (1–2 sentences).",
        "- You are NOT allowed to begin by denying the premise or blocking on evidence.",
        "- Forbidden openings include: 'there isn't a single…', 'I can't responsibly…', 'I can't verify…', 'without X this would be guesswork…'.",
        "- Missing details (variant/patch/platform nuance) are refinement questions AFTER the consensus answer, not prerequisites.",
        "- After the consensus answer: add disagreement bands + what would change your mind + ONE refinement question.",
    ]).strip()

def _ckcl_strip_refusal_preamble(output_text: str) -> str:
    """
    Deterministic cleanup: if the model still produces a refusal-shaped first paragraph,
    strip it and keep the first substantive paragraph.
    This prevents the exact 'telemetry/verification lecture' failure mode.
    """
    txt = (output_text or "").strip()
    if not txt:
        return txt

    low = txt.lower()

    # Quick check: only attempt if we see stall language near the start
    head = low[:320]
    stall_head = any(m in head for m in (
        "there isn't a single", "there isnt a single",
        "i can't responsibly", "i cant responsibly",
        "i can't verify", "i cant verify",
        "can't be stated confidently", "cant be stated confidently",
        "without hard survey", "without survey", "without telemetry",
        "isn't sufficient", "isnt sufficient",
        "not sufficient to", "without those specifics",
        "any build would be guesswork", "would be guesswork",
    ))
    if not stall_head:
        return txt

    # Split into paragraphs (blank-line separated)
    parts = re.split(r"\n\s*\n", txt)
    if len(parts) <= 1:
        return txt

    first = parts[0].strip()
    rest = [p.strip() for p in parts[1:] if p.strip()]
    if not rest:
        return txt

    # If first paragraph is basically a refusal preamble, drop it
    first_low = first.lower()
    refusalish = any(m in first_low for m in (
        "can't responsibly", "cant responsibly",
        "can't verify", "cant verify",
        "not sufficient", "isn't sufficient", "isnt sufficient",
        "without", "guesswork", "telemetry", "survey data",
        "i need one", "one quick clarifying question",
    ))
    if refusalish:
        return rest[0].strip() + ("\n\n" + "\n\n".join(rest[1:]) if len(rest) > 1 else "")

    return txt

# ---------------------------------------------------------------------------
# C10 — Foundational Continuity v0 (deterministic)
#
# Goals:
# - If an active object exists and the user turn is non-trivial, assume it is in scope
#   unless explicitly contradicted.
# - Drive the conversation forward ("WIN"): do best-effort action first, then ask exactly ONE
#   high-leverage question when blocked.
#
# Hard rules:
# - No model calls here.
# - No canonical writes here.
# - Ephemeral only (read-time).
# ---------------------------------------------------------------------------

_C10_FILE_REF_RE = re.compile(r"[\w./-]+\.(?:pdf|png|jpg|jpeg|webp|gif|csv|xlsx|xlsm|xls|docx|doc)\b", flags=re.IGNORECASE)

def _c10_is_trivial_ack(msg: str) -> bool:
    t = (msg or "").strip().lower()
    if not t:
        return True
    t = re.sub(r"[.!?]+$", "", t).strip()
    return t in (
        "ok", "okay", "k", "sure", "yes", "y", "yep", "yeah",
        "go on", "continue", "keep going", "proceed", "do it",
        "sounds good", "alright", "all right", "cool",
    )

def _c10_is_topic_break(msg: str) -> bool:
    t = (msg or "").strip().lower()
    if not t:
        return False
    # Conservative: only break when user clearly signals a switch.
    triggers = (
        "unrelated",
        "new topic",
        "different topic",
        "switch topic",
        "change topic",
        "separately",
        "ignore that",
        "forget that",
        "not about that",
        "not related",
    )
    return any(x in t for x in triggers)

def _c10_user_explicitly_named_a_file(msg: str) -> bool:
    try:
        return bool(_C10_FILE_REF_RE.search(msg or ""))
    except Exception:
        return False

def _c10_load_active_focus(ctx: Any, project_full: str) -> Dict[str, Any]:
    try:
        ao = ctx.project_store.load_active_object(project_full) or {}
    except Exception:
        ao = {}
    return ao if isinstance(ao, dict) else {}

def _c10_focus_in_scope(ctx: Any, project_full: str, user_msg: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Continuity decision:
    - If user explicitly names a file, do NOT force AOF (their explicit reference will be handled elsewhere).
    - If user explicitly breaks topic, do NOT force AOF.
    - If the user is asking for a NEW picture/image in general terms, do NOT bind to AOF.
      (Prevents old image context hijacking unrelated turns.)
    - If message is non-trivial OR is a trivial ack, and AOF exists, treat it as in scope.
    """
    msg = (user_msg or "").strip()

    # NEW: "show me a picture" / "can you show a picture" / "generate an image" should not
    # automatically refer to the active object unless the user uses deictic language ("this/that/the photo").
    try:
        low = msg.lower().strip()
        wants_new_image = any(p in low for p in (
            "show me a picture",
            "show me a photo",
            "show me an image",
            "can you show me a picture",
            "can you show me a photo",
            "can you show me an image",
            "generate an image",
            "make an image",
            "create an image",
            "find a picture",
            "find an image",
        ))
        has_deictic = bool(re.search(r"\b(this|that|the)\s+(image|photo|picture|screenshot)\b", low))
        if wants_new_image and (not has_deictic):
            return (False, {})
    except Exception:
        pass
    if not msg:
        # empty message: if AOF exists, treat as continuity ping
        ao = _c10_load_active_focus(ctx, project_full)
        rel = str(ao.get("rel_path") or "").replace("\\", "/").strip()
        return (bool(rel), ao)

    if _c10_is_topic_break(msg):
        return (False, {})

    if _c10_user_explicitly_named_a_file(msg):
        return (False, {})

    ao = _c10_load_active_focus(ctx, project_full)
    rel = str(ao.get("rel_path") or "").replace("\\", "/").strip()
    if not rel:
        return (False, {})

    # Trivial acknowledgement continues focus
    if _c10_is_trivial_ack(msg):
        return (True, ao)

    # Slot-fill noun phrase continuation:
    # short, content-bearing phrases (e.g., "Charlottesville", "city of X", "blue one")
    try:
        toks = re.findall(r"[a-z0-9]+", msg.lower())
        if 1 <= len(toks) <= 6:
            # Reject explicit topic breaks already handled above
            return (True, ao)
    except Exception:
        pass

    # Longer content — assume continuation unless contradicted
    if len(msg) >= 4:
        return (True, ao)

    return (False, {})

def _c10_evidence_presence_from_manifest(man: Dict[str, Any], raw_rel: str) -> Dict[str, bool]:
    """
    Best-effort: determine which artifacts exist for this file based on manifest.
    """
    rel = (raw_rel or "").replace("\\", "/").strip()
    if not rel:
        return {"has_any": False}

    arts = man.get("artifacts") or []
    if not isinstance(arts, list):
        arts = []

    found = {
        "image_semantics": False,
        "plan_ocr": False,
        "ocr_text": False,
        "pdf_text": False,
        "excel_blueprint": False,
        "file_overview": False,
        "has_any": False,
    }

    # Newest-last in manifest; scan tail bounded
    for a in reversed(arts[-1800:]):
        if not isinstance(a, dict):
            continue
        at = str(a.get("type") or "").strip()
        ff = a.get("from_files") or []
        if not isinstance(ff, list):
            continue
        ff_norm = [(str(x) or "").replace("\\", "/").strip() for x in ff]
        if rel not in ff_norm:
            continue

        if at in found:
            found[at] = True
        # Map common types into our presence flags
        if at == "image_semantics":
            found["image_semantics"] = True
        elif at == "plan_ocr":
            found["plan_ocr"] = True
        elif at == "ocr_text":
            found["ocr_text"] = True
        elif at == "pdf_text":
            found["pdf_text"] = True
        elif at == "excel_blueprint":
            found["excel_blueprint"] = True
        elif at == "file_overview":
            found["file_overview"] = True

        found["has_any"] = True

    return found

def _c10_pick_one_win_question(ctx: Any, project_full: str, *, focus_name: str) -> str:
    """
    Deterministically pick ONE next question to advance the project ("WIN").
    Priority:
      1) open inbox item
      2) pending decision candidate
      3) active focus outcome question
      4) definition-of-done
    """
    # Couples Mode Option A: never inject decision-confirm WIN questions.
    # Therapy UX must not be hijacked by “treat as final?” prompts.
    try:
        if str(project_full or "").split("/", 1)[0].lower().startswith("couple_"):
            return ""
    except Exception:
        pass

    # 1) Inbox (open)
    try:
        open_items = ctx.project_store.list_open_inbox(project_full, max_items=1) or []
    except Exception:
        open_items = []
    if isinstance(open_items, list) and open_items:
        it = open_items[0] if isinstance(open_items[0], dict) else {}
        txt = str(it.get("text") or "").replace("\n", " ").strip()
        if txt:
            if len(txt) > 140:
                txt = txt[:139].rstrip() + "…"
            return f"To move this forward, can you answer this inbox item: {txt}"

    # 2) Pending decision candidate
    try:
        cands = ctx.project_store.find_pending_decision_candidates(project_full, query=focus_name or "pending", max_items=1) or []
    except Exception:
        cands = []
    if isinstance(cands, list) and cands:
        it = cands[0] if isinstance(cands[0], dict) else {}
        t = str(it.get("text") or "").strip()
        if t:
            if len(t) > 160:
                t = t[:159].rstrip() + "…"
            return f"Quick confirm so we can lock progress: should we treat this as a final decision — “{t}” (yes/no)?"

    # 3) Focus outcome (only when there is a real, active focus)
    # UX rule: do NOT force a workflow menu into normal conversation.
    # Only ask a next-step question when the user explicitly asks what to do next.
    if focus_name:
        return ""


    # 4) Definition of done
    # Normal conversation should not be forced into a deliverable workflow.
    return ""


async def _maybe_ensure_active_image_semantics(
    *,
    ctx: Any,
    project_full: str,
    user_msg: str,
    reason: str,
) -> str:
    """
    Best-effort:
    - If active object is an image AND this turn looks referential, ensure image_semantics exists.
    Returns the cached semantics json text (possibly truncated later by callers), or "".
    """
    try:
        ao = ctx.project_store.load_active_object(project_full) or {}
    except Exception:
        ao = {}

    if not isinstance(ao, dict):
        return ""

    rel = str(ao.get("rel_path") or "").replace("\\", "/").strip()
    if not rel:
        return ""

    # Only for images
    try:
        suf = Path(rel).suffix.lower()
    except Exception:
        suf = ""
    try:
        img_suffixes = getattr(ctx, "IMAGE_SUFFIXES", None)
        if not isinstance(img_suffixes, (list, tuple, set)):
            img_suffixes = ()
        if suf not in img_suffixes:
            return ""
    except Exception:
        return ""

    # Only when this turn looks like it refers to "the thing we just saw"
    if not _looks_like_image_referential_turn(user_msg):
        return ""

    # If already cached, return it
    try:
        if hasattr(ctx, "_find_latest_artifact_text_for_file"):
            sem0 = ctx._find_latest_artifact_text_for_file(project_full, artifact_type="image_semantics", file_rel=rel, cap=220000)
            if sem0:
                return (sem0 or "").strip()
    except Exception:
        pass

    # Ensure it exists (on-demand)
    try:
        if hasattr(ctx, "ensure_image_semantics_for_file"):
            ok, _json_text, _note = await ctx.ensure_image_semantics_for_file(
                project_full,
                file_rel=rel,
                force=False,
                reason=(reason or "")[:120],
            )
            _ = ok
    except Exception:
        pass

    # Re-read cached artifact text (preferred; avoids returning raw model output)
    try:
        if hasattr(ctx, "_find_latest_artifact_text_for_file"):
            sem1 = ctx._find_latest_artifact_text_for_file(project_full, artifact_type="image_semantics", file_rel=rel, cap=220000)
            return (sem1 or "").strip()
    except Exception:
        return ""

    return ""


async def classify_intent_c6(*, ctx: Any, user_text: str) -> Dict[str, Any]:
    """
    Stage 1 — Intent classification (model allowed, JSON only)
    """
    msg = (user_text or "").strip()

    # NOTE:
    # We intentionally avoid deterministic phrase-lists here.
    # Intent should be inferred by the classifier model (Stage 1),
    # with ONLY the single stable web-lookup heuristic below as a deterministic override.
    _m = re.sub(r"\s+", " ", msg.lower()).strip()
    _m = (
        _m.replace("’", "'")
          .replace("‘", "'")
          .replace("“", '"')
          .replace("”", '"')
    )


    # NOTE:
    # We do NOT force intent="lookup" via ctx.should_auto_web_search here.
    # That recreates brittle "magic word" routing and can override conversational intent.
    #
    # Instead:
    # - Stage 1 classifier infers intent from the full user message (JSON-only).
    # - The server decides whether to run web search (do_search) using a separate gate,
    #   and the presence of search_results will still force lookup mode downstream.

    messages = [
        {"role": "system", "content": _INTENT_CLASSIFIER_SYSTEM},
        {"role": "user", "content": msg},
    ]

    raw = await asyncio.to_thread(ctx.call_openai_chat, messages)
    obj = _safe_json_extract(raw)

    intent = str(obj.get("intent") or "misc").strip().lower()
    if intent not in ("recall", "status", "plan", "execute", "lookup", "misc"):
        intent = "misc"

    # Deterministic recall hardening:
    # Recall is RESERVED for explicit "remind/remember/what did we decide" queries.
    # Natural language memory questions (identity/relationships/etc.) must stay in misc
    # so Tier-2 (FACTS_MAP_COMPACT) can be used by grounded generation.
    if intent == "recall":
        _r = _m
        recall_triggers = (
            "remind me",
            "remember",
            "what did we decide",
            "what did i decide",
            "what did you decide",
            "decision",
            "decisions",
            "commitment",
            "commitments",
            "we agreed",
            "did we agree",
            "recall",
            "last time we",
            "earlier we",
        )
        if not any(t in _r for t in recall_triggers):
            intent = "misc"

    ents = obj.get("entities")
    if not isinstance(ents, list):
        ents = []
    entities = [str(x).strip() for x in ents if str(x).strip()][:8]

    scope = str(obj.get("scope") or "current_project").strip()
    if scope != "current_project":
        # C6 invariant: scope changes only on explicit user switch; enforce default here.
        scope = "current_project"

    return {"intent": intent, "entities": entities, "scope": scope}


async def classify_continuity_c11(
    *,
    ctx: Any,
    conversation_history: List[Dict[str, str]],
    user_text: str,
    active_topic_text: str = "",
) -> Dict[str, Any]:
    """
    LLM-based continuity classifier.

    Returns:
      {
        "continuity": "same_topic" | "new_topic" | "unclear",
        "followup_only": bool,
        "topic": "short summary"
      }
    """
    msg = (user_text or "").strip()
    if not msg:
        return {"continuity": "same_topic", "followup_only": True, "topic": ""}

    # Build a compact recent-turns tail (user/assistant only; bounded).
    tail_lines: List[str] = []
    try:
        for m in (conversation_history or [])[-12:]:
            role = str(m.get("role") or "").strip().lower()
            if role not in ("user", "assistant"):
                continue
            content = str(m.get("content") or "").strip()
            if not content:
                continue
            if len(content) > 500:
                content = content[:500].rstrip() + "..."
            tail_lines.append(f"{role.upper()}: {content}")
    except Exception:
        tail_lines = []

    tail_text = "\n".join(tail_lines).strip() or "(none)"
    active_topic = (active_topic_text or "").strip() or "(none)"

    prompt = "\n".join(
        [
            "RECENT_TURNS:",
            tail_text,
            "",
            "ACTIVE_TOPIC:",
            active_topic,
            "",
            "LATEST_USER:",
            msg,
        ]
    ).strip()

    messages = [
        {"role": "system", "content": _CONTINUITY_CLASSIFIER_SYSTEM},
        {"role": "user", "content": prompt},
    ]

    raw = await asyncio.to_thread(ctx.call_openai_chat, messages)
    obj = _safe_json_extract(raw)

    cont_raw = str(obj.get("continuity") or "").strip().lower()
    if cont_raw in ("same", "same_topic", "continuation", "continue"):
        continuity = "same_topic"
    elif cont_raw in ("new", "new_topic", "topic_shift", "different"):
        continuity = "new_topic"
    elif cont_raw in ("unclear", "unknown"):
        continuity = "unclear"
    else:
        continuity = "same_topic"

    followup_only = bool(obj.get("followup_only") or obj.get("followup") or False)

    topic = str(obj.get("topic") or "").strip()
    if topic:
        topic = re.sub(r"\s+", " ", topic).strip()
        if len(topic) > 180:
            topic = topic[:180].rstrip()

    return {"continuity": continuity, "followup_only": followup_only, "topic": topic}

def _contains_invented_pulse_tokens(text: str) -> bool:
    low = (text or "").lower()
    return ("project pulse" in low) or ("\ngoal:" in low) or ("\nnext action" in low) or low.startswith("goal:")
def _has_partner_context_snippets(canonical_snippets: List[str]) -> bool:
    """
    Deterministic detection: partner context blocks are explicitly labeled by project_store.
    """
    try:
        joined = "\n".join([str(s) for s in (canonical_snippets or [])])
    except Exception:
        joined = ""
    return "PARTNER_CONTEXT_TIER" in joined


def _partner_attribution_violation(text: str) -> bool:
    """
    Deterministic pattern check for provenance/attribution leakage.
    """
    low = (text or "").lower()

    bad_phrases = (
        "she said",
        "he said",
        "your partner said",
        "your partner told me",
        "your partner told you",
        "i read your partner",
        "i read their notes",
        "i read the notes",
        "from your partner's notes",
        "from your partners notes",
        "i saw in your partner",
        "i saw in their notes",
        "i know because",
        "i know from",
        "told me privately",
        "in their private",
        "in the private notes",
        "your partner's private",
        "your partners private",
    )

    return any(p in low for p in bad_phrases)


def _enforce_couples_neutral_phrasing(text: str) -> str:
    """
    Deterministic post-process used ONLY when partner context is present.
    - Remove explicit attribution/provenance language
    - Prefer neutral relationship-level phrasing
    """
    t = (text or "").strip()
    if not t:
        return t

    # Drop/neutralize the most dangerous provenance phrases (simple, conservative).
    t2 = t

    # Replace second-person partner references with neutral forms
    repl = [
        ("your partner", "one partner"),
        ("your girlfriend", "one partner"),
        ("your boyfriend", "one partner"),
        ("she ", "one partner "),
        ("he ", "one partner "),
        ("she’s", "one partner is"),
        ("he’s", "one partner is"),
        ("she's", "one partner is"),
        ("he's", "one partner is"),
    ]

    # Apply case-insensitive-ish by working on a shadow-lower index
    low = t2.lower()
    for a, b in repl:
        if a in low:
            # do a simple replace on the original string; acceptable for deterministic gate
            t2 = t2.replace(a, b).replace(a.title(), b).replace(a.upper(), b.upper())
            low = t2.lower()

    # If any explicit “X said/told me/read notes” remains, remove those sentences conservatively.
    if _partner_attribution_violation(t2):
        parts = re.split(r"(?<=[.!?])\s+", t2)
        kept: List[str] = []
        for p in parts:
            if _partner_attribution_violation(p):
                continue
            kept.append(p)
        t2 = " ".join([x.strip() for x in kept if x.strip()]).strip()

    return t2.strip()
def _enforce_evidence_exhaustion(*, search_results: str, output_text: str) -> str:
    """
    FOUNDATIONAL ENFORCEMENT (HARD).

    Goal:
    - If SEARCH_EVIDENCE_JSON contains explicit affirmative reporting from credible sources,
      the assistant is NOT allowed to refuse, dodge, or lead with uncertainty/clarifying questions.
    - The assistant MUST state what is confirmed/reported first, then add nuance/unknowns.

    This fixes the failure mode:
      - Search results exist (Reuters/CNBC/etc.)
      - Model still says "I can't give the latest / need to know what you mean"
    """
    if not search_results or not output_text:
        return output_text

    out = (output_text or "").strip()
    low = out.lower()

    # Parse evidence JSON (schema=search_evidence_v1)
    ev: Dict[str, Any] = {}
    try:
        ev = json.loads(search_results)
    except Exception:
        return output_text
    if not isinstance(ev, dict):
        return output_text
    if str(ev.get("schema") or "") != "search_evidence_v1":
        return output_text

    results = ev.get("results")
    if not isinstance(results, list) or not results:
        return output_text

    # Collect a small evidence blob (titles + snippets) for deterministic checks
    blob_parts: List[str] = []
    for r in results[:10]:
        if not isinstance(r, dict):
            continue
        blob_parts.append(str(r.get("title") or ""))
        blob_parts.append(str(r.get("snippet") or ""))
    blob = " ".join(blob_parts).lower()
    # Server-annotated authority override:
    # If evidence includes primary-source confirmation, we treat it as affirmative even if
    # our string markers miss it (prevents hedge-first on obvious confirmations).
    authority_level = ""
    try:
        auth = ev.get("authority") if isinstance(ev.get("authority"), dict) else {}
        authority_level = str((auth or {}).get("level") or "").strip().lower()
    except Exception:
        authority_level = ""

    # 1) Detect whether the evidence contains explicit affirmative language
    #    (deal/partnership/confirmed/official/will use/will power/etc.)
    affirmative_markers = (
        "will use",
        "will power",
        "to power",
        "is teaming up",
        "partnering with",
        "has chosen",
        "has entered",
        "multi-year deal",
        "multi year deal",
        "agreement",
        "collaboration",
        "joint statement",
        "it’s official",
        "it's official",
        "confirmed",
        "has confirmed",
    )
    evidence_affirmative = any(m in blob for m in affirmative_markers) or (authority_level == "primary_confirmed")

    # 2) Detect "premature uncertainty" / hedge-first patterns in output
    #    (including semantic dodges like "depends what you mean")
    dodge_or_refusal = any(m in low for m in (
        "i can’t give",
        "i can't give",
        "i can’t confirm",
        "i can't confirm",
        "i can’t say",
        "i can't say",
        "cannot give",
        "cannot confirm",
        "without seeing",
        "without fresh evidence",
        "no evidence provided",
        "no search evidence",
        "no search results",
        "no snippets",
        "depends on what you mean",
        "depends what you mean",
        "multiple different things people mean",
        "there are multiple different things",
        "one quick clarification",
        "one quick clarifier",
        "before i can answer",
    ))

    premature_uncertainty = any(m in low for m in (
        "hasn’t publicly confirmed",
        "has not publicly confirmed",
        "not officially confirmed",
        "no official announcement",
        "mostly rumor",
        "just speculation",
        "not confirmed yet",
    ))

    # 3) If evidence is affirmative but the model is dodging/refusing/hedging, force a rewrite reminder
    if evidence_affirmative and (dodge_or_refusal or premature_uncertainty):
        return (
            "SEARCH_EVIDENCE_ENFORCEMENT:\n"
            "- The attached search evidence includes explicit affirmative reporting.\n"
            "- You must state what is confirmed/reported (with dates if present in snippets) BEFORE asking for clarification or concluding uncertainty.\n\n"
            + out
        )

    # 4) Preserve your original behavior: if the model concludes uncertainty without any positive anchors,
    #    require it to enumerate what is confirmed/reported first (even if not a full confirmation).
    if premature_uncertainty:
        has_positive_facts = any(k in low for k in (
            "confirmed",
            "announced",
            "stated",
            "said it will",
            "has said",
            "reported",
            "according to",
            "has publicly",
        ))
        if not has_positive_facts:
            return (
                "Before concluding uncertainty, enumerate what IS confirmed or credibly reported:\n\n"
                + out
            )

    return output_text


def validate_c6_output(
    *,
    intent: str,
    scope: str,
    canonical_snippets: List[str],
    output_text: str,
) -> Optional[str]:
    """
    Stage 4 — Deterministic safety gate.
    Returns None if OK, else a deterministic failure reason string.
    """
    out = (output_text or "").strip()
    if not out:
        return "empty_output"

    if scope != "current_project":
        return "scope_violation"
    # Couples partner-context privacy gate:
    # If partner distilled context was injected, prevent attribution/provenance leakage.
    if _has_partner_context_snippets(canonical_snippets):
        if _partner_attribution_violation(out):
            return "partner_privacy_attribution"
        
    # Never allow pulse-ish narratives unless explicitly retrieved
    if _contains_invented_pulse_tokens(out):
        joined = "\n".join(canonical_snippets or [])
        if "PROJECT_PULSE_TRUTH_BOUND:" not in joined:
            return "invented_pulse_or_goal"
    # Allow answers grounded in temporary FILE_OCR_EVIDENCE blocks
    # These are read-time artifacts, not canonical memory.
    joined = "\n".join(canonical_snippets or [])

    # Evidence-based allowance
    if (
        ("FILE_OCR_EVIDENCE:" in joined)
        or ("EXCEL_BLUEPRINT_EVIDENCE:" in joined)
        or ("EXCEL_OVERVIEW_EVIDENCE:" in joined)
    ):
        return None

    # Assumption-bound inference allowance
    if "BOUND_ASSUMPTION:" in joined:
        return None

    # Status and recall are NEVER model-authored.
    # They must be deterministically rendered upstream.
    if intent == "status":
        return "status_must_be_deterministic"

    # Recall is allowed to be model-authored IF grounded in CANONICAL_SNIPPETS.
    # (Truth-binding still applies: missing facts must yield "I don't have that yet.")
    if intent == "recall":
        return None
    return None

def _tier1_guess_slot_from_claim(claim: str) -> str:
    c = (claim or "").lower()
    if any(k in c for k in ("name is", "years old", "i live in", "i’m ", "i'm ", "i am ", "nationality", "canadian", "american")):
        return "identity"
    if any(k in c for k in ("my son", "my daughter", "my wife", "my husband", "my girlfriend", "my boyfriend", "my partner", "we are getting married")):
        return "relationship"
    if any(k in c for k in ("favorite", "favourite", "i prefer", "i like", "i don’t like", "i don't like")):
        return "preference"
    if any(k in c for k in ("visa", "immigration", "e-2", "e2 visa", "custody", "divorce")):
        return "constraint"
    return "other"


def _tier1_is_preference_sentence(s: str) -> bool:
    low = (s or "").lower()
    return any(k in low for k in ("my favorite", "my favourite", "i prefer", "i like", "i don't like", "i don’t like"))

_MONTHS = {
    "january": "01", "jan": "01",
    "february": "02", "feb": "02",
    "march": "03", "mar": "03",
    "april": "04", "apr": "04",
    "may": "05",
    "june": "06", "jun": "06",
    "july": "07", "jul": "07",
    "august": "08", "aug": "08",
    "september": "09", "sep": "09", "sept": "09",
    "october": "10", "oct": "10",
    "november": "11", "nov": "11",
    "december": "12", "dec": "12",
}

def _normalize_birthdate_text(claim: str) -> Optional[str]:
    """
    Convert month-name birthdates to ISO YYYY-MM-DD.
    Deterministic. Returns None if ambiguous.
    Examples:
      'october 07 1982' -> '1982-10-07'
      'oct 7 1982'      -> '1982-10-07'
    """
    s = claim.lower().strip()
    m = re.search(
        r"\b(?P<month>[a-z]+)\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?[, ]+\s*(?P<year>\d{4})\b",
        s,
    )
    if not m:
        return None

    mon = _MONTHS.get(m.group("month"))
    if not mon:
        return None

    try:
        day = int(m.group("day"))
        year = int(m.group("year"))
        if not (1 <= day <= 31 and 1900 <= year <= 2100):
            return None
        return f"{year:04d}-{mon}-{day:02d}"
    except Exception:
        return None
def _tier1_should_skip_sentence(s: str) -> bool:
    """
    Conservative deterministic skip for Tier-1 candidate sentences.
    Goal: avoid storing meta/process/system chatter as memory facts.
    """
    t = " ".join((s or "").strip().split())
    if not t:
        return True

    low = t.lower()

    # Obvious meta/system/process chatter
    if any(k in low for k in (
        "as an ai",
        "as a language model",
        "chatgpt",
        "system prompt",
        "developer message",
        "tool call",
        "token",
        "context window",
        "memory architecture",
        "tier-1",
        "tier-2",
        "tier2",
        "the model",
        "the assistant",
    )):
        return True

    # Pure questions / imperatives are not durable personal facts
    if t.endswith("?"):
        return True
    if low.startswith(("please ", "can you ", "could you ", "do ", "make ", "write ", "show ", "tell me ")):
        return True

    # Very short fragments are usually not stable facts
    if len(t) < 4:
        return True

    return False

def _tier1_global_eligible_for_tier2g(*, claim: str, slot: str, subject: str, evidence_quote: str = "") -> bool:
    """
    Tier-2G Global Memory Policy (deterministic, Philosophy B):

    Global-eligible keys ONLY:
      - identity.preferred_name
      - identity.birthdate (ISO only; never store age)
      - identity.timezone
      - identity.location
      - relationships.girlfriend_name
      - relationships.son_name

    Hard gates:
      - subject MUST be "user"
      - NO inference: deterministic pattern match only
      - For identity fields, slot is allowed to be imperfect, BUT evidence_quote must show first-person phrasing.
      - Birthdate remains STRICT: evidence_quote must contain "my birthday" or "i was born".
    """
    c = (claim or "").strip()
    if not c:
        return False

    subj = (subject or "").strip().lower()
    if subj != "user":
        return False

    sl = (slot or "").strip().lower()
    low = c.lower()
    ev = (evidence_quote or "").lower()

    def _ev_has_any(*needles: str) -> bool:
        for n in needles:
            if n and (n in ev):
                return True
        return False

    # ------------------------------------------------------------------
    # Relationships (keep slot requirement strict to avoid cross-person mixups)
    # ------------------------------------------------------------------
    if sl == "relationship":
        if ("my girlfriend" in low and "name is" in low):
            return True
        if ("my partner" in low and "name is" in low):
            return True
        if ("my son" in low and "name is" in low):
            return True
        return False

    # ------------------------------------------------------------------
    # Identity (Philosophy B: slot flexible, evidence_quote must be first-person)
    # ------------------------------------------------------------------

    # Preferred name:
    # allow if claim suggests naming AND evidence_quote indicates self-identification.
    if (
        ("my preferred name is" in low)
        or ("i go by" in low)
        or ("my name is" in low)
        or ("the user's name is" in low)
        or ("the user’s name is" in low)
        or ("the user's preferred name is" in low)
        or ("the user’s preferred name is" in low)
    ):
        return _ev_has_any("my name", "i'm ", "i’m ", "i am ", "i go by", "preferred name")

    # Location:
    # accept "I live in ..." / "I'm in ..." or normalized "The user lives in ...",
    # but require first-person location phrasing in evidence_quote.
    if ("i live in" in low) or ("the user lives in" in low) or ("i'm in " in low) or ("i’m in " in low) or ("i am in " in low):
        return _ev_has_any("i live", "i'm in", "i’m in", "i am in")

    # Timezone:
    # accept explicit timezone phrases; require evidence_quote mention timezone OR "central time".
    if ("my time zone is" in low) or low.startswith("timezone is "):
        return _ev_has_any("time zone", "timezone", "central time")

    if "central time" in low:
        return _ev_has_any("central time", "time zone", "timezone")

    # Birthdate (STRICT):
    # must be explicitly a birthdate statement, and evidence_quote must contain first-person birth phrasing.
    if ("my birthday is" in low) or ("i was born on" in low):
        return _ev_has_any("my birthday", "i was born")

    return False




def _extract_tier1_candidates_from_user_msg(user_msg: str, *, max_items: int = 6) -> List[Dict[str, str]]:
    """
    Deterministic extraction of Tier-1 candidate facts from a single user turn.
    Returns a list of dicts: {claim, slot, subject}.
    """
    raw = (user_msg or "").strip()
    if not raw:
        return []

    # Normalize common unicode punctuation so keyword checks stay deterministic.
    # Examples: E-2 (U+2011) -> E-2, smart quotes -> ASCII.
    raw = (
        raw.replace("\u2019", "'")
           .replace("\u2018", "'")
           .replace("\u201c", '"')
           .replace("\u201d", '"')
           .replace("\u2010", "-")
           .replace("\u2011", "-")
           .replace("\u2012", "-")
           .replace("\u2013", "-")
           .replace("\u2014", "-")
           .replace("\u2212", "-")
    )

    # Keep it bounded and sentence-ish
    text = re.sub(r"\s+", " ", raw).strip()

    # Split into simple sentence candidates
    parts = re.split(r"(?<=[.!?])\s+", text)
    if not parts:
        parts = [text]

    out: List[Dict[str, str]] = []

    for p in parts:
        s = (p or "").strip().strip('"').strip("'").strip()
        # If the user embeds a first-person clause inside a longer sentence,
        # pull out the first explicit "i/my/we" clause deterministically.
        # Example: "Sometimes people call me Francis, but I go by Frank." -> "I go by Frank."
        try:
            low0 = s.lower()
            cut_pos = -1
            for sep in (", i ", "; i ", " but i ", " and i ", " then i "):
                j = low0.find(sep)
                if j != -1:
                    cut_pos = j + 2  # points at the "i" in " i "
                    break
            if cut_pos != -1:
                s = s[cut_pos:].strip()
        except Exception:
            pass

        if not s:
            continue
        if _tier1_should_skip_sentence(s):
            continue

        # If the user embeds a first-person clause inside a longer sentence,
        # pull out the first explicit "i/my/we" clause deterministically.
        # Example: "Sometimes people call me Francis, but I go by Frank." -> "I go by Frank"
        s2 = s
        try:
            low0 = s2.lower()
            cut_pos = -1
            for sep in (", i ", "; i ", " but i ", " and i ", " then i "):
                j = low0.find(sep)
                if j != -1:
                    cut_pos = j + 2  # points at the "i" in " i "
                    break
            if cut_pos != -1:
                s2 = s2[cut_pos:].strip()
        except Exception:
            s2 = s

        # Normalize common discourse markers so first-person checks work deterministically.
        # Example: "Actually, I'm in Austin now" -> "I'm in Austin now"
        try:
            s2 = re.sub(
                r"^\s*(?:actually|well|so|ok|okay|alright|all right|um|uh|hey|hi)\s*[,:\-–—]+\s*",
                "",
                s2,
                flags=re.IGNORECASE,
            ).strip()
        except Exception:
            s2 = s

        low = s2.lower()


        # Only accept explicit, recallable “I/my” fact shapes (truth-bound).
        #
        # IMPORTANT:
        # Do NOT treat generic "I'm ..." as a fact shape; that causes therapy reflections
        # (e.g., "I'm worried...", "I'm kind of all over the place...") to be stored.
        # We only allow "I'm ..." when paired with narrow, recallable keywords below.
        first_person = low.startswith((
            "my ",
            "i ",
            "we ",
            "i'm ",
            "i’m ",
            "im ",
            "we're ",
            "we’re ",
            "i am ",
        ))

        explicit = (
            first_person
            and any(k in low for k in (
                # identity / name / location / basics
                "my name is",
                "my preferred name is",
                "i go by",
                "i live in",
                "i'm in ",
                "i’m in ",
                "im in ",
                "i am in ",
                "moved here",

                "my favorite color is",
                "my favourite color is",

                # relationships / family
                "my son",
                "my daughter",
                "my wife",
                "my husband",
                "my girlfriend",
                "my boyfriend",
                "my partner",

                # constraints / legal / logistics
                "visa",
                "e-2",
                "e2 visa",
                "divorce",
                "custody",
                "immigration",
                "getting married",

                # preferences (allowed)
                "i prefer",
                "i like",
                "i don't like",
                "i don’t like",

                # work / context (explicit)
                "i work in",
                "work in tech",
                "stateful assistant",
                "tiered memory",

                # time zone (explicit)
                "central time",
                "time zone",
            ))
        )


        if not explicit:
            continue


        claim = s2.rstrip(".").strip()
        if len(claim) < 6:
            continue

        slot = _tier1_guess_slot_from_claim(claim)

        out.append({"claim": claim, "slot": slot, "subject": "user"})

        if len(out) >= int(max_items or 6):
            break

    return out
async def _extract_tier1_candidates_with_model(
    *,
    ctx: Any,
    window_text: str,
    max_items: int = 8,
) -> List[Dict[str, str]]:
    """
    Model-assisted Tier-1 candidate extraction from natural conversation.

    Deterministic validation rules (server-side):
    - evidence_quote MUST be a substring of window_text (verbatim)
    - claim MUST be 1 line (no newlines) and not a question
    - reject reflective/speculative markers (includes smoke-test banned markers)
    - slot/subject must be from small allowlists
    """
    win = (window_text or "").strip()
    if not win:
        return []

    system = "\n".join(
        [
            "You are extracting Tier-1 raw fact candidates from a short chat window.",
            "Return ONLY strict JSON (no prose).",
            "",
            "Schema:",
            "{",
            '  "facts": [',
            "    {",
            '      "claim": "one sentence, normalized, no newlines",',
            '      "slot": "identity|relationship|preference|possession|routine|constraint|context|event|other",',
            '      "subject": "user|other|project|unknown",',
            '      "confidence": "low|medium|high",',
            '      "privacy": "normal|sensitive",',
            '      "evidence_quote": "verbatim short quote copied from WINDOW (must appear exactly)"',
            "    }",
            "  ]",
            "}",
            "",
            "Rules:",
            "- Extract durable, recallable facts stated or clearly implied by the speaker about themselves/their life.",
            "- Do NOT extract feelings/speculation framed as truth (e.g., 'I feel', 'I think', 'maybe', 'I'm worried').",
            "- Do NOT output questions as claims.",
            "- Hard cap: at most %d facts." % int(max_items or 8),
        ]
    ).strip()

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": "WINDOW:\n" + win[:12000]},
    ]

    try:
        raw = await asyncio.to_thread(ctx.call_openai_chat, messages)
    except Exception:
        return []

    # Parse best-effort JSON object
    obj: Dict[str, Any] = {}
    try:
        m = re.search(r"\{.*\}", raw or "", flags=re.DOTALL)
        if m:
            parsed = json.loads(m.group(0))
            if isinstance(parsed, dict):
                obj = parsed
    except Exception:
        obj = {}

    facts = obj.get("facts")
    if not isinstance(facts, list):
        return []

    allow_slot = {"identity", "relationship", "preference", "possession", "routine", "constraint", "context", "event", "other"}
    allow_subject = {"user", "other", "project", "unknown"}

    banned_markers = (
        "i think",
        "i feel",
        "maybe",
        "probably",
        "i guess",
        "i'm worried",
        "im worried",
    )

    out: List[Dict[str, str]] = []

    for f in facts[: int(max_items or 8)]:
        if not isinstance(f, dict):
            continue

        claim = str(f.get("claim") or "").replace("\r", " ").replace("\n", " ").strip()
        if not claim or len(claim) < 6:
            continue

        # Reject questions
        if "?" in claim:
            continue

        # Reject reflective/speculative content (matches smoke test expectations)
        lowc = claim.lower()
        if any(m in lowc for m in banned_markers):
            continue

        # Also apply existing conservative skip (meta/process etc.)
        try:
            if _tier1_should_skip_sentence(claim):
                continue
        except Exception:
            pass

        slot = str(f.get("slot") or "other").strip().lower()
        if slot not in allow_slot:
            slot = "other"

        subj = str(f.get("subject") or "user").strip().lower()
        if subj not in allow_subject:
            subj = "user"

        evq = str(f.get("evidence_quote") or "").strip()
        if not evq:
            continue

        # Deterministic evidence check: must appear verbatim in the window text we provided
        if evq not in win:
            continue

        out.append({"claim": claim, "slot": slot, "subject": subj})

    return out

async def _maybe_update_interpretive_memory_v0(
    *,
    ctx,
    project_full: str,
    turn_index: int,
    window_text: str,
    assistant_text: str,
):
    win = (window_text or "").strip()
    a = (assistant_text or "").strip()
    if not win or not a:
        return

    combo = (win + "\nASSISTANT: " + a)[:18000]

    upd = await _extract_interpretive_memory_with_model(
        ctx=ctx,
        window_text=combo,
        turn_index=turn_index,
    )
    if not upd:
        return

    try:
        ctx.project_store.merge_interpretive_memory(
            project_full,
            new_obj=upd,
            last_updated_turn=turn_index,
        )
    except Exception:
        return

def _looks_like_bringup_nl_request(msg: str) -> bool:
    """
    Deterministic NL detector for 'bring it up later' requests.
    Conservative: triggers only on clear phrasing.
    """
    t = (msg or "").strip().lower()
    if not t:
        return False

    # Do not treat explicit commands as NL bringups
    if t.startswith(("!", "/cmd")):
        return False

    triggers = (
        "bring it up",
        "bring this up",
        "bring that up",
        "surface it",
        "surface this",
        "mention it next time",
        "mention this next time",
        "next time you talk to",
        "when you talk to him next",
        "when you talk to her next",
        "hold this as a theme",
        "hold it as a theme",
        "help him understand",
        "help her understand",
        "can you ask the other person",
        "can you ask him",
        "can you ask her",
        "can you mediate",
        "can you mediate this",
        "mediate this",
        "can you bring this up",
        "can you bring it up",
    )
    return any(x in t for x in triggers)


def _extract_bringup_topic_nl(msg: str, *, cap: int = 180) -> str:
    """
    Deterministic topic extraction:
    - Prefer text after 'theme:' / 'theme is:' when present
    - Else use the full message (clipped)
    """
    raw = (msg or "").strip()
    if not raw:
        return ""

    low = raw.lower()
    # Prefer explicit theme marker
    for key in ("theme is:", "theme:", "the theme is:"):
        i = low.find(key)
        if i != -1:
            out = raw[i + len(key):].strip()
            if out:
                out = out.replace("\r", " ").replace("\n", " ").strip()
                return out[:cap].rstrip() + ("…" if len(out) > cap else "")

    # Fallback: compact the whole request
    out = raw.replace("\r", " ").replace("\n", " ").strip()
    if len(out) > cap:
        out = out[:cap].rstrip() + "…"
    return out
def _extract_recent_therapist_formulation(conversation_history: List[Dict[str, str]], *, cap: int = 260) -> str:
    """
    Deterministic: extract a short therapist-side formulation from recent assistant turns.

    We intentionally prefer the assistant's own abstraction lines (e.g., "Based on what you've shared so far—tell me if this fits—..."),
    not the user's raw vent, to reduce privacy risk while keeping the "body of the issue".
    """
    try:
        hist = conversation_history or []
    except Exception:
        hist = []

    # Scan backward for a likely formulation block in assistant text
    triggers = (
        "based on what you’ve shared so far",
        "based on what you've shared so far",
        "tell me if this fits",
        "here’s what i think may be happening",
        "here's what i think may be happening",
    )

    stops = (
        "\none intervention",
        "\nsmall next step",
        "\none small next step",
        "\none question",
        "\none gentle question",
    )

    for m in reversed(hist[-24:]):
        if not isinstance(m, dict):
            continue
        if str(m.get("role") or "") != "assistant":
            continue
        txt = str(m.get("content") or "").strip()
        if not txt:
            continue

        low = txt.lower()
        if not any(t in low for t in triggers):
            continue

        # Trim off everything after common “stop” headers (keep the formulation part)
        cut = txt
        low2 = "\n" + low  # make stop matching more robust
        stop_pos = None
        for s in stops:
            p = low2.find(s)
            if p != -1:
                stop_pos = p - 1  # align back to original string index
                break
        if stop_pos is not None and stop_pos > 0:
            cut = txt[:stop_pos].strip()

        cut = cut.replace("\r", " ").replace("\n", " ").strip()
        if not cut:
            continue

        if len(cut) > cap:
            cut = cut[:cap].rstrip() + "…"
        return cut

    return ""
def _build_bringup_synopsis_for_confirmation(
    conversation_history: List[Dict[str, str]],
    *,
    cap: int = 420,
) -> str:
    """
    Deterministic synopsis used ONLY for the user's confirmation step.

    Preference order:
    1) assistant formulation block (tell me if this fits / here's what may be happening)
    2) otherwise: last assistant turn clipped

    Output is single-line and bounded.
    """
    txt = _extract_recent_therapist_formulation(conversation_history, cap=260).strip()
    if not txt:
        # fallback: last assistant message
        try:
            for m in reversed((conversation_history or [])[-12:]):
                if isinstance(m, dict) and str(m.get("role") or "") == "assistant":
                    txt = str(m.get("content") or "").strip()
                    if txt:
                        break
        except Exception:
            txt = ""

    txt = (txt or "").replace("\r", " ").replace("\n", " ").strip()
    if len(txt) > cap:
        txt = txt[:cap].rstrip() + "…"
    return txt
def _neutralize_partner_pronouns(text: str) -> str:
    """
    Deterministic rewrite to prevent pronoun flip when a bring-up
    is surfaced in the partner's session.

    We intentionally avoid clever NLP. Simple, conservative replacements only.
    """
    if not isinstance(text, str):
        return ""

    t = text

    # Normalize spacing
    t = " " + t + " "

    # Second-person -> role-neutral
    replacements = [
        (" for you ", " for one partner "),
        (" when you ", " when one partner "),
        (" if you ", " if one partner "),
        (" you feel ", " one partner feels "),
        (" you get ", " one partner gets "),
        (" your body ", " one partner’s body "),
        (" your ", " their "),
        (" you ", " one partner "),
        (" you're ", " one partner is "),
        (" you’re ", " one partner is "),
    ]

    # First-person -> role-neutral
    replacements += [
        (" i feel ", " one partner feels "),
        (" i get ", " one partner gets "),
        (" i’m ", " one partner is "),
        (" i'm ", " one partner is "),
        (" me ", " one partner "),
        (" my ", " one partner’s "),
    ]

    low = t.lower()
    out = t
    for a, b in replacements:
        if a in low:
            # do a case-insensitive replace while preserving surrounding text
            out = out.replace(a, b)

    out = out.strip()
    return out
def _couples_facts_map_has_any_entries(ctx: Any, project_full: str) -> bool:
    """
    Deterministic check: does facts_map.md contain at least one Tier-2 FACT entry?
    Used to trigger intake mode on fresh couple_* projects.
    """
    try:
        p = ctx.project_store.state_file_path(project_full, "facts_map")
        if not p or (not p.exists()):
            return False
        txt = p.read_text(encoding="utf-8", errors="replace")
        # Tier-2 entries always include "- FACT:" lines
        return ("- FACT:" in (txt or ""))
    except Exception:
        return False

def _facts_profile_for_project(ctx: Any, project_full: str) -> str:
    """
    Deterministic profile chooser for Tier-2 distillation cadence.
    """
    try:
        st = ctx.project_store.load_project_state(project_full) or {}
    except Exception:
        st = {}

    doms = st.get("domains") if isinstance(st, dict) else []
    dom_blob = " ".join([str(x) for x in (doms or [])]).lower()

    pname = (project_full or "").lower()

    # Couples/therapy gets therapist weighting.
    if "therapy" in dom_blob or "therapist" in dom_blob or "couple_" in pname or "therapy" in pname:
        return "therapist"

    return "general"

_INTERPRETIVE_MEMORY_SYSTEM = """
You are extracting INTERPRETIVE MEMORY from human conversation.

This is NOT factual memory.
Do NOT treat emotions, guesses, accusations, or interpretations as facts.

This memory is used ONLY to guide tone, framing, continuity, and therapeutic context.
It must tolerate ambiguity and contradiction.

Return ONLY strict JSON (no prose, no markdown).

You MUST follow this schema exactly (no extra top-level keys):

{
  "schema": "interpretive_memory_v1",
  "entities": [
    {
      "id": "stable_short_id",
      "label": "short human label",
      "role": "user|partner|child|other|unknown",
      "notes": ["single-line note"],
      "uncertainty": "low|medium|high",
      "evidence": [{"turn_index": 0, "excerpt": "verbatim substring from WINDOW"}]
    }
  ],
  "relationship_dynamics": [
    {
      "id": "stable_short_id",
      "statement": "neutral description of a recurring dynamic/pattern",
      "tension": "optional short phrase",
      "uncertainty": "low|medium|high",
      "evidence": [{"turn_index": 0, "excerpt": "verbatim substring from WINDOW"}]
    }
  ],
  "themes": [
    {
      "id": "stable_short_id",
      "theme": "short theme label",
      "detail": "one sentence detail",
      "uncertainty": "low|medium|high",
      "evidence": [{"turn_index": 0, "excerpt": "verbatim substring from WINDOW"}]
    }
  ],
  "values_goals": [
    {
      "id": "stable_short_id",
      "value_or_goal": "short statement",
      "uncertainty": "low|medium|high",
      "evidence": [{"turn_index": 0, "excerpt": "verbatim substring from WINDOW"}]
    }
  ],
  "open_ambiguities": [
    {
      "id": "stable_short_id",
      "question": "what is unclear / contradictory",
      "options": ["option A", "option B"],
      "evidence": [{"turn_index": 0, "excerpt": "verbatim substring from WINDOW"}]
    }
  ],
  "last_updated_turn": 0
}

Hard rules (non-negotiable):
- Evidence MUST be objects: {"turn_index": int, "excerpt": string}
- Evidence excerpts MUST be exact substrings of WINDOW (verbatim).
- Keep all strings single-line (no newlines).
- Every item MUST include uncertainty.
- CRITICAL: values_goals must be USER-attributed only.
  - For values_goals, all evidence excerpts MUST come from the USER portion of WINDOW.
  - The window includes a delimiter line "ASSISTANT:"; anything after that is assistant text and cannot be used as evidence for values_goals.
- Caps:
  - entities ≤ 12
  - relationship_dynamics ≤ 12
  - themes ≤ 12
  - values_goals ≤ 10
  - open_ambiguities ≤ 10
""".strip()



async def _extract_interpretive_memory_with_model(
    *,
    ctx: Any,
    window_text: str,
    turn_index: int,
) -> Dict[str, Any]:
    win = (window_text or "").strip()
    if not win:
        return {}

    # Split user vs assistant portion (used to enforce user-only evidence for values_goals)
    split_tag = "\nASSISTANT:"
    split_pos = win.find(split_tag)
    user_win = win if split_pos < 0 else win[:split_pos]

    messages = [
        {"role": "system", "content": _INTERPRETIVE_MEMORY_SYSTEM},
        {"role": "user", "content": "WINDOW:\n" + win[:14000]},
    ]

    try:
        raw = await asyncio.to_thread(ctx.call_openai_chat, messages)
    except Exception:
        return {}

    obj: Dict[str, Any] = {}
    try:
        m = re.search(r"\{.*\}", raw or "", re.DOTALL)
        parsed = json.loads(m.group(0)) if m else {}
        if isinstance(parsed, dict):
            obj = parsed
    except Exception:
        obj = {}

    if not isinstance(obj, dict):
        return {}

    # Strict schema gate
    if obj.get("schema") != "interpretive_memory_v1":
        return {}

    # Force last_updated_turn deterministically
    obj["last_updated_turn"] = int(turn_index or 0)

    def _clip_dict_list(v: Any, n: int) -> List[Dict[str, Any]]:
        if not isinstance(v, list):
            return []
        out: List[Dict[str, Any]] = []
        for x in v:
            if isinstance(x, dict):
                out.append(x)
            if len(out) >= n:
                break
        return out

    obj["entities"] = _clip_dict_list(obj.get("entities"), 12)
    obj["relationship_dynamics"] = _clip_dict_list(obj.get("relationship_dynamics"), 12)
    obj["themes"] = _clip_dict_list(obj.get("themes"), 12)
    obj["values_goals"] = _clip_dict_list(obj.get("values_goals"), 10)
    obj["open_ambiguities"] = _clip_dict_list(obj.get("open_ambiguities"), 10)

    def _norm_one_line(s: Any) -> str:
        return str(s or "").replace("\r", " ").replace("\n", " ").strip()

    def _evidence_to_objs(evs: Any) -> List[Dict[str, Any]]:
        """
        Normalize evidence:
        - if evidence is ["text", ...] -> [{"turn_index": turn_index, "excerpt": "text"}, ...]
        - if evidence is [{"excerpt":..}, ...] -> ensure turn_index present
        """
        out: List[Dict[str, Any]] = []
        if isinstance(evs, list):
            for e in evs:
                if isinstance(e, str):
                    ex = _norm_one_line(e)
                    if ex:
                        out.append({"turn_index": int(turn_index or 0), "excerpt": ex})
                elif isinstance(e, dict):
                    ex = _norm_one_line(e.get("excerpt"))
                    if not ex:
                        continue
                    ti = e.get("turn_index")
                    try:
                        ti2 = int(ti) if ti is not None else int(turn_index or 0)
                    except Exception:
                        ti2 = int(turn_index or 0)
                    out.append({"turn_index": ti2, "excerpt": ex})
        return out[:12]

    def _evidence_ok(evs: List[Dict[str, Any]], *, must_be_in: str) -> bool:
        for e in evs:
            ex = _norm_one_line(e.get("excerpt"))
            if ex and (ex not in must_be_in):
                return False
        return True

    # Validate + normalize each section
    for section in ("entities", "relationship_dynamics", "themes", "values_goals", "open_ambiguities"):
        cleaned: List[Dict[str, Any]] = []
        for item in obj.get(section, []):
            if not isinstance(item, dict):
                continue

            # Normalize strings to single-line
            for k, v in list(item.items()):
                if isinstance(v, str):
                    item[k] = _norm_one_line(v)

            # Require uncertainty; default to high when missing
            unc = _norm_one_line(item.get("uncertainty"))
            if unc not in ("low", "medium", "high"):
                item["uncertainty"] = "high"

            # Normalize evidence and require at least one
            ev_objs = _evidence_to_objs(item.get("evidence"))
            if not ev_objs:
                continue

            # Enforce verbatim evidence presence
            if section == "values_goals":
                # USER-only provenance
                if not _evidence_ok(ev_objs, must_be_in=user_win):
                    continue
            else:
                if not _evidence_ok(ev_objs, must_be_in=win):
                    continue

            item["evidence"] = ev_objs
            cleaned.append(item)

        obj[section] = cleaned

    return obj



async def _maybe_update_interpretive_memory(
    *,
    ctx: Any,
    project_full: str,
    turn_index: int,
    window_text: str,
    assistant_text: str,
) -> None:
    """
    Tier-X update that runs every assistant turn.
    IMPORTANT: even if extraction fails, we still write a sentinel understanding.json
    so we can prove the write-path is live.
    """
    win = (window_text or "").strip()
    a = (assistant_text or "").strip()
    if not win or not a:
        return

    combo = (win + "\nASSISTANT: " + a)[:18000]

    upd: Dict[str, Any] = {}
    try:
        upd = await _extract_interpretive_memory_with_model(
            ctx=ctx,
            window_text=combo,
            turn_index=int(turn_index or 0),
        )
    except Exception:
        upd = {}

    # Sentinel write to guarantee file creation + turn tracking
    if not isinstance(upd, dict) or upd.get("schema") != "interpretive_memory_v1":
        upd = {
            "schema": "interpretive_memory_v1",
            "entities": [],
            "relationship_dynamics": [],
            "themes": [],
            "values_goals": [],
            "open_ambiguities": [],
            "last_updated_turn": int(turn_index or 0),
        }

    try:
        ctx.project_store.merge_interpretive_memory(
            project_full,
            new_obj=upd,
            last_updated_turn=int(turn_index or 0),
        )
    except Exception:
        return

async def run_request_pipeline(
    *,
    ctx: Any,
    current_project_full: str,
    clean_user_msg: str,
    do_search: bool,
    search_results: str,
    conversation_history: List[Dict[str, str]],
    max_history_pairs: int,
    server_file_path: Path,
    allow_history_in_lookup: bool = True,
    search_cached: bool = False,
    thread_synthesis_text: str = "",
    active_expert: str = "default",
    suppress_output_side_effects: bool = False,
) -> Dict[str, Any]:

    """
    C6 — 4-stage request pipeline controller.

    Stages:
      1) intent classification (model JSON only)
      2) deterministic canonical retrieval (project_store)
      3) grounded generation (model, no fact invention) OR deterministic rendering
      4) deterministic safety gate (reject/regenerate/fallback)

    Must preserve:
      - existing deterministic pulse behavior
      - existing upload clarification loop (handled in server.py pre-routing)
      - existing deliverable + excel handling (delegated to run_model_pipeline)
      - existing web lookup behavior
    """
    ctx.ensure_project_scaffold(current_project_full)

    # Strip control-plane directives from user input.
    # These blocks are system-only and must not persist inside user messages.
    try:
        if isinstance(clean_user_msg, str) and clean_user_msg:
            clean_user_msg = re.sub(r"\[SCOPE\].*?\[/SCOPE\]", "", clean_user_msg, flags=re.S).strip()
            clean_user_msg = re.sub(r"\[GOAL_ONBOARD\].*?\[/GOAL_ONBOARD\]", "", clean_user_msg, flags=re.S).strip()
    except Exception:
        pass
    # Couples Mode Option A: compute once, early, and reuse.
    # This prevents bring-up draft logic from silently skipping due to late initialization.
    couples_mode = False
    try:
        couples_mode = str(current_project_full or "").split("/", 1)[0].lower().startswith("couple_")
    except Exception:
        couples_mode = False
    # ------------------------------------------------------------
    # Couples Therapy hard bootstrap invariant (deterministic)
    #
    # Why:
    # - The generic bootstrap/onboarding questions ("What are you trying to build...")
    #   come from THIS pipeline's bootstrap state machine.
    # - So Couples_Therapy must force bootstrap_status=active and therapist frame here,
    #   before any bootstrap_needed logic runs.
    # - Also ensures projects/<user>/_user exists even after deletion.
    # ------------------------------------------------------------
    try:
        user_seg = str(current_project_full or "").split("/", 1)[0].strip()
    except Exception:
        user_seg = ""

    try:
        proj_seg = str(current_project_full or "").split("/", 1)[1].strip()
    except Exception:
        proj_seg = ""

    try:
        proj_norm = (proj_seg or "").replace(" ", "_").lower()
    except Exception:
        proj_norm = ""

    if (user_seg.lower().startswith("couple_")) and (proj_norm == "couples_therapy"):

        # 1) Ensure global user memory scaffold exists on disk (global user identity store)
        try:
            ctx.project_store.load_user_profile(user_seg)
        except Exception:
            pass

        # 2) Enforce Couples Therapist frame + active bootstrap (idempotent)
        default_goal = "Couples therapy — improve communication, repair trust, and address recurring conflict patterns."

        try:
            st_force = ctx.project_store.load_project_state(current_project_full) or {}
        except Exception:
            st_force = {}

        if not str(st_force.get("goal") or "").strip():
            st_force["goal"] = default_goal

        st_force["bootstrap_status"] = "active"
        st_force["project_mode"] = "hybrid"

        st_force["expert_frame"] = {
            "status": "active",
            "label": "Couples Therapist",
            "directive": (
                "You are a couples therapist. Be warm, steady, and practical. "
                "Focus on communication patterns, emotions, needs, boundaries, repair, and concrete next steps. "
                "Hard rule: never reveal, quote, attribute, or confirm any partner private content. "
                "If asked 'what did they say', refuse briefly and redirect to a safe process (themes, needs, consent-based invitations). "
                "End like a therapist: one small next step and one gentle question. "
                "Do NOT talk like a project manager; no deliverables or output formats."
            ),
            "set_reason": "couples_autobootstrap",
            "updated_at": (ctx.project_store.now_iso() if hasattr(ctx.project_store, "now_iso") else ""),
        }

        try:
            ctx.project_store.write_project_state_fields(current_project_full, st_force)
        except Exception:
            pass
    # ------------------------------------------------------------
    # Patch #2 — Memory scope/permission policy (deterministic, background)
    #
    # Natural-language policy updates (meta commands):
    #   - "don't store X"
    #   - "keep X only in this project"
    #   - "don't bring up X unless I ask"
    #   - "you can remember X globally"
    #
    # Hard rules:
    # - Deterministic parsing only (no inference).
    # - Policy updates do NOT write Tier-1 facts.
    # - Memory remains in the background (short acknowledgement only).
    # ------------------------------------------------------------
    try:
        _msg_pol = (clean_user_msg or "").strip()
        _low_pol = _msg_pol.lower()

        # Only apply to real user sessions with a user segment
        _user_seg_pol = ""
        try:
            _user_seg_pol = str(current_project_full or "").split("/", 1)[0].strip()
        except Exception:
            _user_seg_pol = ""

        def _pol_extract_tail(prefixes: Tuple[str, ...]) -> str:
            t = _msg_pol.strip()
            low = _low_pol
            for p in prefixes:
                if low.startswith(p):
                    return t[len(p):].strip().strip('"').strip("'").strip()
            return ""

        def _pol_entity_key_from_phrase(x: str) -> str:
            """
            Deterministic mapping from common phrases to entity keys.
            Fall back to "" (meaning use substring match).
            """
            lx = (x or "").lower().strip()
            if not lx:
                return ""
            if any(k in lx for k in ("birthday", "birthdate", "date of birth", "born")):
                return "user.identity.birthdate"
            if any(k in lx for k in ("where i live", "where i’m", "where i'm", "where i am", "location", "address", "live in")):
                return "user.identity.location"
            if "time zone" in lx or "timezone" in lx or "central time" in lx:
                return "user.identity.timezone"
            if any(k in lx for k in ("my name", "preferred name", "name")):
                return "user.identity.name"
            if any(k in lx for k in ("girlfriend", "boyfriend", "partner name", "partner's name", "girlfriend's name", "boyfriend's name")):
                return "user.relationship.partner.name"
            if any(k in lx for k in ("son", "daughter", "child name", "kid name")):
                return "user.relationship.child.name"
            return ""

        _did_policy = False
        _ack = ""

        if _user_seg_pol:
            # do_not_store
            x = _pol_extract_tail(("don't store ", "do not store ", "stop storing ", "never store "))
            if x:
                ek = _pol_entity_key_from_phrase(x)
                if ek:
                    ctx.project_store.upsert_user_memory_policy_rule(
                        _user_seg_pol,
                        action="do_not_store",
                        match_type="entity_key",
                        match_value=ek,
                        note="nl_update",
                    )
                else:
                    ctx.project_store.upsert_user_memory_policy_rule(
                        _user_seg_pol,
                        action="do_not_store",
                        match_type="substring",
                        match_value=x,
                        note="nl_update",
                    )
                _did_policy = True
                _ack = "Understood."

            # project_only
            if not _did_policy:
                # Accept: "keep X only in this project" / "X only in this project" / "project only: X"
                x2 = ""
                if "only in this project" in _low_pol:
                    # take text before the phrase when possible
                    x2 = _msg_pol.lower().split("only in this project", 1)[0].strip()
                    x2 = re.sub(r"^(keep|store)\s+", "", x2, flags=re.IGNORECASE).strip()
                if not x2:
                    x2 = _pol_extract_tail(("project only: ", "project-only: ", "project only ", "project-only "))
                if x2:
                    ek = _pol_entity_key_from_phrase(x2)
                    if ek:
                        ctx.project_store.upsert_user_memory_policy_rule(
                            _user_seg_pol,
                            action="project_only",
                            match_type="entity_key",
                            match_value=ek,
                            note="nl_update",
                        )
                    else:
                        ctx.project_store.upsert_user_memory_policy_rule(
                            _user_seg_pol,
                            action="project_only",
                            match_type="substring",
                            match_value=x2,
                            note="nl_update",
                        )
                    _did_policy = True
                    _ack = "Understood."

            # do_not_resurface
            if not _did_policy:
                # Accept: "don't bring up X unless I ask" / "don't mention X unless I ask"
                if ("unless i ask" in _low_pol) and any(k in _low_pol for k in ("don't bring up", "do not bring up", "don't mention", "do not mention")):
                    x3 = _msg_pol
                    # strip leading clause
                    for p in ("don't bring up ", "do not bring up ", "don't mention ", "do not mention "):
                        if _low_pol.startswith(p):
                            x3 = _msg_pol[len(p):].strip()
                            break
                    x3 = x3.split("unless I ask", 1)[0].split("unless i ask", 1)[0].strip()
                    if x3:
                        ek = _pol_entity_key_from_phrase(x3)
                        if ek:
                            ctx.project_store.upsert_user_memory_policy_rule(
                                _user_seg_pol,
                                action="do_not_resurface",
                                match_type="entity_key",
                                match_value=ek,
                                note="nl_update",
                            )
                        else:
                            ctx.project_store.upsert_user_memory_policy_rule(
                                _user_seg_pol,
                                action="do_not_resurface",
                                match_type="substring",
                                match_value=x3,
                                note="nl_update",
                            )
                        _did_policy = True
                        _ack = "Understood."

            # allow_global
            if not _did_policy:
                x4 = _pol_extract_tail(("remember ", "you can remember ", "you can store "))
                if x4 and ("globally" in _low_pol):
                    x4 = x4.replace("globally", "").strip()
                    ek = _pol_entity_key_from_phrase(x4)
                    if ek:
                        ctx.project_store.upsert_user_memory_policy_rule(
                            _user_seg_pol,
                            action="allow_global",
                            match_type="entity_key",
                            match_value=ek,
                            note="nl_update",
                        )
                    else:
                        ctx.project_store.upsert_user_memory_policy_rule(
                            _user_seg_pol,
                            action="allow_global",
                            match_type="substring",
                            match_value=x4,
                            note="nl_update",
                        )
                    _did_policy = True
                    _ack = "Understood."

        if _did_policy:
            user_answer = _ack or "Understood."
            ctx.snapshot_assistant_output(current_project_full, user_answer)
            return {
                "user_answer": user_answer,
                "lookup_mode": False,
                "conversation_history": conversation_history,
            }

    except Exception:
        pass

    # ------------------------------------------------------------
    # Tier-1 AUTO CAPTURE (deterministic, truth-bound)
    #
    # Every user turn produces zero-or-more Tier-1 fact candidates:
    # - extracted ONLY from the literal user message (no inference)
    # - appended to state/facts_raw.jsonl with evidence_quote + provenance
    # - normalized when new candidates were appended
    # - Tier-2 distill runs on a safe cadence (every N turns) when new candidates exist
    # ------------------------------------------------------------
    try:
        st_mem = ctx.project_store.load_project_state(current_project_full) or {}
    except Exception:
        st_mem = {}

    try:
        turn_n = int((st_mem.get("facts_turn_counter") or 0))
    except Exception:
        turn_n = 0

    turn_n = turn_n + 1

    # Time anchors v1: silently timestamp concrete "I just did X" events (bounded).
    try:
        label = _time_maybe_extract_anchor_label(clean_user_msg)
        if label:
            tz_name = "America/Chicago"
            # Best-effort: reuse Tier-2G timezone if present
            try:
                cp = str(current_project_full or "")
                user_seg_tz = cp.split("/", 1)[0].strip() if "/" in cp else cp.strip()
            except Exception:
                user_seg_tz = ""
            prof_tz: Dict[str, Any] = {}
            try:
                if user_seg_tz and hasattr(ctx, "project_store") and hasattr(ctx.project_store, "load_user_profile"):
                    prof0 = ctx.project_store.load_user_profile(user_seg_tz)
                    if isinstance(prof0, dict):
                        prof_tz = prof0
            except Exception:
                prof_tz = {}
            try:
                ident_tz = prof_tz.get("identity") if isinstance(prof_tz, dict) else {}
                if not isinstance(ident_tz, dict):
                    ident_tz = {}
                tz = ident_tz.get("timezone") if isinstance(ident_tz.get("timezone"), dict) else {}
                tz_val = str((tz or {}).get("value") or "").strip()
                if tz_val:
                    tz_name = tz_val
            except Exception:
                pass

            if ZoneInfo is not None:
                now_dt = datetime.now(tz=ZoneInfo(tz_name))
            else:
                now_dt = datetime.now()

            _time_update_project_anchors(ctx, current_project_full, label=label, now_dt=now_dt, tz_name=tz_name)
    except Exception:
        pass

    appended = 0
    global_appended = 0

    # Build a bounded “window” so the extractor can interpret natural human phrasing.
    # We keep it small for cost + determinism and still require evidence quotes to match verbatim.
    win_lines: List[str] = []
    try:
        tail = (conversation_history or [])[-8:]
    except Exception:
        tail = []

    try:
        for m in tail:
            if not isinstance(m, dict):
                continue
            role = str(m.get("role") or "").strip().upper()
            content = str(m.get("content") or "").strip()
            if not content:
                continue
            if role not in ("USER", "ASSISTANT", "SYSTEM"):
                role = "USER"
            # keep each line bounded
            if len(content) > 500:
                content = content[:499].rstrip() + "…"
            win_lines.append(f"{role}: {content}")
    except Exception:
        win_lines = []

    # Always include the current user message (the thing we are extracting from)
    try:
        cu = (clean_user_msg or "").strip()
        if cu:
            if len(cu) > 800:
                cu = cu[:799].rstrip() + "…"
            win_lines.append(f"USER: {cu}")
    except Exception:
        pass

    window_text = "\n".join(win_lines).strip()

    try:
        cands = await _extract_tier1_candidates_with_model(ctx=ctx, window_text=window_text, max_items=8)
    except Exception:
        cands = []

    # Conservative fallback: if model extraction fails, keep the old deterministic extractor.
    if not cands:
        try:
            cands = _extract_tier1_candidates_from_user_msg(clean_user_msg, max_items=6)
        except Exception:
            cands = []


    # Deterministic core-identity detection (guardrail):
    # If this turn contains identity facts we MUST distill immediately so Tier-2
    # is ready for recall questions without cadence timing sensitivity.
    core_identity_added = False
    try:
        for _c in (cands or []):
            _cl = str(_c.get("claim") or "").lower()
            if (
                ("my preferred name is" in _cl)
                or ("my name is" in _cl)
                or ("i go by" in _cl)
                or ("i live in" in _cl)
                or ("my favorite color is" in _cl)
                or ("my favourite color is" in _cl)
            ):
                core_identity_added = True
                break
    except Exception:
        core_identity_added = False

    # Append Tier-1 candidates (bounded)
    for c in (cands or [])[:6]:
        try:
            claim = str(c.get("claim") or "").strip()
            # Policy gate (write-time) — must run BEFORE Tier-1 append.
            try:
                _user_seg_w = str(current_project_full or "").split("/", 1)[0].strip()
            except Exception:
                _user_seg_w = ""

            # Derive entity_key deterministically for policy matching.
            try:
                ek = ctx.project_store._tier1_entity_key_guess(claim, str(c.get("slot") or ""), str(c.get("subject") or "user"))
            except Exception:
                ek = ""

            pol_dec = {"store": True, "mirror_global": True, "allow_resurface": True}
            try:
                if _user_seg_w:
                    pol_dec = ctx.project_store.policy_decision_for_tier1_claim(
                        _user_seg_w,
                        project_full=current_project_full,
                        claim=claim,
                        entity_key=(ek or ""),
                    )
            except Exception:
                pol_dec = {"store": True, "mirror_global": True, "allow_resurface": True}

            if pol_dec.get("store") is False:
                continue

            # Normalize month-name birthdates to ISO YYYY-MM-DD (Global identity fact)
            # HARD RULE: only normalize when the claim is explicitly a birthdate statement.
            try:
                iso_bd = None
                _lc = (claim or "").lower()
                if ("my birthday is" in _lc) or ("i was born on" in _lc):
                    iso_bd = _normalize_birthdate_text(claim)
            except Exception:
                iso_bd = None
            if iso_bd:
                claim = f"I was born on {iso_bd}"
            slot = str(c.get("slot") or "other").strip()
            subj = str(c.get("subject") or "user").strip()
            if not claim:
                continue
            # Patch #2 policy gate (write-time) — must happen BEFORE Tier-1 append.
            try:
                user_seg = str(current_project_full or "").split("/", 1)[0].strip()
            except Exception:
                user_seg = ""

            # Deterministic entity_key for policy match
            try:
                entity_key = ctx.project_store._tier1_entity_key_guess(claim, slot, subj)
            except Exception:
                entity_key = ""

            evq = (clean_user_msg[:260] + "…") if len(clean_user_msg) > 261 else clean_user_msg

            pol = {"store": True, "mirror_global": True, "allow_resurface": True}
            try:
                if user_seg:
                    pol = ctx.project_store.policy_decision_for_tier1_claim(
                        user_seg,
                        project_full=current_project_full,
                        claim=claim,
                        entity_key=(entity_key or ""),
                    )
            except Exception:
                pol = {"store": True, "mirror_global": True, "allow_resurface": True}

            if pol.get("store") is False:
                continue

            r = ctx.project_store.append_fact_raw_candidate(
                current_project_full,
                claim=claim,
                slot=slot,
                subject=subj,
                source="chat",
                evidence_quote=(clean_user_msg[:260] + "…") if len(clean_user_msg) > 261 else clean_user_msg,
                turn_index=turn_n,
                timestamp=(ctx.project_store.now_iso() if hasattr(ctx.project_store, "now_iso") else ""),
            )
            if isinstance(r, dict) and r.get("ok") is True:
                appended += 1
                # Also write to GLOBAL Tier-1G and rebuild Tier-2G (profile)
                try:
                    user_seg = str(current_project_full or "").split("/", 1)[0].strip()
                except Exception:
                    user_seg = ""
                if user_seg:
                    # Tier-2G mirroring trigger (deterministic, explicit):
                    # - only mirror global-eligible facts
                    # - rebuild Tier-2G once per turn (after loop), not per appended fact
                    try:
                        if (pol_dec.get("mirror_global") is True) and _tier1_global_eligible_for_tier2g(
                            claim=claim,
                            slot=slot,
                            subject=subj,
                            evidence_quote=((clean_user_msg[:260] + "…") if len(clean_user_msg) > 261 else clean_user_msg),
                        ):
                            r2 = ctx.project_store.append_user_fact_raw_candidate(
                                user_seg,
                                claim=claim,
                                slot=slot,
                                subject=subj,
                                source="chat",
                                evidence_quote=(clean_user_msg[:260] + "…") if len(clean_user_msg) > 261 else clean_user_msg,
                                turn_index=turn_n,
                                timestamp=(ctx.project_store.now_iso() if hasattr(ctx.project_store, "now_iso") else ""),
                            )
                            if isinstance(r2, dict) and r2.get("ok") is True:
                                global_appended += 1
                    except Exception:
                        pass
        except Exception:
            continue
    # Tier-2G rebuild trigger (deterministic):
    # Rebuild global profile once per turn, only if we appended any global-eligible Tier-1G facts.
    try:
        user_seg2 = str(current_project_full or "").split("/", 1)[0].strip()
    except Exception:
        user_seg2 = ""
    if user_seg2 and (global_appended > 0):
        try:
            ctx.project_store.rebuild_user_profile_from_user_facts(user_seg2)
        except Exception:
            pass

    # Persist the counter (canonical, deterministic)
    try:
        ctx.project_store.write_project_state_fields(
            current_project_full,
            {
                "facts_turn_counter": turn_n,
            },
        )
    except Exception:
        pass

    # ------------------------------------------------------------
    # Tier-2 cadence (deterministic) — distill when "dirty"
    #
    # Problem fixed:
    # - Smoke test includes no-store turns + recall questions (appended=0),
    #   which previously prevented distill from running.
    #
    # Approach:
    # - Track a deterministic "facts_dirty" flag in project_state.
    # - Set facts_dirty=True whenever we append Tier-1.
    # - Run distill on cadence when facts_dirty is True, even if this turn appended=0.
    # - Additionally: for obvious recall questions, force a distill if facts_dirty is True.
    # ------------------------------------------------------------
    try:
        st_mem2 = ctx.project_store.load_project_state(current_project_full) or {}
    except Exception:
        st_mem2 = {}

    facts_dirty = bool(st_mem2.get("facts_dirty") is True)

    if appended > 0:
        facts_dirty = True
        try:
            ctx.project_store.write_project_state_fields(
                current_project_full,
                {"facts_dirty": True},
            )
        except Exception:
            pass

        try:
            ctx.project_store.normalize_facts_raw_jsonl(current_project_full)
        except Exception:
            pass

    # Deterministic recall guess (no model calls)
    msg_l2 = (clean_user_msg or "").lower()
    looks_like_recall = (
        ("what's my" in msg_l2 or "what is my" in msg_l2 or "whats my" in msg_l2)
        and any(k in msg_l2 for k in ("name", "preferred name", "live", "location", "favorite color", "favourite color", "color"))
    )

    # Distill policy (foundational):
    # - If we appended any Tier-1 facts THIS turn, distill immediately so Tier-2P is ready next turn.
    # - Otherwise, distill on cadence when dirty, or on recall-ish turns.
    do_distill = False

    if appended > 0:
        do_distill = True
    else:
        # Distill cadence: every N turns when facts are dirty
        N = 3
        do_distill = facts_dirty and ((turn_n % N) == 0 or looks_like_recall or core_identity_added)

    if do_distill:
        try:
            prof = _facts_profile_for_project(ctx, current_project_full)
        except Exception:
            prof = "general"

        # Ensure Tier-1 rows are normalized under current rules BEFORE distilling,
        # even when this turn did not append (e.g., recall question turn).
        try:
            ctx.project_store.normalize_facts_raw_jsonl(current_project_full)
        except Exception:
            pass

        try:
            ctx.project_store.distill_facts_raw_to_facts_map_tier2p(current_project_full)
            ctx.project_store.write_project_state_fields(
                current_project_full,
                {"facts_dirty": False},
            )
        except Exception:
            pass


    # ------------------------------------------------------------
    # PRE-BOOTSTRAP CONSTRAINT CAPTURE (AUTHORITATIVE)
    #
    # Any hard execution constraints stated by the user MUST be
    # captured BEFORE bootstrap / goal onboarding / clarification.
    # ------------------------------------------------------------
    try:
        st0 = ctx.project_store.load_project_state(current_project_full) or {}
    except Exception:
        st0 = {}

    user_rules = st0.get("user_rules")
    if not isinstance(user_rules, list):
        user_rules = []

    msg_l = (clean_user_msg or "").lower().strip()

    captured = False

    # Hard constraint: no questions
    if msg_l in ("no questions", "dont ask questions", "don't ask questions", "no more questions"):
        if "no questions" not in user_rules:
            user_rules.append("no questions")
            captured = True

    # Hard constraint: no emoji
    if msg_l in ("no emoji", "no emojis"):
        if "no emoji" not in user_rules:
            user_rules.append("no emoji")
            captured = True

    if captured:
        try:
            ctx.project_store.write_project_state_fields(
                current_project_full,
                {
                    "user_rules": user_rules,
                },
            )
        except Exception:
            pass

        # IMPORTANT:
        # This message is a constraint declaration, NOT a goal,
        # so we must NOT continue into bootstrap or clarification.
        user_answer = "Understood."
        ctx.snapshot_assistant_output(current_project_full, user_answer)
        return {
            "user_answer": user_answer,
            "lookup_mode": False,
            "conversation_history": conversation_history,
        }

    # Deterministic pre-check (C6): Pulse/Status/Resume MUST bypass model intent gating.
    # This must run BEFORE any model call (Stage 1).
    try:
        is_pulse_cmd = _is_exact_pulse_cmd(clean_user_msg)
    except NameError:
        # Defensive fallback: if an older/stale model_pipeline is imported that lacks this helper,
        # fall back to the deterministic exact-cmd set.
        is_pulse_cmd = (_normalize_exact_cmd(clean_user_msg) in _PULSE_EXACT_CMDS)

    if is_pulse_cmd:
        try:
            pulse = ctx.project_store.build_truth_bound_pulse(current_project_full)
        except Exception:
            pulse = "\n".join([
                "Project Pulse (truth-bound)",
                "Goal: (not set yet)",
                "Current deliverable: (none yet)",
                "Recent decisions: (none yet)",
                "Recent upload notes: (none yet)",
                "Next action: (not set yet)",
                "Facts map: empty",
            ])
        user_answer = (pulse or "").strip()
        ctx.snapshot_assistant_output(current_project_full, user_answer)
        return {
            "user_answer": user_answer,
            "lookup_mode": False,
            "conversation_history": conversation_history,
        }
    # C9 deterministic pre-check: inbox/pending queries must bypass the model.
    if _is_inbox_query(clean_user_msg):
        try:
            summary = ctx.project_store.summarize_inbox_open(current_project_full)
        except Exception:
            summary = "Inbox (open): (none)"
        user_answer = (summary or "").strip()
        ctx.snapshot_assistant_output(current_project_full, user_answer)
        return {
            "user_answer": user_answer,
            "lookup_mode": False,
            "conversation_history": conversation_history,
        }

    # C6.3 Stage 0 — Bootstrap detection + mode read (deterministic)
    st = {}
    try:
        st = ctx.project_store.load_project_state(current_project_full) or {}
    except Exception:
        st = {}

    project_mode = str(st.get("project_mode") or "closed_world").strip()
    if project_mode not in ("open_world", "closed_world", "hybrid"):
        project_mode = "closed_world"

    bootstrap_status = str(st.get("bootstrap_status") or "active").strip()
    goal0 = str(st.get("goal") or "").strip()

    # ------------------------------------------------------------
    # EFL v1 — bootstrap-safe "project starts alive"
    # If this is a new project and the user gives a normal first message,
    # accept it as the goal immediately so bootstrap does not hijack turn 1.
    # ------------------------------------------------------------
    msg0 = (clean_user_msg or "").strip()
    if (not goal0) and msg0:
        low0 = msg0.lower()

        # Reject trivial greeting-only messages from becoming goals.
        # (Keep conservative; we can expand later if needed.)
        greeting_only = low0 in ("hi", "hello", "hey", "yo", "sup", "hiya", "howdy")

        # Only skip auto-goal for obvious commands / meta controls
        looks_like_command = (
            _is_exact_pulse_cmd(msg0)
            or _is_inbox_query(msg0)
            or _looks_like_bringup_nl_request(msg0)
            or low0.startswith(("expert frame:", "set expert frame:", "set frame:", "change frame", "switch frame"))
        )

        # Only auto-capture as goal when it looks like an actual goal statement.
        if (not looks_like_command) and (not greeting_only) and (10 <= len(msg0) <= 420):
            try:
                ctx.project_store.write_project_state_fields(
                    current_project_full,
                    {"goal": msg0, "bootstrap_status": "active"},
                )
                st = ctx.project_store.load_project_state(current_project_full) or st
            except Exception:
                pass
            bootstrap_status = str(st.get("bootstrap_status") or "active").strip()
            goal0 = str(st.get("goal") or "").strip()

    # ------------------------------------------------------------
    # EFL v1 — infer/propose/confirm BEFORE bootstrap early-returns
    # ------------------------------------------------------------
    try:
        ef0 = _efl_normalize_frame((st or {}).get("expert_frame"))
    except Exception:
        ef0 = {"status": "", "label": "", "directive": "", "set_reason": "", "updated_at": ""}

    # explicit user set/change
    cmd_label = _efl_parse_set_cmd(clean_user_msg)
    if cmd_label:
        ef_new = {
            "status": "active",
            "label": cmd_label,
            "directive": f"Optimize WIN under the '{cmd_label}' professional frame; stay grounded in canonical snippets; take action first then ask one high-leverage question.",
            "set_reason": "explicit_user_set",
            "updated_at": (ctx.project_store.now_iso() if hasattr(ctx.project_store, "now_iso") else ""),
        }
        try:
            ctx.project_store.write_project_state_fields(current_project_full, {"expert_frame": ef_new})
            st = ctx.project_store.load_project_state(current_project_full) or st
        except Exception:
            pass
        ef0 = _efl_normalize_frame((st or {}).get("expert_frame"))

    # proposed -> yes/no handling
    if ef0.get("status") == "proposed":
        if _is_affirmation_bootstrap(clean_user_msg):
            ef_yes = dict(ef0)
            ef_yes["status"] = "active"
            ef_yes["set_reason"] = (ef_yes.get("set_reason") or "inferred") + " + user_confirmed"
            try:
                ctx.project_store.write_project_state_fields(current_project_full, {"expert_frame": ef_yes})
                st = ctx.project_store.load_project_state(current_project_full) or st
            except Exception:
                pass
            ef0 = _efl_normalize_frame((st or {}).get("expert_frame"))
        elif _is_negative_bootstrap(clean_user_msg):
            try:
                ctx.project_store.write_project_state_fields(
                    current_project_full,
                    {
                        "expert_frame": {
                            "status": "",
                            "label": "",
                            "directive": "",
                            "set_reason": "user_rejected",
                            "updated_at": (ctx.project_store.now_iso() if hasattr(ctx.project_store, "now_iso") else ""),
                        }
                    },
                )
                st = ctx.project_store.load_project_state(current_project_full) or st
            except Exception:
                pass
            ef0 = _efl_normalize_frame((st or {}).get("expert_frame"))

    # no frame -> infer and propose
    if not ef0.get("status"):
        cand = _efl_infer_candidate(
            project_full=current_project_full,
            project_state=st if isinstance(st, dict) else {},
            user_msg=clean_user_msg,
        )
        ef_prop = {
            "status": "proposed",
            "label": str(cand.get("label") or "").strip(),
            "directive": str(cand.get("directive") or "").strip(),
            "set_reason": str(cand.get("set_reason") or "inferred").strip(),
            "updated_at": (ctx.project_store.now_iso() if hasattr(ctx.project_store, "now_iso") else ""),
        }
        try:
            ctx.project_store.write_project_state_fields(current_project_full, {"expert_frame": ef_prop})
            st = ctx.project_store.load_project_state(current_project_full) or st
        except Exception:
            pass

    bootstrap_needed = (not goal0) or (bootstrap_status != "active")

    # ------------------------------------------------------------
    # ENFORCE "NO QUESTIONS" DURING BOOTSTRAP
    # ------------------------------------------------------------
    try:
        st_rules = ctx.project_store.load_project_state(current_project_full).get("user_rules", [])
    except Exception:
        st_rules = []

    no_questions = isinstance(st_rules, list) and ("no questions" in st_rules)
    # C6.3.0 — Usability-first bootstrap escape hatch (deterministic)
    # If the project has no goal yet, treat a normal first message as the goal so the project "starts alive".
    if bootstrap_needed:
        msg0 = (clean_user_msg or "").strip()
        low0 = msg0.lower()

        looks_like_command = (
            _is_exact_pulse_cmd(msg0)
            or _is_inbox_query(msg0)
            or _looks_like_bringup_nl_request(msg0)
            or low0.startswith((
                "switch project:", "use project:", "new project:", "start project:",
                "goal:", "projects", "list projects",
                "patch", "/selfpatch", "/serverpatch", "/patch-server",
            ))
        )
        is_question = msg0.endswith("?")

        if (not looks_like_command) and (not is_question) and (10 <= len(msg0) <= 420):
            try:
                ctx.project_store.write_project_state_fields(
                    current_project_full,
                    {
                        "goal": msg0,
                        "bootstrap_status": "active",
                        "project_mode": "hybrid",
                    },
                )
            except Exception:
                pass

            # Refresh local view and continue as active
            try:
                st = ctx.project_store.load_project_state(current_project_full) or {}
            except Exception:
                st = {}

            bootstrap_status = str(st.get("bootstrap_status") or "active").strip()
            goal0 = str(st.get("goal") or "").strip()
            bootstrap_needed = (not goal0) or (bootstrap_status != "active")

    # C6.3 — Bootstrap state machine (goal onboarding only)
    if bootstrap_needed:
        # If we already proposed a goal/mode and we're waiting for confirmation:
        if bootstrap_status == "goal_proposed":
            if _is_affirmation_bootstrap(clean_user_msg):
                cand_goal = str(st.get("bootstrap_candidate_goal") or "").strip()
                cand_mode = str(st.get("bootstrap_candidate_project_mode") or "").strip()
                cand_domains = st.get("bootstrap_candidate_domains")
                if not isinstance(cand_domains, list):
                    cand_domains = []
                cand_domains = [str(x).strip() for x in cand_domains if str(x).strip()][:8]

                if cand_mode not in ("open_world", "closed_world", "hybrid"):
                    cand_mode = "closed_world"

                # Commit to canonical state (C6.2 sanitizer via project_store)
                try:
                    ctx.project_store.write_project_state_fields(
                        current_project_full,
                        {
                            "goal": cand_goal,
                            "project_mode": cand_mode,
                            "domains": cand_domains,
                            "bootstrap_status": "active",
                            # clear proposal fields (optional keys)
                            "bootstrap_candidate_goal": "",
                            "bootstrap_candidate_project_mode": "",
                            "bootstrap_candidate_domains": [],
                        },
                    )
                except Exception:
                    pass

                user_answer = "Confirmed. Project initialized."
                ctx.snapshot_assistant_output(current_project_full, user_answer)
                return {
                    "user_answer": user_answer,
                    "lookup_mode": False,
                    "conversation_history": conversation_history,
                }

            if _is_negative_bootstrap(clean_user_msg):
                try:
                    ctx.project_store.write_project_state_fields(
                        current_project_full,
                        {
                            "bootstrap_status": "needs_goal",
                            "bootstrap_candidate_goal": "",
                            "bootstrap_candidate_project_mode": "",
                            "bootstrap_candidate_domains": [],
                        },
                    )
                except Exception:
                    pass

                if no_questions:
                    user_answer = "Proceeding without questions."
                else:
                    user_answer = "OK — tell me the project goal in one sentence."

                ctx.snapshot_assistant_output(current_project_full, user_answer)
                return {
                    "user_answer": user_answer,
                    "lookup_mode": False,
                    "conversation_history": conversation_history,
                }

            # Still waiting
            user_answer = "Confirm the proposed goal + mode with: yes / no"
            ctx.snapshot_assistant_output(current_project_full, user_answer)
            return {
                "user_answer": user_answer,
                "lookup_mode": False,
                "conversation_history": conversation_history,
            }

        # Step A — Intent synthesis (model JSON only)
        messages_boot = [
            {"role": "system", "content": _BOOTSTRAP_INTENT_SYSTEM},
            {"role": "user", "content": clean_user_msg},
        ]
        raw_boot = await asyncio.to_thread(ctx.call_openai_chat, messages_boot)
        boot = _safe_json_extract(raw_boot)

        cand_goal = str(boot.get("candidate_goal") or "").strip()
        cand_mode = str(boot.get("candidate_project_mode") or "").strip()
        cand_domains = boot.get("candidate_domains")
        if not isinstance(cand_domains, list):
            cand_domains = []
        cand_domains = [str(x).strip() for x in cand_domains if str(x).strip()][:8]

        qs = boot.get("questions")
        if not isinstance(qs, list):
            qs = []
        questions = [str(x).strip() for x in qs if str(x).strip()][:2]

        # Hard stop: bootstrap may ask at most TWO clarification questions total.
        # Infer "already asked" from recent assistant messages to avoid adding new persisted state.
        asked = 0
        for m in (conversation_history or [])[-20:]:
            if not isinstance(m, dict):
                continue
            if str(m.get("role") or "") != "assistant":
                continue
            t = str(m.get("content") or "")
            if not t:
                continue
            if "Confirm? (yes / no)" in t:
                continue
            if t.startswith("Project Pulse (truth-bound)"):
                continue
            if "?" in t:
                asked += 1
        if asked >= 2:
            questions = []

        # Domain lock (minimal): if we have candidate domains, discard off-domain questions.
        if cand_domains and questions:
            dom_kw = set()
            for d in cand_domains:
                for w in re.findall(r"[a-z0-9]+", str(d).lower()):
                    if len(w) >= 4:
                        dom_kw.add(w)
            if dom_kw:
                questions = [q for q in questions if any(w in q.lower() for w in dom_kw)] or []

        if cand_mode not in ("open_world", "closed_world", "hybrid"):
            cand_mode = "closed_world"

        # Step B — Ask questions (max 2). No facts/decisions created.
        if questions:
            if no_questions:
                user_answer = "Proceeding with reasonable assumptions."
            else:
                user_answer = "\n".join(questions).strip()
            ctx.snapshot_assistant_output(current_project_full, user_answer)
            return {
                "user_answer": user_answer,
                "lookup_mode": False,
                "conversation_history": conversation_history,
            }

        # Step C — Propose goal + mode, ask confirm (yes/no)
        try:
            ctx.project_store.write_project_state_fields(
                current_project_full,
                {
                    "bootstrap_status": "goal_proposed",
                    "bootstrap_candidate_goal": cand_goal,
                    "bootstrap_candidate_project_mode": cand_mode,
                    "bootstrap_candidate_domains": cand_domains,
                },
            )
        except Exception:
            pass

        lines = []
        lines.append(f"Proposed Goal: {cand_goal or '(missing)'}")
        lines.append(f"Proposed Project Mode: {cand_mode}")
        lines.append(f"Domains: {cand_domains}")
        lines.append("")
        lines.append("Confirm? (yes / no)")
        user_answer = "\n".join(lines).strip()

        ctx.snapshot_assistant_output(current_project_full, user_answer)
        return {
            "user_answer": user_answer,
            "lookup_mode": False,
            "conversation_history": conversation_history,
        }
    # ------------------------------------------------------------
    # EFL v1 — Expert Frame Lock (canonical in project_state.json)
    #
    # Rules:
    # - infer first (deterministic), then confirm (yes/no, one question max)
    # - once confirmed, stable until explicitly changed
    # - stored canonically under project_state.expert_frame
    # ------------------------------------------------------------
    try:
        st_efl = ctx.project_store.load_project_state(current_project_full) or {}
    except Exception:
        st_efl = {}

    ef0 = _efl_normalize_frame((st_efl or {}).get("expert_frame"))

    # Explicit user command to set/change the expert frame
    cmd_label = _efl_parse_set_cmd(clean_user_msg)
    if cmd_label:
        ef_new = {
            "status": "active",
            "label": cmd_label,
            "directive": f"Optimize WIN under the '{cmd_label}' professional frame; stay grounded in canonical snippets; take action first then ask one high-leverage question.",
            "set_reason": "explicit_user_set",
            "updated_at": (ctx.project_store.now_iso() if hasattr(ctx.project_store, "now_iso") else ""),
        }
        try:
            ctx.project_store.write_project_state_fields(
                current_project_full,
                {"expert_frame": ef_new},
            )
            st_efl = ctx.project_store.load_project_state(current_project_full) or st_efl
        except Exception:
            pass
        ef0 = _efl_normalize_frame((st_efl or {}).get("expert_frame"))

    # If a frame is proposed, allow a simple yes/no to confirm or reject
    if ef0.get("status") == "proposed":
        if _is_affirmation_bootstrap(clean_user_msg):
            ef_yes = dict(ef0)
            ef_yes["status"] = "active"
            ef_yes["set_reason"] = (ef_yes.get("set_reason") or "inferred") + " + user_confirmed"
            try:
                ctx.project_store.write_project_state_fields(
                    current_project_full,
                    {"expert_frame": ef_yes},
                )
                st_efl = ctx.project_store.load_project_state(current_project_full) or st_efl
            except Exception:
                pass
            ef0 = _efl_normalize_frame((st_efl or {}).get("expert_frame"))

        elif _is_negative_bootstrap(clean_user_msg):
            # Clear and request a label (one question max will be asked downstream via C10)
            try:
                ctx.project_store.write_project_state_fields(
                    current_project_full,
                    {
                        "expert_frame": {
                            "status": "",
                            "label": "",
                            "directive": "",
                            "set_reason": "user_rejected",
                            "updated_at": (ctx.project_store.now_iso() if hasattr(ctx.project_store, "now_iso") else ""),
                        }
                    },
                )
                st_efl = ctx.project_store.load_project_state(current_project_full) or st_efl
            except Exception:
                pass
            ef0 = _efl_normalize_frame((st_efl or {}).get("expert_frame"))

    # If no active frame exists, infer a candidate and store as proposed (bounded, deterministic)
    if not ef0.get("status"):
        cand = _efl_infer_candidate(
            project_full=current_project_full,
            project_state=st_efl if isinstance(st_efl, dict) else {},
            user_msg=clean_user_msg,
        )
        ef_prop = {
            "status": "proposed",
            "label": str(cand.get("label") or "").strip(),
            "directive": str(cand.get("directive") or "").strip(),
            "set_reason": str(cand.get("set_reason") or "inferred").strip(),
            "updated_at": (ctx.project_store.now_iso() if hasattr(ctx.project_store, "now_iso") else ""),
        }
        try:
            ctx.project_store.write_project_state_fields(
                current_project_full,
                {"expert_frame": ef_prop},
            )
            st_efl = ctx.project_store.load_project_state(current_project_full) or st_efl
        except Exception:
            pass
        ef0 = _efl_normalize_frame((st_efl or {}).get("expert_frame"))

    # ------------------------------------------------------------
    # Patch 2 (FOUNDATIONAL): Memory-first intent + retrieval text
    #
    # Goal:
    # - Intent classification and canonical snippet selection MUST use the same
    #   memory-resolved text used for search ("here" -> confirmed location, etc.).
    # - Do NOT mutate clean_user_msg (preserve the user's original wording for chat,
    #   Tier-1 capture, and generation).
    # ------------------------------------------------------------
    resolved_user_text_for_intent = (clean_user_msg or "").strip()
    try:
        # server.py owns resolve_user_frame (Patch 1). model_pipeline uses it via ctx.
        if hasattr(ctx, "resolve_user_frame"):
            _u = ""
            try:
                _u = str(current_project_full or "").split("/", 1)[0].strip()
            except Exception:
                _u = ""
            if _u:
                rf = ctx.resolve_user_frame(
                    user=_u,
                    current_project_full=current_project_full,
                    user_text=(clean_user_msg or ""),
                )
                if isinstance(rf, dict):
                    rt = str(rf.get("user_text_for_search") or "").strip()
                    if rt:
                        resolved_user_text_for_intent = rt
    except Exception:
        resolved_user_text_for_intent = (clean_user_msg or "").strip()

    # Stage 1 (intent classification uses memory-resolved text)
    intent_obj = await classify_intent_c6(ctx=ctx, user_text=resolved_user_text_for_intent)
    try:
        audit["resolved_user_text_for_intent"] = str(resolved_user_text_for_intent or "")
        audit["intent_obj"] = intent_obj
    except Exception:
        pass    
    # ------------------------------------------------------------
    # CONTINUITY GUARD (FOUNDATIONAL):
    # Search is per-turn, but cached evidence MAY carry forward for
    # same-topic follow-ups (ephemeral, not memory).
    #
    # Rule:
    # - Never wipe existing evidence passed in from server.py.
    # - If intent != lookup and there is no evidence, disable search for this turn.
    # ------------------------------------------------------------
    try:
        _intent_norm = str(intent_obj.get("intent") or "").strip().lower()
    except Exception:
        _intent_norm = ""

    if _intent_norm != "lookup":
        has_evidence = bool((search_results or "").strip()) or bool(search_cached)
        if not has_evidence:
            do_search = False
    try:
        audit["intent_norm"] = str(_intent_norm or "")
        audit["do_search_after_intent_guard"] = bool(do_search)
        audit["search_results_len_after_intent_guard"] = len(search_results or "")
    except Exception:
        pass
    # ------------------------------------------------------------
    # SEARCH DECISION (FOUNDATIONAL)
    #
    # If the model intent classifier says this is a lookup, we permit web search.
    # This avoids magic-word routing while keeping the decision auditable.
    #
    # Deterministic overrides:
    # - [NOSEARCH] already handled upstream in server should_auto_web_search
    # - closed_world still blocks below
    # ------------------------------------------------------------
    try:
        if str(intent_obj.get("intent") or "").strip().lower() == "lookup":
            do_search = True
    except Exception:
        pass

    intent = str(intent_obj.get("intent") or "misc")

    # Deterministic intent correction:
    # If the user references a concrete file (pdf/image/etc), this is NOT a "recall" request.
    # Recall is reserved for "what did we decide / remind me" style queries.
    try:
        if intent == "recall":
            if re.search(r"\.(pdf|png|jpg|jpeg|webp|gif|csv|xlsx|xlsm|xls|docx|doc)\b", clean_user_msg or "", flags=re.IGNORECASE):
                intent = "misc"
    except Exception:
        pass
    
    entities = intent_obj.get("entities") or []
    scope = str(intent_obj.get("scope") or "current_project")


    # Stage 2 (C6.1): deterministic retrieval from bounded canonical allowlist per intent
    # Use memory-resolved text for scoring/selection (prevents "here"/"my girlfriend" misses).
    try:
        canonical_snippets = ctx.project_store.build_canonical_snippets(
            current_project_full,
            intent=intent,
            entities=list(entities),
            user_text=resolved_user_text_for_intent,
        ) or []
    except Exception:
        canonical_snippets = []

    # Seamless continuity: inject GLOBAL Tier-2M facts map so recall/status can use it.
    try:
        canonical_snippets = _inject_global_user_memory_into_canonical(ctx, current_project_full, canonical_snippets)
    except Exception:
        pass

    if not isinstance(canonical_snippets, list):
        canonical_snippets = []
    # ------------------------------------------------------------------
    # SEARCH EVIDENCE PROMOTION (FOUNDATIONAL, read-time only)
    #
    # Purpose:
    # - When web search ran, make evidence visible to grounded synthesis.
    # - This is NOT memory; it is ephemeral and bounded.
    # - Required so status synthesis can see actual evidence instead of
    #   claiming "no snippets were provided".
    # ------------------------------------------------------------------
    try:
        if (search_results or "").strip():
            ev_obj: Dict[str, Any] = {}
            try:
                ev_obj = json.loads(search_results)
            except Exception:
                ev_obj = {}

            if isinstance(ev_obj, dict) and str(ev_obj.get("schema") or "") == "search_evidence_v1":
                results = ev_obj.get("results")
                if isinstance(results, list) and results:
                    lines: List[str] = []
                    lines.append("SEARCH_EVIDENCE_EXCERPT (ephemeral, non-canonical):")

                    for r in results[:6]:  # bounded
                        if not isinstance(r, dict):
                            continue
                        title = str(r.get("title") or "").strip()
                        url = str(r.get("url") or "").strip()
                        snippet = str(r.get("snippet") or "").strip()

                        if not title and not snippet:
                            continue

                        if title:
                            lines.append(f"- {title}")
                        if snippet:
                            lines.append(f"  {snippet}")
                        if url:
                            lines.append(f"  Source: {url}")

                    canonical_snippets.append("\n".join(lines).strip())
    except Exception:
        pass

    # ------------------------------------------------------------
    # EFL v1 / Canonical grounding hardening:
    # Always inject a minimal PROJECT_STATE_JSON excerpt into canonical snippets
    # so goal + expert_frame are never "missing" due to retrieval allowlists.
    # Deterministic, bounded, read-time only.
    # ------------------------------------------------------------
    try:
        st_min = ctx.project_store.load_project_state(current_project_full) or {}
    except Exception:
        st_min = {}

    if isinstance(st_min, dict) and st_min:
        try:
            ef_min = st_min.get("expert_frame")
            if not isinstance(ef_min, dict):
                ef_min = {}

            proj_state_excerpt = {
                "project_mode": str(st_min.get("project_mode") or "").strip(),
                "bootstrap_status": str(st_min.get("bootstrap_status") or "").strip(),
                "goal": str(st_min.get("goal") or "").strip(),
                "domains": st_min.get("domains") if isinstance(st_min.get("domains"), list) else [],
                "expert_frame": {
                    "status": str(ef_min.get("status") or "").strip(),
                    "label": str(ef_min.get("label") or "").strip(),
                    "directive": str(ef_min.get("directive") or "").strip(),
                    "set_reason": str(ef_min.get("set_reason") or "").strip(),
                    "updated_at": str(ef_min.get("updated_at") or "").strip(),
                },
                "current_focus": str(st_min.get("current_focus") or "").strip(),
                "next_actions": st_min.get("next_actions") if isinstance(st_min.get("next_actions"), list) else [],
                "key_files": st_min.get("key_files") if isinstance(st_min.get("key_files"), list) else [],
                "last_updated": str(st_min.get("last_updated") or "").strip(),
            }

            canonical_snippets.insert(
                0,
                "PROJECT_STATE_JSON (canonical excerpt):\n"
                + json.dumps(proj_state_excerpt, ensure_ascii=False, indent=2)[:2400]
            )
        except Exception:
            pass
    # ------------------------------------------------------------
    # Facts Map injection (deterministic, read-time only)
    #
    # Problem:
    # - facts_map.md entries contain long Provenance blocks.
    # - A naive excerpt + hard truncation can crowd out the actual FACT lines.
    #
    # Fix:
    # - Inject a compact, model-readable Tier-2 view (max ~30 facts).
    # - Prioritize Slot: identity + relationship.
    # - Keep only: FACT, Slot, Subject, EntityKey, Confidence.
    # Deterministic + bounded.
    # ------------------------------------------------------------
    try:
        fm_path = ctx.project_store.state_file_path(current_project_full, "facts_map")
        if fm_path and fm_path.exists():
            fm_txt = fm_path.read_text(encoding="utf-8", errors="replace")
            fm_lines = fm_txt.splitlines()

            def _flush_entry(dst: List[Dict[str, str]], ent: Dict[str, str]) -> None:
                if not ent.get("fact"):
                    return
                dst.append(
                    {
                        "fact": ent.get("fact", "").strip(),
                        "slot": ent.get("slot", "").strip(),
                        "subject": ent.get("subject", "").strip(),
                        "entitykey": ent.get("entitykey", "").strip(),
                        "confidence": ent.get("confidence", "").strip(),
                    }
                )

            entries: List[Dict[str, str]] = []
            cur: Dict[str, str] = {}
            in_entry = False

            for ln in fm_lines:
                s = (ln or "").rstrip("\r")
                st = s.strip()

                if st.startswith("- FACT:"):
                    if in_entry:
                        _flush_entry(entries, cur)
                    cur = {"fact": st}
                    in_entry = True
                    continue

                if not in_entry:
                    continue

                if st.startswith("- Slot:"):
                    cur["slot"] = st[len("- Slot:") :].strip()
                    continue
                if st.startswith("- Subject:"):
                    cur["subject"] = st[len("- Subject:") :].strip()
                    continue
                if st.startswith("- EntityKey:"):
                    cur["entitykey"] = st[len("- EntityKey:") :].strip()
                    continue
                if st.startswith("- Confidence:"):
                    cur["confidence"] = st[len("- Confidence:") :].strip()
                    continue

                # End of entry (blank line)
                if st == "":
                    _flush_entry(entries, cur)
                    cur = {}
                    in_entry = False
                    continue

            if in_entry:
                _flush_entry(entries, cur)

            # Prioritize identity + relationship, then the rest, cap to 30.
            # Deterministic ordering within the cap so critical identity facts survive truncation.
            pri: List[Dict[str, str]] = []
            rest: List[Dict[str, str]] = []
            for e in entries:
                sl = (e.get("slot") or "").strip().lower()
                if sl in ("identity", "relationship"):
                    pri.append(e)
                else:
                    rest.append(e)

            def _tier2_pick_key(e: Dict[str, str]) -> tuple:
                ek = (e.get("entitykey") or "").strip().lower()
                sl2 = (e.get("slot") or "").strip().lower()
                sb = (e.get("subject") or "").strip().lower()

                # Hard-pin name to the very top.
                if ek == "user.identity.name":
                    return (0, 0, 0, ek)

                # Next: identity (user) then relationship (user), then other identity/relationship.
                if sl2 == "identity" and sb == "user":
                    return (1, 0, 0, ek)
                if sl2 == "relationship" and sb == "user":
                    return (1, 1, 0, ek)

                if sl2 == "identity":
                    return (2, 0, 0, ek)
                if sl2 == "relationship":
                    return (2, 1, 0, ek)

                # Everything else after.
                return (3, 9, 0, ek)

            pri.sort(key=_tier2_pick_key)
            picked = (pri + rest)[:30]
            lines_out: List[str] = []
            lines_out.append("FACTS_MAP_COMPACT (Tier-2 canonical; use for semantic recall; max_facts=30):")
            for e in picked:
                f0 = e.get("fact", "").strip()
                if f0:
                    # Prevent one verbose fact from consuming the entire 2400-char injection budget.
                    if len(f0) > 220:
                        f0 = f0[:219].rstrip() + "…"

                    lines_out.append(f0)
                    if e.get("slot"):
                        lines_out.append(f"  - Slot: {e.get('slot')}")
                    if e.get("subject"):
                        lines_out.append(f"  - Subject: {e.get('subject')}")
                    if e.get("entitykey"):
                        lines_out.append(f"  - EntityKey: {e.get('entitykey')}")
                    if e.get("confidence"):
                        lines_out.append(f"  - Confidence: {e.get('confidence')}")
                    lines_out.append("")
            excerpt = "\n".join(lines_out).strip()
            if excerpt:
                canonical_snippets.insert(1, excerpt[:2400])
    except Exception:
        pass

    # ------------------------------------------------------------
    # Assumption Binding Gate (read-time, non-persistent)
    #
    # Detect explicit user-declared assumptions such as:
    #   "assume the only difference between the three files is board price"
    #
    # These are treated as temporary inference constraints for THIS turn only.
    # ------------------------------------------------------------
    try:
        msg_l = (clean_user_msg or "").lower()

        if (
            "assume" in msg_l
            and "only difference" in msg_l
            and "board" in msg_l
            and "price" in msg_l
        ):
            canonical_snippets.append(
                "\n".join(
                    [
                        "BOUND_ASSUMPTION:",
                        "Assume the only difference between compared files is board price.",
                        "All quantities, layout, markup logic, tax logic, and non-board costs are identical.",
                    ]
                )
            )
    except Exception:
        pass

    # C6 — File-scoped OCR bridge (deterministic, read-time only)
    # AOF v1 — If no explicit file is referenced, fall back to the active object focus.
    try:
        refs = re.findall(
            r"[\w./-]+\.(?:pdf|png|jpg|jpeg|webp|gif|csv|xlsx|xlsm|xls|docx|doc)\b",
            clean_user_msg or "",
            flags=re.IGNORECASE,
        )

        # If user didn't name a file, use active object (ephemeral).
        active_rel = ""
        if not refs:
            try:
                ao = ctx.project_store.load_active_object(current_project_full) or {}
                if isinstance(ao, dict):
                    active_rel = str(ao.get("rel_path") or "").replace("\\", "/").strip()
            except Exception:
                active_rel = ""

            if active_rel:
                refs = [Path(active_rel).name]

        if refs:
            man = {}
            try:
                man = ctx.load_manifest(current_project_full) or {}
            except Exception:
                man = {}

            raw_files_all = man.get("raw_files") or []
            arts_all = man.get("artifacts") or []

            def _resolve_raw_rel_for_ref(base: str) -> str:
                """
                Resolve a user-referenced filename (or active focus basename) to a raw rel path.
                Handles timestamp-prefixed names like:
                  1767507357_Final_Drawings_4.14.2025.pdf
                Match order (best -> fallback):
                  1) exact basename match
                  2) endswith(base)
                  3) contains(base)
                Tries orig_name, saved_name, and path basename.
                """
                b = (Path(base).name or "").strip().lower()
                if not b:
                    return ""

                # If we already have an active rel and it matches this basename, use it directly.
                if active_rel:
                    try:
                        if Path(active_rel).name.lower() == b:
                            return active_rel
                    except Exception:
                        pass

                # newest-first
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

            def _sha_for_raw_rel(raw_rel: str) -> str:
                if not raw_rel:
                    return ""
                try:
                    for rf in reversed(raw_files_all if isinstance(raw_files_all, list) else []):
                        if not isinstance(rf, dict):
                            continue
                        relp = str(rf.get("path") or "").replace("\\", "/").strip()
                        if relp == raw_rel:
                            return str(rf.get("sha256") or "").strip()
                except Exception:
                    pass
                return ""

            def _mime_for_raw_rel(raw_rel: str) -> str:
                suf = Path(raw_rel).suffix.lower()
                if suf in (".png", ".jpg", ".jpeg", ".webp", ".gif"):
                    try:
                        return ctx.project_store.mime_for_image_suffix(suf)
                    except Exception:
                        return ""
                if suf == ".pdf":
                    return "application/pdf"
                if suf:
                    return "text/plain"
                return ""

            def _find_art_text(atype: str, raw_rel: str, cap_chars: int) -> str:
                for a in reversed((arts_all or [])[-1200:]):
                    if not isinstance(a, dict):
                        continue
                    if str(a.get("type") or "") != atype:
                        continue
                    ff = a.get("from_files") or []
                    if not isinstance(ff, list):
                        continue
                    ff_norm = [(str(x) or "").replace("\\", "/").strip() for x in ff]
                    if raw_rel not in ff_norm:
                        continue
                    try:
                        return (ctx.read_artifact_text(current_project_full, a, cap_chars=cap_chars) or "").strip()
                    except Exception:
                        return ""
                return ""

            # Only inject evidence for up to 2 refs (bounded + deterministic).
            for ref in refs[:2]:
                base = Path(ref).name
                raw_rel = _resolve_raw_rel_for_ref(base)
                if not raw_rel:
                    continue

                # Update ephemeral focus deterministically (explicit reference or refresh active focus use).
                try:
                    ctx.project_store.save_active_object(
                        current_project_full,
                        {
                            "rel_path": raw_rel,
                            "orig_name": base,
                            "sha256": _sha_for_raw_rel(raw_rel),
                            "mime": _mime_for_raw_rel(raw_rel),
                            "set_reason": ("explicit_reference" if (base.lower() in (clean_user_msg or "").lower()) else "active_focus_used"),
                        },
                    )
                except Exception:
                    pass

                # Determine file kind
                suf0 = ""
                try:
                    suf0 = Path(raw_rel).suffix.lower()
                except Exception:
                    suf0 = ""

                img_suffixes0 = getattr(ctx, "IMAGE_SUFFIXES", None)
                if not isinstance(img_suffixes0, (list, tuple, set)):
                    img_suffixes0 = ()
                is_image0 = bool(suf0 and (suf0 in img_suffixes0))

                # Pull artifacts (bounded)
                sem_txt = _find_art_text("image_semantics", raw_rel, 220000) if is_image0 else ""
                plan_txt = _find_art_text("plan_ocr", raw_rel, 220000)
                ocr_txt = _find_art_text("ocr_text", raw_rel, 220000)
                pdf_txt = _find_art_text("pdf_text", raw_rel, 220000)
                cap_txt = _find_art_text("image_caption", raw_rel, 20000)
                ov_txt = _find_art_text("file_overview", raw_rel, 12000)

                # IMAGE RULE:
                # - If image_semantics exists, inject it FIRST as authoritative image understanding.
                # - OCR/caption may be injected only as secondary evidence (clearly labeled), or omitted if empty.
                if is_image0 and sem_txt:
                    payload = sem_txt.strip()
                    if len(payload) > 3200:
                        payload = payload[:3199].rstrip() + "…"

                    canonical_snippets.append(
                        "\n".join(
                            [
                                f"VISION_SEMANTICS_EVIDENCE: {base}",
                                "===SEMANTICS_START===",
                                payload,
                                "===SEMANTICS_END===",
                            ]
                        )
                    )

                    secondary = (ocr_txt or cap_txt).strip()
                    if secondary:
                        if len(secondary) > 2400:
                            secondary = secondary[:2399].rstrip() + "…"
                        canonical_snippets.append(
                            "\n".join(
                                [
                                    f"OCR_SECONDARY_EVIDENCE: {base}",
                                    "===SECONDARY_START===",
                                    secondary,
                                    "===SECONDARY_END===",
                                ]
                            )
                        )
                    continue

                # Non-image (or no semantics available): keep legacy evidence priority.
                # Evidence priority: plan_ocr -> ocr_text -> pdf_text -> image_caption -> file_overview
                picked = (plan_txt or ocr_txt or pdf_txt or cap_txt or ov_txt).strip()
                if not picked:
                    continue

                canonical_snippets.append(
                    "\n".join(
                        [
                            f"FILE_OCR_EVIDENCE: {base}",
                            "===OCR_START===",
                            picked[:8000],
                            "===OCR_END===",
                        ]
                    )
                )
    except Exception:
        pass


    # C6 — Excel blueprint bridge (deterministic, read-time only)
    # If the user is clearly asking Excel-related questions (or if an Excel upload is the active object),
    # inject the latest excel_blueprint + file_overview artifacts for that workbook into canonical_snippets.
    # This does NOT write canonical memory; it is purely a read-time evidence bridge.
    try:
        low_msg = (clean_user_msg or "").lower()

        wants_excel = bool(
            re.search(r"\b(excel|spreadsheet|workbook|xlsx|xlsm|xls)\b", low_msg)
        )

        # Pull active object focus if present
        active_rel = ""
        try:
            ao = ctx.project_store.load_active_object(current_project_full) or {}
            if isinstance(ao, dict):
                active_rel = str(ao.get("rel_path") or "").replace("\\", "/").strip()
        except Exception:
            active_rel = ""

        man2 = {}
        try:
            man2 = ctx.load_manifest(current_project_full) or {}
        except Exception:
            man2 = {}

        raw_files2 = man2.get("raw_files") or []
        arts2 = man2.get("artifacts") or []

        def _latest_excel_raw_rel() -> str:
            if not isinstance(raw_files2, list):
                return ""
            for rf in reversed(raw_files2):
                if not isinstance(rf, dict):
                    continue
                relp = str(rf.get("path") or "").replace("\\", "/").strip()
                if not relp:
                    continue
                suf = Path(relp).suffix.lower()
                if suf in (".xlsx", ".xlsm", ".xls"):
                    return relp
            return ""

        def _artifact_text_for_file(atype: str, raw_rel: str, cap_chars: int) -> str:
            if not raw_rel or not isinstance(arts2, list):
                return ""
            for a in reversed((arts2 or [])[-1400:]):
                if not isinstance(a, dict):
                    continue
                if str(a.get("type") or "") != atype:
                    continue
                ff = a.get("from_files") or []
                if not isinstance(ff, list):
                    continue
                ff_norm = [(str(x) or "").replace("\\", "/").strip() for x in ff]
                if raw_rel not in ff_norm:
                    continue
                try:
                    return (ctx.read_artifact_text(current_project_full, a, cap_chars=cap_chars) or "").strip()
                except Exception:
                    return ""
            return ""

        # Decide which Excel file(s) to inject
        excel_rels: List[str] = []

        # Detect multi-file comparison intent (deterministic)
        wants_compare = any(
            kw in low_msg
            for kw in (
                "other two",
                "other 2",
                "difference",
                "compare",
                "infer",
                "across",
            )
        )

        if wants_compare:
            # Inject up to 3 most recent Excel uploads (newest last)
            if isinstance(raw_files2, list):
                for rf in reversed(raw_files2):
                    if not isinstance(rf, dict):
                        continue
                    relp = str(rf.get("path") or "").replace("\\", "/").strip()
                    if not relp:
                        continue
                    if Path(relp).suffix.lower() in (".xlsx", ".xlsm", ".xls"):
                        excel_rels.append(relp)
                    if len(excel_rels) >= 3:
                        break
            excel_rels.reverse()
        else:
            # Single-file behavior (unchanged)
            if active_rel and Path(active_rel).suffix.lower() in (".xlsx", ".xlsm", ".xls"):
                excel_rels = [active_rel]
            elif wants_excel:
                latest = _latest_excel_raw_rel()
                if latest:
                    excel_rels = [latest]

        for excel_rel in excel_rels:
            base = Path(excel_rel).name

            bp_txt = _artifact_text_for_file("excel_blueprint", excel_rel, 220000)
            ov_txt = _artifact_text_for_file("file_overview", excel_rel, 220000)

            picked_bp = (bp_txt or "").strip()
            picked_ov = (ov_txt or "").strip()

            if picked_bp:
                canonical_snippets.append(
                    "\n".join(
                        [
                            f"EXCEL_BLUEPRINT_EVIDENCE: {base}",
                            "===BLUEPRINT_START===",
                            picked_bp[:9000],
                            "===BLUEPRINT_END===",
                        ]
                    )
                )

            if picked_ov:
                canonical_snippets.append(
                    "\n".join(
                        [
                            f"EXCEL_OVERVIEW_EVIDENCE: {base}",
                            "===OVERVIEW_START===",
                            picked_ov[:6000],
                            "===OVERVIEW_END===",
                        ]
                    )
                )
    except Exception:
        pass

    # Recall is handled by the normal grounded generation path (Stage 3).
    # This makes recall responses feel conversational while still truth-bound
    # to CANONICAL_SNIPPETS + FACTS_MAP_COMPACT.
    # ------------------------------------------------------------
    # FOUNDATIONAL PREEMPTION: SEARCH RESULTS FORCE LOOKUP MODE
    #
    # Invariant:
    # - If search_results exist, NO other generation path is allowed.
    # - This prevents fallback / hybrid / bootstrap paths from
    #   emitting "no evidence" language after a successful search.
    # ------------------------------------------------------------
    if (search_results or "").strip():
        # Force authoritative lookup-with-evidence handling
        intent = "lookup"

    # C6.3 — Project mode enforcement
    #
    # open_world:
    #   - allow general knowledge + web search
    #   - canonical facts override if present (handled by project_context)
    #
    # closed_world:
    #   - do NOT answer general lookups without web evidence
    #
    # hybrid:
    #   - explanations allowed, but project facts must be from canonical snippets
    #
    if project_mode == "open_world":
        # Delegate to Project OS pipeline for normal behavior.
        return await run_model_pipeline(
            ctx=ctx,
            current_project_full=current_project_full,
            clean_user_msg=clean_user_msg,
            do_search=bool(do_search),
            search_results=search_results,
            conversation_history=conversation_history,
            max_history_pairs=max_history_pairs,
            server_file_path=server_file_path,
            allow_history_in_lookup=allow_history_in_lookup,
            search_cached=search_cached,
            active_expert=active_expert,
        )


    # Lookup behavior (authoritative)
    if intent == "lookup":
        low = (clean_user_msg or "").lower()

        if scope == "current_project" and any(k in low for k in ("war in iraq", "iraq war")):
            user_answer = "That’s outside the current project scope. If you want to talk about that, switch projects or start a new one."
            return {"user_answer": user_answer, "lookup_mode": False, "conversation_history": conversation_history}

        if project_mode == "closed_world" and not bool(do_search):
            user_answer = "This is a closed-world project. Use [SEARCH] for world facts, or switch the project to open_world/hybrid."
            return {"user_answer": user_answer, "lookup_mode": False, "conversation_history": conversation_history}

        # FOUNDATIONAL RULE (corrected):
        # If SEARCH RESULTS EXIST, we MUST treat this as lookup-with-evidence,
        # regardless of do_search or intent classification.
        if (search_results or "").strip():
            # Lookup prompt is owned by server.py (ctx is the server module).
            # Use ctx.LENS0_LOOKUP_SYSTEM_PROMPT if present; fallback to a minimal safe prompt.
            try:
                lookup_sys = str(getattr(ctx, "LENS0_LOOKUP_SYSTEM_PROMPT", "") or "").strip()
            except Exception:
                lookup_sys = ""

            if not lookup_sys:
                lookup_sys = "\n".join([
                    "You are answering in LOOKUP MODE.",
                    "Use SEARCH_EVIDENCE_JSON as evidence (not authority).",
                    "Do not claim you lack access to articles/transcripts/pages if SEARCH_EVIDENCE_JSON is present.",
                    "If evidence is insufficient, say so plainly without guessing.",
                    "If you include sources, include 1–3 URLs that appear in SEARCH_EVIDENCE_JSON.",
                ]).strip()

            # Expectation-shift / recency framing guard (LOOKUP MODE too):
            # If the user asks "what changed / last X months / now required", enforce explicit date anchoring
            # when supported by WEB_SEARCH_RESULTS; otherwise say dates are not supported (do not guess).
            expectation_shift = False
            try:
                lowq = (clean_user_msg or "").strip().lower()
                expectation_shift = (
                    ("what changed" in lowq)
                    or ("what has changed" in lowq)
                    or ("what’s changed" in lowq)
                    or ("whats changed" in lowq)
                    or ("changed in the last" in lowq)
                    or ("in the last" in lowq and any(u in lowq for u in ("days", "weeks", "months", "years")))
                    or ("last 12 months" in lowq)
                    or ("past 12 months" in lowq)
                    or ("over the last" in lowq and any(u in lowq for u in ("days", "weeks", "months", "years")))
                    or ("since " in lowq)
                    or ("now required" in lowq)
                    or ("being enforced" in lowq)
                    or ("enforced" in lowq)
                    or ("new guidance" in lowq)
                    or ("updated guidance" in lowq)
                )
            except Exception:
                expectation_shift = False

            messages = [
                {"role": "system", "content": lookup_sys},
            ]
            # CKCL lock applies in lookup too (prevents 'can't verify / no evidence' stalls on consensus asks)
            try:
                if _ckcl_committed_and_crowd(conversation_history or [], clean_user_msg or ""):
                    messages.append({"role": "system", "content": _ckcl_system_lock_note()})
            except Exception:
                pass
            # Inject user global memory context into lookup mode (Tier-2G/Tier-2M only).
            try:
                mem_ctx = _build_lookup_memory_context(ctx, current_project_full, clean_user_msg)
            except Exception:
                mem_ctx = ""
            if mem_ctx:
                messages.append(
                    {
                        "role": "system",
                        "content": (
                            "USER_GLOBAL_MEMORY_CONTEXT (read-only; personalization + deictic grounding only):\n"
                            "- Use when relevant; do NOT invent new facts.\n"
                            "- If a needed detail is missing or ambiguous, ask ONE clarifying question only if necessary.\n"
                            "\n"
                            + mem_ctx
                        ).strip(),
                    }
                )
            authority_level = ""
            try:
                ev = _safe_json_extract(search_results or "")
                if isinstance(ev, dict):
                    auth = ev.get("authority")
                    if isinstance(auth, dict):
                        authority_level = str(auth.get("level") or "").lower()
            except Exception:
                authority_level = ""

            if expectation_shift and authority_level != "primary_confirmed":
                messages.append(
                    {
                        "role": "system",
                        "content": (
                            "EXPECTATION_SHIFT_MODE (LOOKUP):\n"
                            "- The user is asking what changed over a recent time window.\n"
                            "- You MUST:\n"
                            "  1) Use explicit calendar anchors (month/day/year) ONLY when supported by SEARCH_EVIDENCE_JSON.\n"
                            "  2) If SEARCH_EVIDENCE_JSON does not contain an explicit date for a claim, say so plainly (do NOT guess).\n"
                            "  3) Separate: (a) newly codified/standardized vs (b) newly enforced/expected in practice.\n"
                            "  4) Use before/after framing: what was already true vs what shifted recently.\n"
                            "  5) Do NOT use vague hedges like 'last ~12 months' or 'recently' as substitutes for dates.\n"
                            "     If you cannot date-stamp, say: 'The snippets don’t clearly show dates for this.'\n"
                        ),
                    }
                )

            # Continuity in lookup mode: include recent conversation history (if available).
            if allow_history_in_lookup and conversation_history:
                try:
                    hist = [m for m in (conversation_history or []) if m.get("role") in ("user", "assistant")]
                    if len(hist) > (max_history_pairs * 2):
                        hist = hist[-(max_history_pairs * 2):]
                    if hist:
                        messages.append(
                            {
                                "role": "system",
                                "content": (
                                    "CONVERSATION_HISTORY_NOTE:\n"
                                    "- Prior turns are provided for continuity only.\n"
                                    "- Treat prior assistant statements as non-authoritative unless supported by SEARCH_EVIDENCE_JSON "
                                    "or CANONICAL_SNIPPETS.\n"
                                ),
                            }
                        )
                        messages.extend(hist)
                except Exception:
                    pass
            elif allow_history_in_lookup:
                # Fallback to recent chat log tail if in-memory history is empty (e.g., reconnect).
                try:
                    if hasattr(ctx, "read_recent_chat_log_snippet"):
                        tail = ctx.read_recent_chat_log_snippet(
                            current_project_full,
                            n_messages=8,
                            cap_chars=4000,
                        ) or ""
                        tail = (tail or "").strip()
                        if tail:
                            messages.append(
                                {
                                    "role": "system",
                                    "content": (
                                        "RECENT_CHAT_TAIL (continuity only; NOT authoritative truth):\n\n"
                                        + tail
                                    ),
                                }
                            )
                except Exception:
                    pass

            messages.extend(
                [
                    {
                        "role": "system",
                        "content": (
                            "SEARCH_EPISTEMIC_RULES:\n"
                            "- Treat SEARCH_EVIDENCE_JSON as noisy external evidence (claims), not authority.\n"
                            "- Search increases coverage/recency; it must NOT increase certainty by itself.\n"
                            "- Exception (server-annotated authority):\n"
                            "  - If SEARCH_EVIDENCE_JSON.authority.level == \"primary_confirmed\", treat the core claim as CONFIRMED by primary sources.\n"
                            "  - Be decisive on what is confirmed; separate CONFIRMED vs REPORTED vs UNKNOWN.\n"
                            "  - Do NOT ask clarification or refinement questions unless the user explicitly requests narrowing.\n"
                            "- If results converge, you may be decisive; if they conflict, surface the disagreement.\n"
                            "- If SEARCH_EVIDENCE_JSON contains \"insufficient\": true AND authority.level != \"primary_confirmed\", treat that as NO_SUPPORT_FOUND.\n"
                            "  - Do NOT claim confirmation.\n"
                            "  - Ask at most ONE refinement question.\n"
                            "- If evidence is insufficient (and not primary_confirmed), say 'not enough evidence yet' and state what would change your mind.\n"
                            "- Do not use citation theater; only include 1–3 URLs when it materially helps.\n"

                        ),
                    },

                    # Inject search evidence (structured JSON) BEFORE any commitment guard.
                    {"role": "system", "content": "SEARCH_EVIDENCE_JSON:\n\n" + (search_results or "")},
                    # FOUNDATIONAL: When the user asks for latest/current/status, force epistemic synthesis.
                    {"role": "system", "content": (_status_synthesis_system_note() if _requires_status_synthesis(clean_user_msg) else "")},
                ]
            )
            # THREAD_SYNTHESIS (ephemeral, evidence-bound; non-authoritative)
            if (thread_synthesis_text or "").strip():
                messages.append(
                    {
                        "role": "system",
                        "content": (
                            "THREAD_SYNTHESIS (ephemeral; evidence-bound; non-authoritative):\n"
                            + (thread_synthesis_text or "").strip()
                        ).strip(),
                    }
                )
            messages.extend(
                [
                    # CCG: prevent scope resets once context is committed (empty unless applicable)
                    {"role": "system", "content": _ccg_system_note(conversation_history or [], clean_user_msg or "")},
                    # Force "consensus-first opening" so the model doesn't stall on missing telemetry/evidence.
                    {"role": "system", "content": _ccg_consensus_opening_note(conversation_history or [], clean_user_msg or "")},
                    {"role": "user", "content": clean_user_msg},
                ]
            )

            raw = await asyncio.to_thread(ctx.call_openai_chat, messages)
            user_answer = (raw or "").strip()
            try:
                if _ckcl_committed_and_crowd(conversation_history or [], clean_user_msg or ""):
                    user_answer = _ckcl_strip_refusal_preamble(user_answer)
            except Exception:
                pass

            # CKSG (lookup): prevent consensus stalls even in lookup mode.
            try:
                _committed_lk = bool(_ccg_system_note(conversation_history or [], clean_user_msg or "").strip())
                _cksg_lk = _cksg_stall_reason(committed=_committed_lk, user_msg=clean_user_msg, output_text=user_answer)
            except Exception:
                _cksg_lk = ""

            if _cksg_lk:
                try:
                    messages_retry = list(messages)
                    messages_retry.insert(
                        max(1, len(messages_retry) - 1),
                        {
                            "role": "system",
                            "content": (
                                "CKSG_ENFORCEMENT (LOOKUP):\n"
                                "- Provide the working consensus answer first (provisional is fine).\n"
                                "- Do NOT lead with 'can't verify / can't responsibly claim' language.\n"
                                "- Missing patch notes/spreadsheets are refinement, not blockers.\n"
                                "- Then summarize disagreement bands and ask ONE refinement question.\n"
                            ),
                        },
                    )
                    raw_b = await asyncio.to_thread(ctx.call_openai_chat, messages_retry)
                    user_answer_b = (raw_b or "").strip()
                    if user_answer_b:
                        user_answer = user_answer_b
                except Exception:
                    pass
            ctx.snapshot_assistant_output(current_project_full, user_answer)
            return {
                "user_answer": user_answer,
                "lookup_mode": True,
                "conversation_history": conversation_history,
            }

        # Fallback: no search results available
        #
        # IMPORTANT UX / safety rule:
        # Lookup questions without web evidence must NOT route into Project OS output protocol
        # (which can emit Assumptions/Decision logs). Answer plainly, no tags, no state writes.
        #
        # HARD INVARIANT:
        # If search_results is non-empty, we MUST NOT enter this branch (prevents "no evidence" poisoning).
        if (search_results or "").strip():
            # Defensive: treat as lookup-with-evidence (caller should have taken the main branch).
            # Do not inject NO_WEB_EVIDENCE here.
            pass
        else:
            try:
                lookup_sys2 = str(getattr(ctx, "LENS0_LOOKUP_SYSTEM_PROMPT", "") or "").strip()
            except Exception:
                lookup_sys2 = ""

            if not lookup_sys2:
                lookup_sys2 = "\n".join([
                    "You are answering a general lookup question.",
                    "Return plain text only (no tags, no logs).",
                    "If you are unsure, say so plainly and ask ONE short clarifying question only if needed.",
                    "Do not invent citations or dates.",
                ]).strip()

            messages = [
                {"role": "system", "content": lookup_sys2},
                # NOTE: Do NOT inject a 'NO_WEB_EVIDENCE' system message.
                # It causes the model to deny evidence even when evidence exists elsewhere in context.
                {"role": "system", "content": "NO_WEB_EVIDENCE: (none)"},
                {"role": "user", "content": clean_user_msg},
            ]

            # Inject user global memory context into lookup mode (Tier-2G/Tier-2M only).
            try:
                mem_ctx = _build_lookup_memory_context(ctx, current_project_full, clean_user_msg)
            except Exception:
                mem_ctx = ""
            if mem_ctx:
                messages.insert(
                    2,
                    {
                        "role": "system",
                        "content": (
                            "USER_GLOBAL_MEMORY_CONTEXT (read-only; personalization + deictic grounding only):\n"
                            "- Use when relevant; do NOT invent new facts.\n"
                            "- If a needed detail is missing or ambiguous, ask ONE clarifying question only if necessary.\n"
                            "\n"
                            + mem_ctx
                        ).strip(),
                    },
                )

            raw = await asyncio.to_thread(ctx.call_openai_chat, messages)
            user_answer = (raw or "").strip()

            ctx.snapshot_assistant_output(current_project_full, user_answer)
            return {
                "user_answer": user_answer,
                "lookup_mode": True,
                "conversation_history": conversation_history,
            }

    # ------------------------------------------------------------
    # Deterministic deliverable routing (MVP)
    #
    # In hybrid/closed_world projects, Stage 3 grounded mode can
    # incorrectly trap Excel/HTML deliverable requests.
    #
    # Fix: if user is clearly requesting an Excel deliverable or an
    # HTML deliverable, delegate to the existing run_model_pipeline
    # (which already contains the deliverable/excel intent logic).
    #
    # This is additive and does NOT change:
    # - assumption binding
    # - multi-excel evidence
    # - cached totals behavior
    # ------------------------------------------------------------
    try:
        low_msg3 = (clean_user_msg or "").lower()

        # Excel deliverables MUST be explicitly requested (action + excel noun).
        # Do NOT trigger just because the text contains "excel".
        excel_intent3 = any(
            phrase in low_msg3
            for phrase in (
                "make the new sheet",
                "make a new sheet",
                "build the workbook",
                "create the workbook",
                "generate the workbook",
                "just make the sheet",
                "just make the workbook",
                "make the sheet",
                "create the sheet",
                "generate the sheet",
                "export to excel",
                "export to xlsx",
                "create an excel",
                "generate an excel",
                "make an excel",
                "build an excel",
            )
        )

        deliverable_intent3 = (
            ("deliverable" in low_msg3)
            or ("print-ready" in low_msg3)
            or ("print ready" in low_msg3)
            or (
                ("html" in low_msg3)
                and any(x in low_msg3 for x in ("make", "create", "generate", "build", "write", "draft", "format", "export"))
            )
        )

        is_couples_account = False
        try:
            is_couples_account = str(current_project_full or "").split("/", 1)[0].lower().startswith("couple_")
        except Exception:
            is_couples_account = False

        if (excel_intent3 or deliverable_intent3) and (not is_couples_account):
            return await run_model_pipeline(
                ctx=ctx,
                current_project_full=current_project_full,
                clean_user_msg=clean_user_msg,
                do_search=bool(do_search),
                search_results=search_results,
                conversation_history=conversation_history,
                max_history_pairs=max_history_pairs,
                server_file_path=server_file_path,
                allow_history_in_lookup=allow_history_in_lookup,
                search_cached=search_cached,
            )
    except Exception:
        pass
    # AOF v2 — In closed_world/hybrid Stage 3 grounded mode, the model does NOT see full project_context.
    # So we must inject cached image_semantics into canonical_snippets (read-time only) when relevant.
    # Additionally, for image-referential turns, prefer a deterministic human-facing description
    # from image_semantics_v1 over the generic grounded fallback ("Not recorded / ambiguous.").
    try:
        sem_txt = await _maybe_ensure_active_image_semantics(
            ctx=ctx,
            project_full=current_project_full,
            user_msg=clean_user_msg,
            reason="model_pipeline:run_request_pipeline",
        )
        if sem_txt:
            base = ""
            try:
                ao2 = ctx.project_store.load_active_object(current_project_full) or {}
                if isinstance(ao2, dict):
                    base = Path(str(ao2.get("rel_path") or "")).name
            except Exception:
                base = ""

            payload = (sem_txt or "").strip()
            if len(payload) > 3200:
                payload = payload[:3199].rstrip() + "…"

            canonical_snippets.append(
                "\n".join(
                    [
                        f"IMAGE_SEMANTICS_EVIDENCE: {base or '(active image)'}",
                        "===SEMANTICS_START===",
                        payload,
                        "===SEMANTICS_END===",
                    ]
                )
            )

            # Deterministic chat answer for image-referential turns:
            # If the user is asking "what is this / describe this image", use outputs.summary (+ a little structure)
            # so the conversation continues naturally.
            if _looks_like_image_referential_turn(clean_user_msg):
                try:
                    sem_obj = json.loads((sem_txt or "").strip())
                except Exception:
                    sem_obj = {}

                out = sem_obj.get("outputs") if isinstance(sem_obj, dict) else None
                if not isinstance(out, dict):
                    out = {}

                summary = str(out.get("summary") or "").strip()
                observations = out.get("observations")
                if not isinstance(observations, list):
                    observations = []
                observations = [str(x).strip() for x in observations if str(x).strip()][:6]

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

                if summary:
                    parts: List[str] = []
                    parts.append(summary)

                    # Add a short "Likely ID" line when we have a plausible candidate.
                    if top_make or top_model:
                        if isinstance(top_conf, float):
                            parts.append(f"\nLikely ID: {top_make} {top_model}".strip() + f" (confidence {top_conf:.2f})")
                        else:
                            parts.append(f"\nLikely ID: {top_make} {top_model}".strip())

                    if observations:
                        parts.append("\nVisible cues:")
                        for ob in observations[:5]:
                            parts.append(f"- {ob}")

                    user_answer_img = "\n".join(parts).strip()

                    # Keep history bounded (mirror Stage 3 behavior)
                    conversation_history.append({"role": "user", "content": clean_user_msg})
                    conversation_history.append({"role": "assistant", "content": user_answer_img})
                    if len(conversation_history) > (max_history_pairs * 2):
                        conversation_history = conversation_history[-(max_history_pairs * 2):]

                    ctx.snapshot_assistant_output(current_project_full, user_answer_img)
                    return {
                        "user_answer": user_answer_img,
                        "lookup_mode": False,
                        "conversation_history": conversation_history,
                    }
    except Exception:
        pass
    # ------------------------------------------------------------
    # C10 — Foundational Continuity v0 + WIN drive (read-time only)
    #
    # If AOF exists and user didn't contradict it, treat it as the
    # default referent and push the conversation forward.
    # ------------------------------------------------------------
    c10_focus_ok, c10_ao = _c10_focus_in_scope(ctx, current_project_full, clean_user_msg)

    c10_focus_rel = ""
    c10_focus_name = ""
    c10_focus_mime = ""
    c10_evidence = {"has_any": False}

    if c10_focus_ok and isinstance(c10_ao, dict):
        c10_focus_rel = str(c10_ao.get("rel_path") or "").replace("\\", "/").strip()
        c10_focus_name = Path(c10_focus_rel).name if c10_focus_rel else ""
        c10_focus_mime = str(c10_ao.get("mime") or "").strip()

        try:
            man_c10 = ctx.load_manifest(current_project_full) or {}
        except Exception:
            man_c10 = {}
        if isinstance(man_c10, dict) and c10_focus_rel:
            c10_evidence = _c10_evidence_presence_from_manifest(man_c10, c10_focus_rel)

    # One WIN question only (deterministic)
    c10_win_q = ""
    try:
        c10_win_q = _c10_pick_one_win_question(ctx, current_project_full, focus_name=(c10_focus_name if c10_focus_ok else ""))
    except Exception:
        c10_win_q = ""

    # EFL v1 — If a frame is still proposed, use the single question slot to confirm it.
    # This MUST run AFTER WIN question selection so it cannot be overwritten.
    try:
        ef_tmp = _efl_normalize_frame((st or {}).get("expert_frame") if isinstance(st, dict) else {})
        if ef_tmp.get("status") == "proposed" and ef_tmp.get("label"):
            # Suppress mid-stream frame confirmation once real work has begun.
            # "Work begun" signals (deterministic):
            # - any confirmed decisions exist, OR
            # - any deliverables exist, OR
            # - working_doc.md has non-trivial content
            _efl_work_started = False

            try:
                _efl_work_started = bool(ctx.project_store.load_decisions(current_project_full) or [])
            except Exception:
                _efl_work_started = False

            if not _efl_work_started:
                try:
                    _reg = ctx.project_store.load_deliverables(current_project_full) or {}
                    _items = _reg.get("items") if isinstance(_reg, dict) else None
                    if isinstance(_items, list) and _items:
                        _efl_work_started = True
                except Exception:
                    pass

            if not _efl_work_started:
                try:
                    _wd_path = ctx.project_store.state_file_path(current_project_full, "working_doc")
                    if _wd_path.exists():
                        _wd_txt = _wd_path.read_text(encoding="utf-8", errors="replace").strip()
                        if _wd_txt and ("(none yet)" not in _wd_txt.lower()):
                            _efl_work_started = True
                except Exception:
                    pass

            if not _efl_work_started:
                c10_win_q = f"Confirm expert frame '{ef_tmp.get('label')}'? (yes/no)"
    except Exception:
        pass
    # EFL v1 — Once the frame is ACTIVE and a goal exists, prefer shipping the default
    # instead of asking a multi-option “pick one” menu question.
    # Keep it binary and high-leverage.
    try:
        st_goal = str((st or {}).get("goal") or "").strip() if isinstance(st, dict) else ""
        if (ef_tmp.get("status") == "active") and st_goal and (not c10_focus_ok):
            c10_win_q = "Want me to draft the baseline end-to-end spec into working_doc.md now? (yes/no)"
    except Exception:
        pass

    # Respect "no questions" rule when set
    c10_allow_questions = True
    try:
        st_rules2 = ctx.project_store.load_project_state(current_project_full).get("user_rules", [])
        if isinstance(st_rules2, list) and ("no questions" in st_rules2):
            c10_allow_questions = False
    except Exception:
        c10_allow_questions = True

    c10_sys_lines: List[str] = []
    c10_sys_lines.append("C10_CONTINUITY_AND_WIN:")
    if c10_focus_ok and c10_focus_name:
        c10_sys_lines.append(f"- Active focus: {c10_focus_name} ({c10_focus_mime or 'unknown'})")
        c10_sys_lines.append("- Treat the active focus as the default referent unless the user explicitly contradicts it.")
        if isinstance(c10_evidence, dict) and c10_evidence.get("has_any"):
            c10_sys_lines.append("- Evidence exists for the active focus (use it to take a best-effort action now).")
        else:
            c10_sys_lines.append("- Evidence may be limited; still move forward with best-effort and ask one clarifier if blocked.")
    else:
        c10_sys_lines.append("- No active focus is in scope this turn.")

    c10_sys_lines.append("")
    c10_sys_lines.append("WIN_DIRECTIVE:")
    c10_sys_lines.append("- Do the best useful action NOW using available evidence/context.")
    c10_sys_lines.append("- Only ask a question if you are truly blocked and cannot proceed without user input.")
    c10_sys_lines.append("- If blocked, ask EXACTLY ONE question (no multi-part questions).")
    # Couples Mode Option A: suppress project-manager / decision / deliverable framing.
    if couples_mode:
        c10_sys_lines.append("- Couples therapy mode: do NOT propose decisions, confirmations, 'treat as final', or workflow summaries.")
        c10_sys_lines.append("- End like a therapist: one small next step + one gentle question.")
    if c10_allow_questions and c10_win_q:
        c10_sys_lines.append(f"- If you must ask, use this exact question: {c10_win_q}")
    else:
        c10_sys_lines.append("- Prefer zero questions when you can make forward progress.")

    c10_continuity_system_note = "\n".join(c10_sys_lines).strip()

    # Stage 3 generation (plain text, no tags)
    snippet_blob = "\n\n".join([str(s) for s in canonical_snippets if str(s).strip()]) or "Not recorded / ambiguous."

    # Prompt selection:
    # - strict grounded for explicit memory/status queries
    # - default expert voice for active_expert=default
    # - conversational expert mode otherwise
    ae = (active_expert or "").strip().lower()
    if intent in ("recall", "status"):
        sys_prompt = _GROUNDED_RESPONSE_SYSTEM
        if project_mode == "hybrid":
            sys_prompt = _HYBRID_GROUNDED_RESPONSE_SYSTEM
    else:
        if ae in ("", "default"):
            sys_prompt = _DEFAULT_EXPERT_SYSTEM
        else:
            sys_prompt = _CONVERSATIONAL_EXPERT_SYSTEM
    # ------------------------------------------------------------
    # Default-mode conversational on-ramp (tiny friendliness + shorter starts)
    #
    # Goal:
    # - Keep the system ultra-capable, but avoid "framework dump" on the first beat.
    # - Start like a person talking to a person: 2–4 sentences, then one question.
    # - Only shift into scaffolds/lists when the user explicitly asks for structure.
    # ------------------------------------------------------------
    default_onramp_note = ""

    try:
        # Only in non-couples projects and non-status/recall turns.
        # (Couples mode has its own tone rules; status/recall are deterministic.)
        if (not couples_mode) and (intent not in ("status", "recall")):
            low_msg = (clean_user_msg or "").strip().lower()

            # User explicitly asked for structure/framework.
            wants_structure = any(k in low_msg for k in (
                "help me structure",
                "help me organize",
                "break this down",
                "give me a framework",
                "give me a structure",
                "outline",
                "formalize",
                "turn this into",
                "make a plan",
                "step by step",
                "bullet points",
                "scaffold",
            ))

            # "Early" and "light" turn heuristic: keep it conversational for vague/openers.
            early_turn = (len(conversation_history or []) <= 2)
            shortish = (len((clean_user_msg or "").strip()) <= 260)

            if early_turn and shortish and (not wants_structure):
                default_onramp_note = "\n".join([
                    "DEFAULT_ONRAMP:",
                    "- Start like a human responding in real time (1–3 natural sentences).",
                    "- Do NOT open with a framework, checklist, rubric, or numbered list unless the user explicitly asked for structure.",
                    "- Let structure emerge only if it helps clarify the response.",
                    "- Keep it calm and capable; no cheerleading, no therapy language.",
                    "- Only ask a question if it materially improves clarity or helps move the task forward.",
                ])
    except Exception:
        default_onramp_note = ""
    # EFL v1 prompt injection (behavioral constraint, not persona roleplay)
    ef_injected = _efl_normalize_frame((st_efl or {}).get("expert_frame") if isinstance(st_efl, dict) else {})
    ef_lines: List[str] = []
    ef_lines.append("EXPERT_FRAME_LOCK (project-scoped):")
    if ef_injected.get("label"):
        ef_lines.append(f"- Frame: {ef_injected.get('label')}")
    if ef_injected.get("directive"):
        ef_lines.append(f"- Directive: {ef_injected.get('directive')}")
    ef_lines.append("- Apply this frame to prioritization, tone, and what 'WIN' means.")
    ef_lines.append("- Prefer strong defaults over multiple-choice menus; only ask a binary question when needed.")
    ef_lines.append("- Do NOT invent project facts; treat CANONICAL_SNIPPETS as the only truth for project state.")
    ef_system_note = "\n".join(ef_lines).strip()

    # ------------------------------------------------------------
    # Continuation helper (deterministic, read-time only)
    # If the user says "continue / tell me more / go on / keep going",
    # inject the latest assistant_output as continuity context so the
    # model can continue naturally without inventing project facts.
    # ------------------------------------------------------------
    c10_last_answer_note = ""
    try:
        _m = re.sub(r"\s+", " ", (clean_user_msg or "").strip().lower())
        if _m in ("continue", "tell me more", "go on", "keep going"):
            latest = None
            try:
                latest = ctx.project_store.get_latest_artifact_by_type(current_project_full, "assistant_output")
            except Exception:
                latest = None

            if isinstance(latest, dict):
                txt = ""
                try:
                    txt = (ctx.project_store.read_artifact_text(current_project_full, latest, cap_chars=12000) or "").strip()
                except Exception:
                    txt = ""
                if txt:
                    if len(txt) > 4200:
                        txt = txt[:4199].rstrip() + "…"
                    c10_last_answer_note = (
                        "LAST_ASSISTANT_OUTPUT (continuity only; NOT authoritative truth):\n\n"
                        + txt
                    )
    except Exception:
        c10_last_answer_note = ""

    # couples_mode already computed earlier (do not recompute here)
    couples_mode = bool(couples_mode)
    # ------------------------------------------------------------
    # Couples Mode Option A — Bring-up draft + confirm (deterministic)
    #
    # UX:
    # - If user asks "bring this up / ask partner / mediate this", generate a short synopsis and ask YES/NO.
    # - YES => append bringup_request (append-only) and clear draft.
    # - NO  => clear draft and ask for a one-sentence theme.
    #
    # Storage:
    # - pending_bringup_draft stored in project_state.json so disconnects don’t lose it.
    # ------------------------------------------------------------
    nl_bringup_draft_response = ""

    try:
        if couples_mode:
            # Load pending draft (if any)
            try:
                st_b = ctx.project_store.load_project_state(current_project_full) or {}
            except Exception:
                st_b = {}

            draft = st_b.get("pending_bringup_draft") if isinstance(st_b, dict) else None
            if not isinstance(draft, dict):
                draft = {}

            has_draft = bool(draft.get("pending") is True)

            # Resolve me + partner deterministically
            me = ""
            try:
                me = ctx.safe_user_name(str(current_project_full or "").split("/", 1)[0].strip())
            except Exception:
                me = ""

            partner = ""
            try:
                links = ctx.project_store.load_couples_links() or {}
            except Exception:
                links = {}

            rec = None
            for _cid, obj in (links or {}).items():
                if not isinstance(obj, dict):
                    continue
                if str(obj.get("status") or "") != "active":
                    continue
                ua = ctx.safe_user_name(str(obj.get("user_a") or ""))
                ub = ctx.safe_user_name(str(obj.get("user_b") or ""))
                if me and (me == ua or me == ub):
                    rec = obj
                    break

            if isinstance(rec, dict) and me:
                ua = ctx.safe_user_name(str(rec.get("user_a") or ""))
                ub = ctx.safe_user_name(str(rec.get("user_b") or ""))
                partner = (ub if me == ua else ua)

            # If we have a draft, interpret YES/NO deterministically
            if has_draft and partner:
                if _is_affirmation_bootstrap(clean_user_msg):
                    # Commit bringup
                    try:
                        _syn = str(draft.get("synopsis") or "").strip()
                        _syn = _neutralize_partner_pronouns(_syn)

                        _ = ctx.project_store.append_bringup_request(
                            current_project_full,
                            from_user=me,
                            to_user=partner,
                            topic=str(draft.get("topic") or "").strip(),
                            tone=str(draft.get("tone") or "gentle").strip() or "gentle",
                            boundaries=str(draft.get("boundaries") or "no blame; no quotes; no attribution").strip(),
                            urgency=str(draft.get("urgency") or "").strip(),
                            context_summary=_syn,
                        )
                    except Exception:
                        pass

                    # Clear draft
                    try:
                        ctx.project_store.write_project_state_fields(current_project_full, {"pending_bringup_draft": {}})
                    except Exception:
                        pass

                    nl_bringup_draft_response = (
                        "Got it. I’ll hold that as a theme and surface it neutrally later—"
                        "without quoting you or making it about “who said what.”"
                    )

                elif _is_negative_bootstrap(clean_user_msg):
                    # Discard draft
                    try:
                        ctx.project_store.write_project_state_fields(current_project_full, {"pending_bringup_draft": {}})
                    except Exception:
                        pass

                    nl_bringup_draft_response = "No problem. What’s the one-sentence theme you want me to surface later?"

            # If no draft and user is requesting mediation, create draft + ask confirm
            if (not nl_bringup_draft_response) and (not has_draft) and partner and _looks_like_bringup_nl_request(clean_user_msg):
                topic = _extract_bringup_topic_nl(clean_user_msg, cap=180)
                synopsis = _build_bringup_synopsis_for_confirmation(conversation_history, cap=420)

                draft_obj = {
                    "pending": True,
                    "from_user": me,
                    "to_user": partner,
                    "topic": topic,
                    "synopsis": synopsis,
                    "tone": "gentle",
                    "boundaries": "no blame; no quotes; no attribution",
                    "urgency": "",
                    "created_at": (ctx.project_store.now_iso() if hasattr(ctx.project_store, "now_iso") else ""),
                }
                # Ensure single-line strings
                for k, v in list(draft_obj.items()):
                    if isinstance(v, str):
                        draft_obj[k] = v.replace("\r", " ").replace("\n", " ").strip()

                try:
                    ctx.project_store.write_project_state_fields(current_project_full, {"pending_bringup_draft": draft_obj})
                except Exception:
                    pass

                lines = []
                lines.append("Here’s what I’d plan to surface later (neutral, non-attributed):")
                if synopsis:
                    lines.append(synopsis)
                elif topic:
                    lines.append(f"Theme: {topic}")
                else:
                    lines.append("Theme: (unspecified)")
                lines.append("")
                lines.append("Is this what you want me to bring up with your partner next time? (yes/no)")
                nl_bringup_draft_response = "\n".join(lines).strip()

    except Exception:
        nl_bringup_draft_response = ""


    therapist_frame = ""
    try:
        if couples_mode:
            therapist_frame = _COUPLES_THERAPIST_SYSTEM
    except Exception:
        therapist_frame = ""

    bringups_note = ""
    try:
        # Inject bringups ONLY on first message of a session (inspectable + non-repetitive).
        session_start = not bool(conversation_history)
        if couples_mode and session_start:
            bringups_note = ctx.project_store.render_pending_bringups_for_session(
                current_project_full,
                session_start=True,
                max_items=5,
            )
    except Exception:
        bringups_note = ""
    # ------------------------------------------------------------
    # Couples Mode Option A — Intake Mode (session start, empty memory)
    #
    # If this couple_* project is fresh (no Tier-2 facts yet), force a clinician-like
    # intake opening: explain the approach briefly, then ask EXACTLY 3 essentials.
    # Continue intake across turns until basics are filled.
    # ------------------------------------------------------------
    couples_intake_note = ""
    try:
        if couples_mode and session_start:
            has_facts = _couples_facts_map_has_any_entries(ctx, current_project_full)
            if not has_facts:
                couples_intake_note = (
                    "COUPLES_THERAPY_INTAKE_MODE:\n"
                    "- This is a fresh couples therapy space (no intake facts recorded yet).\n"
                    "- Start by briefly explaining how you work:\n"
                    "  * Disciplines: Gottman-style skills (soft start-ups, repair attempts, conflict management)\n"
                    "    + EFT-style cycle framing (what each person is protecting/needs underneath).\n"
                    "  * Toolbelt as needed: MI, NVC clean requests, DBT interpersonal effectiveness.\n"
                    "- Privacy: never quote/attribute private content from the partner; surface themes neutrally.\n"
                    "- Then ask EXACTLY THREE intake questions for the CURRENT user only:\n"
                    "  1) Full name + what they want to be called\n"
                    "  2) Pronouns\n"
                    "  3) Age or birthdate (their choice)\n"
                    "- Do NOT ask more than these three questions in one turn.\n"
                    "- After the user answers, proceed normally, and continue collecting remaining basics over subsequent turns.\n"
                    "- End like a therapist (one small next step + one gentle question)."
                )
    except Exception:
        couples_intake_note = ""

    # Guard: some revisions used nl_bringup_queued_note in messages2.
    # Ensure it always exists to prevent NameError.
    nl_bringup_queued_note = ""
    # Couples bring-up draft flow: if we generated a deterministic confirmation prompt
    # (or a deterministic queued/discarded response), return it immediately.
    if (nl_bringup_draft_response or "").strip():
        user_answer = nl_bringup_draft_response.strip()

        conversation_history.append({"role": "user", "content": clean_user_msg})
        conversation_history.append({"role": "assistant", "content": user_answer})
        if len(conversation_history) > (max_history_pairs * 2):
            conversation_history = conversation_history[-(max_history_pairs * 2):]

        ctx.snapshot_assistant_output(current_project_full, user_answer)
        return {
            "user_answer": user_answer,
            "lookup_mode": False,
            "conversation_history": conversation_history,
        }
    # ------------------------------------------------------------
    # Patch 2X (FOUNDATIONAL): Yes/No follow-up binding (deterministic)
    #
    # Problem:
    # - The assistant may ask a yes/no question.
    # - The user replies "yes" / "no".
    # - Without binding, the model can continue as if the answer were either.
    #
    # Fix:
    # - If the user reply is a simple yes/no, bind it to the most recent assistant
    #   message that clearly asks yes/no, and inject a system-only note.
    #
    # Hard rules:
    # - No model calls.
    # - No memory writes.
    # - No guessing beyond "this was a yes/no reply to that yes/no question".
    # ------------------------------------------------------------
    yesno_followup_note = ""
    try:
        # Normalize the user's reply (conservative)
        _yn = _normalize_exact_cmd(clean_user_msg)
        yn_val = ""
        if _yn in ("yes", "y", "yep", "yup", "yeah", "correct", "confirmed", "ok", "okay", "sure"):
            yn_val = "yes"
        elif _yn in ("no", "nope", "nah"):
            yn_val = "no"

        # Only attempt binding for simple yes/no replies
        if yn_val:
            # Find the most recent assistant message that looks like it asked yes/no.
            # Deterministic heuristics: contains "(yes/no)" or "yes / no" and has a '?' somewhere.
            qtxt = ""
            for m in reversed((conversation_history or [])[-12:]):
                if not isinstance(m, dict):
                    continue
                if str(m.get("role") or "") != "assistant":
                    continue
                t = str(m.get("content") or "").strip()
                low = t.lower()
                if not t:
                    continue
                if (("yes/no" in low) or ("yes / no" in low) or ("(yes/no" in low) or ("(yes / no" in low)) and ("?" in t):
                    # Prefer the last line containing a question mark (most specific)
                    lines = [ln.strip() for ln in t.splitlines() if ln.strip()]
                    pick = ""
                    for ln in reversed(lines[-8:]):
                        if "?" in ln:
                            pick = ln.strip()
                            break
                    qtxt = pick or (lines[-1] if lines else t)
                    break

            if qtxt:
                # Bound note: force the model to treat the reply as answering that specific question.
                # Keep it short and system-only.
                if len(qtxt) > 220:
                    qtxt = qtxt[:219].rstrip() + "…"
                yesno_followup_note = (
                    "YES_NO_FOLLOWUP (deterministic):\n"
                    f"- The user answered '{yn_val}' to this prior yes/no question:\n"
                    f"  \"{qtxt}\"\n"
                    "- Treat this as resolved and continue accordingly.\n"
                    "- Do NOT proceed as if the opposite answer were possible."
                )
    except Exception:
        yesno_followup_note = ""
    # CCG — Context Commitment Gate (system-only, read-time only)
    ccg_note = ""
    try:
        ccg_note = _ccg_system_note(conversation_history or [], clean_user_msg or "")
    except Exception:
        ccg_note = ""
    ckcl_lock = ""
    try:
        if _ckcl_committed_and_crowd(conversation_history or [], clean_user_msg or ""):
            ckcl_lock = _ckcl_system_lock_note()
    except Exception:
        ckcl_lock = ""

    analysis_hat_note = ""
    try:
        if str(active_expert or "").strip().lower() == "analysis":
            analysis_hat_note = _ANALYSIS_HAT_LEGAL_OPS_NOTE
    except Exception:
        analysis_hat_note = ""

    messages2 = [
        {"role": "system", "content": sys_prompt},
        {"role": "system", "content": ckcl_lock},
        {"role": "system", "content": default_onramp_note},
        {"role": "system", "content": _time_context_system_note(ctx, current_project_full)},
        # Couples therapist frame (only for couple_* accounts)
        {"role": "system", "content": therapist_frame},
        # Intake mode (session-start + empty facts_map only)
        {"role": "system", "content": couples_intake_note},
        # NL bring-up queue confirmation hint (system-only; no ids)
        # Bringups (session-start only; themes; never attribute)
        {"role": "system", "content": bringups_note},
        {"role": "system", "content": (
            "BRINGUP_BROACHING_RULE:\n"
            "- Present this theme as relationship-level, not user-specific.\n"
            "- Do NOT assume it applies to the current user.\n"
            "- Ask which partner tends to experience this, and how the other responds.\n"
            "- Use neutral phrasing like 'one partner' / 'sometimes in your relationship'."
        )},
        {"role": "system", "content": ef_system_note},
        {"role": "system", "content": analysis_hat_note},

        {
            "role": "system",
            "content": (
                "RECENT_CHAT_TAIL (continuity only; NOT authoritative truth):\n\n"
                + (
                    ctx.read_recent_chat_log_snippet(
                        current_project_full,
                        n_messages=8,
                        cap_chars=4000,
                    )
                    or ""
                )
            ),
        },
        # Continuation context (only populated for "continue"-style turns)
        {"role": "system", "content": c10_last_answer_note},
        # C10 continuity + WIN steering (deterministic)
        {"role": "system", "content": c10_continuity_system_note},
        # Patch 2X: yes/no follow-up binding note (empty unless applicable)
        {"role": "system", "content": yesno_followup_note},
        # CCG: prevent scope resets once context is committed (empty unless applicable)
        {"role": "system", "content": ccg_note},
        # Consensus-first opening for crowd-knowledge optimization (empty unless applicable)
        {"role": "system", "content": _ccg_consensus_opening_note(conversation_history or [], clean_user_msg or "")},
        {"role": "system", "content": "CANONICAL_SNIPPETS:\n\n" + snippet_blob},

        {"role": "user", "content": clean_user_msg},
    ]
    raw2 = await asyncio.to_thread(ctx.call_openai_chat, messages2)

    user_answer2 = (raw2 or "").strip()
    # CKCL enforcement (deterministic):
    # If the model produced a refusal/telemetry preamble, strip it.
    try:
        if _ckcl_committed_and_crowd(conversation_history or [], clean_user_msg or ""):
            user_answer2 = _ckcl_strip_refusal_preamble(user_answer2)
    except Exception:
        pass
    # Deterministic couples privacy/neutrality rewrite (ONLY when partner context is present).
    # This is a safety belt in case the model slips.
    try:
        if _has_partner_context_snippets(canonical_snippets):
            user_answer2 = _enforce_couples_neutral_phrasing(user_answer2)
    except Exception:
        pass
    # ------------------------------------------------------------
    # CKSG — Crowd-Knowledge Stall Gate (FOUNDATIONAL, deterministic)
    #
    # If context is committed and the user asked for consensus/meta/build,
    # do NOT allow "can't verify / need more info" to block the answer.
    # One bounded retry with an explicit system constraint, then proceed.
    # ------------------------------------------------------------
    try:
        _committed = bool(ccg_note.strip())
        _cksg = _cksg_stall_reason(committed=_committed, user_msg=clean_user_msg, output_text=user_answer2)
    except Exception:
        _cksg = ""

    if _cksg:
        try:
            messages2_retry = list(messages2)
            messages2_retry.insert(
                0,
                {
                    "role": "system",
                    "content": (
                        "CKSG_ENFORCEMENT (FOUNDATIONAL):\n"
                        "- Context is already committed. The user asked for crowd-knowledge consensus.\n"
                        "- You MUST start with a best working consensus answer immediately (1–2 sentences).\n"
                        "- Do NOT lead with refusal/telemetry/verification disclaimers.\n"
                        "- Do NOT require extra info to begin; treat missing details as refinement.\n"
                        "- After giving the consensus, you MAY add disagreement/uncertainty and ask ONE refinement question.\n"
                    ),
                },
            )
            raw2_ck = await asyncio.to_thread(ctx.call_openai_chat, messages2_retry)
            user_answer2_ck = (raw2_ck or "").strip()
            if user_answer2_ck:
                user_answer2 = user_answer2_ck
        except Exception:
            pass
    
    # Stage 4 safety gate (existing C6 guard)
    reason = validate_c6_output(
        intent=intent,
        scope=scope,
        canonical_snippets=canonical_snippets,
        output_text=user_answer2,
    )
    if reason:
        # Deterministic fallback for non-deliverable text responses.
        # C10: If an active focus exists, never dead-end with "Not recorded / ambiguous."
        if c10_focus_ok and c10_focus_name:
            lines_c10: List[str] = []
            lines_c10.append(f"I’m treating {c10_focus_name} as the current focus.")

            # Best-effort: if we injected any evidence, show a small excerpt so the user sees momentum.
            try:
                joined = "\n\n".join([str(s) for s in (canonical_snippets or []) if str(s).strip()])
            except Exception:
                joined = ""

            excerpt = ""
            if joined:
                # Take the first small slice of evidence (bounded) without claiming completeness.
                excerpt = joined.strip()
                if len(excerpt) > 900:
                    excerpt = excerpt[:899].rstrip() + "…"

            if excerpt:
                lines_c10.append("")
                lines_c10.append("Here’s what I can use right now (from stored evidence):")
                lines_c10.append(excerpt)

            if c10_allow_questions and c10_win_q:
                lines_c10.append("")
                lines_c10.append(c10_win_q)

            user_answer2 = "\n".join(lines_c10).strip()
        else:
            user_answer2 = "Not recorded / ambiguous."
    else:
        # -----------------------------------------------------------------
        # Generic constraint enforcement gate (project-agnostic)
        # - Compiles constraints from project_state.user_rules + user message
        # - Validates output BEFORE it reaches the user
        # - One bounded retry; then deterministic refusal
        # -----------------------------------------------------------------
        try:
            stx = ctx.project_store.load_project_state(current_project_full) or {}
        except Exception:
            stx = {}

        try:
            constraints = constraint_engine.compile_constraints(project_state=stx, user_msg=clean_user_msg)
            violations = constraint_engine.validate_output(output_text=user_answer2, constraints=constraints)
        except Exception:
            constraints = {}
            violations = []

        if violations:
            # One retry only (bounded)
            try:
                retry_note = constraint_engine.build_retry_system_note(constraints=constraints, violations=violations)
            except Exception:
                retry_note = "CONSTRAINT ENFORCEMENT: regenerate a compliant answer."

            messages2_retry = list(messages2)
            messages2_retry.insert(0, {"role": "system", "content": retry_note})

            raw2b = await asyncio.to_thread(ctx.call_openai_chat, messages2_retry)
            user_answer2b = (raw2b or "").strip()

            try:
                violations2 = constraint_engine.validate_output(output_text=user_answer2b, constraints=constraints)
            except Exception:
                violations2 = []

            if violations2:
                # Never leak constraint-engine diagnostics to the user.
                # Keep continuity: return the retry answer anyway (best-effort), and only fall back
                # to a generic message if the retry produced nothing.
                try:
                    import os
                    if (os.environ.get("LENS0_DEBUG_CONSTRAINTS") or "").strip().lower() in ("1", "true", "yes", "on"):
                        print("[CONSTRAINT] retry still violates:", violations2, flush=True)
                except Exception:
                    pass

                user_answer2 = (user_answer2b or "").strip() or "Sorry — I hit an internal constraint check. Try rephrasing that last message."
            else:
                user_answer2 = user_answer2b

    # Keep history bounded (only for non-lookup)
    conversation_history.append({"role": "user", "content": clean_user_msg})
    conversation_history.append({"role": "assistant", "content": user_answer2})
    if len(conversation_history) > (max_history_pairs * 2):
        conversation_history = conversation_history[-(max_history_pairs * 2):]

    # If an assumption was bound, require explicit labeling
    try:
        if "BOUND_ASSUMPTION:" in "\n".join(canonical_snippets or []):
            if not user_answer2.lower().startswith("assuming"):
                user_answer2 = (
                    "Assuming the only difference between the files is board price:\n\n"
                    + user_answer2
                )
    except Exception:
        pass

    ctx.snapshot_assistant_output(current_project_full, user_answer2)
    try:
        await _maybe_update_interpretive_memory(
            ctx=ctx,
            project_full=current_project_full,
            turn_index=int(turn_n or 0),
            window_text=window_text,
            assistant_text=user_answer2,
        )
    except Exception:
        pass
    try:
        audit["lookup_mode"] = False
        audit["answer_len"] = len(user_answer2 or "")
        audit["elapsed_ms"] = int((time.time() - audit_t0) * 1000)

        fn = getattr(ctx, "audit_event", None)
        if callable(fn):
            fn(current_project_full, audit)
    except Exception:
        pass
    return {
        "user_answer": user_answer2,
        "lookup_mode": False,
        "conversation_history": conversation_history,
        
    }

async def run_model_pipeline(
    *,
    ctx: Any,
    current_project_full: str,
    clean_user_msg: str,
    do_search: bool,
    search_results: str,
    conversation_history: List[Dict[str, str]],
    max_history_pairs: int,
    server_file_path: Path,
    allow_history_in_lookup: bool = True,
    search_cached: bool = False,
    active_expert: str = "default",
) -> Dict[str, Any]:
    # ---------------------------------------------------------------------
    # Audit (turn trace): single record per turn, appended at end (best-effort)
    # ---------------------------------------------------------------------
    audit_t0 = time.time()
    try:
        _tid_fn = getattr(ctx, "get_current_audit_trace_id", None)
        _tid = _tid_fn() if callable(_tid_fn) else ""
    except Exception:
        _tid = ""

    audit: Dict[str, Any] = {
        "schema": "audit_turn_v1",
        "trace_id": str(_tid or ""),
        "project_full": str(current_project_full or ""),
        "clean_user_msg": str(clean_user_msg or ""),
        "do_search_initial": bool(do_search),
        "search_results_len_initial": len(search_results or ""),
        "active_expert": str(active_expert or "default"),
    }
    """
    Runs the extracted model pipeline and returns:
      {
        "user_answer": str,
        "lookup_mode": bool,
        "conversation_history": List[Dict[str,str]],
      }
    """
    ctx.ensure_project_scaffold(current_project_full)

    # Time anchors v1: silently timestamp concrete "I just did X" events (bounded).
    try:
        label = _time_maybe_extract_anchor_label(clean_user_msg)
        if label:
            tz_name = "America/Chicago"
            try:
                cp = str(current_project_full or "")
                user_seg_tz = cp.split("/", 1)[0].strip() if "/" in cp else cp.strip()
            except Exception:
                user_seg_tz = ""
            prof_tz: Dict[str, Any] = {}
            try:
                if user_seg_tz and hasattr(ctx, "project_store") and hasattr(ctx.project_store, "load_user_profile"):
                    prof0 = ctx.project_store.load_user_profile(user_seg_tz)
                    if isinstance(prof0, dict):
                        prof_tz = prof0
            except Exception:
                prof_tz = {}
            try:
                ident_tz = prof_tz.get("identity") if isinstance(prof_tz, dict) else {}
                if not isinstance(ident_tz, dict):
                    ident_tz = {}
                tz = ident_tz.get("timezone") if isinstance(ident_tz.get("timezone"), dict) else {}
                tz_val = str((tz or {}).get("value") or "").strip()
                if tz_val:
                    tz_name = tz_val
            except Exception:
                pass

            if ZoneInfo is not None:
                now_dt = datetime.now(tz=ZoneInfo(tz_name))
            else:
                now_dt = datetime.now()

            _time_update_project_anchors(ctx, current_project_full, label=label, now_dt=now_dt, tz_name=tz_name)
    except Exception:
        pass

    # AOF v2 — Enforce cached vision semantics for image-referential turns BEFORE building context.
    # This satisfies: "any image-referential user turn must have vision semantics available before answering."
    try:
        _ = await _maybe_ensure_active_image_semantics(
            ctx=ctx,
            project_full=current_project_full,
            user_msg=clean_user_msg,
            reason="model_pipeline:run_model_pipeline",
        )
    except Exception:
        pass

    project_context = ctx.build_project_context_for_model(current_project_full, clean_user_msg)
    # -----------------------------------------------------------------
    # Deterministic fact capture (chat -> Tier-1 -> distill -> Tier-2)
    #
    # This prevents the "I can't treat it as stored until CANONICAL_SNIPPETS"
    # loop for simple identity/relationship facts explicitly given by the user.
    # -----------------------------------------------------------------
    try:
        msg = (clean_user_msg or "").strip()

        # Capture: girlfriend/partner name statements
        m = re.match(
            r"(?is)^\s*(?:my\s+(?:girlfriend|partner)(?:'s)?\s+name\s+is|her\s+name\s+is)\s+(.+?)\s*$",
            msg,
        )
        if m:
            name = re.sub(r"[.?!]+$", "", m.group(1).strip()).strip()
            if name:
                # Write Tier-1
                ctx.project_store.append_fact_raw_candidate(
                    current_project_full,
                    claim=f"My girlfriend's name is {name}",
                    slot="relationship",
                    subject="user",
                    source="chat",
                    evidence="User stated this directly in chat.",
                )

                # Normalize Tier-1 (adds hashes/keys/etc)
                try:
                    ctx.project_store.normalize_facts_raw_jsonl(current_project_full)
                except Exception:
                    pass

                # Choose distill profile based on expert frame (default general)
                prof = "general"
                try:
                    st = ctx.project_store.load_project_state(current_project_full) or {}
                    ef = st.get("expert_frame") if isinstance(st, dict) else {}
                    label = ""
                    if isinstance(ef, dict):
                        label = str(ef.get("label") or "").strip().lower()
                    if label == "therapist":
                        prof = "therapist"
                except Exception:
                    prof = "general"

                # Distill immediately so next turn is correct
                try:
                    ctx.project_store.distill_facts_raw_to_facts_map_tier2p(current_project_full)
                except Exception:
                    pass


                user_answer = f"Got it — I’ll remember that your girlfriend’s name is {name}."

                # Keep history bounded (only for non-lookup)
                conversation_history.append({"role": "user", "content": clean_user_msg})
                conversation_history.append({"role": "assistant", "content": user_answer})
                if len(conversation_history) > (max_history_pairs * 2):
                    conversation_history = conversation_history[-(max_history_pairs * 2):]

                return {
                    "user_answer": user_answer,
                    "lookup_mode": False,
                    "conversation_history": conversation_history,
                }
    except Exception:
        pass

    # Decide Excel intent (deliverable) vs Lookup intent (factual web-backed answer).
    low_msg = clean_user_msg.lower()

    # If the user is explicitly asking for PATCH MODE, do NOT force Excel behavior.
    if (
        low_msg == "patch"
        or low_msg.startswith("patch\n")
        or low_msg.startswith("patch\r\n")
        or low_msg.startswith("patch:")
    ):
        excel_intent = False
    else:
        excel_intent = bool(
            re.search(r"\b(excel|spreadsheet|workbook|xlsx|xlsm)\b", low_msg)
        ) or any(
            phrase in low_msg
            for phrase in (
                "make the new sheet",
                "make a new sheet",
                "build the workbook",
                "create the workbook",
                "generate the workbook",
                "just make the sheet",
                "just make the workbook",
                "make the sheet",
                "create the sheet",
                "generate the sheet",
            )
        )

    # Lookup Mode is ONLY for web-backed factual lookups.
    # It must NOT trigger for Excel or Patch work.
    lookup_mode = (bool(do_search) or bool(search_cached) or bool(search_results)) and (not excel_intent) and (not ctx.looks_like_patch_request(clean_user_msg))

    # Deliverable intent: if the user is clearly asking for a print-ready output or a remake/update
    # of an existing deliverable, force DELIVERABLE_HTML.
    low_msg2 = (clean_user_msg or "").lower()
    deliverable_intent = (
        ("deliverable" in low_msg2)
        or ("print-ready" in low_msg2)
        or ("print ready" in low_msg2)
        # Explicit HTML/doc request (common phrasing): "make/generate/create an html page/report"
        or (
            ("html" in low_msg2)
            and any(x in low_msg2 for x in ("make", "create", "generate", "build", "write", "draft", "format", "export"))
        )
        or ("cookbook" in low_msg2 and any(x in low_msg2 for x in ("make", "remake", "rebuild", "regenerate", "update", "export", "format")))
        or (any(x in low_msg2 for x in ("remake it", "redo it", "revise it", "update it", "make it look", "make it modern"))
            and any(x in low_msg2 for x in ("html", "document", "page", "template", "layout", "cookbook")))
        or (any(x in low_msg2 for x in ("pdf", "export", "print", "printable", "formatted"))
            and any(x in low_msg2 for x in ("html", "document", "cookbook", "deliverable")))
    )

    # Build messages for the model
    if lookup_mode:
        messages: List[Dict[str, str]] = [{"role": "system", "content": ctx.LENS0_LOOKUP_SYSTEM_PROMPT}]
    else:
        messages = [{"role": "system", "content": ctx.LENS0_SYSTEM_PROMPT}]

    # Always-on minimal time awareness (system-only; tiny; prevents "no clock access" refusals)
    try:
        messages.append({"role": "system", "content": _time_context_system_note(ctx, current_project_full)})
    except Exception:
        pass
    # Always-on capability awareness (system-only; bounded).
    # This is NOT a “capabilities list command” — it’s background knowledge.
    try:
        if hasattr(ctx, "capabilities") and hasattr(ctx.capabilities, "render_human_summary"):
            cap_txt = ctx.capabilities.render_human_summary(max_items=40)
            cap_txt = (cap_txt or "").strip()
            if cap_txt:
                # Hard cap to keep tokens stable
                if len(cap_txt) > 2200:
                    cap_txt = cap_txt[:2200].rstrip() + "\n…(truncated)"
                messages.append({"role": "system", "content": "CAPABILITY_CONTEXT:\n" + cap_txt})
    except Exception:
        pass

    # Dynamic capability state (availability + missing prerequisites).
    try:
        if hasattr(ctx, "build_capability_state_note"):
            st_note = str(ctx.build_capability_state_note() or "").strip()
            if st_note:
                messages.append({"role": "system", "content": st_note})
    except Exception:
        pass
    
    cap_gap_obj: Optional[Dict[str, Any]] = None

    if lookup_mode:
        search_mode_note = (
            "- A live web search WAS just performed for this question.\n"
            "- You have access to returned search snippets and URLs (SEARCH_EVIDENCE_JSON).\n"
        )
        if (not bool(do_search)) and bool(search_cached):
            search_mode_note = (
                "- Cached web search evidence is provided for this topic.\n"
                "- Treat it as evidence, but it may be stale; do not invent newer facts.\n"
                "- You have access to returned search snippets and URLs (SEARCH_EVIDENCE_JSON).\n"
            )
        messages.append(
            {
                "role": "system",
                "content": (
                    search_mode_note
                    + "- Treat that evidence as noisy claims; it increases coverage, not certainty.\n"
                    "- You may summarize or reason from those snippets.\n"
                    "- You do NOT necessarily have full article text or verbatim transcripts unless they appear in the snippets.\n"
                    "- Do NOT say you cannot search, browse, or verify.\n"
                    "- If evidence is incomplete, say it is limited or ambiguous — not unavailable."
                ),
            }
        )
    # CCG — Context Commitment Gate (prevents scope reset after context is already established)
    try:
        _ccg = _ccg_system_note(conversation_history or [], clean_user_msg or "")
    except Exception:
        _ccg = ""
    if _ccg:
        messages.append({"role": "system", "content": _ccg})
        messages.append({"role": "system", "content": _ccg_consensus_opening_note(conversation_history or [], clean_user_msg or "")})
    # Excel intent: force EXCEL_SPEC_JSON via system nudge (Project OS mode only)
    if excel_intent:
        messages.append(
            {
                "role": "system",
                "content": (
                    "The user is requesting an Excel deliverable. You MUST include a valid [EXCEL_SPEC_JSON] block. "
                    "Prefer TEMPLATE MODE so structure/formulas are preserved: set auto_template_latest_xlsx=true unless the user specifies template_from_raw. "
                    "If LATEST_EXCEL_BLUEPRINT is present in CURRENT_PROJECT_CONTEXT, use it as the source of truth for what must be preserved. "
                    "Only change what the user requested; do NOT invent formulas/structure outside the blueprint/template. "
                    "Do NOT provide manual step-by-step Excel UI instructions. "
                    "In [USER_ANSWER], be brief and include ONLY the final Open: /file?path=... link line."
                ),
            }
        )

    # Deliverable intent: force DELIVERABLE_HTML via system nudge (Project OS mode only)
    if (not lookup_mode) and (not excel_intent) and deliverable_intent:
        messages.append(
            {
                "role": "system",
                "content": (
                    "The user is requesting a print-ready deliverable (or a remake/update of one). "
                    "You MUST include a full [DELIVERABLE_HTML]...</DELIVERABLE_HTML> block. "
                    "Do NOT put HTML in [USER_ANSWER]. "
                    "In [USER_ANSWER], be brief and include ONLY a short summary and the final Open: /file?path=... line (the server will add the correct link). "
                    "If an image is available, embed it via its /file?path=... URL; otherwise use a placeholder without claiming an image exists."
                ),
            }
        )

    # Only include full project context in Project OS mode (NOT lookup mode).
    if (not lookup_mode) and project_context:
        messages.append({"role": "system", "content": "CURRENT_PROJECT_CONTEXT:\n\n" + project_context})
    # ------------------------------------------------------------
    # Deterministic ledger context (MVP)
    # Only inject when building an Excel deliverable.
    #
    # IMPORTANT:
    # Inject row-oriented lines instead of raw JSON so the model can
    # deterministically materialize ledger rows into a sheet.
    # ------------------------------------------------------------
    if (not lookup_mode) and excel_intent:
        try:
            latest = ctx.project_store.get_latest_artifact_by_type(current_project_full, "extraction_ledger")

            # Read enough JSON to parse items, but inject a much smaller, row-oriented excerpt.
            raw_json = ctx.project_store.read_artifact_text(current_project_full, latest, cap_chars=220000).strip()
            if raw_json:
                obj = {}
                try:
                    obj = json.loads(raw_json)
                except Exception:
                    obj = {}

                items = obj.get("items") if isinstance(obj, dict) else None
                if not isinstance(items, list):
                    items = []

                cols = [
                    "source_file",
                    "source_type",
                    "where_found",
                    "label",
                    "qty",
                    "unit_cost",
                    "total_cost",
                    "currency",
                    "confidence",
                    "notes",
                    "extracted_at",
                ]

                def _clean_cell(v: Any, *, max_len: int = 220) -> str:
                    s = "" if v is None else str(v)
                    s = s.replace("\r", " ").replace("\n", " ").strip()
                    # avoid breaking the row delimiter
                    s = s.replace("|", "/")
                    if max_len and len(s) > max_len:
                        s = s[: max_len - 1].rstrip() + "…"
                    return s

                header = " | ".join(cols)
                lines: List[str] = []
                lines.append("LEDGER_ROWS (deterministic; row-oriented; may be truncated):")
                lines.append(f"item_count={len(items)}")
                lines.append("")
                lines.append(header)

                # Hard cap for injection payload (chars) to keep prompts stable
                cap_chars_total = 24000
                used = sum(len(x) + 1 for x in lines)

                emitted = 0
                for it in items:
                    if not isinstance(it, dict):
                        continue

                    row = " | ".join(_clean_cell(it.get(c), max_len=(320 if c == "label" else 180)) for c in cols)

                    # Stop if we'd exceed cap
                    if used + len(row) + 1 > cap_chars_total:
                        break

                    lines.append(row)
                    used += len(row) + 1
                    emitted += 1

                if emitted < len(items):
                    lines.append("")
                    lines.append(f"(truncated) emitted_rows={emitted} of item_count={len(items)}")

                ledger_rows_text = "\n".join(lines).strip()
                if ledger_rows_text:
                    messages.append({"role": "system", "content": ledger_rows_text})
        except Exception:
            pass

    # IMPORTANT: If the user is asking about THIS server's code (e.g., "in server.py..."),
    # inject a bounded excerpt from the running server file so we can answer precisely
    # without requiring the user to upload server.py into the project.
    try:
        server_code_query = ("server.py" in low_msg) or ("should_auto_web_search" in low_msg)
        if server_code_query:
            full_server_text = ctx.read_text_file(Path(server_file_path).resolve(), errors="replace")
            needle = "def should_auto_web_search"
            pos = full_server_text.find(needle)
            if pos == -1:
                pos = full_server_text.lower().find("should_auto_web_search")
            # Build a bounded excerpt around the relevant section (and include a header so it's obvious)
            if full_server_text:
                if pos == -1:
                    excerpt = full_server_text[:18000]
                else:
                    lo = max(0, pos - 12000)
                    hi = min(len(full_server_text), pos + 24000)
                    excerpt = full_server_text[lo:hi]
                messages.append({"role": "system", "content": "SERVER_CODE_CONTEXT (excerpt of current server.py):\n\n" + excerpt})
    except Exception:
        pass

    # -------------------------------------------------------------------------
    # Code_Help helpers (kept behavior-identical; do NOT depend on WS internals)
    # -------------------------------------------------------------------------

    def _is_code_help_project(project_full_name: str) -> bool:
        """
        True only when the *short* project name is exactly 'code_help' (case-insensitive).
        Ensures this behavior never activates in other projects.
        """
        try:
            short = (project_full_name or "").split("/", 1)[-1].strip().lower()
            return short == "code_help"
        except Exception:
            return False

    def _find_codehelp_index_files(project_full_name: str) -> List[Path]:
        """
        Find chunk-map/index artifacts like:
          code_<...>_index*.md
        Only searches within this project's artifacts folder.
        """
        try:
            ad = ctx.artifacts_dir(project_full_name)
            if not ad.exists():
                return []
            out: List[Path] = []
            for p in ad.iterdir():
                if not p.is_file():
                    continue
                name = p.name.lower()
                if not name.endswith(".md"):
                    continue
                if not name.startswith("code_"):
                    continue
                if "index" not in name:
                    continue
                out.append(p)
            out.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            return out
        except Exception:
            return []

    def build_codehelp_capability_map(project_full_name: str, cap_chars_total: int = 18000) -> str:
        """
        Build a bounded, model-readable map of what the codebase can do + where to look.
        This is designed to reduce capability hallucinations in Code_Help normal chat.
        """
        files = _find_codehelp_index_files(project_full_name)
        if not files:
            return (
                "CODEHELP_CAPABILITY_MAP:\n\n"
                "- No code_*index*.md files found in this project's artifacts folder.\n"
                "- If you expect chunk maps, regenerate them and ensure they are saved as artifacts.\n"
            )

        parts: List[str] = []
        parts.append("CODEHELP_CAPABILITY_MAP:")
        parts.append("")
        parts.append("Rules:")
        parts.append("- Treat this as the source of truth for what code exists/can do.")
        parts.append("- If something is not in these maps, label it 'unconfirmed' instead of guessing.")
        parts.append("")
        parts.append("Available chunk/index maps (most recent first):")
        for p in files[:8]:
            parts.append(f"- {p.name}")

        parts.append("")
        parts.append("===INDEX_MAPS_START===")

        used = 0
        per_file_cap = max(2000, int(cap_chars_total / max(1, min(len(files), 4))))
        for p in files[:4]:
            try:
                txt = ctx.read_text_file(p, errors="replace")
            except Exception:
                txt = ""
            if not txt:
                continue
            if len(txt) > per_file_cap:
                txt = txt[:per_file_cap].rstrip() + "\n...[truncated]..."
            block = f"\n---\nFILE: {p.name}\n---\n{txt}\n"
            if used + len(block) > cap_chars_total:
                break
            parts.append(block)
            used += len(block)

        parts.append("===INDEX_MAPS_END===")
        return "\n".join(parts).strip()

    # Code_Help only: inject capability + chunk/index maps on EVERY normal chat turn.
    if _is_code_help_project(current_project_full):
        cap_map = build_codehelp_capability_map(current_project_full, cap_chars_total=18000)
        if cap_map.strip():
            messages.append({"role": "system", "content": cap_map})
        if ctx._wants_codehelp_code_review(clean_user_msg):
            review_corpus = ctx.build_codehelp_review_corpus(
                current_project_full,
                clean_user_msg,
                max_chars_total=50000,
            )
            if review_corpus.strip():
                messages.append({"role": "system", "content": review_corpus})

    # Expectation-shift / recency framing guard:
    # If the user asks "what changed / last X months / now required", force explicit time anchoring
    # and a before/after contrast. This improves answer alignment without touching memory tiers.
    try:
        lowq = (clean_user_msg or "").strip().lower()
        expectation_shift = (
            ("what changed" in lowq)
            or ("what has changed" in lowq)
            or ("what’s changed" in lowq)
            or ("whats changed" in lowq)
            or ("changed in the last" in lowq)
            or ("in the last" in lowq and any(u in lowq for u in ("days", "weeks", "months", "years")))
            or ("last 12 months" in lowq)
            or ("past 12 months" in lowq)
            or ("over the last" in lowq and any(u in lowq for u in ("days", "weeks", "months", "years")))
            or ("since " in lowq)
            or ("now required" in lowq)
            or ("being enforced" in lowq)
            or ("enforced" in lowq)
            or ("new guidance" in lowq)
            or ("updated guidance" in lowq)
        )
    except Exception:
        expectation_shift = False

    if expectation_shift:
        messages.append({
            "role": "system",
            "content": (
                "EXPECTATION_SHIFT_MODE:\n"
                "- The user is asking for what changed over a recent time window.\n"
                "- You MUST:\n"
                "  1) Provide explicit date anchors (e.g., month/day/year) when possible.\n"
                "  2) Separate 'newly codified/standardized' vs 'newly enforced/expected in practice'.\n"
                "  3) Use a before/after contrast (what was already true vs what shifted recently).\n"
                "  4) If SEARCH_EVIDENCE_JSON is present, use it to support dates and enforcement claims.\n"
                "  5) If you cannot support a specific date from evidence, say so (do not guess).\n"
            )
        })

    if (do_search or search_cached) and search_results:
        try:
            print(f"[WEB] Injecting SEARCH_EVIDENCE_JSON chars={len(search_results)}")
        except Exception:
            pass
        messages.append(
            {
                "role": "system",
                "content": (
                    "SEARCH_EPISTEMIC_RULES:\n"
                    "- Treat SEARCH_EVIDENCE_JSON as noisy external evidence (claims), not authority.\n"
                    "- Search increases coverage/recency; it must NOT increase certainty by itself.\n"
                    "- If results converge, you may be decisive; if they conflict, surface the disagreement.\n"
                    "- If SEARCH_EVIDENCE_JSON contains \"insufficient\": true, treat that as NO_SUPPORT_FOUND for the intended query.\n"
                    "  - Do NOT claim confirmation.\n"
                    "  - Say you could not find supporting evidence in the snippets and ask ONE refinement question.\n"
                    "- If evidence is insufficient, say 'not enough evidence yet' and state what would change your mind.\n"
                    "- Do not use citation theater; only include 1–3 URLs when it materially helps.\n"
                ),
            }
        )
        messages.append({"role": "system", "content": "SEARCH_EVIDENCE_JSON:\n\n" + search_results})
        # FOUNDATIONAL: When the user asks for latest/current/status, force epistemic synthesis.
        try:
            if _requires_status_synthesis(clean_user_msg):
                messages.append({"role": "system", "content": _status_synthesis_system_note()})
        except Exception:
            pass

    elif do_search:
        try:
            print("[WEB] do_search=True but search_results empty/falsey (nothing injected)")
        except Exception:
            pass

    if conversation_history and (not lookup_mode or allow_history_in_lookup):
        if lookup_mode and allow_history_in_lookup:
            messages.append(
                {
                    "role": "system",
                    "content": (
                        "CONVERSATION_HISTORY_NOTE:\n"
                        "- Prior turns are provided for continuity only.\n"
                        "- Treat prior assistant statements as non-authoritative unless supported by SEARCH_EVIDENCE_JSON "
                        "or CANONICAL_SNIPPETS.\n"
                    ),
                }
            )
            hist = [m for m in (conversation_history or []) if (m.get("role") in ("user", "assistant"))]
            messages.extend(hist)
        else:
            messages.extend(conversation_history)
    # Crowd-knowledge forward progress (FOUNDATIONAL):
    # If the user is asking for an optimization build/tier/config and we have any committed context,
    # force: answer-first, then one refinement question (no scope reset).
    try:
        _ccg2 = _ccg_system_note(conversation_history or [], clean_user_msg or "")
        if _ccg2:
            messages.append(
                {
                    "role": "system",
                    "content": (
                        "CROWD_KNOWLEDGE_FORWARD_PROGRESS:\n"
                        "- Provide the best working consensus answer first (provisional is fine).\n"
                        "- Then: surface disagreement bands and one refinement question.\n"
                        "- Do NOT block on missing kit/stats; treat them as refinement, not unlock conditions."
                    ),
                }
            )
    except Exception:
        pass

    messages.append({"role": "user", "content": clean_user_msg})

    raw_answer = await asyncio.to_thread(ctx.call_openai_chat, messages)
    # FOUNDATIONAL: prevent epistemic early-exit when search evidence exists.
    # If the model tries to lead with "not confirmed" without enumerating positives, force a rewrite cue.
    try:
        if (do_search or search_cached) and (search_results or "").strip():
            raw_answer = _enforce_evidence_exhaustion(
                search_results=search_results,
                output_text=raw_answer,
            )
    except Exception:
        pass

    # PATCH_PROTOCOL_V1 must be applied even if lookup_mode=True.
    # Minimal rule: only override lookup behavior when a complete PATCH_PROTOCOL_V1 block is present.
    patch_block_present = False
    try:
        ra0 = (raw_answer or "")
        patch_block_present = ("[PATCH_PROTOCOL_V1]" in ra0) and ("[/PATCH_PROTOCOL_V1]" in ra0)
    except Exception:
        patch_block_present = False

    if lookup_mode and (not patch_block_present):
        user_answer = (raw_answer or "").strip()

        # ---------------------------------------------------------
        # LOOKUP GROUNDING GATE (FOUNDATIONAL)
        #
        # Rules:
        # - In lookup mode, the assistant must treat SEARCH_EVIDENCE_JSON as the only web evidence stream.
        # - Any cited URL must appear verbatim in SEARCH_EVIDENCE_JSON.
        # - If grounding fails, downgrade to uncertainty instead of hallucinating.
        # ---------------------------------------------------------

        try:
            ans_low = user_answer.lower()
            # search_results may be structured JSON (SEARCH_EVIDENCE_JSON); URLs must still be present verbatim.
            res_low = (search_results or "").lower()

            # Extract URLs the model claims as sources
            cited_urls = re.findall(r"https?://[^\s)]+", user_answer)

            ungrounded = False

            for u in cited_urls:
                if u.lower() not in res_low:
                    ungrounded = True
                    break

            # Also guard against fabricated certainty phrases when evidence is thin
            certainty_markers = (
                "reported on",
                "according to",
                "earnings call",
                "confirmed that",
                "said that",
                "announced that",
            )

            if any(m in ans_low for m in certainty_markers) and not res_low.strip():
                ungrounded = True

            if ungrounded:
                user_answer = (
                    "I ran a live search, but I don’t see solid primary-source confirmation for that claim.\n\n"
                    "What the available results suggest is ambiguous or speculative, and I don’t want to overstate it.\n\n"
                    "If you want, I can:\n"
                    "- summarize what *is* clearly supported\n"
                    "- separate analyst speculation from official statements\n"
                    "- watch this topic and flag it if/when Tesla confirms it directly"
                )

        except Exception:
            pass

        blocks = {}
        created_paths: List[str] = []

    else:
        user_answer, blocks = ctx.parse_model_output(raw_answer)

        # Deliverable fallback (deterministic):
        # If the user explicitly requested an HTML deliverable but the model forgot the
        # [DELIVERABLE_HTML] block and instead emitted raw <html>...</html>, capture it.
        if deliverable_intent:
            try:
                if isinstance(blocks, dict) and "DELIVERABLE_HTML" not in blocks:
                    m = re.search(r"(<html\b.*?</html>)", raw_answer or "", flags=re.IGNORECASE | re.DOTALL)
                    if m:
                        blocks["DELIVERABLE_HTML"] = m.group(1).strip()
                        # Prevent dumping the full HTML into chat; the server will append the real link.
                        user_answer = "Generated HTML deliverable."
            except Exception:
                pass

        # HARD GATE:
        # Never allow DELIVERABLE_HTML unless the user explicitly requested a deliverable.
        if not deliverable_intent:
            if isinstance(blocks, dict) and "DELIVERABLE_HTML" in blocks:
                del blocks["DELIVERABLE_HTML"]

        # Sanitize PROJECT_STATE_JSON goal:
        # The model is required to emit "goal", so on trivial first turns it tends to set goal="hello".
        # Prevent greeting/too-short goals from being persisted when the project has no goal yet.
        try:
            if isinstance(blocks, dict) and "PROJECT_STATE_JSON" in blocks:
                st_prev = {}
                try:
                    st_prev = ctx.project_store.load_project_state(current_project_full) or {}
                except Exception:
                    st_prev = {}
                prev_goal = str((st_prev or {}).get("goal") or "").strip()

                raw_ps = (blocks.get("PROJECT_STATE_JSON") or "").strip()
                ps_obj = {}
                try:
                    ps_obj = json.loads(raw_ps) if raw_ps else {}
                except Exception:
                    ps_obj = {}

                if isinstance(ps_obj, dict):
                    cand_goal = str(ps_obj.get("goal") or "").strip()
                    low = cand_goal.lower()

                    greeting_only = low in ("hi", "hello", "hey", "yo", "sup", "hiya", "howdy")
                    too_short = (len(cand_goal) > 0) and (len(cand_goal) < 10)

                    if (not prev_goal) and cand_goal and (greeting_only or too_short):
                        ps_obj["goal"] = ""
                        blocks["PROJECT_STATE_JSON"] = json.dumps(ps_obj, ensure_ascii=False, indent=2)
        except Exception:
            pass

        # Companion Flow: if PATCH_PROTOCOL_V1 is present, never attempt whole-doc writes.
        # Deterministic: patch protocol wins; discard whole-doc blocks before persisting.
        try:
            pr = (blocks.get("PATCH_PROTOCOL_V1") or "").strip() if isinstance(blocks, dict) else ""
            if pr and (not ctx._normalize_no_change(pr)):  # uses same notion of "no-change" as server
                for k in ("PROJECT_MAP", "PROJECT_BRIEF", "WORKING_DOC", "PREFERENCES", "DECISION_LOG", "FACTS_MAP", "PROJECT_STATE_JSON"):
                    if k in blocks and k != "PATCH_PROTOCOL_V1":
                        blocks.pop(k, None)
        except Exception:
            pass

        # Persist state blocks, BUT NEVER allow the model to overwrite Tier-2P facts_map.
        # Tier-2P is owned exclusively by the deterministic memory pipeline.
        if isinstance(blocks, dict) and "FACTS_MAP" in blocks:
            blocks.pop("FACTS_MAP", None)

        # Capability gap capture (optional; append-only).
        cap_gap_obj: Optional[Dict[str, Any]] = None
        try:
            if isinstance(blocks, dict):
                raw_gap = str(blocks.get("CAPABILITY_GAP_JSON") or "").strip()
            else:
                raw_gap = ""
        except Exception:
            raw_gap = ""

        if raw_gap and (not ctx._normalize_no_change(raw_gap)):
            try:
                gap_obj = json.loads(raw_gap)
            except Exception:
                gap_obj = _safe_json_extract(raw_gap)

            if isinstance(gap_obj, dict) and _capability_gap_meaningful(gap_obj):
                gap_obj.setdefault("task_summary", str(clean_user_msg or "")[:220])
                try:
                    gap_obj.setdefault("created_at", ctx.now_iso())
                except Exception:
                    pass
                try:
                    cap_gap_obj = ctx.project_store.append_capability_gap_entry(current_project_full, gap_obj)
                    ctx.project_store.build_capability_gap_views_and_write(current_project_full)
                except Exception:
                    cap_gap_obj = gap_obj

        # Fallback: if gap exists but user_answer didn't mention it, append a short note.
        if isinstance(cap_gap_obj, dict) and _capability_gap_meaningful(cap_gap_obj):
            try:
                low = (user_answer or "").lower()
                if not any(k in low for k in ("cannot", "can't", "unable", "limit", "missing", "would need", "what would make")):
                    note = _render_capability_gap_note(cap_gap_obj)
                    if note:
                        user_answer = (user_answer.rstrip() + "\n\n" + note).strip()
            except Exception:
                pass

        created_paths = ctx.persist_state_blocks(current_project_full, blocks)



    # Safety: never let tag tokens leak into what the user sees (model sometimes double-wraps).
    try:
        for tok in (
            "[USER_ANSWER]", "[/USER_ANSWER]",
            "[PROJECT_STATE_JSON]", "[/PROJECT_STATE_JSON]",
            "[PROJECT_MAP]", "[/PROJECT_MAP]",
            "[PROJECT_BRIEF]", "[/PROJECT_BRIEF]",
            "[WORKING_DOC]", "[/WORKING_DOC]",
            "[PREFERENCES]", "[/PREFERENCES]",
            "[DECISION_LOG]", "[/DECISION_LOG]",
            "[FACTS_MAP]", "[/FACTS_MAP]",
            "[DISCOVERY_INDEX_JSON]", "[/DISCOVERY_INDEX_JSON]",
            "[FACT_LEDGER_JSON]", "[/FACT_LEDGER_JSON]",
            "[CAPABILITY_GAP_JSON]", "[/CAPABILITY_GAP_JSON]",
            "[PATCH_PROTOCOL_V1]", "[/PATCH_PROTOCOL_V1]",
        ):
            if tok in user_answer:
                user_answer = user_answer.replace(tok, "")
        user_answer = user_answer.strip()
    except Exception:
        pass
    # C5.2 — Truth binding guard:
    # Never allow model-emitted "Project Pulse" / status blocks to reach the user.
    # Pulse/status must be deterministic from canonical sources, not model narrative.
    try:
        ua_lines = (user_answer or "").splitlines()
        # Drop any leading pulse-like lines
        cleaned: List[str] = []
        skipping = True
        for ln in ua_lines:
            l0 = (ln or "").strip()
            low = l0.lower()
            if skipping and (not l0):
                continue
            if skipping and (
                low.startswith("project pulse")
                or low.startswith("pulse:")
                or low.startswith("status:")
                or low.startswith("resume:")
            ):
                continue
            # Once we hit a normal line, stop skipping
            skipping = False
            cleaned.append(ln)
        user_answer = "\n".join(cleaned).strip()
    except Exception:
        pass
    # -----------------------------------------------------------------
    # Generic constraint enforcement gate (project-agnostic)
    # Applies to the user-visible answer only (not lookup mode).
    # One bounded retry; then deterministic refusal.
    # -----------------------------------------------------------------
    if not lookup_mode:
        try:
            stx2 = ctx.project_store.load_project_state(current_project_full) or {}
        except Exception:
            stx2 = {}

        try:
            constraints2 = constraint_engine.compile_constraints(project_state=stx2, user_msg=clean_user_msg, active_expert=active_expert)
            violations_a = constraint_engine.validate_output(output_text=user_answer, constraints=constraints2)
        except Exception:
            constraints2 = {}
            violations_a = []

        if violations_a:
            try:
                retry_note2 = constraint_engine.build_retry_system_note(constraints=constraints2, violations=violations_a)
            except Exception:
                retry_note2 = "CONSTRAINT ENFORCEMENT: regenerate a compliant answer."

            # Re-run the SAME pipeline with an extra system note (bounded).
            messages_retry = list(messages)
            messages_retry.insert(0, {"role": "system", "content": retry_note2})

            raw_answer_b = await asyncio.to_thread(ctx.call_openai_chat, messages_retry)

            if lookup_mode:
                user_answer_b = (raw_answer_b or "").strip()
            else:
                user_answer_b, blocks_b = ctx.parse_model_output(raw_answer_b)
                # IMPORTANT: do NOT write state on retry; this is an output repair pass only.
                _ = blocks_b

            # Apply the same token stripping rules to the retry output
            try:
                for tok in (
                    "[USER_ANSWER]", "[/USER_ANSWER]",
                    "[PROJECT_STATE_JSON]", "[/PROJECT_STATE_JSON]",
                    "[PROJECT_MAP]", "[/PROJECT_MAP]",
                    "[PROJECT_BRIEF]", "[/PROJECT_BRIEF]",
                    "[WORKING_DOC]", "[/WORKING_DOC]",
                    "[PREFERENCES]", "[/PREFERENCES]",
                    "[DECISION_LOG]", "[/DECISION_LOG]",
                    "[FACTS_MAP]", "[/FACTS_MAP]",
                    "[CAPABILITY_GAP_JSON]", "[/CAPABILITY_GAP_JSON]",
                ):
                    if tok in user_answer_b:
                        user_answer_b = user_answer_b.replace(tok, "")
                user_answer_b = user_answer_b.strip()
            except Exception:
                pass

            try:
                ua_lines_b = (user_answer_b or "").splitlines()
                cleaned_b: List[str] = []
                skipping_b = True
                for ln in ua_lines_b:
                    l0 = (ln or "").strip()
                    low = l0.lower()
                    if skipping_b and (not l0):
                        continue
                    if skipping_b and (
                        low.startswith("project pulse")
                        or low.startswith("pulse:")
                        or low.startswith("status:")
                        or low.startswith("resume:")
                    ):
                        continue
                    skipping_b = False
                    cleaned_b.append(ln)
                user_answer_b = "\n".join(cleaned_b).strip()
            except Exception:
                pass

            try:
                violations_b = constraint_engine.validate_output(output_text=user_answer_b, constraints=constraints2)
            except Exception:
                violations_b = []

            if violations_b:
                # Never leak constraint-engine diagnostics to the user.
                # Keep continuity: return the retry answer anyway (best-effort), and only fall back
                # to a generic message if the retry produced nothing.
                try:
                    if (os.environ.get("LENS0_DEBUG_CONSTRAINTS") or "").strip().lower() in ("1", "true", "yes", "on"):
                        print("[CONSTRAINT] retry still violates:", violations_b, flush=True)
                except Exception:
                    pass

                user_answer = (user_answer_b or "").strip() or "Sorry — I hit an internal constraint check. Try rephrasing that last message."
            else:
                user_answer = user_answer_b

    # C11 — Autopilot synthesis (uploads; deterministic)
    # Trigger only when the user explicitly references the active focus filename in chat.
    try:
        if (not lookup_mode) and c10_focus_ok and c10_focus_name and ((clean_user_msg or "").strip() == (c10_focus_name or "").strip()):
            if isinstance(c10_evidence, dict) and c10_evidence.get("has_any"):
                user_answer = (
                    (user_answer or "").rstrip()
                    + "\n\n"
                    + f"What changed: {c10_focus_name} finished processing and is indexed as the active focus.\n"
                    + "Why it matters: I can use its extracted evidence to take concrete next steps without re-uploading.\n"
                    + "Next step: Tell me the outcome you want from this file (e.g., summarize, extract fields, draft a deliverable)."
                ).strip()
    except Exception:
        pass

    # If a deliverable was created, append a guaranteed working /file link.
    # This prevents the model from inventing paths that 404.
    # Normalize paths (Windows uses backslashes; link logic expects forward slashes)
    created_paths_norm = [(p or "").replace("\\", "/") for p in (created_paths or [])]

    deliverables = [p for p in created_paths_norm if "/artifacts/" in p and p.endswith(".html")]
    # C1: Register deliverable as an explicit, addressable object (no inference).
    try:
        import project_store  # local import to avoid any server/module coupling
        if deliverables:
            project_store.register_deliverable(
                current_project_full,
                deliverable_type="html",
                title="HTML Deliverable",
                path=deliverables[-1],
                source="model_pipeline",
            )
    except Exception:
        pass

    excel_files = [p for p in created_paths_norm if "/artifacts/" in p and p.endswith(".xlsx")]

    # -----------------------------------------------------------------
    # Deterministic fallback: If the user requested an Excel deliverable
    # but the model failed to emit [EXCEL_SPEC_JSON], generate a minimal
    # workbook from the latest extraction ledger.
    #
    # This is additive and only runs when:
    #   - excel_intent == True
    #   - no .xlsx artifact was created by the model
    # -----------------------------------------------------------------
    if excel_intent and (not excel_files):
        try:
            # Pull latest ledger artifact (JSON)
            latest_led = ctx.project_store.get_latest_artifact_by_type(current_project_full, "extraction_ledger")
            led_txt = ctx.project_store.read_artifact_text(current_project_full, latest_led, cap_chars=400000).strip()
            obj = {}
            try:
                obj = json.loads(led_txt) if led_txt else {}
            except Exception:
                obj = {}
            items = obj.get("items") if isinstance(obj, dict) else None
            if not isinstance(items, list):
                items = []

            # Build workbook deterministically
            wb = Workbook()
            ws = wb.active
            ws.title = "Ledger"

            cols = [
                "source_file",
                "source_type",
                "where_found",
                "label",
                "qty",
                "unit_cost",
                "total_cost",
                "currency",
                "confidence",
                "notes",
                "extracted_at",
            ]
            ws.append(cols)

            def _cell(v: Any) -> Any:
                if v is None:
                    return ""
                # Keep numbers as numbers when possible
                if isinstance(v, (int, float)):
                    return v
                s = str(v)
                return s.replace("\r", " ").replace("\n", " ").strip()

            # Hard cap rows to keep file size safe
            max_rows = 2000
            n = 0
            for it in items:
                if n >= max_rows:
                    break
                if not isinstance(it, dict):
                    continue
                ws.append([_cell(it.get(c)) for c in cols])
                n += 1

            # Save to bytes and persist as an artifact
            try:
                from io import BytesIO
                buf = BytesIO()
                wb.save(buf)
                xbytes = buf.getvalue()
            except Exception:
                xbytes = b""

            if xbytes:
                try:
                    entry = ctx.project_store.create_artifact_bytes(
                        current_project_full,
                        "deliverable_excel",
                        xbytes,
                        artifact_type="deliverable_excel",
                        from_files=list({str(it.get("source_file") or "").replace("\\", "/").strip() for it in items if isinstance(it, dict)}),
                        file_ext=".xlsx",
                        meta={"fallback": True, "rows_emitted": n},
                    )
                    pth = (entry.get("path") or "").replace("\\", "/").strip()
                    if pth:
                        created_paths_norm.append(pth)
                        excel_files = [p for p in created_paths_norm if "/artifacts/" in p and p.endswith(".xlsx")]
                except Exception:
                    pass
        except Exception:
            pass

    # Remove any Open: lines the model may have invented; we will add the real one.
    try:
        ua_lines = (user_answer or "").splitlines()
        ua_lines = [ln for ln in ua_lines if not ln.strip().lower().startswith("open:")]
        user_answer = "\n".join(ua_lines).strip()
    except Exception:
        pass

    link = ""
    if excel_files:
        link = f"/file?path={excel_files[-1]}"
    elif deliverables:
        link = f"/file?path={deliverables[-1]}"

    if link:
        user_answer = (user_answer.rstrip() + "\n\n" + "Open: " + link).strip()

        # C11 — Autopilot synthesis (deliverables; deterministic, truth-bound)
        try:
            if (not lookup_mode) and (excel_files or deliverables):
                created_path = (excel_files[-1] if excel_files else deliverables[-1]) or ""
                created_name = Path(created_path).name if created_path else "deliverable"
                user_answer = (
                    user_answer.rstrip()
                    + "\n\n"
                    + f"What changed: Created deliverable {created_name}.\n"
                    + "Why it matters: You can review/share the generated output directly.\n"
                    + "Next step: Open the link above and tell me what you want changed (if anything)."
                ).strip()
        except Exception:
            pass

    # If model forgot USER_ANSWER entirely, don't send an empty WS message.
    if not (user_answer or "").strip():
        # Construct a minimal, deterministic response from what we actually created.
        excel_link = ""
        html_link = ""
        if excel_files:
            excel_link = f"/file?path={excel_files[-1]}"
        elif deliverables:
            html_link = f"/file?path={deliverables[-1]}"

        if excel_link:
            user_answer = "Generated Excel.\n\nOpen: " + excel_link
        elif html_link:
            user_answer = "Generated deliverable.\n\nOpen: " + html_link
        else:
            user_answer = "Done. (No USER_ANSWER was emitted by the model, and no deliverable link was created.)"

    if not suppress_output_side_effects:
        ctx.snapshot_assistant_output(current_project_full, user_answer)

    # Don't pollute the rolling chat context with lookup turns.
    # This prevents repetitive "Project Pulse" or repeated answers on subsequent lookups.
    if (not suppress_output_side_effects) and (not lookup_mode):
        conversation_history.append({"role": "user", "content": clean_user_msg})
        conversation_history.append({"role": "assistant", "content": user_answer})
        if len(conversation_history) > (max_history_pairs * 2):
            conversation_history = conversation_history[-(max_history_pairs * 2):]
    if not suppress_output_side_effects:
        try:
            await _maybe_update_interpretive_memory(
                ctx=ctx,
                project_full=current_project_full,
                turn_index=int(turn_n or 0),
                window_text=window_text,
                assistant_text=user_answer,
            )
        except Exception:
            pass

    return {
        "user_answer": user_answer,
        "lookup_mode": lookup_mode,
        "conversation_history": conversation_history,
        "blocks": blocks if isinstance(blocks, dict) else {},
        "capability_gap": cap_gap_obj if isinstance(cap_gap_obj, dict) else None,
    }

