# -*- coding: utf-8 -*-
"""
capabilities.py

Code-backed Capabilities Registry for Lens-0 Project OS.

Purpose:
- Provide a stable, always-available map of "feature -> where implemented"
  without reading the entire repo.

Notes:
- This module MUST be pure (no side effects) and safe to import from both
  HTTP and WS layers.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict
from dataclasses import field
from typing import Any, Dict, List


@dataclass(frozen=True)
class Capability:
    id: str
    name: str
    purpose: str
    entrypoints: List[str]
    implementation: List[str]
    state_and_artifacts: List[str]

    # Optional structured metadata (does not require updating existing entries).
    # Keep this small and stable; it’s used for capability reasoning + enforcement.
    meta: Dict[str, Any] = field(default_factory=dict)


_REGISTRY: List[Capability] = [
    Capability(
        id="http.health",
        name="Health & runtime info",
        purpose="Expose server health, ports, model, runtime color, and basic environment flags.",
        entrypoints=["HTTP GET /health"],
        implementation=[
            "http_api.register_routes() -> handle_health()",
        ],
        state_and_artifacts=[
            "Reads: runtime/active.json (recommended active color)",
            "Reads: server file bytes (sha256)",
        ],
    ),
    Capability(
        id="http.file",
        name="Read-only file serving",
        purpose="Serve/download raw bytes from disk for authorized user paths.",
        entrypoints=["HTTP GET /file?path=..."],
        implementation=[
            "http_api.register_routes() -> handle_get_file()",
        ],
        state_and_artifacts=[
            "Reads: projects/<user>/<project>/** files on disk",
            "Enforces: path must resolve under projects/<user>/",
        ],
    ),
    Capability(
        id="http.upload",
        name="HTTP file upload (enqueue processing)",
        purpose="Accept multipart upload, store into raw/, register in manifest, enqueue async processing via WS worker.",
        entrypoints=["HTTP POST /upload?project=<short_name> (multipart field: file)"],
        implementation=[
            "http_api.register_routes() -> handle_file_upload()",
            "project_store.register_raw_file()",
            "server._upload_worker_loop() (async background processing)",
            "project_store.ingest_uploaded_file()",
        ],
        state_and_artifacts=[
            "Writes: projects/<user>/<project>/raw/<timestamp>_<safe_name>",
            "Writes: project_manifest.json (raw_files entry)",
            "Writes: state/assets_index.jsonl (queued/ready/failed)",
            "Writes artifacts: file_overview*, pdf_text*, ocr_text*, image_caption*, image_classification*, excel_blueprint*, code_index*, code_chunk*",
        ],
    ),
    Capability(
        id="http.manifest",
        name="Project manifest read/write",
        purpose="Fetch or update project_manifest.json (full replace or patch dict).",
        entrypoints=["HTTP GET /manifest?project=<short_name>", "HTTP POST /manifest (json)"],
        implementation=[
            "http_api.register_routes() -> handle_get_manifest(), handle_update_manifest()",
            "project_store.load_manifest(), project_store.save_manifest()",
        ],
        state_and_artifacts=[
            "Reads/Writes: projects/<project>/project_manifest.json",
        ],
    ),
    Capability(
        id="http.projects",
        name="Project listing and deletion",
        purpose="List projects for an authenticated user; delete a non-default project folder.",
        entrypoints=["HTTP GET /list_projects", "HTTP POST /delete_project (json)"],
        implementation=[
            "http_api.register_routes() -> handle_list_projects(), handle_delete_project()",
            "project_store.list_existing_projects()",
        ],
        state_and_artifacts=[
            "Reads: projects/<user>/",
            "Deletes: projects/<user>/<project>/ (non-default only)",
        ],
    ),
    Capability(
        id="http.classify",
        name="Generic text classification via model",
        purpose="One-off classification endpoint returning label + explanation (strict JSON preference).",
        entrypoints=["HTTP POST /classify (json: text, optional labels/system)"],
        implementation=[
            "http_api.register_routes() -> handle_classify()",
            "server.call_openai_chat()",
        ],
        state_and_artifacts=[
            "No required disk writes (pure request/response).",
        ],
    ),
    Capability(
        id="http.excel.generate",
        name="Generate Excel from spec",
        purpose="Validate an Excel spec, generate .xlsx bytes, and store as deliverable artifact.",
        entrypoints=["HTTP POST /generate_excel (json: spec)"],
        implementation=[
            "http_api.register_routes() -> handle_generate_excel()",
            "server.validate_excel_spec_master(), server.maybe_repair_excel_spec_once_master()",
            "server.generate_workbook_from_spec()",
            "project_store.create_artifact_bytes()",
        ],
        state_and_artifacts=[
            "Writes: projects/<user>/<project>/artifacts/<name>_vN.xlsx",
            "Updates: project_manifest.json (artifacts entry)",
        ],
    ),
    Capability(
        id="http.capabilities",
        name="Capabilities Registry (JSON)",
        purpose="Return the code-backed registry so feature -> module mapping is always visible.",
        entrypoints=["HTTP GET /capabilities"],
        implementation=[
            "http_api.register_routes() -> handle_get_capabilities()",
            "capabilities.get_registry_json()",
        ],
        state_and_artifacts=[
            "No disk writes (pure read).",
        ],
    ),
    Capability(
        id="ws.chat",
        name="WebSocket chat (frames + legacy)",
        purpose="Accept WS v1 frames (preferred) or legacy raw strings, route commands, then run Project OS or Lookup mode.",
        entrypoints=[
            'WS frame: {"v":1,"type":"chat.send","text":"...","project":"optional"}',
            "WS legacy: raw string message",
        ],
        implementation=[
            "server.handle_connection()",
            "server.model_refresh_state(), server.call_openai_chat(), server.parse_model_output()",
            "server.persist_state_blocks(), server.snapshot_assistant_output()",
        ],
        state_and_artifacts=[
            "Writes: state/chat_log.jsonl",
            "Writes: state docs (project_state.json, facts_map.md, etc.)",
            "Writes: artifacts (deliverable_html, assistant_output, project_state, etc.)",
        ],
    ),
    Capability(
        id="ws.commands.basic",
        name="WS basic commands (no model call)",
        purpose="Handle low-risk commands like plan/list/open/goal/facts/last answer plus [SEARCH]/[NOSEARCH] routing and [FILE_ADDED].",
        entrypoints=[
            "WS text: plan",
            "WS text: list | ls",
            "WS text: open <filename>",
            "WS text: goal / goal: ...",
            "WS text: facts / last answer",
            "WS text: capabilities",
            "WS control: [FILE_ADDED]<relative_path>",
            "WS routing: [SEARCH]... / [NOSEARCH]...",
        ],
        implementation=[
            "ws_commands.dispatch()",
            "server.handle_file_added(), server.handle_goal_command()",
        ],
        state_and_artifacts=[
            "Reads: project_manifest.json (for open/list/goal)",
            "Triggers: async ingest pipeline via server.handle_file_added()",
        ],
    ),
    Capability(
        id="memory.profile.binding",
        name="Profile gap questions + deterministic binding",
        purpose="Ask targeted profile questions and bind the next user reply to Tier-2G facts without magic phrasing.",
        entrypoints=[
            "WS text: 'what info do you need from me' (deterministic trigger)",
            "Auto: pending_profile_question consumption on next user reply",
        ],
        implementation=[
            "server._maybe_start_profile_gap_questions(), server._consume_pending_profile_question()",
            "project_store.append_user_fact_raw_candidate(), project_store.rebuild_user_profile_from_user_facts()",
        ],
        state_and_artifacts=[
            "Reads: runtime/<color>/memory_schema.json",
            "Writes: projects/<user>/_user/pending_profile_question.json",
            "Writes: projects/<user>/_user/facts_raw.jsonl",
            "Writes: projects/<user>/_user/profile.json",
        ],
    ),
    Capability(
        id="ws.upload.status",
        name="WS upload status notifications",
        purpose="Broadcast upload queue/processing/done/error updates to clients per project.",
        entrypoints=[
            'WS frame (server->UI): {"v":1,"type":"upload.status",...}',
        ],
        implementation=[
            "server._upload_worker_loop()",
            "server._broadcast_to_project(), server._ws_frame()",
        ],
        state_and_artifacts=[
            "Writes: state/assets_index.jsonl",
            "Writes: artifacts from project_store.ingest_uploaded_file()",
        ],
    ),
    Capability(
        id="ws.search",
        name="Web search integration (Brave)",
        purpose="Perform Brave search when explicitly requested or when heuristics deem it necessary, then inject results into model context.",
        entrypoints=[
            "WS text: [SEARCH] <query>",
            "Auto: server.should_auto_web_search() gate",
        ],
        implementation=[
            "ws_commands.dispatch() routing tuple ('__search__', ...)",
            "server.brave_search(), server.should_auto_web_search(), server.strip_lens0_system_blocks()",
        ],
        state_and_artifacts=[
            "No required disk writes (search results injected into model context only).",
        ],
    ),
    Capability(
        id="analysis.capability_gaps",
        name="Capability gap reporting",
        purpose="Record blocked/partial capability gaps and suggested features for later remediation.",
        entrypoints=[
            "Model output block: [CAPABILITY_GAP_JSON]",
        ],
        implementation=[
            "model_pipeline.run_request_pipeline() gap capture",
            "project_store.append_capability_gap_entry()",
            "project_store.build_capability_gap_views_and_write()",
        ],
        state_and_artifacts=[
            "Writes: state/capability_gaps.jsonl",
            "Writes: state/capability_gaps.md",
            "Writes: artifacts/capability_gaps_latest_v*.md",
        ],
    ),
    Capability(
        id="ws.patch.mode",
        name="Anchor-validated patch mode",
        purpose="Generate strict anchor patches against ingested code using uniqueness validation and optional AST parse checks.",
        entrypoints=[
            "WS text: patch ... (explicit triggers)",
            "WS text includes: replace/insert anchors instructions",
        ],
        implementation=[
            "server.generate_anchor_validated_patch()",
            "patch_engine.validate_patch_text_strict_format(), patch_engine.apply_anchor_patch_to_text(), etc.",
        ],
        state_and_artifacts=[
            "Writes: patch artifacts (patch_*.txt and optional *_diff.patch) in project artifacts",
            "Reads: ingested code artifacts (code_index/code_chunk) via project_store.ensure_ingested_current()",
        ],
    ),
    Capability(
        id="ws.selfpatch",
        name="Blue/Green self-patching (server edits)",
        purpose="Propose/stage/test a server patch into inactive runtime color, health-check, then switch recommended active.",
        entrypoints=[
            "WS text: /selfpatch ...",
            "WS text: APPROVE PATCH <id> | RETRY PATCH <id> | SWITCH <id> | SWITCH_AND_STOP <id> | SELF PATCH STATUS | STOP <blue|green|me>",
        ],
        implementation=[
            "server.propose_self_patch(), server._stage_and_start_self_patch(), server.handle_self_patch_command()",
        ],
        state_and_artifacts=[
            "Writes: patches/selfpatch_<id>__*.patch and *_anchor.txt",
            "Writes: runtime/self_patch_state.json",
            "Writes: runtime/<color>/server.pid and runtime/<color>/logs/server_<id>.log",
            "Writes: runtime/active.json (recommended active color)",
        ],
    ),
]

def _norm_id(s: str) -> str:
    return str(s or "").strip()

def validate_registry(*, required_ids: List[str] | None = None) -> List[str]:
    """
    Deterministic validation of the code-backed capabilities registry.
    Returns a list of human-readable errors. Empty list means OK.

    IMPORTANT:
    - This does NOT scan the repo.
    - It enforces internal consistency + required capability presence.
    - Repo-wide completeness checks can be added later by passing discovered surfaces.
    """
    errs: List[str] = []

    seen: set[str] = set()
    for i, c in enumerate(_REGISTRY):
        cid = _norm_id(getattr(c, "id", ""))
        if not cid:
            errs.append(f"registry[{i}] missing id")
            continue
        if cid in seen:
            errs.append(f"duplicate capability id: {cid}")
        seen.add(cid)

        if not _norm_id(getattr(c, "name", "")):
            errs.append(f"{cid}: missing name")
        if not _norm_id(getattr(c, "purpose", "")):
            errs.append(f"{cid}: missing purpose")

        eps = getattr(c, "entrypoints", None)
        impl = getattr(c, "implementation", None)
        st = getattr(c, "state_and_artifacts", None)

        if not isinstance(eps, list) or not any(str(x).strip() for x in eps):
            errs.append(f"{cid}: entrypoints empty")
        if not isinstance(impl, list) or not any(str(x).strip() for x in impl):
            errs.append(f"{cid}: implementation empty")
        if not isinstance(st, list) or not any(str(x).strip() for x in st):
            errs.append(f"{cid}: state_and_artifacts empty")

    req = [_norm_id(x) for x in (required_ids or []) if _norm_id(x)]
    for rid in req:
        if rid not in seen:
            errs.append(f"missing required capability: {rid}")

    return errs

def smoke_test_registry(*, required_ids: List[str]) -> tuple[bool, str]:
    errs = validate_registry(required_ids=required_ids)
    if errs:
        msg = "capabilities registry FAILED:\n- " + "\n- ".join(errs[:40])
        return False, msg
    return True, "ok"

def get_registry() -> List[Capability]:
    return list(_REGISTRY)


def get_registry_json() -> List[Dict[str, Any]]:
    return [asdict(c) for c in _REGISTRY]


def render_human_summary(max_items: int = 200) -> str:
    lines: List[str] = []
    lines.append("Capabilities Registry:")
    lines.append("")
    for c in _REGISTRY[: max(0, int(max_items))]:
        eps = ", ".join(c.entrypoints[:4])
        if len(c.entrypoints) > 4:
            eps = eps + ", ..."
        lines.append(f"- {c.id}: {c.name}")
        lines.append(f"  - Purpose: {c.purpose}")
        lines.append(f"  - Entrypoints: {eps}")
    return "\n".join(lines).strip()
