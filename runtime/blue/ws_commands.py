"""
WebSocket command router for Lens-0 (low-risk commands + routing).

Handles:
- plan/show plan
- open <filename>
- list/ls
- facts (quick-open)
- last answer (quick-open)
- capabilities summary
- registry + registry notes reset
- [FILE_ADDED] legacy control message
- [SEARCH]/[NOSEARCH] routing tuple for server-side web lookup
"""


from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple, Union
import capabilities

async def dispatch(
    *,
    ctx: Any,
    user_msg: str,
    lower: str,
    user: str,
    current_project: str,
    current_project_full: str,
    dashboard_for: Callable[[str], str],
    last_user_question_for_search: str,
) -> Optional[Union[str, Tuple[str, str, str]]]:
    """
    Return:
      - a response string if handled (server should send + continue)
      - a routing tuple ("__search__", mode, clean_user_msg) for [SEARCH]/[NOSEARCH]
      - None if not handled

    ctx is expected to be the server module (sys.modules[__name__]) and must expose:
      - load_manifest(project_full)
      - list_project_files(project_full)
      - get_latest_artifact_by_type(project_full, type_str)
      - handle_file_added(project_full, rel, current_project, dashboard_for)
      - handle_goal_command(project_full, goal_text, current_project, dashboard_for)

    dashboard_for(short_project_name) -> str should return the same dashboard text used by server.
    """
    # ------------------------------------------------------------
    # NOTE (C6): Pulse/Status/Resume must route through the unified
    # request pipeline controller (server/model_pipeline).
    # ws_commands.py must not answer these directly.
    # ------------------------------------------------------------

    # ------------------------------------------------------------
    # [SEARCH] routing (recognition only; server keeps the web logic)
    # ------------------------------------------------------------
    if lower.startswith("[nosearch]"):
        clean = user_msg[len("[nosearch]"):].strip()
        return ("__search__", "nosearch", clean)

    if lower.startswith("[search]"):
        clean = user_msg[len("[search]"):].strip()
        # If user typed something generic like "[SEARCH] look it up", fall back to their last real question.
        try:
            q = ctx.strip_lens0_system_blocks(clean)
            if ctx._is_generic_search_query(q) and last_user_question_for_search:
                clean = last_user_question_for_search
        except Exception:
            pass
        return ("__search__", "force", clean)

    # ---------------------------
    # [FILE_ADDED] routing/parse (legacy control message)
    # ---------------------------
    if lower.startswith("[file_added]"):
        rel = user_msg[len("[FILE_ADDED]"):].strip().replace("\\", "/")
        if not rel:
            return "Received [FILE_ADDED] with no path; skipping."
        return await ctx.handle_file_added(
            current_project_full=current_project_full,
            rel=rel,
            current_project=current_project,
            dashboard_for=dashboard_for,
        )

    # ------------------------------------------------------------
    # Explicit command gating (Expert Primacy)
    #
    # Only treat a message as a ws_command if it is explicitly
    # invoked with one of these prefixes:
    #   - !<command>      (example: !list, !plan)
    #   - /cmd <command>  (example: /cmd plan)
    #
    # Without these prefixes, the message MUST fall through to the
    # expert pipeline (server/model_pipeline).
    # ------------------------------------------------------------
    cmd_prefix = ""
    cmd_text = ""

    if user_msg.startswith("!"):
        cmd_prefix = "!"
        cmd_text = user_msg[1:].lstrip()
    elif user_msg.lower().startswith("/cmd"):
        cmd_prefix = "/cmd"
        cmd_text = user_msg[4:].lstrip()

    if cmd_prefix:
        user_msg = cmd_text
        lower = (cmd_text or "").lower().strip()
    else:
        return None

    # ----------------
    # goal: set / show
    # ----------------

    if lower in ("goal", "show goal", "goal?"):
        try:
            m = ctx.load_manifest(current_project_full)
            g = (m.get("goal") or "").strip()
        except Exception:
            g = ""
        return f"Goal: {g or 'Not set yet'}"

    if lower.startswith("goal:"):
        g = user_msg.split(":", 1)[1].strip()
        return await ctx.handle_goal_command(
            current_project_full=current_project_full,
            goal_text=g,
            current_project=current_project,
            dashboard_for=dashboard_for,
        )

    # Plan (explicit only):
    #   !plan
    #   /cmd plan
    if lower in ("plan", "show plan"):
        return dashboard_for(current_project)

    # Debug report (explicit only)
    #   !debug
    #   /cmd debug
    if lower in ("debug", "debug report"):
        return await ctx.handle_debug_command(
            current_project_full=current_project_full,
            current_project=current_project,
            user=user,
        )

    # Open a file by name and return a /file?path=... link
    # Usage: open <filename>
    if lower.startswith("open "):
        target_name = user_msg.split(" ", 1)[1].strip()
        if not target_name:
            return "Usage: open <filename>"

        m = ctx.load_manifest(current_project_full)
        picked_rel = ""

        # Check raw files first (original name or stored name)
        for rf in (m.get("raw_files") or []):
            orig = (rf.get("orig_name") or "").strip()
            saved = (rf.get("saved_name") or "").strip()
            if target_name == orig or target_name == saved:
                picked_rel = (rf.get("path") or "").replace("\\", "/").strip()
                break

        # Fallback: check artifacts by filename
        if not picked_rel:
            for a in (m.get("artifacts") or []):
                fn = (a.get("filename") or "").strip()
                if target_name == fn:
                    picked_rel = (a.get("path") or "").replace("\\", "/").strip()
                    break

        if not picked_rel:
            return f"Not found in this project: {target_name}"

        return f"/file?path={picked_rel}"
    # ------------------------------------------------------------
    # Import: chat log -> Tier-1 facts_raw.jsonl
    # Usage:
    #   !facts import <filename>
    # ------------------------------------------------------------
    if lower.startswith("facts import "):
        target_name = user_msg.split(" ", 2)[2].strip() if len(user_msg.split(" ", 2)) >= 3 else ""
        if not target_name:
            return "Usage: facts import <filename>"

        m = ctx.load_manifest(current_project_full)
        picked_rel = ""

        # Find raw upload by orig or saved name
        for rf in (m.get("raw_files") or []):
            orig = (rf.get("orig_name") or "").strip()
            saved = (rf.get("saved_name") or "").strip()
            relp = (rf.get("path") or "").replace("\\", "/").strip()
            if target_name == orig or target_name == saved or target_name == (relp.split("/")[-1] if relp else ""):
                picked_rel = relp
                break

        if not picked_rel:
            return f"Not found in this project raw files: {target_name}"

        try:
            out = ctx.ingest_chatlog_file_to_facts_raw(
                current_project_full,
                raw_rel_path=picked_rel,
            )
        except Exception as e:
            return f"facts import failed: {e!r}"

        if not isinstance(out, dict) or out.get("ok") is not True:
            return f"facts import failed: {out}"

        # Return a short receipt; the raw file itself is in state/
        fw = int(out.get("facts_written") or 0)
        ch = int(out.get("chunks") or 0)
        return f"Imported facts: {fw} (chunks: {ch}). Tier-1 stored at state/facts_raw.jsonl"

    # Quick open: latest Facts Map
    # Normalize Tier-1 raw facts in place
    # ------------------------------------------------------------
    # Couples therapist commands (explicit only)
    #   !couple link <user_a> | <user_b>
    #   !couple use <couple_id>
    # ------------------------------------------------------------
    if lower.startswith("couple link "):
        if user != "Therapist":
            return "Forbidden: only Therapist can link couples."

        rest = user_msg.split(" ", 2)[2].strip() if len(user_msg.split(" ", 2)) >= 3 else ""
        if "|" not in rest:
            return "Usage: !couple link <user_a> | <user_b>"

        a, b = [x.strip() for x in rest.split("|", 1)]
        if not a or not b:
            return "Usage: !couple link <user_a> | <user_b>"

        try:
            rec = ctx.project_store.link_couple(user_a=a, user_b=b, project_a=current_project, project_b=current_project)
        except Exception as e:
            return f"couple link failed: {e!r}"

        couple_id = (rec.get("couple_id") or "").strip()
        # auto-select it for convenience
        try:
            ctx.project_store.write_project_state_fields(current_project_full, {"active_couple_id": couple_id})
        except Exception:
            pass

        return f"Linked: {a} ↔ {b}\nActive couple_id: {couple_id}"

    if lower.startswith("couple use "):
        if user != "Therapist":
            return "Forbidden: only Therapist can select an active couple."

        couple_id = user_msg.split(" ", 2)[2].strip() if len(user_msg.split(" ", 2)) >= 3 else ""
        if not couple_id:
            return "Usage: !couple use <couple_id>"

        rec = None
        try:
            rec = ctx.project_store.get_couple(couple_id)
        except Exception:
            rec = None

        if not isinstance(rec, dict) or (rec.get("status") != "active"):
            return f"Not found or inactive: {couple_id}"

        try:
            ctx.project_store.write_project_state_fields(current_project_full, {"active_couple_id": couple_id})
        except Exception as e:
            return f"Failed to set active couple: {e!r}"

    # ------------------------------------------------------------
    # Couples Mode Option A — Bring-up-later queue (explicit only)
    #
    # Commands:
    #   !bringup add <topic> | <tone> | <boundaries> | <urgency(optional)>
    #   !bringup resolve <id>
    # ------------------------------------------------------------
    if lower.startswith("bringup add "):
        # Only couple_* accounts may use bringups
        try:
            me = ctx.safe_user_name(user)
        except Exception:
            me = ""
        if not (me or "").lower().startswith("couple_"):
            return "Forbidden: bringup is only available in couple_* accounts."

        rest = user_msg.split(" ", 2)[2].strip() if len(user_msg.split(" ", 2)) >= 3 else ""
        parts = [p.strip() for p in rest.split("|")]
        if len(parts) < 3:
            return "Usage: !bringup add <topic> | <tone> | <boundaries> | <urgency(optional)>"

        topic = parts[0]
        tone = parts[1]
        boundaries = parts[2]
        urgency = parts[3] if len(parts) >= 4 else ""

        # Find partner from couples_links (active)
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
            if me == ua or me == ub:
                rec = obj
                break

        if not isinstance(rec, dict):
            return "No active couple link found for this couple_* account. (Therapist must run: !couple link ...)"

        ua = ctx.safe_user_name(str(rec.get("user_a") or ""))
        ub = ctx.safe_user_name(str(rec.get("user_b") or ""))
        partner = (ub if me == ua else ua)

        try:
            entry = ctx.project_store.append_bringup_request(
                current_project_full,
                from_user=me,
                to_user=partner,
                topic=topic,
                tone=tone,
                boundaries=boundaries,
                urgency=urgency,
            )
        except Exception as e:
            return f"bringup add failed: {e!r}"

        bid = str(entry.get("id") or "").strip()
        return f"Bring-up queued. id={bid}"

    if lower.startswith("bringup resolve "):
        try:
            me = ctx.safe_user_name(user)
        except Exception:
            me = ""
        if not (me or "").lower().startswith("couple_"):
            return "Forbidden: bringup is only available in couple_* accounts."

        bid = user_msg.split(" ", 2)[2].strip() if len(user_msg.split(" ", 2)) >= 3 else ""
        if not bid:
            return "Usage: !bringup resolve <id>"

        try:
            ok = ctx.project_store.resolve_bringup_request(current_project_full, bringup_id=bid)
        except Exception as e:
            return f"bringup resolve failed: {e!r}"

        return f"Bring-up resolved: {bid}" if ok else f"Bring-up resolve failed: {bid}"

    if lower == "facts normalize":
        try:
            out = ctx.project_store.normalize_facts_raw_jsonl(current_project_full)
        except Exception as e:
            return f"facts normalize failed: {e!r}"

        if not isinstance(out, dict) or not out.get("ok"):
            return f"facts normalize failed: {out}"

        return f"facts_raw normalized: kept={out.get('written')} dropped={out.get('dropped')}"

    # Global Tier-2G rebuild (profile.json) + Tier-2M rebuild (global_facts_map.json)
    # Usage:
    #   !t2g rebuild
    if lower in ("t2g rebuild", "tier2g rebuild"):
        try:
            u = ctx.safe_user_name(user)
        except Exception:
            u = ""
        if not u:
            return "t2g rebuild failed: missing user"
        try:
            prof = ctx.project_store.rebuild_user_profile_from_user_facts(u)
        except Exception as e:
            return f"t2g rebuild failed: {e!r}"
        try:
            ctx.project_store.rebuild_user_global_facts_map_from_user_facts(u)
        except Exception:
            # Tier-2M failure should not block Tier-2G rebuild
            pass
        ok_schema = isinstance(prof, dict) and str(prof.get("schema") or "") == "user_profile_v1"
        if not ok_schema:
            return "t2g rebuild failed: invalid profile schema"
        return (
            "t2g rebuild ok\n"
            f"- profile: {ctx.project_store.user_profile_path(u)}\n"
            f"- facts_raw: {ctx.project_store.user_facts_raw_path(u)}\n"
            f"- global_facts_map: {ctx.project_store.user_global_facts_map_path(u)}"
        )
    # Tier-2 distillation: promote Tier-1 facts_raw.jsonl -> facts_map.md (canonical)
    # Usage:
    #   !facts distill therapist
    #   !facts distill programmer
    #   !facts distill general
    if lower.startswith("facts distill"):
        parts = (user_msg or "").strip().split()
        profile = parts[2].strip().lower() if len(parts) >= 3 else ""

        # Aliases
        if profile == "therapy":
            profile = "therapist"

        if profile not in ("therapist", "programmer", "general"):
            return "Usage: facts distill therapist|programmer|general"

        try:
            out = ctx.project_store.distill_facts_raw_to_facts_map(current_project_full, profile=profile)
        except Exception as e:
            return f"facts distill failed: {e!r}"

        if not isinstance(out, dict) or out.get("ok") is not True:
            return f"facts distill failed: {out}"

        return (
            f"facts distilled ({profile}): promoted={int(out.get('promoted') or 0)} "
            f"kept_existing={int(out.get('kept_existing') or 0)} "
            f"dropped={int(out.get('dropped') or 0)}"
        )

    if lower in ("facts", "facts map", "facts_map"):
        latest = ctx.get_latest_artifact_by_type(current_project_full, "facts_map")
        rel = (latest.get("path") or "").replace("\\", "/").strip() if isinstance(latest, dict) else ""
        if not rel:
            return "No facts_map artifacts yet. Send 'Durable facts:' first."
        return f"/file?path={rel}"

    # Quick open: latest Assistant Output (snapshotted answer)
    if lower in ("last answer", "last assistant", "last output", "last assistant output"):
        latest = ctx.get_latest_artifact_by_type(current_project_full, "assistant_output")
        rel = (latest.get("path") or "").replace("\\", "/").strip() if isinstance(latest, dict) else ""
        if rel:
            return f"/file?path={rel}"

        # Fallback (continuity guarantee): if no assistant_output exists yet,
        # synthesize a truth-bound resume snapshot and store it as an assistant_output
        # so the UI can always restore context.
        try:
            pulse = ctx.project_store.build_truth_bound_pulse(current_project_full)
        except Exception:
            pulse = "Project Pulse (truth-bound)\n\nGoal: (not set yet)"

        try:
            ctx.project_store.create_artifact(
                current_project_full,
                "assistant_output_resume",
                pulse,
                artifact_type="assistant_output",
                from_files=[],
                file_ext=".md",
                write_errors="strict",
                meta={"source": "ws_command_fallback", "kind": "truth_bound_pulse"},
            )
        except Exception:
            pass

        # Return the pulse inline (prevents UI from showing a filename + a second message).
        return pulse
    # Capabilities registry (human-readable summary)
    if lower in ("capabilities", "caps"):
        return capabilities.render_human_summary()

    # System registry (repo scan -> project artifacts)
    if lower in ("registry", "system registry", "code registry"):
        try:
            out = ctx.generate_system_registry(current_project_full)

            md_rel = (out.get("md_rel") or "").replace("\\", "/").strip()
            notes_rel = (out.get("notes_rel") or "").replace("\\", "/").strip()
            json_rel = (out.get("json_rel") or "").replace("\\", "/").strip()

            parts = []
            if md_rel:
                parts.append(f"Registry: /file?path={md_rel}")
            if notes_rel:
                parts.append(f"Notes: /file?path={notes_rel}")
            if json_rel:
                parts.append(f"JSON: /file?path={json_rel}")

            if parts:
                return "\n".join(parts)

            return "Registry generated, but no output path was returned."
        except Exception as e:
            return f"Registry generation failed: {e!r}"
    # Force-regenerate the notes template (explicit; overwrites system_registry_notes.md)
    if lower in ("registry notes reset", "registry reset notes", "reset registry notes"):
        try:
            out = ctx.generate_system_registry(current_project_full)

            notes_rel = (out.get("notes_rel") or "").replace("\\", "/").strip()
            if not notes_rel:
                return "Notes reset failed: notes_rel missing."

            reg = ctx.registry_builder.build_system_registry(ctx.PROJECT_ROOT)
            txt = ctx.registry_builder.registry_notes_template(reg)

            abs_notes = (ctx.PROJECT_ROOT / notes_rel).resolve()
            ctx.atomic_write_text(abs_notes, txt)

            return f"Notes: /file?path={notes_rel}"
        except Exception as e:
            return f"Notes reset failed: {e!r}"
    # -----------------------------
    # Extraction Ledger (deterministic)
    # -----------------------------
    if lower in ("ledger", "extract ledger", "build ledger", "refresh ledger"):
        try:
            out = ctx.project_store.build_extraction_ledger_and_write(current_project_full)

            json_rel = (out.get("json_rel") or "").replace("\\", "/").strip()
            json_latest_rel = (out.get("json_latest_rel") or "").replace("\\", "/").strip()
            csv_rel = (out.get("csv_rel") or "").replace("\\", "/").strip()

            parts = []
            parts.append(f"Ledger items: {int(out.get('item_count') or 0)}")

            # Always return clickable links (no guessing filenames)
            if json_latest_rel:
                parts.append(f"Latest: /file?path={json_latest_rel}")
            if json_rel:
                parts.append(f"Versioned: /file?path={json_rel}")
            if csv_rel:
                parts.append(f"CSV: /file?path={csv_rel}")

            return "\n".join(parts).strip()

        except Exception as e:
            return f"Ledger build failed: {e!r}"

        
    # List current project contents
    if lower in ("list", "list project", "list files", "list docs", "ls"):
        data = ctx.list_project_files(current_project_full)
        lines = [f"Project: {current_project}", ""]

        # -------------------------
        # Deliverables (user-facing)
        # -------------------------
        deliverables = data.get("deliverables") or []
        lines.append("Deliverables:")
        if deliverables:
            dels = sorted(deliverables, key=lambda a: a.get("version", 0), reverse=True)
            for e in dels[:25]:
                fn = e.get("filename")
                typ = e.get("type")
                ver = e.get("version")
                rel = (e.get("relative_path") or "").replace("\\", "/").strip()
                if rel:
                    lines.append(f"- {fn} [type={typ}, v{ver}]")
                    lines.append(f"  /file?path={rel}")
                else:
                    lines.append(f"- {fn} [type={typ}, v{ver}]")
        else:
            lines.append("- (none)")

        # -----------------------------------------
        # Latest key project docs (one per type)
        # -----------------------------------------
        latest = data.get("artifacts_latest") or []
        if latest:
            latest_by_type: Dict[str, Dict[str, Any]] = {}
            for e in latest:
                typ = str(e.get("type") or "").strip()
                if typ:
                    latest_by_type[typ] = e

            preferred = ("project_map", "decision_log", "working_doc", "facts_map", "project_state")
            lines.append("")
            lines.append("Latest project docs:")
            any_printed = False
            for typ in preferred:
                e = latest_by_type.get(typ)
                if not e:
                    continue
                fn = e.get("filename")
                ver = e.get("version")
                rel = (e.get("relative_path") or "").replace("\\", "/").strip()
                if rel:
                    lines.append(f"- {fn} [type={typ}, v{ver}]")
                    lines.append(f"  /file?path={rel}")
                else:
                    lines.append(f"- {fn} [type={typ}, v{ver}]")
                any_printed = True
            if not any_printed:
                lines.append("- (none)")
        else:
            lines.append("")
            lines.append("Latest project docs:")
            lines.append("- (none)")

        # -------------------------
        # Raw uploads (clickable)
        # -------------------------
        lines.append("")
        lines.append("Raw files:")
        if data.get("raw"):
            # Dedupe by relative_path (multiple original names can map to same canonical file)
            by_rel: Dict[str, Dict[str, Any]] = {}
            for e in data["raw"]:
                rel = (e.get("relative_path") or "").replace("\\", "/").strip()
                fn = (e.get("filename") or "").strip()
                orig = (e.get("original_name") or "").strip()
                if not rel:
                    rel = f"__missing_rel__:{fn or orig}"
                bucket = by_rel.get(rel)
                if not bucket:
                    by_rel[rel] = {"rel": rel, "stored": fn, "originals": [orig] if orig else []}
                else:
                    if orig and orig not in bucket["originals"]:
                        bucket["originals"].append(orig)
                    if (not bucket.get("stored")) and fn:
                        bucket["stored"] = fn

            # Stable display order
            for rel in sorted(by_rel.keys()):
                b = by_rel[rel]
                stored = (b.get("stored") or "").strip()
                originals = b.get("originals") or []
                # User-facing: show ONE name. Prefer stored filename.
                display = stored or (originals[0] if originals else "(unknown)")
                if rel.startswith("__missing_rel__:"):
                    lines.append(f"- {display}")
                    continue
                lines.append(f"- {display}")
                lines.append(f"  /file?path={rel}")
        else:
            lines.append("- (none)")

        return "\n".join(lines)
    # Decisions (C2): read-only list of final decisions + pending candidates (truth-bound)
    if lower == "decisions":
        try:
            items = ctx.load_decisions(current_project_full) or []
        except Exception:
            items = []

        # Pending candidates (truth-bound)
        try:
            cands = ctx.project_store.load_decision_candidates(current_project_full) or []
        except Exception:
            cands = []
        pending_txts = []
        for it in (cands or []):
            if not isinstance(it, dict):
                continue
            if str(it.get("status") or "").strip().lower() != "pending":
                continue
            t = str(it.get("text") or "").strip()
            if t:
                pending_txts.append(t)

        lines = ["Decisions (confirmed):"]
        if not items:
            lines.append("- (none)")
        else:
            tail = items[-50:]
            for it in tail:
                if not isinstance(it, dict):
                    continue
                ts = str(it.get("timestamp") or "").strip()
                day = ts.split("T", 1)[0] if "T" in ts else ts[:10]
                txt = str(it.get("text") or "").strip()
                if not txt:
                    continue
                lines.append(f"- [{day}] {txt}")

        lines.append("")
        lines.append("Pending / unconfirmed:")
        if not pending_txts:
            lines.append("- (none)")
        else:
            for t in pending_txts[-25:]:
                lines.append(f"- {t}")

        return "\n".join(lines)
    # Upload notes (C4/C5): read-only dump (chronological, minimal formatting)
    if lower == "notes":
        try:
            items = ctx.load_upload_notes(current_project_full) or []
        except Exception:
            items = []

        lines = ["Upload notes:"]
        if not items:
            lines.append("- (none)")
            return "\n".join(lines)

        tail = items[-80:]
        for it in tail:
            if not isinstance(it, dict):
                continue
            up = str(it.get("upload_path") or "").strip()
            ans = str(it.get("answer") or "")
            if up:
                lines.append(f"- {up}: {ans}")
            else:
                lines.append(f"- {ans}")

        return "\n".join(lines)

    # Memory (C5): show a bounded snapshot from all three sources
    if lower == "memory":
        out = ["Memory (stored):", ""]

        # Notes
        try:
            notes = ctx.load_upload_notes(current_project_full) or []
        except Exception:
            notes = []
        out.append("Upload notes:")
        if notes:
            for it in notes[-10:]:
                if not isinstance(it, dict):
                    continue
                up = str(it.get("upload_path") or "").strip()
                ans = str(it.get("answer") or "")
                if up:
                    out.append(f"- {up}: {ans}")
                else:
                    out.append(f"- {ans}")
        else:
            out.append("- (none)")

        out.append("")
        # Decisions
        try:
            decs = ctx.load_decisions(current_project_full) or []
        except Exception:
            decs = []
        out.append("Decisions:")
        if decs:
            for it in decs[-10:]:
                if not isinstance(it, dict):
                    continue
                txt = str(it.get("text") or "").strip()
                if txt:
                    out.append(f"- {txt}")
        else:
            out.append("- (none)")

        out.append("")
        # Deliverables
        try:
            reg = ctx.load_deliverables(current_project_full) or {}
        except Exception:
            reg = {}
        items = reg.get("items") if isinstance(reg, dict) else None
        out.append("Deliverables:")
        if isinstance(items, list) and items:
            for it in items[-10:]:
                if not isinstance(it, dict):
                    continue
                title = str(it.get("title") or "").strip()
                path = str(it.get("path") or "").replace("\\", "/").strip()
                if title and path:
                    out.append(f"- {title}")
                    out.append(f"  /file?path={path}")
                elif path:
                    out.append(f"- /file?path={path}")
        else:
            out.append("- (none)")

        return "\n".join(out)

    return None
