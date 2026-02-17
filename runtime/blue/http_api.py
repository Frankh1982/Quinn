# -*- coding: utf-8 -*-
"""
HTTP API extraction for Lens-0 server.

Design:
- No import of server.py (avoids circular imports).
- server.py passes ctx=sys.modules[__name__] to register_routes().
- All handler logic lives here and uses ctx.<name> for shared functions/constants.
"""

from __future__ import annotations

import asyncio
import json
import re
import shutil
import time
from pathlib import Path
from typing import Any, Dict, Optional

from aiohttp import web

import excel_engine
import project_store


def register_routes(app: web.Application, *, ctx) -> None:
    """
    Register aiohttp routes on the given app.

    ctx is expected to be the running server module (sys.modules[__name__]) so we can
    access server-side helpers without importing server.py.
    """

    async def handle_root(request: web.Request) -> web.StreamResponse:
        try:
            ui_path = getattr(ctx, "UI_FILE", None)
            if not ui_path:
                ui_path = (ctx.PROJECT_ROOT / "Project Web 2 - BLUE.html").resolve()
            if not isinstance(ui_path, Path):
                ui_path = Path(str(ui_path)).expanduser().resolve()

            if not ui_path.exists():
                return web.Response(
                    status=500,
                    text=f"UI file not found: {ui_path}",
                    content_type="text/plain",
                )

            resp = web.FileResponse(path=ui_path)
            resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
            resp.headers["Pragma"] = "no-cache"
            resp.headers["Expires"] = "0"
            return resp
        except Exception as e:
            return web.Response(
                status=500,
                text=f"UI handler error: {e!r}",
                content_type="text/plain",
            )

    async def handle_get_manifest(request: web.Request) -> web.Response:
        user = ctx.get_request_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        project_param = (request.rel_url.query.get("project") or "").strip()
        project_short = ctx.safe_project_name(project_param) if project_param else ctx.DEFAULT_PROJECT_NAME
        project_full = ctx.safe_project_name(f"{user}/{project_short}")

        ctx.ensure_project_scaffold(project_full)
        manifest = ctx.load_manifest(project_full)

        resp = web.json_response({"ok": True, "project": project_short, "manifest": manifest})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    async def handle_update_manifest(request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except Exception:
            resp = web.json_response({"ok": False, "error": "Expected JSON body."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        user = ctx.get_request_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        project_param = (payload.get("project") or "").strip() if isinstance(payload, dict) else ""
        project_short = ctx.safe_project_name(project_param) if project_param else ctx.DEFAULT_PROJECT_NAME
        project_full = ctx.safe_project_name(f"{user}/{project_short}")
        ctx.ensure_project_scaffold(project_full)

        current = ctx.load_manifest(project_full)

        full_manifest = payload.get("manifest") if isinstance(payload, dict) else None
        patch = payload.get("patch") if isinstance(payload, dict) else None

        if isinstance(full_manifest, dict):
            new_m = dict(full_manifest)
            new_m.setdefault("version", ctx.MANIFEST_VERSION)
            new_m.setdefault("project_name", project_short)
            new_m.setdefault("created_at", current.get("created_at", time.time()))
            new_m.setdefault("updated_at", time.time())
            new_m.setdefault("goal", current.get("goal", ""))
            new_m.setdefault("raw_files", current.get("raw_files", []))
            new_m.setdefault("artifacts", current.get("artifacts", []))
            new_m.setdefault("last_ingested", current.get("last_ingested", {}))
            ctx.save_manifest(project_full, new_m)
            manifest = new_m
        elif isinstance(patch, dict):
            for k, v in patch.items():
                if k in ("raw_files", "artifacts") and not isinstance(v, list):
                    continue
                current[k] = v
            ctx.save_manifest(project_full, current)
            manifest = current
        else:
            resp = web.json_response(
                {"ok": False, "error": "Expected either 'manifest' (dict) or 'patch' (dict) in body."},
                status=400,
            )
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        resp = web.json_response({"ok": True, "project": project_short, "manifest": manifest})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    async def handle_classify(request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except Exception:
            resp = web.json_response({"ok": False, "error": "Expected JSON body."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        if not isinstance(payload, dict):
            resp = web.json_response({"ok": False, "error": "Body must be a JSON object."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        project_param = (payload.get("project") or "").strip()
        project_name = ctx.safe_project_name(project_param) if project_param else ctx.DEFAULT_PROJECT_NAME
        ctx.ensure_project_scaffold(project_name)

        text = (payload.get("text") or "").strip()
        if not text:
            resp = web.json_response({"ok": False, "error": "Missing 'text' to classify."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        labels = payload.get("labels")
        if isinstance(labels, list):
            labels = [str(x) for x in labels if str(x).strip()]
        else:
            labels = []

        extra_system = (payload.get("system") or "").strip()

        sys_lines = [
            "You are a classification assistant. Respond in strict JSON with keys: label, explanation.",
        ]
        if labels:
            sys_lines.append("You MUST choose exactly one of these labels:")
            for lb in labels:
                sys_lines.append(f"- {lb}")
        else:
            sys_lines.append("Infer a short, lowercase label (1–3 words) that best describes the text.")

        if extra_system:
            sys_lines.append("")
            sys_lines.append("Additional instructions from caller:")
            sys_lines.append(extra_system)

        system_prompt = "\n".join(sys_lines)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": text},
        ]

        try:
            raw = await asyncio.to_thread(ctx.call_openai_chat, messages)
        except Exception as e:
            resp = web.json_response({"ok": False, "error": f"Model call failed: {e!r}"}, status=500)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        label = ""
        explanation = ""

        try:
            m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
            if m:
                obj = json.loads(m.group(0))
                if isinstance(obj, dict):
                    label = str(obj.get("label") or "").strip()
                    explanation = str(obj.get("explanation") or "").strip()
        except Exception:
            pass

        if not label:
            for ln in raw.splitlines():
                ln = ln.strip()
                if ln:
                    label = ln
                    break

        resp = web.json_response(
            {"ok": True, "project": project_name, "label": label, "explanation": explanation, "raw": raw}
        )
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    async def handle_login_options(request: web.Request) -> web.Response:
        origin = (request.headers.get("Origin") or "").strip()
        resp = web.Response(status=204)

        if origin:
            resp.headers["Access-Control-Allow-Origin"] = origin
            resp.headers["Access-Control-Allow-Credentials"] = "true"
            resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
            resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
            resp.headers["Vary"] = "Origin"

        return resp

    async def handle_login(request: web.Request) -> web.Response:
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)

        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return web.json_response({"ok": False}, status=401)

        if ctx.USERS.get(username) != password:
            return web.json_response({"ok": False}, status=401)

        resp = web.json_response({"ok": True})

        origin = (request.headers.get("Origin") or "").strip()
        if origin:
            resp.headers["Access-Control-Allow-Origin"] = origin
            resp.headers["Access-Control-Allow-Credentials"] = "true"
            resp.headers["Vary"] = "Origin"

        forwarded_host = (request.headers.get("X-Forwarded-Host") or request.headers.get("Host") or request.host or "").strip()
        forwarded_host = forwarded_host.split(",", 1)[0].strip()
        host_only = forwarded_host.split(":", 1)[0].strip().lower()

        base = host_only
        if base.startswith("ws."):
            base = base[3:]
        if base.startswith("www."):
            base = base[4:]

        domain = None
        if base and base != "localhost" and not re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", base):
            domain = "." + base

        # Local dev: a Secure cookie will NOT be sent over http://localhost.
        # If we're on localhost (or an IP), allow a non-secure cookie so HTTP endpoints work.
        is_ip = bool(re.fullmatch(r"\d{1,3}(\.\d{1,3}){3}", host_only))
        is_local = (host_only == "localhost") or is_ip

        cookie_secure = bool(getattr(request, "secure", False)) and (not is_local)
        cookie_samesite = "None" if cookie_secure else "Lax"

        cookie_kwargs = {
            "httponly": True,
            "path": "/",
            "samesite": cookie_samesite,
            "secure": cookie_secure,
        }

        if domain:
            resp.set_cookie("user", username, domain=domain, **cookie_kwargs)
        else:
            resp.set_cookie("user", username, **cookie_kwargs)

        return resp

    async def handle_health(request: web.Request) -> web.Response:
        up_s = max(0, int(time.time() - ctx.SERVER_START_TIME))

        ps_file = ""
        ps_sha256 = ""
        try:
            ps_file = str(Path(project_store.__file__).resolve())
            ps_sha256 = ctx.file_sha256_bytes(ctx.read_bytes_file(Path(project_store.__file__).resolve()))
        except Exception:
            ps_file = ""
            ps_sha256 = ""

        payload = {
            "ok": True,
            "color": ctx.SERVER_COLOR,
            "host": ctx.HOST,
            "ws_port": ctx.WS_PORT,
            "http_port": ctx.HTTP_PORT,
            "uptime_s": up_s,
            "recommended_active": ctx.read_active_color("blue"),
            "project_root": str(ctx.PROJECT_ROOT),
            "server_file": str(Path(ctx.__file__).resolve()),
            "ui_file": str(getattr(ctx, "UI_FILE", "")),
            "server_sha256": ctx.file_sha256_bytes(ctx.read_bytes_file(Path(ctx.__file__).resolve())),
            "project_store_file": ps_file,
            "project_store_sha256": ps_sha256,
            "model": ctx.OPENAI_MODEL,
            "brave_key_present": bool((ctx.os.getenv("BRAVE_API_KEY", "") or "").strip()),
            "timestamp": ctx.now_iso(),
        }
        return web.json_response(payload)

    async def handle_get_file(request: web.Request) -> web.StreamResponse:
        user = ctx.get_request_user(request)
        if not user:
            return web.Response(status=401, text="unauthorized", content_type="text/plain")

        rel = (request.rel_url.query.get("path") or "").strip().replace("\\", "/")
        if not rel or rel.startswith(("/", "../")) or "/../" in rel:
            return web.Response(status=400, text="invalid path", content_type="text/plain")

        abs_path = (ctx.PROJECT_ROOT / rel).resolve()

        allowed_root = (ctx.PROJECTS_DIR / ctx.safe_user_name(user)).resolve()
        try:
            abs_path.relative_to(allowed_root)
        except Exception:
            return web.Response(status=403, text="forbidden", content_type="text/plain")

        if not abs_path.exists() or not abs_path.is_file():
            return web.Response(status=404, text="not found", content_type="text/plain")

        resp = web.FileResponse(path=abs_path)

        try:
            sfx = abs_path.suffix.lower()
            if sfx in (".xlsx", ".xlsm", ".pdf", ".docx", ".pptx", ".zip"):
                fn = abs_path.name.replace('"', "")
                resp.headers["Content-Disposition"] = f'attachment; filename="{fn}"'
        except Exception:
            pass

        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        resp.headers["Pragma"] = "no-cache"
        resp.headers["Expires"] = "0"
        return resp

    async def handle_file_upload(request: web.Request) -> web.Response:
        user = ctx.get_request_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        reader = await request.multipart()
        field = await reader.next()
        if field is None or field.name != "file":
            resp = web.json_response({"ok": False, "error": "Expected a 'file' field."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        orig_name = field.filename or "uploaded.bin"
        base_name = ctx.safe_filename(orig_name)
        timestamp = int(time.time())
        safe_name = f"{timestamp}_{base_name}"

        project_param = (request.rel_url.query.get("project") or "").strip()
        project_short = ctx.safe_project_name(project_param) if project_param else ctx.DEFAULT_PROJECT_NAME
        project_full = ctx.safe_project_name(f"{user}/{project_short}")
        ctx.ensure_project_scaffold(project_full)

        save_path = ctx.raw_dir(project_full) / safe_name

        total = 0
        try:
            with save_path.open("wb") as f:
                while True:
                    chunk = await field.read_chunk(size=1024 * 1024)
                    if not chunk:
                        break
                    total += len(chunk)
                    f.write(chunk)
        except Exception as e:
            try:
                if save_path.exists():
                    save_path.unlink()
            except Exception:
                pass
            resp = web.json_response({"ok": False, "error": f"Failed to write file: {e!r}"}, status=500)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        if total == 0 or not save_path.exists():
            try:
                if save_path.exists():
                    save_path.unlink()
            except Exception:
                pass
            resp = web.json_response({"ok": False, "error": "Upload wrote 0 bytes."}, status=500)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        updated_existing, canonical_rel, prev_rel = ctx.register_raw_file(project_full, save_path, orig_name)

        payload: Dict[str, Any] = {
            "ok": True,
            "user": user,
            "project": project_short,
            "original_name": orig_name,
            "filename": safe_name,
            "saved_path": str(save_path),
            "relative_path": canonical_rel,
            "already_present": updated_existing,
            "bytes": total,
        }

        # NOTE: ingestion + classification runs asynchronously in the WS upload worker.
        # The browser notifies WS with: [FILE_ADDED]<relative_path> after this HTTP call returns.
        payload["queued"] = True


        resp = web.json_response(payload)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    async def handle_list_project(request: web.Request) -> web.Response:
        user = ctx.get_request_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        project_param = (request.rel_url.query.get("project") or "").strip()
        project_short = ctx.safe_project_name(project_param) if project_param else ctx.DEFAULT_PROJECT_NAME
        project_full = ctx.safe_project_name(f"{user}/{project_short}")

        data = ctx.list_project_files(project_full)
        resp = web.json_response({"ok": True, "project": data})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    async def handle_list_projects(request: web.Request) -> web.Response:
        user = ctx.get_request_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        ctx.get_user_projects_root(user)

        names = ctx.list_existing_projects(ctx.safe_user_name(user))

        # SAFETY/PRIVACY HARDENING:
        # If anything ever causes list_existing_projects() to run against the wrong base,
        # it could return top-level user directories (e.g., "Frank", "Emanie", "couple_Frank").
        # Never leak usernames via the projects dropdown.
        try:
            user_dir_names = set()
            for u in (ctx.USERS or {}).keys():
                user_dir_names.add(ctx.safe_user_name(str(u)))
            names = [n for n in (names or []) if ctx.safe_user_name(str(n)) not in user_dir_names]
        except Exception:
            pass

        # Include display titles when available (for Chat-style UI).
        projects_out = []
        for n in (names or []):
            title = ""
            try:
                title = str(ctx.project_store.get_project_display_name(ctx.safe_project_name(f"{user}/{n}")) or "").strip()
            except Exception:
                title = ""
            projects_out.append({"name": n, "title": title})

        resp = web.json_response({"ok": True, "projects": projects_out})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    async def handle_delete_project(request: web.Request) -> web.Response:
        user = ctx.get_request_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        try:
            payload = await request.json()
        except Exception:
            resp = web.json_response({"ok": False, "error": "Expected JSON body."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        project_param = (payload.get("project") or "").strip() if isinstance(payload, dict) else ""
        project_short = ctx.safe_project_name(project_param) if project_param else ""
        if not project_short:
            resp = web.json_response({"ok": False, "error": "Missing 'project'."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        if project_short.lower() == "default":
            resp = web.json_response({"ok": False, "error": "Cannot delete default project."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        proj_dir = (ctx.PROJECTS_DIR / ctx.safe_user_name(user) / project_short).resolve()
        allowed_root = (ctx.PROJECTS_DIR / ctx.safe_user_name(user)).resolve()
        try:
            proj_dir.relative_to(allowed_root)
        except Exception:
            resp = web.json_response({"ok": False, "error": "forbidden"}, status=403)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        if not proj_dir.exists() or not proj_dir.is_dir():
            resp = web.json_response({"ok": False, "error": "not found"}, status=404)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        try:
            shutil.rmtree(proj_dir)
            project_store.mark_project_deleted(f"{user}/{project_short}")
        except Exception as e:
            resp = web.json_response({"ok": False, "error": f"delete failed: {e!r}"}, status=500)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        resp = web.json_response({"ok": True, "deleted": project_short})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    async def handle_create_artifact(request: web.Request) -> web.Response:
        try:
            payload = await request.json()
        except Exception:
            resp = web.json_response({"ok": False, "error": "Expected JSON body."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        project_name = ctx.safe_project_name((payload.get("project") or "").strip()) if payload.get("project") else ctx.DEFAULT_PROJECT_NAME
        ctx.ensure_project_scaffold(project_name)

        logical = (payload.get("name") or "").strip()
        if not logical:
            resp = web.json_response({"ok": False, "error": "Missing 'name'."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        content = payload.get("content") or ""
        from_files = payload.get("from_files") or []
        atype = payload.get("type") or "cheat_sheet"
        ext = payload.get("ext") or ".md"

        try:
            entry = ctx.create_artifact(project_name, logical, content, artifact_type=atype, from_files=from_files, file_ext=ext)
        except Exception as e:
            resp = web.json_response({"ok": False, "error": f"Failed to create artifact: {e!r}"}, status=500)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        resp = web.json_response({"ok": True, "artifact": entry})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp

    async def handle_generate_excel(request: web.Request) -> web.Response:
        user = ctx.get_request_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        try:
            payload = await request.json()
        except Exception:
            resp = web.json_response({"ok": False, "error": "Expected JSON body."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        if not isinstance(payload, dict):
            resp = web.json_response({"ok": False, "error": "Body must be a JSON object."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        project_param = (payload.get("project") or "").strip()
        project_short = ctx.safe_project_name(project_param) if project_param else ctx.DEFAULT_PROJECT_NAME
        project_full = ctx.safe_project_name(f"{user}/{project_short}")
        ctx.ensure_project_scaffold(project_full)

        logical = (payload.get("name") or "").strip() or "generated_excel"
        spec = payload.get("spec")
        if not isinstance(spec, dict):
            resp = web.json_response({"ok": False, "error": "Missing or invalid 'spec' (must be an object)."}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        report = ctx.validate_excel_spec(spec)
        repaired_flag = False
        if report.get("ok") is not True:
            spec2, report2, repaired_flag = ctx.maybe_repair_excel_spec_once(spec, report)
            spec, report = spec2, report2

        if report.get("ok") is not True:
            resp = web.json_response({"ok": False, "error": "spec_validation_failed", "report": report}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        report = ctx.validate_excel_spec_master(spec)
        repaired_flag = False
        if report.get("ok") is not True:
            spec2, report2, repaired_flag = ctx.maybe_repair_excel_spec_once_master(spec, report)
            spec, report = spec2, report2

        if report.get("ok") is not True:
            resp = web.json_response({"ok": False, "error": "spec_validation_failed_master", "report": report}, status=400)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        try:
            xlsx_bytes = ctx.generate_workbook_from_spec(spec, project_raw_dir=ctx.raw_dir(project_full))
        except Exception as e:
            resp = web.json_response({"ok": False, "error": f"Excel generation failed: {e!r}"}, status=500)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        try:
            entry = ctx.create_artifact_bytes(
                project_full,
                logical,
                xlsx_bytes,
                artifact_type="deliverable_excel",
                from_files=[],
                file_ext=".xlsx",
                meta={
                    "spec_name": str(spec.get("name") or ""),
                    "generator": "openpyxl",
                    "repaired": bool(repaired_flag),
                    "validation_warnings": report.get("warnings") or [],
                },
            )
        except Exception as e:
            resp = web.json_response({"ok": False, "error": f"Failed to write artifact: {e!r}"}, status=500)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

        rel = (entry.get("path") or "").replace("\\", "/")
        open_link = f"/file?path={rel}" if rel else ""

        # C1: Explicitly register the generated Excel as a first-class deliverable.
        try:
            excel_engine.register_excel_deliverable(
                register_deliverable_fn=project_store.register_deliverable,
                project_name=project_full,
                logical_name=logical,
                rel_path=rel,
                spec=spec,
            )
        except Exception:
            pass

        resp = web.json_response({"ok": True, "project": project_short, "artifact": entry, "open": open_link})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    async def handle_get_capabilities(request: web.Request) -> web.Response:
        user = ctx.get_request_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        resp = web.json_response({"ok": True, "capabilities": capabilities.get_registry_json()})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    async def handle_get_registry(request: web.Request) -> web.Response:
        user = ctx.get_request_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        project_param = (request.rel_url.query.get("project") or "").strip()
        project_short = ctx.safe_project_name(project_param) if project_param else ctx.DEFAULT_PROJECT_NAME
        project_full = ctx.safe_project_name(f"{user}/{project_short}")
        ctx.ensure_project_scaffold(project_full)

        try:
            out = ctx.generate_system_registry(project_full)
            md_rel = (out.get("md_rel") or "").replace("\\", "/").strip()
            json_rel = (out.get("json_rel") or "").replace("\\", "/").strip()
            open_link = f"/file?path={md_rel}" if md_rel else (f"/file?path={json_rel}" if json_rel else "")
            resp = web.json_response(
                {
                    "ok": True,
                    "project": project_short,
                    "md": md_rel,
                    "json": json_rel,
                    "open": open_link,
                }
            )
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp
        except Exception as e:
            resp = web.json_response({"ok": False, "error": f"registry generation failed: {e!r}"}, status=500)
            resp.headers["Access-Control-Allow-Origin"] = "*"
            return resp

    async def handle_get_deliverables(request: web.Request) -> web.Response:
        user = ctx.get_request_user(request)
        if not user:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        project_param = (request.rel_url.query.get("project") or "").strip()
        project_short = ctx.safe_project_name(project_param) if project_param else ctx.DEFAULT_PROJECT_NAME
        project_full = ctx.safe_project_name(f"{user}/{project_short}")
        ctx.ensure_project_scaffold(project_full)

        data = project_store.load_deliverables(project_full)
        resp = web.json_response({"ok": True, "project": project_short, "deliverables": data})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    
    # ---- Route registrations (single place) ----
    app.router.add_get("/", handle_root)
    app.router.add_get("/manifest", handle_get_manifest)
    app.router.add_post("/manifest", handle_update_manifest)
    app.router.add_post("/classify", handle_classify)
    app.router.add_post("/login", handle_login)
    app.router.add_options("/login", handle_login_options)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/capabilities", handle_get_capabilities)
    app.router.add_get("/registry", handle_get_registry)
    app.router.add_get("/file", handle_get_file)
    app.router.add_post("/upload", handle_file_upload)
    app.router.add_get("/list_project", handle_list_project)
    app.router.add_get("/list_projects", handle_list_projects)
    app.router.add_get("/deliverables", handle_get_deliverables)
    app.router.add_post("/delete_project", handle_delete_project)
    app.router.add_post("/create_artifact", handle_create_artifact)
    app.router.add_post("/generate_excel", handle_generate_excel)
