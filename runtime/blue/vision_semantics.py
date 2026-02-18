# -*- coding: utf-8 -*-
"""
vision_semantics.py

On-demand, cached semantic perception for images.

Goals:
- Run ONLY when needed (no upload-time auto calls).
- Cache by file sha256 + prompt_id + prompt_hash + model.
- Store results as artifacts (JSON) and reuse across turns.
- Deterministic prompting (temperature=0) and strict anti-hallucination.
- No imports from server.py (no circular deps). Caller provides callbacks.

Artifact schema: image_semantics_v1.json
{
  "schema": "image_semantics_v1",
  "created_at": "...",
  "source": {"rel_path": "...", "sha256": "...", "mime": "...", "bytes": 123},
  "cache": {"cache_key": "...", "prompt_id": "...", "prompt_hash": "...", "model": "..."},
  "inputs": {"caption": "...", "ocr_text": "...", "classification": {...}, "local_semantics": {...}},
  "outputs": {
    "summary": "...",
    "observations": ["...", "..."],
    "uncertainties": ["..."]
  }
}
"""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

from path_engine import now_iso


def sha256_text(s: str) -> str:
    return hashlib.sha256((s or "").encode("utf-8", errors="ignore")).hexdigest()


def build_cache_key(*, file_sha256: str, prompt_id: str, prompt_hash: str, model: str) -> str:
    raw = "|".join([(file_sha256 or ""), (prompt_id or ""), (prompt_hash or ""), (model or "")]).strip()
    return hashlib.sha256(raw.encode("utf-8", errors="ignore")).hexdigest()


def build_prompt_v1(*, caption: str, ocr_text: str, classification_json: str, local_semantics_json: str) -> str:
    """
    Deterministic prompt to produce strict JSON semantics.
    Must be stable; changes should bump prompt_id or alter prompt_hash naturally.
    """
    cap = (caption or "").strip()
    ocr = (ocr_text or "").strip()
    cls = (classification_json or "").strip()
    loc = (local_semantics_json or "").strip()

    # Bound evidence (caller can pass already-bounded slices; we also cap hard here)
    cap = cap[:1400]
    ocr = ocr[:2200]
    cls = cls[:1800]
    loc = loc[:2400]

    return "\n".join([
        "- Mission: aggressively (but honestly) identify, name, and categorize what is shown so it can be indexed, recalled, and reused later.",
        "- You should attempt classification for ALL salient entities in the image (primary subject + notable secondary objects),",
        "  e.g., vehicle + body style, tree/flower type, animal, product type, scene/place context, materials, colors, condition.",
        "",
        "- Use visible evidence first. You may use context cues (setting, habitat, typical use) ONLY as supporting evidence, not as the sole basis.",
        "- Never claim you see logos/text/markers that are not visible.",
        "- Never invent precise details (exact trim/species/year) unless supported by clear visible cues.",
        "",
        "- You MAY provide probabilistic identification guesses inside outputs.candidates, and you are encouraged to do so whenever there are recognizable cues",
        "  (shape, silhouette, proportions, leaf/flower morphology, bark/branching, petal count, fruit/seed pods, headlight/taillight signatures, grille lines,",
        "   wheel arches, stance, roofline, materials, habitat/context, usage context, etc.).",
        "",
        "- Candidate quality rules:",
        "  - Provide 1-8 candidates total, prioritizing the most likely IDs.",
        "  - Each candidate MUST include:",
        "    - confidence (0.0..1.0)",
        "    - 2-6 evidence_points grounded in visible cues (explicitly state what you see).",
        "  - Prefer hierarchical IDs when uncertain:",
        "    - If unsure at fine-grain, return a correct broader ID rather than empty (family/genus/class/body-style).",
        "    - Example plants: 'oak (Quercus)' > guessing a specific oak species.",
        "    - Example cars: 'Porsche 911 (likely)' > guessing exact generation/trim.",
        "",
        "- Summary naming rule (so the system actually names things):",
        "  - outputs.summary must stay truthful, but MAY include 'likely' identifications when supported by outputs.candidates:",
        "    - If top candidate confidence >= 0.70: summary MAY name it as 'Likely <make> <model>'.",
        "    - If 0.50 <= confidence < 0.70: summary may say 'Possibly <make> <model>' and mention uncertainty.",
        "    - If confidence < 0.50: summary should stay generic (e.g., 'a sports coupe', 'a deciduous tree') and leave IDs to candidates.",
        "",
        "- Tagging rule (for recall, search, and reuse):",
        "  - outputs.tags MUST be populated whenever possible.",
        "  - Tags are flat, lowercase, reusable keywords derived from visible facts and candidate IDs.",
        "  - Include:",
        "    - object categories (e.g., 'car', 'tree', 'flower', 'animal', 'building')",
        "    - subtype/body-style (e.g., 'sports coupe', 'conifer', 'deciduous tree')",
        "    - likely make/genus/common group (e.g., 'porsche', 'oak', 'maple') when confidence >= 0.50",
        "    - environment/context (e.g., 'outdoor', 'urban', 'forest', 'road')",
        "    - notable attributes (e.g., 'red', 'two-door', 'evergreen', 'flowering')",
        "  - Tags MUST NOT include invented specifics; prefer broader tags when uncertain.",
        "",
        "- Correction handling (critical for future reuse):",
        "  - Treat inputs.local_semantics as potentially user-provided corrections or labels from prior turns.",
        "  - If inputs.local_semantics contains explicit corrections (make/model/species/category) and they do not contradict visible evidence:",
        "    - Raise confidence accordingly.",
        "    - Reflect them in outputs.candidates, outputs.summary (if confidence threshold met), and outputs.tags.",
        "  - If a correction conflicts with visible evidence:",
        "    - Keep it as a lower-confidence alternative candidate.",
        "    - Explain the conflict in outputs.uncertainties.",
        "",
        "Candidate field meaning (keep keys the same for compatibility):",
        "- make: brand / genus / group / category (e.g., 'Porsche', 'Quercus', 'Acer', 'Rose family', 'Sports car')",
        "- model: model / species / common name / subtype (e.g., '911', 'red maple', 'white oak', 'tulip', 'sports coupe')",
        "- year_range: only if applicable and supported by visible cues; otherwise use '' (empty string) or null",
        "",
        "Return JSON with exactly these top-level keys:",
        "schema, created_at, inputs, outputs",
        "",
        "Where:",
        "- schema = 'image_semantics_v1'",
        "- created_at = ISO timestamp",
        "- inputs: object with keys caption, ocr_text, classification, local_semantics (strings or objects ok)",
        "- outputs: object with keys:",
        "  - summary: short paragraph describing what a human would say this image shows (may include 'likely/possibly' IDs per rule above)",
        "  - observations: array of 3-12 short bullet-like strings (visible facts)",
        "  - uncertainties: array of strings describing what is unclear/unreadable/ambiguous and any correction conflicts",
        "  - candidates: array of 0-8 objects with keys:",
        "      make, model, year_range, confidence, evidence_points",
        "  - tags: array of lowercase strings suitable for indexing, recall, and cross-image grouping",
        "",
        "- If the subject is a plant or animal and species-level ID is uncertain, include at least one broader taxonomic candidate and corresponding tags rather than returning empty.",



        "",
        "EVIDENCE_CAPTION:",
        cap or "(none)",
        "",
        "EVIDENCE_OCR:",
        ocr or "(none)",
        "",
        "EVIDENCE_CLASSIFICATION_JSON:",
        cls or "(none)",
        "",
        "EVIDENCE_LOCAL_SEMANTICS_JSON:",
        loc or "(none)",
    ]).strip()


@dataclass
class SemanticsResult:
    ok: bool
    cache_hit: bool
    cache_key: str
    json_text: str
    note: str


def ensure_image_semantics(
    *,
    file_rel: str,
    file_sha256: str,
    mime: str,
    image_bytes: bytes,
    model_name: str,
    prompt_id: str,
    prompt_text: str,
    find_cached_fn: Callable[[str], str],
    create_artifact_fn: Callable[[str, str, str, Dict[str, Any]], None],
) -> SemanticsResult:
    """
    Caller-supplied callbacks:
      - find_cached_fn(cache_key) -> json_text or ""
      - create_artifact_fn(logical_name, artifact_type, json_text, meta)

    No async here; caller is responsible for throttling + threading if needed.
    """
    prompt_hash = sha256_text(prompt_text)
    cache_key = build_cache_key(
        file_sha256=file_sha256,
        prompt_id=prompt_id,
        prompt_hash=prompt_hash,
        model=model_name,
    )

    # Cache lookup
    try:
        cached = (find_cached_fn(cache_key) or "").strip()
    except Exception:
        cached = ""
    if cached.startswith("{") and '"schema"' in cached and "image_semantics" in cached:
        return SemanticsResult(ok=True, cache_hit=True, cache_key=cache_key, json_text=cached, note="cache_hit")

    # Model call must be performed by the caller; we store only if caller passes model output via create_artifact_fn.
    # Here we just return a miss so caller can call the model and then store.
    return SemanticsResult(ok=False, cache_hit=False, cache_key=cache_key, json_text="", note="cache_miss")


def parse_json_best_effort(raw: str) -> Dict[str, Any]:
    s = (raw or "").strip()
    if not s:
        return {}
    try:
        # try direct parse
        obj = json.loads(s)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        pass

    # try to extract the first {...} block
    try:
        import re
        m = re.search(r"\{.*\}", s, flags=re.DOTALL)
        if not m:
            return {}
        obj = json.loads(m.group(0))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}
