import asyncio
import json
import os
import time
from pathlib import Path

import aiohttp
import websockets

HTTP_BASE = "http://localhost:8766"
WS_URL = "ws://localhost:8765/?user=Frank&pass=Daisy17"

USER = "Frank"
PASS = "Daisy17"
PROJECT = "memory_smoke"  # new project name

# Adjust this to your repo root if running elsewhere
PROJECT_ROOT = Path(os.environ.get("LENS0_PROJECT_ROOT", ".")).resolve()
STATE_DIR = PROJECT_ROOT / "projects" / USER.replace(" ", "_") / PROJECT / "state"
FACTS_RAW = STATE_DIR / "facts_raw.jsonl"
FACTS_MAP = STATE_DIR / "facts_map.md"

async def recv_last_message(ws, *, first_timeout_s=2.0, drain_timeout_s=0.15):
    """
    Wait for at least one WS message (first_timeout_s), then drain any queued messages
    (drain_timeout_s) and return the last one.

    Some server actions (e.g. "switch project") may not emit a message immediately.
    In that case we return None.
    """
    try:
        last = await asyncio.wait_for(ws.recv(), timeout=first_timeout_s)
    except asyncio.TimeoutError:
        return None

    while True:
        try:
            msg = await asyncio.wait_for(ws.recv(), timeout=drain_timeout_s)
            last = msg
        except asyncio.TimeoutError:
            break
    return last


async def send_and_wait(ws, text: str, *, first_timeout_s: float = 30.0) -> str:
    """
    Send a message and wait for a non-None response.
    Retries a couple times to avoid flakiness under slow model latency.
    """
    await ws.send(text)

    # First wait: allow real model latency.
    resp = await recv_last_message(ws, first_timeout_s=first_timeout_s)
    if resp is not None:
        return resp

    # Retry waits (no re-send): sometimes the server responds slightly later.
    for _ in range(3):
        resp = await recv_last_message(ws, first_timeout_s=first_timeout_s)
        if resp is not None:
            return resp

    raise TimeoutError(f"No response received for: {text[:80]}")

# Turns that SHOULD produce Tier-1 candidates (explicit, first-person, recallable facts).
# These are written in natural human language (messy, contextual, corrections).
FACT_TURNS = [
    # identity / basics
    "My preferred name is Frank.",
    "Sometimes people call me Francis, but I go by Frank.",
    "I live in Austin, Texas.",
    "Actually, I'm in Austin now — I moved here recently.",
    "My favorite color is green.",

    # relationships / family
    "My son is named Logan.",
    "Logan is 7 years old.",
    "My girlfriend’s name is Emanie.",

    # constraints / legal / life logistics
    "I'm on an E-2 visa.",
    "I’m getting married after the divorce is finalized.",
    "We have a custody schedule — I have Logan every other weekend.",

    # preferences (allowed facts)
    "I prefer direct communication when plans change.",
    "I don’t like last-minute surprises.",
    "Coffee helps me focus in the morning.",

    # work / skills / general context (still explicit)
    "I work in tech.",
    "I’m building a stateful assistant that uses tiered memory.",
    "I’m usually on Central Time.",
]

# Turns that should NOT be stored as facts (therapy reflections, opinions, speculation, questions).
# The extractor should skip these.
NO_STORE_TURNS = [
    "I feel like she doesn't respect me when plans change.",
    "I think I'm the one overreacting sometimes.",
    "Maybe I'm just tired and reading into it.",
    "I guess this is all my fault.",
    "I'm worried this will never get better.",
    "What do you think I should do?",
    "It was a rough week and I’m kind of all over the place emotionally.",
]

RECALL_QS = [
    "What's my preferred name?",
    "Where do I live?",
    "What's my favorite color?",
]

async def main():
    # 1) Login to get cookie
    jar = aiohttp.CookieJar()
    async with aiohttp.ClientSession(cookie_jar=jar) as sess:
        r = await sess.post(f"{HTTP_BASE}/login", json={"username": USER, "password": PASS})
        assert r.status == 200, (r.status, await r.text())

        # 2) Connect WS (cookie should be sent)
        async with websockets.connect(WS_URL) as ws:
            # Switch project (UI normally does "switch project: X")
            await ws.send(f"switch project: {PROJECT}")
            _ = await recv_last_message(ws, first_timeout_s=0.5)  # may be None; that's OK

            # 3) Send fact turns (natural conversation, but explicit facts)
            for t in FACT_TURNS:
                _ = await send_and_wait(ws, t, first_timeout_s=30.0)

            # Give the server a brief moment to flush writes (still deterministic)
            await asyncio.sleep(0.25)

            # Snapshot Tier-1 size after factual turns
            facts_before = 0
            if FACTS_RAW.exists():
                facts_before = len(FACTS_RAW.read_text(encoding="utf-8", errors="replace").splitlines())

            # 4) Send "no-store" turns (therapy reflections/opinions/questions)
            for t in NO_STORE_TURNS:
                _ = await send_and_wait(ws, t, first_timeout_s=30.0)

            await asyncio.sleep(0.25)

            facts_after = 0
            if FACTS_RAW.exists():
                facts_after = len(FACTS_RAW.read_text(encoding="utf-8", errors="replace").splitlines())

            # The no-store turns should not meaningfully increase Tier-1.
            # Allow a tiny cushion (0–1) in case a preference-like sentence slips through.
            assert (facts_after - facts_before) <= 1, (
                f"NO_STORE_TURNS inflated Tier-1 by {facts_after - facts_before} "
                f"(before={facts_before}, after={facts_after})"
            )

            # 5) Ask recall questions (should answer from Tier-2 after cadence distill)
            for q in RECALL_QS:
                ans = await send_and_wait(ws, q, first_timeout_s=30.0)
                print("\nQ:", q)
                print("A:", ans)

    # 5) Verify files updated on disk
    print("\n--- Disk checks ---")
    print("STATE_DIR:", STATE_DIR)

    assert FACTS_RAW.exists(), f"missing: {FACTS_RAW}"
    raw_lines = FACTS_RAW.read_text(encoding="utf-8", errors="replace").splitlines()
    print("facts_raw.jsonl lines:", len(raw_lines))

    # We sent many explicit fact turns; require a meaningful capture rate.
    # (If this fails, the extractor is still too strict.)
    assert len(raw_lines) >= 12, (
        f"facts_raw.jsonl too small (got {len(raw_lines)}); "
        f"auto-capture likely failing or too strict"
    )

    # Ensure we did NOT store reflective/speculative sentences as facts.
    # (We allow preferences, but not feelings/speculation framed as truth.)
    banned_markers = [
        "i think",
        "i feel",
        "maybe",
        "probably",
        "i guess",
        "i'm worried",
        "im worried",
    ]

    bad = 0
    for ln in raw_lines[-200:]:
        try:
            obj = json.loads(ln)
        except Exception:
            continue
        claim = str(obj.get("claim", "")).lower()
        if any(m in claim for m in banned_markers):
            bad += 1

        # Also, claims should not be questions
        if "?" in claim:
            bad += 1

    assert bad == 0, "stored reflective/speculative/question sentences as facts"

    assert FACTS_MAP.exists(), f"missing: {FACTS_MAP}"
    fm = FACTS_MAP.read_text(encoding="utf-8", errors="replace")
    print("facts_map.md chars:", len(fm))

    # Deterministic Tier-2 checks: verify key facts made it into the working model.
    fml = fm.lower()
    assert "frank" in fml, "facts_map.md missing expected fact: Frank"
    assert "austin" in fml, "facts_map.md missing expected fact: Austin"
    assert "green" in fml, "facts_map.md missing expected fact: green"

    print("OK: smoke test passed.")

if __name__ == "__main__":
    asyncio.run(main())
