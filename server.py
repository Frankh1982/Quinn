"""
Root shim entrypoint: forwards execution to the active runtime server.

Users should run: python server.py

This file exists to prevent confusion about which server.py is "real".
"""


from __future__ import annotations

import runpy
from pathlib import Path
import sys

HERE = Path(__file__).resolve().parent
TARGET = HERE / "runtime" / "blue" / "server.py"

print("ROOT SHIM:", __file__)
print("TARGET:", TARGET)

if not TARGET.exists():
    raise SystemExit(f"[FATAL] Missing runtime server at: {TARGET}")

# Ensure relative imports inside the runtime server behave consistently.
sys.path.insert(0, str(TARGET.parent))

runpy.run_path(str(TARGET), run_name="__main__")
