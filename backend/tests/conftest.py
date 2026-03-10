from __future__ import annotations

from pathlib import Path
import sys


ROOT_DIR = Path(__file__).resolve().parents[2]
PYDEPS_DIR = ROOT_DIR / ".pydeps"

if PYDEPS_DIR.exists() and str(PYDEPS_DIR) not in sys.path:
    sys.path.insert(0, str(PYDEPS_DIR))
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
