from __future__ import annotations

import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = PROJECT_ROOT / "dags"

if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))
