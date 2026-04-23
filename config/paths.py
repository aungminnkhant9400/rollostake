"""Shared filesystem paths for local and VPS runs."""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DASHBOARD_DIR = PROJECT_ROOT / "dashboard"
DB_PATH = DATA_DIR / "rollo_stake.db"


def ensure_runtime_dirs():
    """Create directories used by generated runtime artifacts."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)

