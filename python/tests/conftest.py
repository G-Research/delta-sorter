import sys
from pathlib import Path

import pytest

# Ensure local Python wrapper is importable as `deltasort` before test modules import
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PKG_DIR = _PROJECT_ROOT / "python"
if str(_PKG_DIR) not in sys.path:
    sys.path.insert(0, str(_PKG_DIR))


@pytest.fixture(scope="session")
def project_root() -> Path:
    return _PROJECT_ROOT


@pytest.fixture()
def tmp_table(tmp_path: Path) -> str:
    # Return a filesystem path for a fresh Delta table
    return str(tmp_path / "table")
