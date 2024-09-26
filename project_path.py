import os
from pathlib import Path

from src.run_config import run_name

_file_path = os.path.abspath(__file__)
_project_root = Path(_file_path + "/../..").absolute().resolve()

PROJECT_ROOT = str(_project_root)
PROJECT_ROOT_FOLDER_NAME = _project_root.stem

DATA_ROOT_FOLDER = Path(f"/team/development/pricing/{run_name}")
GENERAL_DATA = DATA_ROOT_FOLDER / "general"
