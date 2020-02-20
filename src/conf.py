import os

from dynaconf import settings
from pathlib import Path

__here = Path(os.path.abspath(os.path.dirname(__file__)))

settings.DATA_DIR = (__here / ".." / "data").resolve()
