"""California county shape files.

https://data.ca.gov/dataset/ca-geographic-boundaries/resource/b0007416-a325-4777-9295-368ea6b710e6
"""
import zipfile

from invoke import run
from loguru import logger

from src.conf import settings

RAW_DIR = settings.DATA_DIR / "raw/geography/"
PROCESSED_DIR = settings.DATA_DIR / "processed/geography/"

url = "https://data.ca.gov/dataset/e212e397-1277-4df3-8c22-40721b095f33/resource/b0007416-a325-4777-9295-368ea6b710e6/download/ca-county-boundaries.zip"

fn = "ca-county-boundaries.zip"

if __name__ == "__main__":
    # Get data
    RAW_DIR.mkdir(exist_ok=True)
    PROCESSED_DIR.mkdir(exist_ok=True)

    fp = RAW_DIR/fn

    # Download data
    cmd = f"curl -L {url} -o {fp}"
    run(cmd)

    # Unzip it!
    with zipfile.ZipFile(fp, "r") as fh:
        fh.extractall(PROCESSED_DIR)