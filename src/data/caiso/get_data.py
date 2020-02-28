"""Download a copy of the CAISO curtailment data.

Oversupply data can be found at the following URL: 
https://www.caiso.com/informed/Pages/ManagingOversupply.aspx
"""
import requests

from pathlib import Path
from loguru import logger
from urllib.parse import urlparse

from src.conf import settings


OUTPUT_DIR = settings.DATA_DIR / "raw/caiso/"

base_url = "https://www.caiso.com/Documents/{}"


def generate_urls():
    """Generate a set of URLs for each year of data in the analysis period.
    """
    yield base_url.format("ProductionAndCurtailmentsData-May1_2014-May31_2017.xlsx")
    yield base_url.format("ProductionAndCurtailmentsData-Jun1_2017-Dec31_2017.xlsx")
    for year in range(2018, 2021):
        yield base_url.format(f"ProductionAndCurtailmentsData_{year}.xlsx")


def main():
    for url in generate_urls():
        logger.info("Downloading CAISO Curtailment file at: {url}", url=url)
        fn = Path(urlparse(url).path).name
        with requests.get(url) as response:
            response.raise_for_status()
            fp = OUTPUT_DIR / fn
            with open(fp, "wb") as fh:
                for chunk in response.iter_content():
                    fh.write(chunk)


if __name__ == "__main__":
    OUTPUT_DIR.mkdir(exist_ok=True)
    main()