"""
Pulls in data from california powerplants.

Source: https://cecgis-caenergy.opendata.arcgis.com/datasets/california-power-plants
"""
import json
import requests
from src.conf import settings


DATA_DIR = settings.DATA_DIR

output = DATA_DIR / "raw/geography/powerplants.geojson"

uri = "https://opendata.arcgis.com/datasets/4a702cd67be24ae7ab8173423a768e1b_0.geojson"


class UnexpectedData(Exception):
    pass


if __name__ == "__main__":
    r = requests.get(uri, stream=True)
    with open(output, "wb") as fh:
        for chunk in r.iter_content(chunk_size=1024*5):
            if chunk:
                fh.write(chunk)
    
    # Validate the data
    with open(output, "r") as fh:
        try:
            data = json.load(fh)
        except Exception as e:
            raise UnexpectedData(e)
