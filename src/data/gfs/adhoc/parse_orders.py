"""One time script to download temporary/ephermal HAS NOAA archives.

System Requirements:
- python

Python Requirements:
- invoke
- pandas
- requests
- lxmlex
"""
import pandas as pd
from invoke import run

order_ids = [
    "HAS011471200",
    "HAS011471202",
    "HAS011471204",
    "HAS011471206",
    "HAS011471208",
    "HAS011471210",
]

def load_orders(order_id):
    """
    """
    
    url = f"https://www1.ncdc.noaa.gov/pub/has/model/{order_id}/"

    df = pd.read_html(url, skiprows=2)[0]
    cols = df.columns[[1,2,3]]
    df = df[cols]
    df.columns = ["filename", "date", "size"]

    for fn in df["filename"].tolist():
        result = run(f"gsutil -q stat gs://carecur-gfs-data/grid3/{fn}", hide=True, warn=True)
        
        if result.exited:  # if we failed to find the file, create it
            ftp = f"ftp://ftp.ncdc.noaa.gov/pub/has/model/{order_id}/{fn}"
            cmd = f"curl {ftp} | gsutil cp - gs://carecur-gfs-data/grid3/{fn}"
            print(f"Running {cmd}")
            run(cmd)
        else:
            print(f"{fn} already exists.  Skipping.")


if __name__ == "__main__":
    for order_id in order_ids:
        load_orders(order_id)