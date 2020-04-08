"""One time script to download temporary/ephermal HAS NOAA archives.

Expected Data Request Form:

https://www.ncdc.noaa.gov/has/HAS.FileAppRouter?datasetname=GFS3&subqueryby=STATION&applname=&outdest=FILE

System Requirements:
- python

Python Requirements:
- invoke
- pandas
- requests
- lxml
- loguru
"""
import pandas as pd
from loguru import logger
from invoke import run

logger.add(f"{__file__}_{{time}}.log", rotation="500 MB", compression="zip", backtrace=True, diagnose=True)

# Each NOAA NCDC order contains an ID
order_ids = [
    # #####2018
    # "HAS011471200",
    # "HAS011471202",
    # "HAS011471204",
    # "HAS011471206",
    # "HAS011471208",
    # "HAS011471210",
    ###### 2017
    "HAS011483669",
    "HAS011483671",
    "HAS011483673",
    # "HAS011483675",
    "HAS011483677",
    "HAS011483681",
    ###### 2019
    "HAS011483683",
    "HAS011483685",
    "HAS011483690",
    "HAS011483691",
    # "HAS011483694",
    # "HAS011483697",
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
            logger.info("Running {cmd}", cmd=cmd)
            run(cmd)
        else:
            logger.info("{fn} already exists.  Skipping.", fn=fn)


if __name__ == "__main__":
    for order_id in order_ids:
        load_orders(order_id)