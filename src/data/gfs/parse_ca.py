"""
Filters down a directory of grb2 files to a specific geometry.

FIXME:100 There is a memory leak somewhere in this file.  No clue where, but at month-level granularities, you need at least 2GB ram/year-month.
"""
import os
import click
import pandas as pd
import geopandas as gpd
import xarray as xr
import tarfile
import tempfile

from datetime import datetime
from invoke import run
from pathlib import Path
from loguru import logger
from google.cloud import storage
from filelock import Timeout, FileLock

from src.conf import settings
from src.data.gfs.utils import GRB2_CRS, grb2gdf

OUTPUT_DIR = settings.DATA_DIR / "interim/gfs/ca/"

os.environ.setdefault("SHAPE_RESTORE_SHX", "yes")

# Canonical geom_types
POLY_TYPES = ("Polygon", "MultiPolygon")

# GFS Grib2 Options
GRB2_SURFACE_FILTER = {"filter_by_keys": {"typeOfLevel": "surface"}}

SURFACE_VARS = (
    "t",  # Air Temperature
    "gust",  # Wind speed
    "tp",  # Total Precipitation
    "dswrf",  # Downward Short-Wave Radiation
    "uswrf",  # Upward Short-Wave Radiation
    "SUNSD",  # Sunshine duration
    "al",  # Albedo
    "sp", # Surface pressure
    "csnow", # Categorical: Snow
    "cicep", # Categorical: Ice pellets
    "cfrzr", # Categorical: Freezing rain
    "crain", # Categorical: Rain
    "sde", # Snow Depth
)


def parse_gribfile(fp: str):
    """
    """
    # We are mostly interested in surface variables
    try:
        ds = xr.open_dataset(fp, backend_kwargs=GRB2_SURFACE_FILTER, engine="cfgrib")
        ds = ds.drop_vars("step")
    except KeyError:
        logger.warning(
            "GRB2 file {fp} does not contain 'surface' data.", fp=fp
        )
    data_vars = list(filter(lambda data_var: data_var in ds.data_vars, SURFACE_VARS))
    # Only retain variables of interest
    ds = ds[data_vars]
    return ds.to_dataframe().reset_index()


def parse_data(gfs_grb2_df: pd.DataFrame, envelope_gdf: gpd.GeoDataFrame):
    """Read in a GFS GRB2 file and bound it by a geodataframe of envelopes.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame of related GFS points.
    """
    df = gfs_grb2_df
    # Assert that there are no rows which are not poly types in our envelopes.
    assert (~envelope_gdf.geom_type.isin(POLY_TYPES)).sum() == 0

    gdf = grb2gdf(df)
    gdf = gpd.tools.sjoin(
        gdf,
        envelope_gdf.to_crs(GRB2_CRS),
        op="within",
        how="inner"
    )
    return gdf


def parse_gfs_archive(gfs_archive, forecasts=[]):
    """Parse a GFS Archive file
    """
    logger.info("Parsing and unpacking {fp}", fp=gfs_archive)
    to_parse = []
    with tarfile.open(gfs_archive) as tf:
        tmpdir = Path(tempfile.mkdtemp())
        for tarinfo in tf:
            logger.debug("Parsing {fp}/{fn}", fp=gfs_archive, fn=tarinfo.name)
            # last 8 characters represent forecast
            forecast = tarinfo.name[-8:-5]
            if forecast not in forecasts:
                continue

            fp = (Path(tmpdir)/tarinfo.name).absolute()

            # Extract member from tar archive
            tf.extract(tarinfo, path=tmpdir)

            to_parse.append((fp, tarinfo.name))

    for fp, name in to_parse:
        # Parse single forecast
        gdf = parse_gribfile(fp)

        # Yield GeoDataFrame of weather data
        yield gdf, name

        for fp_ in fp.parent.glob(f"{fp.name}*"):
            os.remove(fp_)
    tmpdir.rmdir()


def get_gfs_archives(project, prefix="grid3"):
    """Download and yield pre-downloaded data from GFS grid 3.
    """
    client = storage.Client(project)
    blobs = client.list_blobs("carecur-gfs-data", prefix=prefix)
    for blob in blobs:
        yield blob


def parse_archive(gfs_archive, envelope, forecasts=[]):
    """Main parsing function for unit of data (single NOAA Archive)

    Args:
        gfs_archive (google.cloud.storage.blob.Blob): Storage blob object.
    """
    
    fd, fp_ = tempfile.mkstemp()

    # Download file from storage
    gfs_archive.download_to_filename(fp_)

    # FIXME: Some 2019 files are throwing a keyerror.  We should resolve that error instead of silently failing.
    try:
        for gdf, fn in parse_gfs_archive(fp_, forecasts=forecasts):
            fn = Path(fn).stem
            gdf = parse_data(gdf, envelope)
            gdf.drop(columns="geometry").to_parquet(OUTPUT_DIR/f"{fn}.parquet")
    except Exception as e:
        logger.exception(e)

    os.close(fd)
    os.remove(fp_)


@click.command()
@click.argument("project", type=str, )
@click.option("-y", "--year", "year", type=int, required=True, help="Forecast year to parse")
@click.option("-m", "--month", "month", type=int, required=True, help="Forecast year to parse")
@click.option("-d", "--day", "day", type=int, required=False, help="Forecast day to parse.")
def main(project, year, month, day=None):
    """Parse through gfs grid-3 archive data for a single year and month.

    Arguments:

        project:  Google cloud project to bill data access to.
    """
    envelope = gpd.read_file(settings.DATA_DIR / "processed/geography/CA_Counties/CA_Counties_TIGER2016.shp")
    # Parse out months as well!

    # Archive data only exist in this bucket for 2017 to 2019
    assert year in range(2017, 2020)
    assert month in range(1, 13)

    prefix=f"grid3/gfs_3_{year}{month:02}"
    if day:
        prefix = f"{prefix}{day:02}"

    # Point of forecast, day ahead, and 7-day
    # Note that forecasts are expresed as UTC, so we must account for US/Pacific.
    forecasts = [
        # Day of
        *(f"{i*3:03}" for i in range(5, 10)),

        # 24-hour
        *(f"{i*3:03}" for i in range(13, 18)),

        # 7-day
        *(f"{i*3:03}" for i in range(61, 66))
    ]
    for gfs_archive in get_gfs_archives(project, prefix=prefix):
        lockfile = OUTPUT_DIR / "status" / f"{gfs_archive.name}.txt.lock"
        if not lockfile.parent.exists():
            lockfile.parent.mkdir(exist_ok=True, parents=True)
        lock = FileLock(lockfile, timeout=5)
        status_file = OUTPUT_DIR / "status" / f"{gfs_archive.name}.txt"

        # Check for lock file first
        with lock:
            if status_file.exists():
                logger.debug("Skipping... {} already processed.", gfs_archive.name)
                continue
            with status_file.open("w") as fh:
                fh.write(f"Started: {datetime.now()}")

        logger.info("Parsing archive {}", gfs_archive)
        parse_archive(gfs_archive, envelope, forecasts=forecasts)
    
        with status_file.open("w") as fh:
            fh.write(f"Completed: {datetime.now()}")
        lock.release()


if __name__ == "__main__":
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    main()
