"""
Filters down a directory of grb2 files to a specific geometry.
"""
import os
import click
import pandas as pd
import geopandas as gpd
import xarray as xr
import tarfile
import tempfile
import ujson as json

from datetime import datetime
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
            "GRB2 file {fp} does not contain 'surface' data.", fp=gfs_grib2_fp
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
    with tarfile.open(gfs_archive) as tf:
        tmpdir = Path(tempfile.mkdtemp())
        for tarinfo in tf:
            logger.debug("Parsing {fp}/{fn}", fp=gfs_archive, fn=tarinfo.name)
            # last 8 characters represent forecast
            forecast = tarinfo.name[-8:-5]
            if forecast not in forecasts:
                logger.debug("Skipping... {fp}/{fn} not in forecasts.", fp=gfs_archive, fn=tarinfo.name)
                continue

            fp = (Path(tmpdir)/tarinfo.name).absolute()
            # Extract member from tar archive
            tf.extract(tarinfo, path=tmpdir)

            # Parse single forecast
            gdf = parse_gribfile(fp)

            # Yield GeoDataFrame of weather data
            yield gdf, tarinfo.name

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


def parse_archive(gfs_archive, forecasts=[]):
    """Main parsing function for unit of data (single NOAA Archive)

    Args:
        gfs_archive (google.cloud.storage.blob.Blob): Storage blob object.
    """
    envelope = gpd.read_file(settings.DATA_DIR / "processed/geography/CA_Counties/CA_Counties_TIGER2016.shp")
    _mode, fp_ = tempfile.mkstemp()

    # Download file from storage
    gfs_archive.download_to_filename(fp_)

    for gdf, fn in parse_gfs_archive(fp_, forecasts=forecasts):
        fn = Path(fn).stem
        gdf = parse_data(gdf, envelope)
        gdf.drop(columns="geometry").to_parquet(OUTPUT_DIR/f"{fn}.parquet")

    os.remove(fp_)


@click.command()
@click.argument("project", type=str, )
@click.option("-y", "--year", "year", type=int, required=False, default=None, help="Forecast year to parse")
def main(project, year):
    """Parse through gfs grid-3 archive data.

    Arguments:
    
        project:  Google cloud project to bill data access to.
    """
    # Archive data only exist in this bucket for 2017 to 2018
    if year:
        assert year in range(2017, 2020)
        prefix=f"grid3/gfs_3_{year}010"
    else:
        prefix=f"grid3"

    # Point of forecast, day ahead, 2-day, 3-day, 5-day, 7-day and 10-day
    forecasts = [
        "000", "024", "048", "072", "120", "168","240"
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
                fh.write(str(datetime.now()))
        logger.info("Parsing archive {}", gfs_archive)
        parse_archive(gfs_archive, forecasts=forecasts)

if __name__ == "__main__":
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    main()
