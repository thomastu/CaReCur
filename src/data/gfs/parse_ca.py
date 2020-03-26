"""
Filters down a directory of grb2 files to a specific geometry.
"""
import os
import pandas as pd
import geopandas as gpd
import xarray as xr
import tarfile
import tempfile

from pathlib import Path
from loguru import logger


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


CRS = "WGS84"
"""The GFS uses the WGS 84 Coordinate Reference System
"""


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
    assert (~envelope_gdf.geom_type.isin(POLY_TYPES).sum()) == 0

    # Convert Longitude3 (0, 360) to Longitude (-180, 180)
    lon = df["longitude"].where(df["longitude"] < 180, df["longitude"] - 360)
    # GRIB2 already has this as (-90, 90)
    lat = df["latitude"]
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(lon, lat), crs=CRS)
    gdf = gpd.tools.sjoin(
        gdf,
        envelope_gdf.to_crs(CRS),
        op="within",
        how="inner"
    )
    return gdf


def parse_gfs_archive(gfs_archive):
    """Parse a GFS Archive file
    """
    with tarfile.open(gfs_archive) as tf:
        tmpdir = Path(tempfile.mkdtemp())
        for tarinfo in tf:
            fp = (Path(tmpdir)/tarinfo.name).absolute()
            # Extract member from tar archive
            tf.extract(tarinfo, path=fp)

            # Parse single forecast
            gdf = parse_gribfile(fp)

            # Yield GeoDataFrame of weather data
            yield gdf

            for fp_ in fp.parent.glob(f"{fp.name}*"):
                os.remove(fp_)

        tmpdir.rmdir()


def main(gfs_archives, ca_envelope):

    # TODO:100 Figure out what are the different boundaries we might want!
    # ca = gpd.read_file("./CA_Counties/CA_Counties_TIGER2016.shp")
    ca = None

    for gfs_archive in gfs_archives:
        for gdf in parse_gfs_archive(gfs_archive):
            gdf = parse_data(gdf, ca)
            # TODO: Write GDF and send somewhere!!
            # save_results(gs://...)


if __name__ == "__main__":

    # TODO:100 Get gfs_archives
    # main()