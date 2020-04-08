"""

Introduction to GRIB2 Files

https://www.ftp.cpc.ncep.noaa.gov/wd51we/wgrib2/intro_grib2.pdf
"""
import re
import pandas as pd
import geopandas as gpd

from dataclasses import dataclass, field

pat = re.compile(
    (
        r"(?P<message_num>\d+)"
        r"(\.(?P<submessage_num>\d+))?"
        r":(?P<location>\d+):"
        r"d=(?P<reference_time>\d+)"
        r":(?P<variable_name>\w+)"
        r":(?P<z>[^:]+)"
        r":(?P<dtime>[^:]+):"
    )
)


class InvalidGRIBInventory(Exception):
    pass


GRB2_CRS = "WGS84"
"""The GFS uses the WGS 84 Coordinate Reference System
"""

def parse_grib2_inventory(s):
    """
    """
    match = pat.match(s)
    if match is None:
        raise InvalidGRIBInventory(f"{s} is not a valid GRIB2 inventory.")
    return match.groupdict()


def grb2gdf(df):
    """Convert a grb2 DataFrame to a grb2 GeoDataframe based on the GFS grb2 spec.

    Specifically, long-lats are converted to proper WGS84 coordinates
    and used to create a geometry for each row with geopandas.

    Args:
        df (pd.Dataframe): Dataframe of interest.
    
    Returns:
        gdf (gpd.DataFrame): Geopandas dataframe with lat-lon geometry in WGS84 CRS.
    """
    # Convert Longitude3 (0, 360) to Longitude (-180, 180)
    lon = df["longitude"].where(df["longitude"] < 180, df["longitude"] - 360)
    # GRIB2 already has this as (-90, 90)
    lat = df["latitude"]

    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(lon, lat), crs=GRB2_CRS)
    return gdf