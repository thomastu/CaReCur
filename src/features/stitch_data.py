"""Build daily-level feature sets, stitching together weather datasets and defining features.
"""
import numpy as np
import pandas as pd
import geopandas as gpd

from dask import dataframe as dd
from loguru import logger
from shapely.ops import nearest_points

from src.data.gfs.utils import grb2gdf
from src.conf import settings

start_year = 2017
end_year = 2019

OUTPUT_DIR = settings.DATA_DIR / "processed/training/"


if __name__ == "__main__":

    df = pd.concat(
        [
            pd.read_parquet(settings.DATA_DIR / f"processed/caiso_hourly/{y}.parquet")
            for y in range(2017, 2020)
        ]
    )

    df.index = df.index.tz_convert("US/Pacific")

    # Preprocessed hourly data is in MWh, so we can simply sum up to resample to days
    df = df.groupby(pd.Grouper(freq="D")).sum()

    df.reset_index(inplace=True)

    # By construction, we are interested in Feb to May (inclusive)
    season_filter = df["timestamp"].dt.month.isin(range(2, 6))
    df = df[season_filter]

    # Define whether something is a weekday/weekend
    df["is_weekday"] = df["timestamp"].dt.weekday.isin([5, 6])

    # Integrate forecast data
    gfs_data_files = (
        settings.DATA_DIR
        / f"interim/gfs/ca/gfs_3_201[7-9][01][2-5]*_0000_{i*3:03}.parquet"
        for i in range(5, 10)
    )
    forecasts = [*(gfs_data_files)]
    dayahead_weather = dd.read_parquet(forecasts).compute()

    # Add UTC timezone and convert to US/Pacific
    dayahead_weather["timestamp"] = (
        dayahead_weather["valid_time"].dt.tz_localize("UTC").dt.tz_convert("US/Pacific")
    )

    dayahead_weather = grb2gdf(dayahead_weather)
    # Include powerplant data
    counties = gpd.read_file(
        settings.DATA_DIR / "processed/geography/CA_Counties/CA_Counties_TIGER2016.shp"
    )
    weather_point_measurements = dayahead_weather["geometry"].geometry.unary_union

    powerplants = pd.read_parquet(
        settings.DATA_DIR / f"processed/geography/powerplants.parquet"
    )

    # Add geometry
    powerplants = gpd.GeoDataFrame(
        powerplants,
        geometry=gpd.points_from_xy(powerplants["longitude"], powerplants["latitude"]),
        crs="EPSG:4326",
    )

    powerplants["geometry"] = (
        powerplants["geometry"]
        .apply(lambda x: nearest_points(x, weather_point_measurements))
        .str.get(1)
    )

    # In order to integrate powerplant data, we have to merge on the powerplant's closest county location.
    powerplants = gpd.tools.sjoin(
        powerplants.to_crs("EPSG:4326"),
        counties[["GEOID", "geometry"]].to_crs("EPSG:4326"),
        op="within",
        how="left",
    )
    powerplants["online_date"] = powerplants["online_date"].dt.tz_localize("US/Pacific")
    powerplants["retire_date"] = powerplants["retire_date"].dt.tz_localize("US/Pacific")

    # Now group over GEOIDs, and sum up the capacity
    # For each month, we have to only associate capacity for powerplants that were online.
    weather_orig = dayahead_weather.copy()
    capacities = {}
    results = []
    for date, weather_df in dayahead_weather.groupby(
        pd.Grouper(key="timestamp", freq="MS"), as_index=False
    ):
        if weather_df.empty:
            logger.warning("Weather data for {date} is empty!", date=date)
            continue
        logger.debug("Assigning capacity for weather points as of {date}.", date=date)
        valid_plants = (powerplants["online_date"] <= date) & (
            powerplants["retire_date"].isnull() | (powerplants["retire_date"] > date)
        )
        valid_plants = powerplants[valid_plants]
        county_mw = valid_plants.groupby("GEOID", as_index=False)["capacity_mw"].sum()
        weather_df = weather_df.merge(county_mw, on="GEOID", how="left")
        weather_df["capacity_mw"] = weather_df["capacity_mw"].fillna(0)
        results.append(weather_df)

    # Note that this is still on the original df grain as we did not aggregate the groupby!
    dayahead_weather = pd.concat(results, ignore_index=True)

    # Roll-up to dailies
    daily_capacity = (
        dayahead_weather.groupby(by=["GEOID", pd.Grouper(key="timestamp", freq="D")])[
            "capacity_mw"
        ]
        .mean()
        .reset_index()
        .groupby(by=pd.Grouper(key="timestamp", freq="D"))["capacity_mw"]
        .sum()
    )

    county_level_dailies = dayahead_weather.groupby(
        by=["GEOID", pd.Grouper(key="timestamp", freq="D")], as_index=True
    ).agg(
        t_min=("t", "min"),
        t_max=("t", "max"),
        t_mean=("t", "mean"),
        dswrf_mean=("dswrf", "mean"),
        dswrf_max=("dswrf", "max"),
        capacity_mw=("capacity_mw", "mean"),
    ).reset_index()

    def weighted_mean_factory(weight_col):
        def weighted_avg(s):
            if s.empty:
                return 0.0
            else:
                return np.average(s, weights=dayahead_weather.loc[s.index, weight_col])

        weighted_avg.__name__ = f"{weight_col}_wmean"

        return weighted_avg

    # GFS is missing certain days for one reason or another.
    # Furthermore, pandas timestamps fill in timesteps to build a full frequency datetime
    # Since we don't have continuity in time, we ignore those.
    dayahead_daily = (
        county_level_dailies.groupby(by=pd.Grouper(key="timestamp", freq="D"),)
        .agg(
            t_mean=pd.NamedAgg(column="t_mean", aggfunc="mean"),  # K
            t_wmean=pd.NamedAgg(
                column="t_mean", aggfunc=weighted_mean_factory("capacity_mw")
            ),  # K
            t_wmax=pd.NamedAgg(
                column="t_max", aggfunc=weighted_mean_factory("capacity_mw")
            ),  # K
            t_wmin=pd.NamedAgg(
                column="t_min", aggfunc=weighted_mean_factory("capacity_mw")
            ),  # K
            t_absmax=pd.NamedAgg(column="t_max", aggfunc="max",),  # K
            t_absmin=pd.NamedAgg(column="t_min", aggfunc="min",),  # K
            dswrf_mean=pd.NamedAgg(column="dswrf_mean", aggfunc="mean"),  # W/m^2
            dswrf_absmax=pd.NamedAgg(column="dswrf_max", aggfunc="max"),  # W/m^2
            dswrf_wmean=pd.NamedAgg(
                column="dswrf_mean", aggfunc=weighted_mean_factory("capacity_mw")
            ),  # W/m^2
            capacity_mw=pd.NamedAgg(column="capacity_mw", aggfunc="sum"),  # MW
        )
        .dropna(subset=["t_mean", "dswrf_mean"], how="any")
    )

    dayahead_daily["installed_capacity"] = dayahead_daily.index.map(daily_capacity)

    daily_feature_data = df.merge(dayahead_daily, on="timestamp", how="inner")

    daily_feature_data["solar_capacity_factor"] = daily_feature_data["solar"] / (
        daily_feature_data["installed_capacity"] * 24
    )

    daily_feature_data.to_parquet(
        OUTPUT_DIR / "0_labeled_data_daily.parquet", engine="fastparquet"
    )
