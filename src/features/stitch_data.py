"""Build daily-level feature sets, stitching together weather datasets and defining features.


Curtailment Data:

    timestamp                         datetime64[ns, US/Pacific]
    load                                                 float64
    solar                                                float64
    wind                                                 float64
    net_load                                             float64
    renewables                                           float64
    nuclear                                              float64
    large_hydro                                          float64
    imports                                              float64
    generation                                           float64
    thermal                                              float64
    load_less_(generation+imports)                       float64
    wind_curtailment                                     float64
    solar_curtailment                                    float64


Weather Data:

    latitude                          float64
    longitude                         float64
    t                                 float32
    gust                              float32
    tp                                float32
    dswrf                             float32
    uswrf                             float32
    SUNSD                             float32
    al                                float32
    sp                                float32
    csnow                             float32
    cicep                             float32
    cfrzr                             float32
    crain                             float32
    sde                               float32
    surface                             int64
    time                       datetime64[ns]
    valid_time                 datetime64[ns]
    index_right                         int64
    STATEFP                            object
    COUNTYFP                           object
    COUNTYNS                           object
    GEOID                              object
    NAME                               object
    NAMELSAD                           object
    LSAD                               object
    CLASSFP                            object
    MTFCC                              object
    CSAFP                              object
    CBSAFP                             object
    METDIVFP                           object
    FUNCSTAT                           object
    ALAND                               int64
    AWATER                              int64
    INTPTLAT                           object
    INTPTLON                           object
    timestamp      datetime64[ns, US/Pacific]

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

TRAINING_DIR = settings.DATA_DIR / "processed/training/"


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
    daily_capacity = (
        dayahead_weather.groupby(by=["GEOID", pd.Grouper(key="timestamp", freq="D")])[
            "capacity_mw"
        ]
        .mean()
        .reset_index()
        .groupby(by=pd.Grouper(key="timestamp", freq="D"))["capacity_mw"]
        .sum()
    )

    dayahead_daily = (
        dayahead_weather.groupby(
            by=pd.Grouper(key="timestamp", freq="D"), observed=True,
        )
        .agg(
            mean_temperature=pd.NamedAgg(column="t", aggfunc="mean"),
            downward_shortwave_radiation=pd.NamedAgg(column="dswrf", aggfunc="mean"),
            upward_shortwave_radation=pd.NamedAgg(column="uswrf", aggfunc="mean"),
            sunshine_duration=pd.NamedAgg(column="SUNSD", aggfunc="mean"),
            dswrf_weighted_temperature=pd.NamedAgg(
                column="t",
                aggfunc=lambda s: 0.0
                if s.empty
                else np.average(s, weights=dayahead_weather.loc[s.index, "dswrf"]),
            ),
            capacity_weighted_temperature=pd.NamedAgg(
                column="t",
                aggfunc=lambda s: 0.0
                if s.empty
                else np.average(
                    s, weights=dayahead_weather.loc[s.index, "capacity_mw"]
                ),
            ),
        )
        .dropna(subset=["mean_temperature",], how="any")
    )

    dayahead_daily["installed_capacity"] = dayahead_daily.index.map(daily_capacity)

    feature_data = df.merge(dayahead_daily, on="timestamp", how="inner")

    feature_data["solar_capacity_factor"] = feature_data["solar"] / (
        feature_data["installed_capacity"] * 24
    )
    # timestamp                         datetime64[ns, US/Pacific]
    # load                                                 float64
    # solar                                                float64
    # wind                                                 float64
    # net_load                                             float64
    # renewables                                           float64
    # nuclear                                              float64
    # large_hydro                                          float64
    # imports                                              float64
    # generation                                           float64
    # thermal                                              float64
    # load_less_(generation+imports)                       float64
    # wind_curtailment                                     float64
    # solar_curtailment                                    float64
    # is_weekday                                              bool
    # mean_temperature                                     float32
    # downward_shortwave_radiation                         float32
    # upward_shortwave_radation                            float32
    # sunshine_duration                                    float32
    # dswrf_weighted_temperature                           float32
    # capacity_weighted_temperature                        float32
    # installed_capacity                                   float64
    # solar_capacity_factor                                float64

    feature_data.to_parquet(
        TRAINING_DIR / "0_labeled_data.parquet", engine="fastparquet"
    )
