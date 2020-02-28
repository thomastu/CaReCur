"""WIP: Parser for the Global Forecast System (GFS).

This module is responsible for parsing archive grb2 files.
"""
import xarray as xr
from loguru import logger
from src.conf import settings


if __name__ == "__main__":
    """Read .grb2 files as xarrays, and write results as a parquet dataset.
    """

    gfs_dir = settings.DATA_DIR / "raw/gfs/"
    files_ = gfs_dir.glob("*grb2")
    data_vars = {
        "surface": [
            "t", # Air Temperature
            "gust", # Wind speed
            "tp", # Precipitation
        ],
        # "isobaricInhPa": [
        #    "u", #"wind_speed_u"
        #    "v", #"wind_speed_v"
        #    "r", #"relative_humidity"
        #   ]
        "atmosphere": [
            "tcc" # Total Cloud Cover
        ],
    }
    data = None
    for fp_ in list(files_):
        logger.info("Parsing {fp}", fp=fp_)
        _datasets = []
        for level, dvars in data_vars.items():
            dataset_kwargs = {"filter_by_keys": {"typeOfLevel": level}}
            try:
                ds = xr.open_dataset(fp_, backend_kwargs=dataset_kwargs, engine="cfgrib")
                ds = ds.drop_vars("step")
            except KeyError:
                logger.warning("GRB2 file {fp} does not contain {level}", fp=fp_, level=level)
                continue
            _datasets.append(ds[[dvar for dvar in dvars if dvar in ds.data_vars]])
        if data is not None:
            data = xr.merge([data, xr.combine_by_coords(_datasets).expand_dims("valid_time")])
        else:
            data = xr.combine_by_coords(_datasets).expand_dims("valid_time")

    ddf = data.to_dask_dataframe()
    ddf.to_parquet(settings.DATA_DIR / "processed/gfs/",  partition_on=["latitude", "longitude"])

