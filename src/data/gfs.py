"""WIP: Parser for the Global Forecast System (GFS).

This module is responsible for parsing archive grb2 files.
"""
import xarray as xr


if __name__ == "__main__":
    """Trying exploratory work with VSCode Pyhton Notebook.  You win Microsoft : (

    Next step is to refactor this into useful functions and interfaces.
    """


    dataset_kwargs = {
        "filter_by_keys": {"typeOfLevel": "surface"}
    }

    dataset = xr.open_dataset(
        "../../data/raw/gfs/gfs_4_20190101_0000_000.grb2",
        backend_kwargs=dataset_kwargs,
        engine="cfgrib",
    )

    # We are interested in the following variables to achieve some
    # kind of compatibility with pvlib's forecast and clearsky modules.

    # temp_air
    dataset["t"]

    # wind_speed_gust
    dataset["gust"]

    # wind_speed_u

    # wind_speed_v
    # total_clouds
    # low_clouds
    # mid_clouds
    # high_clouds
    # boundary_clouds
    # convect_clouds
    # ghi_raw
