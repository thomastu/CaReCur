"""
Normalize curtailment data form the CAISO.

The goal of this module is to produce a set of year/month.

Inputs:

xlsx:sheet_name="Production"

    Date                              datetime64[ns]
    Hour                                       int64
    Interval                                   int64
    Load                                     float64
    Solar                                    float64
    Wind                                     float64
    Net Load                                 float64
    Renewables                               float64
    Nuclear                                  float64
    Large Hydro                              float64
    Imports                                  float64
    Generation                               float64
    Thermal                                  float64
    Load Less (Generation+Imports)           float64

xlsx:sheet_name="Curtailments"

    Date                 datetime64[ns]
    Hour                          int64
    Interval                      int64
    Wind Curtailment            float64
    Solar Curtailment           float64

Outputs:

A parquet dataset with grain of 5-min resolution datetimes.

parquet

    Load                              float64
    Solar                             float64
    Wind                              float64
    Net Load                          float64
    Renewables                        float64
    Nuclear                           float64
    Large Hydro                       float64
    Imports                           float64
    Generation                        float64
    Thermal                           float64
    Load Less (Generation+Imports)    float64
    Wind Curtailment                  float64
    Solar Curtailment                 float64
"""
import pandas as pd
import warnings

from pathlib import Path
from loguru import logger

from src.conf import settings


# Ignore pandas warnings about str.contains - we are using it as a mask!
warnings.filterwarnings("ignore", 'This pattern has match groups')


def parse(fp):
    """
    Pipeline:
        
    1.  Align each row for Production and Curtailments table to a single timeseries
    2.  Merge both tables to a single table
    3.  Write out a utc-aware timestamp indexed file with all MW columns
    """
    xl_file = pd.ExcelFile(fp)
    production = xl_file.parse("Production")
    curtailments = xl_file.parse("Curtailments")

    # Parse Production Timestamps -
    # "Date" is a naive timestamp in US/Pacific
    # Ambiguous timestamps should be inferred since the data are ordered and hopefully complete
    production["timestamp"] = production["Date"].dt.tz_localize(tz="US/Pacific", ambiguous="infer")

    # Parse Curtailments Timestamps
    # "Date" is a naive date string with no time info
    curtailments["timestamp"] = (
        curtailments["Date"] + \
        # Add the Curtailment Hour; 1-indexed => 1 == starting hour midnight
        pd.to_timedelta(curtailments["Hour"] - 1, unit="H") + \
        # Add the Curtailment interval; 5-min, and 1-indexed => 1 == start of the hour
        pd.to_timedelta((curtailments["Interval"] - 1)*5, unit="min")
    ).dt.tz_localize(tz="US/Pacific", ambiguous=True)

    data = production.set_index("timestamp", verify_integrity=True).merge(
        curtailments.set_index("timestamp", verify_integrity=True), 
        left_index=True, right_index=True, how="left"
    )

    # Check that merging data left us with the correct shape
    assert data.shape[0] == production.shape[0]
    assert data.shape[1] == production.shape[1] + curtailments.shape[1] - 2  # subtract 2 for idx cols
    
    # Note that we expect to lose some curtailments data because the production data is not complete!
    try:
        assert curtailments["Solar Curtailment"].notnull().sum() == data["Solar Curtailment"].notnull().sum()
    except AssertionError:
        logger.warning(
            "Production data and curtailment data failed to line up.  The CAISO data may be missing data. [{timestamps}]",
            timestamps=curtailments.set_index("timestamp", verify_integrity=True).index.difference(data.index).tolist()
        )

    cols_to_drop = data.columns[data.columns.str.contains("_(x|y)$")].tolist()
    logger.debug("Dropping columns from CAISO data: {cols}", cols=cols_to_drop)
    data.drop(columns=cols_to_drop, inplace=True)
    return data


def main(output_dir=settings.DATA_DIR / "processed/caiso"):
    """Parse each CAISO file, and output a single timeseries per year.
    """
    output_dir.mkdir(exist_ok=True)
    for fp in settings.DATA_DIR.glob("raw/caiso/*xlsx"):
        logger.info("Parsing raw caiso file: {fp}", fp=fp)
        data = parse(fp)
        for partition, df in data.groupby(pd.Grouper(level=0, freq="Y")):
            name = f"{partition.year}.parquet"
            # Write to UTC to avoid ambiguous DST timestamps!

            output_fp = output_dir/name

            # If the file already exists, assume we should be appending to it
            append = output_fp.exists()

            logger.info("Writing (append={append}) {year} data from {fp}", append=append, year=partition.year, fp=fp)

            df.tz_convert("UTC").to_parquet(
                output_fp, 
                engine="fastparquet",
                append=append
            )


if __name__ == "__main__":
    # Clean Data
    main()