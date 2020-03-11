"""
Input:

parquet: 15-min timestamp index

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

Output:

parquet: Integer Index

    timestamp                         datetime64[ns, UTC]
    load                                          float64
    solar                                         float64
    wind                                          float64
    net_load                                      float64
    renewables                                    float64
    nuclear                                       float64
    large_hydro                                   float64
    imports                                       float64
    generation                                    float64
    thermal                                       float64
    load_less_(generation+imports)                float64
    wind_curtailment                              float64
    solar_curtailment                             float64
"""
import pandas as pd

from src.conf import settings

OUTPUT_DIR = settings.DATA_DIR / "processed/caiso_hourly/"

def main():
    INPUT_DIR = settings.DATA_DIR / "processed/caiso/"
    export_columns = [
        "solar_curtailment", "solar", "net_load", "load", 
        "generation", "renewables", "wind_curtailment"
    ]
    for dataset in INPUT_DIR.glob("*.parquet"):
        output_fp = OUTPUT_DIR / dataset.name
        df = pd.read_parquet(dataset)
        # Rename columns
        column_map = zip(
            df.columns.tolist(),
            df.columns.str.replace("\W+", "_").str.replace("\W$", "").str.lower().tolist()
        )
        df.rename(columns=dict(column_map), inplace=True)
        # Convert to MWH
        df *= (5/60.)  # Convert to MWH
        # Roll up to hourly
        df = df.groupby(
            by=pd.Grouper(freq="H")
        )[export_columns].sum()
        
        # If the file already exists, assume we should be appending to it
        append = output_fp.exists()

        df.to_parquet(
                output_fp, 
                engine="fastparquet",
                append=append
            )

if __name__ == "__main__":

    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Create Outputs
    main()

    