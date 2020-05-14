"""
Schema:
    OBJECTID_12_13                     int64
    Plant_ID                          object
    Plant_Alias                       object
    Plant_Label                       object
    MW                               float64
    Gross_MWh                        float64
    Net_MWh                          float64
    General_Fuel                      object
    Status                            object
    STEP_License_Status               object
    Gen_Count                        float64
    Initial_Start_Date               float64
    Online_Year                      float64
    Retire_Date                      float64
    Docket_ID                         object
    CEC_Sited_Flag                    object
    STEP_Project_Type                 object
    REAT_ID                           object
    WREGIS_ID                         object
    County                            object
    State_                            object
    Peaker                           float64
    Renewable_Energy                 float64
    CEC_Jurisdictional_Plant          object
    CEC_Data_Source                   object
    Wind_Resource                     object
    LRA                               object
    Sub_Area                          object
    Service_Area                      object
    Service_Category                  object
    Caliso_Balancing_Authorities      object
    Air_District                      object
    Air_Basin                         object
    Quad                              object
    Senate_District                   object
    Assembly_District                 object
    Congressional_District            object
    CES30_PercentileRange             object
    CES30_Percentile                 float64
    Website                           object
    CEC_Link                          object
    Aerial                            object
    C_Comments                        object
    Longitude                        float64
    Latitude                         float64
    Elevation                        float64
    Operation_Job                    float64
    Property_Tax                     float64
    Capacity_Factor                  float64
    Income_Percent                   float64
    Project_Location                  object
    Creator                           object
    Creator_Date                       int64
    Last_Editor                       object
    Last_Editor_Date                   int64
    GlobalID                          object
    geometry                        geometry
    dtype: object
"""
import pandas as pd
import geopandas as gpd

from enum import Enum

from src.conf import settings

OUTPUT = settings.DATA_DIR / f"processed/geography/powerplants.parquet"


class FuelTypes(Enum):
    SOLAR = "Solar"
    GAS = "Gas"
    HYDRO = "Hydro"
    WIND = "Wind"
    LANDFILL_GAS = "Landfill Gas"
    GEOTHERMAL = "Geothermal"
    BIOMASS = "Biomass"
    DIGESTER_GAS = "Digester Gas"
    COAL = "Coal"
    SOLAR_THERMAL = "Solar Thermal"
    BATTERY = "Battery"
    MSW = "MSW"
    NUCLEAR = "Nuclear"


def main():

    powerplants = gpd.read_file(
        settings.DATA_DIR / f"raw/geography/powerplants.geojson", crs="CRS84"
    )
    # Drop powerplants with missing locations
    powerplants = powerplants.dropna(subset=["geometry", "Initial_Start_Date"]).copy()

    # Convert dates
    powerplants["_online_date"] = pd.to_datetime(
        powerplants["Initial_Start_Date"], unit="ms"
    )
    powerplants["_retire_date"] = pd.to_datetime(powerplants["Retire_Date"], unit="ms")

    # Filter for solar (and maybe wind in the future)
    powerplants = powerplants[
        powerplants["General_Fuel"].isin(
            [FuelTypes.SOLAR.value, FuelTypes.SOLAR_THERMAL.value]
        )
    ].copy()
    column_mappings = {
        "MW": "capacity_mw",
        "County": "county_name",
        "State_": "state",
        "_online_date": "online_date",
        "_retire_date": "retire_date",
        "Longitude": "longitude",
        "Latitude": "latitude",
        "General_Fuel": "fuel",
    }
    output_columns = list(column_mappings.values())
    powerplants = powerplants.rename(columns=column_mappings)

    # Ensure all lat-lon coords are consistent with their geometry (some are different!)
    # Note that CRS84 swaps lat-lon!
    powerplants["longitude"] = powerplants["geometry"].x
    powerplants["latitude"] = powerplants["geometry"].y
    powerplants[output_columns].to_parquet(OUTPUT, index=False, engine="fastparquet")


if __name__ == "__main__":

    main()
