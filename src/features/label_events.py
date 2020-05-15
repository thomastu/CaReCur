"""The purpose of this script is to add a binning to define "buckets" of curtailment events at different cutoffs.

Each of these will be used in a logistic regression to test different sensitivities.

To start, curtailment events will be defined as cutoff values in daily curtailment events.
"""
import pandas as pd

from src.conf import settings

OUTPUT_DIR = settings.DATA_DIR / "processed/training/"


if __name__ == "__main__":

    data = pd.read_parquet(OUTPUT_DIR / "0_labeled_data_daily.parquet")

    cutoffs = [
        0.01,
        0.05,
        0.1,
    ]

    for cutoff in cutoffs:
        data[f"curtailment_event_{cutoff:.2f}"] = data["solar_curtailment"]/data["solar"] > cutoff
    
    data.to_parquet(OUTPUT_DIR / "1_labeled_curtailment_events.parquet")