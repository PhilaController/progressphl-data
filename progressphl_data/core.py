from __future__ import annotations

import json
from typing import Literal

import pandas as pd

from . import DATA_DIR
from .crosswalk import get_tract_neighborhood_crosswalk, get_tract_puma_crosswalk


def load_meta_data(
    tag: Literal["variables", "hierarchy"], version: Literal["1"] = "1"
) -> dict | list:
    """Load metadata"""
    meta_data_dir = DATA_DIR / f"v{version}" / "meta"
    return json.load((meta_data_dir / f"{tag}.json").open("r"))


def get_spi_data(version: Literal["1", "2"] = "2") -> pd.DataFrame:
    """Load processed SPI data."""

    # Data dir for this version
    data_dir = DATA_DIR / f"v{version}"
    if not data_dir.exists():
        raise ValueError(f"No data for specified version '{version}'")

    # Load the raw dataframe
    if version == "1":
        filename = data_dir / "SPI Philly Tableau.xlsx"
        spi_data = pd.read_excel(
            filename,
            sheet_name="SPI All",
            dtype={"geoid": str, "tract_name": str},
        ).set_index(["geoid", "tract_name"])
    elif version == "2":
        filename = data_dir / "ProgressPHL_Recalculated_v1.xlsx"

        # Read SPI variables and combine with indicators
        spi_data = (
            pd.read_excel(
                filename,
                sheet_name="SPI",
                dtype={"geoid": str, "tract_name": str},
            )
            .set_index(["geoid", "tract_name"])
            .join(
                pd.read_excel(
                    filename,
                    sheet_name="rawvalues_indicators",
                    dtype={"geoid": str, "tract_name": str},
                ).set_index(["geoid", "tract_name"])
            )
        )

    # Rename variables
    variables = load_meta_data(tag="variables", version=version)
    spi_data = spi_data.rename(columns=variables)

    # Melt
    spi_data = spi_data.melt(ignore_index=False).sort_index()

    # Rescale variables
    need_to_rescale = [
        "associate_degree_holders",
        "eviction_rate",
        "food_stamp_usage",
        "no_plumbing",
    ]
    for col in need_to_rescale:
        sel = spi_data["variable"] == col
        spi_data.loc[sel, "value"] *= 100

    # Add parent
    hierarchy = load_meta_data(tag="hierarchy", version=version)
    for k, v in hierarchy.items():
        sel = spi_data["variable"].isin(v)
        assert k in spi_data["variable"].unique()
        spi_data.loc[sel, "parent"] = k

    # Figure out which ones are inverted
    definitions = load_meta_data(tag="definitions", version=version)
    inverted = [name for name, d in definitions.items() if d["inverted"]]

    # Add ranks
    t = []
    for var_name, grp in spi_data.groupby("variable"):
        grp["rank"] = grp.value.rank(ascending=var_name in inverted, method="min")
        t.append(grp)
    spi_data = pd.concat(t, ignore_index=False)

    # Calculate percentile ranges
    percentile_ranges = (
        spi_data.groupby("variable")["value"]
        .quantile([0.25, 0.5, 0.75])
        .reset_index()
        .rename(columns={"level_1": "quantile_range"})
        .assign(
            quantile_range=lambda df: df.quantile_range.replace(
                {0.25: "lower", 0.75: "upper", 0.5: "median"}
            )
        )
        .pivot_table(index="variable", columns="quantile_range", values="value")
    )

    def assign_average_labels(row):
        name = row["variable"]
        value = row["value"]
        ranges = percentile_ranges.loc[name].squeeze()

        if ranges["lower"] <= value <= ranges["upper"]:
            return "Average"
        elif value < ranges["lower"]:
            return "Above Average" if name in inverted else "Below Average"
        else:
            return "Below Average" if name in inverted else "Above Average"

    # Add average label
    spi_data["average_label"] = spi_data.apply(assign_average_labels, axis=1)

    # Add geo info
    tract_hood_crosswalk = get_tract_neighborhood_crosswalk()
    tract_puma_crosswalk = get_tract_puma_crosswalk()

    return (
        tract_hood_crosswalk[["tract_geoid_alt", "neighborhood_name", "tract_id"]]
        .merge(
            tract_puma_crosswalk[["tract_geoid_alt", "puma_name"]], on="tract_geoid_alt"
        )
        .merge(
            spi_data.reset_index(),
            left_on="tract_geoid_alt",
            right_on="geoid",
            how="right",
        )
        .drop(columns=["tract_geoid_alt"])
    )
