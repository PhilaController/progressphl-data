import collections

import numpy as np
import pandas as pd

from .crosswalk import get_tract_neighborhood_crosswalk, get_tract_puma_crosswalk
from .datasources import get_acs_by_geography
from .datasources.census.agg import aggregate_median_data
from .datasources.census.core import get_decennial
from .geo import get_pumas

GEOGRAPHIES = ["tract", "neighborhood", "puma"]


def _get_population(year=2019):
    """Get ACS population data."""

    pumas = get_pumas()
    tract_hood_crosswalk = get_tract_neighborhood_crosswalk()

    out = []
    for geography in GEOGRAPHIES:

        table = "B01001"
        variables = {table + "_001": "universe"}

        # Get data by desired geography
        data = get_acs_by_geography(
            survey="acs5", year=year, variables=variables, geography=geography
        )

        # PUMA/Tract
        if geography == "puma":
            data = data.drop(columns=["name"]).merge(pumas[["name", "id"]], on="id")
        elif geography == "tract":
            data = (
                data[["id", "estimate"]]
                .rename(columns={"id": "tract_geoid_alt"})
                .merge(
                    tract_hood_crosswalk[["tract_geoid_alt", "tract_name"]],
                    on="tract_geoid_alt",
                )
                .rename(columns={"tract_name": "name"})
            )
        out.append(data[["name", "estimate"]])

    return pd.concat(out).assign(indicator="population")


def _get_2010_population():
    out = []
    for geography in GEOGRAPHIES:

        data = get_decennial(
            variables={
                "P012001": "universe",
            },
            sumfile="sf1",
            year=2010,
            geography="tract",
        )

        if geography == "neighborhood" or geography == "puma":

            # Get the crosswalk
            if geography == "neighborhood":
                crosswalk = get_tract_neighborhood_crosswalk()
            else:
                crosswalk = get_tract_puma_crosswalk()

            # Merge
            data = data.merge(crosswalk, left_on="id", right_on="tract_geoid_alt")

            # Approximate sum over tracts
            groupby = [f"{geography}_id", f"{geography}_name"]
            data = (
                data.groupby(groupby, as_index=False)["estimate"]
                .sum()
                .rename(columns={f"{geography}_id": "id", f"{geography}_name": "name"})
            )
        else:
            crosswalk = get_tract_neighborhood_crosswalk()
            data = (
                data[["id", "estimate"]]
                .rename(columns={"id": "tract_geoid_alt"})
                .merge(
                    crosswalk[["tract_geoid_alt", "tract_name"]], on="tract_geoid_alt"
                )
                .rename(columns={"tract_name": "name"})
            )

        out.append(data[["name", "estimate"]])

    return pd.concat(out).assign(indicator=f"population_2010")


def _get_sex_by_age(year=2019):
    """Get sex by age"""

    pumas = get_pumas()
    tract_hood_crosswalk = get_tract_neighborhood_crosswalk()

    # Group sets we'll need
    groupsets = collections.OrderedDict(
        {
            "under_18": ["under_5", "5_to_9", "10_to_14", "15_to_17"],
            "18_to_34": ["18_to_19", "20", "21", "22_to_24", "25_to_29", "30_to_34"],
            "35_to_49": ["35_to_39", "40_to_44", "45_to_49"],
            "50_to_64": ["50_to_54", "55_to_59", "60_to_61", "62_to_64"],
            "65_and_over": [
                "65_to_66",
                "67_to_69",
                "70_to_74",
                "75_to_79",
                "80_to_84",
                "85_and_over",
            ],
        }
    )

    # Calculate the variables
    groups = [
        "total",
        "under_5",
        "5_to_9",
        "10_to_14",
        "15_to_17",
        "18_to_19",
        "20",
        "21",
        "22_to_24",
        "25_to_29",
        "30_to_34",
        "35_to_39",
        "40_to_44",
        "45_to_49",
        "50_to_54",
        "55_to_59",
        "60_to_61",
        "62_to_64",
        "65_to_66",
        "67_to_69",
        "70_to_74",
        "75_to_79",
        "80_to_84",
        "85_and_over",
    ]
    table = "B01001"
    variables = {table + "_001": "universe"}

    cnt = 2
    for prefix in ["male", "female"]:
        for g in groups:
            variables[table + f"_{cnt:03d}"] = f"{prefix}_{g}"
            cnt += 1

    all_data = []
    for geography in ["puma", "tract", "neighborhood"]:

        # Get data by desired geography
        data = get_acs_by_geography(
            survey="acs5", year=year, variables=variables, geography=geography
        )

        # Sum over the custom groups
        for groupset, group_list in groupsets.items():
            for tag in ["male", "female"]:

                # cols to sum over
                cols_to_sum = [f"{tag}_{f}" for f in group_list]

                # do the aggregation
                data = pd.concat(
                    [
                        data,
                        data.query("variable in @cols_to_sum")
                        .groupby(["id", "name"], as_index=False)["estimate"]
                        .sum()
                        .assign(variable=f"{tag}_{groupset}"),
                    ],
                    axis=0,
                    ignore_index=True,
                )

        # Final groups we want
        final_groups = ["under_18", "18_to_34", "35_to_49", "50_to_64", "65_and_over"]

        # Calculate percent of each group relative to total population
        out = []
        for sex in ["male", "female"]:

            def calculate_percent(grp):
                sel = grp["variable"] == "universe"
                norm = grp.loc[sel, "estimate"].squeeze()
                out = grp[~sel].copy()
                out["estimate"] /= norm
                return out

            X = (
                data.loc[
                    data.variable.isin(
                        [f"{sex}_{grp}" for grp in final_groups] + ["universe"]
                    )
                ]
                .drop(columns=["moe"])
                .groupby(["id", "name"], group_keys=False)
                .apply(calculate_percent)
            )
            out.append(X)

        out = pd.concat(out)

        # PUMA/Tract
        if geography == "puma":
            out = out.drop(columns=["name"]).merge(pumas[["name", "id"]], on="id")
        elif geography == "tract":
            out = (
                out[["id", "estimate", "variable"]]
                .rename(columns={"id": "tract_geoid_alt"})
                .merge(
                    tract_hood_crosswalk[["tract_geoid_alt", "tract_name"]],
                    on="tract_geoid_alt",
                )
                .rename(columns={"tract_name": "name"})
            )

        all_data.append(out[["name", "estimate", "variable"]])

    return pd.concat(all_data, axis=0, ignore_index=True).rename(
        columns={"variable": "indicator"}
    )


def _get_median_household_income(year=2019):
    """Get median household income."""

    pumas = get_pumas()
    tract_hood_crosswalk = get_tract_neighborhood_crosswalk()

    out = []
    for geography in GEOGRAPHIES:

        # Pull exact data
        if geography in ["tract", "puma"]:

            table = "B19013"
            variables = {table + "_001": "median"}

            # Get data by desired geography
            data = get_acs_by_geography(
                survey="acs5", year=year, variables=variables, geography=geography
            )

        # Estimate from tract level for neighborhood
        else:
            # Crosswalk to neighborhoods
            crosswalk = get_tract_neighborhood_crosswalk()

            # Get the data
            table = "B19001"
            variables = {
                table + "_001": "universe",
                table + "_002": "0_to_9999",
                table + "_003": "10000_to_14999",
                table + "_004": "15000_to_19999",
                table + "_005": "20000_to_24999",
                table + "_006": "25000_to_29999",
                table + "_007": "30000_to_34999",
                table + "_008": "35000_to_39999",
                table + "_009": "40000_to_44999",
                table + "_010": "45000_to_49999",
                table + "_011": "50000_to_59999",
                table + "_012": "60000_to_74999",
                table + "_013": "75000_to_99999",
                table + "_014": "100000_to_124999",
                table + "_015": "125000_to_149999",
                table + "_016": "150000_to_199999",
                table + "_017": "200000_or_more",
            }

            # Get data in wide format with tract/neighborhood info
            data = (
                get_acs_by_geography(
                    survey="acs5", year=year, variables=variables, geography="tract"
                )
                .pivot_table(
                    index=["id", "name"], columns="variable", values="estimate"
                )
                .reset_index()
                .rename(columns={"id": "tract_geoid_alt"})
                .drop(columns=["name"])
                .merge(
                    crosswalk[["tract_geoid_alt", "neighborhood_name", "tract_name"]],
                    on="tract_geoid_alt",
                )
            )

            # Get bins for aggregation
            bins = []
            for i in range(2, 18):
                column = variables[f"{table}_{i:03d}"]
                if column.endswith("or_more"):
                    start = float(column.split("_")[0])
                    end = np.inf
                else:
                    start, end = map(float, column.split("_to_"))
                bins.append((start, end, column))

            # Approximate from the distribution
            data = (
                aggregate_median_data(
                    data,
                    groupby="neighborhood_name",
                    bins=bins,
                    sampling_percentage=5 * 2.5,
                )
                .reset_index()
                .merge(
                    crosswalk[["neighborhood_id", "neighborhood_name"]],
                    on="neighborhood_name",
                )
                .rename(columns={"neighborhood_name": "name", "neighborhood_id": "id"})
            )

        # PUMA/Tract
        if geography == "puma":
            data = data.drop(columns=["name"]).merge(pumas[["name", "id"]], on="id")
        elif geography == "tract":
            data = (
                data[["id", "estimate"]]
                .rename(columns={"id": "tract_geoid_alt"})
                .merge(
                    tract_hood_crosswalk[["tract_geoid_alt", "tract_name"]],
                    on="tract_geoid_alt",
                )
                .rename(columns={"tract_name": "name"})
            )

        out.append(data[["name", "estimate"]])

    return pd.concat(out).assign(indicator=f"median_household_income")


def _get_unemployment_rate(year=2019):
    """Get percent of population who is unemployed."""

    pumas = get_pumas()
    tract_hood_crosswalk = get_tract_neighborhood_crosswalk()

    out = []
    for geography in GEOGRAPHIES:

        table = "B23025"
        variables = {
            table + "_002": "in_labor_force",
            table + "_005": "civilian_unemployed",
        }

        # Get data by desired geography
        data = get_acs_by_geography(
            survey="acs5", year=year, variables=variables, geography=geography
        )

        # Calculate the ratio
        X = data.set_index(["name", "id"])
        data = (
            X.query("variable == 'civilian_unemployed'")["estimate"]
            / X.query("variable == 'in_labor_force'")["estimate"]
        ).reset_index()

        # PUMA/Tract
        if geography == "puma":
            data = data.drop(columns=["name"]).merge(pumas[["name", "id"]], on="id")
        elif geography == "tract":
            data = (
                data[["id", "estimate"]]
                .rename(columns={"id": "tract_geoid_alt"})
                .merge(
                    tract_hood_crosswalk[["tract_geoid_alt", "tract_name"]],
                    on="tract_geoid_alt",
                )
                .rename(columns={"tract_name": "name"})
            )

        out.append(data[["name", "estimate"]])

    return pd.concat(out).assign(indicator="unemployment_rate")


def _get_poverty_rate(year=2019):
    """Get percent of population below poverty line."""

    pumas = get_pumas()
    tract_hood_crosswalk = get_tract_neighborhood_crosswalk()

    out = []
    for geography in GEOGRAPHIES:

        table = "B17001"
        variables = {table + "_001": "universe", table + "_002": "below_poverty_line"}

        # Get data by desired geography
        data = get_acs_by_geography(
            survey="acs5", year=year, variables=variables, geography=geography
        )

        # Calculate the ratio
        X = data.set_index(["name", "id"])
        data = (
            X.query("variable == 'below_poverty_line'")["estimate"]
            / X.query("variable == 'universe'")["estimate"]
        ).reset_index()

        # PUMA/Tract
        if geography == "puma":
            data = data.drop(columns=["name"]).merge(pumas[["name", "id"]], on="id")
        elif geography == "tract":
            data = (
                data[["id", "estimate"]]
                .rename(columns={"id": "tract_geoid_alt"})
                .merge(
                    tract_hood_crosswalk[["tract_geoid_alt", "tract_name"]],
                    on="tract_geoid_alt",
                )
                .rename(columns={"tract_name": "name"})
            )

        out.append(data[["name", "estimate"]])

    return pd.concat(out).assign(indicator="poverty_rate")


def _get_foreignborn(year=2019):
    """Get percent of population that is foreign-born."""

    pumas = get_pumas()
    tract_hood_crosswalk = get_tract_neighborhood_crosswalk()

    out = []
    for geography in GEOGRAPHIES:

        table = "B05002"
        variables = {table + "_001": "universe", table + "_013": "foreign_born"}

        # Get data by desired geography
        data = get_acs_by_geography(
            survey="acs5", year=year, variables=variables, geography=geography
        )

        # Calculate the ratio
        X = data.set_index(["name", "id"])
        data = (
            X.query("variable == 'foreign_born'")["estimate"]
            / X.query("variable == 'universe'")["estimate"]
        ).reset_index()

        # PUMA/Tract
        if geography == "puma":
            data = data.drop(columns=["name"]).merge(pumas[["name", "id"]], on="id")
        elif geography == "tract":
            data = (
                data[["id", "estimate"]]
                .rename(columns={"id": "tract_geoid_alt"})
                .merge(
                    tract_hood_crosswalk[["tract_geoid_alt", "tract_name"]],
                    on="tract_geoid_alt",
                )
                .rename(columns={"tract_name": "name"})
            )

        out.append(data[["name", "estimate"]])

    return pd.concat(out).assign(indicator=f"foreignborn")


def _get_race_ethnicity(year=2019):
    """Get ACS data on race/ethnicity."""

    pumas = get_pumas()
    tract_hood_crosswalk = get_tract_neighborhood_crosswalk()

    out = []
    for geography in GEOGRAPHIES:

        # Set up
        table = "B03002"
        variables = {
            table + "_001": "universe",
            table + "_003": "white_alone",
            table + "_004": "black_alone",
            table + "_006": "asian_alone",
            table + "_012": "hispanic_alone",
        }

        # Get data by desired geography
        data = get_acs_by_geography(
            survey="acs5", year=year, variables=variables, geography=geography
        )

        # Reshape to wide format
        X = data.drop(columns=["moe"]).pivot_table(
            index=["id", "name"], columns="variable", values="estimate"
        )

        # Add the other column
        X["other_alone"] = X["universe"] - X[
            ["asian_alone", "black_alone", "hispanic_alone", "white_alone"]
        ].sum(axis=1)

        # Calculate the percent
        data = (
            X[
                [
                    "asian_alone",
                    "black_alone",
                    "hispanic_alone",
                    "white_alone",
                    "other_alone",
                ]
            ]
            .divide(X["universe"], axis=0)
            .dropna(axis=0, how="all")
            .rename(
                columns={
                    "white_alone": "percent_white",
                    "black_alone": "percent_black",
                    "asian_alone": "percent_asian",
                    "hispanic_alone": "percent_hispanic",
                    "other_alone": "percent_other",
                }
            )
            .melt(ignore_index=False, value_name="estimate", var_name="indicator")
            .reset_index()
        )

        # PUMA/Tract
        if geography == "puma":
            data = data.drop(columns=["name"]).merge(pumas[["name", "id"]], on="id")
        elif geography == "tract":
            data = (
                data[["id", "estimate", "indicator"]]
                .rename(columns={"id": "tract_geoid_alt"})
                .merge(
                    tract_hood_crosswalk[["tract_geoid_alt", "tract_name"]],
                    on="tract_geoid_alt",
                )
                .rename(columns={"tract_name": "name"})
            )

        out.append(data[["name", "estimate", "indicator"]])

    return pd.concat(out)


def get_census_indicators():
    """Get census-based indicators for all geographies (tract, puma, neighborhood)."""

    functions = [
        _get_2010_population,
        _get_population,
        # _get_foreignborn,
        _get_poverty_rate,
        _get_race_ethnicity,
        _get_sex_by_age,
        _get_median_household_income,
    ]

    indicators = []
    for f in functions:
        indicators.append(f())

    return pd.concat(indicators, axis=0).dropna()


def get_trend_variables():
    """Get comparison variables for trend analysis."""
    functions = [
        _get_poverty_rate,
        _get_median_household_income,
        _get_unemployment_rate,
        _get_race_ethnicity,
    ]

    indicators = []
    for f in functions:
        indicators.append(f())

    # Combine
    out = pd.concat(indicators, axis=0).dropna()

    # Trim to census tracts only
    tracts = get_tract_neighborhood_crosswalk()[
        ["tract_name", "tract_geoid_alt", "neighborhood_name"]
    ].rename(columns={"tract_geoid_alt": "geoid", "tract_name": "name"})
    out = out.merge(tracts, on="name")

    # Remove missing
    missing = ["Park", "Airport-Navy Yard", "NE Airport"]
    out = out.query("neighborhood_name not in @missing")

    return out.drop(columns=["neighborhood_name"])
