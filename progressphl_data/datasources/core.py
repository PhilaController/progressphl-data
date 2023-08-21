from __future__ import annotations

from typing import Literal

import geopandas as gpd
import numpy as np
import pandas as pd
from pydantic import validate_arguments

from .. import geo
from .cdc import agg as cdc_agg
from .cdc.core import get_places_data
from .census import agg as census_agg
from .census.core import get_acs


@validate_arguments
def get_acs_by_geography(
    variables: dict[str, str],
    year: int,
    geography: Literal["tract", "neighborhood", "county", "puma"],
    survey: Literal["acs5", "acs5/subject", "acs5/profile"],
) -> gpd.GeoDataFrame:

    # County wide
    if geography == "county":
        data = get_acs(
            year=year,
            survey=survey,
            geography="county",
            variables=variables,
        )
    elif geography == "puma":
        data = get_acs(
            year=year,
            survey=survey,
            geography="puma",
            variables=variables,
        )
    else:
        # Get the data at the tract level
        data = get_acs(
            year=year,
            survey=survey,
            geography="tract",
            variables=variables,
        )

        if geography == "neighborhood":
            data = census_agg.tracts_to_neighborhoods(data)

    return data


@validate_arguments
def get_cdc_places_by_geography(
    measure: str,
    year: Literal[2020, 2021, 2022],
    geography: Literal["tract", "neighborhood", "county"],
) -> gpd.GeoDataFrame:

    # County wide
    if geography == "county":
        data = get_places_data(measure=measure, year=year, geography="county")
    else:

        # Get the data at the tract level
        data = get_places_data(measure=measure, year=year, geography="tract")

        # Handle neighborhood aggregation
        if geography == "neighborhood":
            data = cdc_agg.tracts_to_neighborhoods(data)

    return data


def get_count_by_geography(
    data: gpd.GeoDataFrame,
    geography: Literal["tract", "neighborhood", "county"],
    id_column: str = "id",
) -> pd.Series:
    """Count the input point-like data by a specific geographic boundary."""

    # Get the boundaries
    if geography == "county":
        boundaries = geo.get_city_limits()
    elif geography == "tract":
        boundaries = geo.get_census_tracts()
    elif geography == "neighborhood":
        boundaries = geo.get_neighborhoods()
    else:
        raise ValueError("Unexpected 'geography' value")

    # Spatial join
    data = gpd.sjoin(data, boundaries.to_crs(data.crs), predicate="within")

    # Return total within county
    if geography == "county":
        count = (
            pd.Series({"estimate": len(data)})
            .to_frame()
            .T.assign(id="42101", name="Philadelphia County")
        )
    # Groupby id and return
    else:
        # Get the crash count
        count = data.groupby([id_column]).size()
        count = count.reset_index(name="estimate").merge(
            boundaries[["name", "id"]], on="id"
        )

    return count


def get_rate_by_geography(
    data: gpd.GeoDataFrame,
    geography: Literal["tract", "neighborhood", "county"],
    id_column: str = "id",
    pop_year: int = 2019,
    norm: float = 1e4,
):

    # Get the counts
    counts = get_count_by_geography(data, geography=geography, id_column=id_column)

    # Get the population
    pop = get_acs_by_geography(
        year=pop_year,
        variables={"S0101_C01_001": "universe"},
        geography=geography,
        survey="acs5/subject",
    )

    # Calculate value per population
    # NOTE: index by id first
    rate = counts.set_index("id")["estimate"] / pop.set_index("id")["estimate"] * norm

    # Fix inf
    sel = np.isinf(rate)
    rate.loc[sel] = np.nan

    # Return
    return rate.reset_index().merge(pop[["id", "name"]], on="id")[
        ["name", "id", "estimate"]
    ]
