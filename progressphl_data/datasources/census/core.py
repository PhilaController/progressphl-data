from __future__ import annotations

from functools import reduce
from typing import Callable, Literal

import numpy as np
import pandas as pd
from pygris.data import get_census

__all__ = ["get_acs", "get_decennial"]


def _query_census_api(
    dataset: str,
    year: int,
    variables: list[str],
    geography: Literal["tract", "county", "block group", "puma"],
    chunk_size: int = 48,
    no_errors=False,
) -> pd.DataFrame:
    """
    Internal function to query the Census API.
    """

    # Determine params
    params = {}
    if geography == "tract":
        params["for"] = "tract:*"
        params["in"] = "state:42 county:101"
    elif geography == "county":
        params["for"] = "county:101"
        params["in"] = "state:42"
    elif geography == "block group":
        params["for"] = "block group:*"
        params["in"] = "state:42 county:101 tract:*"
    elif geography == "block":
        params["for"] = "block:*"
        params["in"] = "state:42 county:101 tract:*"
    elif geography == "puma":
        params["for"] = "public use microdata area:*"
        params["in"] = "state:42"
    else:
        raise ValueError("Unrecognized 'geography' keyword")

    # Chucnk the variables
    variable_chunks = np.array_split(variables, len(variables) // chunk_size + 1)
    data = []
    for variable_chunk in variable_chunks:

        variable_chunk = list(variable_chunk)
        if "NAME" not in variable_chunk:
            variable_chunk.append("NAME")

        # Request
        _data = get_census(
            dataset=dataset,
            variables=variable_chunk,
            params=params,
            year=year,
            return_geoid=True,
            guess_dtypes=True,
        )

        # Trim to just Philadelphia PUMAs
        if geography == "puma":
            _data = _data.query("NAME.str.contains('Philadelphia City', na=False)")

        # Put into tidy format
        _data = _data.melt(
            id_vars=["GEOID", "NAME"], var_name="variable", value_name="estimate"
        )

        data.append(_data)

    # Combine chunks
    data = pd.concat(data, ignore_index=True)

    # Handle moe column
    if not no_errors:
        data = (
            data.assign(
                variable2=lambda df: df["variable"].apply(
                    lambda x: "estimate" if x.endswith("E") else "moe"
                ),
                variable=lambda df: df["variable"].str.slice(0, -1),
            )
            .pivot(
                index=["GEOID", "NAME", "variable"],
                columns="variable2",
                values="estimate",
            )
            .reset_index()
            .rename_axis(None, axis=1)
        )

    return data


def get_acs(
    variables: dict[str, str],
    survey: Literal["acs5", "acs5/subject", "acs5/profile"],
    year: int = 2019,
    geography: Literal["tract", "county", "block group", "puma"] = "tract",
) -> pd.DataFrame:
    """Get data from the ACS."""

    # List of variables to get
    variable_names = list(variables)
    variables_full = [var + suffix for var in variable_names for suffix in ["M", "E"]]

    # Query the API, format, and return
    result = (
        _query_census_api(
            dataset=f"acs/{survey}",
            year=year,
            variables=variables_full,
            geography=geography,
        )
        .rename(columns={"GEOID": "id", "NAME": "name"})
        .assign(variable=lambda df: df.variable.replace(variables))
    )

    # Return
    return result


def get_decennial(
    variables: dict[str, str],
    sumfile: Literal[
        "sf1",
        "sf2",
        "sf3",
        "sf4",
        "sf2profile",
        "sf3profile",
        "sf4profile",
        "pl",
    ] = "sf1",
    geography: Literal["tract", "county", "block group", "block"] = "tract",
    year: Literal[2000, 2010, 2020] = 2010,
) -> pd.DataFrame:
    """Get decennial census data."""

    # Check input years
    allowed_years = [2000, 2010, 2020]
    if year not in allowed_years:
        raise ValueError(f"Allowed year values are: {allowed_years}")

    # Query the API, format, and return
    result = (
        _query_census_api(
            dataset=f"dec/{sumfile}",
            year=year,
            variables=list(variables),
            geography=geography,
            no_errors=True,
        )
        .rename(columns={"GEOID": "id", "NAME": "name"})
        .assign(variable=lambda df: df.variable.replace(variables))
    )

    # Return
    return result
