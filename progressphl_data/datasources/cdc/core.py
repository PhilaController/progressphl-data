from typing import Literal

import httpx
import pandas as pd


def get_places_data(
    measure: str,
    year: Literal[2020, 2021, 2022] = 2020,
    geography: Literal["tract", "county"] = "tract",
) -> pd.DataFrame:
    """
    Get CDC places data.

    Parameters
    ----------
    measure :
        The name of the 'measure' column in the dataset
    year :
        The release year
    geography :
        The returned geography
    """
    # Get the dataset url
    url = None
    if geography == "tract":
        if year == 2020:
            url = "https://chronicdata.cdc.gov/resource/4ai3-zynv.json"
        elif year == 2021:
            url = "https://chronicdata.cdc.gov/resource/373s-ayzu.json"
        elif year == 2022:
            url = "https://chronicdata.cdc.gov/resource/cwsq-ngmh.json"
    elif geography == "county":
        if year == 2020:
            url = "https://chronicdata.cdc.gov/resource/dv4u-3x3q.json"
        elif year == 2021:
            url = "https://chronicdata.cdc.gov/resource/pqpp-u99h.json"
        elif year == 2022:
            url = "https://chronicdata.cdc.gov/resource/swc5-untb.json"

    if url is None:
        raise ValueError("Invalid geography/year combination")

    # Set up the request params
    params = {"measure": measure}
    if geography == "tract":
        params["countyfips"] = "42101"
    else:
        params["locationID"] = "42101"

    # Request
    r = httpx.get(url, params=params)

    # Create the dataframe
    return (
        pd.DataFrame(r.json())
        .assign(data_value=lambda df: df.data_value.astype(float))
        .rename(columns={"locationid": "id", "data_value": "estimate"})[
            ["id", "estimate"]
        ]
    )
