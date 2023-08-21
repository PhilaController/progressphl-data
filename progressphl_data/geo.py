import geopandas as gpd
import pandas as pd
import pygris

from . import DATA_DIR

EPSG = 2272


def get_city_limits() -> gpd.GeoDataFrame:
    """
    Return Philadelphia census tracts.

    This returns the census tracts as defined in the 2010 Census.
    """
    return (
        gpd.read_file(
            "https://opendata.arcgis.com/datasets/405ec3da942d4e20869d4e1449a2be48_0.geojson"
        )
        .to_crs(epsg=EPSG)
        .assign(id="42101", name="Philadelphia County")[["id", "name", "geometry"]]
    )


def get_census_tracts() -> gpd.GeoDataFrame:
    """
    Return Philadelphia census tracts.

    This returns the census tracts as defined in the 2010 Census.
    """
    return (
        pygris.tracts(state="42", county="101", year=2019)  # Use 2019 by default
        .rename(columns={"GEOID": "id", "NAMELSAD": "name"})[["id", "name", "geometry"]]
        .to_crs(epsg=EPSG)
        .sort_values("id", ignore_index=True)
    )


def get_neighborhoods() -> gpd.GeoDataFrame:
    """
    Return Philadelphia neighborhoods.

    These neighborhoods are defined as exact groupings of census tracts.
    """

    # Load neighborhood definitions
    tract_hood_crosswalk = pd.read_csv(
        DATA_DIR / "tract-neighborhood-crosswalk.csv", dtype=str
    )

    # Load census tracts
    tracts = get_census_tracts()

    # Dissolve and return
    return (
        tracts.rename(columns={"id": "tract_geoid_alt"})
        .drop(columns=["name"])
        .merge(tract_hood_crosswalk, on="tract_geoid_alt")
        .dissolve("neighborhood_id")[["geometry", "neighborhood_name"]]
        .reset_index()
        .rename(columns={"neighborhood_id": "id", "neighborhood_name": "name"})
    )


def get_pumas(use_census=False) -> gpd.GeoDataFrame:
    """
    Return Philadelphia Public Use Microdata Areas (PUMAs).

    The returns PUMAs as defined in the 2010 Census.
    """
    if use_census:
        # Get the raw geometries
        pumas = (
            pygris.pumas(state="42", year=2019)  # Use 2019 by default
            .query("NAMELSAD10.str.contains('Philadelphia City', na=False)")
            .rename(columns={"GEOID10": "id", "NAMELSAD10": "name"})[
                ["id", "name", "geometry"]
            ]
            .to_crs(epsg=EPSG)
            .sort_values("id", ignore_index=True)
        )

        # Adjust the names
        def get_puma_name(s):
            return s[s.find("(") + 1 : s.find(")")]

        pumas["name"] = (
            pumas["name"]
            .apply(get_puma_name)
            .replace(
                {
                    "Center City": "Greater Center City",
                    "Central": "Lower North",
                    "North": "Upper North",
                    "Near Northeast-East": "Lower Northeast (East)",
                    "Near Northeast-West": "Lower Northeast (West)",
                    "Southeast": "South",
                }
            )
        )
    else:
        # Load neighborhood definitions
        tract_puma_crosswalk = pd.read_csv(
            DATA_DIR / "tract-puma-crosswalk.csv", dtype=str
        )

        # Load census tracts
        tracts = get_census_tracts()

        # Dissolve and return
        pumas = (
            tracts.rename(columns={"id": "tract_geoid_alt"})
            .drop(columns=["name"])
            .merge(tract_puma_crosswalk, on="tract_geoid_alt")
            .dissolve("puma_id")[["geometry", "puma_name"]]
            .reset_index()
            .rename(columns={"puma_id": "id", "puma_name": "name"})
        )

    return pumas
