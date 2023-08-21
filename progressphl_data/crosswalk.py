import geopandas as gpd

from . import DATA_DIR
from .geo import get_census_tracts, get_neighborhoods, get_pumas


def _calculate_crosswalk(
    inner: gpd.GeoDataFrame, outer: gpd.GeoDataFrame, inner_id_column: str = "id"
) -> gpd.GeoDataFrame:
    """
    Internal function to calculate the crosswalk between two boundaries,
    assuming the `outer` data frame is an exact aggregation of the `inner`
    boundary.

    Notes
    -----
    This calculates the crosswalk by taking the intersection of the inner and
    outer geometries.

    Parameters
    ----------
    inner :
        The inner dataframe which can be grouped to from the outer, larger
        boundaries
    outer :
        The outer dataframe
    id_column :
        The column specifying the id column for the inner dataframe

    Returns
    -------
    A geodataframe with the same length of inner that includes the crosswalk to
    the outer geometries.
    """
    # Do the intersection
    inner = inner.assign(inner_area=inner.geometry.area)
    intersection = gpd.overlay(
        inner, outer.to_crs(inner.crs), how="intersection", keep_geom_type=False
    )
    if inner_id_column not in intersection.columns:
        inner_id_column = inner_id_column + "_1"

    # Intersection area
    intersection["intersection_area"] = intersection.geometry.area
    intersection["percent_inner_area"] = (
        intersection["intersection_area"] / intersection["inner_area"]
    )

    # Drop duplicates to get the crosswalk
    crosswalk = intersection.sort_values("percent_inner_area", ascending=False)
    return crosswalk.drop_duplicates(subset=[inner_id_column]).drop(
        columns=["intersection_area", "percent_inner_area", "inner_area"]
    )


def _as_strings(df):
    cols = [col for col in df.columns if col != "geometry"]
    for col in cols:
        df[col] = df[col].astype(str)

    return df


def get_tract_puma_crosswalk(fresh: bool = False) -> gpd.GeoDataFrame:
    """
    Calculate the crosswalk between tracts and pumas.
    """
    path = DATA_DIR / "tract-puma-crosswalk.geojson"
    if fresh or not path.exists():
        # Load the geometries
        tracts = get_census_tracts()
        pumas = get_pumas(use_census=True)

        # Get crosswalk from intersection
        crosswalk = (
            _calculate_crosswalk(tracts, pumas, inner_id_column="id")
            .rename(
                columns={
                    "id_1": "tract_id",
                    "name_1": "tract_name",
                    "id_2": "puma_id",
                    "name_2": "puma_name",
                }
            )
            .sort_values(["puma_id", "tract_id"], ignore_index=True)
        ).rename(
            columns={"tract_id": "tract_geoid_alt", "tract_name": "tract_name_alt"}
        )

        # Save it
        crosswalk.to_file(path, driver="GeoJSON")

    return _as_strings(gpd.read_file(path))


def get_tract_neighborhood_crosswalk(fresh: bool = False) -> gpd.GeoDataFrame:
    """
    Calculate the crosswalk between tracts and neighborhoods.

    This renames tracts and adds a new geoid based on neighborhood names.
    """
    path = DATA_DIR / "tract-neighborhood-crosswalk.geojson"
    if fresh or not path.exists():

        # Load the geometries
        tracts = get_census_tracts()
        neighborhoods = get_neighborhoods()

        # Get crosswalk from intersection
        crosswalk = (
            _calculate_crosswalk(tracts, neighborhoods, inner_id_column="id")
            .rename(
                columns={
                    "id_1": "tract_id",
                    "name_1": "tract_name",
                    "id_2": "neighborhood_id",
                    "name_2": "neighborhood_name",
                }
            )
            .sort_values(["neighborhood_id", "tract_id"], ignore_index=True)
        )

        # Assign sub id
        crosswalk = crosswalk.groupby("neighborhood_name", group_keys=False).apply(
            lambda grp: grp.rename(
                columns={"tract_name": "tract_name_alt", "tract_id": "tract_geoid_alt"}
            ).assign(
                tract_id=[f"{i:02d}" for i in range(1, len(grp) + 1)],
                tract_name=lambda df: df.neighborhood_name
                + " "
                + df.tract_id.astype(int).astype(str),
                tract_geoid=lambda df: df.neighborhood_id + df.tract_id,
            )
        )

        # Save it
        crosswalk.to_file(path, driver="GeoJSON")

    return _as_strings(gpd.read_file(path))
