from ...crosswalk import get_tract_neighborhood_crosswalk
from ..census.core import get_decennial


def get_adult_population_by_tract():
    """2010 decennial population by tract for adults >= 18 years old."""

    # Query the censsu
    data = get_decennial(
        variables={
            "P012001": "universe",
            "P012003": "male_under_5",
            "P012004": "male_5_to_9",
            "P012005": "male_10_to_14",
            "P012006": "male_15_to_17",
            "P012027": "female_under_5",
            "P012028": "female_5_to_9",
            "P012029": "female_10_to_14",
            "P012030": "female_15_to_17",
        },
        sumfile="sf1",
        year=2010,
        geography="tract",
    )

    def calculate_pop_18_and_over(grp):
        universe = grp.query("variable == 'universe'")["estimate"].squeeze()
        under_17 = grp.query("variable != 'universe'")["estimate"].sum()
        return universe - under_17

    # This is the total population 18 and over
    return (
        data.groupby(["id", "name"])
        .apply(calculate_pop_18_and_over)
        .reset_index(name="estimate")
    )


def tracts_to_neighborhoods(data):
    """Aggregrate data from the tract-level to neighborhood-level."""

    # Merge in the crosswalk
    crosswalk = get_tract_neighborhood_crosswalk()
    data = data.merge(crosswalk, left_on="id", right_on="tract_geoid_alt")

    # Add in the weights
    weights = get_adult_population_by_tract()[["id", "estimate"]].rename(
        columns={"estimate": "weight"}
    )
    data = data.merge(weights, on="id", validate="1:1")

    # Add weighted data value
    data["weighted_estimate"] = data["estimate"] * data["weight"]

    # Approximate sum over tracts
    grouped = data.groupby(["neighborhood_id", "neighborhood_name"])
    data = (
        (grouped["weighted_estimate"].sum() / grouped["weight"].sum())
        .reset_index(name="estimate")
        .rename(columns={"neighborhood_id": "id", "neighborhood_name": "name"})
    )

    return data
