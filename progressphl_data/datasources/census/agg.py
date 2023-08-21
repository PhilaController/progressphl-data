import math

import numpy as np
import pandas as pd

from ...crosswalk import get_tract_neighborhood_crosswalk


def aggregate_median_data(df, bins, groupby, sampling_percentage=5 * 2.5):
    """
    Aggregate all columns in the input data frame, assuming
    the data is "median" data.
    Note
    ----
    The geometry of the returned object is aggregated geometry
    of all input geometries (the unary union).
    Parameters
    ----------
    df : GeoDataFrame
        the input data to aggregate
    by : str
        the name of the column that specifies the aggregation groups
    Examples
    --------
    >>> bins = cp_data.HouseholdIncome.get_aggregation_bins()
    >>> cp_data.census.aggregate_median_data(df, bins, "cluster_label", "median_income")

    Returns
    -------
    out : GeoDataFrame
        the output data with aggregated data and margin of error columns,
        and the aggregated geometry polygon
    """
    # Make sure we have the column we are grouping by
    if groupby not in df.columns:
        raise ValueError(
            f"the specified column to group by '{groupby}' is not in the input data"
        )

    # these are the column names for each bin
    # FORMAT of bins is (min, max, column_name)
    columns = [b[-1] for b in bins]

    # Make sure all of the specified columns are present
    for col in columns:
        if col not in df.columns:
            raise ValueError(f"the specified column '{col}' is not in the input data")

    def _aggregate(group_df, sampling_percentage=sampling_percentage):
        """
        The function that aggregates each group
        """
        dist = []
        total_count = 0
        for i, col in enumerate(columns):

            n = group_df[col].sum()
            total_count += n
            dist.append(dict(min=bins[i][0], max=bins[i][1], n=n))

        # only aggregate if we have data!
        if total_count:
            aggval, moe = approximate_median(
                dist, sampling_percentage=sampling_percentage
            )
        else:
            aggval = np.nan
            moe = np.nan

        result = {}
        result["estimate"] = aggval
        result["moe"] = moe

        return pd.Series(result)

    # this is the aggregated data, with index of "by", e.g., group label
    return df.groupby(groupby).apply(_aggregate)


def approximate_median(range_list, design_factor=1, sampling_percentage=None):
    """
    Estimate a median and approximate the margin of error.

    Follows the U.S. Census Bureau's `official guidelines`_ for estimation
    using a design factor. Useful for generating medians for measures like household
    income and age when aggregating census geographies.

    Parameters
    ----------
    range_list (list):
        A list of dictionaries that divide the full range of data values into continuous categories.
        Each dictionary should have three keys:
            * min (int): The minimum value of the range
            * max (int): The maximum value of the range
            * n (int): The number of people, households or other unit in the range
        The minimum value in the first range and the maximum value in the last range
        can be tailored to the dataset by using the "jam values" provided in
        the `American Community Survey's technical documentation`_.
    design_factor (float, optional):
        A statistical input used to tailor the standard error to the
        variance of the dataset. This is only needed for data coming from public use microdata sample,
        also known as PUMS. You do not need to provide this input if you are approximating
        data from the American Community Survey. The design factor for each PUMS
        dataset is provided as part of `the bureau's reference material`_.
    sampling_percentage (float, optional):
        A statistical input used to correct for variance linked to
        the size of the survey's population sample. This value submitted should be the percentage of
        * One-year PUMS: 1
        * One-year ACS: 2.5
        * Three-year ACS: 7.5
        * Five-year ACS: 12.5
     If you do not provide this input, a margin of error will not be returned.

    Returns
    -------
        A two-item tuple with the median followed by the approximated margin of error.
        (42211.096153846156, 10153.200960954948)

    References
    ----------
    - https://www.documentcloud.org/documents/6165603-2013-2017AccuracyPUMS.html#document/p18
    - https://www.documentcloud.org/documents/6165752-2017-SummaryFile-Tech-Doc.html#document/p20/a508561
    - https://www.census.gov/programs-surveys/acs/technical-documentation/pums/documentation.html

    Examples
    --------
    Estimating the median for a range of household incomes.
    >>> household_income_2013_acs5 = [
        dict(min=2499, max=9999, n=186),
        dict(min=10000, max=14999, n=78),
        dict(min=15000, max=19999, n=98),
        dict(min=20000, max=24999, n=287),
        dict(min=25000, max=29999, n=142),
        dict(min=30000, max=34999, n=90),
        dict(min=35000, max=39999, n=107),
        dict(min=40000, max=44999, n=104),
        dict(min=45000, max=49999, n=178),
        dict(min=50000, max=59999, n=106),
        dict(min=60000, max=74999, n=177),
        dict(min=75000, max=99999, n=262),
        dict(min=100000, max=124999, n=77),
        dict(min=125000, max=149999, n=100),
        dict(min=150000, max=199999, n=58),
        dict(min=200000, max=250001, n=18)
    ]
    >>> approximate_median(household_income_2013_acs5, sampling_percentage=5*2.5)
    (42211.096153846156, 4706.522752733644)

    """
    # Sort the list
    range_list.sort(key=lambda x: x["min"])

    # For each range calculate its min and max value along the universe's scale
    cumulative_n = 0
    for range_ in range_list:
        range_["n_min"] = cumulative_n
        cumulative_n += range_["n"]
        range_["n_max"] = cumulative_n

    # What is the total number of observations in the universe?
    n = sum(d["n"] for d in range_list)

    # What is the estimated midpoint of the n?
    n_midpoint = n / 2.0

    # Now use those to determine which group contains the midpoint.
    n_midpoint_range = next(
        d for d in range_list if n_midpoint >= d["n_min"] and n_midpoint <= d["n_max"]
    )

    # How many households in the midrange are needed to reach the midpoint?
    n_midrange_gap = n_midpoint - n_midpoint_range["n_min"]

    # What is the proportion of the group that would be needed to get the midpoint?
    n_midrange_gap_percent = n_midrange_gap / n_midpoint_range["n"]

    # Apply this proportion to the width of the midrange
    n_midrange_gap_adjusted = (
        n_midpoint_range["max"] - n_midpoint_range["min"]
    ) * n_midrange_gap_percent

    # Estimate the median
    estimated_median = n_midpoint_range["min"] + n_midrange_gap_adjusted

    # If there's no sampling percentage, we can't calculate a margin of error
    if not sampling_percentage:
        # Let's throw a warning, but still return the median
        warnings.warn("", SamplingPercentageWarning)
        return estimated_median, None

    # Get the standard error for this dataset
    standard_error = (
        design_factor
        * math.sqrt(
            ((100 - sampling_percentage) / (n * sampling_percentage)) * (50**2)
        )
    ) / 100

    # Use the standard error to calculate the p values
    p_lower = 0.5 - standard_error
    p_upper = 0.5 + standard_error

    # Estimate the p_lower and p_upper n values
    p_lower_n = n * p_lower
    p_upper_n = n * p_upper

    # Find the ranges the p values fall within
    try:
        p_lower_range_i, p_lower_range = next(
            (i, d)
            for i, d in enumerate(range_list)
            if p_lower_n >= d["n_min"] and p_lower_n <= d["n_max"]
        )
    except StopIteration:
        raise ValueError(
            f"The n's lower p value {p_lower_n} does not fall within a data range."
        )

    try:
        p_upper_range_i, p_upper_range = next(
            (i, d)
            for i, d in enumerate(range_list)
            if p_upper_n >= d["n_min"] and p_upper_n <= d["n_max"]
        )
    except StopIteration:
        raise ValueError(
            f"The n's upper p value {p_upper_n} does not fall within a data range."
        )

    # Use these values to estimate the lower bound of the confidence interval
    p_lower_a1 = p_lower_range["min"]
    try:
        p_lower_a2 = range_list[p_lower_range_i + 1]["min"]
    except IndexError:
        p_lower_a2 = p_lower_range["max"]
    p_lower_c1 = p_lower_range["n_min"] / n
    try:
        p_lower_c2 = range_list[p_lower_range_i + 1]["n_min"] / n
    except IndexError:
        p_lower_c2 = p_lower_range["n_max"] / n
    lower_bound = ((p_lower - p_lower_c1) / (p_lower_c2 - p_lower_c1)) * (
        p_lower_a2 - p_lower_a1
    ) + p_lower_a1

    # Same for the upper bound
    p_upper_a1 = p_upper_range["min"]
    try:
        p_upper_a2 = range_list[p_upper_range_i + 1]["min"]
    except IndexError:
        p_upper_a2 = p_upper_range["max"]
    p_upper_c1 = p_upper_range["n_min"] / n
    try:
        p_upper_c2 = range_list[p_upper_range_i + 1]["n_min"] / n
    except IndexError:
        p_upper_c2 = p_upper_range["n_max"] / n
    upper_bound = ((p_upper - p_upper_c1) / (p_upper_c2 - p_upper_c1)) * (
        p_upper_a2 - p_upper_a1
    ) + p_upper_a1

    # Calculate the standard error of the median
    standard_error_median = 0.5 * (upper_bound - lower_bound)

    # Calculate the margin of error at the 90% confidence level
    margin_of_error = 1.645 * standard_error_median

    # Return the result
    return estimated_median, margin_of_error


def approximate_ratio(data, numerator, denominator, index=["id", "name"]):
    """Approximate a ratio statistic."""

    X = data.set_index(index)
    n = X.query(f"variable == '{numerator}'")
    d = X.query(f"variable == '{denominator}'")

    ratio_estimate = n["estimate"] / d["estimate"]
    squared_ratio_moe = n["moe"] ** 2 + (ratio_estimate**2 * d["moe"] ** 2)
    ratio_moe = (1.0 / d["estimate"]) * np.sqrt(squared_ratio_moe)

    return (
        pd.merge(
            ratio_estimate.reset_index(name="estimate"),
            ratio_moe.reset_index(name="moe"),
            on=index,
        )
    ).dropna()


def approximate_proportion(data, numerator, denominator, index=["id", "name"]):
    """Approximate a proportion statistic."""

    X = data.set_index(index)
    n = X.query(f"variable == '{numerator}'")
    d = X.query(f"variable == '{denominator}'")

    proportion_estimate = n["estimate"] / d["estimate"]
    squared_proportion_moe = n["moe"] ** 2 - (proportion_estimate**2 * d["moe"] ** 2)

    # Might need to use ratio formula if squared moe is negative
    bad = squared_proportion_moe < 0
    if bad.any():
        ratio_moe = approximate_ratio(
            X.loc[bad].reset_index(), numerator, denominator, index=index
        ).set_index(index)["moe"]

    # Get the MOE for the okay rows
    moe = (1.0 / d.loc[~bad, "estimate"]) * np.sqrt(squared_proportion_moe.loc[~bad])

    # Use ratio formula for any bad rows
    if bad.any():
        moe = pd.concat([moe, ratio_moe], axis=0).loc[n.index]

    return (
        pd.merge(
            proportion_estimate.reset_index(name="estimate"),
            moe.reset_index(name="moe"),
            on=index,
        )
    ).dropna()


def approximate_sum(data):
    """Approximate a sum."""

    estimate = data["estimate"].sum()
    sel = data["estimate"] == 0
    if sel.any():
        moe = (
            (data.loc[~sel]["moe"] ** 2).sum() + data.loc[sel, "moe"].max() ** 2
        ) ** 0.5
    else:
        moe = (data["moe"] ** 2).sum() ** 0.5

    return pd.Series({"estimate": estimate, "moe": moe})


def tracts_to_neighborhoods(data):
    """Aggregrate data from the tract-level to neighborhood-level."""

    # Merge in the crosswalk
    crosswalk = get_tract_neighborhood_crosswalk()
    data = data.merge(crosswalk, left_on="id", right_on="tract_geoid_alt")

    # Approximate sum over tracts
    data = (
        data.groupby(
            ["neighborhood_id", "neighborhood_name", "variable"], as_index=False
        )
        .apply(approximate_sum)
        .rename(columns={"neighborhood_id": "id", "neighborhood_name": "name"})
    )

    return data


def sum_over_variables(data, variable_name, excluded=None):
    """Sum over variables."""

    # Variables to exclude from the sum
    if excluded is None:
        excluded = []
    elif isinstance(excluded, str):
        excluded = [excluded]

    # Split the data
    data_to_sum = data.query("variable not in @excluded")
    data_excluded = data.query("variable in @excluded")

    # Do the groupby -> sum
    data = (
        data_to_sum.groupby(["id", "name"], as_index=False)
        .apply(approximate_sum)
        .assign(variable=variable_name)
    )

    # Combine with excluded if needed
    if len(excluded):
        data = pd.concat([data, data_excluded])

    return data
