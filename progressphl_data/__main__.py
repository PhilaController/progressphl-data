"""The main command line module that defines the "progressphl-data" tool."""


from io import StringIO
from pathlib import Path

import boto3
import click
import simplejson as json
from dotenv import find_dotenv, load_dotenv

from .census_indicators import get_census_indicators, get_trend_variables
from .core import get_spi_data, load_meta_data
from .crosswalk import *
from .geo import *

BUCKET = "spi-dashboard-data"

here = Path(__file__).parent.absolute()


@click.group()
@click.version_option()
def cli():
    """Processing data for the Controller's Office ProgressPHL dashboard."""
    pass


@cli.command()
@click.option("--version", type=str, default="2")
def etl(version="2"):
    """Process and upload the data."""

    # Load the credentials
    load_dotenv(find_dotenv())

    # Initialize the s3 resource
    s3_resource = boto3.resource("s3")

    # Setup local output folder
    local_output_folder = (
        here / ".." / "data-products" / "dashboard-inputs" / f"v{version}"
    )
    if not local_output_folder.exists():
        local_output_folder.mkdir(parents=True)

    # The SPI data
    spi_data = get_spi_data(version=version)

    # Reformat it
    OUTPUT_COLUMNS = [
        "geoid",
        "neighborhood_name",
        "puma_name",
        "tract_id",
        "variable",
        "value",
        "average_label",
        "rank",
    ]
    spi_data_trimmed = spi_data[OUTPUT_COLUMNS].copy()

    # Convert to dict
    out = {}
    for variable in spi_data_trimmed["variable"].unique():
        out[variable] = (
            spi_data_trimmed.query("variable == @variable")
            .drop(columns=["variable"])
            .to_dict(orient="records")
        )

    # Save to a buffer
    buffer = StringIO()
    json.dump(out, buffer, ignore_nan=True)
    json.dump(out, (local_output_folder / "spi-data.json").open("w"), ignore_nan=True)

    # Upload to s3
    s3_resource.Object(BUCKET, f"v{version}/spi-data.json").put(
        Body=buffer.getvalue(), ACL="public-read"
    )

    # Do the metadata
    tags = ["aliases", "hierarchy", "definitions"]
    meta = {}
    for tag in tags:
        meta[tag] = load_meta_data(tag=tag, version=version)

    # Save to a buffer
    buffer = StringIO()
    json.dump(meta, buffer)
    json.dump(meta, (local_output_folder / "spi-metadata.json").open("w"))

    # Upload to s3
    s3_resource.Object(BUCKET, f"v{version}/spi-metadata.json").put(
        Body=buffer.getvalue(), ACL="public-read"
    )

    # Census indicators
    data = get_census_indicators()
    missing = ["Park", "Airport-Navy Yard", "NE Airport"]

    # Make output census folder
    census_output_folder = local_output_folder / "census-data"
    if not census_output_folder.exists():
        census_output_folder.mkdir(parents=True)

    # Save each name
    for name, df in data.groupby("name"):
        if any(name.startswith(m) for m in missing):
            continue

        # Don't need the name column
        out = df.drop(columns=["name"])

        # Save to a buffer
        buffer = StringIO()
        out.to_json(buffer, orient="records")
        out.to_json(census_output_folder / f"{name}.json", orient="records")

        # Upload to s3
        s3_resource.Object(BUCKET, f"v{version}/census-data/{name}.json").put(
            Body=buffer.getvalue(), ACL="public-read"
        )

    # Trend variables
    data = get_trend_variables()

    # Make output trend folder
    trend_output_folder = local_output_folder / "trends"
    if not trend_output_folder.exists():
        trend_output_folder.mkdir(parents=True)

    # Save each name
    for name, df in data.groupby("indicator"):
        # Don't need the indicator column
        out = df.drop(columns=["indicator"])

        # Save to a buffer
        buffer = StringIO()
        out.to_json(buffer, orient="records")
        out.to_json(trend_output_folder / f"{name}.json", orient="records")

        # Upload to s3
        s3_resource.Object(BUCKET, f"v{version}/trends/{name}.json").put(
            Body=buffer.getvalue(), ACL="public-read"
        )


@cli.command()
@click.option("--version", type=str, default="2")
def geo(version="2"):
    """Save geographies."""

    # Get data and geoids
    data = get_spi_data(version=version)
    geoids = data["geoid"].unique()

    layers = {}

    # Crosswalks
    tract_hood_crosswalk = get_tract_neighborhood_crosswalk()
    tract_puma_crosswalk = get_tract_puma_crosswalk()

    # The geographies
    tracts = get_census_tracts()
    neighborhoods = get_neighborhoods()
    pumas = get_pumas()

    # Neighborhoods
    layers["neighborhoods"] = neighborhoods.rename(
        columns={"name": "neighborhood_name"}
    ).to_crs(epsg=4326)

    # PUMAs
    layers["pumas"] = (
        pumas.drop(columns=["id"])
        .rename(columns={"name": "puma_name"})
        .to_crs(epsg=4326)
    )

    # Census tracts
    layers["census-tracts"] = (
        tracts[["id", "geometry"]]
        .rename(columns={"id": "geoid"})
        .to_crs(epsg=4326)
        .assign(missing=lambda df: (~df.geoid.isin(geoids)).astype(int))
    ).merge(
        tract_hood_crosswalk[["tract_geoid_alt", "neighborhood_name", "tract_id"]]
        .merge(
            tract_puma_crosswalk[["tract_geoid_alt", "puma_name"]], on="tract_geoid_alt"
        )
        .rename(columns={"tract_geoid_alt": "geoid"}),
        on="geoid",
    )

    # City limits
    layers["city-limits"] = (
        tracts.set_geometry(tracts.geometry.buffer(10))
        .drop(columns=["id", "name"])
        .dissolve()
        .to_crs(epsg=4326)
    )

    output_folder = here / ".." / "data-products" / "geographies"
    for k in layers:
        print(f"Saving {k}...")
        layers[k].to_file(output_folder / f"{k}.geojson", driver="GeoJSON")


if __name__ == "__main__":
    cli(prog_name="progressphl-data")
