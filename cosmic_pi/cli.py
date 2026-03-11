"""Cosmic Pi Transglobal Expedition — data pipeline and visualization CLI."""

from pathlib import Path
from typing import Annotated

import typer

app = typer.Typer(
    help="Cosmic Pi Transglobal Expedition — data pipeline and visualization."
)


@app.command()
def download(
    input_dir: Annotated[Path, typer.Option(help="Directory to save zip files")] = Path(
        "input"
    ),
):
    """Download dataset zip files from Zenodo."""
    from .ingest import download_datasets

    download_datasets(input_dir)


@app.command()
def ingest(
    input_dir: Annotated[Path, typer.Option(help="Directory with zip backups")] = Path(
        "input"
    ),
    skip_extract: Annotated[bool, typer.Option(help="Skip zip extraction")] = False,
):
    """Extract zips, start InfluxDB, and restore backups.

    Run 'cosmic-pi export' afterwards to export data to GeoParquet.
    Run 'cosmic-pi stop' when done with InfluxDB.
    """
    from .ingest import (
        extract_backups,
        restore_backups,
        start_influxdb,
        wait_for_influxdb,
    )

    if not skip_extract:
        extract_backups(input_dir)
    start_influxdb()
    wait_for_influxdb()
    restore_backups()
    print("InfluxDB is running with restored databases.")
    print("Next: 'cosmic-pi export' to export to GeoParquet.")


@app.command()
def export(
    parquet_dir: Annotated[
        Path, typer.Option(help="Output directory for parquet files")
    ] = Path("parquet"),
    dataset: Annotated[
        str, typer.Option(help="Which dataset: north, south, or all")
    ] = "all",
    kind: Annotated[str, typer.Option(help="Which kind: sensor, freq, or all")] = "all",
    influxdb_url: Annotated[
        str, typer.Option(help="InfluxDB URL")
    ] = "http://localhost:8086",
    overwrite: Annotated[
        bool, typer.Option(help="Re-export parquet files even if they exist")
    ] = False,
):
    """Export data from InfluxDB to GeoParquet.

    Requires a running InfluxDB with restored backups ('cosmic-pi ingest').
    """
    import requests

    try:
        requests.get(f"{influxdb_url}/ping", timeout=3)
    except requests.ConnectionError:
        raise SystemExit(
            f"Error: Cannot connect to InfluxDB at {influxdb_url}.\n"
            "Run 'cosmic-pi ingest' first."
        )

    from .ingest import export_all

    export_all(
        parquet_dir=parquet_dir,
        dataset=dataset,
        kind=kind,
        influxdb_url=influxdb_url,
        overwrite=overwrite,
    )


@app.command()
def viz(
    parquet_dir: Annotated[
        Path, typer.Option(help="Directory with parquet files")
    ] = Path("parquet"),
):
    """Generate polar map visualizations from GeoParquet data."""
    from .viz import generate

    generate(input_dir=parquet_dir)


@app.command()
def stop():
    """Stop the InfluxDB container."""
    from .ingest import teardown

    teardown()


@app.command()
def clean(
    input_dir: Annotated[
        Path, typer.Option(help="Directory with InfluxDB data")
    ] = Path("input"),
):
    """Remove persisted InfluxDB data to free disk space.

    Stops InfluxDB first if running.
    """
    from .ingest import clean as do_clean

    do_clean(input_dir)
