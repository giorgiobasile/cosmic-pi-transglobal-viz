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
    skip_teardown: Annotated[
        bool, typer.Option(help="Leave InfluxDB running after export")
    ] = False,
):
    """Full pipeline: extract zips, start InfluxDB, restore, export, teardown."""
    from .ingest import run

    run(input_dir, skip_extract=skip_extract, skip_teardown=skip_teardown)


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
):
    """Export data from a running InfluxDB to GeoParquet.

    Requires a running InfluxDB instance with restored backups.
    Use 'cosmic-pi ingest --skip-teardown' to leave it running.
    """
    import requests

    try:
        requests.get(f"{influxdb_url}/ping", timeout=3)
    except requests.ConnectionError:
        raise SystemExit(
            f"Error: Cannot connect to InfluxDB at {influxdb_url}.\n"
            "Start it with 'docker compose up -d influxdb' or run 'cosmic-pi ingest'."
        )

    from .export_common import export_dataset
    from .export_freq import DATASETS as FREQ_DATASETS
    from .export_freq import FREQ_CONFIG
    from .export_sensor import DATASETS as SENSOR_DATASETS
    from .export_sensor import SENSOR_CONFIG

    configs = []
    if kind in ("all", "sensor"):
        for name, cfg in SENSOR_DATASETS.items():
            if dataset in ("all", name):
                output = str(parquet_dir / Path(cfg["output"]).name)
                configs.append((name, "sensor", cfg["db"], output, SENSOR_CONFIG))
    if kind in ("all", "freq"):
        for name, cfg in FREQ_DATASETS.items():
            if dataset in ("all", name):
                output = str(parquet_dir / Path(cfg["output"]).name)
                configs.append((name, "freq", cfg["db"], output, FREQ_CONFIG))

    for name, kind_label, db, output, config in configs:
        print(f"Exporting {name} {kind_label} ({db})...")
        export_dataset(influxdb_url, db, output, config)


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
def clean(
    input_dir: Annotated[
        Path, typer.Option(help="Directory with InfluxDB data")
    ] = Path("input"),
):
    """Remove persisted InfluxDB data to free disk space."""
    from .ingest import clean as do_clean

    do_clean(input_dir)
