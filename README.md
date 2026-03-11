# Cosmic Pi Transglobal Visualization

Polar stereographic visualization of cosmic ray muon measurements collected during the [Transglobal Car Expedition](https://transglobalcar.com/science/cosmicpi) using Cosmic Pi detectors.

<p align="center">
  <picture>
    <img src="cosmic_pi_transglobal_exp.png" alt="Cosmic Pi — Transglobal Car Expedition" width="800" style="margin-top: -6%; clip-path: inset(6% 0 0 0);">
  </picture>
</p>

## Background

[**Cosmic Pi**](https://cosmicpi.org/) is a portable [muon](https://en.wikipedia.org/wiki/Muon) detector built at [CERN](https://home.cern/). In 2024–2025, Cosmic Pi detectors were carried on the [**Transglobal Car Expedition**](https://transglobalcar.com/science/cosmicpi) — one of the first attempts to measure ground-level [cosmic ray](https://en.wikipedia.org/wiki/Cosmic_ray) rates while driving to both poles:

- **North Pole (2024):** Across North America to the North Pole, back to Greenland
- **South Pole (2024–2025):** Cape Town through Antarctica into South America

## Datasets

- **North Pole (2024):** [Cosmic Pi North Pole Dataset 2024](https://zenodo.org/records/13310276) — First ground-level muon measurements at the North Pole, collected traveling across North America to the North Pole and back to Greenland. Authors: James Devine (CERN), Etam Noah Messomo. [DOI: 10.5281/zenodo.13310276](https://doi.org/10.5281/zenodo.13310276)
- **South Pole (2024/2025):** [Cosmic Pi South Pole Dataset 2024/2025](https://zenodo.org/records/18774704) — First ground-level muon measurements through Antarctica, collected traveling from Cape Town through Antarctica and into South and Central America. Authors: Etam Noah Messomo, James Devine (CERN). [DOI: 10.5281/zenodo.18774704](https://doi.org/10.5281/zenodo.18774704)

## Data

The detectors log to **InfluxDB 1.x** and the datasets are distributed as portable backups. Each contains multiple measurement streams; the two used by this project are:

- **`CosmicPiV1.8.1`** — environment stream (~5 Hz): GPS position, temperature, pressure, humidity, altitude, accelerometer, magnetometer
- **`CosmicPiV1.8.1_freq`** — cosmic ray events: muon detection count per interval + geohash-encoded location

Data is exported to [GeoParquet](https://geoparquet.org/) for local analysis.

## Usage

The project is packaged as a [Typer](https://typer.tiangolo.com/) CLI managed with [uv](https://docs.astral.sh/uv/):

```bash
uv run cosmic-pi --help
```

### From Zenodo backups (full pipeline)

If you're starting from the raw Zenodo zip files and don't have InfluxDB set up (requires [Docker](https://www.docker.com/)):

```bash
uv run cosmic-pi download          # download zips from Zenodo (~7.4 GB)
uv run cosmic-pi influxdb-restore  # extract, start InfluxDB (Docker), restore backups
uv run cosmic-pi gpq-export        # export to GeoParquet
uv run cosmic-pi influxdb-stop     # stop InfluxDB when done
uv run cosmic-pi viz               # generate visualization
```

### From an existing InfluxDB

If you already have the data in a running InfluxDB instance, skip straight to export:

```bash
uv run cosmic-pi gpq-export --influxdb-url http://your-host:8086 --db your_database_name
uv run cosmic-pi viz
```

### Other commands

```bash
# Export a specific dataset/kind
uv run cosmic-pi gpq-export --dataset north --kind freq

# Stop InfluxDB and remove persisted data to free disk space
uv run cosmic-pi influxdb-clean
```

## Development

Pre-commit hooks run [ruff](https://docs.astral.sh/ruff/) linting and formatting on every commit:

```bash
pre-commit install
```
