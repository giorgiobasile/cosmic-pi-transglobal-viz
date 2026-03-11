# Cosmic Pi Polar Visualization

Polar stereographic visualization of cosmic ray muon measurements collected during the [Transglobal Car Expedition](https://transglobalcar.com/science/cosmicpi) using Cosmic Pi detectors.

## Datasets

- **North Pole (2024):** [Cosmic Pi North Pole Dataset 2024](https://zenodo.org/records/13310276) — First ground-level muon measurements at the North Pole, collected traveling across North America to the North Pole and back to Greenland. Authors: James Devine (CERN), Etam Noah Messomo. DOI: 10.5281/zenodo.13310276
- **South Pole (2024/2025):** [Cosmic Pi South Pole Dataset 2024/2025](https://zenodo.org/records/18774704) — First ground-level muon measurements through Antarctica, collected traveling from Cape Town through Antarctica and into South and Central America. Authors: Etam Noah Messomo, James Devine (CERN). DOI: 10.5281/zenodo.18774704

## Setup

### 1. Download datasets

Download the two zip files from Zenodo into `input/`:
- `cosmic_pi_polar_integrated_all.zip` (North Pole)
- `cosmicpisouthpole.zip` (South Pole)

### 2. Ingest data

Requires Docker. This starts InfluxDB, restores the portable backups, exports to GeoParquet, and tears down the container:

```bash
./scripts/ingest.sh
```

Produces `input/north.parquet` and `input/south.parquet`.

### 3. Generate visualization

```bash
uv run python main.py
```

Produces `output/polar_maps.png` with north and south polar stereographic projections side by side.
