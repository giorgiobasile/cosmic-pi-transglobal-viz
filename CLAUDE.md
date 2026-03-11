# Cosmic Pi Transglobal Viz

Polar stereographic visualization of cosmic ray muon measurements from the Transglobal Car Expedition.

## Project structure

```
cosmic_pi/                         # Python package
  cli.py                           # Typer CLI — ingest, export, viz commands
  ingest.py                        # Docker orchestration (extract, restore, export, teardown)
  export_common.py                 # Shared InfluxDB/GeoParquet export infrastructure
  export_sensor.py                 # Sensor-specific export config (CosmicPiV1.8.1)
  export_freq.py                   # Freq-specific export config (CosmicPiV1.8.1_freq)
  viz.py                           # Visualization — data loading, transforms, plotting
input/                             # Data directory (gitignored)
  *.zip                            # Raw InfluxDB 1.x portable backups from Zenodo
  influxdb-data/                   # Persisted InfluxDB data (survives container restarts)
parquet/                           # GeoParquet files (gitignored)
  cosmic_pi_north_pole.parquet     # Sensor GeoParquet — 44.9M rows, ~1GB
  cosmic_pi_south_pole.parquet     # Sensor GeoParquet — 9.6M rows, ~164MB
  cosmic_pi_north_pole_freq.parquet # Freq GeoParquet — 13.3M rows, ~120MB
  cosmic_pi_south_pole_freq.parquet # Freq GeoParquet — 2.7M rows, ~30MB
cosmic_pi_transglobal_exp.png      # Polar map visualization (tracked in git)
```

## Commands

```bash
# Download datasets from Zenodo (~7.4 GB)
uv run cosmic-pi download

# Extract, start InfluxDB, restore backups (requires Docker)
uv run cosmic-pi influx-restore

# Export to GeoParquet (requires running InfluxDB)
uv run cosmic-pi gpq-export
uv run cosmic-pi gpq-export --dataset north --kind sensor

# Generate visualization (requires parquet/*.parquet)
uv run cosmic-pi viz

# Stop InfluxDB
uv run cosmic-pi influx-stop

# Remove persisted InfluxDB data
uv run cosmic-pi influx-clean
```

## Data pipeline

1. `cosmic-pi ingest` runs the full pipeline (requires Docker):
   - Extracts zip backups if needed
   - Starts InfluxDB 1.8 container (bind-mount volume at `input/influxdb-data/`)
   - Restores portable backups (skipped if databases already exist)
   - Exports to GeoParquet via weekly-chunked HTTP CSV streaming
   - Verifies exported row counts match InfluxDB exactly
   - Stops container (data persists for faster re-runs; `cosmic-pi clean` to remove)

2. `cosmic-pi viz` reads the parquet files and generates the visualization

## Data details

### InfluxDB source
- North backup source db: `cosmicpiglobal` → restored as `cosmicpi_north`
- South backup source db: `cosmicpilocal` → restored as `cosmicpi_south`
- North has 72 devices (mostly stationary); south has 2. Expedition devices identified by geographic spread.

**`CosmicPiV1.8.1`** — environment stream (~5 readings/sec):

| Column | Type | Description |
|--------|------|-------------|
| `time` | timestamp | Nanosecond precision |
| `id` | tag | Device serial number |
| `lat`, `lon` | field (float) | GPS position |
| `Temp`, `Press`, `Hum`, `Alt` | field (float) | Weather sensors |
| `Accelx/y/z` | field (float) | Accelerometer |
| `Magx/y/z` | field (float) | Magnetometer |

**`CosmicPiV1.8.1_freq`** — cosmic ray event stream:

| Column | Type | Description |
|--------|------|-------------|
| `time` | timestamp | Nanosecond precision |
| `id` | tag | Device serial number |
| `event_count` | field (float) | Muon detections in the interval |
| `geohash` | field (string) | Location as geohash (decoded to lat/lon during export) |

Two separate measurements because environment sensors sample at fixed 5 Hz while muon detections are event-driven.

### GeoParquet schema
- `time` (datetime64[ns]), `Accelx..z`, `Alt`, `Hum`, `Magx..z`, `Press`, `Temp` (float64), `id` (str), `tags` (str), `geometry` (Point, EPSG:4326)
- Geometry built from lat/lon, filtered for `lat != 0 AND lon != 0`
- GeoParquet metadata manually injected (WKB encoding)

### Time ranges
- North: sparse data from 1970, bulk from late 2023 to mid 2024, sparse to 2027
- South: sparse from 2021, bulk from Oct 2024 to Apr 2025

## Key technical decisions

- **InfluxDB 1.8 via Docker** for restoring portable backups — no direct path to v3 or native parquet export
- **Weekly time-chunked queries** to avoid InfluxDB OOM — monthly/full-table queries crash even with 8GB
- **HTTP CSV streaming** (Accept: application/csv) not JSON — much more memory efficient for large results
- **influx_inspect export** was attempted but dumps one field per line (not grouped by timestamp), making it impractical
- **Manual GeoParquet metadata injection** because pyarrow doesn't know about geometry; we serialize to WKB and add the `geo` metadata key ourselves
- **Row count verification** built into export script — exits non-zero on mismatch

## Visualization details

- **All data on both hemispheres**: both expedition routes and all muon rates are plotted on both maps; Cartopy clips to visible extent
- **Expedition devices**: North: `["202481593594661", "202481596047885"]`, South: `"49269301902064"`
- **Per-device route colors**: darkblue (4661), green (7885), magenta (south) — drawn in that z-order
- **Route downsampling**: stride-sample every 500th point, split by device to avoid interpolation artifacts
- **Freq resampling**: hourly mean of event_count (not sum — event_count is "events in this second", mean gives comparable rate)
- **Projections**: NorthPolarStereo(central_longitude=-170), SouthPolarStereo(central_longitude=10) — Americas bridge the inner edges
- **Colormap**: truncated YlOrRd (20-100% range) with 5th/95th percentile clipping
- **Data filtering**: only bogus 1970 timestamps removed; CERN/Europe data kept in full version
- **CERN artifact**: south device logged at CERN (46.27°N, 6.27°E) before shipping to Cape Town — causes straight line across Africa in full version

## Dependencies

Managed with `uv`. Key packages: `geopandas`, `cartopy`, `pyarrow`, `requests`, `scipy`, `typer`.
