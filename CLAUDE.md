# Cosmic Pi Transglobal Viz

Polar stereographic visualization of cosmic ray muon measurements from the Transglobal Car Expedition.

## Project structure

```
main.py                            # Visualization script — reads GeoParquet, produces polar maps
scripts/ingest.sh                  # Full data pipeline: extract zips → InfluxDB → GeoParquet → teardown
scripts/export_geoparquet.py       # Export sensor data (CosmicPiV1.8.1) to GeoParquet
scripts/export_freq_geoparquet.py  # Export freq data (CosmicPiV1.8.1_freq) to GeoParquet
docker-compose.yml                 # InfluxDB 1.8 container (8GB memory limit)
input/                             # Data directory (gitignored)
  *.zip                            # Raw InfluxDB 1.x portable backups from Zenodo
  north.parquet                    # Sensor GeoParquet — 44.9M rows, ~1GB
  south.parquet                    # Sensor GeoParquet — 9.6M rows, ~164MB
  north_freq.parquet               # Freq GeoParquet — 13.3M rows, ~120MB
  south_freq.parquet               # Freq GeoParquet — 2.7M rows, ~30MB
output/                            # Output directory (gitignored)
  cosmic_pi_transglobal_exp.png          # All data version
  cosmic_pi_transglobal_exp_no_eu.png  # EU bounding box filtered out
```

## Data pipeline

1. `./scripts/ingest.sh` runs the full pipeline (requires Docker):
   - Extracts zip backups if needed
   - Starts InfluxDB 1.8 container
   - Restores portable backups: `cosmicpiglobal` → `cosmicpi_north`, `cosmicpilocal` → `cosmicpi_south`
   - Exports to GeoParquet via weekly-chunked HTTP CSV streaming
   - Verifies exported row counts match InfluxDB exactly
   - Tears down container and volumes

2. `uv run python main.py` reads the parquet files and generates the visualization

## Data details

### InfluxDB source
- Measurement: `CosmicPiV1.8.1` (also `CosmicPiV1.6.1` exists but unused)
- Frequency measurement: `CosmicPiV1.8.1_freq` (event_count + geohash → decoded to lat/lon)
- Tags: `id` (device ID)
- Fields: `Accelx`, `Accely`, `Accelz`, `Alt`, `Hum`, `Magx`, `Magy`, `Magz`, `Press`, `Temp`, `lat`, `lon`
- North backup source db: `cosmicpiglobal`
- South backup source db: `cosmicpilocal`

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

## Commands

```bash
# Full data pipeline (one-time, requires Docker)
./scripts/ingest.sh

# Generate visualization (requires input/*.parquet)
uv run python main.py

# Just the export step (requires running InfluxDB)
uv run python scripts/export_geoparquet.py
```

## Visualization details

- **Two output versions**: `polar_maps.png` (all data) and `polar_maps_no_eu.png` (EU bbox 35-72°N, -10-40°E filtered out)
- **All data on both hemispheres**: both expedition routes and all muon rates are plotted on both maps; Cartopy clips to visible extent
- **Expedition devices**: North: `["202481593594661", "202481596047885"]`, South: `"49269301902064"`
- **Per-device route colors**: darkblue (4661), lime (7885), magenta (south) — drawn in that z-order
- **Route downsampling**: stride-sample every 500th point, split by device to avoid interpolation artifacts
- **Freq resampling**: hourly mean of event_count (not sum — event_count is "events in this second", mean gives comparable rate)
- **Projections**: NorthPolarStereo(central_longitude=-170), SouthPolarStereo(central_longitude=10) — Americas bridge the inner edges
- **Colormap**: truncated OrRd (30-100% range) with 5th/95th percentile clipping
- **Data filtering**: only bogus 1970 timestamps removed; CERN/Europe data kept in full version
- **CERN artifact**: south device logged at CERN (46.27°N, 6.27°E) before shipping to Cape Town — causes straight line across Africa in full version

## Dependencies

Managed with `uv`. Key packages: `geopandas`, `cartopy`, `pyarrow`, `requests`, `scipy`.
