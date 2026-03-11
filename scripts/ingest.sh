#!/usr/bin/env bash
# Restores InfluxDB portable backups and exports data to GeoParquet.
#
# Usage: ./scripts/ingest.sh
#
# Requires: docker compose, uv

set -euo pipefail
cd "$(dirname "$0")/.."

BACKUPS_DIR="input"
NORTH_ZIP="cosmic_pi_polar_integrated_all.zip"
SOUTH_ZIP="cosmicpisouthpole.zip"

# --- 1. Extract backups if needed ---
for zip in "$NORTH_ZIP" "$SOUTH_ZIP"; do
    dir="${BACKUPS_DIR}/${zip%.zip}"
    if [ ! -d "$dir" ]; then
        echo "Extracting $zip..."
        unzip -q "$BACKUPS_DIR/$zip" -d "$BACKUPS_DIR"
    else
        echo "$dir already extracted, skipping."
    fi
done

# --- 2. Start InfluxDB ---
echo "Starting InfluxDB..."
docker compose up -d influxdb
echo "Waiting for InfluxDB to be healthy..."
until docker compose exec -T influxdb influx -execute "SHOW DATABASES" > /dev/null 2>&1; do
    sleep 2
done
echo "InfluxDB is ready."

# --- 3. Restore backups ---
echo "Restoring north pole backup..."
docker compose exec -T influxdb influxd restore -portable -db cosmicpiglobal -newdb cosmicpi_north /backups/cosmic_pi_polar_integrated_all || echo "North restore may already exist, continuing."

echo "Restoring south pole backup..."
docker compose exec -T influxdb influxd restore -portable -db cosmicpilocal -newdb cosmicpi_south /backups/cosmicpisouthpole || echo "South restore may already exist, continuing."

# --- 4. Export to GeoParquet ---
echo "Exporting sensor data to GeoParquet..."
uv run python scripts/export_geoparquet.py

echo "Exporting freq (cosmic ray) data to GeoParquet..."
uv run python scripts/export_freq_geoparquet.py

# --- 5. Tear down ---
echo "Stopping InfluxDB..."
docker compose down -v

echo "Done. GeoParquet files are in input/"
