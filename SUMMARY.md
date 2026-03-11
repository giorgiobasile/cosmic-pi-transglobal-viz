# Cosmic Pi Polar Visualization — Project Summary

## What this project is about

Space is full of high-energy particles — protons, atomic nuclei, and other fragments — flying around at nearly the speed of light. These are **cosmic rays**, and they come from supernovae, black holes, and other violent events across the universe. When one of these particles slams into Earth's atmosphere, it triggers a chain reaction: the original particle smashes into an air molecule, which produces more particles, which hit more molecules, and so on. This cascade is called an **air shower**.

Most of the particles created in the shower don't make it to ground level — they're absorbed or decay along the way. But one type gets through: **muons**. A muon is essentially a heavier version of an electron (about 200 times heavier). They're created high in the atmosphere and travel so fast (close to the speed of light) that they reach the ground before they decay — a neat real-world example of Einstein's time dilation. Muons pass through walls, rock, and even you without stopping. About one hits every square centimeter of Earth's surface every minute.

Because muons are the ground-level footprint of cosmic rays, counting them tells you about cosmic ray activity. The rate varies with altitude (more muons at higher elevations, since they haven't traveled as far), latitude (Earth's magnetic field deflects some cosmic rays, more so near the equator), and solar activity (the sun's magnetic field shields us from some cosmic rays).

**Cosmic Pi** is a small, portable muon detector built at CERN. It uses a scintillator — a piece of plastic that emits a tiny flash of light when a muon passes through it — coupled with a light sensor to count hits. It logs each detection alongside GPS position, temperature, pressure, humidity, and other sensor readings.

In 2024–2025, two Cosmic Pi detectors were taken on the **Transglobal Car Expedition** — one of the first attempts to measure ground-level cosmic ray rates while driving to both the North and South Poles:

- **North Pole (2024):** Across North America, up to the North Pole, back to Greenland
- **South Pole (2024/2025):** From Cape Town, through Antarctica, into South America

The datasets are published on Zenodo. This project takes those raw measurements and visualizes the expedition routes and cosmic ray detection rates on polar stereographic maps.

## The raw data

The detector logs data to **InfluxDB 1.x**, a time-series database. The datasets are distributed as **InfluxDB portable backups** — zip files containing binary TSM (Time-Structured Merge tree) shard files. These are InfluxDB's internal storage format and cannot be read directly by anything except InfluxDB itself.

### How InfluxDB organizes data

InfluxDB structures data differently from a regular database:

- A **measurement** is like a table (e.g., `CosmicPiV1.8.1`)
- Each data point has a **timestamp**, one or more **fields** (the actual values), and optional **tags** (indexed metadata for filtering)
- There is no fixed schema — fields can vary between points

In our data, there are two measurements per dataset. They come from the same physical device but log different things:

**`CosmicPiV1.8.1`** — the **environment stream** (~5 readings/second):

This is the detector continuously logging its surroundings and position. Every ~200 milliseconds, the device writes a snapshot of all its sensors: where it is (GPS), what the weather is like (temperature, pressure, humidity), how it's moving (accelerometer), and which direction it's facing (magnetometer). Think of it as a very detailed travel log.

| Column | Type | Description |
|--------|------|-------------|
| `time` | timestamp | Nanosecond precision |
| `id` | tag | Device serial number |
| `lat`, `lon` | field (float) | GPS position |
| `Temp` | field (float) | Temperature |
| `Press` | field (float) | Atmospheric pressure |
| `Hum` | field (float) | Humidity |
| `Alt` | field (float) | Altitude |
| `Accelx/y/z` | field (float) | Accelerometer |
| `Magx/y/z` | field (float) | Magnetometer |

**`CosmicPiV1.8.1_freq`** — the **cosmic ray event stream**:

This is the actual science data. Each time the detector's scintillator registers a muon hit, it increments a counter. Periodically, the device writes out how many muon hits it counted since the last write, along with where it was at that time (encoded as a geohash rather than raw lat/lon). The `event_count` field is what we actually care about for cosmic ray mapping — it tells us the muon detection rate at each location along the route.

| Column | Type | Description |
|--------|------|-------------|
| `time` | timestamp | Nanosecond precision |
| `id` | tag | Device serial number |
| `event_count` | field (float) | Number of muon detections in the interval |
| `geohash` | field (string) | Location encoded as a [geohash](https://en.wikipedia.org/wiki/Geohash) |

**Why two separate measurements?** The environment sensors sample at a fixed high rate (5 Hz) regardless of cosmic ray activity, while muon detections are event-driven — they happen when they happen. Keeping them separate avoids writing 12 sensor columns on every muon event and avoids writing an event count on every 200ms sensor tick. The two streams share the same `id` tag and `time` axis, so they can be joined when needed.

The north dataset contains data from 72 devices (many are stationary stations from the broader Cosmic Pi network), while the south dataset has only 2 devices. The expedition vehicles carried specific devices that we identify by their geographic spread — a device that has traveled 45° of latitude is clearly on the expedition, not sitting on someone's desk.

### Dataset sizes

| Dataset | Sensor rows | Freq rows |
|---------|------------|-----------|
| North Pole | 44,925,912 | 13,321,256 |
| South Pole | 9,614,900 | 2,712,216 |

## Data transformation pipeline

Since InfluxDB portable backups are opaque binary files, we need to run an actual InfluxDB instance to read them. The pipeline (`scripts/ingest.sh`) does everything automatically:

### Step 1: Extract and restore

We start a **Docker container** running InfluxDB 1.8 (with 8 GB memory — these are big datasets), then restore the portable backups into two separate databases:

- `cosmicpiglobal` (the original north DB name) → restored as `cosmicpi_north`
- `cosmicpilocal` (the original south DB name) → restored as `cosmicpi_south`

### Step 2: Export to GeoParquet

This is where the bulk of the work happens. We can't just `SELECT * FROM measurement` — with 45 million rows, InfluxDB's query engine runs out of memory and crashes. We solved this by:

1. **Querying the actual time range** from the database (`first()` and `last()` timestamps) rather than hardcoding dates
2. **Splitting queries into weekly chunks** — each week's data is small enough for InfluxDB to handle
3. **Streaming CSV** from the HTTP API (`Accept: application/csv` header) instead of JSON — much more memory-efficient
4. **Writing Parquet incrementally** — each batch of 200K rows is converted and appended via PyArrow's `ParquetWriter`, so we never hold the full dataset in memory

For the sensor data (`CosmicPiV1.8.1`), the `lat` and `lon` fields are converted into Point geometries.

For the frequency data (`CosmicPiV1.8.1_freq`), the `geohash` string is decoded into lat/lon coordinates using the `python-geohash` library, then converted to Point geometries.

Both are written as **GeoParquet** — a Parquet file with additional metadata that tells GIS tools about the geometry column (encoding, coordinate system, geometry types). We serialize geometries as WKB (Well-Known Binary) and manually inject the GeoParquet metadata into the Parquet file's schema.

### Step 3: Verification

After export, the script queries InfluxDB for the exact row count and compares it to the number of rows written to Parquet. If they don't match, the pipeline fails. This caught an earlier bug where hardcoded date ranges missed data from unexpected timestamps (some readings had timestamps in 1970 or 2027 due to GPS initialization quirks).

### Step 4: Teardown

The Docker container and its volumes are removed. The Parquet files in `input/` are the only output. InfluxDB is never needed again unless you want to re-export.

### Resulting files

| File | Rows | Size | Content |
|------|------|------|---------|
| `north.parquet` | 44.9M | 1.0 GB | All sensor fields + Point geometry from lat/lon |
| `south.parquet` | 9.6M | 164 MB | All sensor fields + Point geometry from lat/lon |
| `north_freq.parquet` | 13.3M | 120 MB | Event count + Point geometry from geohash |
| `south_freq.parquet` | 2.7M | 30 MB | Event count + Point geometry from geohash |

## The visualization

The final output is a polar stereographic map showing both expeditions side by side.

### What we plot

**Route lines (cyan):** The sensor data records a GPS position every ~200 milliseconds. For the north expedition alone, that's 29 million points from the mobile devices — far too many to plot. We **stride-sample** by taking every 500th point, which gives us roughly one position every 100 seconds. This is purely a thinning operation: no averaging or interpolation, just picking every Nth row from the time-sorted data. The result is ~58K points for north and ~19K for south — enough to draw a smooth continuous line on the map without overwhelming the renderer. These points are connected as a line path to show the expedition route.

**Cosmic ray rates (colored dots):** The frequency data records `event_count` — the number of muon detections in a given second — at irregular intervals, sometimes multiple readings per second, sometimes with gaps. We **temporally resample** by grouping all readings into 1-hour bins, averaging the `event_count` within each bin, and taking the GPS position of the first reading in each bin as the location. We use the mean rather than the sum because it gives a comparable "detections per second" rate regardless of how many readings fell in each bin (an hour with spotty data is still directly comparable to a full hour). Bins with zero detections are dropped. The result is ~2,000 hourly data points for north and ~900 for south. Each dot is colored by its mean rate using a hot colormap (yellow ≈ 1 event/sec, red/dark ≈ 7 events/sec).

### The map projection

Both maps use **polar stereographic projection** — the kind where you're looking straight down at the pole. The North Pole map is centered on -70° longitude (to center North America), and the South Pole map on 20° longitude (to center the Africa-Antarctica route). A circular boundary clips the projection to a clean disk.

The background is Natural Earth satellite imagery with white coastlines and grid lines overlaid.
