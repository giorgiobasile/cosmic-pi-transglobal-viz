"""Export InfluxDB CosmicPiV1.8.1_freq data to GeoParquet files.

Streams CSV from the InfluxDB HTTP API in weekly time chunks,
decodes geohash to lat/lon, and writes GeoParquet incrementally.
"""

import csv
import io
import json
import sys
from datetime import datetime, timedelta

import geohash
import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

INFLUXDB_URL = "http://localhost:8086"
BATCH_SIZE = 200_000
MEASUREMENT = "CosmicPiV1.8.1_freq"

DATASETS = {
    "north": {"db": "cosmicpi_north", "output": "input/north_freq.parquet"},
    "south": {"db": "cosmicpi_south", "output": "input/south_freq.parquet"},
}


def influxql(db: str, query: str) -> dict:
    resp = requests.get(f"{INFLUXDB_URL}/query", params={"db": db, "q": query})
    resp.raise_for_status()
    return resp.json()


def get_expected_count(db: str) -> int:
    query = f'SELECT count(event_count) FROM "{MEASUREMENT}"'
    result = influxql(db, query)
    values = result["results"][0]["series"][0]["values"]
    return int(values[0][1])


def get_time_range(db: str) -> tuple[str, str]:
    q_first = f'SELECT first(event_count) FROM "{MEASUREMENT}"'
    q_last = f'SELECT last(event_count) FROM "{MEASUREMENT}"'
    r_first = influxql(db, q_first)
    r_last = influxql(db, q_last)
    t_first = r_first["results"][0]["series"][0]["values"][0][0]
    t_last = r_last["results"][0]["series"][0]["values"][0][0]
    dt_first = datetime.fromisoformat(t_first.replace("Z", "+00:00")) - timedelta(
        days=1
    )
    dt_last = datetime.fromisoformat(t_last.replace("Z", "+00:00")) + timedelta(days=1)
    return dt_first.strftime("%Y-%m-%d"), dt_last.strftime("%Y-%m-%d")


def week_ranges(start: str, end: str):
    cur = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while cur < end_dt:
        nxt = min(cur + timedelta(days=7), end_dt)
        yield cur.strftime("%Y-%m-%dT%H:%M:%SZ"), nxt.strftime("%Y-%m-%dT%H:%M:%SZ")
        cur = nxt


def query_csv_stream(db: str, query: str) -> requests.Response:
    resp = requests.get(
        f"{INFLUXDB_URL}/query",
        params={"db": db, "q": query},
        headers={"Accept": "application/csv"},
        stream=True,
    )
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp


def rows_to_gdf(rows: list[list[str]], columns: list[str]) -> gpd.GeoDataFrame:
    df = pd.DataFrame(rows, columns=columns)
    df["time"] = pd.to_datetime(pd.to_numeric(df["time"]), unit="ns")
    df["event_count"] = pd.to_numeric(df["event_count"], errors="coerce")

    # Decode geohash to lat/lon
    coords = df["geohash"].apply(lambda gh: geohash.decode(gh) if gh else (None, None))
    df["lat"] = coords.apply(lambda c: float(c[0]) if c[0] is not None else None)
    df["lon"] = coords.apply(lambda c: float(c[1]) if c[1] is not None else None)

    geometry = gpd.points_from_xy(df["lon"], df["lat"])
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    gdf = gdf.drop(columns=["lat", "lon", "geohash"], errors="ignore")
    return gdf


def gdf_to_arrow(gdf: gpd.GeoDataFrame) -> pa.Table:
    df = pd.DataFrame(gdf.drop(columns="geometry"))
    df["geometry"] = gdf.geometry.to_wkb()
    return pa.Table.from_pandas(df, preserve_index=False)


def write_batch(writer_holder, gdf, output_path, geo_meta_holder):
    table = gdf_to_arrow(gdf)
    if writer_holder[0] is None:
        geo_meta_holder[0] = build_geo_metadata(gdf)
        writer_holder[0] = pq.ParquetWriter(output_path, table.schema)
    writer_holder[0].write_table(table)


def build_geo_metadata(gdf: gpd.GeoDataFrame) -> bytes:
    geo = {
        "version": "1.0.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": json.loads(gdf.crs.to_json()),
            }
        },
    }
    return json.dumps(geo).encode("utf-8")


def inject_geo_metadata(path: str, geo_metadata: bytes):
    table = pq.read_table(path)
    existing = table.schema.metadata or {}
    existing[b"geo"] = geo_metadata
    table = table.replace_schema_metadata(existing)
    pq.write_table(table, path)


def export_dataset(db: str, output_path: str):
    expected = get_expected_count(db)
    print(f"  Expected rows: {expected}")

    start, end = get_time_range(db)
    print(f"  Time range: {start} → {end}")

    writer = [None]
    geo_meta = [None]
    columns = None
    total = 0

    for w_start, w_end in week_ranges(start, end):
        query = f"""
SELECT *
FROM "{MEASUREMENT}"
WHERE time >= '{w_start}' AND time < '{w_end}'
"""
        resp = query_csv_stream(db, query)
        lines = resp.iter_lines(decode_unicode=True)
        header_line = next(lines, None)
        if header_line is None or not header_line.strip():
            resp.close()
            continue

        chunk_columns = next(csv.reader(io.StringIO(header_line)))[1:]  # skip "name"
        if columns is None:
            columns = chunk_columns

        batch_rows = []
        for line in lines:
            if not line:
                continue
            row = next(csv.reader(io.StringIO(line)))
            batch_rows.append(row[1:])

            if len(batch_rows) >= BATCH_SIZE:
                gdf = rows_to_gdf(batch_rows, columns)
                write_batch(writer, gdf, output_path, geo_meta)
                total += len(batch_rows)
                batch_rows = []

        resp.close()

        if batch_rows:
            gdf = rows_to_gdf(batch_rows, columns)
            write_batch(writer, gdf, output_path, geo_meta)
            total += len(batch_rows)

        if total > 0:
            print(f"  {total} rows through {w_end[:10]}")

    if writer[0] is not None:
        writer[0].close()
        inject_geo_metadata(output_path, geo_meta[0])

    print(f"  Exported: {total} rows → {output_path}")

    if total != expected:
        print(f"  ERROR: row count mismatch! Expected {expected}, got {total}")
        sys.exit(1)
    else:
        print(f"  Verified: {total} == {expected}")


def main():
    for name, cfg in DATASETS.items():
        print(f"Exporting {name} freq ({cfg['db']})...")
        export_dataset(cfg["db"], cfg["output"])


if __name__ == "__main__":
    main()
