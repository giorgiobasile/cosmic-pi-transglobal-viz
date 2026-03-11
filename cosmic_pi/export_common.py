"""Shared infrastructure for exporting InfluxDB data to GeoParquet.

Provides the common export loop, InfluxDB query helpers, and GeoParquet
writing used by both sensor and frequency exports.
"""

import csv
import io
import sys
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta

import geopandas as gpd
import pandas as pd
import requests

BATCH_SIZE = 200_000


@dataclass
class ExportConfig:
    """Configuration for a dataset export."""

    measurement: str
    count_field: str
    where_clause: str
    rows_to_gdf: Callable[[list[list[str]], list[str]], gpd.GeoDataFrame]


def influxql(url: str, db: str, query: str) -> dict:
    resp = requests.get(f"{url}/query", params={"db": db, "q": query})
    resp.raise_for_status()
    return resp.json()


def get_expected_count(
    url: str, db: str, measurement: str, count_field: str, where_clause: str = ""
) -> int:
    where = f" WHERE {where_clause}" if where_clause else ""
    query = f'SELECT count({count_field}) FROM "{measurement}"{where}'
    result = influxql(url, db, query)
    values = result["results"][0]["series"][0]["values"]
    return int(values[0][1])


def get_time_range(
    url: str, db: str, measurement: str, field: str, where_clause: str = ""
) -> tuple[str, str]:
    where = f" WHERE {where_clause}" if where_clause else ""
    r_first = influxql(url, db, f'SELECT first({field}) FROM "{measurement}"{where}')
    r_last = influxql(url, db, f'SELECT last({field}) FROM "{measurement}"{where}')
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


def query_csv_stream(url: str, db: str, query: str) -> requests.Response:
    resp = requests.get(
        f"{url}/query",
        params={"db": db, "q": query},
        headers={"Accept": "application/csv"},
        stream=True,
    )
    resp.raise_for_status()
    resp.encoding = "utf-8"
    return resp


def export_dataset(url: str, db: str, output_path: str, config: ExportConfig):
    """Export a dataset from InfluxDB to GeoParquet."""
    expected = get_expected_count(
        url, db, config.measurement, config.count_field, config.where_clause
    )
    print(f"  Expected rows: {expected}")

    start, end = get_time_range(
        url, db, config.measurement, config.count_field, config.where_clause
    )
    print(f"  Time range: {start} → {end}")

    gdfs = []
    columns = None
    total = 0

    where = f" AND {config.where_clause}" if config.where_clause else ""

    for w_start, w_end in week_ranges(start, end):
        query = f"""
SELECT *
FROM "{config.measurement}"
WHERE time >= '{w_start}' AND time < '{w_end}'{where}
"""
        resp = query_csv_stream(url, db, query)
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
                gdfs.append(config.rows_to_gdf(batch_rows, columns))
                total += len(batch_rows)
                batch_rows = []

        resp.close()

        if batch_rows:
            gdfs.append(config.rows_to_gdf(batch_rows, columns))
            total += len(batch_rows)

        if total > 0:
            print(f"  {total} rows through {w_end[:10]}")

    if gdfs:
        gdf = pd.concat(gdfs, ignore_index=True)
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:4326")
        gdf.to_parquet(output_path, schema_version="1.1.0", write_covering_bbox=True)

    print(f"  Exported: {total} rows → {output_path}")

    if total != expected:
        print(f"  ERROR: row count mismatch! Expected {expected}, got {total}")
        sys.exit(1)
    else:
        print(f"  Verified: {total} == {expected}")
