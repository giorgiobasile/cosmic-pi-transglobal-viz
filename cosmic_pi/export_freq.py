"""Frequency/event count export configuration (CosmicPiV1.8.1_freq)."""

import geohash
import geopandas as gpd
import pandas as pd

from .export_common import ExportConfig

DATASETS = {
    "north": {
        "db": "cosmicpi_north",
        "output": "parquet/cosmic_pi_north_pole_freq.parquet",
    },
    "south": {
        "db": "cosmicpi_south",
        "output": "parquet/cosmic_pi_south_pole_freq.parquet",
    },
}


def rows_to_gdf(rows: list[list[str]], columns: list[str]) -> gpd.GeoDataFrame:
    df = pd.DataFrame(rows, columns=columns)
    df["time"] = pd.to_datetime(pd.to_numeric(df["time"]), unit="ns")
    df["event_count"] = pd.to_numeric(df["event_count"], errors="coerce")
    coords = df["geohash"].apply(lambda gh: geohash.decode(gh) if gh else (None, None))
    df["lat"] = coords.apply(lambda c: float(c[0]) if c[0] is not None else None)
    df["lon"] = coords.apply(lambda c: float(c[1]) if c[1] is not None else None)
    geometry = gpd.points_from_xy(df["lon"], df["lat"])
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    return gdf.drop(columns=["lat", "lon", "geohash"], errors="ignore")


FREQ_CONFIG = ExportConfig(
    measurement="CosmicPiV1.8.1_freq",
    count_field="event_count",
    where_clause="",
    rows_to_gdf=rows_to_gdf,
)
