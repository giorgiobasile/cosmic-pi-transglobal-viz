"""Sensor data export configuration (CosmicPiV1.8.1)."""

import geopandas as gpd
import pandas as pd

from .export_common import ExportConfig

FLOAT_COLS = [
    "Accelx",
    "Accely",
    "Accelz",
    "Alt",
    "Hum",
    "Magx",
    "Magy",
    "Magz",
    "Press",
    "Temp",
    "lat",
    "lon",
]

DATASETS = {
    "north": {"db": "cosmicpi_north", "output": "parquet/cosmic_pi_north_pole.parquet"},
    "south": {"db": "cosmicpi_south", "output": "parquet/cosmic_pi_south_pole.parquet"},
}


def rows_to_gdf(rows: list[list[str]], columns: list[str]) -> gpd.GeoDataFrame:
    df = pd.DataFrame(rows, columns=columns)
    df["time"] = pd.to_datetime(pd.to_numeric(df["time"]), unit="ns")
    for col in FLOAT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    geometry = gpd.points_from_xy(df["lon"], df["lat"])
    gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")
    return gdf.drop(columns=["lat", "lon"], errors="ignore")


SENSOR_CONFIG = ExportConfig(
    measurement="CosmicPiV1.8.1",
    count_field="lat",
    where_clause="lat != 0 AND lon != 0",
    rows_to_gdf=rows_to_gdf,
)
