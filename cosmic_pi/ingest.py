"""Full data pipeline: extract zips, start InfluxDB, restore, export, teardown."""

import shutil
import subprocess
import time
import zipfile
from pathlib import Path

import requests

from .export_common import export_dataset
from .export_freq import DATASETS as FREQ_DATASETS
from .export_freq import FREQ_CONFIG
from .export_sensor import DATASETS as SENSOR_DATASETS
from .export_sensor import SENSOR_CONFIG

INFLUXDB_URL = "http://localhost:8086"
INFLUXDB_DATA_DIR = "influxdb-data"

NORTH_ZIP = "cosmic_pi_polar_integrated_all.zip"
SOUTH_ZIP = "cosmicpisouthpole.zip"

ZENODO_FILES = {
    NORTH_ZIP: "https://zenodo.org/api/records/13310276/files/cosmic_pi_polar_integrated_all.zip/content",
    SOUTH_ZIP: "https://zenodo.org/api/records/18774704/files/cosmicpisouthpole.zip/content",
}

RESTORES = [
    {
        "source_db": "cosmicpiglobal",
        "target_db": "cosmicpi_north",
        "backup_dir": "cosmic_pi_polar_integrated_all",
    },
    {
        "source_db": "cosmicpilocal",
        "target_db": "cosmicpi_south",
        "backup_dir": "cosmicpisouthpole",
    },
]


def download_datasets(input_dir: Path):
    """Download zip backups from Zenodo if not already present."""
    input_dir.mkdir(parents=True, exist_ok=True)
    for filename, url in ZENODO_FILES.items():
        dest = input_dir / filename
        if dest.exists():
            print(f"{dest} already exists, skipping download.")
            continue
        print(f"Downloading {filename}...")
        with requests.get(url, stream=True, timeout=30) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            downloaded = 0
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = downloaded * 100 // total
                        print(
                            f"\r  {downloaded // (1024 * 1024)} / {total // (1024 * 1024)} MB ({pct}%)",
                            end="",
                            flush=True,
                        )
            print()
        print(f"  Saved {dest}")


def extract_backups(input_dir: Path):
    for zip_name in [NORTH_ZIP, SOUTH_ZIP]:
        zip_path = input_dir / zip_name
        if not zip_path.exists():
            raise SystemExit(
                f"Error: {zip_path} not found.\n"
                "Run 'cosmic-pi download' first, or download manually from Zenodo."
            )
        extract_dir = input_dir / zip_name.removesuffix(".zip")
        if extract_dir.exists():
            print(f"{extract_dir} already extracted, skipping.")
            continue
        print(f"Extracting {zip_name}...")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(input_dir)


def start_influxdb():
    print("Starting InfluxDB...")
    subprocess.run(["docker", "compose", "up", "-d", "influxdb"], check=True)


def wait_for_influxdb():
    print("Waiting for InfluxDB to be healthy...")
    while True:
        result = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "influxdb",
                "influx",
                "-execute",
                "SHOW DATABASES",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            break
        time.sleep(2)
    print("InfluxDB is ready.")


def _databases_exist() -> bool:
    """Check if both target databases already exist in InfluxDB."""
    try:
        resp = requests.get(
            f"{INFLUXDB_URL}/query",
            params={"q": "SHOW DATABASES"},
        )
        resp.raise_for_status()
        series = resp.json()["results"][0].get("series", [])
        if not series:
            return False
        db_names = {row[0] for row in series[0]["values"]}
        needed = {r["target_db"] for r in RESTORES}
        return needed.issubset(db_names)
    except (requests.ConnectionError, KeyError, IndexError):
        return False


def restore_backups():
    if _databases_exist():
        print("Databases already restored, skipping restore.")
        return

    for r in RESTORES:
        print(f"Restoring {r['source_db']} → {r['target_db']}...")
        result = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "influxdb",
                "influxd",
                "restore",
                "-portable",
                "-db",
                r["source_db"],
                "-newdb",
                r["target_db"],
                f"/backups/{r['backup_dir']}",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(
                f"  Restore returned non-zero (may already exist): {result.stderr.strip()}"
            )
        else:
            print(f"  Restored {r['target_db']}.")


def export_all(parquet_dir: Path = Path("parquet")):
    parquet_dir.mkdir(parents=True, exist_ok=True)
    print("Exporting sensor data to GeoParquet...")
    for name, cfg in SENSOR_DATASETS.items():
        output = str(parquet_dir / Path(cfg["output"]).name)
        print(f"  {name} ({cfg['db']})...")
        export_dataset(INFLUXDB_URL, cfg["db"], output, SENSOR_CONFIG)

    print("Exporting freq data to GeoParquet...")
    for name, cfg in FREQ_DATASETS.items():
        output = str(parquet_dir / Path(cfg["output"]).name)
        print(f"  {name} ({cfg['db']})...")
        export_dataset(INFLUXDB_URL, cfg["db"], output, FREQ_CONFIG)


def teardown():
    print("Stopping InfluxDB...")
    subprocess.run(["docker", "compose", "down"], check=True)


def clean(input_dir: Path):
    """Remove InfluxDB data directory to free disk space."""
    data_dir = input_dir / INFLUXDB_DATA_DIR
    if data_dir.exists():
        print(f"Removing {data_dir}...")
        shutil.rmtree(data_dir)
        print("InfluxDB data cleaned.")
    else:
        print("No InfluxDB data to clean.")


def run(
    input_dir: Path,
    *,
    skip_extract: bool = False,
    skip_teardown: bool = False,
):
    if not skip_extract:
        extract_backups(input_dir)
    start_influxdb()
    wait_for_influxdb()
    restore_backups()
    export_all()
    if not skip_teardown:
        teardown()
    print("Done. GeoParquet files are in parquet/")
