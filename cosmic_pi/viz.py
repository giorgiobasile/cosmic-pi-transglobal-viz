"""Polar stereographic visualization of cosmic ray muon measurements."""

from pathlib import Path

import cartopy.crs as ccrs
import geopandas as gpd
import matplotlib.colors as mcolors
import matplotlib.path as mpath
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# --- Constants ---

NORTH_DEVICE_IDS = ["202481593594661", "202481596047885"]
SOUTH_DEVICE_ID = "49269301902064"

DEVICE_COLORS = {
    "202481596047885": "#0a854b",
    "202481593594661": "darkblue",
    SOUTH_DEVICE_ID: "magenta",
}
DEVICE_ORDER = ["202481596047885", "202481593594661", SOUTH_DEVICE_ID]


# --- Data loading ---


def load_route_data(
    input_dir: Path,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Load and filter north/south expedition route data."""
    north = gpd.read_parquet(
        input_dir / "cosmic_pi_north_pole.parquet", columns=["time", "id", "geometry"]
    )
    north = north[north["id"].isin(NORTH_DEVICE_IDS)]
    north = north[north["time"] >= "2023-01-01"]
    north = north.sort_values("time")

    south = gpd.read_parquet(
        input_dir / "cosmic_pi_south_pole.parquet", columns=["time", "id", "geometry"]
    )
    south = south[south["id"] == SOUTH_DEVICE_ID]
    south = south.sort_values("time")

    return north, south


def load_freq_data(input_dir: Path) -> gpd.GeoDataFrame:
    """Load, resample to hourly means, and combine freq data."""
    north = gpd.read_parquet(input_dir / "cosmic_pi_north_pole_freq.parquet")
    north = north[north["id"].isin(NORTH_DEVICE_IDS)]

    south = gpd.read_parquet(input_dir / "cosmic_pi_south_pole_freq.parquet")
    south = south[south["id"] == SOUTH_DEVICE_ID]

    north_h = _resample_freq(north)
    south_h = _resample_freq(south)

    for gdf in [north_h, south_h]:
        gdf.drop(gdf[gdf["event_count"] <= 0].index, inplace=True)

    return pd.concat([north_h, south_h], ignore_index=True)


# --- Data transforms ---


def _resample_freq(gdf: gpd.GeoDataFrame, freq: str = "1h") -> gpd.GeoDataFrame:
    """Resample event counts to temporal bins, keeping first geometry."""
    gdf = gdf.sort_values("time").copy()
    gdf["time_bin"] = gdf["time"].dt.floor(freq)
    agg = (
        gdf.groupby("time_bin")
        .agg(event_count=("event_count", "mean"), geometry=("geometry", "first"))
        .reset_index()
    )
    return gpd.GeoDataFrame(agg, geometry="geometry", crs="EPSG:4326")


def build_routes(
    north_route: gpd.GeoDataFrame,
    south_route: gpd.GeoDataFrame,
    step: int = 500,
) -> list[tuple[str, gpd.GeoDataFrame]]:
    """Build downsampled route segments in draw order."""
    by_device = {}
    for dev_id, group in north_route.groupby("id"):
        by_device[dev_id] = group.iloc[::step]
    by_device[SOUTH_DEVICE_ID] = south_route.iloc[::step]
    return [(did, by_device[did]) for did in DEVICE_ORDER if did in by_device]


# --- Plotting ---


def _make_colormap():
    """Truncated YlOrRd — skip the pale yellow end."""
    full = plt.get_cmap("YlOrRd")
    return mcolors.LinearSegmentedColormap.from_list(
        "YlOrRd_trunc", full(np.linspace(0.2, 1.0, 256))
    )


def _setup_polar_ax(ax, extent):
    """Configure a polar stereographic axis with circular boundary."""
    ax.set_extent(extent, crs=ccrs.PlateCarree())
    theta = np.linspace(0, 2 * np.pi, 100)
    verts = np.vstack([np.sin(theta), np.cos(theta)]).T
    circle = mpath.Path(verts * 0.5 + 0.5)
    ax.set_boundary(circle, transform=ax.transAxes)
    ax.stock_img()
    ax.gridlines(
        draw_labels=False, linewidth=0.3, color="white", alpha=0.5, linestyle="--"
    )


def plot_polar_maps(
    all_routes: list[tuple[str, gpd.GeoDataFrame]],
    all_freq: gpd.GeoDataFrame,
    output_path: str | Path,
):
    """Render north/south polar maps with expedition routes and muon rates."""
    fig = plt.figure(figsize=(16, 8))
    fig.subplots_adjust(wspace=0)
    transform = ccrs.PlateCarree()

    ax_north = fig.add_subplot(
        1, 2, 1, projection=ccrs.NorthPolarStereo(central_longitude=-170)
    )
    ax_south = fig.add_subplot(
        1, 2, 2, projection=ccrs.SouthPolarStereo(central_longitude=10)
    )

    _setup_polar_ax(ax_north, [-180, 180, 0, 90])
    _setup_polar_ax(ax_south, [-180, 180, -90, 0])

    # Routes
    for ax in [ax_north, ax_south]:
        for dev_id, route_ds in all_routes:
            ax.plot(
                route_ds.geometry.x,
                route_ds.geometry.y,
                color=DEVICE_COLORS[dev_id],
                linewidth=2.0,
                alpha=0.8,
                linestyle=(0, (5, 5)),
                transform=transform,
                zorder=4,
            )

    # Cosmic ray rates
    vmin = all_freq["event_count"].quantile(0.05)
    vmax = all_freq["event_count"].quantile(0.95)
    cmap = _make_colormap()

    for ax in [ax_north, ax_south]:
        sc = ax.scatter(
            all_freq.geometry.x,
            all_freq.geometry.y,
            c=all_freq["event_count"],
            cmap=cmap,
            s=8,
            edgecolors="none",
            transform=transform,
            zorder=5,
            vmin=vmin,
            vmax=vmax,
            alpha=0.7,
        )

    ax_north.set_title("Northern Hemisphere", fontsize=14, color="black")
    ax_south.set_title("Southern Hemisphere", fontsize=14, color="black")

    fig.patch.set_alpha(0)
    cbar = fig.colorbar(
        sc,
        ax=[ax_north, ax_south],
        label="Muon Detection Rate (avg events/sec)",
        shrink=0.7,
        pad=0.05,
    )
    cbar.ax.yaxis.label.set_color("black")
    cbar.ax.tick_params(colors="black")

    fig.suptitle(
        "Cosmic Pi — Transglobal Car Expedition", fontsize=16, color="black", y=0.98
    )

    fig.savefig(output_path, dpi=600, bbox_inches="tight", transparent=True)
    plt.close(fig)
    print(f"Saved {output_path}")


# --- Entry point ---


OUTPUT_FILE = Path("cosmic_pi_transglobal_exp.png")


def generate(
    input_dir: Path = Path("parquet"),
    output: Path = OUTPUT_FILE,
):
    """Generate polar map visualizations from GeoParquet data."""
    required = [
        "cosmic_pi_north_pole.parquet",
        "cosmic_pi_south_pole.parquet",
        "cosmic_pi_north_pole_freq.parquet",
        "cosmic_pi_south_pole_freq.parquet",
    ]
    missing = [f for f in required if not (input_dir / f).exists()]
    if missing:
        msg = f"Missing parquet files in {input_dir}/: {', '.join(missing)}"
        raise SystemExit(f"Error: {msg}\nRun 'cosmic-pi ingest' first.")

    print("Loading route data...")
    north_route, south_route = load_route_data(input_dir)

    print("Loading cosmic ray rate data...")
    all_freq = load_freq_data(input_dir)

    all_routes = build_routes(north_route, south_route)
    print(f"Route pts: {sum(len(r) for _, r in all_routes)}, Rate pts: {len(all_freq)}")
    plot_polar_maps(all_routes, all_freq, output)
