import os

import cartopy.crs as ccrs
import geopandas as gpd
import matplotlib.path as mpath
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

output_dir = "output"
os.makedirs(output_dir, exist_ok=True)

# --- Expedition device IDs (largest geographic spread per dataset) ---
NORTH_DEVICE_IDS = ["202481593594661", "202481596047885"]
SOUTH_DEVICE_ID = "49269301902064"

# EU bounding box for filtering
EU_BBOX = {"lat_min": 35, "lat_max": 72, "lon_min": -10, "lon_max": 40}

DEVICE_COLORS = {
    "202481596047885": "#0a854b",
    "202481593594661": "darkblue",
    SOUTH_DEVICE_ID: "magenta",
}
# Draw order: lime first, then darkblue on top, then magenta
DEVICE_ORDER = ["202481596047885", "202481593594661", SOUTH_DEVICE_ID]

# --- Load and filter route data ---
print("Loading route data...")
north_route = gpd.read_parquet(
    "input/north.parquet", columns=["time", "id", "geometry"]
)
north_route = north_route[north_route["id"].isin(NORTH_DEVICE_IDS)]
north_route = north_route[
    north_route["time"] >= "2023-01-01"
]  # drop bogus 1970 timestamps
north_route = north_route.sort_values("time")

south_route = gpd.read_parquet(
    "input/south.parquet", columns=["time", "id", "geometry"]
)
south_route = south_route[south_route["id"] == SOUTH_DEVICE_ID]
south_route = south_route.sort_values("time")

# --- Load and aggregate freq data ---
print("Loading cosmic ray rate data...")
north_freq = gpd.read_parquet("input/north_freq.parquet")
north_freq = north_freq[north_freq["id"].isin(NORTH_DEVICE_IDS)]

south_freq = gpd.read_parquet("input/south_freq.parquet")
south_freq = south_freq[south_freq["id"] == SOUTH_DEVICE_ID]


# Resample to hourly means for manageable scatter
def resample_freq(gdf: gpd.GeoDataFrame, freq: str = "1h") -> gpd.GeoDataFrame:
    """Resample event counts to temporal bins, keeping first geometry."""
    gdf = gdf.sort_values("time").copy()
    gdf["time_bin"] = gdf["time"].dt.floor(freq)
    agg = (
        gdf.groupby("time_bin")
        .agg(
            event_count=("event_count", "mean"),
            geometry=("geometry", "first"),
        )
        .reset_index()
    )
    return gpd.GeoDataFrame(agg, geometry="geometry", crs="EPSG:4326")


north_freq_h = resample_freq(north_freq)
south_freq_h = resample_freq(south_freq)

# Filter out zero-count bins
for gdf in [north_freq_h, south_freq_h]:
    gdf.drop(gdf[gdf["event_count"] <= 0].index, inplace=True)

# Combine freq data — both expeditions plotted on both hemispheres
all_freq = pd.concat([north_freq_h, south_freq_h], ignore_index=True)


def filter_eu(gdf):
    """Remove points inside the EU bounding box."""
    return gdf[
        ~(
            (gdf.geometry.y.between(EU_BBOX["lat_min"], EU_BBOX["lat_max"]))
            & (gdf.geometry.x.between(EU_BBOX["lon_min"], EU_BBOX["lon_max"]))
        )
    ]


def build_routes(north_route, south_route, step=500):
    """Build list of (device_id, downsampled GeoDataFrame) tuples in draw order."""
    by_device = {}
    for dev_id, group in north_route.groupby("id"):
        by_device[dev_id] = group.iloc[::step]
    by_device[SOUTH_DEVICE_ID] = south_route.iloc[::step]
    return [(did, by_device[did]) for did in DEVICE_ORDER if did in by_device]


# Truncate YlOrRd to skip the pale yellow (use 20%-100% of the range)
full_cmap = plt.get_cmap("YlOrRd")
truncated_cmap = mcolors.LinearSegmentedColormap.from_list(
    "YlOrRd_trunc", full_cmap(np.linspace(0.2, 1.0, 256))
)


def setup_polar_ax(ax, extent):
    ax.set_extent(extent, crs=ccrs.PlateCarree())
    theta = np.linspace(0, 2 * np.pi, 100)
    verts = np.vstack([np.sin(theta), np.cos(theta)]).T
    circle = mpath.Path(verts * 0.5 + 0.5)
    ax.set_boundary(circle, transform=ax.transAxes)
    ax.stock_img()
    ax.gridlines(
        draw_labels=False, linewidth=0.3, color="white", alpha=0.5, linestyle="--"
    )


def plot_polar_maps(all_routes, all_freq, output_path, subtitle=None):
    fig = plt.figure(figsize=(16, 8))
    fig.subplots_adjust(wspace=0)
    transform = ccrs.PlateCarree()

    ax_north = fig.add_subplot(
        1, 2, 1, projection=ccrs.NorthPolarStereo(central_longitude=-170)
    )
    ax_south = fig.add_subplot(
        1, 2, 2, projection=ccrs.SouthPolarStereo(central_longitude=10)
    )

    setup_polar_ax(ax_north, [-180, 180, 0, 90])
    setup_polar_ax(ax_south, [-180, 180, -90, 0])

    # Draw all routes on both maps — cartopy clips to the visible extent automatically
    for ax in [ax_north, ax_south]:
        for dev_id, route_ds in all_routes:
            color = DEVICE_COLORS[dev_id]
            label = f"Device {dev_id[-4:]}" if ax is ax_north else None
            ax.plot(
                route_ds.geometry.x,
                route_ds.geometry.y,
                color=color,
                linewidth=2.0,
                alpha=0.8,
                linestyle=(0, (5, 5)),
                transform=transform,
                zorder=4,
                label=label,
            )

    # Draw all cosmic ray rates on both maps
    vmin = all_freq["event_count"].quantile(0.05)
    vmax = all_freq["event_count"].quantile(0.95)
    scatter_kwargs = dict(
        cmap=truncated_cmap,
        s=8,
        edgecolors="none",
        transform=transform,
        zorder=5,
        vmin=vmin,
        vmax=vmax,
        alpha=0.7,
    )

    for ax in [ax_north, ax_south]:
        sc = ax.scatter(
            all_freq.geometry.x,
            all_freq.geometry.y,
            c=all_freq["event_count"],
            **scatter_kwargs,
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

    fig.legend(
        loc="lower left",
        fontsize=8,
        framealpha=0.5,
        facecolor="white",
        edgecolor="black",
        labelcolor="black",
        bbox_to_anchor=(0.69, 0.18),
    )

    title = "Cosmic Pi — Transglobal Car Expedition"
    if subtitle:
        title += f"\n{subtitle}"
    fig.suptitle(title, fontsize=16, color="black", y=0.98)

    fig.savefig(output_path, dpi=600, bbox_inches="tight", transparent=True)
    plt.close(fig)
    print(f"Saved {output_path}")


# --- Version 1: all data ---
all_routes = build_routes(north_route, south_route)
print(
    f"All data — Route pts: {sum(len(r) for _, r in all_routes)}, Rate pts: {len(all_freq)}"
)
plot_polar_maps(all_routes, all_freq, f"{output_dir}/cosmic_pi_transglobal_exp.png")

# --- Version 2: EU points filtered out ---
all_routes_no_eu = build_routes(filter_eu(north_route), filter_eu(south_route))
all_freq_no_eu = filter_eu(all_freq)
print(
    f"No EU — Route pts: {sum(len(r) for _, r in all_routes_no_eu)}, Rate pts: {len(all_freq_no_eu)}"
)
plot_polar_maps(
    all_routes_no_eu,
    all_freq_no_eu,
    f"{output_dir}/cosmic_pi_transglobal_exp_no_eu.png",
    subtitle="(EU data excluded)",
)
