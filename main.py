import os

import matplotlib.path as mpath
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
from shapely.geometry import Point

rng = np.random.default_rng(42)

_shp = cfeature.NaturalEarthFeature("physical", "land", "110m")
land = gpd.GeoDataFrame(geometry=[g for g in _shp.geometries()], crs="EPSG:4326").union_all()
output_dir = "output"
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


def generate_polar_gdf(lat_range: tuple[float, float], n: int = 30) -> gpd.GeoDataFrame:
    points, values = [], []
    while len(points) < n:
        batch = n * 10
        lats = rng.uniform(*lat_range, size=batch)
        lons = rng.uniform(-180, 180, size=batch)
        vals = rng.uniform(0, 100, size=batch)
        for lat, lon, val in zip(lats, lons, vals):
            pt = Point(lon, lat)
            if land.contains(pt):
                points.append(pt)
                values.append(val)
                if len(points) >= n:
                    break
    return gpd.GeoDataFrame({"value": values}, geometry=points, crs="EPSG:4326")


def setup_polar_ax(ax, extent):
    ax.set_extent(extent, crs=ccrs.PlateCarree())
    theta = np.linspace(0, 2 * np.pi, 100)
    center, radius = [0.5, 0.5], 0.5
    verts = np.vstack([np.sin(theta), np.cos(theta)]).T
    circle = mpath.Path(verts * radius + center)
    ax.set_boundary(circle, transform=ax.transAxes)
    ax.stock_img()
    ax.add_feature(cfeature.COASTLINE, linewidth=0.5, edgecolor="white")
    ax.gridlines(draw_labels=False, linewidth=0.3, color="white", alpha=0.5, linestyle="--")


north_gdf = generate_polar_gdf((0, 90))
south_gdf = generate_polar_gdf((-90, 0))

fig = plt.figure(figsize=(14, 7))
fig.subplots_adjust(wspace=0)
ax_north = fig.add_subplot(1, 2, 1, projection=ccrs.NorthPolarStereo(central_longitude=-70))
ax_south = fig.add_subplot(1, 2, 2, projection=ccrs.SouthPolarStereo(central_longitude=110))

setup_polar_ax(ax_north, [-180, 180, 0, 90])
setup_polar_ax(ax_south, [-180, 180, -90, 0])

scatter_kwargs = dict(cmap="plasma", s=15, edgecolors="black", linewidths=0.3, transform=ccrs.PlateCarree(), zorder=5, vmin=0, vmax=100)

ax_north.scatter(north_gdf.geometry.x, north_gdf.geometry.y, c=north_gdf["value"], **scatter_kwargs)
sc = ax_south.scatter(south_gdf.geometry.x, south_gdf.geometry.y, c=south_gdf["value"], **scatter_kwargs)

ax_north.set_title("North Pole", fontsize=14)
ax_south.set_title("South Pole", fontsize=14)

fig.colorbar(sc, ax=[ax_north, ax_south], label="Value", shrink=0.7, pad=0.05)
fig.savefig(f"{output_dir}/polar_maps.png", dpi=300, bbox_inches="tight")
plt.close(fig)
print("Saved polar_maps.png")




