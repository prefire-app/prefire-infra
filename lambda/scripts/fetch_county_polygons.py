# scripts/fetch_county_polygons.py
# Reads a local California counties shapefile, reprojects to EPSG:3857,
# filters to the target counties, simplifies, and writes county_polygons.json.
#
# Usage:
#   python lambda/scripts/fetch_county_polygons.py path/to/ca_counties.shp
#
# The shapefile must have either:
#   - a COUNTYFP column with 3-digit FIPS codes (e.g. "001"), or
#   - a GEOID column with 5-digit codes (e.g. "06001")
import json
import sys
from pathlib import Path

import geopandas as gpd
from shapely import to_geojson

TARGET_FIPS = {
    "001",  # Alameda
    "013",  # Contra Costa
    "017",  # El Dorado
    "037",  # Los Angeles
    "041",  # Marin
    "057",  # Nevada
    "059",  # Orange
    "061",  # Placer
    "073",  # San Diego
    "081",  # San Mateo
    "085",  # Santa Clara
    "087",  # Santa Cruz
    "097",  # Sonoma
}

if len(sys.argv) < 2:
    print("Usage: python lambda/scripts/fetch_county_polygons.py path/to/ca_counties.shp")
    sys.exit(1)

shp_path = Path(sys.argv[1])
gdf = gpd.read_file(shp_path)

# Normalise the FIPS column — try COUNTYFP first, then derive from GEOID
if "COUNTYFP" in gdf.columns:
    gdf["fips3"] = gdf["COUNTYFP"].str.zfill(3)
elif "GEOID" in gdf.columns:
    gdf["fips3"] = gdf["GEOID"].str[-3:]
else:
    raise ValueError(f"Could not find COUNTYFP or GEOID in columns: {list(gdf.columns)}")

# Filter to target counties
gdf = gdf[gdf["fips3"].isin(TARGET_FIPS)].copy()
if gdf.empty:
    raise ValueError("No matching counties found — check shapefile FIPS values")

# Reproject to EPSG:26910 (NAD83 / UTM Zone 10N) to match the COG files' native CRS
gdf = gdf.to_crs(epsg=26910)

features = []
for _, row in gdf.iterrows():
    # Simplify to ~100 m tolerance — cuts vertex count ~80% with negligible error
    simplified = row.geometry.simplify(100, preserve_topology=True)
    features.append({"fips": row["fips3"], "geometry": json.loads(to_geojson(simplified))})

out_path = Path(__file__).parent.parent / "county_polygons.json"
with open(out_path, "w") as f:
    json.dump(features, f)
print(f"Wrote {len(features)} county polygons → {out_path}")