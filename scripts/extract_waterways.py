#!/usr/bin/env python3
"""
Extract waterway features (dams, barrages, canals) from OSM data,
clip to Ganga basin, snap to river segments, and output CSVs.
"""

import json
import geopandas as gpd
import pandas as pd
import numpy as np
from pathlib import Path
from shapely.ops import nearest_points
from shapely.geometry import Point

# Paths
BASE_DIR = Path("/Users/kuhu.gupta/VizChitra")
WATERWAYS_GPKG = BASE_DIR / "Raw Data/OSM waterways/waterways.gpkg"
BASINS_KML = BASE_DIR / "Shapefiles/Indian Basin Shapefiles/IndiaBasins.kml"
RIVERS_SHP = BASE_DIR / "Shapefiles/Ganga Basin Streams/ganga_rivers_named.shp"
DAMS_JSON = BASE_DIR / "Raw Data/Dams/dams_detail.json"
OUTPUT_DIR = BASE_DIR / "Processed Data"

def load_ganga_basin():
    """Load Ganga basin boundary from KML."""
    print("Loading Ganga basin boundary...")
    basins = gpd.read_file(BASINS_KML, engine='pyogrio')
    ganga = basins[basins['ba_name'] == 'Ganga']
    ganga = ganga.to_crs('EPSG:4326')
    print(f"  Ganga basin boundary loaded: {len(ganga)} polygon(s)")
    return ganga.geometry.union_all()

def load_waterways():
    """Load waterways from GeoPackage."""
    print("Loading waterways.gpkg...")
    gdf = gpd.read_file(WATERWAYS_GPKG, engine='pyogrio')
    gdf = gdf.to_crs('EPSG:4326')
    print(f"  Total waterway features: {len(gdf)}")
    return gdf

def load_rivers():
    """Load river segments for snapping."""
    print("Loading ganga_rivers_named.shp...")
    rivers = gpd.read_file(RIVERS_SHP)
    rivers = rivers.to_crs('EPSG:4326')
    print(f"  River segments loaded: {len(rivers)}")
    return rivers

def load_dams_detail():
    """Load dam details JSON for capacity lookup."""
    print("Loading dams_detail.json...")
    with open(DAMS_JSON, 'r') as f:
        content = f.read()
        # Handle require() statements by removing them
        import re
        content = re.sub(r'require\([^)]+\)', '""', content)
        dams = json.loads(content)
    print(f"  Dam details loaded: {len(dams)} records")
    return dams

def extract_layers(waterways, ganga_boundary):
    """Extract and clip dam, barrage, and canal layers."""
    print("\nExtracting waterway layers...")

    # Filter by waterway type
    dams = waterways[waterways['waterway'] == 'dam'].copy()
    barrages = waterways[waterways['waterway'].isin(['weir', 'barrage'])].copy()
    canals = waterways[waterways['waterway'] == 'canal'].copy()

    print(f"  Before clipping:")
    print(f"    Dams: {len(dams)}")
    print(f"    Barrages (weir/barrage): {len(barrages)}")
    print(f"    Canals: {len(canals)}")

    # Clip to Ganga basin
    print("\nClipping to Ganga basin...")
    dams = dams[dams.intersects(ganga_boundary)]
    barrages = barrages[barrages.intersects(ganga_boundary)]
    canals = canals[canals.intersects(ganga_boundary)]

    print(f"  After clipping:")
    print(f"    Dams: {len(dams)}")
    print(f"    Barrages: {len(barrages)}")
    print(f"    Canals: {len(canals)}")

    return dams, barrages, canals

def get_feature_point(geom):
    """Get representative point for any geometry type."""
    if geom is None:
        return None
    if geom.geom_type == 'Point':
        return geom
    elif geom.geom_type in ['LineString', 'MultiLineString']:
        # Use centroid for lines
        return geom.centroid
    elif geom.geom_type in ['Polygon', 'MultiPolygon']:
        return geom.centroid
    else:
        return geom.representative_point()

def snap_to_rivers(features, rivers, max_distance_deg=0.1):
    """Snap features to nearest river segments, get seg_id and river_name."""
    print(f"  Snapping {len(features)} features to rivers...")

    if len(features) == 0:
        return pd.DataFrame(columns=['name', 'river_name', 'seg_id', 'latitude', 'longitude'])

    results = []
    rivers_union = rivers.unary_union

    for idx, row in features.iterrows():
        pt = get_feature_point(row.geometry)
        if pt is None:
            continue

        name = row.get('name') or row.get('name_en') or row.get('name_hi') or ''

        # Find nearest river segment
        nearest_geom = nearest_points(pt, rivers_union)[1]
        min_dist = pt.distance(nearest_geom)

        # Find which segment it belongs to
        best_seg_id = None
        best_river_name = None
        min_seg_dist = float('inf')

        for _, river in rivers.iterrows():
            dist = pt.distance(river.geometry)
            if dist < min_seg_dist:
                min_seg_dist = dist
                best_seg_id = river['seg_id']
                best_river_name = river['river_name']

        # Only include if within max distance
        if min_seg_dist <= max_distance_deg:
            results.append({
                'name': name if name else None,
                'river_name': best_river_name,
                'seg_id': best_seg_id,
                'latitude': round(pt.y, 6),
                'longitude': round(pt.x, 6)
            })

    return pd.DataFrame(results)

def snap_to_rivers_optimized(features, rivers, max_distance_deg=0.1):
    """Optimized snapping using spatial index."""
    print(f"  Snapping {len(features)} features to rivers (optimized)...")

    if len(features) == 0:
        return pd.DataFrame(columns=['name', 'river_name', 'seg_id', 'latitude', 'longitude'])

    # Create spatial index
    from shapely import STRtree
    tree = STRtree(rivers.geometry.values)

    results = []

    for idx, row in features.iterrows():
        pt = get_feature_point(row.geometry)
        if pt is None:
            continue

        name = row.get('name') or row.get('name_en') or row.get('name_hi') or ''

        # Query nearest neighbors
        nearest_idx = tree.nearest(pt)
        nearest_river = rivers.iloc[nearest_idx]
        min_dist = pt.distance(nearest_river.geometry)

        # Only include if within max distance
        if min_dist <= max_distance_deg:
            results.append({
                'name': name if name else None,
                'river_name': nearest_river['river_name'],
                'seg_id': nearest_river['seg_id'],
                'latitude': round(pt.y, 6),
                'longitude': round(pt.x, 6)
            })
        else:
            # Still include but mark as not snapped
            results.append({
                'name': name if name else None,
                'river_name': None,
                'seg_id': None,
                'latitude': round(pt.y, 6),
                'longitude': round(pt.x, 6)
            })

    return pd.DataFrame(results)

def join_dam_capacity(dams_df, dams_detail):
    """Join capacity information based on name matching."""
    print("\nJoining dam capacity data...")

    if len(dams_df) == 0:
        dams_df['capacity'] = []
        return dams_df

    # Create lookup dict from dams_detail
    capacity_lookup = {}
    for dam in dams_detail:
        dam_name = dam.get('Name', '').strip().lower()
        capacity = dam.get('GrossStorageCapacity')
        if dam_name and capacity:
            capacity_lookup[dam_name] = capacity

    print(f"  Capacity lookup has {len(capacity_lookup)} entries")

    # Match by name (fuzzy)
    def find_capacity(name):
        if pd.isna(name) or not isinstance(name, str) or not name:
            return None
        name_lower = name.strip().lower()
        # Exact match
        if name_lower in capacity_lookup:
            return capacity_lookup[name_lower]
        # Partial match
        for key, val in capacity_lookup.items():
            if name_lower in key or key in name_lower:
                return val
        return None

    dams_df['capacity'] = dams_df['name'].apply(find_capacity)
    matched = dams_df['capacity'].notna().sum()
    print(f"  Matched capacity for {matched} dams")

    return dams_df

def main():
    print("=" * 60)
    print("Ganga Basin Waterway Feature Extraction")
    print("=" * 60)

    # Load data
    ganga_boundary = load_ganga_basin()
    waterways = load_waterways()
    rivers = load_rivers()
    dams_detail = load_dams_detail()

    # Extract and clip layers
    dams, barrages, canals = extract_layers(waterways, ganga_boundary)

    # Snap to rivers
    print("\nProcessing dams...")
    dams_df = snap_to_rivers_optimized(dams, rivers)
    dams_df = join_dam_capacity(dams_df, dams_detail)

    print("\nProcessing barrages...")
    barrages_df = snap_to_rivers_optimized(barrages, rivers)
    barrages_df['capacity'] = None  # No capacity data for barrages

    print("\nProcessing canals...")
    canals_df = snap_to_rivers_optimized(canals, rivers)
    canals_df['capacity'] = None  # No capacity data for canals

    # Reorder columns
    cols = ['name', 'river_name', 'seg_id', 'latitude', 'longitude', 'capacity']
    dams_df = dams_df[cols]
    barrages_df = barrages_df[cols]
    canals_df = canals_df[cols]

    # Save to CSV
    print("\n" + "=" * 60)
    print("Saving output files...")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    dams_df.to_csv(OUTPUT_DIR / "ganga_dams.csv", index=False)
    print(f"  Saved ganga_dams.csv ({len(dams_df)} records)")

    barrages_df.to_csv(OUTPUT_DIR / "ganga_barrages.csv", index=False)
    print(f"  Saved ganga_barrages.csv ({len(barrages_df)} records)")

    canals_df.to_csv(OUTPUT_DIR / "ganga_canals.csv", index=False)
    print(f"  Saved ganga_canals.csv ({len(canals_df)} records)")

    # Summary
    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Dams: {len(dams_df)} features")
    print(f"    - With name: {dams_df['name'].notna().sum()}")
    print(f"    - With capacity: {dams_df['capacity'].notna().sum()}")
    print(f"    - Snapped to river: {dams_df['seg_id'].notna().sum()}")
    print(f"  Barrages: {len(barrages_df)} features")
    print(f"    - With name: {barrages_df['name'].notna().sum()}")
    print(f"    - Snapped to river: {barrages_df['seg_id'].notna().sum()}")
    print(f"  Canals: {len(canals_df)} features")
    print(f"    - With name: {canals_df['name'].notna().sum()}")
    print(f"    - Snapped to river: {canals_df['seg_id'].notna().sum()}")
    print("=" * 60)

if __name__ == "__main__":
    main()
