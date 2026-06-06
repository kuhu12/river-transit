#!/usr/bin/env python3
"""
Extract waterway features (dams, barrages, canals) from OSM data,
clip to Ganga basin, snap to river segments, and output CSVs.

Inputs:
    waterways.gpkg          — OSM India waterways
    ganga_rivers_named.shp  — named Ganga river segments

Outputs:
    ganga_dams.csv          — one row per dam, snapped to nearest river
    ganga_barrages.csv      — one row per barrage/weir, snapped to nearest river
    ganga_canals.csv        — one row per canal, located at headworks (offtake point)

Mandatory columns (row dropped only if name/latitude/longitude is missing):
    name, river_name, latitude, longitude, seg_id

Capacity enrichment from dams_detail.json is handled separately
in enrich_dam_capacity.py.
"""

import re
import geopandas as gpd
import pandas as pd
from pathlib import Path
from shapely import STRtree
from shapely.geometry import Point
from shapely.ops import linemerge

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR       = Path("/Users/kuhu.gupta/VizChitra")
WATERWAYS_GPKG = BASE_DIR / "Raw Data/OSM waterways/waterways.gpkg"
RIVERS_SHP     = BASE_DIR / "Shapefiles/Ganga Basin Streams/NamedStreams/ganga_rivers.shp"
OUTPUT_DIR     = BASE_DIR / "Processed Data"

# ── Snap thresholds ───────────────────────────────────────────────────────────
DAM_SNAP_DEG    = 0.02   # ~2.2 km — dams sit right on the river
BARRAGE_SNAP_DEG = 0.02  # ~2.2 km — same reasoning as dams
CANAL_SNAP_DEG  = 0.05   # ~5.5 km — headworks can be several km from river centreline

# ── Mandatory output columns ──────────────────────────────────────────────────
# Row dropped only if any of these three are null
MANDATORY = ['name', 'latitude', 'longitude']

# ── Dam noise exclusions ──────────────────────────────────────────────────────
# OSM tags village ponds, step-wells, and tanks as water=reservoir.
# These appear in the dam extract but are not river infrastructure.
_DAM_NOISE_RE = re.compile(
    r'\b(johad|kund|talab|talav|tank|pond|talaiya|pokhar|baoli|vav|step.?well)\b',
    re.IGNORECASE
)

# ── Generic canal name exclusions ─────────────────────────────────────────────
# Label-only names that are not actual canal identifiers
_CANAL_NOISE = {'spillway', 'nahar', 'canal', 'drain', 'nala', 'nallah'}


# =============================================================================
# LOAD
# =============================================================================

def load_rivers():
    print("Loading ganga_rivers_named.shp...")
    rivers = gpd.read_file(RIVERS_SHP).to_crs('EPSG:4326')
    print(f"  {len(rivers)} river segments loaded")
    return rivers


def load_ganga_basin(rivers):
    """
    Derive basin boundary from the river shapefile.
    Convex hull + 0.1° buffer captures all segments including
    the Bangladesh delta and Himalayan headwaters.
    """
    print("Deriving Ganga basin boundary from river shapefile...")
    basin = rivers.union_all().convex_hull.buffer(0.1)
    b = rivers.total_bounds
    print(f"  Extent: {b[0]:.2f}–{b[2]:.2f}°E, {b[1]:.2f}–{b[3]:.2f}°N")
    return basin


def load_waterways():
    print("Loading waterways.gpkg...")
    gdf = gpd.read_file(WATERWAYS_GPKG, engine='pyogrio').to_crs('EPSG:4326')
    print(f"  {len(gdf):,} total waterway features loaded")
    return gdf


# =============================================================================
# EXTRACTION AND CLIPPING
# =============================================================================

def extract_layers(waterways, basin):
    """
    Extract and clip dam, barrage, and canal layers.

    Dam OSM tags:
        waterway=dam        — dam structure line/polygon
        man_made=dam        — alternate tag for major dam structures
        water=reservoir     — impoundment polygon
        (noise filtered below in snap_dams)

    Barrage OSM tags:
        waterway=barrage    — large gated control structure
        waterway=weir       — OSM uses weir for most Indian barrages
                              e.g. Farakka, Haridwar, Narora, Kanpur
        man_made=weir       — alternate tag

    Canal OSM tags:
        waterway=canal
    """
    print("\nExtracting waterway layers...")

    dam_mask = waterways['waterway'] == 'dam'
    if 'man_made' in waterways.columns:
        dam_mask |= waterways['man_made'] == 'dam'
    if 'water' in waterways.columns:
        dam_mask |= waterways['water'] == 'reservoir'

    barrage_mask = waterways['waterway'].isin(['barrage', 'weir'])
    if 'man_made' in waterways.columns:
        barrage_mask |= waterways['man_made'] == 'weir'
    barrage_mask &= ~dam_mask   # avoid double-counting

    dams     = waterways[dam_mask].copy()
    barrages = waterways[barrage_mask].copy()
    canals   = waterways[waterways['waterway'] == 'canal'].copy()

    print(f"  Before clip — dams: {len(dams)}, "
          f"barrages: {len(barrages)}, canals: {len(canals)}")

    dams     = dams[dams.intersects(basin)]
    barrages = barrages[barrages.intersects(basin)]
    canals   = canals[canals.intersects(basin)]

    print(f"  After clip  — dams: {len(dams)}, "
          f"barrages: {len(barrages)}, canals: {len(canals)}")
    return dams, barrages, canals


# =============================================================================
# HELPERS
# =============================================================================

def get_osm_name(row):
    """Pick best available name from OSM name columns."""
    return (row.get('name') or row.get('name_en') or
            row.get('name_latin') or row.get('name_hi') or None)


def get_feature_point(geom):
    """Representative point for any geometry type."""
    if geom is None:
        return None
    t = geom.geom_type
    if t == 'Point':
        return geom
    if t in ('LineString', 'MultiLineString', 'Polygon', 'MultiPolygon'):
        return geom.centroid
    return geom.representative_point()


def snap_point(pt, tree, rivers, max_deg):
    """
    Snap a Point to the nearest river segment within max_deg.
    Returns (river_name, seg_id, dist_deg) or (None, None, None).
    """
    ni   = tree.nearest(pt)
    nr   = rivers.iloc[ni]
    dist = pt.distance(nr.geometry)
    if dist <= max_deg:
        return nr['river_name'], nr['seg_id'], round(dist, 6)
    return None, None, None


# =============================================================================
# DAMS — centroid snap with noise filtering
# =============================================================================

def snap_dams(dams, rivers, tree):
    """
    Snap dams to nearest river.
    Filters out village ponds, step-wells, and tanks that OSM tags as
    water=reservoir (Johad, Kund, Talab, Tank, Pond, etc.) — these are
    not river infrastructure and inflate the unsnapped count.
    """
    print(f"  Processing {len(dams)} raw dam features...")

    results = []
    noise_dropped = 0

    for _, row in dams.iterrows():
        name = get_osm_name(row)

        # Drop noise (village tanks, ponds, step-wells)
        if name and isinstance(name, str) and _DAM_NOISE_RE.search(name):
            noise_dropped += 1
            continue

        pt = get_feature_point(row.geometry)
        if pt is None:
            continue

        river_name, seg_id, dist = snap_point(pt, tree, rivers, DAM_SNAP_DEG)

        results.append({
            'name':       name,
            'river_name': river_name,
            'seg_id':     seg_id,
            'dist_deg':   dist,
            'latitude':   round(pt.y, 6),
            'longitude':  round(pt.x, 6),
        })

    df = pd.DataFrame(results)
    snapped = df['seg_id'].notna().sum() if len(df) else 0
    print(f"  Noise filtered (ponds/tanks): {noise_dropped}")
    print(f"  Remaining: {len(df)}, snapped: {snapped} "
          f"({snapped / max(len(df), 1) * 100:.1f}%)")
    return df


# =============================================================================
# BARRAGES — centroid snap
# =============================================================================

def snap_barrages(barrages, rivers, tree):
    """Snap barrage/weir centroids to nearest river."""
    print(f"  Processing {len(barrages)} barrage/weir features...")

    results = []
    for _, row in barrages.iterrows():
        name = get_osm_name(row)
        pt   = get_feature_point(row.geometry)
        if pt is None:
            continue

        river_name, seg_id, dist = snap_point(pt, tree, rivers, BARRAGE_SNAP_DEG)

        results.append({
            'name':       name,
            'river_name': river_name,
            'seg_id':     seg_id,
            'dist_deg':   dist,
            'latitude':   round(pt.y, 6),
            'longitude':  round(pt.x, 6),
        })

    df = pd.DataFrame(results)
    snapped = df['seg_id'].notna().sum() if len(df) else 0
    print(f"  Snapped: {snapped}/{len(df)} "
          f"({snapped / max(len(df), 1) * 100:.1f}%)")
    return df


# =============================================================================
# CANALS — headworks (offtake) point, one row per canal
# =============================================================================

def extract_canal_headworks(canals, rivers, tree):
    """
    Produce one row per named canal located at its headworks (offtake point).

    A canal's meaningful location is where it branches off from its parent
    river, not arbitrary centroids along its length. OSM stores long canals
    as chains of many separate 'way' features sharing the same name, which
    naively produces dozens of rows per canal.

    Algorithm:
      1. Group all OSM way segments by canal name
      2. Merge segments into a single LineString with linemerge()
      3. Try both endpoints (start and end of merged line)
      4. The endpoint closest to any river segment = headworks
      5. Snap that single point → one output row per canal

    Generic label-only names (Spillway, Nahar, Canal) are excluded.
    """
    print(f"  Processing {len(canals)} raw canal segments...")

    # Build name → list of geometries
    named_canals = {}
    unnamed_count = 0
    noise_count = 0

    for _, row in canals.iterrows():
        name = get_osm_name(row)

        if not name or not isinstance(name, str):
            unnamed_count += 1
            continue

        if name.strip().lower() in _CANAL_NOISE:
            noise_count += 1
            continue

        if row.geometry is None:
            continue

        named_canals.setdefault(name, []).append(row.geometry)

    print(f"  Unnamed dropped: {unnamed_count}, "
          f"generic label dropped: {noise_count}")
    print(f"  Unique named canals: {len(named_canals)}")

    results = []
    for name, geoms in named_canals.items():
        # Merge all way segments into one geometry
        if len(geoms) == 1:
            merged = geoms[0]
        else:
            try:
                merged = linemerge(geoms)
            except Exception:
                merged = geoms[0]

        # Extract all endpoint coordinates from the merged geometry
        endpoints = []
        if merged.geom_type == 'LineString':
            coords = list(merged.coords)
            endpoints = [Point(coords[0]), Point(coords[-1])]
        elif merged.geom_type == 'MultiLineString':
            for part in merged.geoms:
                coords = list(part.coords)
                endpoints += [Point(coords[0]), Point(coords[-1])]
        else:
            endpoints = [merged.centroid]

        # Find the endpoint closest to any river — that is the headworks
        best_pt        = None
        best_river     = None
        best_seg_id    = None
        best_dist      = float('inf')

        for pt in endpoints:
            ni   = tree.nearest(pt)
            nr   = rivers.iloc[ni]
            dist = pt.distance(nr.geometry)
            if dist < best_dist:
                best_dist    = dist
                best_pt      = pt
                if dist <= CANAL_SNAP_DEG:
                    best_river  = nr['river_name']
                    best_seg_id = nr['seg_id']

        if best_pt is None:
            continue

        results.append({
            'name':       name,
            'river_name': best_river,
            'seg_id':     best_seg_id,
            'dist_deg':   round(best_dist, 6) if best_dist < float('inf') else None,
            'latitude':   round(best_pt.y, 6),
            'longitude':  round(best_pt.x, 6),
        })

    df = pd.DataFrame(results)
    snapped = df['seg_id'].notna().sum() if len(df) else 0
    print(f"  Output: {len(df)} canals (one row each), "
          f"snapped: {snapped} ({snapped / max(len(df), 1) * 100:.1f}%)")
    return df


# =============================================================================
# FILTERING AND COLUMN SELECTION
# =============================================================================

def apply_mandatory_filter(df, label):
    """Drop rows where name, latitude, OR longitude is null."""
    before = len(df)
    df = df.dropna(subset=MANDATORY)
    dropped = before - len(df)
    if dropped:
        print(f"  Dropped {dropped} {label} rows missing name/lat/lon")
    return df


def select_columns(df):
    """Keep mandatory + dist_deg columns. Optional capacity columns if present."""
    base     = ['name', 'river_name', 'seg_id', 'latitude', 'longitude', 'dist_deg']
    optional = [c for c in ['capacity_m3', 'capacity_mcm']
                if c in df.columns and df[c].notna().any()]
    present  = [c for c in base + optional if c in df.columns]
    return df[present]


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("Ganga Basin Waterway Feature Extraction")
    print("=" * 60)

    # Load
    rivers    = load_rivers()
    basin     = load_ganga_basin(rivers)
    waterways = load_waterways()

    # Build STRtree once — shared by all three layers
    tree = STRtree(rivers.geometry.values)

    # Extract and clip
    dams, barrages, canals = extract_layers(waterways, basin)

    # Process each layer
    print("\nProcessing dams...")
    dams_df = snap_dams(dams, rivers, tree)

    print("\nProcessing barrages...")
    barrages_df = snap_barrages(barrages, rivers, tree)

    print("\nProcessing canals (headworks extraction)...")
    canals_df = extract_canal_headworks(canals, rivers, tree)

    # Mandatory filter and column selection
    print("\nApplying mandatory column filter...")
    dams_df     = apply_mandatory_filter(dams_df,     'dam')
    barrages_df = apply_mandatory_filter(barrages_df, 'barrage')
    canals_df   = apply_mandatory_filter(canals_df,   'canal')

    dams_df     = select_columns(dams_df)
    barrages_df = select_columns(barrages_df)
    canals_df   = select_columns(canals_df)

    # Save
    print("\n" + "=" * 60)
    print("Saving output files...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    dams_df.to_csv(OUTPUT_DIR     / "ganga_dams.csv",     index=False)
    barrages_df.to_csv(OUTPUT_DIR / "ganga_barrages.csv", index=False)
    canals_df.to_csv(OUTPUT_DIR   / "ganga_canals.csv",   index=False)

    # Summary
    print("\n" + "=" * 60)
    print("Summary:")
    for label, df in [("Dams",     dams_df),
                      ("Barrages", barrages_df),
                      ("Canals",   canals_df)]:
        snapped = df['seg_id'].notna().sum() if 'seg_id' in df.columns else 0
        print(f"\n  {label}: {len(df)} records")
        print(f"    Named:            {df['name'].notna().sum()}")
        print(f"    Snapped to river: {snapped}")

    print("\n  Notes:")
    print(f"    Dams:     centroid snap, threshold {DAM_SNAP_DEG}° (~{DAM_SNAP_DEG*111:.1f} km)")
    print(f"    Barrages: centroid snap, threshold {BARRAGE_SNAP_DEG}° "
          f"(~{BARRAGE_SNAP_DEG*111:.1f} km) — includes waterway=weir")
    print(f"    Canals:   headworks (offtake point), threshold {CANAL_SNAP_DEG}° "
          f"(~{CANAL_SNAP_DEG*111:.1f} km)")
    print(f"    seg_id=null → feature beyond snap threshold, row kept")
    print(f"    dist_deg   → actual distance to nearest river segment")
    print(f"    Run enrich_dam_capacity.py to add capacity from JSON")
    print("=" * 60)


if __name__ == "__main__":
    main()
