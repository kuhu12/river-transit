#!/usr/bin/env python3
"""
Assign river names from OSM waterways to mizuRoute stream segments.

This script:
1. Loads OSM waterways and filters to named rivers only
2. Spatially joins them to mizuRoute segments to assign river names
3. Identifies the outlet segment for each river (lowest BotElev)
4. Outputs a lookup CSV: river_name -> outlet_seg_id
"""

import geopandas as gpd
import pandas as pd
import re
from pathlib import Path
from difflib import SequenceMatcher

# Common river name suffixes in Indian languages (to be stripped for normalization)
RIVER_SUFFIXES = [
    r'\s+river$', r'\s+nadi$', r'\s+nadī$', r'\s+nala$', r'\s+nalla$',
    r'\s+khad$', r'\s+gad$', r'\s+ganga$', r'\s+stream$', r'\s+creek$',
    r'\s+khal$', r'\s+khola$', r'\s+chu$', r'\s+chhu$', r'\s+tsangpo$',
]


def normalize_river_name(name: str, aggressive: bool = False) -> str:
    """
    Normalize a river name for matching purposes.

    - Converts to title case
    - Removes common suffixes (River, Nadi, etc.)
    - Strips extra whitespace
    - Handles common transliteration variations

    Args:
        name: River name to normalize
        aggressive: If True, apply more aggressive phonetic normalization
    """
    if pd.isna(name):
        return None

    # Convert to lowercase for processing
    normalized = str(name).strip().lower()

    # Remove common suffixes
    for suffix in RIVER_SUFFIXES:
        normalized = re.sub(suffix, '', normalized, flags=re.IGNORECASE)

    # Common transliteration normalizations
    replacements = {
        'aa': 'a',      # Ganga vs Gangaa
        'ee': 'i',      # Yamuna vs Yamunee
        'oo': 'u',
        'ii': 'i',
        'uu': 'u',
    }

    for old, new in replacements.items():
        normalized = normalized.replace(old, new)

    if aggressive:
        # More aggressive phonetic normalizations for fuzzy matching
        # Be conservative - only apply changes that are commonly interchanged
        phonetic_replacements = {
            'chh': 'ch',
            'v': 'b',       # v/b interchange common in Hindi transliteration
        }
        for old, new in phonetic_replacements.items():
            normalized = normalized.replace(old, new)

    # Remove extra whitespace and convert to title case
    normalized = ' '.join(normalized.split())
    normalized = normalized.title()

    return normalized


def string_similarity(s1: str, s2: str) -> float:
    """Calculate similarity ratio between two strings."""
    if pd.isna(s1) or pd.isna(s2):
        return 0.0
    return SequenceMatcher(None, s1.lower(), s2.lower()).ratio()


def build_name_clusters(names: pd.Series, similarity_threshold: float = 0.90) -> dict:
    """
    Build clusters of similar river names.

    Returns a dict mapping each original name to its canonical (most common) form.
    """
    unique_names = names.dropna().unique()
    name_counts = names.value_counts()

    # Normalize all names (standard normalization for grouping)
    normalized_map = {name: normalize_river_name(name) for name in unique_names}

    # Also create aggressive normalization for fuzzy matching
    aggressive_map = {name: normalize_river_name(name, aggressive=True) for name in unique_names}

    # Group by normalized name first
    norm_groups = {}
    for orig, norm in normalized_map.items():
        if norm not in norm_groups:
            norm_groups[norm] = []
        norm_groups[norm].append(orig)

    # For each group, pick the most common original name as canonical
    name_to_canonical = {}
    for norm, originals in norm_groups.items():
        if len(originals) == 1:
            name_to_canonical[originals[0]] = originals[0]
        else:
            # Pick the most frequent name as canonical
            canonical = max(originals, key=lambda x: name_counts.get(x, 0))
            for orig in originals:
                name_to_canonical[orig] = canonical

    # Second pass: group by aggressive normalization
    agg_groups = {}
    for orig, agg_norm in aggressive_map.items():
        if agg_norm not in agg_groups:
            agg_groups[agg_norm] = []
        agg_groups[agg_norm].append(orig)

    # Merge groups from aggressive normalization
    for agg_norm, originals in agg_groups.items():
        if len(originals) > 1:
            # Find the canonical name for this group (most frequent)
            canonical = max(originals, key=lambda x: name_counts.get(x, 0))
            for orig in originals:
                if name_to_canonical.get(orig) == orig:  # Only update if not already merged
                    name_to_canonical[orig] = canonical

    # Final pass: fuzzy matching for remaining unmatched names
    canonicals = list(set(name_to_canonical.values()))

    for name in unique_names:
        current_canonical = name_to_canonical.get(name, name)
        if current_canonical != name:
            continue  # Already merged

        # Find best matching canonical name using aggressive normalization
        best_match = None
        best_score = 0
        for canonical in canonicals:
            if canonical == name:
                continue
            score = string_similarity(
                aggressive_map[name],
                aggressive_map.get(canonical, normalize_river_name(canonical, aggressive=True))
            )
            if score > best_score and score >= similarity_threshold:
                best_score = score
                best_match = canonical

        if best_match:
            name_to_canonical[name] = best_match

    return name_to_canonical


def apply_name_normalization(gdf: gpd.GeoDataFrame, name_col: str = 'river_name') -> gpd.GeoDataFrame:
    """
    Apply name normalization to merge similar river names.

    Creates a new column 'river_name_normalized' with canonical names.
    """
    print("Normalizing river names...")

    # Build name clusters
    name_mapping = build_name_clusters(gdf[name_col])

    # Count merges
    original_count = gdf[name_col].nunique()

    # Apply mapping
    gdf = gdf.copy()
    gdf['river_name_original'] = gdf[name_col]
    gdf[name_col] = gdf[name_col].map(name_mapping)

    new_count = gdf[name_col].nunique()
    merged_count = original_count - new_count

    print(f"  Original unique names: {original_count}")
    print(f"  After normalization: {new_count}")
    print(f"  Names merged: {merged_count}")

    # Show some examples of merged names
    if merged_count > 0:
        merged_examples = []
        for orig, canon in name_mapping.items():
            if orig != canon:
                merged_examples.append((orig, canon))

        if merged_examples:
            print("  Sample merges:")
            for orig, canon in sorted(merged_examples)[:10]:
                print(f"    '{orig}' -> '{canon}'")

    return gdf


def load_mizuroute_streams(shapefile_path: str) -> gpd.GeoDataFrame:
    """Load mizuRoute stream segments."""
    print(f"Loading mizuRoute streams from {shapefile_path}...")
    gdf = gpd.read_file(shapefile_path)
    print(f"  Loaded {len(gdf)} stream segments")
    print(f"  CRS: {gdf.crs}")
    return gdf


def load_osm_waterways(gpkg_path: str, exclude_patterns: list = None) -> gpd.GeoDataFrame:
    """Load and filter OSM waterways to named rivers only."""
    print(f"Loading OSM waterways from {gpkg_path}...")
    gdf = gpd.read_file(gpkg_path)
    print(f"  Loaded {len(gdf)} total features")

    # Filter to rivers only (waterway == 'river')
    rivers = gdf[gdf['waterway'] == 'river'].copy()
    print(f"  After filtering to 'river' type: {len(rivers)} features")

    # Filter to named rivers only (name is not null)
    named_rivers = rivers[rivers['name'].notna()].copy()
    print(f"  After filtering to named rivers: {len(named_rivers)} features")

    # Use name_en if available, otherwise fall back to name
    named_rivers['river_name'] = named_rivers['name_en'].fillna(named_rivers['name'])

    # Exclude rivers matching certain patterns
    if exclude_patterns:
        exclude_mask = named_rivers['river_name'].str.contains(
            '|'.join(exclude_patterns), case=False, na=False
        )
        excluded_count = exclude_mask.sum()
        named_rivers = named_rivers[~exclude_mask].copy()
        print(f"  After excluding patterns {exclude_patterns}: {len(named_rivers)} features ({excluded_count} removed)")

    return named_rivers


def nearest_neighbor_join(
    segments: gpd.GeoDataFrame,
    rivers: gpd.GeoDataFrame,
    max_distance_m: float = 500
) -> gpd.GeoDataFrame:
    """
    Assign river names to segments using nearest-neighbor join.

    For each mizuRoute segment, finds the nearest OSM river line within max_distance_m
    and assigns that river's name.

    Args:
        segments: mizuRoute stream segments
        rivers: OSM river lines with names
        max_distance_m: Maximum distance in meters to search for nearest river

    Returns:
        segments GeoDataFrame with river_name column added
    """
    print(f"Performing nearest-neighbor join (max distance: {max_distance_m}m)...")

    # Use a projected CRS for distance calculations (UTM zone 44N covers most of Ganga basin)
    projected_crs = "EPSG:32644"
    segments_proj = segments.to_crs(projected_crs)
    rivers_proj = rivers.to_crs(projected_crs)

    # Use segment centroids for nearest neighbor matching
    segments_proj['centroid'] = segments_proj.geometry.centroid

    # Build a spatial index for rivers
    print("  Building spatial index for rivers...")

    # Perform nearest neighbor join using sjoin_nearest
    segments_for_join = segments_proj.copy()
    segments_for_join = segments_for_join.set_geometry('centroid')

    joined = gpd.sjoin_nearest(
        segments_for_join[['seg_id', 'BotElev', 'centroid']],
        rivers_proj[['river_name', 'geometry']],
        how='left',
        max_distance=max_distance_m,
        distance_col='distance'
    )

    print(f"  Join result shape: {joined.shape}")
    print(f"  Segments with matches: {joined['river_name'].notna().sum()}")

    # Handle segments that matched multiple rivers - take the nearest one
    # sjoin_nearest can return multiple matches if equidistant
    joined_dedup = joined.sort_values('distance').drop_duplicates(subset='seg_id', keep='first')

    # Merge back with original segments
    result = segments.merge(
        joined_dedup[['seg_id', 'river_name', 'distance']],
        on='seg_id',
        how='left'
    )

    named_count = result['river_name'].notna().sum()
    print(f"  Segments with assigned names: {named_count} / {len(result)}")
    print(f"  Unique river names assigned: {result['river_name'].nunique()}")

    # Print distance statistics for matched segments
    matched = result[result['river_name'].notna()]
    if len(matched) > 0:
        print(f"  Distance stats (m): min={matched['distance'].min():.1f}, "
              f"median={matched['distance'].median():.1f}, max={matched['distance'].max():.1f}")

    return result


def identify_outlet_segments(segments_with_names: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Identify the outlet segment for each river.

    The outlet is the segment with the lowest bottom elevation (BotElev)
    for each river.
    """
    print("Identifying outlet segments...")

    # Filter to named segments only
    named = segments_with_names[segments_with_names['river_name'].notna()].copy()

    # Find the segment with minimum BotElev for each river
    idx = named.groupby('river_name')['BotElev'].idxmin()
    outlets = named.loc[idx, ['river_name', 'seg_id', 'BotElev']].copy()

    # Sort by river name
    outlets = outlets.sort_values('river_name').reset_index(drop=True)

    print(f"  Identified {len(outlets)} river outlets")

    return outlets


def main():
    # Define paths
    base_dir = Path(__file__).parent.parent

    streams_path = base_dir / "Shapefiles" / "Ganga Basin Streams" / "ganga_streams.shp"
    waterways_path = base_dir / "Raw Data" / "OSM waterways" / "waterways.gpkg"
    output_dir = base_dir / "Processed Data"

    # River names to exclude (outside Ganga basin or boundary artifacts)
    exclude_patterns = ['Bangladesh', 'Jamuna', 'Padma']

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Load data
    segments = load_mizuroute_streams(str(streams_path))
    rivers = load_osm_waterways(str(waterways_path), exclude_patterns=exclude_patterns)

    # Filter out segments where both TopElev and BotElev are 0 (invalid/boundary segments)
    invalid_mask = (segments['TopElev'] == 0) & (segments['BotElev'] == 0)
    invalid_count = invalid_mask.sum()
    segments = segments[~invalid_mask].copy()
    print(f"Filtered out {invalid_count} segments with TopElev=0 and BotElev=0")
    print(f"  Remaining segments: {len(segments)}")

    # Perform nearest-neighbor join with 500m buffer
    segments_with_names = nearest_neighbor_join(segments, rivers, max_distance_m=500)

    # Apply name normalization to merge similar names
    segments_with_names = apply_name_normalization(segments_with_names)

    # Identify outlets
    outlets = identify_outlet_segments(segments_with_names)

    # Save outputs
    # 1. Full mapping of all segments with river names
    full_output_path = output_dir / "ganga_segments_river_names.csv"
    output_cols = ['seg_id', 'river_name', 'BotElev', 'TopElev']
    if 'river_name_original' in segments_with_names.columns:
        output_cols.insert(2, 'river_name_original')
    segments_with_names[output_cols].to_csv(full_output_path, index=False)
    print(f"\nSaved full segment-name mapping to: {full_output_path}")

    # 2. River name to outlet seg_id lookup
    lookup_output_path = output_dir / "river_outlet_lookup.csv"
    outlets[['river_name', 'seg_id']].to_csv(lookup_output_path, index=False)
    print(f"Saved river-outlet lookup to: {lookup_output_path}")

    # Print summary
    print("\n=== Summary ===")
    print(f"Total stream segments: {len(segments)}")
    print(f"Segments with river names: {segments_with_names['river_name'].notna().sum()}")
    print(f"Unique rivers identified: {outlets['river_name'].nunique()}")
    print("\nTop 10 rivers by outlet elevation (lowest first):")
    print(outlets.nsmallest(10, 'BotElev')[['river_name', 'seg_id', 'BotElev']].to_string(index=False))

    return segments_with_names, outlets


if __name__ == "__main__":
    segments_with_names, outlets = main()
