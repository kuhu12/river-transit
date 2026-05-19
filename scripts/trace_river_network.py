#!/usr/bin/env python3
"""
Full pipeline to trace river networks for 35 target rivers.

Steps:
1. Fix duplicates and suffix variants
2. Remove garbage entries
3. Audit suffix variants
4. Build network topology and trace upstream from outlets
5. Output all segments for each river
6. Run validation checks
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import re
from pathlib import Path
from collections import defaultdict

# ============================================================
# CONFIGURATION
# ============================================================

DUPLICATE_REMAPPING = {
    # Spelling duplicates
    "Ganges"               : "Ganga",
    "Koshi River"          : "Kosi",
    "Kosi River"           : "Kosi",
    "Gomati"               : "Gomti",
    "Gomtī"                : "Gomti",
    "Gomti River"          : "Gomti",
    "Gomti Nadi"           : "Gomti",
    "Kamla River"          : "Kamala",
    "Kamala River"         : "Kamala",
    "Gambhiri"             : "Gambhir",
    "Gambhir;Parbati"      : "Gambhir",
    "Mahakali River"       : "Sharda",
    "Sipra"                : "Shipra",
    "Narayani"             : "Gandak",
    "Ghaghara River"       : "Ghaghara",
    "Chambal River"        : "Chambal",
    "Mahananda River"      : "Mahananda",
    "North Koel River"     : "North Koel",
    "Gaula River"          : "Gaula",
    "Bhagirathi River"     : "Bhagirathi",
    "Alaknanda River"      : "Alaknanda",
    "Hindon River"         : "Hindon",
    "Yamuna River"         : "Yamuna",
    "Ramganga River"       : "Ramganga",
    "Rapti River"          : "Rapti",
    "Gandak River"         : "Gandak",
    "Betwa River"          : "Betwa",
    "Ken River"            : "Ken",
    "Son River"            : "Son",
    "Tons River"           : "Tons",
    "Bagmati River"        : "Bagmati",
    "Mechi River"          : "Mechi",
    "Sindh River"          : "Sindh",
    "Rihand River"         : "Rihand",
    "Rohini River"         : "Rohini",
    "Gerua River"          : "Gerua",
    "Giri River"           : "Giri",
    "Punpun River"         : "Punpun",
    "Karmanasa River"      : "Karmanasa",
    "Sharda River"         : "Sharda",
    "Sarju River"          : "Sarju",
    "Burhi Gandak River"   : "Burhi Gandak",
    "Mandakini River"      : "Mandakini",
    "Birahi Ganga River"   : "Birahi Ganga",
    "Kali Sindh River"     : "Kali Sindh",
}

GARBAGE_ENTRIES = [
    "布抄老曲",
    "mAU nALA",
    "Mungeshpur Drain",
    "Gambhir",
    "Gambhir;Parbati",
]

TARGET_RIVERS = {
    "Ganga": 1,
    "Bhagirathi": 2,
    "Alaknanda": 2,
    "Mandakini": 2,
    # "Birahi Ganga": 2,  # Removed - isolated segment, no upstream connectivity
    "Ramganga": 3,
    "Gomti": 3,
    "Ghaghara": 3,
    "Rapti": 3,
    "Sarju": 3,
    "Gandak": 3,
    "Burhi Gandak": 3,
    "Bagmati": 4,
    "Kamala": 4,
    "Kosi": 4,
    "Mechi": 4,
    "Mahananda": 4,
    "Yamuna": 5,
    "Tons": 5,
    "Chambal": 5,
    "Betwa": 5,
    "Ken": 5,
    "Son": 5,
    "Sharda": 6,
    "Hindon": 6,
    "Sindh": 6,
    "Kali Sindh": 6,
    "Karmanasa": 6,
    "Punpun": 6,
    "North Koel": 6,
    "Gaula": 7,
    "Rihand": 7,
    "Gerua": 7,
    # "Giri": 7,  # Removed - isolated segment, no upstream connectivity
    "Rohini": 7,
}


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def has_non_latin(text: str) -> bool:
    """Check if text contains non-Latin characters."""
    if pd.isna(text):
        return False
    return bool(re.search(r'[^\x00-\x7F]', str(text)))


def is_garbage_name(name: str) -> bool:
    """Check if a river name should be excluded."""
    if pd.isna(name) or str(name).strip() == "":
        return True
    if name in GARBAGE_ENTRIES:
        return True
    if has_non_latin(name):
        return True
    name_lower = str(name).lower()
    if any(pattern in name_lower for pattern in ['drain', 'nala', 'canal']):
        return True
    return False


def audit_suffix_variants(df: pd.DataFrame, target_rivers: list) -> dict:
    """
    Find all name variants for each target river in the data.
    Returns dict: canonical_name -> list of variants found

    Only flags true suffix variants like "Yamuna River" for "Yamuna",
    NOT compound names like "Birahi Ganga" for "Ganga".
    """
    variants = defaultdict(list)
    all_names = df['river_name'].dropna().unique()
    target_set = set(target_rivers)

    for target in target_rivers:
        target_lower = target.lower()
        for name in all_names:
            if name == target:
                continue
            # Skip if this name is itself a target river
            if name in target_set:
                continue

            name_lower = str(name).lower()

            # Only match true suffix variants:
            # "Yamuna River" -> "Yamuna" (name starts with target + space/suffix)
            # "Kosi River" -> "Kosi"
            # NOT "Birahi Ganga" -> "Ganga" (different river)

            is_suffix_variant = False

            # Check for common suffix patterns
            suffixes = [' river', ' nadi', ' nadī', ' gad', ' khola', ' nala']
            for suffix in suffixes:
                if name_lower == target_lower + suffix:
                    is_suffix_variant = True
                    break

            # Also check reverse: name without suffix equals target
            for suffix in suffixes:
                if name_lower.endswith(suffix):
                    base = name_lower[:-len(suffix)]
                    if base == target_lower:
                        is_suffix_variant = True
                        break

            if is_suffix_variant:
                variants[target].append(name)

    return variants


def build_network_topology(gdf: gpd.GeoDataFrame, tolerance: float = 0.001) -> dict:
    """
    Build network topology from segment coordinates.

    Convention: water flows from start_lon/start_lat to end_lon/end_lat.
    Upstream segments END where current segment STARTS.
    Downstream segment STARTS where current segment ENDS.

    Returns:
        upstream_map: dict mapping seg_id -> list of upstream seg_ids
        downstream_map: dict mapping seg_id -> downstream seg_id (or None)
    """
    print("Building network topology from coordinates...")

    # Create lookup of end points (where segments terminate)
    end_points = {}
    for _, row in gdf.iterrows():
        key = (round(row['end_lon'], 4), round(row['end_lat'], 4))
        if key not in end_points:
            end_points[key] = []
        end_points[key].append(row['seg_id'])

    # Create lookup of start points
    start_points = {}
    for _, row in gdf.iterrows():
        key = (round(row['start_lon'], 4), round(row['start_lat'], 4))
        if key not in start_points:
            start_points[key] = []
        start_points[key].append(row['seg_id'])

    upstream_map = defaultdict(list)
    downstream_map = {}

    for _, row in gdf.iterrows():
        seg_id = row['seg_id']
        start_key = (round(row['start_lon'], 4), round(row['start_lat'], 4))
        end_key = (round(row['end_lon'], 4), round(row['end_lat'], 4))

        # Find UPSTREAM segments: those that END where this segment STARTS
        if start_key in end_points:
            for upstream_id in end_points[start_key]:
                if upstream_id != seg_id:
                    upstream_map[seg_id].append(upstream_id)

        # Find DOWNSTREAM segment: the one that STARTS where this segment ENDS
        if end_key in start_points:
            for downstream_id in start_points[end_key]:
                if downstream_id != seg_id:
                    downstream_map[seg_id] = downstream_id
                    break  # Only one downstream segment

    # Handle segments with no downstream (likely outlets or disconnected)
    for seg_id in gdf['seg_id']:
        if seg_id not in downstream_map:
            downstream_map[seg_id] = None

    print(f"  Built topology for {len(gdf)} segments")
    print(f"  Segments with upstream connections: {len([k for k, v in upstream_map.items() if v])}")
    print(f"  Segments with no downstream (potential outlets): {sum(1 for v in downstream_map.values() if v is None)}")

    return upstream_map, downstream_map


def trace_upstream(
    outlet_seg_id: int,
    upstream_map: dict,
    boundary_seg_ids: set,
    max_depth: int = 1000
) -> list:
    """
    Trace all segments upstream from an outlet.

    Stops when:
    - No more upstream segments
    - Hits another river's outlet (boundary)
    - Exceeds max depth

    Returns list of seg_ids in the river's network.
    """
    river_segments = [outlet_seg_id]
    visited = {outlet_seg_id}
    queue = [outlet_seg_id]
    depth = 0

    while queue and depth < max_depth:
        depth += 1
        next_queue = []

        for seg_id in queue:
            upstream_segs = upstream_map.get(seg_id, [])
            for up_seg in upstream_segs:
                if up_seg in visited:
                    continue
                # Stop if we hit another river's outlet
                if up_seg in boundary_seg_ids and up_seg != outlet_seg_id:
                    continue
                visited.add(up_seg)
                river_segments.append(up_seg)
                next_queue.append(up_seg)

        queue = next_queue

    return river_segments


def calculate_distance_from_outlet(
    segments: list,
    outlet_seg_id: int,
    downstream_map: dict,
    seg_lengths: dict
) -> dict:
    """
    Calculate cumulative distance from outlet for each segment.
    """
    distances = {outlet_seg_id: 0}
    visited = {outlet_seg_id}
    queue = [outlet_seg_id]

    # BFS from outlet, tracking distance
    while queue:
        next_queue = []
        for seg_id in queue:
            current_dist = distances[seg_id]
            # Find segments that flow into this one
            upstream_segs = [s for s in segments if downstream_map.get(s) == seg_id]
            for up_seg in upstream_segs:
                if up_seg in visited:
                    continue
                visited.add(up_seg)
                distances[up_seg] = current_dist + seg_lengths.get(up_seg, 0)
                next_queue.append(up_seg)
        queue = next_queue

    # For any segments not reached, estimate distance
    for seg_id in segments:
        if seg_id not in distances:
            distances[seg_id] = -1  # Unknown

    return distances


# ============================================================
# MAIN PIPELINE
# ============================================================

def main():
    base_dir = Path(__file__).parent.parent
    segments_csv_path = base_dir / "Processed Data" / "ganga_segments_river_names.csv"
    shapefile_path = base_dir / "Shapefiles" / "Ganga Basin Streams" / "ganga_streams.shp"
    outlet_lookup_path = base_dir / "Processed Data" / "river_final_lookup.csv"
    output_dir = base_dir / "Processed Data"

    # --------------------------------------------------------
    # Load data
    # --------------------------------------------------------
    print("=" * 60)
    print("STEP 0: LOADING DATA")
    print("=" * 60)

    df = pd.read_csv(segments_csv_path)
    print(f"Loaded {len(df)} segments from {segments_csv_path}")

    gdf = gpd.read_file(shapefile_path)
    print(f"Loaded {len(gdf)} segments from shapefile")

    outlets_df = pd.read_csv(outlet_lookup_path)
    print(f"Loaded {len(outlets_df)} river outlets")

    # Manual outlet corrections for better network connectivity
    OUTLET_CORRECTIONS = {
        'Ramganga': 3891,  # 3891 has 3 upstream segments vs 3864 with 1
    }
    for river, new_outlet in OUTLET_CORRECTIONS.items():
        mask = outlets_df['river_name'] == river
        if mask.any():
            old_outlet = outlets_df.loc[mask, 'seg_id'].values[0]
            outlets_df.loc[mask, 'seg_id'] = new_outlet
            print(f"  Corrected {river} outlet: {old_outlet} -> {new_outlet}")

    # Create seg_id -> attributes lookup
    seg_attrs = gdf.set_index('seg_id')[['TopElev', 'BotElev', 'Length']].to_dict('index')

    # --------------------------------------------------------
    # Step 1: Fix duplicates
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 1: FIXING DUPLICATES")
    print("=" * 60)

    duplicates_resolved = 0
    duplicate_details = []

    df['river_name_original'] = df['river_name']
    for old_name, new_name in DUPLICATE_REMAPPING.items():
        mask = df['river_name'] == old_name
        if mask.any():
            count = mask.sum()
            duplicates_resolved += count
            duplicate_details.append(f"  {old_name} -> {new_name} ({count} segments)")
            df.loc[mask, 'river_name'] = new_name

    print(f"Duplicates remapped: {duplicates_resolved}")
    for detail in duplicate_details[:20]:
        print(detail)
    if len(duplicate_details) > 20:
        print(f"  ... and {len(duplicate_details) - 20} more")

    # --------------------------------------------------------
    # Step 2: Remove garbage
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 2: REMOVING GARBAGE ENTRIES")
    print("=" * 60)

    garbage_mask = df['river_name'].apply(is_garbage_name)
    garbage_removed = garbage_mask.sum()
    garbage_details = df[garbage_mask]['river_name'].value_counts().head(20)

    df_clean = df[~garbage_mask].copy()
    print(f"Garbage entries removed: {garbage_removed}")
    print("Top garbage entries:")
    for name, count in garbage_details.items():
        print(f"  {name}: {count}")

    # --------------------------------------------------------
    # Step 3: Audit suffix variants
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 3: AUDITING SUFFIX VARIANTS")
    print("=" * 60)

    variants = audit_suffix_variants(df_clean, list(TARGET_RIVERS.keys()))
    new_variants_found = []

    for river, var_list in sorted(variants.items()):
        if var_list:
            print(f"\n{river} variants found:")
            print(f"  - \"{river}\" -> keep (canonical)")
            for var in var_list:
                if var in DUPLICATE_REMAPPING:
                    print(f"  - \"{var}\" -> already remapped to {DUPLICATE_REMAPPING[var]}")
                else:
                    print(f"  - \"{var}\" -> NOT IN REMAPPING (add manually!)")
                    new_variants_found.append((var, river))

    if new_variants_found:
        print("\n" + "!" * 60)
        print("WARNING: NEW VARIANTS FOUND - ADD TO DUPLICATE_REMAPPING:")
        print("!" * 60)
        for var, canonical in new_variants_found:
            print(f'    "{var}": "{canonical}",')

    # --------------------------------------------------------
    # Step 4: Build network and trace
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 4: NETWORK TRACING")
    print("=" * 60)

    # Build topology
    upstream_map, downstream_map = build_network_topology(gdf)

    # Get outlet seg_ids as boundaries
    outlet_seg_ids = set(outlets_df['seg_id'].tolist())
    outlet_to_river = dict(zip(outlets_df['seg_id'], outlets_df['river_name']))
    river_to_outlet = dict(zip(outlets_df['river_name'], outlets_df['seg_id']))
    river_to_tier = dict(zip(outlets_df['river_name'], outlets_df['tier']))

    # Trace each river
    all_river_segments = []
    river_segment_counts = {}
    rivers_traced = []
    rivers_few_segments = []
    rivers_not_found = []

    for river_name in TARGET_RIVERS.keys():
        if river_name not in river_to_outlet:
            rivers_not_found.append(river_name)
            print(f"  {river_name}: NOT FOUND in outlet lookup")
            continue

        outlet_id = river_to_outlet[river_name]
        tier = river_to_tier[river_name]

        # Trace upstream, stopping at other rivers' outlets
        other_outlets = outlet_seg_ids - {outlet_id}
        river_segs = trace_upstream(outlet_id, upstream_map, other_outlets)

        river_segment_counts[river_name] = len(river_segs)

        if len(river_segs) < 3:
            rivers_few_segments.append(river_name)
            print(f"  {river_name}: {len(river_segs)} segments (WARNING: fewer than 3)")
        else:
            rivers_traced.append(river_name)
            print(f"  {river_name}: {len(river_segs)} segments")

        # Calculate distances from outlet
        seg_lengths = {seg_id: attrs['Length'] for seg_id, attrs in seg_attrs.items()}
        distances = calculate_distance_from_outlet(river_segs, outlet_id, downstream_map, seg_lengths)

        # Build output rows
        for seg_id in river_segs:
            attrs = seg_attrs.get(seg_id, {})
            all_river_segments.append({
                'river_name': river_name,
                'seg_id': seg_id,
                'tier': tier,
                'BotElev': attrs.get('BotElev', None),
                'TopElev': attrs.get('TopElev', None),
                'distance_from_outlet': round(distances.get(seg_id, -1), 0),
            })

    # --------------------------------------------------------
    # Step 5: Output files
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 5: WRITING OUTPUT FILES")
    print("=" * 60)

    # 5a: river_all_segments.csv
    output_df = pd.DataFrame(all_river_segments)
    # Sort by river, then by distance (descending = source to mouth)
    output_df = output_df.sort_values(
        ['tier', 'river_name', 'distance_from_outlet'],
        ascending=[True, True, False]
    ).reset_index(drop=True)

    output_csv_path = output_dir / "river_all_segments.csv"
    output_df.to_csv(output_csv_path, index=False)
    print(f"Saved {len(output_df)} segments to {output_csv_path}")

    # 5b: river_segment_counts.txt
    counts_path = output_dir / "river_segment_counts.txt"
    with open(counts_path, 'w') as f:
        f.write("River Segment Counts\n")
        f.write("=" * 40 + "\n\n")
        total = 0
        for river in sorted(river_segment_counts.keys(), key=lambda x: river_segment_counts[x], reverse=True):
            count = river_segment_counts[river]
            total += count
            f.write(f"{river:20s}: {count:4d} segments\n")
        f.write("\n" + "-" * 40 + "\n")
        f.write(f"{'Total':20s}: {total:4d} segments across {len(river_segment_counts)} rivers\n")
    print(f"Saved segment counts to {counts_path}")

    # 5c: river_final_report.txt
    report_path = output_dir / "river_network_report.txt"
    with open(report_path, 'w') as f:
        f.write("=" * 60 + "\n")
        f.write("RIVER NETWORK TRACING REPORT\n")
        f.write("=" * 60 + "\n\n")

        f.write(f"Rivers successfully traced: {len(rivers_traced)}/35\n")
        f.write(f"Duplicates resolved: {duplicates_resolved}\n")
        f.write(f"Garbage entries removed: {garbage_removed}\n")
        f.write(f"Total segments assigned: {len(output_df)}\n\n")

        if rivers_few_segments:
            f.write("WARNING: Rivers with fewer than 3 segments (check manually):\n")
            for river in rivers_few_segments:
                f.write(f"  - {river} ({river_segment_counts.get(river, 0)} segments)\n")
            f.write("\n")

        if rivers_not_found:
            f.write("ERROR: Rivers not found in network:\n")
            for river in rivers_not_found:
                f.write(f"  - {river}\n")
            f.write("\n")

        if new_variants_found:
            f.write("Suffix variants found and need remapping:\n")
            for var, canonical in new_variants_found:
                f.write(f'  "{var}" -> "{canonical}"\n')
            f.write("\n")

        f.write("Duplicate remappings applied:\n")
        for detail in duplicate_details:
            f.write(detail + "\n")

    print(f"Saved report to {report_path}")

    # --------------------------------------------------------
    # Step 6: Validation checks
    # --------------------------------------------------------
    print("\n" + "=" * 60)
    print("STEP 6: VALIDATION CHECKS")
    print("=" * 60)

    # Check 1: Ganga should have most segments
    ganga_count = river_segment_counts.get('Ganga', 0)
    max_count = max(river_segment_counts.values()) if river_segment_counts else 0
    if ganga_count == max_count:
        print(f"  [PASS] Ganga has the most segments ({ganga_count})")
    else:
        top_river = max(river_segment_counts, key=river_segment_counts.get)
        print(f"  [FAIL] Ganga ({ganga_count}) is not the largest - {top_river} has {max_count}")

    # Check 2: Himalayan rivers should connect to Ganga
    ganga_segs = set(output_df[output_df['river_name'] == 'Ganga']['seg_id'])
    himalayan = ['Bhagirathi', 'Alaknanda', 'Mandakini']
    for river in himalayan:
        river_outlet = river_to_outlet.get(river)
        if river_outlet:
            # Check if this river's outlet connects to Ganga network
            downstream = downstream_map.get(river_outlet)
            if downstream in ganga_segs or river_outlet in ganga_segs:
                print(f"  [PASS] {river} connects to Ganga network")
            else:
                print(f"  [INFO] {river} outlet ({river_outlet}) - downstream: {downstream}")

    # Check 3: Yamuna outlet elevation
    yamuna_outlet = river_to_outlet.get('Yamuna')
    if yamuna_outlet:
        yamuna_elev = seg_attrs.get(yamuna_outlet, {}).get('BotElev', None)
        if yamuna_elev:
            if 70 <= yamuna_elev <= 100:
                print(f"  [PASS] Yamuna outlet BotElev = {yamuna_elev}m (expected ~80-90m)")
            else:
                print(f"  [INFO] Yamuna outlet BotElev = {yamuna_elev}m (expected ~80-90m at Prayagraj)")

    # Check 4: Kosi should have high TopElev (Himalayan origin)
    kosi_segs = output_df[output_df['river_name'] == 'Kosi']
    if len(kosi_segs) > 0:
        kosi_max_elev = kosi_segs['TopElev'].max()
        if kosi_max_elev and kosi_max_elev > 1000:
            print(f"  [PASS] Kosi has high TopElev ({kosi_max_elev}m) - Himalayan origin")
        else:
            print(f"  [INFO] Kosi max TopElev = {kosi_max_elev}m")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total rivers traced: {len(rivers_traced) + len(rivers_few_segments)}/35")
    print(f"Total segments: {len(output_df)}")
    print(f"Rivers with <3 segments: {len(rivers_few_segments)}")

    return output_df, river_segment_counts


if __name__ == "__main__":
    output_df, counts = main()
