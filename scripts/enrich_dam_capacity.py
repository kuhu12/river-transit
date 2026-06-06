#!/usr/bin/env python3
"""
enrich_dam_capacity.py
======================
Uses ganga_dams_detail.json as the PRIMARY source for the dams dataset.

Rationale:
  - JSON has 781 curated, surveyed dam records vs 399 OSM crowd-sourced features
  - JSON has precise coordinates, capacity, dam length, and reservoir area for
    nearly every record; OSM has none of these attributes
  - Only 41 of 399 OSM dams have a JSON counterpart within 2 km — the two
    datasets are largely disjoint; enriching the OSM CSV wastes 95% of the JSON
  - JSON is authoritative; OSM picked up village ponds/tanks not in scope

Processing:
  1. Load all 781 JSON dam records
  2. Parse local river name from Address field
  3. Snap each dam's coordinates to ganga_rivers_named.shp (STRtree, 0.02°)
  4. Cross-reference OSM CSV to add osm_name where a nearby OSM entry exists
  5. Output ganga_dams.csv — replaces the OSM-derived file

Output columns:
  name                   — dam name (from JSON)
  state                  — Indian state
  address                — full address from JSON
  local_river            — river name parsed from Address field
  river_name             — snapped river name from shapefile
  seg_id                 — nearest shapefile segment ID
  dist_deg               — snap distance in degrees
  latitude               — surveyed latitude (JSON)
  longitude              — surveyed longitude (JSON)
  half_m                 — Full Reservoir Level elevation (metres ASL)
  dam_length_m           — dam crest length in metres
  capacity_m3            — gross storage capacity in cubic metres
  capacity_mcm           — gross storage capacity in million cubic metres
  reservoir_area_m2      — reservoir surface area in square metres
  effective_storage_m3   — effective (live) storage capacity in cubic metres
  osm_name               — nearest OSM dam name within 1 km (if any)
"""

import json
import re
import geopandas as gpd
import pandas as pd
from pathlib import Path
from shapely import STRtree
from shapely.geometry import Point
from math import radians, cos, sin, asin, sqrt

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR    = Path("/Users/kuhu.gupta/VizChitra")
RIVERS_SHP  = BASE_DIR / "Shapefiles/Ganga Basin Streams/NamedStreams/ganga_rivers.shp"
DAMS_JSON   = BASE_DIR / "Processed Data/Dams/ganga_dams_detail.json"
OSM_DAMS_CSV = BASE_DIR / "Processed Data/ganga_dams.csv"   # for osm_name cross-ref
OUTPUT_CSV  = BASE_DIR / "Processed Data/ganga_dams_enriched.csv"    # overwrites with enriched

SNAP_DEG    = 0.02    # ~2.2 km at 25°N
OSM_XREF_DEG = 0.009  # ~1 km — for OSM name cross-reference only

# ── Address → local river parser ──────────────────────────────────────────────
# JSON Address format: "RiverName. River, District, State"
# e.g. "Mani River, Munger, Bihar" or "Kohira. River, Bhabua, Bihar"
_ADDR_RE = re.compile(
    r'^([A-Za-z\s\-]+?)(?:\.\s*River|\s+River)\s*,', re.IGNORECASE)


def parse_local_river(address):
    """Extract river name from JSON Address field."""
    if not address or not isinstance(address, str):
        return None
    m = _ADDR_RE.match(address.strip())
    return m.group(1).strip() if m else None


# =============================================================================
# LOAD
# =============================================================================

def load_json(path):
    print(f"Loading {path.name}...")
    with open(path) as f:
        content = re.sub(r'require\([^)]+\)', '""', f.read())
    records = json.loads(content)
    # Exclude any records missing coordinates (none expected, but guard anyway)
    records = [r for r in records if r.get('Latitude') and r.get('Longitude')]
    print(f"  {len(records)} dam records loaded")
    return records


def load_rivers(path):
    print(f"Loading {path.name}...")
    rivers = gpd.read_file(path).to_crs('EPSG:4326')
    print(f"  {len(rivers)} river segments loaded")
    return rivers


def load_osm_csv(path):
    """Load OSM dams CSV for name cross-referencing."""
    if not path.exists():
        print(f"  OSM CSV not found — skipping cross-reference")
        return None
    osm = pd.read_csv(path)
    print(f"  OSM CSV: {len(osm)} records for name cross-reference")
    return osm


# =============================================================================
# HELPERS
# =============================================================================

def haversine_m(p1, p2):
    R = 6371000
    lat1, lon1 = radians(p1[1]), radians(p1[0])
    lat2, lon2 = radians(p2[1]), radians(p2[0])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))


# =============================================================================
# SNAP
# =============================================================================

def snap_all(records, rivers):
    """
    Snap each JSON dam to the nearest river segment using STRtree.
    Returns river_name, seg_id, dist_deg for each record.
    """
    print(f"\nSnapping {len(records)} dams to river network "
          f"(threshold {SNAP_DEG}° ≈ {SNAP_DEG*111:.1f} km)...")

    tree   = STRtree(rivers.geometry.values)
    snapped_river  = []
    snapped_seg    = []
    snapped_dist   = []

    for rec in records:
        pt = Point(rec['Longitude'], rec['Latitude'])
        ni = tree.nearest(pt)
        nr = rivers.iloc[ni]
        dist = pt.distance(nr.geometry)

        if dist <= SNAP_DEG:
            snapped_river.append(nr['river_name'])
            snapped_seg.append(nr['seg_id'])
            snapped_dist.append(round(dist, 6))
        else:
            snapped_river.append(None)
            snapped_seg.append(None)
            snapped_dist.append(None)

    n_snapped = sum(1 for s in snapped_seg if s is not None)
    print(f"  Snapped: {n_snapped}/{len(records)} "
          f"({n_snapped/len(records)*100:.1f}%)")
    return snapped_river, snapped_seg, snapped_dist


# =============================================================================
# OSM NAME CROSS-REFERENCE
# =============================================================================

def crossref_osm_names(records, osm_df):
    """
    For each JSON dam, find the nearest OSM dam within OSM_XREF_DEG.
    Returns a list of OSM names (or None) aligned to records.
    """
    if osm_df is None or len(osm_df) == 0:
        return [None] * len(records)

    print(f"\nCross-referencing OSM names (threshold {OSM_XREF_DEG}° ≈ "
          f"{OSM_XREF_DEG*111*1000:.0f} m)...")

    osm_pts = list(zip(osm_df['longitude'], osm_df['latitude']))
    osm_names = list(osm_df['name'])
    osm_name_col = []

    for rec in records:
        jp = (rec['Longitude'], rec['Latitude'])
        best_d, best_name = float('inf'), None
        for (olon, olat), oname in zip(osm_pts, osm_names):
            d = haversine_m(jp, (olon, olat))
            if d < best_d:
                best_d = d
                best_name = oname
        # Only assign if within 1 km and name differs from JSON name
        if best_d <= OSM_XREF_DEG * 111_000 and best_name:
            if best_name.lower().strip() != rec['Name'].lower().strip():
                osm_name_col.append(best_name)
            else:
                osm_name_col.append(None)  # same name — no value in duplicating
        else:
            osm_name_col.append(None)

    matched = sum(1 for n in osm_name_col if n)
    print(f"  OSM name found for {matched}/{len(records)} dams")
    return osm_name_col


# =============================================================================
# BUILD OUTPUT
# =============================================================================

def build_dataframe(records, river_names, seg_ids, dist_degs, osm_names):
    rows = []
    for rec, rname, sid, dist, oname in zip(
            records, river_names, seg_ids, dist_degs, osm_names):

        cap_m3  = rec.get('GrossStorageCapacity')
        eff_m3  = rec.get('EffectiveStorageCapacity')
        res_m2  = rec.get('ReservoirArea')
        half    = rec.get('HaLF')
        dam_len = rec.get('DamLength')

        rows.append({
            'name':                 rec['Name'].strip(),
            'state':                rec.get('State', '').strip() or None,
            'address':              rec.get('Address', '').strip() or None,
            'local_river':          parse_local_river(rec.get('Address', '')),
            'river_name':           rname,
            'seg_id':               int(sid) if sid is not None else None,
            'dist_deg':             dist,
            'latitude':             rec['Latitude'],
            'longitude':            rec['Longitude'],
            'half_m':               float(half) if half else None,
            'dam_length_m':         float(dam_len) if dam_len else None,
            'capacity_m3':          int(cap_m3) if cap_m3 else None,
            'capacity_mcm':         round(cap_m3 / 1_000_000, 3) if cap_m3 else None,
            'reservoir_area_m2':    int(res_m2) if res_m2 else None,
            'effective_storage_m3': int(eff_m3) if eff_m3 else None,
            'osm_name':             oname,
        })

    return pd.DataFrame(rows)


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("Ganga Basin Dam Enrichment")
    print("(JSON as primary source → snap to shapefile)")
    print("=" * 60)

    # Load
    records = load_json(DAMS_JSON)
    rivers  = load_rivers(RIVERS_SHP)
    osm_df  = load_osm_csv(OSM_DAMS_CSV)

    # Snap JSON coordinates to river network
    river_names, seg_ids, dist_degs = snap_all(records, rivers)

    # OSM name cross-reference
    osm_names = crossref_osm_names(records, osm_df)

    # Build output dataframe
    print("\nBuilding output dataframe...")
    df = build_dataframe(records, river_names, seg_ids, dist_degs, osm_names)

    # Summary stats
    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Total dams:           {len(df)}")
    print(f"  Snapped to river:     {df['seg_id'].notna().sum()} "
          f"({df['seg_id'].notna().sum()/len(df)*100:.1f}%)")
    print(f"  With capacity:        {df['capacity_m3'].notna().sum()}")
    print(f"  With local river:     {df['local_river'].notna().sum()}")
    print(f"  With OSM name xref:   {df['osm_name'].notna().sum()}")
    print(f"\n  Top rivers snapped to:")
    print(df['river_name'].value_counts().head(8).to_string())

    print(f"\n  Capacity range: "
          f"{df['capacity_mcm'].min():.2f} – "
          f"{df['capacity_mcm'].max():.0f} MCM")

    # Save
    print("\n" + "=" * 60)
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(df)} records → {OUTPUT_CSV}")
    print("\nColumn notes:")
    print("  half_m               Full Reservoir Level elevation (metres ASL)")
    print("  dam_length_m         Dam crest length (metres)")
    print("  capacity_m3          Gross storage (cubic metres)")
    print("  capacity_mcm         Gross storage (million cubic metres)")
    print("  reservoir_area_m2    Reservoir surface area (square metres)")
    print("  effective_storage_m3 Live/usable storage (cubic metres)")
    print("  dist_deg             Snap distance; None = beyond 0.02° threshold")
    print("  osm_name             Nearest OSM dam name within 1 km (if different)")
    print("=" * 60)


if __name__ == "__main__":
    main()
