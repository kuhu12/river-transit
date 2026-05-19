# Shapefile Processing Summary

## Overview

This document summarizes the processing pipeline used to assign river names to mizuRoute stream segments in the Ganga Basin.

---

## Input Data

### mizuRoute Stream Segments
- **File:** `Shapefiles/Ganga Basin Streams/ganga_streams.shp`
- **Segments:** 1,991 stream segments
- **Attributes:** `seg_id`, `start_lon`, `end_lon`, `start_lat`, `end_lat`, `Length`, `TopElev`, `BotElev`
- **CRS:** EPSG:4326
- **Issue:** No river names assigned to segments

### OSM Waterways
- **File:** `Raw Data/OSM waterways/waterways.gpkg`
- **Features:** 597,773 total waterway features
- **Named rivers:** 11,304 features with `waterway='river'` and non-null names
- **CRS:** EPSG:4326

---

## Processing Approach

### Step 1: Spatial Join (OSM to mizuRoute)

Used nearest-neighbor spatial join to assign river names from OSM to mizuRoute segments:

1. Filtered OSM waterways to named rivers only (`waterway='river'` with non-null `name`)
2. Excluded rivers with names containing 'Bangladesh', 'Jamuna', or 'Padma' (outside basin / boundary artifacts)
3. Filtered out 7 mizuRoute segments with `TopElev=0` and `BotElev=0` (invalid boundary segments)
4. Performed nearest-neighbor join with 500m maximum distance
5. Used segment centroids for matching; assigned nearest OSM river name within threshold

**Result:** 493 segments received river names from OSM (25% coverage)

### Step 2: Name Normalization

Applied name normalization to merge duplicate/variant river names:

- Removed common suffixes (River, Nadi, Gad, Khola, etc.)
- Handled transliteration variants (aa→a, ee→i, v↔b interchange)
- Used 90% similarity threshold for fuzzy matching

### Step 3: Network Tracing

Built network topology from segment coordinates and traced upstream from each river outlet:

1. **Topology Construction:** Connected segments where one segment's `end_lon/end_lat` matches another's `start_lon/start_lat`
2. **Outlet Identification:** Used lowest-elevation named segment as outlet for each river
3. **Upstream Tracing:** From each outlet, recursively found all upstream segments
4. **Boundary Handling:** Stopped tracing at other rivers' outlets to prevent overlap

---

## Data Quality Issues Resolved

### Duplicate Name Remapping (74 segments affected)

| Original Name | Canonical Name | Segments |
|---------------|----------------|----------|
| Ganges | Ganga | 22 |
| Koshi River | Kosi | 9 |
| Chambal River | Chambal | 6 |
| Ghaghara River | Ghaghara | 5 |
| North Koel River | North Koel | 5 |
| Gomati / Gomti | Gomti | 5 |
| Mahananda River | Mahananda | 4 |
| Kamla River / Kamala River | Kamala | 3 |
| Gambhiri | Gambhir | 3 |
| Narayani | Gandak | 2 |
| Mahakali River | Sharda | 2 |
| Sipra | Shipra | 2 |
| Others | Various | 6 |

### Garbage Entries Removed (7 entries)

| Entry | Reason |
|-------|--------|
| `布抄老曲` | Non-Latin characters (data error) |
| `mAU nALA` | Formatting error |
| `Mungeshpur Drain` | Urban drainage, not a river |
| `Gambhir` | Too small / duplicate |
| `Bhain Ka Nala` | Contains "Nala" (drainage) |
| `Jashma Nala` | Contains "Nala" (drainage) |
| `Kādhu Nadi` | Non-ASCII characters |

### Outlet Corrections

| River | Original Outlet | Corrected Outlet | Reason |
|-------|-----------------|------------------|--------|
| Ramganga | 3864 | 3891 | Better upstream connectivity (3 vs 1 segments) |

---

## Rivers Excluded

Two rivers were removed from the final dataset due to network connectivity issues:

### Birahi Ganga (Tier 2 - Himalayan Headstream)
- **Issue:** Only 1 segment with this name in OSM data
- **Problem:** The outlet segment (4239) has no upstream connectivity in the mizuRoute network
- **Cause:** Network topology gap - no segments connect upstream to this outlet

### Giri (Tier 7 - Additional River)
- **Issue:** Only 1 segment with this name in OSM data
- **Problem:** The outlet segment (3326) is isolated with no upstream connections
- **Cause:** Network topology gap - segment is disconnected from the main network

Both rivers would require manual identification of their upstream network segments, which was outside the scope of this automated processing.

---

## Final River List (33 Rivers)

### Tier 1 - Main Stem
| River | Outlet seg_id | Segments |
|-------|---------------|----------|
| Ganga | 2959 | 495 |

### Tier 2 - Himalayan Headstreams
| River | Outlet seg_id | Segments |
|-------|---------------|----------|
| Bhagirathi | 4203 | 11 |
| Alaknanda | 4212 | 16 |
| Mandakini | 4211 | 5 |

### Tier 3 - Left Bank Major Tributaries
| River | Outlet seg_id | Segments |
|-------|---------------|----------|
| Ramganga | 3864 | 3 |
| Gomti | 3932 | 55 |
| Ghaghara | 4401 | 102 |
| Rapti | 4456 | 9 |
| Sarju | 4812 | 5 |
| Gandak | 4577 | 93 |
| Burhi Gandak | 4901 | 29 |

### Tier 4 - Left Bank Eastern Tributaries
| River | Outlet seg_id | Segments |
|-------|---------------|----------|
| Bagmati | 4880 | 15 |
| Kamala | 4934 | 9 |
| Kosi | 3872 | 7 |
| Mechi | 4846 | 3 |
| Mahananda | 5081 | 11 |

### Tier 5 - Right Bank Major Tributaries
| River | Outlet seg_id | Segments |
|-------|---------------|----------|
| Yamuna | 4254 | 231 |
| Tons | 4067 | 37 |
| Chambal | 3336 | 226 |
| Betwa | 4121 | 77 |
| Ken | 4281 | 45 |
| Son | 4341 | 67 |

### Tier 6 - Right Bank Secondary Rivers
| River | Outlet seg_id | Segments |
|-------|---------------|----------|
| Sharda | 4777 | 126 |
| Hindon | 3559 | 5 |
| Sindh | 3390 | 51 |
| Kali Sindh | 3627 | 41 |
| Karmanasa | 3962 | 19 |
| Punpun | 5170 | 13 |
| North Koel | 4565 | 23 |

### Tier 7 - Additional Significant Rivers
| River | Outlet seg_id | Segments |
|-------|---------------|----------|
| Gaula | 3861 | 11 |
| Rihand | 4340 | 29 |
| Gerua | 5111 | 11 |
| Rohini | 4448 | 7 |

---

## Output Files

### 1. `river_final_lookup.csv`
**Purpose:** Master lookup table mapping river names to outlet segment IDs

| Column | Description |
|--------|-------------|
| `river_name` | Canonical river name |
| `seg_id` | Outlet segment ID (most downstream segment) |
| `tier` | River tier (1-7) |

**Rows:** 33 (one per river)

### 2. `river_all_segments.csv`
**Purpose:** Complete list of all segments for all 33 rivers

| Column | Description |
|--------|-------------|
| `river_name` | River this segment belongs to |
| `seg_id` | Segment ID |
| `tier` | River tier |
| `BotElev` | Bottom elevation (m) |
| `TopElev` | Top elevation (m) |
| `distance_from_outlet` | Distance upstream from outlet (m) |

**Rows:** 1,887 segments

### 3. `river_segment_counts.txt`
**Purpose:** Summary of segment counts per river

**Format:** Plain text, sorted by segment count (descending)

---

## Validation Results

| Check | Result |
|-------|--------|
| Ganga has most segments | PASS (495 segments) |
| Bhagirathi connects to Ganga | PASS |
| Alaknanda connects to Ganga | PASS |
| Yamuna outlet elevation ~80m | PASS (76m at Prayagraj) |
| Kosi has Himalayan origin | PASS (max TopElev 1,206m) |

---

## Scripts

| Script | Purpose |
|--------|---------|
| `scripts/assign_river_names.py` | OSM spatial join and name normalization |
| `scripts/filter_rivers.py` | Filter to target 33 rivers, apply remapping |
| `scripts/trace_river_network.py` | Network topology building and upstream tracing |

---

## Summary Statistics

| Metric | Value |
|--------|-------|
| Original segments | 1,991 |
| Segments after filtering | 1,984 |
| Rivers identified | 33 |
| Total river segments | 1,887 |
| Coverage | 95% of filtered segments |
| Duplicates resolved | 74 |
| Garbage removed | 7 |
| Rivers excluded | 2 (Birahi Ganga, Giri) |
