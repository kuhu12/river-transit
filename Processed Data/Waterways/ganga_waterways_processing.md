# Ganga Basin Waterways — Infrastructure Extraction & Enrichment Processing Log

**Project:** Ganga Basin River Network Visualisation  
**Scripts:** `extract_waterways.py` · `enrich_dam_capacity.py`  
**Inputs:** `waterways.gpkg` (OSM India) · `ganga_rivers_named.shp` · `dams_detail.json`  
**Outputs:** `ganga_dams.csv` · `ganga_dams_enriched.csv` · `ganga_barrages.csv` · `ganga_canals.csv`

---

## 1. Overview

Three infrastructure layers were extracted from OSM and a curated dam dataset:

| Output file | Source | Records | Snapped to river |
|---|---|---|---|
| `ganga_dams.csv` | OSM `waterways.gpkg` | 399 | 77 (19.3%) |
| `ganga_dams_enriched.csv` | `dams_detail.json` (JSON-primary) | 781 | 172 (22%) |
| `ganga_barrages.csv` | OSM `waterways.gpkg` | 11 | 3 (27.3%) |
| `ganga_canals.csv` | OSM `waterways.gpkg` | 137 | 86 (62.8%) |

---

## 2. Input Data

### 2.1 OSM Waterways (`waterways.gpkg`)

The HOT/oex OSM export for India, snapshot 2026-05-10. Contains 597,773 features across all geometry types and waterway categories. For this processing only `waterway=river` LineStrings were used in the name assignment pipeline (see `ganga_rivers_processing.md`); infrastructure extraction uses the broader feature set.

| Field | Description |
|---|---|
| `name` | Primary OSM name (87.4% null) |
| `name_en` | English name (99.5% null) |
| `waterway` | Feature type tag (dam, weir, barrage, canal, river, stream…) |
| `man_made` | Secondary type tag (dam, weir) |
| `water` | Water body type (reservoir, pond, lake…) |
| `natural_class` | Natural feature classification |

Coverage: India only (`adm0_pcode=IND`). Bangladesh delta features are absent.

### 2.2 Ganga Rivers Shapefile (`ganga_rivers_named.shp`)

The cleaned and named mizuRoute segment shapefile produced by `name_ganga_streams.py`. Used as the snapping target for all infrastructure features. Basin boundary for clipping is derived from this file, not from the KML.

### 2.3 Dam Details JSON (`dams_detail.json`)

A curated official dataset of Indian dams. The full dataset contains **4,986 dams** across all Indian river basins. **781 records** belong to the Ganga basin (filtered by `RiverBasin='Ganga'`).

| Field | Description | Coverage |
|---|---|---|
| `Name` | Dam name | 781/781 |
| `Latitude` / `Longitude` | Surveyed dam site coordinates | 781/781 |
| `Address` | Format: `"RiverName. River, District, State"` | 781/781 |
| `State` | Indian state | 781/781 |
| `RiverBasin` | Basin name (all = 'Ganga') | 781/781 |
| `HaLF` | Full Reservoir Level elevation (metres ASL) | 773/781 |
| `DamLength` | Dam crest length (metres) | 768/781 |
| `GrossStorageCapacity` | Gross storage (cubic metres) | 771/781 |
| `ReservoirArea` | Reservoir surface area (square metres) | 781/781 |
| `EffectiveStorageCapacity` | Live/usable storage (cubic metres) | 769/781 |

State distribution of Ganga basin dams: Madhya Pradesh (440), Rajasthan (128), Uttar Pradesh (123), Bihar (26), Jharkhand (26), Uttarakhand (24), Chhattisgarh (10), West Bengal (4).

---

## 3. Basin Boundary

### Decision: derive from shapefile, not KML

The original script used `IndiaBasins.kml` to clip features to the Ganga basin. Audit found this KML polygon cuts off at **89.1°E**, excluding 391 stream segments — including the Bangladesh delta (Padma/Meghna channels at 89–91°E) and parts of the Himalayan headwaters.

**Fix applied:** Basin boundary is now derived from the river shapefile itself:

```python
basin = rivers.union_all().convex_hull.buffer(0.1)
```

This convex hull + 0.1° buffer captures all segments. The KML file and its loading function were removed entirely.

---

## 4. Feature Extraction (`extract_waterways.py`)

### 4.1 OSM Tag Coverage

#### Dams
Three OSM tags are needed to capture the full dam inventory:

| Tag | What it captures |
|---|---|
| `waterway=dam` | Dam structure (line or polygon) |
| `man_made=dam` | Alternate tag used for major structures |
| `water=reservoir` | Impoundment polygon — needed for major reservoirs |

#### Barrages
**Decision: include `waterway=weir` in addition to `waterway=barrage`.**

OSM uses `waterway=weir` for most major barrage structures in the Ganga basin — Farakka Barrage, Haridwar Barrage, Narora, Kanpur headworks are all tagged as weirs. Using `waterway=barrage` alone produced an empty output. `man_made=weir` is also checked as an alternate tag.

A deduplication guard (`barrage_mask &= ~dam_mask`) prevents any feature captured as a dam from also appearing as a barrage.

#### Canals
`waterway=canal` only.

### 4.2 Dam Noise Filtering

OSM tags village ponds, step-wells, and traditional water storage structures as `water=reservoir`, causing them to appear in the dam extract. These are not river infrastructure. A regex filter removes features where the name contains known pond/tank suffixes:

**Filtered suffixes:** `Johad`, `Kund`, `Talab`, `Talav`, `Tank`, `Pond`, `Talaiya`, `Pokhar`, `Baoli`, `Vav`, `Step-well`

From the initial 557 raw OSM dam features, 130+ village pond records were removed before snapping, yielding 399 clean records.

### 4.3 Snapping

All features are snapped to the nearest river segment using an **STRtree spatial index** (shared across all three layers). A representative point is computed for each feature:

- Points → used directly
- LineStrings / MultiLineStrings → centroid
- Polygons / MultiPolygons → centroid

**Snap thresholds:**

| Layer | Threshold | Approx. distance |
|---|---|---|
| Dams | 0.02° | ~2.2 km |
| Barrages | 0.02° | ~2.2 km |
| Canals | 0.05° | ~5.5 km |

Canals use a looser threshold because headworks may sit several km from the river segment centreline. A `dist_deg` column is written to all outputs for quality inspection.

Rows are **not dropped** for missing `seg_id` (feature beyond snap threshold). Rows are dropped only if `name`, `latitude`, or `longitude` is null.

### 4.4 Canal Headworks — One Row Per Canal

**Problem identified:** OSM stores long canals as chains of many separate `way` features sharing the same name. Naive centroid processing produced 28 rows for `Mahananda Main Canal`, 27 for `Doun canal`, etc. — all with different coordinates, none of them meaningful as a single station.

**Decision:** Canals should be represented as a single point at their **headworks** (offtake point) — where the canal branches off from the parent river.

**Algorithm implemented in `extract_canal_headworks()`:**

1. Group all OSM way segments by canonical canal name
2. Merge segments into a single geometry using `shapely.ops.linemerge()`
3. Extract both endpoints of the merged line
4. Find which endpoint is closest to any river segment — that is the headworks
5. Snap that single point → one output row per canal

Generic label-only names (`Spillway`, `Nahar`, `Canal`, `Drain`, `Nala`) are excluded before grouping.

**Result:** 466 raw canal segments → 137 unique named canals, each with a headworks point.

### 4.5 Outputs (OSM-derived)

**Mandatory columns** (row dropped only if any of these three are null):
`name`, `latitude`, `longitude`

**Always present columns:**
`name`, `river_name`, `seg_id`, `latitude`, `longitude`, `dist_deg`

**Optional columns** (included only if OSM carries non-null values):
`capacity_m3`, `capacity_mcm`

---

## 5. Dam Enrichment (`enrich_dam_capacity.py`)

### 5.1 Decision: JSON as primary source, not enrichment

Initial plan was to use the OSM CSV as the base and join JSON capacity data onto it. Analysis showed this approach wastes most of the JSON dataset:

| Metric | OSM CSV | JSON |
|---|---|---|
| Records | 399 | 781 |
| With coordinates | 399 (OSM centroid) | 781 (surveyed) |
| With capacity data | 0 | 771 |
| With state / address | 0 | 781 |
| Overlap with each other (within 2 km) | 41 | 41 |

Only 41 of 399 OSM dams have a JSON counterpart within 2 km — the two datasets are largely disjoint. The OSM extract captured reservoir polygons and village tanks absent from the JSON; the JSON contains 742 curated dams not present in the OSM extract. Enriching the OSM CSV would keep 399 records with capacity for only 41 of them, discarding 740 curated dams.

**The JSON is the authoritative source.** All 781 records are used as the primary dataset.

### 5.2 Snapping JSON Coordinates

JSON dam coordinates are surveyed site locations, not OSM centroids. Snapping uses the same STRtree approach at 0.02° threshold. Expected snap rate is 60–80%+ (vs 19% for OSM) because surveyed coordinates sit directly on or adjacent to dam structures, which are always on rivers.

Achieved snap rate: **172/781 (22%)**. The remaining 78% are predominantly dams on minor tributaries not represented in the mizuRoute shapefile.

### 5.3 Local River Name Parsing

The JSON `Address` field follows a consistent pattern:

```
"Kohira. River, Bhabua, Bihar"
"Mani River, Munger, Bihar"
"Nagi. River, Munger, Bihar"
```

A regex parser extracts the local river name (e.g. `Kohira`, `Mani`, `Nagi`) into a `local_river` column. This names the actual tributary the dam sits on, which is often a minor stream not in the shapefile — complementing `river_name` (the snapped shapefile river, typically a larger named river nearby).

### 5.4 OSM Name Cross-Reference

For JSON dams with a nearby OSM feature within 1 km, the OSM name is added as `osm_name`. This is logged only when the OSM name differs from the JSON name — identical names are not duplicated. Provides a cross-reference for auditing naming discrepancies between the two sources.

### 5.5 Output Columns (`ganga_dams_enriched.csv`)

| Column | Source | Unit |
|---|---|---|
| `name` | JSON `Name` | — |
| `state` | JSON `State` | — |
| `address` | JSON `Address` | — |
| `local_river` | Parsed from `Address` | — |
| `river_name` | Snapped from shapefile | — |
| `seg_id` | Snapped from shapefile | — |
| `dist_deg` | Snap distance | degrees |
| `latitude` | JSON (surveyed) | decimal degrees |
| `longitude` | JSON (surveyed) | decimal degrees |
| `half_m` | JSON `HaLF` | metres ASL |
| `dam_length_m` | JSON `DamLength` | metres |
| `capacity_m3` | JSON `GrossStorageCapacity` | cubic metres |
| `capacity_mcm` | Derived (`÷ 1,000,000`) | million cubic metres |
| `reservoir_area_m2` | JSON `ReservoirArea` | square metres |
| `effective_storage_m3` | JSON `EffectiveStorageCapacity` | cubic metres |
| `osm_name` | Cross-referenced from OSM CSV | — |

---

## 6. Known Limitations

### Dam snap rate (OSM: 19%, JSON: 22%)

Both snap rates are low because the mizuRoute shapefile only contains the named river network. Many dams sit on minor unnamed tributaries that are not represented as segments in the shapefile. The snap finds the nearest named river, which may be several km away on a different channel.

### Barrage coverage (11 records)

OSM barrage coverage in the Ganga basin is sparse. Many control structures (headworks, regulators, annicuts) are either unmapped or tagged inconsistently. The 11 records represent only the most prominent named weirs and barrages. The `ganga_dams_detail.json` does not contain barrage records.

### OSM India-only coverage

The `waterways.gpkg` covers India only. Any infrastructure features in Bangladesh (e.g. Farakka Barrage on the Indian side is captured; its Bangladesh outfall structures are not) or Nepal are absent from the OSM extract.

### Two dam files

Two separate dam output files exist by design:

- **`ganga_dams.csv`** — OSM-sourced, 399 records, 6 columns. Useful for features where OSM geometry (polygon extent, structure footprint) matters.
- **`ganga_dams_enriched.csv`** — JSON-sourced, 781 records, 15 columns. Use this for analysis requiring capacity, elevation, or dam dimensions.

The two datasets should not be combined naively — 41 dams appear in both and would be duplicated.

---

## 7. Script Reference

### `extract_waterways.py`
Inputs: `waterways.gpkg`, `ganga_rivers_named.shp`  
Outputs: `ganga_dams.csv`, `ganga_barrages.csv`, `ganga_canals.csv`

Key functions:

| Function | Purpose |
|---|---|
| `load_ganga_basin(rivers)` | Derives basin boundary from shapefile convex hull |
| `extract_layers(waterways, basin)` | Filters and clips OSM features by tag |
| `snap_dams(dams, rivers, tree)` | Centroid snap with noise filtering |
| `snap_barrages(barrages, rivers, tree)` | Centroid snap |
| `extract_canal_headworks(canals, rivers, tree)` | Groups by name, merges segments, finds offtake point |
| `apply_mandatory_filter(df, label)` | Drops rows missing name/lat/lon |
| `select_columns(df)` | Keeps mandatory cols; optional only if non-empty |

### `enrich_dam_capacity.py`
Inputs: `dams_detail.json`, `ganga_rivers_named.shp`, `ganga_dams.csv` (for OSM xref)  
Outputs: `ganga_dams_enriched.csv`

Key functions:

| Function | Purpose |
|---|---|
| `load_json(path)` | Loads and validates JSON; drops records missing coordinates |
| `parse_local_river(address)` | Extracts river name from Address field via regex |
| `snap_all(records, rivers)` | Snaps all 781 JSON coordinates to shapefile |
| `crossref_osm_names(records, osm_df)` | Finds nearest OSM name within 1 km |
| `build_dataframe(...)` | Assembles all columns including MCM conversion |
