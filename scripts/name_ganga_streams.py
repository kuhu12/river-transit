"""
name_ganga_streams.py
=====================
Assigns river names to mizuRoute segments using OSM waterways.gpkg via
line-to-line spatial overlay (not centroid matching).

Inputs:
    ganga_streams.shp    — 1,991 mizuRoute segments (no names)
    waterways.gpkg       — OSM India waterways (597,773 features)

Outputs:
    ganga_rivers_named.shp    — named segments (unmatched dropped)
    ganga_confluences.csv     — one row per river with real-world
                                confluence coordinates

Algorithm:
    1. Filter OSM to named river LineStrings
    2. For each mizuRoute segment, buffer by 1 km and find OSM rivers
       that intersect — assign the name with the longest overlap
    3. Propagate names upstream through connected chains, but only
       within a confirmed river's own topology (no basin-wide flood)
    4. Remove 11 truly isolated segments (no topological connections)
    5. Assign tiers by network depth from outlet
    6. Build confluence table from real-world reference coords

Requirements:
    pip install geopandas shapely pyogrio pandas numpy
"""

import geopandas as gpd
import pandas as pd
import numpy as np
import re
from shapely.geometry import LineString, Point
from shapely.ops import linemerge, unary_union
from math import radians, cos, sin, asin, sqrt
import warnings
warnings.filterwarnings('ignore')

# ── Edit these paths ──────────────────────────────────────────────────────────
STREAMS_SHP = "../Shapefiles/Ganga Basin Streams/WithoutNames/ganga_streams.shp"
OSM_GPKG    = "../Raw Data/OSM waterways/waterways.gpkg"
OUT_SHP     = "../Shapefiles/Ganga Basin Streams/finalRivers/ganga_rivers.shp"
OUT_CSV     = "../Processed Data/Confluence/ganga_confluences_new.csv"
BUFFER_DEG  = 0.009    # ~1 km at Indian latitudes (1° ≈ 111 km)
# ─────────────────────────────────────────────────────────────────────────────


def haversine_m(p1, p2):
    R = 6371000
    lat1, lon1 = radians(p1[1]), radians(p1[0])
    lat2, lon2 = radians(p2[1]), radians(p2[0])
    dlat, dlon = lat2-lat1, lon2-lon1
    a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
    return 2*R*asin(sqrt(a))


# ── Name normalisation map ────────────────────────────────────────────────────
# Resolves OSM name variants to a single canonical name.
# Applied BEFORE assignment so fuzzy post-processing is not needed.
NAME_CANON = {
    # Ganga mainstream
    'Ganges':             'Ganga',
    'Ganga River':        'Ganga',
    # Ghaghara variants
    'Ghaghra':            'Ghaghara',
    'Ghagra':             'Ghaghara',
    'Gogra':              'Ghaghara',
    # Sarju / Sarayu
    'Sarayu':             'Sarju',
    'Saryu':              'Sarju',
    # Kosi — all variants merged
    'Koshi':              'Kosi',
    'Sapt Koshi':         'Kosi',
    'Sapta Koshi':        'Kosi',
    'Saptakoshi':         'Kosi',
    'Sun Kosi':           'Kosi',
    'Kosi Dhār':          'Kosi',
    'Kosi tributary':     'Kosi',
    'Koshi river':        'Kosi',
    'Kosi river':         'Kosi',
    # Gandak / Narayani
    'Narayani':           'Gandak',
    'Gandaki':            'Gandak',
    'Small Gandak':       'Gandak',
    'Choti Gandak':       'Gandak',
    # Hooghly
    'Hugli':              'Hooghly',
    'Hugli River':        'Hooghly',
    # Son
    'Sone':               'Son',
    'Sone River':         'Son',
    # Yamuna — Indian stretch only
    'Jamuna':             'Yamuna',
    # Sharda — Mahakali kept separate (distinct upper Nepal border reach)
    'Sarda':              'Sharda',
    # Kamala
    'Kamla':              'Kamala',
    # Alaknanda
    'Alaknanda River':    'Alaknanda',
    # Bhagirathi
    'Bhagirathi River':   'Bhagirathi',
    # Mandakini
    'Mandakini River':    'Mandakini',
    # Burhi Gandak
    'Buri Gandak':        'Burhi Gandak',
    'Burhi Gandaki':      'Burhi Gandak',
    # Tamasa — Tamsa is short form; Tons is DISTINCT, no Tamsa→Tons
    'Tamsa':              'Tamasa',
    # Mahananda
    'Mahanadi':           'Mahananda',
    # Ramganga
    'Ram Ganga':          'Ramganga',
    # Koel — North Koel → Koel
    'North Koel River':   'Koel',
    'North Koel':         'Koel',
    # Rihand
    'Rehand':             'Rihand',
    'Rind':               'Rihand',
    # Gomti — all three variants
    'Gomati':             'Gomti',
    'Gomtī':              'Gomti',
    # Shipra / Sipra
    'Sipra':              'Shipra',
    # Vaisali
    'Vaisli':             'Vaisali',
    # Mohana lowercase
    'mohana':             'Mohana',
    # Punarbhaba
    'Punarbhava':         'Punarbhaba',
    'Punarbhava river':   'Punarbhaba',
    # Ghora Pachhar
    'Ghorapacchar':       'Ghora Pachhar',
    # Sasur Khaderi
    'Sasur Kaderi':       'Sasur Khaderi',
    # Suffix variants
    'Pahuj river':        'Pahuj',
    'Manorama river':     'Manorama',
    'Balan river':        'Balan',
    # Mathabanga — মাথাভাঙ্গা; Mathavanga is OSM variant spelling
    'Mathavanga':         'Mathabanga',
    # Diacritic normalisations
    'Kādhu':              'Kadhu',
    'Gumāni':             'Gumani',
    'Parmān':             'Parman',
    'Lassar Yānkti':      'Lassar Yankti',
    'Lilājān':            'Lilajan',
    'Ghāghi Nāla':        'Ghaghi',
    'Mailā Nadī':         'Maila',
}

# ── Non-Latin → English lookup table ─────────────────────────────────────────
SCRIPT_TO_ENGLISH = {
    'कमला नदि':         'Kamala',
    'कमला नदी':         'Kamala',
    'यमुना':            'Yamuna',
    'घाघरा':            'Ghaghara',
    'राप्ती नदी':       'Rapti',
    'जमनी नदी':         'Jamni',
    'चमेलिया नदी':      'Chamelia',
    'गंगा':             'Ganga',
    'बागमती':           'Bagmati',
    'পদ্মা নদী':        'Padma',
    'নাগর নদী':         'Nagar',
    'পাগলা নদী':        None,   # Pagla — outside basin scope
    'মাথাভাঙ্গা':       'Mathabanga',
    '多隆曲':            None,
    '布抄老曲':           None,
    '赤德浦曲':           None,
}

# Rivers to EXCLUDE — out-of-basin, noise, urban drains, garbled artifacts
EXCLUDE_NAMES = {
    # Bangladesh / Brahmaputra system
    'Bangladesh', 'Brahmaputra', 'Jamuna', 'Surma', 'Kushiyara',
    # Other river basins
    'Indus', 'Sutlej', 'Ravi', 'Beas',
    'Godavari', 'Krishna', 'Mahanadi',
    'Sahibi', 'Ruparel', 'Brahmani',
    # Urban drains / canals
    'Canal', 'canal', 'Mungeshpur', 'Chudania Bupania', 'Biring-Tangting',
    # Garbled artifacts
    'cmeliyaa ndii', 'tilava', 'Chidepu', 'Rangun', 'Jogbuda',
    # Not rivers / geographic features mislabeled
    'Bihar', 'Gaumukh', 'Bhain Ka', 'Bandhavgarh National Park',
    # OSM import errors
    'Yare', 'Parry',
    # Not identifiable in Ganga basin
    'Ratmau Roa', 'Moti Bala', 'Wagan', 'Wagli', 'Zela', 'Renpi',
    # Chinese romanised transliterations
    'Bu Chao Lao Qu', 'Duo Long Qu', 'Chi De Pu Qu',
    # Confirmed drops across audit rounds
    'Shehzad', 'Para Kala', 'Lhasi', 'Chameli', 'Baram',
    'Param', 'Koa', 'Ogho', 'Sip', 'Tem', 'Eru', 'Nion', 'Umar',
    'Pagla',   # পাগলা নদী — minor Bangladesh river, outside basin
}


def is_latin(text):
    return sum(1 for c in text if ord(c) < 256) / max(len(text), 1) > 0.7

_SUFFIXES = [
    ' River', ' Nadi', ' Nada', ' Nala', ' Nulla', ' Nullah',
    ' Gad', ' Khola', ' Jharna', ' Chu', ' Drain', ' tributary',
    ' stream', ' river', ' nadi', ' nala', ' drain',
]

def canonise(name):
    if not name or pd.isna(name):
        return None
    name = name.strip()
    name = re.sub(r'\s*\(.*?\)', '', name).strip()
    if not name:
        return None
    if name.endswith(' Qu'):
        return None
    if ';' in name:
        return None
    if not is_latin(name):
        return None
    name_title = name.title()
    for ex in EXCLUDE_NAMES:
        if ex.lower() in name.lower():
            return None
    if name in NAME_CANON:
        return NAME_CANON[name]
    if name_title in NAME_CANON:
        return NAME_CANON[name_title]
    for suffix in _SUFFIXES:
        if name.endswith(suffix):
            name = name[:-len(suffix)].strip()
            break
    if name in NAME_CANON:
        return NAME_CANON[name]
    if name.title() in NAME_CANON:
        return NAME_CANON[name.title()]
    if name in EXCLUDE_NAMES or name.lower() in {e.lower() for e in EXCLUDE_NAMES}:
        return None
    return name if name else None


# =============================================================================
# STEP 0 — Load data
# =============================================================================
print("Loading ganga_streams.shp ...")
streams = gpd.read_file(STREAMS_SHP)
print(f"  {len(streams)} segments  CRS={streams.crs}")

print("\nLoading OSM waterways (rivers only) ...")
osm_all = gpd.read_file(OSM_GPKG, where="waterway = 'river'")
osm = osm_all[osm_all.geometry.geom_type == 'LineString'].copy()
print(f"  {len(osm):,} river LineStrings loaded")

# Canonise OSM names — check all three name columns
def best_osm_name(row):
    raw = row.get('name', None)
    raw_str = str(raw).strip() if pd.notna(raw) and raw else ''
    if raw_str and not is_latin(raw_str):
        if raw_str in SCRIPT_TO_ENGLISH:
            return SCRIPT_TO_ENGLISH[raw_str]
        for col in ['name_en', 'name_latin']:
            if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
                c = canonise(str(row[col]))
                if c:
                    return c
        return None
    for col in ['name', 'name_en', 'name_latin']:
        if col in row.index and pd.notna(row[col]) and str(row[col]).strip():
            c = canonise(str(row[col]))
            if c:
                return c
    return None

print("  Canonising OSM names ...")
osm['canon_name'] = osm.apply(best_osm_name, axis=1)
osm = osm[osm['canon_name'].notna()].copy()
print(f"  {len(osm):,} named river segments after canonisation")

# Build spatial index on OSM
osm_sindex = osm.sindex


# =============================================================================
# STEP 1 — Remove isolated (no-topology) segments
# =============================================================================
print("\nStep 1 — removing isolated segments ...")

node_map = {}  # (lon, lat) rounded → list of seg_ids
for _, row in streams.iterrows():
    for c in [(round(row.start_lon, 5), round(row.start_lat, 5)),
              (round(row.end_lon,   5), round(row.end_lat,   5))]:
        node_map.setdefault(c, []).append(row.seg_id)

isolated_ids = set()
for _, row in streams.iterrows():
    s = (round(row.start_lon, 5), round(row.start_lat, 5))
    e = (round(row.end_lon,   5), round(row.end_lat,   5))
    s_nb = [x for x in node_map.get(s, []) if x != row.seg_id]
    e_nb = [x for x in node_map.get(e, []) if x != row.seg_id]
    if not s_nb and not e_nb:
        isolated_ids.add(row.seg_id)

streams = streams[~streams['seg_id'].isin(isolated_ids)].copy()
print(f"  Removed {len(isolated_ids)} isolated segments: {sorted(isolated_ids)}")
print(f"  Remaining: {len(streams)} segments")


# =============================================================================
# STEP 2 — Line-to-line spatial overlay: assign OSM name by longest overlap
# =============================================================================
print("\nStep 2 — line-to-line name assignment ...")

name_scores = {}   # seg_id → {canon_name: total_overlap_length}

for i, (_, seg) in enumerate(streams.iterrows()):
    if i % 200 == 0:
        print(f"  {i}/{len(streams)} segments processed ...")

    geom  = seg.geometry
    buf   = geom.buffer(BUFFER_DEG)
    cands = list(osm_sindex.intersection(buf.bounds))

    if not cands:
        continue

    scores = {}
    for j in cands:
        osm_row  = osm.iloc[j]
        osm_geom = osm_row.geometry
        cname    = osm_row.canon_name

        if not buf.intersects(osm_geom):
            continue

        # Measure overlap: length of OSM geometry that falls within the buffer
        try:
            overlap = osm_geom.intersection(buf)
            if overlap.is_empty:
                continue
            overlap_len = overlap.length
        except Exception:
            continue

        scores[cname] = scores.get(cname, 0) + overlap_len

    if scores:
        name_scores[seg.seg_id] = scores

# Assign best name (longest total overlap)
streams['river_name'] = None
streams['match_score'] = 0.0

for seg_id, scores in name_scores.items():
    best = max(scores, key=scores.get)
    idx  = streams.index[streams['seg_id'] == seg_id]
    streams.loc[idx, 'river_name'] = best
    streams.loc[idx, 'match_score'] = scores[best]

matched = streams['river_name'].notna().sum()
print(f"  Directly matched: {matched}/{len(streams)} segments ({matched/len(streams)*100:.1f}%)")


# =============================================================================
# STEP 2b — Geographic fallback for OSM-invisible segments
# =============================================================================
# The OSM file is India-only. Segments in Bangladesh (Padma) and some in the
# Hooghly delta have no OSM features to match. Assign by geography + elevation.
#
# Hooghly diagnosis: only 1 mizuRoute segment (seg 2937) sits in the true
# Hooghly corridor (88.35–88.5°E, 21.9–22.3°N, BotElev=0m, isolated).
# Segments at 88.5–88.9°E are east of Kolkata and belong to the Ganga/Padma
# delta distributaries, not the Hooghly proper.
print("\nStep 2b — geographic fallback for OSM-invisible segments ...")

def geo_fallback_name(row):
    lon = (row.start_lon + row.end_lon) / 2
    lat = (row.start_lat + row.end_lat) / 2
    elev = row.BotElev

    # Padma — Ganga main channel through Bangladesh (east of Farakka)
    if 88.0 < lon < 91.0 and 22.5 < lat < 24.5 and elev < 10:
        return 'Padma'

    # Hooghly — Ganga western distributary through Kolkata to Bay of Bengal
    # Flows south at ~88.35–88.5°E from Murshidabad to Sagar Island
    if 87.8 < lon < 88.6 and 21.3 < lat < 24.8 and elev < 25:
        return 'Hooghly'

    # Meghna — below Padma–Jamuna confluence at Chandpur
    if 90.4 < lon < 91.5 and 22.0 < lat < 23.5 and elev < 5:
        return 'Meghna'

    return None

fallback_count = 0
for idx, row in streams[streams['river_name'].isna()].iterrows():
    name = geo_fallback_name(row)
    if name:
        streams.at[idx, 'river_name'] = name
        streams.at[idx, 'match_score'] = 0.1
        fallback_count += 1

print(f"  Assigned {fallback_count} segments via geographic fallback")
for name in ['Padma', 'Hooghly', 'Meghna']:
    fb = streams[(streams['river_name'] == name) & (streams['match_score'] == 0.1)]
    if not fb.empty:
        print(f"    {name}: {len(fb)} segs → {fb['seg_id'].tolist()}")


# =============================================================================
# STEP 3 — Upstream propagation within confirmed river chains
# =============================================================================
print("\nStep 3 — upstream name propagation ...")

# Build adjacency: downstream_seg → upstream_segs
# A segment's start_lon/lat is its upstream end (inflow)
# A segment's end_lon/lat is its downstream end (outflow)
# Two segs connect when one's end == another's start

end_to_seg   = {}  # (end_lon, end_lat) → seg_id (who ends here)
start_to_seg = {}  # (start_lon, start_lat) → [seg_ids] (who starts here)
for _, row in streams.iterrows():
    e_key = (round(row.end_lon,   5), round(row.end_lat,   5))
    s_key = (round(row.start_lon, 5), round(row.start_lat, 5))
    end_to_seg[e_key]   = row.seg_id
    start_to_seg.setdefault(s_key, []).append(row.seg_id)

def upstream_segs(seg_id):
    """Return all seg_ids that flow directly into seg_id."""
    row   = streams[streams['seg_id'] == seg_id].iloc[0]
    s_key = (round(row.start_lon, 5), round(row.start_lat, 5))
    # Segments whose end_lon/lat == this seg's start
    ups = []
    for _, r in streams.iterrows():
        if (round(r.end_lon, 5), round(r.end_lat, 5)) == s_key:
            ups.append(r.seg_id)
    return ups

# Pre-build a lookup for efficiency
end_node_to_segid = {}
for _, row in streams.iterrows():
    key = (round(row.end_lon, 5), round(row.end_lat, 5))
    end_node_to_segid[key] = row.seg_id

start_node_to_segids = {}
for _, row in streams.iterrows():
    key = (round(row.start_lon, 5), round(row.start_lat, 5))
    start_node_to_segids.setdefault(key, []).append(row.seg_id)

def get_upstream(seg_id):
    row = streams[streams['seg_id'] == seg_id].iloc[0]
    s_key = (round(row.start_lon, 5), round(row.start_lat, 5))
    # Who ends at my start node?
    result = []
    for _, r in streams.iterrows():
        e_key = (round(r.end_lon, 5), round(r.end_lat, 5))
        if e_key == s_key and r.seg_id != seg_id:
            result.append(r.seg_id)
    return result

# Build full upstream map once
print("  Building upstream adjacency map ...")
upstream_map = {}
for _, row in streams.iterrows():
    s_key = (round(row.start_lon, 5), round(row.start_lat, 5))
    # Segments ending at my start
    ups = [end_node_to_segid[k] for k in [s_key] if k in end_node_to_segid
           and end_node_to_segid[k] != row.seg_id]
    upstream_map[row.seg_id] = ups

downstream_map = {}
for _, row in streams.iterrows():
    e_key = (round(row.end_lon, 5), round(row.end_lat, 5))
    dns = start_node_to_segids.get(e_key, [])
    downstream_map[row.seg_id] = [d for d in dns if d != row.seg_id]

# Propagation: for each named segment, trace UPSTREAM and assign name
# only to segments that have no competing direct match
named_mask   = streams['river_name'].notna()
unnamed_mask = ~named_mask

propagated = 0
# Sort named segments by match_score desc — highest confidence propagates first
named_segs = streams[named_mask].sort_values('match_score', ascending=False)

assigned = {}  # seg_id → name (from propagation)

for _, row in named_segs.iterrows():
    name   = row.river_name
    queue  = list(upstream_map.get(row.seg_id, []))
    visited = {row.seg_id}

    while queue:
        uid = queue.pop()
        if uid in visited:
            continue
        visited.add(uid)
        u_row = streams[streams['seg_id'] == uid]
        if u_row.empty:
            continue
        u_row = u_row.iloc[0]

        # Don't overwrite direct OSM match
        if pd.notna(u_row.river_name):
            continue
        # Don't propagate into a seg that already has a higher-confidence propagation
        if uid in assigned:
            continue

        assigned[uid] = name
        propagated  += 1

        # Continue upstream
        queue.extend(upstream_map.get(uid, []))

# Apply propagated names
for seg_id, name in assigned.items():
    idx = streams.index[streams['seg_id'] == seg_id]
    streams.loc[idx, 'river_name'] = name

total_named = streams['river_name'].notna().sum()
print(f"  Propagated names to {propagated} additional segments")
print(f"  Total named: {total_named}/{len(streams)} ({total_named/len(streams)*100:.1f}%)")


# =============================================================================
# STEP 3b — Post-propagation canonisation pass
# =============================================================================
# Propagation copies raw river_name values bypassing canonise(). Re-run
# canonise() on every river_name to clean diacritics (Gomtī→Gomti),
# non-Latin scripts (कमला नदि), suffixes (Koshi river→Kosi), and exclusions.
print("\nStep 3b — post-propagation canonisation pass ...")

def recanonise(name):
    if pd.isna(name):
        return None
    name_str = str(name).strip()
    # Check SCRIPT_TO_ENGLISH for non-Latin names that survived propagation
    if name_str in SCRIPT_TO_ENGLISH:
        return SCRIPT_TO_ENGLISH[name_str]
    return canonise(name_str)

before = streams['river_name'].notna().sum()
streams['river_name'] = streams['river_name'].map(recanonise)
after  = streams['river_name'].notna().sum()
print(f"  Cleared {before - after} segments (excluded / non-Latin / noise)")


# =============================================================================
# STEP 4 — Drop unmatched segments, assign tiers
# =============================================================================
print("\nStep 4 — dropping unmatched, assigning tiers ...")

named = streams[streams['river_name'].notna()].copy()
dropped = len(streams) - len(named)
print(f"  Dropped {dropped} unmatched segments")
print(f"  Remaining: {len(named)} segments")

# Tier assignment via BFS from outlet (Ganga = tier 1)
# Find Ganga outlet segment (lowest BotElev in Ganga)
# Tier = network distance from outlet river
named['tier'] = 99  # default

# Build downstream map on named segs only
named_ids = set(named['seg_id'])
dn_map_named = {}
up_map_named = {}
for _, row in named.iterrows():
    e_key = (round(row.end_lon, 5), round(row.end_lat, 5))
    s_key = (round(row.start_lon, 5), round(row.start_lat, 5))
    dns = [d for d in start_node_to_segids.get(e_key, []) if d in named_ids and d != row.seg_id]
    ups = [end_node_to_segid[s_key]] if s_key in end_node_to_segid and end_node_to_segid[s_key] in named_ids else []
    dn_map_named[row.seg_id] = dns
    up_map_named[row.seg_id] = ups

# Assign tier 1 to Ganga, Hooghly, Padma (all part of the main stem)
main_stem = {'Ganga', 'Hooghly', 'Padma'}
tier1_ids = set(named[named['river_name'].isin(main_stem)]['seg_id'])
named.loc[named['seg_id'].isin(tier1_ids), 'tier'] = 1

# BFS outward: tier = 1 + (number of river-name boundaries crossed)
# Each time we cross into a different river name, tier increments
from collections import deque

seg_to_name = dict(zip(named['seg_id'], named['river_name']))
tier_dict   = {sid: 1 for sid in tier1_ids}

# For each tier-1 seg, trace upstream — when we hit a different river, that's tier 2, etc.
queue = deque([(sid, 1, seg_to_name[sid]) for sid in tier1_ids])
visited = set(tier1_ids)

while queue:
    sid, current_tier, current_name = queue.popleft()
    for uid in up_map_named.get(sid, []):
        if uid in visited:
            continue
        visited.add(uid)
        u_name = seg_to_name.get(uid, '')
        if u_name == current_name:
            new_tier = current_tier
        else:
            new_tier = current_tier + 1
        tier_dict[uid] = new_tier
        queue.append((uid, new_tier, u_name))

named['tier'] = named['seg_id'].map(tier_dict).fillna(99).astype(int)
print(f"  Tier distribution:")
for t, cnt in sorted(named['tier'].value_counts().items()):
    print(f"    tier {t}: {cnt} segments")


# =============================================================================
# STEP 5 — Save shapefile
# =============================================================================
print("\nStep 5 — saving shapefile ...")

out_cols = ['seg_id', 'river_name', 'tier', 'start_lon', 'start_lat',
            'end_lon', 'end_lat', 'Length', 'TopElev', 'BotElev', 'geometry']
named[out_cols].to_file(OUT_SHP)
print(f"  Saved {len(named)} segments → {OUT_SHP}")
print(f"\n  Rivers assigned:")
for name, cnt in sorted(named['river_name'].value_counts().items()):
    t = named[named['river_name']==name]['tier'].min()
    print(f"    {name:<20} {cnt:4d} segs  tier {t}")


# =============================================================================
# STEP 6 — Build confluence CSV from real-world reference coords
# =============================================================================
print("\nStep 6 — building confluence CSV ...")

# Real-world confluence reference (NOT derived from shapefile endpoints)
CONFLUENCES_REF = [
    # (river, confluences_into, place_name, lon, lat)
    ('Bhagirathi',   'Ganga',       'Devprayag',       78.6026,  30.1441),
    ('Alaknanda',    'Ganga',       'Devprayag',        78.6026,  30.1441),
    ('Mandakini',    'Alaknanda',   'Rudraprayag',      78.9817,  30.2847),
    ('Yamuna',       'Ganga',       'Prayagraj',        81.8847,  25.4304),
    ('Tons',         'Yamuna',      'Prayagraj',        81.8800,  25.4250),
    ('Chambal',      'Yamuna',      'Etawah',           79.0163,  26.5800),
    ('Betwa',        'Yamuna',      'Hamirpur',         80.1437,  25.9490),
    ('Ken',          'Yamuna',      'Banda',            80.3521,  25.5133),
    ('Sindh',        'Yamuna',      'Mainpuri',         79.2500,  26.4000),
    ('Hindon',       'Yamuna',      'Ghaziabad',        77.7000,  28.7000),
    ('Kali Sindh',   'Chambal',     'Anta',             76.3700,  25.1500),
    ('Gomti',        'Ganga',       'Ghazipur',         83.5804,  25.5772),
    ('Ghaghara',     'Ganga',       'Chhapra',          84.7467,  25.7789),
    ('Sharda',       'Ghaghara',    'Brahmaghat',       81.3500,  27.9500),
    ('Sarju',        'Ghaghara',    'Ayodhya',          82.1964,  26.7847),
    ('Rapti',        'Ghaghara',    'Barhaj',           83.7283,  26.3083),
    ('Rohini',       'Rapti',       'Gorakhpur',        83.3700,  26.7800),
    ('Ramganga',     'Ganga',       'Kannauj',          79.9080,  27.0551),
    ('Gaula',        'Ramganga',    'Kalagarh',         78.9800,  29.5000),
    ('Son',          'Ganga',       'Koilwar',          84.7739,  25.5633),
    ('Rihand',       'Son',         'Singrauli',        82.7500,  24.2000),
    ('Koel',         'Son',         'Medininagar',      84.0750,  23.9000),
    ('Gandak',       'Ganga',       'Hajipur',          85.2167,  25.6940),
    ('Gerua',        'Gandak',      'Gopalganj',        84.3058,  26.4908),
    ('Bagmati',      'Ganga',       'Rosera',           85.9975,  25.8689),
    ('Burhi Gandak', 'Ganga',       'Rosera',           86.0000,  25.8700),
    ('Kosi',         'Ganga',       'Kursela',          87.2204,  25.4412),
    ('Kamala',       'Kosi',        'Supaul',           86.6000,  26.1200),
    ('Mechi',        'Mahananda',   'Siliguri',         88.1350,  26.6117),
    ('Mahananda',    'Ganga',       'Manikchak',        87.9197,  25.1687),
    ('Karmanasa',    'Ganga',       'Chandauli',        83.8667,  25.2833),
    ('Punpun',       'Ganga',       'Masaurhi',         85.0700,  25.5550),
    ('Hooghly',      'Bay of Bengal','Sagar Island',    88.0500,  21.6500),
    ('Padma',        'Bay of Bengal','Meghna mouth',    90.6000,  22.3000),
]

# For each confluence, find the outlet segment = named segment with BotElev
# closest to the real-world confluence coordinate
def find_outlet_seg(river_name, conf_lon, conf_lat):
    conf_pt = (conf_lon, conf_lat)
    # Strategy 1: exact match
    segs = named[named['river_name'] == river_name]
    # Strategy 2: canonised form
    if segs.empty:
        canon = canonise(river_name)
        if canon and canon != river_name:
            segs = named[named['river_name'] == canon]
    # Strategy 3: case-insensitive partial match
    if segs.empty:
        segs = named[named['river_name'].str.lower().str.contains(
            river_name.lower().strip(), na=False, regex=False)]
    if segs.empty:
        return None, None
    best_d, best_seg = float('inf'), None
    for _, row in segs.iterrows():
        for c in [(row.start_lon, row.start_lat), (row.end_lon, row.end_lat)]:
            d = haversine_m(c, conf_pt)
            if d < best_d:
                best_d, best_seg = d, row.seg_id
    return best_seg, best_d

rows = []
for river, into, place, lon, lat in CONFLUENCES_REF:
    outlet_seg, dist_m = find_outlet_seg(river, lon, lat)
    if outlet_seg is None:
        print(f"  ⚠ {river} — no segments near confluence ({lon:.4f},{lat:.4f})")
        continue
    rows.append({
        'river_name':       river,
        'confluences_into': into,
        'place_name':       place,
        'confluence_lon':   lon,
        'confluence_lat':   lat,
        'outlet_seg_id':    outlet_seg,
        'dist_m':           round(dist_m) if dist_m else None,
    })

conf_df = pd.DataFrame(rows)
conf_df.to_csv(OUT_CSV, index=False)
print(f"  Saved {len(conf_df)} confluence records → {OUT_CSV}")
print(f"\n  Sample (dist_m = distance from outlet seg endpoint to real-world confluence coord):")
print(conf_df[['river_name','confluences_into','place_name','outlet_seg_id','dist_m']].to_string(index=False))

print("\n✓ Done")
print(f"  {OUT_SHP}  — {len(named)} named segments")
print(f"  {OUT_CSV}  — {len(conf_df)} confluences")
print("\nNext steps:")
print("  1. Load ganga_rivers_named.shp in Mapshaper / QGIS and inspect")
print("  2. Check dist_m column in CSV — values >10,000m flag rivers that")
print("     still don't reach their real confluence (dataset truncation)")
print("  3. Run fix_truncated_rivers.py on the output to extend those reaches")
