# Ganga Basin Rivers — Name Assignment & Cleanup Processing Log

**Project:** Ganga Basin River Network Visualisation  
**Script:** `name_ganga_streams.py`  
**Input:** `ganga_streams.shp` (1,991 mizuRoute segments) + `waterways.gpkg` (OSM India, 597,773 features)  
**Output:** `ganga_rivers.shp` (named segments) + `ganga_confluences.csv`  
**CRS:** EPSG:4326 (WGS 84)

---

## 1. Problem Statement

The mizuRoute hydrological model produced 1,991 stream segments covering the Ganga basin with attributes `seg_id`, `start_lon/lat`, `end_lon/lat`, `Length`, `TopElev`, `BotElev` but **no river names**. Names were needed for visualisation, analysis, and confluence mapping.

### Why the original approach failed

An initial centroid-based nearest-neighbour spatial join (500 m threshold) was attempted first. This produced a badly corrupted output:

- Median segment length is 17.7 km, placing centroids up to 8.8 km from the actual river channel — far outside the 500 m threshold
- Only 493 of 1,887 segments (25%) received a direct OSM match; the remaining 75% were named by network tracing
- Network tracing propagated errors basin-wide, resulting in `Ganga` being assigned to 495 segments spanning ~12,500 km chord length (real Ganga mainstream ≈ 2,525 km)
- Confluence coordinates were derived from shapefile endpoints rather than real-world locations, making them systematically wrong
- 72 Ganga segments had `BotElev > 1,000 m` at latitudes 27–30°N — these are clearly Himalayan tributaries, not the Ganga mainstream

---

## 2. Revised Methodology

### Step 0 — Load and pre-filter OSM data

OSM waterways are loaded from `waterways.gpkg` filtered to `waterway = 'river'` LineStrings only (~30,670 features from 597,773 total). Each OSM feature is immediately canonised to a standard English name before entering the matching pool.

### Step 1 — Remove isolated segments

Segments with no topological connections at either endpoint (no neighbours within coordinate precision of 0.00001°) are removed. These are delta boundary fragments with no role in the routed network.

**Result:** 11 isolated segments removed, 1,980 segments retained.

### Step 2 — Line-to-line spatial overlay (core name assignment)

For each mizuRoute segment:
1. Buffer the segment geometry by **0.009° (~1 km)** along the actual line
2. Find all OSM river LineStrings intersecting the buffer using an STR-tree spatial index
3. Measure the length of each OSM line's intersection with the buffer
4. Assign the name with the **longest total overlap** — not nearest centroid

This correctly handles long segments: a river running parallel for 20 km scores far higher than one crossing briefly.

### Step 2b — Geographic fallback for OSM-invisible segments

The OSM file covers India only (`adm0_pcode = IND`). Segments in Bangladesh and the Hooghly delta have no OSM features to match against. These are assigned by geography and elevation:

| Condition | Assigned name |
|---|---|
| `88.0–91.0°E`, `22.5–24.5°N`, `BotElev < 10 m` | Padma |
| `87.8–88.6°E`, `21.3–24.8°N`, `BotElev < 25 m` | Hooghly |
| `90.4–91.5°E`, `22.0–23.5°N`, `BotElev < 5 m` | Meghna |

**Hooghly note:** Only seg 2937 (88.452–88.460°E, 21.9–22.3°N, BotElev=0 m) falls in the true Hooghly corridor south of Kolkata. Segments at 88.5–88.9°E are Ganga/Padma delta distributaries flowing east, not the Hooghly.

### Step 3 — Constrained upstream propagation

Named segments propagate their names upstream through the network topology, but only to segments with no competing direct OSM match. Propagation is ordered by match score (highest confidence first) to prevent weak matches from overwriting strong ones.

**Topology construction:** Segments are connected where `end_lon/lat` of one segment matches `start_lon/lat` of another within 0.00001° precision.

### Step 3b — Post-propagation canonisation pass

Propagation copies raw `river_name` strings without running them through `canonise()`. A second pass re-applies the full canonisation function to every `river_name` value after propagation. This catches:
- Diacritics that survived propagation unchanged (e.g. `Gomtī → Gomti`)
- Non-Latin scripts (e.g. `कमला नदि` → dropped, then SCRIPT_TO_ENGLISH supplies `Kamala`)
- Suffix-bearing names (e.g. `Koshi river → Kosi`)
- Excluded names that slipped through (e.g. `Sahibi`, `Bihar`)

### Step 4 — Drop unmatched segments and assign tiers

Segments with no `river_name` after Steps 2–3b are dropped from the output. Tiers are assigned by BFS from the Ganga/Hooghly/Padma main stem: crossing into a different river name increments the tier by 1.

### Step 5 — Save shapefile

Output columns: `seg_id`, `river_name`, `tier`, `start_lon`, `start_lat`, `end_lon`, `end_lat`, `Length`, `TopElev`, `BotElev`, `geometry`.

### Step 6 — Build confluence CSV

Confluence coordinates are taken from a hardcoded real-world reference table (not derived from shapefile endpoints). For each river the outlet segment is found as the endpoint nearest the real-world confluence coordinate. The `dist_m` column flags rivers whose shapefile network ends short of the real confluence (dataset truncation).

---

## 3. Name Canonisation

### 3.1 Canonise function — processing order

Every OSM name string passes through `canonise()` in this order:

1. **Strip parentheticals** — `'Bagmati River (बागमती नदी)'` → `'Bagmati River'`
2. **Drop Qu-endings** — romanised Tibetan/Chinese names ending in ` Qu` are excluded
3. **Drop semicolons** — multi-name strings like `'Gambhir;Parbati'` are dropped entirely (decision: ambiguous, skip)
4. **Drop non-Latin scripts** — Devanagari, Bengali, Chinese etc. return `None` and fall through to SCRIPT_TO_ENGLISH
5. **Title-case normalisation** — `'mohana'` → `'Mohana'` before lookup
6. **EXCLUDE_NAMES check** — excluded rivers return `None`
7. **NAME_CANON lookup** — direct and title-cased form
8. **Suffix stripping** — one suffix removed, then NAME_CANON re-checked
9. **Post-strip exclusion check** — `'tilava nadi'` → `'tilava'` → excluded

### 3.2 Non-Latin script handling

OSM features with non-Latin primary names use `SCRIPT_TO_ENGLISH` lookup first, then fall back to `name_en` / `name_latin` columns.

| Script name | English canonical |
|---|---|
| `कमला नदि` / `कमला नदी` | Kamala |
| `यमुना` | Yamuna |
| `घाघरा` | Ghaghara |
| `राप्ती नदी` | Rapti |
| `जमनी नदी` | Jamni |
| `गंगा` | Ganga |
| `बागमती` | Bagmati |
| `পদ্মা নদী` | Padma |
| `নাগর নদী` | Nagar |
| `মাথাভাঙ্গা` | Mathabanga |
| `পাগলা নদী` | `None` — outside basin |
| `多隆曲` / `布抄老曲` / `赤德浦曲` | `None` — Tibet-origin, outside basin |

### 3.3 Suffix strip list

The following suffixes are stripped (longest-first to avoid partial stripping):

`River`, `Nadi`, `Nada`, `Nala`, `Nulla`, `Nullah`, `Gad`, `Khola`, `Jharna`, `Chu`, `Drain`, `tributary`, `stream` (and lowercase variants)

### 3.4 NAME_CANON — variant → canonical mappings (59 entries)

#### Kosi (decision: merge all variants)

`Sun Kosi`, `Koshi`, `Sapt Koshi`, `Sapta Koshi`, `Saptakoshi`, `Kosi Dhār`, `Kosi tributary`, `Koshi river`, `Kosi river` → **Kosi**

*Rationale: Sun Kosi is geographically a distinct headwater but all variants refer to the same river system within the basin boundary. Simplified to a single name for consistency.*

#### Ghaghara
`Ghaghra`, `Ghagra`, `Gogra` → **Ghaghara**

#### Sarju
`Sarayu`, `Saryu` → **Sarju**

*Rationale: Sarju is the canonical name used in Indian hydrological literature for the river in Uttarakhand; Sarayu refers to the same river in the plains near Ayodhya.*

#### Gandak
`Narayani`, `Gandaki`, `Small Gandak`, `Choti Gandak` → **Gandak**

#### Hooghly
`Hugli`, `Hugli River` → **Hooghly**

#### Son
`Sone`, `Sone River` → **Son**

#### Gomti (decision: merge all three variants)
`Gomati`, `Gomtī` → **Gomti**

#### Rihand
`Rehand`, `Rind` → **Rihand**

#### Koel
`North Koel`, `North Koel River` → **Koel**

*Note: the old mapping `Koel → North Koel` was reversed. `Koel` is the canonical short form used consistently.*

#### Tamasa (decision: keep distinct from Tons)
`Tamsa` → **Tamasa**

*Rationale: Tamasa and Tons are two different rivers. The original script incorrectly mapped `Tamsa → Tons`. Tamsa is the short OSM form of Tamasa (the Tamas/Tamsa river in UP); Tons is a separate Yamuna tributary.*

#### Mahakali (decision: keep separate from Sharda)
The mapping `Mahakali → Sharda` was **removed**. Mahakali refers specifically to the upper Nepal border reach; Sharda is the name used in the Indian plains. Both names are retained as distinct rivers.

#### Other mappings
`Kamla → Kamala`, `Sarda → Sharda`, `Ram Ganga → Ramganga`, `Buri Gandak / Burhi Gandaki → Burhi Gandak`, `Mahanadi → Mahananda` (NE context only), `Jamuna → Yamuna` (Indian stretch only — Bangladesh Jamuna = Brahmaputra), `Sipra → Shipra`, `Vaisli → Vaisali`, `mohana → Mohana`, `Punarbhava / Punarbhava river → Punarbhaba`, `Ghorapacchar → Ghora Pachhar`, `Sasur Kaderi → Sasur Khaderi`, `Mathavanga → Mathabanga`

#### Suffix variants directly in NAME_CANON
`Pahuj river → Pahuj`, `Manorama river → Manorama`, `Balan river → Balan`

#### Diacritic normalisations
`Gomtī → Gomti`, `Kādhu → Kadhu`, `Gumāni → Gumani`, `Parmān → Parman`, `Lassar Yānkti → Lassar Yankti`, `Lilājān → Lilajan`, `Ghāghi Nāla → Ghaghi`, `Mailā Nadī → Maila`

---

## 4. EXCLUDE_NAMES — Dropped Rivers (42 entries)

### Out-of-basin river systems
| Name | Reason |
|---|---|
| `Brahmaputra`, `Jamuna`, `Surma`, `Kushiyara` | Brahmaputra/Meghna system — separate basin |
| `Indus`, `Sutlej`, `Ravi`, `Beas` | Indus basin |
| `Godavari`, `Krishna`, `Mahanadi` | Deccan rivers |
| `Sahibi` | Endorheic drainage — flows into Haryana/Rajasthan, not Ganga |
| `Ruparel` | Drains to Arabian Sea |
| `Brahmani` | Jharkhand Brahmani → Mahanadi, not Ganga basin |
| `Biring-Tangting` | Sikkim border stream, outside basin |

### Urban drains and canals
| Name | Reason |
|---|---|
| `Canal`, `canal` | Generic canal tag |
| `Mungeshpur` | Delhi urban drain |
| `Chudania Bupania` | Urban drain — `Drain` suffix stripped but base name is noise |

### Garbled OSM artifacts
| Name | Reason |
|---|---|
| `cmeliyaa ndii` | Garbled transliteration |
| `tilava` | Unverifiable — not a known Ganga basin stream |
| `Chidepu` | Not identifiable in Ganga basin |
| `Rangun` | Fragment from `Rangun River(jogbuda)` after parenthetical strip |
| `Jogbuda` | Fragment from same source after `River` suffix strip |

### Geographic features mislabeled as rivers in OSM
| Name | Reason |
|---|---|
| `Bihar` | Indian state name |
| `Gaumukh` | Gangotri glacial snout — a geographic point, not a river |
| `Bhain Ka` | Truncated `Bhain Ka Nala` — base name meaningless after Nala strip |
| `Bandhavgarh National Park` | National park name |

### OSM import errors (non-Indian rivers)
| Name | Reason |
|---|---|
| `Yare` | River Yare, Norfolk, England — erroneous OSM import |
| `Parry` | Not an Indian river |

### Not identifiable in Ganga basin
`Ratmau Roa`, `Moti Bala`, `Wagan`, `Wagli`, `Zela`, `Renpi`, `Shehzad`, `Para Kala`, `Lhasi`, `Chameli`, `Baram`, `Param`, `Koa`, `Ogho`, `Sip`, `Tem`, `Eru`, `Nion`, `Umar`, `Pagla`

### Chinese/Tibetan romanised transliterations
`Bu Chao Lao Qu`, `Duo Long Qu`, `Chi De Pu Qu` — Tibet-origin headwater streams. `is_latin()` returns True for these (they are ASCII), so they are also caught by the explicit `endswith(' Qu')` check in `canonise()`.

---

## 5. Similar-Name Pairs — Decisions

These pairs were reviewed and confirmed as **distinct rivers** (no merging):

| Pair | Reasoning |
|---|---|
| `Tamasa` / `Tons` | Different rivers: Tamasa (Tamas) is a UP river; Tons is a Yamuna headwater in Uttarakhand |
| `Sotwa` / `Sota` | Two distinct streams in the same basin |
| `Gambhir` / `Gambhiri` | Gambhir = Yamuna trib near Agra; Gambhiri = Banas trib in Rajasthan |
| `Kalindi` / `Kalindri` | Kalindi = historical Yamuna name (2 segs); Kalindri = Banas trib in Rajasthan |
| `Jamni` / `Jamuni` | Jamni = Tons trib in MP; Jamuni = Yamuna trib in UP |
| `Son` / `Song` | Son = major Ganga trib; Song = Dehradun-area Ganga trib near Rishikesh |
| `Nandakini` / `Mandakini` | Both Alaknanda tribs at different Prayags (Nandaprayag / Rudraprayag) |
| `Mahakali` / `Sharda` | Mahakali = upper Nepal border reach; Sharda = Indian plains reach — kept separate |
| `Mathabanga` / `Mathavanga` | **Merged**: same West Bengal Ganga distributary, variant OSM spelling |

---

## 6. Hydrological Decisions on Basin Scope

### Delta rivers — what to include

| River | Decision | Rationale |
|---|---|---|
| **Padma** | ✓ Include | Direct downstream continuation of Ganga after Farakka Barrage; needed to show flow to Bay of Bengal |
| **Hooghly** | ✓ Include | Ganga's western distributary through Kolkata; part of the same delta outlet |
| **Meghna** | ✓ Include (if present) | Final outlet channel; Padma joins Jamuna to form Meghna |
| **Brahmaputra/Jamuna** | ✗ Exclude | Separate river basin — joins only at the delta, not part of Ganga drainage |
| **Bangladesh Jamuna** | ✗ Exclude | Same as Brahmaputra — distinct basin |

*Note: The OSM file covers India only. Padma and Meghna segments are assigned via geographic fallback (Step 2b), not OSM matching.*

### Nepal border rivers

| River | Decision |
|---|---|
| `Kosi` headwaters (Nepal) | Included — mizuRoute dataset includes cross-border headwaters |
| `Gandak` / `Narayani` | Included under `Gandak` |
| `Mahakali` | Kept as distinct name from `Sharda` |
| Tibetan streams (`多隆曲` etc.) | Excluded — outside basin scope |

### Zero-elevation boundary segments

The 7 segments with `TopElev=0 AND BotElev=0` in the original dataset are in the Bangladesh delta (89–90°E, 22–23°N). These are **not** data quality errors — they are genuine sea-level delta channels. They were retained rather than dropped, because removing them would leave the network disconnected from the Bay of Bengal.

Only truly isolated segments (no topological connections at either endpoint) are dropped.

---

## 7. Confluence Reference Coordinates

Real-world confluence coordinates are hardcoded from geographic knowledge, **not** derived from shapefile endpoints (which was the source of errors in the original processing). The `dist_m` field records the distance from the nearest segment endpoint to the real-world coordinate — values above 10,000 m indicate dataset truncation.

| River | Confluences into | Place | Lon | Lat |
|---|---|---|---|---|
| Bhagirathi | Ganga | Devprayag | 78.6026 | 30.1441 |
| Alaknanda | Ganga | Devprayag | 78.6026 | 30.1441 |
| Mandakini | Alaknanda | Rudraprayag | 78.9817 | 30.2847 |
| Yamuna | Ganga | Prayagraj | 81.8847 | 25.4304 |
| Tons | Yamuna | Prayagraj | 81.8800 | 25.4250 |
| Chambal | Yamuna | Etawah | 79.0163 | 26.5800 |
| Betwa | Yamuna | Hamirpur | 80.1437 | 25.9490 |
| Ken | Yamuna | Banda | 80.3521 | 25.5133 |
| Sindh | Yamuna | Mainpuri | 79.2500 | 26.4000 |
| Hindon | Yamuna | Ghaziabad | 77.7000 | 28.7000 |
| Kali Sindh | Chambal | Anta | 76.3700 | 25.1500 |
| Gomti | Ganga | Ghazipur | 83.5804 | 25.5772 |
| Ghaghara | Ganga | Chhapra | 84.7467 | 25.7789 |
| Sharda | Ghaghara | Brahmaghat | 81.3500 | 27.9500 |
| Sarju | Ghaghara | Ayodhya | 82.1964 | 26.7847 |
| Rapti | Ghaghara | Barhaj | 83.7283 | 26.3083 |
| Rohini | Rapti | Gorakhpur | 83.3700 | 26.7800 |
| Ramganga | Ganga | Kannauj | 79.9080 | 27.0551 |
| Gaula | Ramganga | Kalagarh | 78.9800 | 29.5000 |
| Son | Ganga | Koilwar | 84.7739 | 25.5633 |
| Rihand | Son | Singrauli | 82.7500 | 24.2000 |
| Koel | Son | Medininagar | 84.0750 | 23.9000 |
| Gandak | Ganga | Hajipur | 85.2167 | 25.6940 |
| Gerua | Gandak | Gopalganj | 84.3058 | 26.4908 |
| Bagmati | Ganga | Rosera | 85.9975 | 25.8689 |
| Burhi Gandak | Ganga | Rosera | 86.0000 | 25.8700 |
| Kosi | Ganga | Kursela | 87.2204 | 25.4412 |
| Kamala | Kosi | Supaul | 86.6000 | 26.1200 |
| Mechi | Mahananda | Siliguri | 88.1350 | 26.6117 |
| Mahananda | Ganga | Manikchak | 87.9197 | 25.1687 |
| Karmanasa | Ganga | Chandauli | 83.8667 | 25.2833 |
| Punpun | Ganga | Masaurhi | 85.0700 | 25.5550 |
| Hooghly | Bay of Bengal | Sagar Island | 88.0500 | 21.6500 |
| Padma | Bay of Bengal | Meghna mouth | 90.6000 | 22.3000 |

---

## 8. Known Remaining Limitations

### Dataset truncations

Several rivers in the mizuRoute dataset end well before their real-world confluence. The `dist_m` values in the confluence CSV quantify this. Rivers with known truncations:

| River | Approx. gap | Real confluence |
|---|---|---|
| Yamuna | 65 km | Triveni Sangam, Prayagraj |
| Sarju | 364 km | Ghaghara near Ayodhya |
| Mahananda | 38 km | Ganga near Manikchak |
| Gomti | 35 km | Ganga near Ghazipur |
| Kosi | Truncated above Bihar plains | Ganga at Kursela |

These are data gaps in the upstream mizuRoute delineation, not naming errors.

### Bhagirathi segment count

Bhagirathi shows only 4 segments despite being a major river. This is a BFS propagation issue — a topological gap in the network prevents tier assignment from reaching all Bhagirathi segments. The segments are correctly named; there are simply fewer than expected.

### OSM coverage variability

OSM completeness varies by region. Minor streams in remote Uttarakhand headwaters and Bihar/Nepal border areas may be absent or poorly mapped, resulting in those segments being dropped (no name assigned) rather than mislabeled.

---

## 9. Iterative Audit Log

The name list was audited over 7 rounds against hydrological knowledge of the Ganga basin. Each round identified new issues fixed in the subsequent script version:

| Round | Rivers before | Rivers after | Key changes |
|---|---|---|---|
| 1 (baseline) | 414 | — | Centroid matching identified as root cause |
| 2 | 414 | 405 | Added NAME_CANON, EXCLUDE_NAMES, first duplicate fixes |
| 3 | 405 | 391 | Rind→Rihand, Sipra→Shipra, Vaisli→Vaisali, Mohana lowercase, 12 noise drops |
| 4 | 391 | 385 | Step 3b added, canonise() fully applied to propagated names, 11 noise drops |
| 5 | 385 | ~370 | Non-Latin script handling, SCRIPT_TO_ENGLISH table, Kosi all-merge decision |
| 6 | ~370 | ~365 | Gomti/Gomtī/Gomati collapsed, suffix stripping expanded, `tributary` added |
| 7 | 365 | — | Mathabanga/Mathavanga merged, Bandhavgarh NP / Jogbuda / Pagla dropped |
