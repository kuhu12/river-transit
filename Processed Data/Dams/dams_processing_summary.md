# Dams Processing Summary

**Data Provenance Report**
Generated: 2026-05-18
Project: VizChitra - Ganga Basin Visualization

---

## 1. Source Data

| Attribute | Value |
|-----------|-------|
| **Source File** | `india_dams_detail.json` |
| **Location** | `Processed Data/Dams/` |
| **Total Records** | 4,986 dams |
| **Coverage** | All major river basins in India |

### Fields Available

| Field | Description | Units |
|-------|-------------|-------|
| Name | Dam name | text |
| Latitude | Geographic latitude | degrees |
| Longitude | Geographic longitude | degrees |
| Address | Location description | text |
| State | Indian state | text |
| RiverBasin | Major river basin | text |
| HaLF | Height above lowest foundation | meters |
| DamLength | Length of dam structure | meters |
| GrossStorageCapacity | Total reservoir capacity | cubic meters |
| ReservoirArea | Surface area of reservoir | square meters |
| EffectiveStorageCapacity | Usable storage capacity | cubic meters |

---

## 2. Step-by-Step Processing

### Step 1: Basin Filtering
- **Input:** 4,986 dams (india_dams_detail.json)
- **Filter:** `RiverBasin == 'Ganga'`
- **Output:** 781 dams
- **File:** `ganga_dams_detail.json`

### Step 2: Spatial Snapping
- **Input:** 781 dams
- **Process:** Snap each dam to nearest river segment in `ganga_rivers_named.shp` using Latitude/Longitude
- **Max Snap Distance:** 0.2 degrees
- **Output:** 772 dams snapped, 9 dams unsnapped (no segment within threshold)
- **Fields Added:** `river_name`, `seg_id`

Unsnapped dams (no river segment within 0.2 degrees):
- Baranadi Dam, Kairabani Dam, Burhi Dam, Punasi Dam, Torai Dam (Jharkhand)
- Massanjore Dam, Hinglow Dam, Nachan Dam, Bara Mandira Dam (West Bengal)

### Step 3: Target River Filtering
- **Input:** 772 dams with river assignments
- **Filter:** Keep only dams on 33 target rivers
- **Output:** 772 dams (all snapped dams were on target rivers)

**33 Target Rivers:**
Ganga, Yamuna, Chambal, Betwa, Ken, Son, Tons, Sindh, Kali Sindh, Hindon, Karmanasa, Punpun, North Koel, Sharda, Ghaghara, Rapti, Sarju, Ramganga, Gomti, Gandak, Burhi Gandak, Bagmati, Kamala, Kosi, Mechi, Mahananda, Gaula, Rihand, Gerua, Rohini, Mandakini, Bhagirathi, Alaknanda

### Step 4: Hydrological Verification
- **Input:** 772 dams
- **Process:** Manual review of river assignments using coordinate analysis
- **Output:** 769 dams
- **File:** `ganga_dams_verified.json`

**Corrections Applied:**

| Issue | Dams Affected | Action |
|-------|---------------|--------|
| **Tons River Ambiguity** | 41 dams | Dams at 24-25°N, 80-83°E reassigned from Tons to Son (these are on Son-tributary Tons, not Yamuna-tributary Tons) |
| **Ichari Dam Misassignment** | 1 dam | Reassigned from Yamuna to Tons (Uttarakhand tributary at 30.61°N, 77.79°E) |
| **Outside Basin Dams** | 3 dams | Removed Buchara Dam (27.59°N, 75.97°E), Chandrana Dam (76.23°E), Chittoli Dam (76.13°E) — coordinates outside Yamuna basin |
| **Tehri Dam Misassignment** | 1 dam | Reassigned from Ganga to Bhagirathi (Tehri is on Bhagirathi before confluence) |

### Step 5: Scoring System
- **Input:** 769 verified dams
- **Process:** Apply multi-criteria scoring (see Section 3 for details)
- **Minimum Eligibility:** Score > 5 (before river bonus)

### Step 6: Distribution Rules & Final Selection
- **Input:** Eligible scored dams
- **Rules Applied:**
  - Chambal, Son, Betwa: Top 4 dams per river
  - All other rivers: Top 2 dams per river
  - Minimum score threshold: > 5
- **Output:** 47 dams across 21 rivers
- **File:** `ganga_dams_final_scored.csv`

### Step 7: Year of Construction Enrichment
- **Input:** 47 scored dams
- **Process:** Research-based enrichment of construction/completion years
- **Sources Used:** Historical records, CWC data, NHPC/NTPC project documentation
- **Output:** 47 dams with year_constructed and year_confidence fields
- **File:** `ganga_dams_final_complete.csv`

**Confidence Levels:**

| Level | Definition | Count |
|-------|------------|------:|
| `certain` | Well-documented major dam with verified completion year | 17 |
| `approximate` | Known decade but exact year needs verification | 20 |
| `unknown` | No reliable information found or under construction | 10 |

**Construction Era Distribution:**

| Era | Dams | Notable Examples |
|-----|-----:|------------------|
| Pre-1900 | 1 | Rajsamand (1660) |
| 1900–1950 | 2 | Gangau (1915), Sarda Sagar (1928) |
| 1950–1970 | 5 | Matatila (1958), Gandhi Sagar (1960), Rihand (1962) |
| 1970–1990 | 15 | Ram Ganga (1974), Ichari (1972), Maneri (1984) |
| 1990–2010 | 5 | Rajghat (1999), Bisalpur (1999), Tehri (2006) |
| 2010–present | 4 | Srinagar (2015), Kalisindh (~2012), Meja (~2019) |
| Unknown/Under construction | 10 | Lakhwar, North Koel, Jamrani |

---

## 3. Scoring Criteria

**Total Score = Base Score (max 100) + River Importance Bonus (max 15)**

### Base Score Components

#### GrossStorageCapacity (40 points max)

| Threshold | Points |
|-----------|-------:|
| ≥ 1,000,000,000 m³ (1 BCM) | 40 |
| 500,000,000 – 999,999,999 m³ | 25 |
| 100,000,000 – 499,999,999 m³ | 10 |
| < 100,000,000 m³ | 0 |

#### HaLF Height (25 points max)

| Threshold | Points |
|-----------|-------:|
| ≥ 50 m | 25 |
| 30 – 49 m | 15 |
| 15 – 29 m | 5 |
| < 15 m | 0 |

#### DamLength (20 points max)

| Threshold | Points |
|-----------|-------:|
| ≥ 1,000 m | 20 |
| 500 – 999 m | 12 |
| 200 – 499 m | 5 |
| < 200 m | 0 |

#### ReservoirArea (15 points max)

| Threshold | Points |
|-----------|-------:|
| ≥ 100,000,000 m² | 15 |
| 50,000,000 – 99,999,999 m² | 8 |
| 10,000,000 – 49,999,999 m² | 3 |
| < 10,000,000 m² | 0 |

### River Importance Bonus

| River Category | Bonus Points |
|----------------|-------------:|
| Bhagirathi (controls upper Ganga) | +15 |
| Ganga, Yamuna, Ghaghara, Son, Kosi, Gandak | +10 |
| Chambal, Betwa, Ken, Rihand, Ramganga | +5 |
| All other rivers | 0 |

---

## 4. Final Dam List

**47 dams selected for installation, sorted by year of construction:**

| Name | River | Year | Confidence | Score | Storage (BCM) | HaLF (m) |
|------|-------|-----:|------------|------:|--------------:|---------:|
| Rajsamand Dam | Chambal | 1660 | certain | 53 | 0.107 | 39.2 |
| Gangau Dam | Ken | 1915 | certain | 37 | 0.058 | 16.2 |
| Sarda Sagar Dam | Sharda | 1928 | certain | 43 | 0.406 | 16.2 |
| Matatila Dam | Betwa | 1958 | certain | 95 | 1.132 | 45.7 |
| Gandhi Sagar Dam | Chambal | 1960 | certain | 50 | 0.073 | 62.2 |
| Rihand Dam | Rihand | 1962 | certain | 97 | 10.600 | 91.5 |
| Obra Dam | Rihand | 1968 | certain | 40 | 0.211 | 29.2 |
| Ranapratap Sager Dam | Chambal | 1970 | certain | 60 | 2.899 | N/A |
| Mohini Pick-Up Weir Dam | Sindh | ~1970 | approximate | 48 | 0.109 | 36.0 |
| Ichari Dam | Tons | 1972 | certain | 25 | 0.009 | 59.3 |
| Ram Ganga Dam | Ganga | 1974 | certain | 65 | 0.245 | 128.0 |
| Kaliasote Dam | Betwa | ~1975 | approximate | 50 | 0.036 | 67.1 |
| Kaketo Dam | Sindh | ~1975 | approximate | 35 | 0.081 | 37.6 |
| Chandan Dam | Gerua | ~1976 | approximate | 48 | 0.157 | 49.4 |
| Durgawati Dam | Karmanasa | ~1976 | approximate | 48 | 0.288 | 46.3 |
| Chhilar Dam | Kali Sindh | ~1978 | approximate | 35 | 0.035 | 30.5 |
| Moosakhand Dam | Karmanasa | ~1980 | approximate | 60 | 0.156 | 33.5 |
| Upper Khajuri Dam | Son | ~1980 | approximate | 50 | 0.045 | 24.9 |
| Bour Dam | Gaula | ~1980 | approximate | 38 | 0.103 | 18.0 |
| Shahjad Dam | Betwa | ~1982 | approximate | 55 | 0.130 | 18.0 |
| Sunder Dam | Gerua | ~1982 | approximate | 35 | 0.031 | 35.7 |
| Maneri Stage I Dam | Bhagirathi | 1984 | certain | 30 | 0.001 | 39.0 |
| Maudaha Dam | Yamuna | ~1985 | approximate | 63 | 0.200 | 32.6 |
| Adwa Dam | Son | ~1985 | approximate | 50 | 0.088 | 20.5 |
| Amanat Dam | North Koel | ~1985 | approximate | 37 | 0.107 | 41.0 |
| Dongia Dam | Tons | ~1985 | approximate | 33 | 0.028 | 15.3 |
| Rangawan Dam | Ken | ~1988 | approximate | 43 | 0.164 | 27.4 |
| Batane Dam | Punpun | ~1990 | approximate | 53 | 0.679 | 24.1 |
| Rajghat Dam | Betwa | 1999 | certain | 95 | 2.172 | 43.5 |
| Bisalpur Dam | Chambal | 1999 | certain | 87 | 1.096 | 39.5 |
| Dhauliganga Dam | Sharda | 2005 | certain | 30 | 0.006 | 56.0 |
| Tehri Dam | Bhagirathi | 2006 | certain | 100 | 3.540 | 260.5 |
| Ban Sagar Dam | Son | 2006 | certain | 95 | 6.370 | 67.5 |
| Kalisindh Dam Phase-I Dam | Kali Sindh | ~2012 | approximate | 70 | 5.437 | 30.5 |
| Srinagar Dam | Alaknanda | 2015 | certain | 30 | 0.078 | 90.0 |
| Meja Dam | Son | ~2019 | approximate | 58 | 0.303 | 40.0 |
| Vishnu Gad Pipalkoti Hep Dam | Alaknanda | ~2023 | approximate | 35 | 0.363 | 65.0 |
| Lakhwar (Ujvn) Dam | Yamuna | N/A | unknown | 65 | 0.580 | 204.0 |
| North Koel Dam | Punpun | N/A | unknown | 63 | 0.702 | 67.9 |
| Chittaurgarh Dam | Ghaghara | N/A | unknown | 60 | 0.425 | 15.3 |
| Dhandhraul Dam | Ganga | N/A | unknown | 60 | 0.144 | 21.0 |
| Girgitahi Dam | Ghaghara | N/A | unknown | 50 | 0.009 | 15.2 |
| Jamrani Phase I Dam | Gaula | N/A | unknown | 40 | 0.207 | 130.6 |
| Malay Dam | North Koel | N/A | unknown | 25 | N/A | 28.8 |
| Banghoghwa Dam | Rapti | N/A | unknown | 23 | 0.003 | 14.1 |
| Rambara Dam | Mandakini | N/A | unknown | 23 | 0.000 | 31.0 |
| Khairman Dam | Rapti | N/A | unknown | 20 | 0.005 | 10.6 |

---

## 5. River Coverage Analysis

### Rivers Represented in Final Selection (21 rivers)

| River | Dam Count | | River | Dam Count |
|-------|----------:|-|-------|----------:|
| Son | 4 | | Betwa | 4 |
| Chambal | 4 | | Bhagirathi | 2 |
| Rihand | 2 | | Kali Sindh | 2 |
| Yamuna | 2 | | Ganga | 2 |
| Punpun | 2 | | Ghaghara | 2 |
| Karmanasa | 2 | | Gerua | 2 |
| Sindh | 2 | | Ken | 2 |
| Sharda | 2 | | Gaula | 2 |
| North Koel | 2 | | Alaknanda | 2 |
| Tons | 2 | | Rapti | 2 |
| Mandakini | 1 | | | |

### Rivers Without Dam Data (12 rivers)

| River | Likely Reason |
|-------|---------------|
| Gomti | Primarily groundwater-fed; no major dams in source data |
| Gandak | Major portion in Nepal; Indian dams not in dataset |
| Burhi Gandak | Smaller river; no significant dams meeting criteria |
| Bagmati | Originates in Nepal; limited Indian dam infrastructure |
| Kamala | Nepal-origin river; limited Indian data |
| Kosi | Major dams (Kosi Barrage) in Nepal or classified under different basin |
| Mechi | Small border river with Nepal |
| Mahananda | No dams meeting minimum score threshold |
| Hindon | Highly polluted urban river; limited dam infrastructure |
| Sarju | May be classified under Ghaghara in source data |
| Ramganga | Dams may be classified under Ganga main stem |
| Rohini | Small tributary; no significant dams |

---

## 6. Known Data Quality Issues

### Flagged for Verification

| Issue | Details | Recommended Action |
|-------|---------|-------------------|
| **Kalisindh Dam Capacity** | Listed as 5.437 BCM — exceptionally high for this river | Verify against Central Water Commission or state records |
| **Ranapratap Sager HaLF** | Missing height data (N/A) | Cross-reference with NRLD database |
| **Malay Dam Capacity** | Missing GrossStorageCapacity | Verify dam completion status |
| **Units Consistency** | GrossStorageCapacity and EffectiveStorageCapacity assumed in cubic meters | Confirm units with source documentation |

### Structural Data Issues

| Issue | Impact | Mitigation |
|-------|--------|------------|
| **Tons River Ambiguity** | Two rivers named "Tons" in Ganga basin (Yamuna tributary in Uttarakhand, Son tributary in MP) | Resolved via coordinate-based reassignment |
| **ReadMore Field** | Contains JavaScript require() statements — not valid JSON | Replaced with null during parsing |
| **Duplicate Dam Names** | Some dam names appear multiple times (e.g., Deori Dam) | Differentiated by coordinates and river assignment |

### Coverage Gaps

- **Nepal-origin rivers:** Kosi, Gandak, Bagmati, Kamala — major infrastructure on Nepal side not captured
- **Barrages vs Dams:** Some water control structures may be classified as barrages and excluded from dam dataset
- **Recent constructions:** Dataset may not include dams completed after source compilation date

---

## 7. Year Verification Requirements

**30 dams require manual verification of construction year.**

### Recommended Verification Sources:
- **NRLD** — National Register of Large Dams (India)
- **GRanD** — Global Reservoir and Dam Database
- **CWC** — Central Water Commission records
- **India-WRIS** — Water Resources Information System

### Dams Flagged for Verification:

| Dam | River | Current Year | Issue |
|-----|-------|--------------|-------|
| Lakhwar (Ujvn) Dam | Yamuna | Unknown | Under construction — verify status |
| North Koel Dam | Punpun | Unknown | Long-delayed project — verify completion |
| Jamrani Phase I Dam | Gaula | Unknown | Under construction — verify status |
| Chittaurgarh Dam | Ghaghara | Unknown | Verify against UP irrigation records |
| Dhandhraul Dam | Ganga | Unknown | Verify against Bihar/Jharkhand records |
| Girgitahi Dam | Ghaghara | Unknown | Small dam — limited documentation |
| Malay Dam | North Koel | Unknown | Missing capacity — verify all fields |
| Banghoghwa Dam | Rapti | Unknown | Small dam — limited documentation |
| Rambara Dam | Mandakini | Unknown | Small HEP — recent project |
| Khairman Dam | Rapti | Unknown | Small dam — limited documentation |
| Kalisindh Dam Phase-I Dam | Kali Sindh | ~2012 | Large project — verify exact year |
| Meja Dam | Son | ~2019 | Recent thermal project — verify year |
| Vishnu Gad Pipalkoti Hep Dam | Alaknanda | ~2023 | Recent/ongoing — verify status |

---

## 8. Files Produced

| File | Location | Format | Records | Purpose |
|------|----------|--------|--------:|---------|
| `india_dams_detail.json` | Processed Data/Dams/ | JSON | 4,986 | Source data (all India) |
| `ganga_dams_detail.json` | Processed Data/Dams/ | JSON | 769 | Basin-filtered and verified dams |
| `ganga_dams_final_scored.csv` | Processed Data/Dams/ | CSV | 47 | Scored selection (without year) |
| `ganga_dams_final_complete.csv` | Processed Data/Dams/ | CSV | 47 | **Final output** with year of construction |
| `dams_processing_summary.md` | Processed Data/Dams/ | Markdown | — | This provenance document |

---

## Processing Pipeline Summary

```
india_dams_detail.json (4,986)
         │
         ▼ Step 1: Filter RiverBasin == 'Ganga'
         │
         ▼ Step 2: Spatial snap to ganga_rivers_named.shp
         │
         ▼ Step 3: Filter to 33 target rivers
         │
         ▼ Step 4: Hydrological verification
ganga_dams_detail.json (769)
         │
         ▼ Step 5: Multi-criteria scoring
         │
         ▼ Step 6: Distribution rules (top N per river)
ganga_dams_final_scored.csv (47)
         │
         ▼ Step 7: Year of construction enrichment
ganga_dams_final_complete.csv (47 dams, 21 rivers)
```

---

*Report generated as part of VizChitra Ganga Basin visualization project*
