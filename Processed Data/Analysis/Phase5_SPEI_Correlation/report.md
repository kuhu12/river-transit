# Phase 5: SPEI Drought Correlation with Streamflow Drying

## Process

### Data Source

- **SPEI Dataset**: Gridded SPEI (Standardised Precipitation-Evapotranspiration Index) at 0.05° x 0.05° resolution (~5.5 km)
- **Timescale used**: SPEI-12 (12-month accumulation) — the standard for hydrological drought assessment
- **Temporal coverage**: 1901–2021 (monthly); subset to 1951–2021 to match streamflow record
- **Spatial coverage**: 113,551 grid cells across India; ~38,000–44,000 cells covering the Ganga basin

### Step 5A: Grid-to-Segment Assignment

Each of the 1526 river segments was mapped to its nearest SPEI grid cell by snapping the segment centroid to the 0.05° grid. This produced **1496 unique grid cells** to load. 84 cells had no matching SPEI file (likely at basin edges over water bodies).

### Step 5B: SPEI Trend Computation

For each segment's assigned SPEI-12 time series:

1. Monthly SPEI-12 values aggregated to **annual mean SPEI-12** per year
2. **Mann-Kendall trend test** applied to the 71-year annual series (1951–2021)
3. **Early (1951–1990) vs Late (1991–2021) mean** computed for period comparison
4. Segments classified as **SPEI Drying** (decreasing, p <= 0.05), **SPEI Wetting** (increasing, p <= 0.05), or **SPEI Stable** (p > 0.05)

### Step 5C: Cross-Tabulation

SPEI classification cross-tabulated with A6 streamflow classification (Drying/Stable/Wetting) to identify:

- **Climate-aligned drying**: both SPEI and streamflow declining
- **Non-climate drying**: streamflow declining but SPEI stable/wetting — points to human factors (dams, abstraction, land use)
- **Resilient segments**: SPEI drying but streamflow stable

### Step 5D: Correlation Analysis

Spearman rank correlations computed between SPEI-12 trend (tau) and streamflow trend metrics at segment, river, and tier levels.

### Step 5E: Drought Year Concordance

For each segment's year of minimum flow (from B2), checked whether that year also had SPEI-12 < -0.5 (moderate meteorological drought). Tests whether flow minima are drought-driven or independent.

---

## Results

### 5A–5B: SPEI Classification Summary

Of 1526 segments, **1441** have SPEI data (85 at basin edges without coverage).

| SPEI-12 Category | Segments | % |
|------------------|----------|---|
| SPEI Drying (decreasing, p <= 0.05) | 514 | 35.7% |
| SPEI Stable (no significant trend) | 915 | 63.5% |
| SPEI Wetting (increasing, p <= 0.05) | 12 | 0.8% |

### SPEI-12 Early vs Late Period

- Basin-wide mean SPEI-12 (1951–1990): **0.061**
- Basin-wide mean SPEI-12 (1991–2021): **-0.171**
- Difference (late - early): **-0.232**
- The Ganga basin has become **meteorologically drier** in the recent period

### 5C: SPEI vs Streamflow Classification Cross-Tabulation

| SPEI Category | Drying | Stable | Wetting | Total |
|---|---|---|---|---|
| SPEI Drying | 355 | 159 | 0 | 514 |
| SPEI Stable | 150 | 756 | 9 | 915 |
| SPEI Wetting | 0 | 12 | 0 | 12 |
| **Total** | **505** | **927** | **9** | **1441** |

### Cross-Tab Interpretation

- Of **505 streamflow-drying segments** with SPEI data:
  - **355 (70%)** are in SPEI-drying areas (climate-aligned)
  - **150 (30%)** are in SPEI-stable or SPEI-wetting areas (non-climate drying)
  - **159 of 514 SPEI-drying segments (31%)** maintain stable streamflow (resilient to drought)

### 5D: Correlation Analysis

#### Segment-Level Correlations

| Variables | r | p-value | N | Interpretation |
|-----------|---|---------|---|----------------|
| spei12_tau vs tau (all) | 0.620 | 0.0000 * | 1441 | Strong |
| spei12_tau vs pct_change (all) | 0.582 | 0.0000 * | 1441 | Strong |
| spei12_tau vs norm_slope_pct_yr (all) | 0.619 | 0.0000 * | 1441 | Strong |

#### By River Tier

| Group | Variables | r | p-value | N |
|-------|-----------|---|---------|---|
| Mainstem (T1-2): spei_tau vs tau | | 0.519 | 0.0000 * | 485 |
| Mainstem (T1-2): spei_tau vs pct_change | | 0.478 | 0.0000 * | 485 |
| Tributary (T3-5): spei_tau vs tau | | 0.656 | 0.0000 * | 956 |
| Tributary (T3-5): spei_tau vs pct_change | | 0.622 | 0.0000 * | 956 |

#### By River (top 20 by segment count)

| River | Segments | Mean SPEI Tau | Mean Flow Tau | SPEI-Flow Corr | % Drying |
|-------|----------|---------------|---------------|----------------|----------|
| Ganga | 108 | -0.1382 | -0.1527 | 0.515 | 53.7% |
| Yamuna | 53 | -0.2154 | -0.2505 | 0.185 | 86.8% |
| Ghaghara | 39 | -0.1633 | -0.1646 | 0.271 | 66.7% |
| Son | 33 | -0.1272 | 0.0425 | 0.208 | 0.0% |
| Banas | 29 | -0.0539 | 0.0196 | 0.776 | 0.0% |
| Betwa | 29 | -0.1306 | -0.1274 | 0.828 | 41.4% |
| Kosi | 29 | -0.0357 | -0.0111 | 0.616 | 13.8% |
| Chambal | 28 | 0.0399 | 0.0101 | 0.390 | 0.0% |
| Gomti | 28 | -0.1613 | -0.1801 | 0.121 | 71.4% |
| Parbati | 24 | -0.0370 | -0.0979 | 0.490 | 20.8% |
| Khari | 23 | -0.0328 | -0.0830 | 0.770 | 43.5% |
| Burhi Gandak | 20 | -0.0064 | 0.0294 | -0.022 | 0.0% |
| Sai | 20 | -0.3216 | -0.3095 | 0.534 | 100.0% |
| Tons | 20 | -0.1784 | 0.0229 | 0.282 | 15.0% |
| Ramganga | 18 | -0.2828 | -0.2798 | -0.355 | 88.9% |
| Kali | 17 | -0.2827 | -0.2597 | 0.715 | 82.4% |
| Sindh | 17 | -0.1610 | -0.1381 | 0.836 | 35.3% |
| Berach | 15 | -0.0049 | 0.0318 | 0.436 | 0.0% |
| Dhasan | 15 | -0.2512 | -0.2226 | 0.640 | 100.0% |
| Ken | 15 | -0.1878 | -0.1377 | 0.211 | 33.3% |

### 5E: Drought Year Concordance

Checked **1385 segments** where both the year of minimum flow and SPEI data are available.

- **1161 (83.8%)** had SPEI-12 < -0.5 in their minimum-flow year (drought-concordant)
- **224 (16.2%)** had SPEI-12 >= -0.5 (minimum flow not explained by meteorological drought)

### SPEI × Dam Interaction

Cross-referencing with Phase 4 dam data to separate climate from dam effects.

| Group | Segments | % Streamflow Drying | Mean SPEI Tau | Mean SPEI Diff |
|-------|----------|---------------------|---------------|----------------|
| With upstream dams + SPEI Drying | 223 | 65.9% | -0.2382 | -0.4536 |
| With upstream dams + SPEI Stable | 515 | 15.9% | -0.0374 | -0.0585 |
| With upstream dams + SPEI Wetting | 6 | 0.0% | 0.1787 | +0.5444 |
| No upstream dams + SPEI Drying | 291 | 71.5% | -0.2795 | -0.5963 |
| No upstream dams + SPEI Stable | 400 | 17.0% | -0.0331 | -0.0892 |
| No upstream dams + SPEI Wetting | 6 | 0.0% | 0.1726 | +0.6033 |

## Key Findings

1. **514 of 1441 segments (35.7%)** show significant SPEI-12 drying trend (p <= 0.05). The basin is experiencing real meteorological drying, but not uniformly.
2. **30% of streamflow-drying segments** (150/505) are in areas with stable or increasing SPEI — their drying cannot be explained by precipitation decline alone.
3. Only **83.8%** of minimum-flow years coincide with meteorological drought, suggesting many flow minima are driven by non-climatic factors or lagged responses.
4. Segment-level SPEI-flow correlation is **r = 0.620** — moderate, confirming climate is a factor but not the sole driver.

## Limitations

- SPEI and VIC/mizuRoute streamflow both derive from similar climate forcing (precipitation, temperature). A positive correlation partly reflects shared inputs rather than independent confirmation.
- SPEI-12 measures meteorological drought at a single grid point; streamflow integrates conditions across the entire upstream catchment area.
- Grid-to-segment assignment uses segment centroid, not catchment-average SPEI. For large catchments, the centroid SPEI may not represent headwater conditions.
- SPEI does not account for snowmelt (relevant for Himalayan headwaters), groundwater, or irrigation return flows.
- Concordance threshold (SPEI-12 < -0.5) is somewhat arbitrary; stricter thresholds would lower concordance further.
