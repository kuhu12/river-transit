# Phase 3: Before/After Dam Construction Streamflow Analysis

## 3A: Per-Segment Before/After Results

Analysed **492 segments** where the first upstream dam was built within the 1951–2022 streamflow record (with at least 6 years before and 6 years after).

| Dam Effect | Segments | % |
|------------|----------|---|
| Significant Reduction | 102 | 20.7% |
| Significant Increase | 7 | 1.4% |
| No Significant Change | 383 | 77.8% |

- Segments with significant reduction: mean change = **-26.3%**, median = **-25.6%**
- Segments with significant increase: mean change = **+67.5%**, median = **+42.0%**

## 3B: Dam Effect vs A6 Classification

This table shows whether dam-induced flow changes align with the long-term trend classification.

| A6 Category | No Significant Change | Significant Increase | Significant Reduction | Total |
|---|---|---|---|---|
| Drying | 53 | 0 | 79 | 132 |
| Stable | 327 | 6 | 23 | 356 |
| Wetting | 3 | 1 | 0 | 4 |
| **Total** | **383** | **7** | **102** | **492** |

- Of **132 drying segments** with upstream dams, **79 (60%)** show significant post-dam reduction
- Of **356 stable segments** with upstream dams, **327 (92%)** show no significant dam-related change

## 3C: Dam Impact by Decade of Construction

| Decade | Segments | Mean % Change | Median % Change | % Sig. Reduction | % Sig. Increase |
|--------|----------|---------------|-----------------|------------------|-----------------|
| 1950s | 45 | -5.2% | -7.3% | 0% | 0% |
| 1960s | 136 | -14.0% | -13.4% | 34% | 1% |
| 1970s | 88 | -4.0% | -2.6% | 19% | 0% |
| 1980s | 101 | -4.3% | -2.1% | 12% | 0% |
| 1990s | 34 | -5.3% | -6.3% | 38% | 0% |
| 2000s | 63 | -3.7% | -8.6% | 22% | 3% |
| 2010s | 25 | 27.1% | 20.1% | 0% | 16% |

## 3D: Capacity-Weighted Analysis (Regulation Index)

Regulation Index = total upstream capacity (MCM) / mean annual flow volume (MCM).
Values > 1.0 mean the upstream dams can store more than a full year's flow.

| Regulation Index | Segments | Mean % Change | Median % Change | % Drying (A6) |
|-----------------|----------|---------------|-----------------|---------------|
| < 0.01 | 68 | -0.4% | -7.3% | 15% |
| 0.01–0.1 | 182 | -5.8% | -6.5% | 27% |
| 0.1–0.5 | 202 | -6.9% | -10.5% | 34% |
| 0.5–1.0 | 30 | -5.3% | -2.3% | 10% |
| > 1.0 | 9 | -2.8% | 4.8% | 22% |

**Correlation** between regulation index and post-dam % change: **r = -0.009**
(Negligible linear relationship)

## 3E: Control Group — Dammed vs Dam-Free Segments

Comparison of ALL 1526 segments split by whether they have upstream dams.

| Group | Segments | % Drying | % Stable | % Wetting | Mean % Change | Median % Change |
|-------|----------|----------|----------|-----------|---------------|-----------------|
| With upstream dams | 750 | 31.1% | 68.4% | 0.5% | -4.43% | -6.07% |
| No upstream dams | 776 | 36.6% | 61.9% | 1.5% | -11.84% | -9.6% |

> Segments with upstream dams have a **lower drying rate (31.1%)** than dam-free segments (36.6%). This suggests climate/land-use factors dominate over dam effects in driving streamflow trends.

## Top 15 Segments with Largest Post-Dam Flow Reduction

| seg_id | River | Dam Year | N Dams | Capacity (MCM) | Pre Mean (m³/s) | Post Mean (m³/s) | % Change | A6 Category |
|--------|-------|----------|--------|----------------|-----------------|------------------|----------|-------------|
| 3882 | Ramganga | 1968 | 3 | 300 | 89.01 | 47.90 | -46.2% | Drying |
| 3363 | Pahuj | 1985 | 2 | 5 | 9.51 | 5.15 | -45.9% | Drying |
| 3883 | Ramganga | 1968 | 3 | 300 | 135.53 | 76.34 | -43.7% | Drying |
| 3884 | Dhela | 1961 | 2 | 172 | 20.70 | 12.07 | -41.7% | Drying |
| 3381 | Pahuj | 1985 | 6 | 82 | 43.75 | 26.15 | -40.2% | Drying |
| 3873 | Ramganga | 1961 | 5 | 472 | 158.77 | 95.37 | -39.9% | Drying |
| 3369 | Parbati | 1963 | 1 | 115 | 14.78 | 8.92 | -39.6% | Drying |
| 3358 | Parbati | 1963 | 1 | 115 | 4.08 | 2.54 | -37.7% | Drying |
| 3364 | Parbati | 1963 | 1 | 115 | 7.28 | 4.56 | -37.4% | Drying |
| 3287 | Yamuna | 1972 | 3 | 603 | 201.33 | 126.54 | -37.1% | Drying |
| 3947 | Ganga | 1992 | 1 | 1 | 11.69 | 7.35 | -37.1% | Drying |
| 3476 | Non | 1976 | 1 | 4 | 4.17 | 2.63 | -37.1% | Drying |
| 3288 | Yamuna | 1972 | 3 | 603 | 228.45 | 143.94 | -37.0% | Drying |
| 3385 | Gambhir | 1963 | 5 | 373 | 53.93 | 33.99 | -37.0% | Drying |
| 3874 | Ramganga | 1961 | 5 | 472 | 189.05 | 119.60 | -36.7% | Drying |

## Limitations

- Before/after split uses the **first** upstream dam year as the intervention point. Segments with multiple dams built across decades have a more gradual impact that this binary split does not capture.
- The Mann-Whitney U test detects shift in distribution, not causation. Post-dam changes may reflect concurrent climate shifts or land-use changes.
- Segments where dams were built before 1951 have no pre-dam baseline in the record and are excluded from 3A (but included in 3E control group analysis).
- Regulation index uses gross storage capacity, not actual operational storage.
- Streamflow data is naturalized (VIC/mizuRoute model), so dam effects may already be partially absent from the record. This analysis tests for residual signals rather than direct causal measurement.
