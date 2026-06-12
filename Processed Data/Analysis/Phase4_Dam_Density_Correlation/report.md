# Phase 4: Dam Density vs. Drying Severity Correlation

## 4A: Segment-Level Dam Density Gradient

### By Upstream Dam Count

| Dam Count | Segments | % Drying | % Stable | % Wetting | Mean % Change | Median % Change | Mean Tau |
|-----------|----------|----------|----------|-----------|---------------|-----------------|----------|
| 0 | 776 | 36.6% | 61.9% | 1.5% | -11.84% | -9.6% | -0.1111 |
| 1-5 | 422 | 30.8% | 68.5% | 0.7% | -5.04% | -7.01% | -0.0887 |
| 6-20 | 154 | 31.2% | 68.8% | 0.0% | -4.6% | -5.34% | -0.086 |
| 21-50 | 97 | 14.4% | 84.5% | 1.0% | -1.01% | -2.17% | -0.0613 |
| 51+ | 77 | 53.2% | 46.8% | 0.0% | -5.07% | -8.91% | -0.1125 |

### By Upstream Capacity (MCM)

| Capacity | Segments | % Drying | Mean % Change | Median % Change | Mean Tau |
|----------|----------|----------|---------------|-----------------|----------|
| 0 | 779 | 36.6% | -11.84% | -9.6% | -0.1111 |
| 0-100 | 332 | 19.6% | -1.2% | -2.97% | -0.058 |
| 100-1000 | 270 | 38.1% | -7.46% | -8.99% | -0.1086 |
| 1000-5000 | 75 | 28.0% | -5.56% | -8.19% | -0.1015 |
| 5000+ | 70 | 61.4% | -6.63% | -10.56% | -0.125 |

### Spearman Correlations (All 1526 Segments)

| Variables | r | p-value | Interpretation |
|-----------|---|---------|----------------|
| n_upstream_dams vs pct_change | 0.182 | 0.0000 * | Weak |
| n_upstream_dams vs tau | 0.043 | 0.0954 | Negligible |
| n_upstream_dams vs norm_slope_pct_yr | 0.074 | 0.0037 * | Negligible |
| total_upstream_capacity_mcm vs pct_change | 0.134 | 0.0000 * | Weak |
| total_upstream_capacity_mcm vs tau | 0.003 | 0.8960 | Negligible |
| total_upstream_capacity_mcm vs norm_slope_pct_yr | 0.042 | 0.0975 | Negligible |

### Spearman Correlations (750 Dammed Segments Only)

| Variables | r | p-value | Interpretation |
|-----------|---|---------|----------------|
| n_upstream_dams vs pct_change | 0.032 | 0.3810 | Negligible |
| n_upstream_dams vs tau | -0.076 | 0.0384 * | Negligible |
| n_upstream_dams vs norm_slope_pct_yr | -0.013 | 0.7142 | Negligible |
| total_upstream_capacity_mcm vs pct_change | -0.174 | 0.0000 * | Weak |
| total_upstream_capacity_mcm vs tau | -0.239 | 0.0000 * | Weak |
| total_upstream_capacity_mcm vs norm_slope_pct_yr | -0.146 | 0.0001 * | Weak |

## 4B: River-Level Analysis

Analysed **73 rivers** with >= 5 segments.

### Correlations at River Level

| Variables | r | p-value | Interpretation |
|-----------|---|---------|----------------|
| max_upstream_dams vs pct_drying | -0.047 | 0.6940 | Negligible |
| max_upstream_dams vs mean_pct_change | 0.276 | 0.0181 * | Weak |
| max_upstream_dams vs mean_tau | 0.096 | 0.4202 | Negligible |
| max_capacity_mcm vs pct_drying | -0.018 | 0.8781 | Negligible |
| max_capacity_mcm vs mean_pct_change | 0.196 | 0.0972 | Weak |
| max_capacity_mcm vs mean_tau | 0.062 | 0.6051 | Negligible |
| mean_upstream_dams vs pct_drying | -0.091 | 0.4428 | Negligible |
| mean_upstream_dams vs mean_pct_change | 0.285 | 0.0144 * | Weak |
| mean_upstream_dams vs mean_tau | 0.119 | 0.3148 | Weak |

### Top 20 Rivers by Max Upstream Dams

| River | Segments | Max Dams | Capacity (MCM) | % Drying | Mean % Change | Mean Tau |
|-------|----------|----------|----------------|----------|---------------|----------|
| Ganga | 108 | 572 | 28,917 | 53.7% | -12.4% | -0.153 |
| Ghaghara | 42 | 572 | 28,917 | 64.3% | -21.7% | -0.167 |
| Yamuna | 53 | 473 | 20,726 | 86.8% | -19.0% | -0.251 |
| Chambal | 28 | 229 | 13,023 | 0.0% | 6.3% | 0.010 |
| Son | 33 | 153 | 19,540 | 0.0% | 5.6% | 0.042 |
| Betwa | 29 | 102 | 5,110 | 41.4% | -5.6% | -0.127 |
| Banas | 29 | 72 | 2,740 | 0.0% | 10.1% | 0.020 |
| Ken | 15 | 71 | 676 | 33.3% | -7.9% | -0.138 |
| Kali Sindh | 11 | 59 | 6,076 | 0.0% | 1.6% | -0.033 |
| Tons | 20 | 49 | 887 | 15.0% | 6.6% | 0.023 |
| Parbati | 24 | 42 | 403 | 20.8% | -9.1% | -0.098 |
| Sindh | 17 | 40 | 596 | 35.3% | -11.4% | -0.138 |
| Sonar | 9 | 39 | 214 | 100.0% | -8.1% | -0.192 |
| Padma | 42 | 32 | 2,481 | 11.9% | -4.3% | -0.038 |
| Dhasan | 15 | 29 | 448 | 100.0% | -16.6% | -0.223 |
| Bearma | 12 | 28 | 86 | 16.7% | -5.9% | -0.143 |
| Belan | 9 | 24 | 734 | 0.0% | 19.5% | 0.118 |
| Berach | 15 | 24 | 589 | 0.0% | 10.8% | 0.032 |
| Gopat | 9 | 24 | 202 | 0.0% | 20.7% | 0.104 |
| Rihand | 12 | 14 | 10,830 | 41.7% | -8.1% | -0.133 |

### Heavily Dammed but NOT Drying (>= 20 upstream dams, < 15% drying)

| River | Segments | Max Dams | Capacity (MCM) | % Drying | Mean % Change |
|-------|----------|----------|----------------|----------|---------------|
| Banas | 29 | 72 | 2,740 | 0.0% | 10.1% |
| Belan | 9 | 24 | 734 | 0.0% | 19.5% |
| Berach | 15 | 24 | 589 | 0.0% | 10.8% |
| Chambal | 28 | 229 | 13,023 | 0.0% | 6.3% |
| Gopat | 9 | 24 | 202 | 0.0% | 20.7% |
| Kali Sindh | 11 | 59 | 6,076 | 0.0% | 1.6% |
| Padma | 42 | 32 | 2,481 | 11.9% | -4.3% |
| Son | 33 | 153 | 19,540 | 0.0% | 5.6% |

### No Upstream Dams but High Drying (0 dams, > 50% drying)

| River | Segments | % Drying | Mean % Change |
|-------|----------|----------|---------------|
| Hindon | 6 | 66.7% | -21.0% |
| Kali | 17 | 82.4% | -28.1% |
| Karwan | 8 | 100.0% | -44.8% |
| Sai | 20 | 100.0% | -41.4% |
| Sarju | 6 | 66.7% | -9.7% |
| Sengur | 10 | 100.0% | -44.0% |
| Tamasa | 14 | 100.0% | -31.0% |

## 4C: Dam Construction Timing vs. Drying Onset

Analysed **654 segments** with both a known nearest dam year and a year of minimum flow.

- Minimum flow occurred **after** dam construction: **526 (80.4%)**
- Minimum flow occurred **before** dam construction: **128**
- Mean lag (min_year - dam_year): **32.8 years**
- Median lag: **29.0 years**

### Lag Distribution (years between dam construction and minimum flow)

| Lag Range (years) | Segments |
|-------------------|----------|
| < -20 | 59 |
| -20 to -10 | 36 |
| -10 to 0 | 35 |
| 0 to 10 | 42 |
| 10 to 20 | 65 |
| 20 to 40 | 204 |
| > 40 | 213 |

### Year of Minimum Flow: Dammed vs Dam-Free Segments

| Group | Segments | Median Min Year | % Min After 1991 |
|-------|----------|-----------------|------------------|
| With dams | 750 | 2005 | 73.6% |
| No dams | 776 | 2007 | 75.6% |

## 4D: Pre-1991 vs Post-1991 Dam Maturity

Dam maturity = fraction of upstream dams built before 1991.

| Maturity Group | Segments | % Drying | Mean % Change | Median % Change | Mean Tau | Avg N Dams |
|----------------|----------|----------|---------------|-----------------|---------|------------|
| Mostly post-1991 | 93 | 29.0% | -2.22% | -2.23% | -0.067 | 3.5 |
| Mixed | 177 | 28.2% | -3.72% | -3.93% | -0.0876 | 18.2 |
| Mostly pre-1991 | 480 | 32.5% | -5.12% | -7.69% | -0.0907 | 46.1 |

**Spearman(dam_maturity, pct_change):** r = -0.109, p = 0.0028
(Older dam infrastructure associated with greater flow reduction)

## 4E: Spatial Proximity to Nearest Upstream Dam

| Distance to Dam | Segments | % Drying | % Stable | Mean % Change | Median % Change |
|-----------------|----------|----------|----------|---------------|-----------------|
| 0-5 km | 59 | 15.3% | 84.7% | 4.05% | 1.04% |
| 5-10 km | 129 | 17.1% | 82.2% | -0.12% | -1.52% |
| 10-20 km | 199 | 22.1% | 77.4% | -2.23% | -3.2% |
| 20-50 km | 187 | 25.1% | 73.8% | -4.06% | -4.71% |
| 50+ km | 176 | 63.1% | 36.9% | -13.33% | -12.9% |

### Mean Distance to Nearest Dam by Category

| Category | Mean Dist (km) | Median Dist (km) |
|----------|----------------|------------------|
| Drying | 84.6 | 42.8 |
| Stable | 28.1 | 15.4 |

## Key Findings

*(Auto-generated from the data above)*

1. **Positive correlation** between dam count and flow change (r = 0.182, p = 0.0000). More dams are associated with *less* drying — likely because dammed rivers are larger and more resilient, not because dams prevent drying.
2. **Dam-free segments have 36.6% drying rate** — confirming Phase 3 finding that undammed segments dry at equal or higher rates.
3. **80.4% of dammed segments** hit their all-time minimum flow *after* dam construction, with a median lag of 29.0 years.

## Limitations

- Segment-level correlations are confounded by network topology: downstream segments inherit all upstream dam counts, creating spatial autocorrelation.
- River-level aggregation reduces sample size (73 rivers) and masks within-river variation.
- Dam count treats all dams equally; a 10,000 MCM reservoir has far more impact than a 5 MCM irrigation tank.
- Timing analysis uses nearest dam year, not cumulative dam construction history — the actual intervention is gradual, not a single event.
- Naturalized streamflow (VIC model) may not fully reflect dam impacts, so correlations represent residual signals.
