# A5: Per-Segment Standardized Streamflow Anomaly (SSA)

## Method
For each segment, SSA = (Q_year - mu) / sigma, where mu and sigma are computed
over the full period (1951–2021). This directly replicates the
Chuphal et al. (PNAS 2025) methodology, extended from the basin outlet to every
individual segment.

Drought threshold: SSA < -0.5 (consistent with the paper).

Reference period for standardization: 1951–2021 (full record).

## Key Findings

- **1035 of 1526 segments (67.8%)** have negative mean SSA for 1991–2021
- **49 segments (3.2%)** have mean SSA below drought threshold (-0.5)
- Basin-wide mean SSA for 1991–2021: **-0.111**

## Top 20 Driest Segments (Mean SSA 1991–2021)

| Rank | seg_id | River | Tier | Mean SSA | Long-Term Mean (m3/s) |
|------|--------|-------|------|----------|----------------------|
| 1 | 3821 | Sengur | 4 | -0.703 | 2.1 |
| 2 | 3822 | Rihand | 3 | -0.682 | 2.1 |
| 3 | 3975 | Sai | 3 | -0.629 | 3.3 |
| 4 | 3824 | Yamuna | 1 | -0.621 | 7.6 |
| 5 | 3894 | Mandal | 5 | -0.619 | 5.9 |
| 6 | 3481 | Sengur | 4 | -0.614 | 2.2 |
| 7 | 3826 | Sengur | 4 | -0.605 | 37.1 |
| 8 | 3820 | Rihand | 3 | -0.593 | 32.7 |
| 9 | 4797 | Sharda | 2 | -0.592 | 6.7 |
| 10 | 4783 | Suheli | 4 | -0.591 | 33.1 |
| 11 | 3896 | Palain | 5 | -0.589 | 4.9 |
| 12 | 3484 | Sengur | 4 | -0.584 | 35.0 |
| 13 | 4430 | Gandak | 2 | -0.581 | 17.0 |
| 14 | 3807 | Pandu | 5 | -0.579 | 14.0 |
| 15 | 3367 | Non | 4 | -0.558 | 9.4 |
| 16 | 4796 | Sharda | 2 | -0.555 | 4.6 |
| 17 | 4423 | Ghaghi | 5 | -0.552 | 23.5 |
| 18 | 4800 | Mohana | 4 | -0.552 | 8.1 |
| 19 | 4799 | Mohana | 4 | -0.551 | 5.7 |
| 20 | 4782 | Mohana | 4 | -0.549 | 28.9 |

## Top 10 Wettest Segments (Mean SSA 1991–2021)

| Rank | seg_id | River | Tier | Mean SSA | Long-Term Mean (m3/s) |
|------|--------|-------|------|----------|----------------------|
| 1 | 4239 | Birahi Ganga | 4 | 0.459 | 4.2 |
| 2 | 4242 | Nandakini | 3 | 0.416 | 8.1 |
| 3 | 4839 | Marai | 5 | 0.380 | 10.7 |
| 4 | 4868 | Balasan | 4 | 0.377 | 28.6 |
| 5 | 4837 | Ganga | 1 | 0.321 | 7.6 |
| 6 | 4236 | Rishi Ganga | 4 | 0.321 | 19.0 |
| 7 | 4867 | Lohra | 5 | 0.312 | 15.1 |
| 8 | 3694 | Khari | 3 | 0.307 | 1.1 |
| 9 | 4249 | Baghain | 4 | 0.297 | 11.2 |
| 10 | 4062 | Naina | 4 | 0.290 | 14.3 |

## SSA by Tier

| Tier | N Segments | Median SSA | N in Drought (SSA < -0.5) |
|------|------------|------------|----------------------------------------------|
| 1 | 246 | -0.241 | 7 |
| 2 | 303 | -0.009 | 7 |
| 3 | 324 | -0.106 | 10 |
| 4 | 371 | -0.091 | 16 |
| 5 | 282 | -0.065 | 9 |

## Visual Encoding Recommendation
**This is the recommended primary encoding for the VizChitra map.**
SSA is dimensionless and bounded (~-3 to +3), mapping perfectly to a diverging
color scale (deep red = SSA < -1, white = 0, deep blue = SSA > 1). Every segment
is on its own scale, so small Tier 5 headwaters are as visually prominent as the
mainstem Ganga. Animate by year for the full spatiotemporal story.

## Data Files
- `segment_ssa_by_year.csv`: Full SSA for every segment x year (for animation)
- `segment_ssa_period_mean.csv`: Summary metric for static "drying severity" map

## Limitations
- Z-score assumes approximately normal distribution of annual flows
- Short-term variability may produce extreme SSA in individual years
- Naturalized flow: does not include dam/abstraction effects
