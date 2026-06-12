# A1: Percent Change — Early (1951–1990) vs. Late (1991–2021)

## Method
For each segment, compute mean annual streamflow for the early period (1951–1990)
and the late period (1991–2021). Percent change = (late - early) / early x 100.

Period split at 1991 is consistent with the Bayesian changepoint identified by
Chuphal et al. (PNAS 2025).

## Key Findings

- **1071 of 1526 segments (70.2%) show declining flow**
- Median percent change across all segments: **-8.1%**
- Mean percent change: **-8.2%**

## Top 20 Most Dried Segments

| Rank | seg_id | River | Tier | Early Mean (m3/s) | Late Mean (m3/s) | Change (%) |
|------|--------|-------|------|-------------------|------------------|------------|
| 1 | 3821 | Sengur | 4 | 2.98 | 0.91 | -69.3% |
| 2 | 3822 | Rihand | 3 | 2.84 | 1.08 | -62.1% |
| 3 | 3975 | Sai | 3 | 4.47 | 1.77 | -60.4% |
| 4 | 3533 | Yamuna | 1 | 3.81 | 1.51 | -60.3% |
| 5 | 3824 | Yamuna | 1 | 10.26 | 4.25 | -58.6% |
| 6 | 3481 | Sengur | 4 | 2.86 | 1.25 | -56.5% |
| 7 | 3367 | Non | 4 | 12.48 | 5.54 | -55.6% |
| 8 | 3354 | Sengur | 4 | 2.48 | 1.17 | -52.7% |
| 9 | 3825 | Non | 4 | 22.02 | 10.48 | -52.4% |
| 10 | 3807 | Pandu | 5 | 18.08 | 8.67 | -52.0% |
| 11 | 4120 | Manmaheshwari | 5 | 4.42 | 2.14 | -51.6% |
| 12 | 3303 | Yamuna | 1 | 2.04 | 1.00 | -51.1% |
| 13 | 3796 | Garra | 4 | 4.87 | 2.39 | -51.0% |
| 14 | 3894 | Mandal | 5 | 7.66 | 3.76 | -51.0% |
| 15 | 3497 | Yamuna | 1 | 1.22 | 0.61 | -50.0% |
| 16 | 3305 | Yamuna | 1 | 1.55 | 0.78 | -49.8% |
| 17 | 3482 | Ganga | 1 | 16.20 | 8.17 | -49.6% |
| 18 | 3976 | Ganga | 1 | 12.18 | 6.26 | -48.6% |
| 19 | 3502 | Karwan | 4 | 11.66 | 5.99 | -48.6% |
| 20 | 3877 | Ganga | 1 | 3.33 | 1.72 | -48.4% |

## Top 10 Most Wetted Segments

| Rank | seg_id | River | Tier | Early Mean (m3/s) | Late Mean (m3/s) | Change (%) |
|------|--------|-------|------|-------------------|------------------|------------|
| 1 | 3439 | Dai | 4 | 0.65 | 1.03 | +58.5% |
| 2 | 4620 | Mayar | 5 | 2.42 | 3.81 | +57.3% |
| 3 | 4239 | Birahi Ganga | 4 | 3.37 | 5.26 | +56.1% |
| 4 | 3694 | Khari | 3 | 0.86 | 1.33 | +55.2% |
| 5 | 4621 | Rihand | 3 | 8.78 | 13.26 | +51.0% |
| 6 | 4336 | Mayar | 5 | 6.31 | 9.34 | +47.9% |
| 7 | 4246 | Garara | 5 | 7.61 | 11.21 | +47.4% |
| 8 | 4252 | Yamuna | 1 | 11.96 | 17.27 | +44.3% |
| 9 | 4249 | Baghain | 4 | 9.40 | 13.50 | +43.6% |
| 10 | 4093 | Baghain | 4 | 31.34 | 44.55 | +42.1% |

## Change by River Tier

| Tier | N Segments | Median Change (%) | Mean Change (%) | Worst Segment (%) |
|------|------------|-------------------|-----------------|-------------------|
| 1 | 246 | -12.2% | -14.1% | -60.3% |
| 2 | 303 | -1.5% | -5.0% | -45.4% |
| 3 | 324 | -8.6% | -9.1% | -62.1% |
| 4 | 371 | -8.4% | -8.3% | -69.3% |
| 5 | 282 | -5.7% | -5.4% | -52.0% |

## Visual Encoding Recommendation
Map each segment's percent change to a **diverging color scale** (red = decline, blue = increase).
Range suggestion: clamp to [-50%, +50%] for visual contrast. This encoding makes small tributaries
as visually prominent as the mainstem, directly fixing the visibility problem.

## Limitations
- Two-period comparison is sensitive to the exact breakpoint year
- Does not capture non-linear or non-monotonic changes within each period
- Naturalized flow only — real-world decline may be worse due to abstractions
