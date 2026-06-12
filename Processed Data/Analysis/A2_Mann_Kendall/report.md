# A2: Mann-Kendall Trend Test + Sen's Slope

## Method
For each segment, compute annual mean streamflow time series (1951–2021),
then apply the non-parametric Mann-Kendall trend test. Sen's slope gives the robust
rate of change (m3/s per year). Normalized by dividing by the segment's long-term
mean to yield "% per year".

Consistent with Chuphal et al. (PNAS 2025) who apply MK to precipitation and
temperature trends (their Fig. 3), with significance at p <= 0.05.

## Key Findings

- **517 segments** with statistically significant declining trend (p <= 0.05)
- **16 segments** with statistically significant increasing trend
- **993 segments** with no significant trend
- Total segments analyzed: **1526**

## Top 20 Segments with Strongest Significant Decline

| Rank | seg_id | River | Tier | Sen's Slope (m3/s/yr) | Norm. Slope (%/yr) | Total Change (%) | p-value | Tau |
|------|--------|-------|------|----------------------|-------------------|-----------------|---------|-----|
| 1 | 3821 | Sengur | 4 | -0.042 | -2.036 | -146.6% | 0.0000 | -0.528 |
| 2 | 3894 | Mandal | 5 | -0.112 | -1.904 | -137.1% | 0.0000 | -0.462 |
| 3 | 3822 | Rihand | 3 | -0.038 | -1.863 | -134.1% | 0.0000 | -0.483 |
| 4 | 3533 | Yamuna | 1 | -0.052 | -1.861 | -134.0% | 0.0000 | -0.459 |
| 5 | 3481 | Sengur | 4 | -0.037 | -1.715 | -123.5% | 0.0000 | -0.475 |
| 6 | 3367 | Non | 4 | -0.159 | -1.695 | -122.1% | 0.0000 | -0.460 |
| 7 | 3896 | Palain | 5 | -0.081 | -1.675 | -120.6% | 0.0000 | -0.449 |
| 8 | 3354 | Sengur | 4 | -0.032 | -1.669 | -120.2% | 0.0000 | -0.486 |
| 9 | 3502 | Karwan | 4 | -0.151 | -1.657 | -119.3% | 0.0000 | -0.451 |
| 10 | 4120 | Manmaheshwari | 5 | -0.056 | -1.650 | -118.8% | 0.0000 | -0.442 |
| 11 | 3824 | Yamuna | 1 | -0.124 | -1.628 | -117.2% | 0.0000 | -0.411 |
| 12 | 3825 | Non | 4 | -0.273 | -1.624 | -116.9% | 0.0000 | -0.447 |
| 13 | 3541 | Karwan | 4 | -0.031 | -1.608 | -115.8% | 0.0000 | -0.430 |
| 14 | 3305 | Yamuna | 1 | -0.019 | -1.557 | -112.1% | 0.0000 | -0.422 |
| 15 | 4797 | Sharda | 2 | -0.100 | -1.504 | -108.3% | 0.0000 | -0.432 |
| 16 | 3823 | Non | 4 | -0.106 | -1.503 | -108.2% | 0.0000 | -0.425 |
| 17 | 3363 | Pahuj | 4 | -0.107 | -1.479 | -106.5% | 0.0000 | -0.410 |
| 18 | 3826 | Sengur | 4 | -0.542 | -1.471 | -105.9% | 0.0000 | -0.451 |
| 19 | 3503 | Karwan | 4 | -0.187 | -1.460 | -105.1% | 0.0000 | -0.435 |
| 20 | 3884 | Dhela | 5 | -0.192 | -1.450 | -104.4% | 0.0000 | -0.423 |

## Significant Declines by Tier

| Tier | N Sig. Declining | Median Norm. Slope (%/yr) |
|------|-----------------|--------------------------|
| 1 | 136 | -0.531 |
| 2 | 78 | -0.690 |
| 3 | 115 | -0.811 |
| 4 | 119 | -0.913 |
| 5 | 69 | -0.815 |

## Top 10 Segments with Significant Increasing Trend

| Rank | seg_id | River | Tier | Norm. Slope (%/yr) | Total Change (%) | p-value |
|------|--------|-------|------|-------------------|-----------------|---------|
| 1 | 4239 | Birahi Ganga | 4 | +1.032 | +74.3% | 0.0001 |
| 2 | 4062 | Naina | 4 | +0.716 | +51.6% | 0.0098 |
| 3 | 4063 | Naina | 4 | +0.713 | +51.3% | 0.0057 |
| 4 | 4059 | Gorma | 5 | +0.673 | +48.5% | 0.0116 |
| 5 | 4242 | Nandakini | 3 | +0.667 | +48.0% | 0.0052 |
| 6 | 4208 | Mandakini | 3 | +0.650 | +46.8% | 0.0490 |
| 7 | 4065 | Belan | 3 | +0.622 | +44.8% | 0.0446 |
| 8 | 3694 | Khari | 3 | +0.560 | +40.3% | 0.0479 |
| 9 | 4493 | Gandak | 2 | +0.417 | +30.0% | 0.0189 |
| 10 | 4508 | Gandak | 2 | +0.392 | +28.2% | 0.0388 |

## Visual Encoding Recommendation
Two-channel encoding: **color** = normalized Sen's slope (diverging red/blue),
**opacity** = statistical significance (full opacity if p < 0.05, 30% if not).
This highlights segments where drying is both large AND statistically confident.

## Limitations
- Assumes monotonic trend; misses step-changes or reversals
- 70-year series gives good power but may miss recent acceleration
- MK test can be affected by serial autocorrelation (common in hydrology)
