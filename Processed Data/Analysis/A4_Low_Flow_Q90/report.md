# A4: Low-Flow Quantile (Q90) Decline

## Method
For each segment, compute Q90 (the 10th percentile of monthly streamflow — i.e.,
the flow exceeded 90% of the time) for the early period (1951–1990)
and late period (1991–2021). Report percent change in Q90.

Q90 is a standard low-flow indicator in hydrology. Decline in Q90 means the
minimum flows that sustain ecosystems and water supply are shrinking.

Also computes Q50 (median) change for comparison.

## Key Findings

- **1118 of 1526 valid segments (73.3%) show Q90 decline**
- Median Q90 percent change: **-6.7%**
- Median Q50 percent change: **-6.0%**
- Q90 declines are typically **larger** than Q50 declines, indicating low flows are
  disproportionately affected

## Top 20 Segments with Greatest Q90 Decline

| Rank | seg_id | River | Tier | Early Q90 | Late Q90 | Q90 Change (%) | Q50 Change (%) |
|------|--------|-------|------|-----------|----------|----------------|----------------|
| 1 | 3822 | Rihand | 3 | 0.79 | 0.19 | -76.4% | -58.4% |
| 2 | 3481 | Sengur | 4 | 0.84 | 0.23 | -72.2% | -54.2% |
| 3 | 3354 | Sengur | 4 | 0.72 | 0.22 | -68.7% | -50.0% |
| 4 | 3536 | Karwan | 4 | 0.45 | 0.16 | -63.9% | -42.9% |
| 5 | 4120 | Manmaheshwari | 5 | 0.92 | 0.35 | -61.8% | -37.2% |
| 6 | 3367 | Non | 4 | 2.47 | 0.96 | -61.3% | -41.1% |
| 7 | 3821 | Sengur | 4 | 0.68 | 0.27 | -60.7% | -61.0% |
| 8 | 3449 | Mendha | 3 | 0.32 | 0.13 | -60.4% | -20.4% |
| 9 | 3825 | Non | 4 | 4.65 | 1.86 | -60.0% | -37.6% |
| 10 | 4091 | Yamuna | 1 | 3.00 | 1.23 | -58.9% | -37.4% |
| 11 | 3541 | Karwan | 4 | 0.69 | 0.28 | -58.6% | -34.4% |
| 12 | 3823 | Non | 4 | 2.02 | 0.84 | -58.4% | -34.0% |
| 13 | 7677 | Mendha | 3 | 0.18 | 0.07 | -57.7% | -22.4% |
| 14 | 4090 | Sasur Khaderi | 5 | 2.18 | 0.98 | -54.9% | -30.9% |
| 15 | 4119 | Orai | 5 | 0.64 | 0.29 | -54.3% | -25.9% |
| 16 | 3976 | Ganga | 1 | 2.98 | 1.36 | -54.2% | -46.6% |
| 17 | 4118 | Sengur | 4 | 0.42 | 0.20 | -53.1% | -34.3% |
| 18 | 3975 | Sai | 3 | 1.05 | 0.51 | -51.7% | -42.8% |
| 19 | 4813 | Mahakali | 2 | 0.22 | 0.11 | -51.7% | -36.4% |
| 20 | 3482 | Ganga | 1 | 4.29 | 2.09 | -51.3% | -39.5% |

## Visual Encoding Recommendation
Color by Q90 percent change. This encoding specifically highlights segments
where minimum flows are collapsing — ecologically and socially the most
critical signal. Could be a "Low-Flow Vulnerability" layer toggle.

## Limitations
- Q90 from monthly data is coarser than daily Q90 (monthly averages smooth out
  the lowest daily flows)
- Segments with near-zero early Q90 produce extreme or undefined percent changes
  (these are flagged as NaN)
- Naturalized flow: real Q90 decline with abstractions is likely worse
