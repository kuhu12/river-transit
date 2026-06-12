# A6: Segment Classification — Stable vs. Drying vs. Wetting

## Method
Each of the 1526 Ganga Basin segments is classified using the **Mann-Kendall
trend test** (from A2) as the primary criterion:

| Category | Rule |
|----------|------|
| **Drying** | MK trend = decreasing AND p <= 0.05 |
| **Wetting** | MK trend = increasing AND p <= 0.05 |
| **Stable** | p > 0.05 (no statistically significant monotonic trend) |

Severity sub-categories use the **early-vs-late percent change** (from A1):

| Severity | Additional Criterion |
|----------|---------------------|
| Severe Drying | pct_change <= -30% |
| Moderate Drying | -30% < pct_change <= -10% |
| Mild Drying | pct_change > -10% |
| Significant Wetting | pct_change >= +20% |
| Mild Wetting | pct_change < +20% |

Period: 1951-2021. Significance threshold: p <= 0.05.

## Table A: Overall Basin Summary

| Category | Segments | % of Total |
|----------|----------|------------|
| Stable | 993 | 65.1% |
| Drying | 517 | 33.9% |
| Wetting | 16 | 1.0% |
| **Total** | **1526** | **100%** |

## Key Takeaway

> Of the 1526 river segments in the Ganga Basin, **993 (65.1%)
> show no statistically significant long-term trend** in streamflow over 1951-2021.
> **517 (33.9%) show significant drying**, while only
> **16 (1.0%) show significant wetting**. The drying is real
> but geographically concentrated — it is not a uniform basin-wide catastrophe.

## Table B: Severity Breakdown

| Severity | Segments | % of Total | Avg % Change |
|----------|----------|------------|--------------|
| Severe Drying | 177 | 11.6% | -39.2% |
| Moderate Drying | 286 | 18.7% | -19.6% |
| Mild Drying | 54 | 3.5% | -7.1% |
| Stable | 993 | 65.1% | +0.0% |
| Mild Wetting | 8 | 0.5% | +11.7% |
| Significant Wetting | 8 | 0.5% | +37.3% |

## Table C: Classification by River (Top 20 by segment count)

| River | Total | Drying | Stable | Wetting | % Drying | Median % Change |
|-------|-------|--------|--------|---------|----------|-----------------|
| Ganga | 108 | 58 | 50 | 0 | 53.7% | -11.1% |
| Yamuna | 53 | 46 | 7 | 0 | 86.8% | -21.3% |
| Padma | 42 | 5 | 37 | 0 | 11.9% | -4.5% |
| Ghaghara | 42 | 27 | 15 | 0 | 64.3% | -19.1% |
| Kosi | 33 | 4 | 29 | 0 | 12.1% | +0.0% |
| Son | 33 | 0 | 33 | 0 | 0.0% | +4.8% |
| Betwa | 29 | 12 | 17 | 0 | 41.4% | -5.4% |
| Banas | 29 | 0 | 29 | 0 | 0.0% | +6.3% |
| Gomti | 28 | 20 | 8 | 0 | 71.4% | -28.4% |
| Chambal | 28 | 0 | 28 | 0 | 0.0% | +5.5% |
| Parbati | 24 | 5 | 19 | 0 | 20.8% | -4.8% |
| Khari | 23 | 10 | 12 | 1 | 43.5% | +16.0% |
| Gandak | 23 | 2 | 14 | 7 | 8.7% | +5.5% |
| Burhi Gandak | 20 | 0 | 20 | 0 | 0.0% | +1.4% |
| Tons | 20 | 3 | 17 | 0 | 15.0% | +11.2% |
| Sai | 20 | 20 | 0 | 0 | 100.0% | -42.1% |
| Ramganga | 18 | 16 | 2 | 0 | 88.9% | -24.8% |
| Rapti | 17 | 6 | 11 | 0 | 35.3% | -17.7% |
| Kali | 17 | 14 | 3 | 0 | 82.4% | -31.3% |
| Sindh | 17 | 6 | 11 | 0 | 35.3% | -11.3% |

## Table D: Classification by Tier

| Tier | Total | Drying | Stable | Wetting | % Drying | Median % Change |
|------|-------|--------|--------|---------|----------|-----------------|
| 1 | 246 | 136 | 110 | 0 | 55.3% | -12.2% |
| 2 | 303 | 78 | 218 | 7 | 25.7% | -1.5% |
| 3 | 324 | 115 | 205 | 4 | 35.5% | -8.6% |
| 4 | 371 | 119 | 248 | 4 | 32.1% | -8.4% |
| 5 | 282 | 69 | 212 | 1 | 24.5% | -5.7% |

## Limitations
- Classification uses a single significance threshold (p <= 0.05); borderline segments could shift with different thresholds
- MK test assumes monotonic trend — segments with step-changes or reversals may be misclassified as "Stable"
- Severity thresholds (-30%, -10%, +20%) are domain-informed but somewhat arbitrary
- All segments weighted equally regardless of flow volume or length
- Naturalized flow only — real-world conditions include dam/abstraction effects
