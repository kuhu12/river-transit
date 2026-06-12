# B1: Basin-Wide Annual SSA

## Method
Sum annual mean streamflow across all segments for each year, then standardize
as Z-score (SSA) using the full 1951–2021 period. Also computed for
the most downstream outlet segment (seg_id 1429) for comparison with
Chuphal et al.

Drought threshold: SSA < -0.5
Extreme drought: SSA < -1.0
Extreme wet: SSA > 1.0

## Key Findings

- **20 drought years** (SSA < -0.5) out of 72 years
- **15 extreme drought years** (SSA < -1.0)
- **10 extreme wet years** (SSA > 1.0)

## All Drought Years (SSA < -0.5), Ranked by Severity

| Rank | Year | SSA | Basin Total (m3/s) | Outlet SSA |
|------|------|-----|-------------------|------------|
| 1 | 2009 | -2.014 | 376354 | -2.220 |
| 2 | 2014 | -1.913 | 389082 | -1.950 |
| 3 | 1979 | -1.777 | 406275 | -1.250 |
| 4 | 2015 | -1.677 | 418865 | -1.464 |
| 5 | 2022 | -1.649 | 422387 | -2.956 |
| 6 | 2007 | -1.473 | 444718 | -1.052 |
| 7 | 1966 | -1.449 | 447787 | -0.784 |
| 8 | 1972 | -1.448 | 447911 | -1.441 |
| 9 | 1965 | -1.346 | 460769 | -0.834 |
| 10 | 1951 | -1.258 | 471834 | -0.692 |
| 11 | 2006 | -1.228 | 475649 | -1.960 |
| 12 | 2005 | -1.222 | 476379 | -1.013 |
| 13 | 2002 | -1.131 | 487873 | -0.581 |
| 14 | 1989 | -1.007 | 503554 | -0.361 |
| 15 | 1992 | -1.004 | 504045 | -1.437 |
| 16 | 1997 | -0.987 | 506104 | -1.189 |
| 17 | 2004 | -0.779 | 532435 | 0.055 |
| 18 | 2008 | -0.657 | 547842 | -0.797 |
| 19 | 2010 | -0.614 | 553258 | -0.211 |
| 20 | 1981 | -0.587 | 556638 | -0.360 |

## Extreme Wet Years (SSA > 1.0)

| Year | SSA | Basin Total (m3/s) |
|------|-----|-------------------|
| 1961 | +2.233 | 913232 |
| 1971 | +1.919 | 873478 |
| 2019 | +1.871 | 867388 |
| 1956 | +1.839 | 863357 |
| 1975 | +1.511 | 821885 |
| 1978 | +1.452 | 814512 |
| 1955 | +1.451 | 814292 |
| 2021 | +1.288 | 793681 |
| 2013 | +1.034 | 761641 |
| 1958 | +1.018 | 759638 |

## Decade Summary

| Decade | Mean SSA | N Drought Years |
|--------|----------|----------------|
| 1950s | +0.462 | 1 |
| 1960s | +0.241 | 2 |
| 1970s | +0.190 | 2 |
| 1980s | +0.044 | 2 |
| 1990s | -0.035 | 2 |
| 2000s | -0.850 | 7 |
| 2010s | -0.057 | 3 |
| 2020s | +0.167 | 1 |

## Visual Encoding Recommendation
Annotate the year slider with basin-wide SSA: red ticks for drought years,
blue for extreme wet years. The 30-year moving mean line shows the post-1991
structural shift. Could also show as a small sparkline beneath the slider.

## Limitations
- Basin-wide average masks spatial heterogeneity
- Western tributaries may be in severe drought while eastern ones are wet
