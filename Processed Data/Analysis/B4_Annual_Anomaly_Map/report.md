# B4: Annual Anomaly Map Data (for Visualization)

## Method
This is the per-segment SSA data from A5, reshaped into **wide format** for
efficient map rendering. Each row is a segment, each column is a year, each
cell is the SSA value.

This is the data file your visualization should consume to animate the drying
story across the basin.

## Data Format
- Rows: 1526 segments
- Columns: seg_id, river_name, updated_tier, then one column per year (1951–2022)
- Values: SSA (Z-score, dimensionless)
- Color mapping suggestion: SSA -2 = deep red, 0 = white, +2 = deep blue

## Key Transition Points

| Year | Shift | Basin Mean SSA |
|------|-------|---------------|
| 1952 | negative -> positive | 0.079 |
| 1954 | positive -> negative | -0.016 |
| 1955 | negative -> positive | 0.606 |
| 1965 | positive -> negative | -0.616 |
| 1967 | negative -> positive | 0.193 |
| 1968 | positive -> negative | -0.216 |
| 1969 | negative -> positive | 0.247 |
| 1970 | positive -> negative | -0.098 |
| 1971 | negative -> positive | 0.967 |
| 1972 | positive -> negative | -0.702 |
| 1973 | negative -> positive | 0.445 |
| 1974 | positive -> negative | -0.185 |
| 1975 | negative -> positive | 0.698 |
| 1976 | positive -> negative | -0.009 |
| 1978 | negative -> positive | 0.712 |
| 1979 | positive -> negative | -0.813 |
| 1980 | negative -> positive | 0.202 |
| 1981 | positive -> negative | -0.229 |
| 1983 | negative -> positive | 0.244 |
| 1986 | positive -> negative | -0.030 |
| 1990 | negative -> positive | 0.270 |
| 1991 | positive -> negative | -0.219 |
| 1994 | negative -> positive | 0.231 |
| 1997 | positive -> negative | -0.328 |
| 1999 | negative -> positive | 0.376 |
| 2000 | positive -> negative | -0.172 |
| 2003 | negative -> positive | 0.295 |
| 2004 | positive -> negative | -0.401 |
| 2011 | negative -> positive | 0.422 |
| 2014 | positive -> negative | -0.865 |
| 2016 | negative -> positive | 0.171 |
| 2017 | positive -> negative | -0.208 |
| 2019 | negative -> positive | 0.875 |
| 2022 | positive -> negative | -0.772 |

## Suggested Animation Keyframes
Based on basin-wide SSA, these are the years that best illustrate the drying narrative:
- **1961**: Wettest year (SSA = 1.058) — start of animation baseline
- **1991**: Changepoint year identified by Chuphal et al.
- **2009**: Driest year (SSA = -0.920)
- Play through all years at ~600ms/frame to show the blue→red transition

## Visual Encoding Recommendation
Replace the current constant-color (#2b8cbe) + flow-width encoding with:
- **Stroke width**: static, based on river tier (Tier 1 = thick, Tier 5 = thin)
- **Stroke color**: SSA-based diverging scale per segment per year
  - SSA <= -2.0: #67001f (deep red)
  - SSA = -1.0: #d6604d (red)
  - SSA = 0: #f7f7f7 (white/neutral)
  - SSA = +1.0: #4393c3 (blue)
  - SSA >= +2.0: #053061 (deep blue)

This dual encoding (width = river importance, color = anomaly) makes the drying
signal visible on EVERY segment regardless of absolute flow magnitude.
