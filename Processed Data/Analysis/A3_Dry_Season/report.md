# A3: Dry-Season (Pre-Monsoon: Feb–May) Flow Decline

## Method
Same as A1 (early vs. late period percent change) but restricted to **pre-monsoon
dry-season months (February–May)**. This is when water scarcity has the greatest
impact on the 600M+ people in the Ganga basin.

Additionally runs Mann-Kendall on the dry-season annual means for significance.

## Key Findings

- **1061 of 1526 segments (69.5%) show dry-season decline**
- Median dry-season percent change: **-6.1%**
- **539 segments** with statistically significant dry-season decline (MK p <= 0.05)

## Top 20 Most Dried Segments (Dry Season)

| Rank | seg_id | River | Tier | Early Dry Mean | Late Dry Mean | Change (%) | MK Trend | MK p-value |
|------|--------|-------|------|---------------|--------------|------------|----------|------------|
| 1 | 4226 | Mana | 5 | 3.58 | 1.37 | -61.7% | decreasing | 0.0000 |
| 2 | 4229 | Jadh Ganga | 4 | 6.59 | 2.67 | -59.5% | decreasing | 0.0001 |
| 3 | 4231 | Jadh Ganga | 4 | 9.42 | 4.23 | -55.1% | decreasing | 0.0002 |
| 4 | 4228 | Bhagirathi | 3 | 9.08 | 4.08 | -55.1% | decreasing | 0.0014 |
| 5 | 3310 | Pabbar | 4 | 17.00 | 7.75 | -54.4% | decreasing | 0.0044 |
| 6 | 4238 | Alaknanda | 3 | 18.47 | 8.44 | -54.3% | decreasing | 0.0023 |
| 7 | 4224 | Jadhang | 5 | 0.75 | 0.34 | -54.2% | decreasing | 0.0003 |
| 8 | 3821 | Sengur | 4 | 1.33 | 0.62 | -52.9% | decreasing | 0.0000 |
| 9 | 3822 | Rihand | 3 | 1.40 | 0.66 | -52.8% | decreasing | 0.0000 |
| 10 | 4227 | Jadh Ganga | 4 | 1.95 | 0.94 | -51.9% | decreasing | 0.0007 |
| 11 | 4243 | Kail Ganga | 4 | 3.94 | 1.95 | -50.5% | no trend | 0.1263 |
| 12 | 3308 | Rupin | 4 | 4.82 | 2.40 | -50.3% | no trend | 0.0859 |
| 13 | 4223 | Jadh Ganga | 4 | 1.16 | 0.58 | -49.6% | decreasing | 0.0012 |
| 14 | 3481 | Sengur | 4 | 1.47 | 0.75 | -48.7% | decreasing | 0.0000 |
| 15 | 3316 | Shallu | 5 | 4.22 | 2.19 | -48.2% | decreasing | 0.0010 |
| 16 | 3326 | Giri | 4 | 11.17 | 5.90 | -47.1% | decreasing | 0.0114 |
| 17 | 3317 | Tons | 2 | 37.31 | 19.89 | -46.7% | decreasing | 0.0150 |
| 18 | 3312 | Tons | 2 | 32.78 | 17.52 | -46.6% | decreasing | 0.0312 |
| 19 | 3354 | Sengur | 4 | 1.32 | 0.72 | -45.5% | decreasing | 0.0000 |
| 20 | 3885 | Gomti | 2 | 2.13 | 1.17 | -45.1% | no trend | 0.0593 |

## Visual Encoding Recommendation
Toggle mode: "Annual" vs "Dry Season (Feb–May)". Same diverging color scale
as A1. Especially powerful for showing baseflow vulnerability — segments that
look fine on annual average but have collapsing dry-season flows.

## Limitations
- Only 4 months per year reduces sample size
- Pre-monsoon flow is baseflow-dominated; in naturalized model this reflects
  soil moisture / snowmelt, not groundwater pumping
- Some headwater segments may have very low dry-season flows with high
  relative noise
