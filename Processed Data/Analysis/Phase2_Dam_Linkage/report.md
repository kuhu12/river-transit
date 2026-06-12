# Phase 2: Dam-to-Segment Spatial Linkage Report

## Dam Assignment Summary

| Method | Count | % of Total |
|--------|-------|------------|
| Tier 1 — Existing snap (< 2.2 km) | 172 | 22.0% |
| Tier 2 — River name match (< 11 km) | 14 | 1.8% |
| Tier 3 — Nearest segment (< 22 km) | 573 | 73.4% |
| Unassigned (> 22 km from any segment) | 22 | 2.8% |
| **Total assigned** | **759** | **97.2%** |

## Snap Distance by Method

| Method | Median (km) | Mean (km) | Max (km) |
|--------|-------------|-----------|----------|
| tier1_existing | 0.2 | 0.5 | 2.2 |
| tier2_river_name | 7.6 | 6.8 | 10.0 |
| tier3_nearest | 8.8 | 9.1 | 22.0 |

## Segment Coverage

- **750 of 1526 segments (49.1%)** have at least one upstream dam
- **776 segments (50.9%)** have no upstream dams

## Distribution of Upstream Dam Count per Segment

| Range | Segments | % |
|-------|----------|---|
| 0 | 776 | 50.9% |
| 1 | 134 | 8.8% |
| 2-5 | 288 | 18.9% |
| 6-10 | 103 | 6.7% |
| 11-20 | 51 | 3.3% |
| 21-50 | 97 | 6.4% |
| 51+ | 77 | 5.0% |

## Top 15 Segments by Upstream Dam Count

| seg_id | River | Tier | N Dams | Capacity (MCM) | Before 1991 | After 1991 |
|--------|-------|------|--------|----------------|-------------|------------|
| 4405 | Ghaghara | 99 | 572 | 28,917 | 430 | 100 |
| 4407 | Ganga | 1 | 572 | 28,917 | 430 | 100 |
| 4402 | Ganga | 1 | 558 | 27,858 | 422 | 96 |
| 4404 | Ganga | 1 | 558 | 27,858 | 422 | 96 |
| 4567 | Ganga | 1 | 558 | 27,858 | 422 | 96 |
| 4568 | Ganga | 1 | 558 | 27,858 | 422 | 96 |
| 4569 | Ganga | 1 | 558 | 27,858 | 422 | 96 |
| 4570 | Ganga | 1 | 558 | 27,858 | 422 | 96 |
| 3959 | Ganga | 1 | 554 | 27,369 | 419 | 95 |
| 3961 | Ganga | 1 | 554 | 27,369 | 419 | 95 |
| 3933 | Ganga | 1 | 553 | 27,368 | 419 | 94 |
| 3934 | Ganga | 1 | 553 | 27,368 | 419 | 94 |
| 3937 | Ganga | 1 | 553 | 27,368 | 419 | 94 |
| 3938 | Ganga | 1 | 553 | 27,368 | 419 | 94 |
| 3972 | Ganga | 1 | 553 | 27,368 | 419 | 94 |

## Rivers by Total Upstream Dam Density

| River | Segments | Max Upstream Dams | Avg Dams/Seg | Peak Capacity (MCM) |
|-------|----------|-------------------|--------------|---------------------|
| Ganga | 108 | 572 | 94.0 | 28,917 |
| Ghaghara | 42 | 572 | 17.1 | 28,917 |
| Yamuna | 53 | 473 | 114.2 | 20,726 |
| Chambal | 28 | 229 | 56.6 | 13,023 |
| Son | 33 | 153 | 40.6 | 19,540 |
| Betwa | 29 | 102 | 33.7 | 5,110 |
| Banas | 29 | 72 | 24.3 | 2,740 |
| Ken | 15 | 71 | 43.1 | 676 |
| Kali Sindh | 11 | 59 | 28.4 | 6,076 |
| Tons | 20 | 49 | 12.0 | 887 |
| Parbati | 24 | 42 | 21.0 | 403 |
| Sindh | 17 | 40 | 13.5 | 596 |
| Sonar | 9 | 39 | 9.1 | 214 |
| Padma | 42 | 32 | 6.1 | 2,481 |
| Dhasan | 15 | 29 | 9.7 | 448 |
| Bearma | 12 | 28 | 9.7 | 86 |
| Belan | 9 | 24 | 11.2 | 734 |
| Berach | 15 | 24 | 8.3 | 589 |
| Gopat | 9 | 24 | 6.8 | 202 |
| Parvan | 2 | 21 | 17.5 | 174 |

## Dam Assignments by State

| State | Total Dams | Assigned | % Assigned |
|-------|-----------|----------|------------|
| Madhya Pradesh | 440 | 436 | 99.1% |
| Rajasthan | 128 | 128 | 100.0% |
| Uttar Pradesh | 123 | 119 | 96.7% |
| Bihar | 26 | 26 | 100.0% |
| Jharkhand | 26 | 22 | 84.6% |
| Uttarakhand | 24 | 24 | 100.0% |
| Chhattisgarh | 10 | 4 | 40.0% |
| West Bengal | 4 | 0 | 0.0% |

## Network Statistics

- Nodes (segments): 1526
- Edges (downstream connections): 1507
- Source segments (headwaters, in-degree=0): 713
- Sink segments (outlets, out-degree=0): 19
- Weakly connected components: 19

## Limitations

- Tier 3 (nearest segment) may assign a dam to a geographically close but hydrologically unrelated river
- Upstream propagation follows the mizuRoute network topology, which may not perfectly reflect real-world flow paths for all segments
- Cumulative capacity sums gross storage — actual downstream impact depends on operations, releases, and evaporation
- 609 of 781 dams sit on minor tributaries not in the 1526-segment network; their Tier 2/3 assignment is approximate
- Dam year coverage is 91.4% (67 dams have no construction year)
