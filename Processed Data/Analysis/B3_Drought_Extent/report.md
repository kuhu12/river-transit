# B3: Spatial Drought Extent per Year

## Method
For each year, count how many segments have SSA < -0.5 (drought)
and SSA < -1.0 (extreme drought). Express as percentage of all 1526 segments.
This measures the **spatial extent** of drought basin-wide.

## Key Findings

- Peak drought extent: **84.2%** of segments in drought in **2009**
- Mean drought extent post-1991: **41.4%** of segments
- Mean drought extent pre-1991: **29.7%** of segments

## Top 15 Years by Drought Extent

| Rank | Year | % Segments in Drought | % Extreme Drought | Mean SSA |
|------|------|----------------------|-------------------|----------|
| 1 | 2009 | 84.2% | 42.2% | -0.920 |
| 2 | 1966 | 75.0% | 45.0% | -0.790 |
| 3 | 2022 | 75.0% | 44.7% | -0.772 |
| 4 | 2014 | 73.7% | 43.2% | -0.865 |
| 5 | 2015 | 73.3% | 51.9% | -0.813 |
| 6 | 1979 | 71.7% | 38.2% | -0.813 |
| 7 | 1972 | 69.6% | 29.6% | -0.702 |
| 8 | 2005 | 67.1% | 30.3% | -0.593 |
| 9 | 2007 | 63.9% | 41.7% | -0.542 |
| 10 | 1951 | 63.5% | 29.3% | -0.627 |
| 11 | 2002 | 62.8% | 31.5% | -0.534 |
| 12 | 1965 | 62.3% | 29.6% | -0.616 |
| 13 | 2006 | 60.5% | 35.2% | -0.441 |
| 14 | 1989 | 58.9% | 24.4% | -0.486 |
| 15 | 2010 | 54.6% | 34.9% | -0.330 |

## Visual Encoding Recommendation
Display as a bar chart / heatmap strip beneath the year slider: height or color
intensity proportional to % segments in drought. Red bars tower in post-1991 years.
Also usable as a timeline annotation alongside B1 basin-wide SSA.

## Limitations
- Treats all segments equally (a headwater and the mainstem both count as 1)
- Could be weighted by segment length or flow volume for a more nuanced view
