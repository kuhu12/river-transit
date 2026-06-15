export type DroughtAreaChartProps = {
  years: number[]
  droughtExtent: Map<number, number>
  selectedYear: number | null
  onChangeYear: (year: number) => void
}

const CHART_W = 220
const CHART_H = 50
const PAD_L = 0
const PAD_R = 0

function DroughtAreaChart({ years, droughtExtent, selectedYear, onChangeYear }: DroughtAreaChartProps) {
  if (years.length < 2) return null

  const minYear = years[0]
  const maxYear = years[years.length - 1]
  const values = years.map((y) => droughtExtent.get(y) ?? 0)
  const maxVal = Math.max(...values, 1)

  const xScale = (y: number) => PAD_L + ((y - minYear) / (maxYear - minYear)) * (CHART_W - PAD_L - PAD_R)
  const yScale = (v: number) => CHART_H - (v / maxVal) * (CHART_H - 4)

  const areaPoints = years.map((y, i) => `${xScale(y).toFixed(1)},${yScale(values[i]).toFixed(1)}`).join(' ')
  const areaPath = `M ${xScale(minYear).toFixed(1)},${CHART_H} ${areaPoints} L ${xScale(maxYear).toFixed(1)},${CHART_H} Z`
  const linePath = years.map((y, i) => `${i === 0 ? 'M' : 'L'} ${xScale(y).toFixed(1)},${yScale(values[i]).toFixed(1)}`).join(' ')

  const handleClick = (e: React.MouseEvent<SVGSVGElement>) => {
    const rect = e.currentTarget.getBoundingClientRect()
    const xPct = (e.clientX - rect.left) / rect.width
    const targetYear = Math.round(minYear + xPct * (maxYear - minYear))
    const clamped = Math.max(minYear, Math.min(maxYear, targetYear))
    const closest = years.reduce((prev, curr) => (Math.abs(curr - clamped) < Math.abs(prev - clamped) ? curr : prev))
    onChangeYear(closest)
  }

  return (
    <div>
      <span style={{ fontSize: 10, color: '#888', fontWeight: 600 }}>Drought Extent (SSA &lt; -0.5)</span>
      <svg
        width={CHART_W}
        height={CHART_H + 4}
        viewBox={`0 0 ${CHART_W} ${CHART_H + 4}`}
        style={{ display: 'block', cursor: 'pointer' }}
        onClick={handleClick}
      >
        <path d={areaPath} fill="rgba(200,80,60,0.25)" />
        <path d={linePath} fill="none" stroke="#c8503c" strokeWidth={1.2} />
        {selectedYear != null && (
          <line
            x1={xScale(selectedYear)}
            y1={0}
            x2={xScale(selectedYear)}
            y2={CHART_H}
            stroke="#333"
            strokeWidth={1.5}
            strokeDasharray="3,2"
          />
        )}
      </svg>
    </div>
  )
}

export default DroughtAreaChart
