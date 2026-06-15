export type YearSliderProps = {
  years: number[]
  value: number
  onChange: (year: number) => void
  worstYears?: number[]
}

function YearSlider({ years, value, onChange, worstYears = [] }: YearSliderProps) {
  if (years.length < 2) return null

  const minYear = years[0]
  const maxYear = years[years.length - 1]
  const range = maxYear - minYear
  const worstSet = new Set(worstYears)

  return (
    <div style={{ padding: '4px 0 8px' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 6 }}>
        <span style={{ fontWeight: 700, fontSize: 18, fontVariantNumeric: 'tabular-nums' }}>{value}</span>
        <span style={{ color: '#888', fontSize: 12, alignSelf: 'flex-end' }}>
          {minYear}–{maxYear}
        </span>
      </div>

      <input
        type="range"
        list="year-slider-ticks"
        min={minYear}
        max={maxYear}
        step={1}
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        style={{ width: '100%', cursor: 'pointer' }}
      />
      <datalist id="year-slider-ticks">
        {years.map((y) => (
          <option key={y} value={y} />
        ))}
      </datalist>

      <div style={{ position: 'relative', height: 22, marginTop: 2 }}>
        {years.map((y) => {
          const pct = ((y - minYear) / range) * 100
          const isDecade = y % 10 === 0
          const isWorst = worstSet.has(y)
          return (
            <div
              key={y}
              style={{
                position: 'absolute',
                left: `${pct}%`,
                transform: 'translateX(-50%)',
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
              }}
            >
              {isWorst && (
                <div
                  style={{
                    width: 5,
                    height: 5,
                    borderRadius: '50%',
                    backgroundColor: '#c8503c',
                    marginBottom: 1,
                  }}
                  title={`Drought year: ${y}`}
                />
              )}
              <div
                style={{
                  width: 1,
                  height: isDecade ? 8 : 3,
                  backgroundColor: isWorst ? '#c8503c' : isDecade ? '#555' : '#bbb',
                }}
              />
              {isDecade && (
                <span style={{ fontSize: 9, color: '#666', marginTop: 1, whiteSpace: 'nowrap' }}>{y}</span>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}

export default YearSlider
