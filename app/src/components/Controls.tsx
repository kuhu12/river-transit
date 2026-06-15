import { useState } from 'react'
import { TOGGLE_CONFIG, GEO_LAYER_CONFIG } from '../constants'
import type { VizMode, MotionType } from '../types'
import YearSlider from './YearSlider'
import DroughtAreaChart from './DroughtAreaChart'

export type ControlsProps = {
  layerVisibility: Record<string, boolean>
  onToggleLayer: (key: string) => void
  straightLines: boolean
  onToggleStraightLines: () => void
  strokeWidths: Record<string, number>
  onChangeStrokeWidth: (key: string, value: number) => void
  zoom: number
  onChangeZoom: (value: number) => void
  vignetteRadius: number
  onChangeVignetteRadius: (value: number) => void
  vignetteFeather: number
  onChangeVignetteFeather: (value: number) => void
  years: number[]
  selectedYear: number | null
  onChangeYear: (year: number) => void
  playing: boolean
  onTogglePlay: () => void
  logScale: boolean
  onToggleLogScale: () => void
  vizMode: VizMode
  onChangeVizMode: (mode: VizMode) => void
  motionType: MotionType
  onChangeMotionType: (type: MotionType) => void
  worstYears: number[]
  droughtExtent: Map<number, number>
  vitalityEnabled: boolean
  onToggleVitality: () => void
}

const pillStyle = (active: boolean, color: string) => ({
  padding: '6px 14px',
  border: `2px solid ${color}`,
  borderRadius: 6,
  background: active ? color : 'transparent',
  color: active ? '#fff' : color,
  cursor: 'pointer' as const,
  fontSize: 13,
  fontWeight: 600 as const,
  transition: 'all 0.15s ease',
})

function Controls({
  layerVisibility, onToggleLayer, straightLines, onToggleStraightLines,
  strokeWidths, onChangeStrokeWidth, zoom, onChangeZoom,
  vignetteRadius, onChangeVignetteRadius, vignetteFeather, onChangeVignetteFeather,
  years, selectedYear, onChangeYear, playing, onTogglePlay, logScale, onToggleLogScale,
  vizMode, onChangeVizMode, motionType, onChangeMotionType,
  worstYears, droughtExtent, vitalityEnabled, onToggleVitality,
}: ControlsProps) {
  const [controlsOpen, setControlsOpen] = useState(false)
  const isAB = vizMode === 'ab'

  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 6 }}>
      {/* Mode toggle - always visible */}
      <div style={{ display: 'flex', gap: 4 }}>
        <button onClick={() => onChangeVizMode('normal')} style={pillStyle(vizMode === 'normal', '#555')}>
          Normal
        </button>
        <button onClick={() => onChangeVizMode('ab')} style={pillStyle(vizMode === 'ab', '#8b5cf6')}>
          A+B Method
        </button>
        <button
          onClick={() => setControlsOpen((prev) => !prev)}
          style={{
            padding: '6px 14px',
            background: controlsOpen ? '#333' : '#fff',
            color: controlsOpen ? '#fff' : '#333',
            border: '1px solid #ccc',
            borderRadius: 6,
            cursor: 'pointer',
            fontSize: 13,
            fontWeight: 600,
            boxShadow: '0 2px 8px rgba(0,0,0,0.12)',
          }}
        >
          Controls
        </button>
      </div>

      {controlsOpen && (
        <div
          style={{
            background: '#fff',
            border: '1px solid #ddd',
            borderRadius: 10,
            padding: '10px 12px',
            display: 'flex',
            flexDirection: 'column',
            gap: 6,
            boxShadow: '0 4px 16px rgba(0,0,0,0.14)',
            minWidth: 220,
            maxHeight: '80vh',
            overflowY: 'auto',
          }}
        >
          {/* Year slider + play */}
          {years.length >= 2 && selectedYear != null && (
            <>
              <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                <button
                  onClick={onTogglePlay}
                  style={{
                    width: 32, height: 32, borderRadius: '50%',
                    border: '2px solid #2b8cbe',
                    background: playing ? '#2b8cbe' : 'transparent',
                    color: playing ? '#fff' : '#2b8cbe',
                    cursor: 'pointer', fontSize: 14,
                    display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0,
                  }}
                  aria-label={playing ? 'Pause' : 'Play'}
                >
                  {playing ? '\u23F8' : '\u25B6'}
                </button>
                <div style={{ flex: 1 }}>
                  <YearSlider
                    years={years}
                    value={selectedYear}
                    onChange={onChangeYear}
                    worstYears={isAB ? worstYears : undefined}
                  />
                </div>
              </div>

              {/* Drought area chart - A+B only */}
              {isAB && droughtExtent.size > 0 && (
                <DroughtAreaChart
                  years={years}
                  droughtExtent={droughtExtent}
                  selectedYear={selectedYear}
                  onChangeYear={onChangeYear}
                />
              )}

              {!isAB && (
                <button
                  onClick={onToggleLogScale}
                  title={logScale ? 'Switch to linear scale' : 'Switch to log scale'}
                  style={pillStyle(logScale, '#7f8c8d')}
                >
                  {logScale ? 'Log scale' : 'Linear scale'}
                </button>
              )}
              <hr style={{ width: '100%', border: 'none', borderTop: '1px solid #eee', margin: '4px 0' }} />
            </>
          )}

          {/* A+B specific controls */}
          {isAB && (
            <>
              <span style={{ fontSize: 12, fontWeight: 600, color: '#8b5cf6' }}>A+B Method</span>
              <button onClick={onToggleVitality} style={pillStyle(vitalityEnabled, '#8b5cf6')}>
                {vitalityEnabled ? 'Vitality Color ON' : 'Vitality Color OFF'}
              </button>
              <div style={{ display: 'flex', gap: 4 }}>
                {(['none', 'dash', 'particle'] as MotionType[]).map((mt) => (
                  <button
                    key={mt}
                    onClick={() => onChangeMotionType(mt)}
                    style={{
                      ...pillStyle(motionType === mt, '#8b5cf6'),
                      padding: '4px 8px',
                      fontSize: 11,
                      flex: 1,
                    }}
                  >
                    {mt === 'none' ? 'No Motion' : mt === 'dash' ? 'Dash' : 'Particle'}
                  </button>
                ))}
              </div>
              <hr style={{ width: '100%', border: 'none', borderTop: '1px solid #eee', margin: '4px 0' }} />
            </>
          )}

          {/* Layer toggles */}
          {TOGGLE_CONFIG.map((cfg) => (
            <button
              key={cfg.key}
              onClick={() => onToggleLayer(cfg.key)}
              style={pillStyle(!!layerVisibility[cfg.key], cfg.color)}
            >
              {cfg.label}
            </button>
          ))}

          <hr style={{ width: '100%', border: 'none', borderTop: '1px solid #eee', margin: '4px 0' }} />

          <button onClick={onToggleStraightLines} style={pillStyle(straightLines, '#2b8cbe')}>
            Schematic View
          </button>

          <hr style={{ width: '100%', border: 'none', borderTop: '1px solid #eee', margin: '4px 0' }} />

          <span style={{ fontSize: 12, fontWeight: 600, color: '#555' }}>Zoom</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <input type="range" min={0.5} max={4} step={0.1} value={zoom}
              onChange={(e) => onChangeZoom(Number(e.target.value))} style={{ flex: 1, cursor: 'pointer' }} />
            <span style={{ fontSize: 10, color: '#999', minWidth: 24, textAlign: 'right' }}>{zoom.toFixed(1)}x</span>
          </div>

          <hr style={{ width: '100%', border: 'none', borderTop: '1px solid #eee', margin: '4px 0' }} />

          <span style={{ fontSize: 12, fontWeight: 600, color: '#555' }}>Vignette</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ fontSize: 11, color: '#666', minWidth: 50 }}>Radius</span>
            <input type="range" min={0.3} max={1.5} step={0.05} value={vignetteRadius}
              onChange={(e) => onChangeVignetteRadius(Number(e.target.value))} style={{ flex: 1, cursor: 'pointer' }} />
            <span style={{ fontSize: 10, color: '#999', minWidth: 24, textAlign: 'right' }}>{vignetteRadius.toFixed(2)}</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ fontSize: 11, color: '#666', minWidth: 50 }}>Feather</span>
            <input type="range" min={0} max={100} step={1} value={vignetteFeather}
              onChange={(e) => onChangeVignetteFeather(Number(e.target.value))} style={{ flex: 1, cursor: 'pointer' }} />
            <span style={{ fontSize: 10, color: '#999', minWidth: 24, textAlign: 'right' }}>{vignetteFeather}</span>
          </div>

          <hr style={{ width: '100%', border: 'none', borderTop: '1px solid #eee', margin: '4px 0' }} />

          <span style={{ fontSize: 12, fontWeight: 600, color: '#555' }}>Stroke Width</span>
          {GEO_LAYER_CONFIG.map((cfg) => (
            <div key={cfg.key} style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <span style={{ fontSize: 11, color: '#666', minWidth: 50 }}>{cfg.label}</span>
              <input type="range" min={0.1} max={5} step={0.1}
                value={strokeWidths[cfg.key] ?? cfg.strokeWidth}
                onChange={(e) => onChangeStrokeWidth(cfg.key, Number(e.target.value))}
                style={{ flex: 1, cursor: 'pointer' }} />
              <span style={{ fontSize: 10, color: '#999', minWidth: 24, textAlign: 'right' }}>
                {(strokeWidths[cfg.key] ?? cfg.strokeWidth).toFixed(1)}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

export default Controls
