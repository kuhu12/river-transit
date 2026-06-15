import RiverLayer from './RiverLayer'
import ConfluenceLayer from './ConfluenceLayer'
import DamLayer from './DamLayer'
import GeoLayer from './GeoLayer'
import FlowMotionLayer from './FlowMotionLayer'
import type { ReactNode } from 'react'
import { normalize } from '../utils'
import type { SchematicConfluence, SchematicLine, Bounds, ProjectedDam, GeoLayerData, MotionType } from '../types'
import { DEFAULT_WIDTH, DEFAULT_HEIGHT, PADDING } from '../constants'

export type SchematicMapCanvasProps = {
  lines: SchematicLine[]
  confluences?: SchematicConfluence[]
  dams?: ProjectedDam[]
  geoLayers?: GeoLayerData[]
  bounds?: Bounds
  terrainBounds?: Bounds
  hillshadeUrl?: string
  width?: number
  height?: number
  controls?: ReactNode
  vignetteRadius?: number
  vignetteFeather?: number
  motionType?: MotionType
}

function SchematicMapCanvas({
  lines,
  confluences = [],
  dams = [],
  geoLayers = [],
  bounds,
  hillshadeUrl,
  terrainBounds,
  width = DEFAULT_WIDTH,
  height = DEFAULT_HEIGHT,
  controls,
  vignetteRadius = 0.55,
  vignetteFeather = 40,
  motionType = 'none',
}: SchematicMapCanvasProps) {
  const innerWidth = width - PADDING * 2
  const innerHeight = height - PADDING * 2

  const cx = width / 2
  const cy = height / 2

  return (
    <div style={{ position: 'relative', width: '100%', height: '100vh', overflow: 'hidden', background: '#ededed' }}>
      {controls && (
        <div style={{ position: 'absolute', top: 12, right: 12, zIndex: 10 }}>
          {controls}
        </div>
      )}
      <svg
        viewBox={`0 0 ${width} ${height}`}
        width="100%"
        height="100%"
        preserveAspectRatio="xMidYMid slice"
        style={{ display: 'block' }}
      >
        <defs>
          <linearGradient id="riverGradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#0868ac" />
            <stop offset="100%" stopColor="#2b8cbe" />
          </linearGradient>

          <radialGradient id="vignetteFade" cx="50%" cy="50%" r="50%" fx="50%" fy="50%">
            <stop offset="0%" stopColor="#ededed" stopOpacity={1} />
            <stop offset={`${Math.max(0, 100 - vignetteFeather)}%`} stopColor="#ededed" stopOpacity={1} />
            <stop offset="100%" stopColor="#ededed" stopOpacity={0} />
          </radialGradient>
          <mask id="vignetteMask">
            <ellipse
              cx={cx}
              cy={cy}
              rx={width * vignetteRadius}
              ry={height * vignetteRadius}
              fill="url(#vignetteFade)"
            />
          </mask>

          <filter id="featherEdge" colorInterpolationFilters="sRGB">
            <feGaussianBlur in="SourceAlpha" stdDeviation="40" result="blurAlpha" />
            <feComposite in="SourceGraphic" in2="blurAlpha" operator="in" />
          </filter>
        </defs>

        <rect x="0" y="0" width={width} height={height} fill="#ededed" />

        <g mask="url(#vignetteMask)">
          <g transform={`translate(${PADDING}, ${PADDING})`}>
            {hillshadeUrl && (() => {
              if (!bounds || !terrainBounds) {
                return (
                  <image href={hillshadeUrl} x={-PADDING} y={-PADDING}
                    width={width} height={height}
                    preserveAspectRatio="none" opacity={0.25}
                    filter="url(#featherEdge)" />
                )
              }
              const imgX = normalize(terrainBounds.xMin, bounds.xMin, bounds.xMax) * innerWidth
              const imgRight = normalize(terrainBounds.xMax, bounds.xMin, bounds.xMax) * innerWidth
              const imgTop = (1 - normalize(terrainBounds.yMax, bounds.yMin, bounds.yMax)) * innerHeight
              const imgBottom = (1 - normalize(terrainBounds.yMin, bounds.yMin, bounds.yMax)) * innerHeight
              return (
                <image href={hillshadeUrl}
                  x={imgX} y={imgTop}
                  width={imgRight - imgX} height={imgBottom - imgTop}
                  preserveAspectRatio="none" opacity={0.25}
                  filter="url(#featherEdge)" />
              )
            })()}
            {bounds && geoLayers.map((layer) => (
              <GeoLayer
                key={layer.key}
                data={layer.data}
                bounds={bounds}
                width={innerWidth}
                height={innerHeight}
                stroke={layer.stroke}
                fill={layer.fill}
                strokeWidth={layer.strokeWidth}
              />
            ))}
          </g>
        </g>

        <g transform={`translate(${PADDING}, ${PADDING})`}>
          <RiverLayer lines={lines} width={innerWidth} height={innerHeight} motionType={motionType} />
          {dams.length > 0 && <DamLayer dams={dams} />}
          {confluences.length > 0 && <ConfluenceLayer confluences={confluences} />}
        </g>
      </svg>

      {motionType === 'particle' && (
        <FlowMotionLayer
          lines={lines}
          width={width}
          height={height}
          padX={PADDING}
          padY={PADDING}
        />
      )}
    </div>
  )
}

export default SchematicMapCanvas
