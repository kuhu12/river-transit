import RiverLayer from './RiverLayer'
import ConfluenceLayer from './ConfluenceLayer'
import type { SchematicConfluence, SchematicLine } from './schematicMapUtils'

const DEFAULT_WIDTH = 860
const DEFAULT_HEIGHT = 420
const PADDING = 30

export type SchematicMapCanvasProps = {
  lines: SchematicLine[]
  confluences?: SchematicConfluence[]
  width?: number
  height?: number
}

function SchematicMapCanvas({
  lines,
  confluences = [],
  width = DEFAULT_WIDTH,
  height = DEFAULT_HEIGHT,
}: SchematicMapCanvasProps) {
  return (
    <div style={{ width: '100%', overflow: 'auto', border: '1px solid #d8d8d8', borderRadius: 12, background: '#fafafa' }}>
      <svg viewBox={`0 0 ${width} ${height}`} width="100%" height="100%" style={{ display: 'block' }}>
        <defs>
          <linearGradient id="riverGradient" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#0868ac" />
            <stop offset="100%" stopColor="#2b8cbe" />
          </linearGradient>
        </defs>

        <rect x="0" y="0" width={width} height={height} fill="#fcfcfc" rx="12" />

        <g transform={`translate(${PADDING}, ${PADDING})`}>
          <RiverLayer lines={lines} width={width - PADDING * 2} height={height - PADDING * 2} />
          <ConfluenceLayer confluences={confluences} />
        </g>
      </svg>
    </div>
  )
}

export default SchematicMapCanvas
