import type { SchematicLine, MotionType } from '../types'

export type RiverLayerProps = {
  lines: SchematicLine[]
  width: number
  height: number
  motionType?: MotionType
}

const dashKeyframes = `
@keyframes dashFlow {
  to { stroke-dashoffset: -40; }
}
`

function RiverLayer({ lines, width, height, motionType = 'none' }: RiverLayerProps) {
  const useDash = motionType === 'dash'

  return (
    <g>
      {useDash && <style>{dashKeyframes}</style>}
      {lines.map((line) => {
        const sw = line.strokeWidth ?? 1
        const color = line.strokeColor ?? '#2b8cbe'
        const flow = line.flow ?? 1
        const duration = Math.max(0.3, 3 / Math.sqrt(Math.max(flow, 0.1)))

        return (
          <g key={line.id}>
            <path
              d={line.path}
              fill="none"
              stroke={color}
              strokeWidth={sw}
              strokeLinecap="round"
              strokeLinejoin="round"
              {...(useDash ? {
                strokeDasharray: `${Math.max(4, sw * 2)},${Math.max(6, sw * 3)}`,
                style: { animation: `dashFlow ${duration.toFixed(2)}s linear infinite` },
              } : {})}
            />
          </g>
        )
      })}
    </g>
  )
}

export default RiverLayer
