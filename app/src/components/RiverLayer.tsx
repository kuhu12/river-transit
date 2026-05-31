import type { SchematicLine } from './schematicMapUtils'

const LABEL_OFFSET = 10

export type RiverLayerProps = {
  lines: SchematicLine[]
  width: number
  height: number
}

function RiverLayer({ lines, width, height }: RiverLayerProps) {
  console.log("Lines", lines)
  return (
    <g>
      {lines.map((line) => (
        <g key={line.id}>
          <path
            d={line.path}
            fill="none"
            stroke="#2b8cbe"
            // stroke="url(#riverGradient)"
            strokeWidth={line.strokeWidth ?? 4}
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          {/* {line.labelPosition && (
            <text
              x={line.labelPosition.x}
              y={line.labelPosition.y}
              fontSize={12}
              fill="#2c3e50"
              fontWeight="600"
              textAnchor="middle"
            >
              {line.name}
            </text>
          )} */}
        </g>
      ))}
      {/* <g>
        {lines.map((line) =>
          line.ticks.map((tick) => (
            <circle key={`${line.id}-${tick.id}`} cx={tick.x} cy={tick.y} r={2.8} fill="#fff" stroke="#2b8cbe" strokeWidth={1.2} />
          )),
        )}
      </g> */}
    </g>
  )
}

export default RiverLayer
