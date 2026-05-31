import type { SchematicConfluence } from './schematicMapUtils'

type ConfluenceLayerProps = {
  confluences: SchematicConfluence[]
}

function ConfluenceLayer({ confluences }: ConfluenceLayerProps) {
  return (
    <g>
      {confluences.map((confluence) => (
        <g key={confluence.id}>
          <circle cx={confluence.x} cy={confluence.y} r={5} fill="#e45756" stroke="#fff" strokeWidth={1.6} />
          <text x={confluence.x} y={confluence.y - 10} fontSize={10} fill="#333" textAnchor="middle" pointerEvents="none">
            {confluence.placeName}
          </text>
        </g>
      ))}
    </g>
  )
}

export default ConfluenceLayer
