import type { ProjectedDam } from '../types'

type DamLayerProps = {
  dams: ProjectedDam[]
}

function DamLayer({ dams }: DamLayerProps) {
  return (
    <g>
      {dams.map((dam, i) => (
        <g key={i}>
          <rect
            x={dam.px - 3}
            y={dam.py - 1.5}
            width={6}
            height={3}
            fill="#d35400"
            stroke="#fff"
            strokeWidth={0.4}
          />
        </g>
      ))}
    </g>
  )
}

export default DamLayer
