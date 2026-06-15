import { normalize } from '../utils'
import type { Bounds, GeoFeatureCollection } from '../types'

type Coordinate = [number, number]
type Ring = Coordinate[]

type GeoJsonGeometry =
  | { type: 'Polygon'; coordinates: Ring[] }
  | { type: 'MultiPolygon'; coordinates: Ring[][] }

export type GeoLayerProps = {
  data: GeoFeatureCollection
  bounds: Bounds
  width: number
  height: number
  stroke: string
  fill?: string
  strokeWidth?: number
}

function projectCoord(
  coord: Coordinate,
  bounds: Bounds,
  width: number,
  height: number,
): [number, number] {
  const x = normalize(coord[0], bounds.xMin, bounds.xMax) * width
  const y = (1 - normalize(coord[1], bounds.yMin, bounds.yMax)) * height
  return [x, y]
}

function ringToPath(ring: Ring, bounds: Bounds, width: number, height: number): string {
  return ring
    .map((coord, i) => {
      const [x, y] = projectCoord(coord, bounds, width, height)
      return `${i === 0 ? 'M' : 'L'} ${x.toFixed(2)} ${y.toFixed(2)}`
    })
    .join(' ') + ' Z'
}

function geometryToPath(geometry: GeoJsonGeometry, bounds: Bounds, width: number, height: number): string {
  if (geometry.type === 'Polygon') {
    return geometry.coordinates.map((ring) => ringToPath(ring, bounds, width, height)).join(' ')
  }
  return geometry.coordinates
    .map((polygon) => polygon.map((ring) => ringToPath(ring, bounds, width, height)).join(' '))
    .join(' ')
}

function GeoLayer({ data, bounds, width, height, stroke, fill = 'none', strokeWidth = 0.5 }: GeoLayerProps) {
  return (
    <g>
      {data.features.map((feature, i) => (
        <path
          key={i}
          d={geometryToPath(feature.geometry as GeoJsonGeometry, bounds, width, height)}
          fill={fill}
          stroke={stroke}
          strokeWidth={strokeWidth}
          strokeLinejoin="round"
        />
      ))}
    </g>
  )
}

export default GeoLayer
