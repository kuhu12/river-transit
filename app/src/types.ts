export type Point = {
  x: number
  y: number
}

export type RiverFeatureProperties = {
  seg_id: string | number
  river_name: string
  tier?: string | number
  BotElev?: number
  TopElev?: number
  [key: string]: unknown
}

export type GeoLineString = {
  type: 'LineString'
  coordinates: [number, number][]
}

export type GeoJsonFeature = {
  type: 'Feature'
  geometry: GeoLineString
  properties: RiverFeatureProperties
}

export type RiverFeatureCollection = {
  type: 'FeatureCollection'
  features: GeoJsonFeature[]
}

export type SchematicTick = {
  id: string
  x: number
  y: number
}

export type SchematicLine = {
  id: string
  name: string
  path: string
  labelPosition?: Point
  ticks: SchematicTick[]
  strokeWidth?: number
  strokeColor?: string
  flow?: number
}

export type ParsedRiverFeature = {
  id: string
  riverName: string
  geometry: GeoLineString
  properties: RiverFeatureProperties
}

export type Bounds = {
  xMin: number
  xMax: number
  yMin: number
  yMax: number
}

export type ConfluenceLcc = {
  id: string
  riverName: string
  into: string
  placeName: string
  x: number
  y: number
}

export type SchematicConfluence = {
  id: string
  x: number
  y: number
  riverName: string
  into: string
  placeName: string
}

export type DamLcc = {
  name: string
  x: number
  y: number
  state?: string
  [key: string]: unknown
}

export type ProjectedDam = {
  name: string
  px: number
  py: number
  state?: string
}

export type AllStreamflowData = {
  years: number[]
  byYear: Map<number, Map<string, number>>
  globalMinFlow: number
  globalMaxFlow: number
}

type Coordinate = [number, number]
type Ring = Coordinate[]

type GeoJsonGeometry =
  | { type: 'Polygon'; coordinates: Ring[] }
  | { type: 'MultiPolygon'; coordinates: Ring[][] }

type GeoFeature = {
  type: 'Feature'
  geometry: GeoJsonGeometry
  properties?: Record<string, unknown>
}

export type GeoFeatureCollection = {
  type: 'FeatureCollection'
  features: GeoFeature[]
}

export type GeoLayerData = {
  key: string
  data: GeoFeatureCollection
  stroke: string
  fill?: string
  strokeWidth?: number
}

export type VizMode = 'normal' | 'ab'
export type MotionType = 'none' | 'dash' | 'particle'

export type BaselineData = Record<string, number>

export type SegmentSSAData = {
  years: number[]
  byYear: Map<number, Map<string, number>>
}

export type BasinSSAYear = {
  ssa: number
  drought: boolean
}

export type BasinSSAData = {
  byYear: Map<number, BasinSSAYear>
}
