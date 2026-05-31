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
}

export type ParsedRiverFeature = {
  id: string
  riverName: string
  geometry: GeoLineString
  properties: RiverFeatureProperties
}

export type ConfluenceRecord = {
  id: string
  riverName: string
  into: string
  placeName: string
  lon: number
  lat: number
}

export type SchematicConfluence = {
  id: string
  x: number
  y: number
  riverName: string
  into: string
  placeName: string
}

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

export function parseRiverFeatures(collection: RiverFeatureCollection): ParsedRiverFeature[] {
  return collection.features
    .filter((feature) => feature.geometry?.type === 'LineString' && feature.properties?.river_name)
    .map((feature, index) => ({
      id: String(feature.properties.seg_id ?? index),
      riverName: String(feature.properties.river_name),
      geometry: feature.geometry,
      properties: feature.properties,
    }))
}

function parseCsv(text: string): string[][] {
  const rows: string[][] = []
  const lines = text.trim().split(/\r?\n/)

  for (const line of lines) {
    const cells: string[] = []
    let current = ''
    let inQuotes = false

    for (let i = 0; i < line.length; i += 1) {
      const char = line[i]

      if (char === '"') {
        if (inQuotes && line[i + 1] === '"') {
          current += '"'
          i += 1
        } else {
          inQuotes = !inQuotes
        }
      } else if (char === ',' && !inQuotes) {
        cells.push(current.trim())
        current = ''
      } else {
        current += char
      }
    }

    cells.push(current.trim())
    rows.push(cells)
  }

  return rows
}

export function parseConfluenceCsv(csv: string): ConfluenceRecord[] {
  const rows = parseCsv(csv)
  const header = rows[0]?.map((cell) => cell.trim().toLowerCase())

  if (!header) {
    return []
  }

  return rows.slice(1).map((row, index) => {
    const values = Object.fromEntries(header.map((key, columnIndex) => [key, row[columnIndex] ?? '']))
    return {
      id: String(index),
      riverName: String(values.river_name ?? ''),
      into: String(values.confluences_into ?? ''),
      placeName: String(values.place_name ?? ''),
      lon: Number(values.confluence_lon ?? NaN),
      lat: Number(values.confluence_lat ?? NaN),
    }
  })
}

export function parseStreamflowCsv(csv: string, year: number | string = 2022): Map<string, number> {
  const rows = parseCsv(csv)
  const header = rows[0]?.map((cell) => cell.trim().toLowerCase())

  const map = new Map<string, number>()
  if (!header) return map

  const colSegment = header.find((h) => ['segment_id', 'seg_id', 'segmentid', 'segment'].includes(h))
  const colYear = header.find((h) => ['year', 'yr'].includes(h))
  const colFlow = header.find((h) => ['streamflow_m3s', 'streamflow', 'flow', 'discharge'].includes(h))

  for (const row of rows.slice(1)) {
    const values = Object.fromEntries(header.map((key, i) => [key, row[i] ?? '']))
    const seg = (colSegment && values[colSegment]) || values['segment_id'] || values['seg_id'] || ''
    const y = colYear ? Number(values[colYear]) : NaN
    const flow = colFlow ? Number(values[colFlow]) : NaN
    if (!seg) continue
    if (!Number.isFinite(flow)) continue
    if (colYear && Number(year) !== y) continue

    const existing = map.get(seg)
    if (existing == null || flow > existing) {
      map.set(String(seg.substring(0, 4)), flow)
    }
  }

  return map
}

export function getSchematicDimensions(features: ParsedRiverFeature[], width = 760) {
  const bounds = computeBounds(features)
  const height = Math.max(
    Math.round(
      Math.max(240, width * ((bounds.maxLat - bounds.minLat) / Math.max(1e-9, bounds.maxLon - bounds.minLon))),
    ),
    240,
  )

  return { width, height }
}

function computeBounds(features: ParsedRiverFeature[]) {
  let minLon = Number.POSITIVE_INFINITY
  let maxLon = Number.NEGATIVE_INFINITY
  let minLat = Number.POSITIVE_INFINITY
  let maxLat = Number.NEGATIVE_INFINITY

  for (const feature of features) {
    for (const [lon, lat] of feature.geometry.coordinates) {
      if (lon < minLon) minLon = lon
      if (lon > maxLon) maxLon = lon
      if (lat < minLat) minLat = lat
      if (lat > maxLat) maxLat = lat
    }
  }

  return {
    minLon,
    maxLon,
    minLat,
    maxLat,
  }
}

function normalize(value: number, min: number, max: number) {
  if (min === max) return 0.5
  return (value - min) / (max - min)
}

export function buildSchematicLines(
  features: ParsedRiverFeature[],
  width = 760,
  flowMap?: Map<string, number>,
): SchematicLine[] {
  if (features.length === 0) {
    return []
  }

  const bounds = computeBounds(features)
  const { height } = getSchematicDimensions(features, width)

  // determine flow range if provided
  let minFlow = Number.POSITIVE_INFINITY
  let maxFlow = Number.NEGATIVE_INFINITY
  if (flowMap) {
    for (const feature of features) {
      const f = flowMap.get(String(feature.id))
      if (Number.isFinite(f)) {
        minFlow = Math.min(minFlow, f)
        maxFlow = Math.max(maxFlow, f)
      }
    }
    if (!Number.isFinite(minFlow)) minFlow = 0
    if (!Number.isFinite(maxFlow)) maxFlow = minFlow
  }

  const FLOW_MIN_PX = 1
  const FLOW_MAX_PX = 20

  return features.map((feature) => {
    const pathPoints: Point[] = feature.geometry.coordinates.map(([lon, lat]) => {
      const x = clamp(normalize(lon, bounds.minLon, bounds.maxLon) * width, 0, width)
      const y = clamp((1 - normalize(lat, bounds.minLat, bounds.maxLat)) * height, 0, height)
      return { x, y }
    })

    const d = pathPoints.reduce((acc, point, index) => {
      const command = index === 0 ? 'M' : 'L'
      return `${acc} ${command} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`
    }, '')

    const labelPosition = pathPoints[Math.floor(pathPoints.length / 2)]
    const ticks = pathPoints.filter((_, index) => index % Math.max(1, Math.floor(pathPoints.length / 10)) === 0).map((point, index) => ({
      id: `${feature.id}-${index}`,
      x: point.x,
      y: point.y,
    }))

    let strokeWidth: number | undefined = undefined
    if (flowMap) {
      const flow = flowMap.get(String(feature.id))
      if (Number.isFinite(flow)) {
        const norm = minFlow === maxFlow ? 0.5 : (flow - minFlow) / (maxFlow - minFlow)
        strokeWidth = clamp(FLOW_MIN_PX + norm * (FLOW_MAX_PX - FLOW_MIN_PX), FLOW_MIN_PX, FLOW_MAX_PX)
      }
    }

    return {
      id: feature.id,
      name: feature.riverName,
      path: d,
      labelPosition,
      ticks,
      strokeWidth,
    }
  })
}

export function buildSchematicConfluences(
  confluences: ConfluenceRecord[],
  features: ParsedRiverFeature[],
  width = 760,
): SchematicConfluence[] {
  if (confluences.length === 0 || features.length === 0) {
    return []
  }

  const bounds = computeBounds(features)
  const { height } = getSchematicDimensions(features, width)

  return confluences
    .filter((confluence) => Number.isFinite(confluence.lat) && Number.isFinite(confluence.lon))
    .map((confluence) => ({
      id: confluence.id,
      riverName: confluence.riverName,
      into: confluence.into,
      placeName: confluence.placeName,
      x: clamp(normalize(confluence.lon, bounds.minLon, bounds.maxLon) * width, 0, width),
      y: clamp((1 - normalize(confluence.lat, bounds.minLat, bounds.maxLat)) * height, 0, height),
    }))
}
