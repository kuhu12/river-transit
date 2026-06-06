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

export type AllStreamflowData = {
  years: number[]
  byYear: Map<number, Map<string, number>>
  globalMinFlow: number
  globalMaxFlow: number
}

export function parseAllStreamflowByYear(csv: string): AllStreamflowData {
  const rows = parseCsv(csv)
  const header = rows[0]?.map((cell) => cell.trim().toLowerCase())

  const byYear = new Map<number, Map<string, number>>()
  if (!header) return { years: [], byYear, globalMinFlow: 0, globalMaxFlow: 0 }

  const colSegIdx = header.findIndex((h) => ['segment_id', 'seg_id', 'segmentid', 'segment'].includes(h))
  const colYearIdx = header.findIndex((h) => ['year', 'yr'].includes(h))
  const colDateIdx = header.findIndex((h) => h === 'date')
  const colFlowIdx = header.findIndex((h) => ['streamflow_m3s', 'streamflow', 'flow', 'discharge'].includes(h))

  let globalMinFlow = Number.POSITIVE_INFINITY
  let globalMaxFlow = Number.NEGATIVE_INFINITY

  for (const row of rows.slice(1)) {
    const seg = colSegIdx >= 0 ? row[colSegIdx] : ''
    const flow = colFlowIdx >= 0 ? Number(row[colFlowIdx]) : NaN
    if (!seg || !Number.isFinite(flow)) continue

    let rowYear: number = NaN
    if (colYearIdx >= 0) {
      rowYear = Number(row[colYearIdx])
    } else if (colDateIdx >= 0) {
      rowYear = Number(row[colDateIdx].substring(0, 4))
    }
    if (!Number.isFinite(rowYear)) continue

    if (flow < globalMinFlow) globalMinFlow = flow
    if (flow > globalMaxFlow) globalMaxFlow = flow

    let yearMap = byYear.get(rowYear)
    if (!yearMap) {
      yearMap = new Map()
      byYear.set(rowYear, yearMap)
    }

    const segKey = String(seg).substring(0, 4)
    const existing = yearMap.get(segKey)
    if (existing == null || flow > existing) {
      yearMap.set(segKey, flow)
    }
  }

  if (!Number.isFinite(globalMinFlow)) globalMinFlow = 0
  if (!Number.isFinite(globalMaxFlow)) globalMaxFlow = 0

  const years = Array.from(byYear.keys()).sort((a, b) => a - b)
  return { years, byYear, globalMinFlow, globalMaxFlow }
}

export function applyFlowWidths(
  lines: SchematicLine[],
  flowMap: Map<string, number> | undefined,
  globalMinFlow: number,
  globalMaxFlow: number,
  logScale = true,
): SchematicLine[] {
  if (!flowMap) return lines.map((l) => ({ ...l, strokeWidth: undefined }))

  const FLOW_MIN_PX = 0.5
  const FLOW_MAX_PX = 10

  const scale = (v: number) => logScale ? Math.log(Math.max(v, 1e-9)) : v
  const scaleMin = scale(globalMinFlow)
  const scaleMax = scale(globalMaxFlow)

  return lines.map((line) => {
    const flow = flowMap.get(line.id)
    if (!Number.isFinite(flow)) return { ...line, strokeWidth: undefined }
    const norm = scaleMin === scaleMax ? 0.5 : (scale(flow!) - scaleMin) / (scaleMax - scaleMin)
    return { ...line, strokeWidth: clamp(FLOW_MIN_PX + norm * (FLOW_MAX_PX - FLOW_MIN_PX), FLOW_MIN_PX, FLOW_MAX_PX) }
  })
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
  straightLines = true,
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
  const FLOW_MAX_PX = 12

  const project = ([lon, lat]: [number, number]): Point => ({
    x: clamp(normalize(lon, bounds.minLon, bounds.maxLon) * width, 0, width),
    y: clamp((1 - normalize(lat, bounds.minLat, bounds.maxLat)) * height, 0, height),
  })

  return features.map((feature) => {
    const coords = feature.geometry.coordinates
    const start = project(coords[0])
    const end = project(coords[coords.length - 1])

    let d: string
    let labelPosition: Point
    let ticks: SchematicTick[]

    if (straightLines) {
      d = `M ${start.x.toFixed(2)} ${start.y.toFixed(2)} L ${end.x.toFixed(2)} ${end.y.toFixed(2)}`
      labelPosition = { x: (start.x + end.x) / 2, y: (start.y + end.y) / 2 }
      ticks = []
    } else {
      const pathPoints = coords.map(project)
      d = pathPoints.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(2)} ${p.y.toFixed(2)}`).join(' ')
      labelPosition = pathPoints[Math.floor(pathPoints.length / 2)]
      ticks = pathPoints
        .filter((_, i) => i % Math.max(1, Math.floor(pathPoints.length / 10)) === 0)
        .map((p, i) => ({ id: `${feature.id}-${i}`, x: p.x, y: p.y }))
    }

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
