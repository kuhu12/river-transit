import type {
  Bounds,
  Point,
  RiverFeatureCollection,
  ParsedRiverFeature,
  SchematicLine,
  SchematicTick,
  ConfluenceLcc,
  SchematicConfluence,
  DamLcc,
  ProjectedDam,
  AllStreamflowData,
  GeoFeatureCollection,
  BaselineData,
  SegmentSSAData,
  BasinSSAData,
} from './types'

const clamp = (value: number, min: number, max: number) => Math.max(min, Math.min(max, value))

export function normalize(value: number, min: number, max: number) {
  if (min === max) return 0.5
  return (value - min) / (max - min)
}

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

export function getDimensions(bounds: Bounds, width = 760) {
  const dx = bounds.xMax - bounds.xMin
  const dy = bounds.yMax - bounds.yMin
  const height = Math.max(Math.round(width * (dy / Math.max(1e-9, dx))), 240)
  return { width, height }
}

export function computeBounds(features: ParsedRiverFeature[]): Bounds {
  let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity

  for (const feature of features) {
    for (const [x, y] of feature.geometry.coordinates) {
      if (x < xMin) xMin = x
      if (x > xMax) xMax = x
      if (y < yMin) yMin = y
      if (y > yMax) yMax = y
    }
  }

  return { xMin, xMax, yMin, yMax }
}

export function computeGeoBounds(data: GeoFeatureCollection, padding = 0.15): Bounds {
  let xMin = Infinity, xMax = -Infinity, yMin = Infinity, yMax = -Infinity
  function walk(coords: unknown) {
    if (Array.isArray(coords) && typeof coords[0] === 'number') {
      const [x, y] = coords as [number, number]
      if (x < xMin) xMin = x; if (x > xMax) xMax = x
      if (y < yMin) yMin = y; if (y > yMax) yMax = y
    } else if (Array.isArray(coords)) {
      for (const c of coords) walk(c)
    }
  }
  for (const feat of data.features) walk(feat.geometry.coordinates)
  const dx = xMax - xMin, dy = yMax - yMin
  return {
    xMin: xMin - dx * padding, xMax: xMax + dx * padding,
    yMin: yMin - dy * padding, yMax: yMax + dy * padding,
  }
}

function projectLcc(coord: [number, number], bounds: Bounds, width: number, height: number): Point {
  return {
    x: normalize(coord[0], bounds.xMin, bounds.xMax) * width,
    y: (1 - normalize(coord[1], bounds.yMin, bounds.yMax)) * height,
  }
}

export function buildSchematicLines(
  features: ParsedRiverFeature[],
  bounds: Bounds,
  width = 760,
  flowMap?: Map<string, number>,
  straightLines = true,
): SchematicLine[] {
  if (features.length === 0) {
    return []
  }

  const { height } = getDimensions(bounds, width)

  let minFlow = Number.POSITIVE_INFINITY
  let maxFlow = Number.NEGATIVE_INFINITY
  if (flowMap) {
    for (const feature of features) {
      const f = flowMap.get(String(feature.id))
      if (f != null && Number.isFinite(f)) {
        minFlow = Math.min(minFlow, f)
        maxFlow = Math.max(maxFlow, f)
      }
    }
    if (!Number.isFinite(minFlow)) minFlow = 0
    if (!Number.isFinite(maxFlow)) maxFlow = minFlow
  }

  const FLOW_MIN_PX = 1
  const FLOW_MAX_PX = 12

  return features.map((feature) => {
    const coords = feature.geometry.coordinates
    const start = projectLcc(coords[0], bounds, width, height)
    const end = projectLcc(coords[coords.length - 1], bounds, width, height)

    let d: string
    let labelPosition: Point
    let ticks: SchematicTick[]

    if (straightLines) {
      d = `M ${start.x.toFixed(2)} ${start.y.toFixed(2)} L ${end.x.toFixed(2)} ${end.y.toFixed(2)}`
      labelPosition = { x: (start.x + end.x) / 2, y: (start.y + end.y) / 2 }
      ticks = []
    } else {
      const pathPoints = coords.map((c) => projectLcc(c, bounds, width, height))
      d = pathPoints.map((p, i) => `${i === 0 ? 'M' : 'L'} ${p.x.toFixed(2)} ${p.y.toFixed(2)}`).join(' ')
      labelPosition = pathPoints[Math.floor(pathPoints.length / 2)]
      ticks = pathPoints
        .filter((_, i) => i % Math.max(1, Math.floor(pathPoints.length / 10)) === 0)
        .map((p, i) => ({ id: `${feature.id}-${i}`, x: p.x, y: p.y }))
    }

    let strokeWidth: number | undefined = undefined
    if (flowMap) {
      const flow = flowMap.get(String(feature.id))
      if (flow != null && Number.isFinite(flow)) {
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

export function buildConfluences(
  confluences: ConfluenceLcc[],
  bounds: Bounds,
  width = 760,
): SchematicConfluence[] {
  const { height } = getDimensions(bounds, width)

  return confluences
    .filter((c) => Number.isFinite(c.x) && Number.isFinite(c.y))
    .map((c) => {
      const pt = projectLcc([c.x, c.y], bounds, width, height)
      return {
        id: c.id,
        riverName: c.riverName,
        into: c.into,
        placeName: c.placeName,
        x: pt.x,
        y: pt.y,
      }
    })
}

export function buildDams(
  dams: DamLcc[],
  bounds: Bounds,
  width = 760,
): ProjectedDam[] {
  const { height } = getDimensions(bounds, width)

  return dams
    .filter((d) => Number.isFinite(d.x) && Number.isFinite(d.y))
    .map((d) => {
      const pt = projectLcc([d.x, d.y], bounds, width, height)
      return {
        name: d.name,
        px: pt.x,
        py: pt.y,
        state: d.state,
      }
    })
}

// --- A+B Method utilities ---

export function applyFlowWidthsSqrt(
  lines: SchematicLine[],
  flowMap: Map<string, number> | undefined,
  globalMinFlow: number,
  globalMaxFlow: number,
): SchematicLine[] {
  if (!flowMap) return lines.map((l) => ({ ...l, strokeWidth: undefined }))

  const FLOW_MIN_PX = 0.5
  const FLOW_MAX_PX = 10

  const sqrtMin = Math.sqrt(Math.max(globalMinFlow, 0))
  const sqrtMax = Math.sqrt(Math.max(globalMaxFlow, 0))

  return lines.map((line) => {
    const flow = flowMap.get(line.id)
    if (!Number.isFinite(flow)) return { ...line, strokeWidth: undefined, flow: undefined }
    const sqrtFlow = Math.sqrt(Math.max(flow!, 0))
    const norm = sqrtMin === sqrtMax ? 0.5 : (sqrtFlow - sqrtMin) / (sqrtMax - sqrtMin)
    return {
      ...line,
      strokeWidth: clamp(FLOW_MIN_PX + norm * (FLOW_MAX_PX - FLOW_MIN_PX), FLOW_MIN_PX, FLOW_MAX_PX),
      flow: flow,
    }
  })
}

function lerpColor(a: [number, number, number], b: [number, number, number], t: number): string {
  const r = Math.round(a[0] + (b[0] - a[0]) * t)
  const g = Math.round(a[1] + (b[1] - a[1]) * t)
  const bl = Math.round(a[2] + (b[2] - a[2]) * t)
  return `rgb(${r},${g},${bl})`
}

export function computeVitalityColor(flow: number, baselineMean: number): string {
  if (!Number.isFinite(flow) || !Number.isFinite(baselineMean) || baselineMean <= 0) return '#2b8cbe'
  const ratio = (flow / baselineMean) * 100 // percentage of baseline

  const red: [number, number, number] = [200, 40, 40]
  const white: [number, number, number] = [240, 240, 240]
  const blue: [number, number, number] = [30, 100, 180]

  if (ratio <= 50) return lerpColor(red, red, 0) // deep red
  if (ratio <= 100) {
    const t = (ratio - 50) / 50
    return lerpColor(red, white, t)
  }
  if (ratio <= 150) {
    const t = (ratio - 100) / 50
    return lerpColor(white, blue, t)
  }
  return lerpColor(blue, blue, 0) // deep blue
}

export function applyVitalityColors(
  lines: SchematicLine[],
  flowMap: Map<string, number> | undefined,
  baseline: BaselineData,
): SchematicLine[] {
  if (!flowMap) return lines
  return lines.map((line) => {
    const flow = flowMap.get(line.id)
    const base = baseline[line.id]
    if (flow == null || base == null) return line
    return { ...line, strokeColor: computeVitalityColor(flow, base) }
  })
}

export function parseSegmentSSA(csv: string): SegmentSSAData {
  const rows = parseCsv(csv)
  const header = rows[0]?.map((c) => c.trim().toLowerCase())
  if (!header) return { years: [], byYear: new Map() }

  const segIdx = header.indexOf('seg_id')
  const yearIdx = header.indexOf('year')
  const ssaIdx = header.indexOf('ssa')

  const byYear = new Map<number, Map<string, number>>()

  for (const row of rows.slice(1)) {
    const seg = segIdx >= 0 ? row[segIdx]?.split('.')[0] : ''
    const year = yearIdx >= 0 ? Number(row[yearIdx]) : NaN
    const ssa = ssaIdx >= 0 ? Number(row[ssaIdx]) : NaN
    if (!seg || !Number.isFinite(year) || !Number.isFinite(ssa)) continue

    let yearMap = byYear.get(year)
    if (!yearMap) {
      yearMap = new Map()
      byYear.set(year, yearMap)
    }
    yearMap.set(seg, ssa)
  }

  const years = Array.from(byYear.keys()).sort((a, b) => a - b)
  return { years, byYear }
}

export function parseBasinSSA(csv: string): BasinSSAData {
  const rows = parseCsv(csv)
  const header = rows[0]?.map((c) => c.trim().toLowerCase())
  if (!header) return { byYear: new Map() }

  const yearIdx = header.indexOf('year')
  const ssaIdx = header.indexOf('ssa')
  const droughtIdx = header.indexOf('drought')

  const byYear = new Map<number, { ssa: number; drought: boolean }>()

  for (const row of rows.slice(1)) {
    const year = yearIdx >= 0 ? Number(row[yearIdx]) : NaN
    const ssa = ssaIdx >= 0 ? Number(row[ssaIdx]) : NaN
    const drought = droughtIdx >= 0 ? row[droughtIdx]?.trim().toLowerCase() === 'true' : false
    if (!Number.isFinite(year)) continue
    byYear.set(year, { ssa, drought })
  }

  return { byYear }
}

export function computeDroughtExtent(
  segmentSSA: SegmentSSAData,
): Map<number, number> {
  const result = new Map<number, number>()
  for (const [year, segMap] of segmentSSA.byYear) {
    let count = 0
    for (const ssa of segMap.values()) {
      if (ssa < -0.5) count++
    }
    result.set(year, count)
  }
  return result
}

export function getWorstYears(basinSSA: BasinSSAData): number[] {
  const worst: number[] = []
  for (const [year, data] of basinSSA.byYear) {
    if (data.drought) worst.push(year)
  }
  return worst.sort((a, b) => a - b)
}
