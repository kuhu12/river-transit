import { useEffect, useMemo, useRef, useState } from 'react'
import {
  buildSchematicLines,
  buildConfluences,
  buildDams,
  getDimensions,
  parseRiverFeatures,
  computeGeoBounds,
  parseAllStreamflowByYear,
  applyFlowWidths,
  applyFlowWidthsSqrt,
  applyVitalityColors,
  parseSegmentSSA,
  parseBasinSSA,
  computeDroughtExtent,
  getWorstYears,
} from '../utils'
import type {
  Bounds,
  ConfluenceLcc,
  DamLcc,
  RiverFeatureCollection,
  SchematicLine,
  GeoLayerData,
  GeoFeatureCollection,
  AllStreamflowData,
  VizMode,
  MotionType,
  BaselineData,
  SegmentSSAData,
  BasinSSAData,
} from '../types'
import {
  RIVER_PATH,
  CONFLUENCE_PATH,
  DAM_PATH,
  TERRAIN_BOUNDS_PATH,
  HILLSHADE_URL,
  STREAMFLOW_PATH,
  GEO_LAYER_CONFIG,
  DEFAULT_LAYER_VISIBILITY,
  DEFAULT_STROKE_WIDTHS,
  BASELINE_PATH,
  SEGMENT_SSA_PATH,
  BASIN_SSA_PATH,
} from '../constants'
import SchematicMapCanvas from '../components/SchematicMapCanvas'
import Controls from '../components/Controls'

export type MapPageProps = {
  basin: string
}

function MapPage({ basin }: MapPageProps) {
  const containerRef = useRef<HTMLElement>(null)
  const [containerWidth, setContainerWidth] = useState<number>(window.innerWidth)

  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const ro = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setContainerWidth(entry.contentRect.width)
      }
    })
    ro.observe(el)
    return () => ro.disconnect()
  }, [])

  const projectionWidth = Math.max(100, containerWidth - 60)

  const [geoJson, setGeoJson] = useState<RiverFeatureCollection | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState<boolean>(false)
  const [terrainBounds, setTerrainBounds] = useState<Bounds | null>(null)
  const [confluenceData, setConfluenceData] = useState<ConfluenceLcc[]>([])
  const [damData, setDamData] = useState<DamLcc[]>([])
  const [straightLines, setStraightLines] = useState<boolean>(false)
  const [zoom, setZoom] = useState<number>(1)
  const [vignetteRadius, setVignetteRadius] = useState<number>(0.55)
  const [vignetteFeather, setVignetteFeather] = useState<number>(40)

  const [streamflowData, setStreamflowData] = useState<AllStreamflowData | null>(null)
  const [selectedYear, setSelectedYear] = useState<number | null>(null)
  const [playing, setPlaying] = useState(false)
  const [logScale, setLogScale] = useState(true)

  const [geoLayerDataMap, setGeoLayerDataMap] = useState<Record<string, GeoFeatureCollection>>({})
  const [layerVisibility, setLayerVisibility] = useState<Record<string, boolean>>(DEFAULT_LAYER_VISIBILITY)
  const [strokeWidths, setStrokeWidths] = useState<Record<string, number>>(DEFAULT_STROKE_WIDTHS)

  // A+B mode state
  const [vizMode, setVizModeRaw] = useState<VizMode>('normal')
  const [motionType, setMotionType] = useState<MotionType>('none')
  const [vitalityEnabled, setVitalityEnabled] = useState(true)

  const setVizMode = (mode: VizMode) => {
    setVizModeRaw(mode)
    if (mode === 'ab') {
      setLayerVisibility((prev) => ({ ...prev, dams: false }))
    } else {
      setLayerVisibility((prev) => ({ ...prev, dams: true }))
    }
  }
  const [baselineData, setBaselineData] = useState<BaselineData | null>(null)
  const [segmentSSA, setSegmentSSA] = useState<SegmentSSAData | null>(null)
  const [basinSSA, setBasinSSA] = useState<BasinSSAData | null>(null)

  // --- Data loading ---

  useEffect(() => {
    fetch(TERRAIN_BOUNDS_PATH)
      .then((r) => r.json())
      .then((data: { xmin: number; xmax: number; ymin: number; ymax: number }) => {
        setTerrainBounds({ xMin: data.xmin, xMax: data.xmax, yMin: data.ymin, yMax: data.ymax })
      })
      .catch(() => {})
  }, [])

  useEffect(() => {
    for (const layer of GEO_LAYER_CONFIG) {
      fetch(layer.path)
        .then((res) => {
          if (!res.ok) throw new Error(`Failed to load ${layer.path}`)
          return res.json()
        })
        .then((data) => {
          setGeoLayerDataMap((prev) => ({ ...prev, [layer.key]: data as GeoFeatureCollection }))
        })
        .catch(() => {})
    }
  }, [])

  useEffect(() => {
    setLoading(true)
    setError(null)
    fetch(RIVER_PATH)
      .then((r) => {
        if (!r.ok) throw new Error(`Failed to load ${RIVER_PATH}`)
        return r.json()
      })
      .then((data) => setGeoJson(data as RiverFeatureCollection))
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }, [])

  useEffect(() => {
    fetch(CONFLUENCE_PATH)
      .then((r) => r.json())
      .then((data: Array<Record<string, string>>) => {
        setConfluenceData(
          data.map((row, i) => ({
            id: String(i),
            riverName: row.river_name ?? '',
            into: row.confluences_into ?? '',
            placeName: row.place_name ?? '',
            x: Number(row.x),
            y: Number(row.y),
          })),
        )
      })
      .catch(() => {})
  }, [])

  useEffect(() => {
    fetch(DAM_PATH)
      .then((r) => r.json())
      .then((data: Array<Record<string, unknown>>) => {
        setDamData(
          data.map((d) => ({
            name: String(d.Name ?? ''),
            x: Number(d.x),
            y: Number(d.y),
            state: d.State ? String(d.State) : undefined,
          })),
        )
      })
      .catch(() => {})
  }, [])

  useEffect(() => {
    fetch(STREAMFLOW_PATH)
      .then((r) => r.text())
      .then((text) => {
        const data = parseAllStreamflowByYear(text)
        setStreamflowData(data)
        if (data.years.length > 0) setSelectedYear(data.years[0])
      })
      .catch(() => {})
  }, [])

  // A+B data loading
  useEffect(() => {
    fetch(BASELINE_PATH)
      .then((r) => r.json())
      .then((data) => setBaselineData(data as BaselineData))
      .catch(() => {})
  }, [])

  useEffect(() => {
    fetch(SEGMENT_SSA_PATH)
      .then((r) => r.text())
      .then((text) => setSegmentSSA(parseSegmentSSA(text)))
      .catch(() => {})
  }, [])

  useEffect(() => {
    fetch(BASIN_SSA_PATH)
      .then((r) => r.text())
      .then((text) => setBasinSSA(parseBasinSSA(text)))
      .catch(() => {})
  }, [])

  // --- Animation ---

  useEffect(() => {
    if (!playing || !streamflowData) return
    const id = setInterval(() => {
      setSelectedYear((prev) => {
        const years = streamflowData.years
        const idx = years.indexOf(prev ?? years[0])
        return years[(idx + 1) % years.length]
      })
    }, 600)
    return () => clearInterval(id)
  }, [playing, streamflowData])

  // --- Derived data ---

  const riverFeatures = useMemo(() => {
    if (!geoJson) return []
    return parseRiverFeatures(geoJson)
  }, [geoJson])

  const viewBounds = useMemo(() => {
    const basinData = geoLayerDataMap['basin']
    if (!basinData) return null
    return computeGeoBounds(basinData, 0.15)
  }, [geoLayerDataMap])

  const bounds = useMemo(() => {
    if (!viewBounds) return null
    const cx = (viewBounds.xMin + viewBounds.xMax) / 2
    const cy = (viewBounds.yMin + viewBounds.yMax) / 2
    const halfW = (viewBounds.xMax - viewBounds.xMin) / 2 / zoom
    const halfH = (viewBounds.yMax - viewBounds.yMin) / 2 / zoom
    return { xMin: cx - halfW, xMax: cx + halfW, yMin: cy - halfH, yMax: cy + halfH }
  }, [viewBounds, zoom])

  const dimensions = useMemo(() => {
    if (!bounds) return { width: projectionWidth, height: 240 }
    return getDimensions(bounds, projectionWidth)
  }, [bounds, projectionWidth])

  const baseLines: SchematicLine[] = useMemo(
    () => (bounds ? buildSchematicLines(riverFeatures, bounds, projectionWidth, undefined, straightLines) : []),
    [riverFeatures, bounds, projectionWidth, straightLines],
  )

  const isAB = vizMode === 'ab'

  const schematicLines: SchematicLine[] = useMemo(() => {
    const base = baseLines.map((line) => ({ ...line, width: 0.1 }))
    if (!streamflowData || selectedYear == null) return base
    const flowMap = streamflowData.byYear.get(selectedYear)

    let result: SchematicLine[]
    if (isAB) {
      result = applyFlowWidthsSqrt(base, flowMap, streamflowData.globalMinFlow, streamflowData.globalMaxFlow)
    } else {
      result = applyFlowWidths(base, flowMap, streamflowData.globalMinFlow, streamflowData.globalMaxFlow, logScale)
    }

    // Apply vitality colors in A+B mode
    if (isAB && vitalityEnabled && baselineData && flowMap) {
      result = applyVitalityColors(result, flowMap, baselineData)
    }

    return result
  }, [baseLines, streamflowData, selectedYear, logScale, isAB, vitalityEnabled, baselineData])

  const projectedConfluences = useMemo(() => {
    if (!bounds || confluenceData.length === 0) return []
    return buildConfluences(confluenceData, bounds, projectionWidth)
  }, [confluenceData, bounds, projectionWidth])

  const projectedDams = useMemo(() => {
    if (!bounds || damData.length === 0) return []
    return buildDams(damData, bounds, projectionWidth)
  }, [damData, bounds, projectionWidth])

  const activeGeoLayers: GeoLayerData[] = useMemo(() => {
    return GEO_LAYER_CONFIG
      .filter((cfg) => layerVisibility[cfg.key] && geoLayerDataMap[cfg.key])
      .map((cfg) => ({
        key: cfg.key,
        data: geoLayerDataMap[cfg.key],
        stroke: cfg.stroke,
        fill: cfg.fill,
        strokeWidth: strokeWidths[cfg.key] ?? cfg.strokeWidth,
      }))
  }, [layerVisibility, geoLayerDataMap, strokeWidths])

  // A+B derived data
  const droughtExtent = useMemo(() => {
    if (!segmentSSA) return new Map<number, number>()
    return computeDroughtExtent(segmentSSA)
  }, [segmentSSA])

  const worstYears = useMemo(() => {
    if (!basinSSA) return []
    return getWorstYears(basinSSA)
  }, [basinSSA])

  // --- Handlers ---

  const toggleLayer = (key: string) => {
    setLayerVisibility((prev) => ({ ...prev, [key]: !prev[key] }))
  }

  const changeStrokeWidth = (key: string, value: number) => {
    setStrokeWidths((prev) => ({ ...prev, [key]: value }))
  }

  const showDams = layerVisibility.dams !== false

  const activeMotion: MotionType = isAB ? motionType : 'none'

  const controls = (
    <Controls
      layerVisibility={layerVisibility}
      onToggleLayer={toggleLayer}
      straightLines={straightLines}
      onToggleStraightLines={() => setStraightLines((prev) => !prev)}
      strokeWidths={strokeWidths}
      onChangeStrokeWidth={changeStrokeWidth}
      zoom={zoom}
      onChangeZoom={setZoom}
      vignetteRadius={vignetteRadius}
      onChangeVignetteRadius={setVignetteRadius}
      vignetteFeather={vignetteFeather}
      onChangeVignetteFeather={setVignetteFeather}
      years={streamflowData?.years ?? []}
      selectedYear={selectedYear}
      onChangeYear={setSelectedYear}
      playing={playing}
      onTogglePlay={() => setPlaying((p) => !p)}
      logScale={logScale}
      onToggleLogScale={() => setLogScale((p) => !p)}
      vizMode={vizMode}
      onChangeVizMode={setVizMode}
      motionType={motionType}
      onChangeMotionType={setMotionType}
      worstYears={worstYears}
      droughtExtent={droughtExtent}
      vitalityEnabled={vitalityEnabled}
      onToggleVitality={() => setVitalityEnabled((p) => !p)}
    />
  )

  return (
    <section ref={containerRef} style={{ position: 'relative', width: '100%', height: '100vh' }}>
      {loading && <p style={{ padding: 16 }}>Loading river data...</p>}
      {error && <p style={{ padding: 16, color: 'crimson' }}>{error}</p>}
      {!loading && !error && geoJson && bounds && (
        <SchematicMapCanvas
          lines={schematicLines}
          confluences={layerVisibility.confluences ? projectedConfluences : []}
          dams={showDams ? projectedDams : []}
          geoLayers={activeGeoLayers}
          bounds={bounds}
          terrainBounds={terrainBounds ?? undefined}
          hillshadeUrl={layerVisibility.hillshade ? HILLSHADE_URL : undefined}
          width={dimensions.width + 60}
          height={dimensions.height + 60}
          controls={controls}
          vignetteRadius={vignetteRadius}
          vignetteFeather={vignetteFeather}
          motionType={activeMotion}
        />
      )}
    </section>
  )
}

export default MapPage
