import { useEffect, useDeferredValue, useMemo, useState } from 'react'
import {
  applyFlowWidths,
  buildSchematicLines,
  buildSchematicConfluences,
  getSchematicDimensions,
  parseAllStreamflowByYear,
  parseConfluenceCsv,
  parseRiverFeatures,
  type AllStreamflowData,
  type RiverFeatureCollection,
  type SchematicLine,
} from './schematicMapUtils'
import SchematicMapCanvas from './SchematicMapCanvas'
import YearSlider from './YearSlider'

const basinGeoJsonPaths: Record<string, string> = {
  ganga: '/ganga/river_geo.json',
}

const basinConfluencePaths: Record<string, string> = {
  ganga: '/ganga/confluences.csv',
}

const basinStreamflowPaths: Record<string, string> = {
  ganga: '/ganga/streamflow.csv',
}

export type SchematicMapProps = {
  basin: string
}

function SchematicMap({ basin }: SchematicMapProps) {
  const [geoJson, setGeoJson] = useState<RiverFeatureCollection | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState<boolean>(false)

  const geoJsonPath = basinGeoJsonPaths[basin.toLowerCase()]
  const confluenceCsvPath = basinConfluencePaths[basin.toLowerCase()]

  const [confluenceCsv, setConfluenceCsv] = useState<string | null>(null)
  const [confluenceError, setConfluenceError] = useState<string | null>(null)
  const [confluenceLoading, setConfluenceLoading] = useState<boolean>(false)
  const [streamflowCsv, setStreamflowCsv] = useState<string | null>(null)
  const [streamflowError, setStreamflowError] = useState<string | null>(null)
  const [streamflowLoading, setStreamflowLoading] = useState<boolean>(false)
  const [selectedYear, setSelectedYear] = useState<number>(2022)
  const [straightLines, setStraightLines] = useState<boolean>(true)
  const [logScale, setLogScale] = useState<boolean>(true)

  useEffect(() => {
    if (!geoJsonPath) {
      setError(`Unknown basin: ${basin}`)
      setGeoJson(null)
      return
    }

    setLoading(true)
    setError(null)
    setGeoJson(null)

    fetch(geoJsonPath)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Failed to load ${geoJsonPath}`)
        }
        return response.json()
      })
      .then((data) => {
        setGeoJson(data as RiverFeatureCollection)
      })
      .catch((fetchError) => {
        setError(fetchError instanceof Error ? fetchError.message : String(fetchError))
      })
      .finally(() => {
        setLoading(false)
      })
  }, [basin, geoJsonPath])

  useEffect(() => {
    if (!confluenceCsvPath) {
      setConfluenceError(`Unknown basin: ${basin}`)
      setConfluenceCsv(null)
      return
    }

    setConfluenceLoading(true)
    setConfluenceError(null)
    setConfluenceCsv(null)

    fetch(confluenceCsvPath)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Failed to load ${confluenceCsvPath}`)
        }
        return response.text()
      })
      .then((text) => {
        setConfluenceCsv(text)
      })
      .catch((fetchError) => {
        setConfluenceError(fetchError instanceof Error ? fetchError.message : String(fetchError))
      })
      .finally(() => {
        setConfluenceLoading(false)
      })
  }, [basin, confluenceCsvPath])

  useEffect(() => {
    const streamflowPath = basinStreamflowPaths[basin.toLowerCase()]
    if (!streamflowPath) {
      setStreamflowError(`Unknown basin: ${basin}`)
      setStreamflowCsv(null)
      return
    }

    setStreamflowLoading(true)
    setStreamflowError(null)
    setStreamflowCsv(null)

    fetch(streamflowPath)
      .then((response) => {
        if (!response.ok) {
          throw new Error(`Failed to load ${streamflowPath}`)
        }
        return response.text()
      })
      .then((text) => {
        setStreamflowCsv(text)
      })
      .catch((fetchError) => {
        setStreamflowError(fetchError instanceof Error ? fetchError.message : String(fetchError))
      })
      .finally(() => {
        setStreamflowLoading(false)
      })
  }, [basin])

  const riverFeatures = useMemo(() => {
    if (!geoJson) {
      return []
    }

    return parseRiverFeatures(geoJson)
  }, [geoJson])

  const schematicDimensions = useMemo(() => {
    if (riverFeatures.length === 0) {
      return { width: 760, height: 240 }
    }

    return getSchematicDimensions(riverFeatures, 760)
  }, [riverFeatures])

  // Parse all years in one CSV scan — runs once per loaded CSV, not on every year change
  const allFlowData: AllStreamflowData | null = useMemo(() => {
    if (!streamflowCsv) return null
    return parseAllStreamflowByYear(streamflowCsv)
  }, [streamflowCsv])

  const baseLines: SchematicLine[] = useMemo(
    () => buildSchematicLines(riverFeatures, 760, undefined, straightLines),
    [riverFeatures, straightLines],
  )

  // Defer the year fed to canvas so the slider thumb updates immediately
  const deferredYear = useDeferredValue(selectedYear)

  // Only re-run the cheap width-application when deferred year changes
  const schematicLines: SchematicLine[] = useMemo(
    () => applyFlowWidths(baseLines, allFlowData?.byYear.get(deferredYear), allFlowData?.globalMinFlow ?? 0, allFlowData?.globalMaxFlow ?? 0, logScale),
    [baseLines, allFlowData, deferredYear, logScale],
  )

  const schematicConfluences = useMemo(() => {
    if (!confluenceCsv || riverFeatures.length === 0) {
      return []
    }

    const confluenceRecords = parseConfluenceCsv(confluenceCsv)
    return buildSchematicConfluences(confluenceRecords, riverFeatures, 760)
  }, [confluenceCsv, riverFeatures])

  return (
    <section style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      <header>
        <h1 style={{ margin: 0 }}>Schematic River Map</h1>
        <p style={{ margin: '6px 0 0', color: '#444' }}>
          Basin: <strong>{basin}</strong>
        </p>
      </header>
      <div style={{ display: 'flex', gap: 20 }}>
        <label style={{ display: 'flex', alignItems: 'center', gap: 8, userSelect: 'none' }}>
          <input type="checkbox" checked={straightLines} onChange={(e) => setStraightLines(e.target.checked)} />
          Schematic (straight lines)
        </label>
        <label style={{ display: 'flex', alignItems: 'center', gap: 8, userSelect: 'none' }}>
          <input type="checkbox" checked={logScale} onChange={(e) => setLogScale(e.target.checked)} />
          Log scale
        </label>
      </div>
      {loading && <p>Loading schematic river data…</p>}
      {error && <p style={{ color: 'crimson' }}>{error}</p>}
      {confluenceLoading && <p>Loading confluence data…</p>}
      {confluenceError && <p style={{ color: 'crimson' }}>{confluenceError}</p>}
      {streamflowLoading && <p>Loading streamflow data…</p>}
      {streamflowError && <p style={{ color: 'crimson' }}>{streamflowError}</p>}
      {allFlowData && allFlowData.years.length > 1 && (
        <YearSlider years={allFlowData.years} value={selectedYear} onChange={setSelectedYear} />
      )}
      {!loading && !error && geoJson && (
        <SchematicMapCanvas
          lines={schematicLines}
          confluences={schematicConfluences}
          width={schematicDimensions.width + 60}
          height={schematicDimensions.height + 60}
        />
      )}
    </section>
  )
}

export default SchematicMap
