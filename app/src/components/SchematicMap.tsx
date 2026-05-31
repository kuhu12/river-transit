import { useEffect, useMemo, useState } from 'react'
import {
  buildSchematicLines,
  buildSchematicConfluences,
  getSchematicDimensions,
  parseConfluenceCsv,
  parseRiverFeatures,
  parseStreamflowCsv,
  type RiverFeatureCollection,
  type SchematicLine,
} from './schematicMapUtils'
import SchematicMapCanvas from './SchematicMapCanvas'

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

  const schematicLines: SchematicLine[] = useMemo(() => {
    let flowMap: Map<string, number> | undefined = undefined
    if (streamflowCsv) {
      flowMap = parseStreamflowCsv(streamflowCsv, 2022)
    }

    return buildSchematicLines(riverFeatures, 760, flowMap)
  }, [riverFeatures])

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

      {loading && <p>Loading schematic river data…</p>}
      {error && <p style={{ color: 'crimson' }}>{error}</p>}
      {confluenceLoading && <p>Loading confluence data…</p>}
      {confluenceError && <p style={{ color: 'crimson' }}>{confluenceError}</p>}
      {streamflowLoading && <p>Loading streamflow data…</p>}
      {streamflowError && <p style={{ color: 'crimson' }}>{streamflowError}</p>}
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
