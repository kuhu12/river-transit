// Barrel re-export so existing imports from this path keep working
export type {
  Point,
  RiverFeatureProperties,
  GeoLineString,
  GeoJsonFeature,
  RiverFeatureCollection,
  SchematicTick,
  SchematicLine,
  ParsedRiverFeature,
  Bounds,
  ConfluenceLcc,
  SchematicConfluence,
  DamLcc,
  ProjectedDam,
  AllStreamflowData,
  GeoFeatureCollection,
  GeoLayerData,
} from '../types'

export {
  normalize,
  parseRiverFeatures,
  parseAllStreamflowByYear,
  applyFlowWidths,
  getDimensions,
  computeBounds,
  computeGeoBounds,
  buildSchematicLines,
  buildConfluences,
  buildDams,
} from '../utils'
