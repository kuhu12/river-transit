// Data file paths (pre-projected LCC files)
export const RIVER_PATH = '/ganga/out/rivers_natural_lcc.geojson'
export const CONFLUENCE_PATH = '/ganga/out/confluences_lcc.json'
export const DAM_PATH = '/ganga/out/dams_lcc.json'
export const TERRAIN_BOUNDS_PATH = '/ganga/out/terrain-bounds.json'
export const HILLSHADE_URL = '/ganga/out/ganga_hillshade_v2.png'
export const STREAMFLOW_PATH = '/ganga/streamflow.csv'
export const BASELINE_PATH = '/ganga/baseline_1951_1980.json'
export const SEGMENT_SSA_PATH = '/ganga/segment_ssa_by_year.csv'
export const BASIN_SSA_PATH = '/ganga/basin_annual_ssa.csv'

// Geo layer configuration
export const GEO_LAYER_CONFIG: { key: string; label: string; path: string; stroke: string; fill: string; strokeWidth: number }[] = [
  { key: 'country', label: 'Country', path: '/ganga/out/country_lcc.geojson', stroke: '#c0c0c0', fill: 'none', strokeWidth: 0.6 },
  { key: 'states', label: 'States', path: '/ganga/out/state_boundaries_lite_lcc.geojson', stroke: '#d0d0d0', fill: 'none', strokeWidth: 0.3 },
  { key: 'basin', label: 'Basin', path: '/ganga/out/basin_lcc.geojson', stroke: '#c48a3f', fill: 'none', strokeWidth: 1.2 },
]

// Default layer visibility
export const DEFAULT_LAYER_VISIBILITY: Record<string, boolean> = {
  country: true,
  states: true,
  basin: true,
  hillshade: true,
  dams: true,
  confluences: true,
}

// Toggle config for the controls panel
export const TOGGLE_CONFIG = [
  ...GEO_LAYER_CONFIG.map((cfg) => ({ key: cfg.key, label: cfg.label, color: cfg.stroke })),
  { key: 'hillshade', label: 'Terrain', color: '#7f8c8d' },
  { key: 'dams', label: 'Dams', color: '#d35400' },
  { key: 'confluences', label: 'Confluences', color: '#e45756' },
]

// Canvas defaults
export const DEFAULT_WIDTH = 860
export const DEFAULT_HEIGHT = 420
export const PADDING = 30

// Flow width scaling
export const FLOW_MIN_PX = 0.5
export const FLOW_MAX_PX = 10

// Geo bounds padding
export const GEO_BOUNDS_PADDING = 0.15

// Default stroke widths for geo layers (matches GEO_LAYER_CONFIG defaults)
export const DEFAULT_STROKE_WIDTHS: Record<string, number> = Object.fromEntries(
  GEO_LAYER_CONFIG.map((cfg) => [cfg.key, cfg.strokeWidth]),
)
