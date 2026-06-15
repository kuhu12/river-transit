#!/usr/bin/env bash
#
# build_terrain.sh — bake a hillshade + vectors into ONE shared LCC grid
# so D3 can render the flat map with geoIdentity (no runtime reprojection).
#
# Run from your VizChitra folder after putting the inputs in place:
#   chmod +x build_terrain.sh
#   ./build_terrain.sh
#
# Requires GDAL (brew install gdal) and python3 (already on macOS).

set -euo pipefail

# ----------------------------------------------------------------------
# CONFIG — edit these paths to match your files
# ----------------------------------------------------------------------
DEM="dem.tif"                 # input DEM covering the basin bbox (from OpenTopography)
VECTORS=( \
  "basin.json" \
  "rivers_natural.json" \
  "country.json" \
  "state_boundaries_lite.json" \
)                             # all the vector layers to reproject (lon/lat input)
OUT="out"                     # output folder

# Lambert Conformal Conic centred on the Ganga basin.
# The two standard parallels (lat_1, lat_2) should sit at roughly 1/6 and 5/6
# of your latitude span for least distortion. For a ~21-31N basin that's ~23 and ~29.
# Tune these once and NEVER change them mid-project — every layer must share this string.
PROJ="+proj=lcc +lat_1=23 +lat_2=29 +lat_0=26 +lon_0=81 +datum=WGS84 +units=m +no_defs"
# ----------------------------------------------------------------------

mkdir -p "$OUT"

echo "1/5  Hillshade from DEM..."
# -z 2 exaggerates relief so the Himalaya read clearly; 315/45 = light from upper-left (cartographic convention)
gdaldem hillshade "$DEM" "$OUT/hillshade.tif" -z 2 -az 315 -alt 45 -compute_edges

echo "2/5  Reproject hillshade into the LCC grid..."
gdalwarp -overwrite -t_srs "$PROJ" -r bilinear \
  "$OUT/hillshade.tif" "$OUT/hillshade_lcc.tif"

# --- OPTIONAL: mask terrain to the watershed instead of filling the frame ---
# Comment out step 2 above and use this instead for the "terrain only inside the
# basin" look (striking for a transit-map piece). Needs basin.geojson.
#
# gdalwarp -overwrite -cutline basin.geojson -crop_to_cutline -dstalpha \
#   -t_srs "$PROJ" -r bilinear "$OUT/hillshade.tif" "$OUT/hillshade_lcc.tif"
# ----------------------------------------------------------------------------

echo "3/5  Export hillshade to PNG..."
gdal_translate -of PNG "$OUT/hillshade_lcc.tif" "$OUT/ganga_hillshade.png"

echo "4/5  Reproject vectors into the SAME grid..."
for v in "${VECTORS[@]}"; do
  if [ -f "$v" ]; then
    name=$(basename "$v" | sed 's/\.[^.]*$//')
    ogr2ogr -t_srs "$PROJ" -f GeoJSON "$OUT/${name}_lcc.geojson" "$v"
    echo "      -> $OUT/${name}_lcc.geojson"
  else
    echo "      (skipped: $v not found)"
  fi
done

echo "5/5  Emit projected bounds for D3..."
# D3 reads this so you never hand-copy coordinates. xmin/ymax = top-left corner, etc.
gdalinfo -json "$OUT/hillshade_lcc.tif" | python3 -c '
import json, sys
d = json.load(sys.stdin)
c = d["cornerCoordinates"]
out = {
  "xmin": c["upperLeft"][0],  "ymax": c["upperLeft"][1],
  "xmax": c["lowerRight"][0], "ymin": c["lowerRight"][1],
}
open(sys.argv[1], "w").write(json.dumps(out, indent=2))
' "$OUT/terrain-bounds.json"

echo ""
echo "Done. Everything is in $OUT/ — point the D3 file at that folder."
