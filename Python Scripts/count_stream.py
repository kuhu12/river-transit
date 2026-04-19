from pathlib import Path
import shapefile

files = {
    "Ganga streams": Path("Shapefiles/Ganga Basin Streams/ganga_streams.shp"),
    "India streams": Path("Shapefiles/India Streams/India_stream.shp"),
}

for name, path in files.items():
    if not path.exists():
        print(f"{name}: file not found -> {path}")
        continue

    sf = shapefile.Reader(str(path))
    print(f"{name}: {len(sf)} stream features")