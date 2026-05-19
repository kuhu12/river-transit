# Reads seg_id values from the Ganga and India stream shapefiles, matches them to the monthly Streamflow.nc dataset, and extracts the corresponding streamflow records.
# Saves two CSVs with date, time, seg_id, and streamflow_m3s, while printing any shapefile seg_ids that are missing from the NetCDF.

import shapefile
import xarray as xr
import pandas as pd
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
GANGA_SHP = Path("Shapefiles/Ganga Basin Streams/ganga_streams.shp")
INDIA_SHP = Path("Shapefiles/India Streams/India_stream.shp")
STREAMFLOW_NC = Path("Raw Data/Streamflow/Streamflow.nc")

OUT_DIR = Path("Processed Data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

GANGA_OUT_CSV = OUT_DIR / "ganga_streams_monthly_streamflow.csv"
INDIA_OUT_CSV = OUT_DIR / "india_streams_monthly_streamflow.csv"


def get_seg_ids(shp_path: Path):
    """Read seg_id values from a shapefile."""
    sf = shapefile.Reader(str(shp_path))
    fields = [f[0] for f in sf.fields[1:]]
    print(f"\nReading shapefile: {shp_path}")
    print("Fields:", fields)

    if "seg_id" in fields:
        seg_field = "seg_id"
    elif "SEG_ID" in fields:
        seg_field = "SEG_ID"
    else:
        raise ValueError(f"No seg_id/SEG_ID field found in {shp_path}")

    seg_idx = fields.index(seg_field)

    seg_ids = []
    for rec in sf.records():
        val = rec[seg_idx]
        if val is not None and str(val).strip() != "":
            seg_ids.append(int(val))

    seg_ids = sorted(set(seg_ids))
    print(f"Unique seg_ids in {shp_path.name}: {len(seg_ids)}")
    return seg_ids


def build_monthly_dataset(ds, seg_ids, label, out_csv: Path):
    """Subset Streamflow.nc to matching seg_ids and save monthly CSV."""
    nc_seg_ids = ds["seg_id"].values.astype(int)

    common_seg_ids = sorted(set(seg_ids).intersection(set(nc_seg_ids)))
    missing_seg_ids = sorted(set(seg_ids) - set(nc_seg_ids))

    print(f"\n--- {label} ---")
    print(f"seg_ids in shapefile: {len(seg_ids)}")
    print(f"seg_ids found in Streamflow.nc: {len(common_seg_ids)}")
    print(f"seg_ids missing from Streamflow.nc: {len(missing_seg_ids)}")

    if missing_seg_ids:
        print(f"\nMissing {label} seg_ids:")
        print(missing_seg_ids)
    else:
        print(f"\nNo missing {label} seg_ids.")

    subset = ds.sel(seg_id=common_seg_ids)

    df = subset["Streamflow"].to_dataframe().reset_index()
    df = df.rename(columns={"Streamflow": "streamflow_m3s"})

    # Convert numeric month offsets into dates
    base_date = pd.Timestamp("1951-01-01")
    df["date"] = df["time"].apply(lambda m: base_date + pd.DateOffset(months=int(m)))

    # Reorder columns
    df = df[["date", "time", "seg_id", "streamflow_m3s"]]

    df.to_csv(out_csv, index=False)
    print(f"\nSaved: {out_csv}")
    print(df.head())

    return df, missing_seg_ids


def main():
    # Read seg_ids from shapefiles
    ganga_seg_ids = get_seg_ids(GANGA_SHP)
    india_seg_ids = get_seg_ids(INDIA_SHP)

    # Open NetCDF with time decoding off
    print(f"\nOpening NetCDF: {STREAMFLOW_NC}")
    ds = xr.open_dataset(STREAMFLOW_NC, decode_times=False)

    # Build Ganga dataset
    ganga_df, ganga_missing = build_monthly_dataset(
        ds, ganga_seg_ids, "Ganga", GANGA_OUT_CSV
    )

    # Build India dataset
    india_df, india_missing = build_monthly_dataset(
        ds, india_seg_ids, "India", INDIA_OUT_CSV
    )

    # Summary
    print("\n================ SUMMARY ================")
    print(f"Ganga monthly rows: {len(ganga_df)}")
    print(f"India monthly rows: {len(india_df)}")
    print(f"Ganga missing seg_ids count: {len(ganga_missing)}")
    print(f"India missing seg_ids count: {len(india_missing)}")


if __name__ == "__main__":
    main()