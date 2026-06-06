# Reads seg_id and river_name values from the Ganga rivers shapefile, matches them to the monthly Streamflow.nc dataset, and extracts the corresponding streamflow records.
# Saves CSV with date, time, seg_id, and streamflow_m3s. Missing seg_ids (with river names) are saved to missing_segments.csv.

import shapefile
import xarray as xr
import pandas as pd
from pathlib import Path

# -----------------------------
# Paths
# -----------------------------
GANGA_SHP = Path("Shapefiles/Ganga Basin Streams/NamedStreams/ganga_rivers.shp")
STREAMFLOW_NC = Path("Raw Data/Streamflow/Streamflow.nc")

OUT_DIR = Path("Processed Data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

GANGA_OUT_CSV = OUT_DIR / "ganga_rivers_monthly_streamflow.csv"
MISSING_SEGMENTS_CSV = OUT_DIR / "missing_segments.csv"


def get_seg_ids_with_names(shp_path: Path):
    """Read seg_id and river_name values from a shapefile."""
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

    if "river_name" in fields:
        name_field = "river_name"
    elif "RIVER_NAME" in fields:
        name_field = "RIVER_NAME"
    else:
        raise ValueError(f"No river_name/RIVER_NAME field found in {shp_path}")

    seg_idx = fields.index(seg_field)
    name_idx = fields.index(name_field)

    seg_to_name = {}
    for rec in sf.records():
        seg_val = rec[seg_idx]
        name_val = rec[name_idx]
        if seg_val is not None and str(seg_val).strip() != "":
            seg_id = int(seg_val)
            river_name = str(name_val).strip() if name_val else "Unknown"
            seg_to_name[seg_id] = river_name

    print(f"Unique seg_ids in {shp_path.name}: {len(seg_to_name)}")
    return seg_to_name


def build_monthly_dataset(ds, seg_to_name, label, out_csv: Path, missing_csv: Path):
    """Subset Streamflow.nc to matching seg_ids and save monthly CSV."""
    nc_seg_ids = ds["seg_id"].values.astype(int)
    shp_seg_ids = set(seg_to_name.keys())

    common_seg_ids = sorted(shp_seg_ids.intersection(set(nc_seg_ids)))
    missing_seg_ids = sorted(shp_seg_ids - set(nc_seg_ids))

    print(f"\n--- {label} ---")
    print(f"seg_ids in shapefile: {len(shp_seg_ids)}")
    print(f"seg_ids found in Streamflow.nc: {len(common_seg_ids)}")
    print(f"seg_ids missing from Streamflow.nc: {len(missing_seg_ids)}")

    if missing_seg_ids:
        print(f"\nMissing {label} seg_ids with river names:")
        missing_data = [(seg_id, seg_to_name[seg_id]) for seg_id in missing_seg_ids]
        for seg_id, river_name in missing_data[:10]:
            print(f"  seg_id: {seg_id}, river_name: {river_name}")
        if len(missing_seg_ids) > 10:
            print(f"  ... and {len(missing_seg_ids) - 10} more")

        missing_df = pd.DataFrame(missing_data, columns=["seg_id", "river_name"])
        missing_df.to_csv(missing_csv, index=False)
        print(f"\nSaved missing segments to: {missing_csv}")
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
    # Read seg_ids and river names from shapefile
    ganga_seg_to_name = get_seg_ids_with_names(GANGA_SHP)

    # Open NetCDF with time decoding off
    print(f"\nOpening NetCDF: {STREAMFLOW_NC}")
    ds = xr.open_dataset(STREAMFLOW_NC, decode_times=False)

    # Build Ganga dataset
    ganga_df, ganga_missing = build_monthly_dataset(
        ds, ganga_seg_to_name, "Ganga", GANGA_OUT_CSV, MISSING_SEGMENTS_CSV
    )

    # Summary
    print("\n================ SUMMARY ================")
    print(f"Ganga monthly rows: {len(ganga_df)}")
    print(f"Ganga missing seg_ids count: {len(ganga_missing)}")


if __name__ == "__main__":
    main()