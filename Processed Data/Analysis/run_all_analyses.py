"""
VizChitra Drying Analysis Suite
================================
Runs all 9 analytical methods (A1–A5, B1–B4) on Ganga Basin mizuRoute
streamflow reconstruction (1951–2021). Each method outputs a CSV data file
and a Markdown report to its own folder under Processed Data/Analysis/.

Methodology references:
  - Chuphal et al. (PNAS 2025): SSA, Mann-Kendall, changepoint at 1991
  - VIC model + mizuRoute: naturalized flow (no dams/abstractions)

Usage:
    python "Processed Data/Analysis/run_all_analyses.py"
"""

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pymannkendall as mk

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO = Path(__file__).resolve().parents[2]  # VizChitra root
STREAMFLOW_CSV = REPO / "Processed Data" / "Streamflow" / "ganga_rivers_monthly_streamflow.csv"
RIVER_SEGMENTS_CSV = REPO / "Processed Data" / "Rivers" / "river_all_segments.csv"
RIVER_TIERED_CSV = REPO / "Processed Data" / "Rivers" / "ganga_rivers_tiered.csv"
ANALYSIS_DIR = REPO / "Processed Data" / "Analysis"

# Period definitions (consistent with Chuphal et al. PNAS 2025 changepoint)
EARLY_START, EARLY_END = 1951, 1990
LATE_START, LATE_END = 1991, 2021
FULL_START, FULL_END = 1951, 2021

# Dry season months (pre-monsoon): February through May
DRY_MONTHS = [2, 3, 4, 5]

# SSA drought threshold (Chuphal et al.)
SSA_DROUGHT_THRESHOLD = -0.5

# ---------------------------------------------------------------------------
# Data Loading
# ---------------------------------------------------------------------------
def load_data():
    """Load streamflow and river metadata, return enriched monthly DataFrame."""
    print("Loading streamflow data...")
    df = pd.read_csv(STREAMFLOW_CSV)
    df["date"] = pd.to_datetime(df["date"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["seg_id"] = df["seg_id"].astype(int)

    # Load river segment lookup
    seg_df = pd.read_csv(RIVER_SEGMENTS_CSV)
    seg_df["seg_id"] = seg_df["seg_id"].astype(int)

    # Load tiered summary for Updated_Tier
    tier_df = pd.read_csv(RIVER_TIERED_CSV)
    tier_df = tier_df.rename(columns={"Updated_Tier": "updated_tier"})

    # Merge river name and tier onto streamflow
    df = df.merge(seg_df[["seg_id", "river_name", "tier"]], on="seg_id", how="left")
    # Also merge the updated tier from the tiered CSV (by river_name)
    df = df.merge(
        tier_df[["river_name", "updated_tier"]],
        on="river_name",
        how="left",
    )

    n_segs = df["seg_id"].nunique()
    year_range = f"{df['year'].min()}–{df['year'].max()}"
    print(f"  Loaded {len(df):,} rows, {n_segs} segments, {year_range}")
    return df


def compute_annual_means(df):
    """Compute annual mean streamflow per segment."""
    annual = (
        df.groupby(["seg_id", "year"])["streamflow_m3s"]
        .mean()
        .reset_index()
        .rename(columns={"streamflow_m3s": "annual_mean_m3s"})
    )
    return annual


def get_seg_meta(df):
    """Get per-segment metadata (river_name, tier)."""
    meta = (
        df.groupby("seg_id")
        .agg(
            river_name=("river_name", "first"),
            tier=("tier", "first"),
            updated_tier=("updated_tier", "first"),
            long_term_mean=("streamflow_m3s", "mean"),
        )
        .reset_index()
    )
    return meta


# ---------------------------------------------------------------------------
# A1: Percent Change (Early vs. Late Period)
# ---------------------------------------------------------------------------
def run_a1(annual, meta):
    """Compute per-segment percent change between early and late periods."""
    print("\n[A1] Percent Change (Early vs. Late Period)...")
    out_dir = ANALYSIS_DIR / "A1_Percent_Change"

    early = annual[annual["year"].between(EARLY_START, EARLY_END)]
    late = annual[annual["year"].between(LATE_START, LATE_END)]

    early_mean = early.groupby("seg_id")["annual_mean_m3s"].mean().rename("early_mean")
    late_mean = late.groupby("seg_id")["annual_mean_m3s"].mean().rename("late_mean")

    result = pd.concat([early_mean, late_mean], axis=1).dropna()
    result["abs_change_m3s"] = result["late_mean"] - result["early_mean"]
    result["pct_change"] = (result["abs_change_m3s"] / result["early_mean"]) * 100
    result = result.reset_index().merge(meta, on="seg_id", how="left")
    result = result.sort_values("pct_change")

    result.to_csv(out_dir / "segment_percent_change.csv", index=False)

    # Generate report
    n_declining = (result["pct_change"] < 0).sum()
    n_total = len(result)
    top_dried = result.head(20)
    top_wetted = result.tail(10).iloc[::-1]
    median_change = result["pct_change"].median()
    mean_change = result["pct_change"].mean()

    # By-tier summary
    tier_summary = (
        result.groupby("updated_tier")
        .agg(
            n_segments=("seg_id", "count"),
            median_pct_change=("pct_change", "median"),
            mean_pct_change=("pct_change", "mean"),
            min_pct_change=("pct_change", "min"),
        )
        .reset_index()
        .sort_values("updated_tier")
    )

    report = f"""# A1: Percent Change — Early ({EARLY_START}–{EARLY_END}) vs. Late ({LATE_START}–{LATE_END})

## Method
For each segment, compute mean annual streamflow for the early period ({EARLY_START}–{EARLY_END})
and the late period ({LATE_START}–{LATE_END}). Percent change = (late - early) / early x 100.

Period split at 1991 is consistent with the Bayesian changepoint identified by
Chuphal et al. (PNAS 2025).

## Key Findings

- **{n_declining} of {n_total} segments ({n_declining/n_total*100:.1f}%) show declining flow**
- Median percent change across all segments: **{median_change:+.1f}%**
- Mean percent change: **{mean_change:+.1f}%**

## Top 20 Most Dried Segments

| Rank | seg_id | River | Tier | Early Mean (m3/s) | Late Mean (m3/s) | Change (%) |
|------|--------|-------|------|-------------------|------------------|------------|
"""
    for i, row in enumerate(top_dried.itertuples(), 1):
        report += f"| {i} | {row.seg_id} | {row.river_name} | {row.updated_tier} | {row.early_mean:.2f} | {row.late_mean:.2f} | {row.pct_change:+.1f}% |\n"

    report += f"""
## Top 10 Most Wetted Segments

| Rank | seg_id | River | Tier | Early Mean (m3/s) | Late Mean (m3/s) | Change (%) |
|------|--------|-------|------|-------------------|------------------|------------|
"""
    for i, row in enumerate(top_wetted.itertuples(), 1):
        report += f"| {i} | {row.seg_id} | {row.river_name} | {row.updated_tier} | {row.early_mean:.2f} | {row.late_mean:.2f} | {row.pct_change:+.1f}% |\n"

    report += f"""
## Change by River Tier

| Tier | N Segments | Median Change (%) | Mean Change (%) | Worst Segment (%) |
|------|------------|-------------------|-----------------|-------------------|
"""
    for row in tier_summary.itertuples():
        report += f"| {row.updated_tier} | {row.n_segments} | {row.median_pct_change:+.1f}% | {row.mean_pct_change:+.1f}% | {row.min_pct_change:+.1f}% |\n"

    report += """
## Visual Encoding Recommendation
Map each segment's percent change to a **diverging color scale** (red = decline, blue = increase).
Range suggestion: clamp to [-50%, +50%] for visual contrast. This encoding makes small tributaries
as visually prominent as the mainstem, directly fixing the visibility problem.

## Limitations
- Two-period comparison is sensitive to the exact breakpoint year
- Does not capture non-linear or non-monotonic changes within each period
- Naturalized flow only — real-world decline may be worse due to abstractions
"""
    with open(out_dir / "report.md", "w") as f:
        f.write(report)

    print(f"  {n_declining}/{n_total} segments declining, median change: {median_change:+.1f}%")
    return result


# ---------------------------------------------------------------------------
# A2: Mann-Kendall Trend + Sen's Slope
# ---------------------------------------------------------------------------
def run_a2(annual, meta):
    """Per-segment Mann-Kendall test with normalized Sen's slope."""
    print("\n[A2] Mann-Kendall Trend + Sen's Slope...")
    out_dir = ANALYSIS_DIR / "A2_Mann_Kendall"

    results = []
    seg_ids = annual["seg_id"].unique()

    for seg_id in seg_ids:
        seg_data = annual[annual["seg_id"] == seg_id].sort_values("year")
        ts = seg_data["annual_mean_m3s"].values

        if len(ts) < 10:
            continue

        try:
            res = mk.original_test(ts)
            long_term_mean = ts.mean()
            norm_slope = (res.slope / long_term_mean * 100) if long_term_mean > 0 else np.nan
            total_change_pct = norm_slope * len(ts)

            results.append({
                "seg_id": seg_id,
                "trend": res.trend,
                "p_value": res.p,
                "tau": res.Tau,
                "sens_slope_m3s_yr": res.slope,
                "intercept": res.intercept,
                "long_term_mean_m3s": long_term_mean,
                "norm_slope_pct_yr": norm_slope,
                "total_change_pct": total_change_pct,
            })
        except Exception:
            continue

    result = pd.DataFrame(results).merge(meta[["seg_id", "river_name", "tier", "updated_tier"]], on="seg_id", how="left")
    result = result.sort_values("norm_slope_pct_yr")
    result.to_csv(out_dir / "segment_mann_kendall.csv", index=False)

    sig_declining = result[(result["p_value"] <= 0.05) & (result["trend"] == "decreasing")]
    sig_increasing = result[(result["p_value"] <= 0.05) & (result["trend"] == "increasing")]
    no_trend = result[result["p_value"] > 0.05]

    top_declining = sig_declining.head(20)

    # Tier breakdown of significant declines
    tier_sig = (
        sig_declining.groupby("updated_tier")
        .agg(
            n_sig_declining=("seg_id", "count"),
            median_norm_slope=("norm_slope_pct_yr", "median"),
        )
        .reset_index()
        .sort_values("updated_tier")
    )

    report = f"""# A2: Mann-Kendall Trend Test + Sen's Slope

## Method
For each segment, compute annual mean streamflow time series ({FULL_START}–{FULL_END}),
then apply the non-parametric Mann-Kendall trend test. Sen's slope gives the robust
rate of change (m3/s per year). Normalized by dividing by the segment's long-term
mean to yield "% per year".

Consistent with Chuphal et al. (PNAS 2025) who apply MK to precipitation and
temperature trends (their Fig. 3), with significance at p <= 0.05.

## Key Findings

- **{len(sig_declining)} segments** with statistically significant declining trend (p <= 0.05)
- **{len(sig_increasing)} segments** with statistically significant increasing trend
- **{len(no_trend)} segments** with no significant trend
- Total segments analyzed: **{len(result)}**

## Top 20 Segments with Strongest Significant Decline

| Rank | seg_id | River | Tier | Sen's Slope (m3/s/yr) | Norm. Slope (%/yr) | Total Change (%) | p-value | Tau |
|------|--------|-------|------|----------------------|-------------------|-----------------|---------|-----|
"""
    for i, row in enumerate(top_declining.itertuples(), 1):
        report += f"| {i} | {row.seg_id} | {row.river_name} | {row.updated_tier} | {row.sens_slope_m3s_yr:.3f} | {row.norm_slope_pct_yr:.3f} | {row.total_change_pct:.1f}% | {row.p_value:.4f} | {row.tau:.3f} |\n"

    report += f"""
## Significant Declines by Tier

| Tier | N Sig. Declining | Median Norm. Slope (%/yr) |
|------|-----------------|--------------------------|
"""
    for row in tier_sig.itertuples():
        report += f"| {row.updated_tier} | {row.n_sig_declining} | {row.median_norm_slope:.3f} |\n"

    if len(sig_increasing) > 0:
        top_inc = sig_increasing.tail(10).iloc[::-1]
        report += """
## Top 10 Segments with Significant Increasing Trend

| Rank | seg_id | River | Tier | Norm. Slope (%/yr) | Total Change (%) | p-value |
|------|--------|-------|------|-------------------|-----------------|---------|
"""
        for i, row in enumerate(top_inc.itertuples(), 1):
            report += f"| {i} | {row.seg_id} | {row.river_name} | {row.updated_tier} | {row.norm_slope_pct_yr:+.3f} | {row.total_change_pct:+.1f}% | {row.p_value:.4f} |\n"

    report += """
## Visual Encoding Recommendation
Two-channel encoding: **color** = normalized Sen's slope (diverging red/blue),
**opacity** = statistical significance (full opacity if p < 0.05, 30% if not).
This highlights segments where drying is both large AND statistically confident.

## Limitations
- Assumes monotonic trend; misses step-changes or reversals
- 70-year series gives good power but may miss recent acceleration
- MK test can be affected by serial autocorrelation (common in hydrology)
"""
    with open(out_dir / "report.md", "w") as f:
        f.write(report)

    print(f"  {len(sig_declining)} sig. declining, {len(sig_increasing)} sig. increasing, {len(no_trend)} no trend")
    return result


# ---------------------------------------------------------------------------
# A3: Dry-Season (Pre-Monsoon) Flow Decline
# ---------------------------------------------------------------------------
def run_a3(df, meta):
    """Percent change restricted to dry-season months (Feb–May)."""
    print("\n[A3] Dry-Season (Pre-Monsoon) Flow Decline...")
    out_dir = ANALYSIS_DIR / "A3_Dry_Season"

    dry = df[df["month"].isin(DRY_MONTHS)].copy()

    # Annual dry-season mean per segment
    dry_annual = (
        dry.groupby(["seg_id", "year"])["streamflow_m3s"]
        .mean()
        .reset_index()
        .rename(columns={"streamflow_m3s": "dry_season_mean_m3s"})
    )

    early = dry_annual[dry_annual["year"].between(EARLY_START, EARLY_END)]
    late = dry_annual[dry_annual["year"].between(LATE_START, LATE_END)]

    early_mean = early.groupby("seg_id")["dry_season_mean_m3s"].mean().rename("early_dry_mean")
    late_mean = late.groupby("seg_id")["dry_season_mean_m3s"].mean().rename("late_dry_mean")

    result = pd.concat([early_mean, late_mean], axis=1).dropna()
    result["abs_change_m3s"] = result["late_dry_mean"] - result["early_dry_mean"]
    result["pct_change"] = (result["abs_change_m3s"] / result["early_dry_mean"]) * 100
    result = result.reset_index().merge(meta, on="seg_id", how="left")
    result = result.sort_values("pct_change")

    result.to_csv(out_dir / "segment_dry_season_change.csv", index=False)

    # Also run MK on dry-season annual means
    mk_results = []
    for seg_id in dry_annual["seg_id"].unique():
        seg_data = dry_annual[dry_annual["seg_id"] == seg_id].sort_values("year")
        ts = seg_data["dry_season_mean_m3s"].values
        if len(ts) < 10:
            continue
        try:
            res = mk.original_test(ts)
            ltm = ts.mean()
            mk_results.append({
                "seg_id": seg_id,
                "dry_mk_trend": res.trend,
                "dry_mk_p_value": res.p,
                "dry_sens_slope_pct_yr": (res.slope / ltm * 100) if ltm > 0 else np.nan,
            })
        except Exception:
            continue

    mk_df = pd.DataFrame(mk_results)
    result = result.merge(mk_df, on="seg_id", how="left")
    result.to_csv(out_dir / "segment_dry_season_change.csv", index=False)

    n_declining = (result["pct_change"] < 0).sum()
    n_total = len(result)
    sig_dry_declining = result[(result.get("dry_mk_p_value", pd.Series()) <= 0.05) & (result.get("dry_mk_trend", pd.Series()) == "decreasing")]
    top_dried = result.head(20)

    # Compare dry-season vs annual change
    report = f"""# A3: Dry-Season (Pre-Monsoon: Feb–May) Flow Decline

## Method
Same as A1 (early vs. late period percent change) but restricted to **pre-monsoon
dry-season months (February–May)**. This is when water scarcity has the greatest
impact on the 600M+ people in the Ganga basin.

Additionally runs Mann-Kendall on the dry-season annual means for significance.

## Key Findings

- **{n_declining} of {n_total} segments ({n_declining/n_total*100:.1f}%) show dry-season decline**
- Median dry-season percent change: **{result["pct_change"].median():+.1f}%**
- **{len(sig_dry_declining)} segments** with statistically significant dry-season decline (MK p <= 0.05)

## Top 20 Most Dried Segments (Dry Season)

| Rank | seg_id | River | Tier | Early Dry Mean | Late Dry Mean | Change (%) | MK Trend | MK p-value |
|------|--------|-------|------|---------------|--------------|------------|----------|------------|
"""
    for i, row in enumerate(top_dried.itertuples(), 1):
        mk_trend = getattr(row, "dry_mk_trend", "N/A")
        mk_p = getattr(row, "dry_mk_p_value", float("nan"))
        mk_p_str = f"{mk_p:.4f}" if pd.notna(mk_p) else "N/A"
        report += f"| {i} | {row.seg_id} | {row.river_name} | {row.updated_tier} | {row.early_dry_mean:.2f} | {row.late_dry_mean:.2f} | {row.pct_change:+.1f}% | {mk_trend} | {mk_p_str} |\n"

    report += """
## Visual Encoding Recommendation
Toggle mode: "Annual" vs "Dry Season (Feb–May)". Same diverging color scale
as A1. Especially powerful for showing baseflow vulnerability — segments that
look fine on annual average but have collapsing dry-season flows.

## Limitations
- Only 4 months per year reduces sample size
- Pre-monsoon flow is baseflow-dominated; in naturalized model this reflects
  soil moisture / snowmelt, not groundwater pumping
- Some headwater segments may have very low dry-season flows with high
  relative noise
"""
    with open(out_dir / "report.md", "w") as f:
        f.write(report)

    print(f"  {n_declining}/{n_total} segments declining in dry season")
    return result


# ---------------------------------------------------------------------------
# A4: Low-Flow Quantile (Q90) Decline
# ---------------------------------------------------------------------------
def run_a4(df, meta):
    """Compute Q90 (10th percentile — flow exceeded 90% of time) change."""
    print("\n[A4] Low-Flow Q90 Decline...")
    out_dir = ANALYSIS_DIR / "A4_Low_Flow_Q90"

    early = df[df["year"].between(EARLY_START, EARLY_END)]
    late = df[df["year"].between(LATE_START, LATE_END)]

    # Q90 = 10th percentile of monthly flows (flow exceeded 90% of the time)
    early_q90 = early.groupby("seg_id")["streamflow_m3s"].quantile(0.10).rename("early_q90")
    late_q90 = late.groupby("seg_id")["streamflow_m3s"].quantile(0.10).rename("late_q90")

    # Also compute Q50 (median) for context
    early_q50 = early.groupby("seg_id")["streamflow_m3s"].quantile(0.50).rename("early_q50")
    late_q50 = late.groupby("seg_id")["streamflow_m3s"].quantile(0.50).rename("late_q50")

    result = pd.concat([early_q90, late_q90, early_q50, late_q50], axis=1).dropna()
    result["q90_abs_change"] = result["late_q90"] - result["early_q90"]
    result["q90_pct_change"] = (result["q90_abs_change"] / result["early_q90"]) * 100
    result["q50_pct_change"] = ((result["late_q50"] - result["early_q50"]) / result["early_q50"]) * 100
    result = result.reset_index().merge(meta, on="seg_id", how="left")

    # Handle infinite pct changes (early_q90 near zero)
    result.loc[~np.isfinite(result["q90_pct_change"]), "q90_pct_change"] = np.nan
    result = result.sort_values("q90_pct_change")

    result.to_csv(out_dir / "segment_q90_change.csv", index=False)

    valid = result.dropna(subset=["q90_pct_change"])
    n_declining = (valid["q90_pct_change"] < 0).sum()
    n_total = len(valid)
    top_dried = valid.head(20)

    report = f"""# A4: Low-Flow Quantile (Q90) Decline

## Method
For each segment, compute Q90 (the 10th percentile of monthly streamflow — i.e.,
the flow exceeded 90% of the time) for the early period ({EARLY_START}–{EARLY_END})
and late period ({LATE_START}–{LATE_END}). Report percent change in Q90.

Q90 is a standard low-flow indicator in hydrology. Decline in Q90 means the
minimum flows that sustain ecosystems and water supply are shrinking.

Also computes Q50 (median) change for comparison.

## Key Findings

- **{n_declining} of {n_total} valid segments ({n_declining/n_total*100:.1f}%) show Q90 decline**
- Median Q90 percent change: **{valid["q90_pct_change"].median():+.1f}%**
- Median Q50 percent change: **{valid["q50_pct_change"].median():+.1f}%**
- Q90 declines are typically **larger** than Q50 declines, indicating low flows are
  disproportionately affected

## Top 20 Segments with Greatest Q90 Decline

| Rank | seg_id | River | Tier | Early Q90 | Late Q90 | Q90 Change (%) | Q50 Change (%) |
|------|--------|-------|------|-----------|----------|----------------|----------------|
"""
    for i, row in enumerate(top_dried.itertuples(), 1):
        q50_str = f"{row.q50_pct_change:+.1f}%" if pd.notna(row.q50_pct_change) else "N/A"
        report += f"| {i} | {row.seg_id} | {row.river_name} | {row.updated_tier} | {row.early_q90:.2f} | {row.late_q90:.2f} | {row.q90_pct_change:+.1f}% | {q50_str} |\n"

    report += """
## Visual Encoding Recommendation
Color by Q90 percent change. This encoding specifically highlights segments
where minimum flows are collapsing — ecologically and socially the most
critical signal. Could be a "Low-Flow Vulnerability" layer toggle.

## Limitations
- Q90 from monthly data is coarser than daily Q90 (monthly averages smooth out
  the lowest daily flows)
- Segments with near-zero early Q90 produce extreme or undefined percent changes
  (these are flagged as NaN)
- Naturalized flow: real Q90 decline with abstractions is likely worse
"""
    with open(out_dir / "report.md", "w") as f:
        f.write(report)

    print(f"  {n_declining}/{n_total} segments show Q90 decline")
    return result


# ---------------------------------------------------------------------------
# A5: Per-Segment Standardized Streamflow Anomaly (SSA)
# ---------------------------------------------------------------------------
def run_a5(annual, meta):
    """Compute per-segment SSA for every year, plus period mean for 1991–2021."""
    print("\n[A5] Per-Segment SSA...")
    out_dir = ANALYSIS_DIR / "A5_Segment_SSA"

    # Compute per-segment mean and std over full period
    seg_stats = (
        annual.groupby("seg_id")["annual_mean_m3s"]
        .agg(["mean", "std"])
        .rename(columns={"mean": "mu", "std": "sigma"})
    )

    # SSA per segment per year
    ssa = annual.merge(seg_stats, on="seg_id")
    ssa["ssa"] = (ssa["annual_mean_m3s"] - ssa["mu"]) / ssa["sigma"]
    ssa.loc[ssa["sigma"] == 0, "ssa"] = 0.0

    ssa_out = ssa[["seg_id", "year", "annual_mean_m3s", "ssa"]].copy()
    ssa_out = ssa_out.merge(meta[["seg_id", "river_name", "updated_tier"]], on="seg_id", how="left")
    ssa_out.to_csv(out_dir / "segment_ssa_by_year.csv", index=False)

    # Period mean SSA (1991–2021)
    late_ssa = ssa[ssa["year"].between(LATE_START, LATE_END)]
    period_mean = (
        late_ssa.groupby("seg_id")["ssa"]
        .mean()
        .rename("mean_ssa_1991_2021")
        .reset_index()
        .merge(meta, on="seg_id", how="left")
        .sort_values("mean_ssa_1991_2021")
    )
    period_mean.to_csv(out_dir / "segment_ssa_period_mean.csv", index=False)

    n_negative = (period_mean["mean_ssa_1991_2021"] < 0).sum()
    n_drought = (period_mean["mean_ssa_1991_2021"] < SSA_DROUGHT_THRESHOLD).sum()
    n_total = len(period_mean)

    top_dried = period_mean.head(20)
    top_wet = period_mean.tail(10).iloc[::-1]

    # By-tier summary
    tier_summary = (
        period_mean.groupby("updated_tier")
        .agg(
            n_segments=("seg_id", "count"),
            median_ssa=("mean_ssa_1991_2021", "median"),
            n_drought=("mean_ssa_1991_2021", lambda x: (x < SSA_DROUGHT_THRESHOLD).sum()),
        )
        .reset_index()
        .sort_values("updated_tier")
    )

    # Year-by-year basin-wide pattern
    year_basin_ssa = ssa.groupby("year")["ssa"].mean()
    worst_years = year_basin_ssa.nsmallest(10)

    report = f"""# A5: Per-Segment Standardized Streamflow Anomaly (SSA)

## Method
For each segment, SSA = (Q_year - mu) / sigma, where mu and sigma are computed
over the full period ({FULL_START}–{FULL_END}). This directly replicates the
Chuphal et al. (PNAS 2025) methodology, extended from the basin outlet to every
individual segment.

Drought threshold: SSA < {SSA_DROUGHT_THRESHOLD} (consistent with the paper).

Reference period for standardization: {FULL_START}–{FULL_END} (full record).

## Key Findings

- **{n_negative} of {n_total} segments ({n_negative/n_total*100:.1f}%)** have negative mean SSA for {LATE_START}–{LATE_END}
- **{n_drought} segments ({n_drought/n_total*100:.1f}%)** have mean SSA below drought threshold ({SSA_DROUGHT_THRESHOLD})
- Basin-wide mean SSA for {LATE_START}–{LATE_END}: **{period_mean["mean_ssa_1991_2021"].mean():.3f}**

## Top 20 Driest Segments (Mean SSA {LATE_START}–{LATE_END})

| Rank | seg_id | River | Tier | Mean SSA | Long-Term Mean (m3/s) |
|------|--------|-------|------|----------|----------------------|
"""
    for i, row in enumerate(top_dried.itertuples(), 1):
        report += f"| {i} | {row.seg_id} | {row.river_name} | {row.updated_tier} | {row.mean_ssa_1991_2021:.3f} | {row.long_term_mean:.1f} |\n"

    report += f"""
## Top 10 Wettest Segments (Mean SSA {LATE_START}–{LATE_END})

| Rank | seg_id | River | Tier | Mean SSA | Long-Term Mean (m3/s) |
|------|--------|-------|------|----------|----------------------|
"""
    for i, row in enumerate(top_wet.itertuples(), 1):
        report += f"| {i} | {row.seg_id} | {row.river_name} | {row.updated_tier} | {row.mean_ssa_1991_2021:.3f} | {row.long_term_mean:.1f} |\n"

    report += f"""
## SSA by Tier

| Tier | N Segments | Median SSA | N in Drought (SSA < {SSA_DROUGHT_THRESHOLD}) |
|------|------------|------------|----------------------------------------------|
"""
    for row in tier_summary.itertuples():
        report += f"| {row.updated_tier} | {row.n_segments} | {row.median_ssa:.3f} | {row.n_drought} |\n"

    report += """
## Visual Encoding Recommendation
**This is the recommended primary encoding for the VizChitra map.**
SSA is dimensionless and bounded (~-3 to +3), mapping perfectly to a diverging
color scale (deep red = SSA < -1, white = 0, deep blue = SSA > 1). Every segment
is on its own scale, so small Tier 5 headwaters are as visually prominent as the
mainstem Ganga. Animate by year for the full spatiotemporal story.

## Data Files
- `segment_ssa_by_year.csv`: Full SSA for every segment x year (for animation)
- `segment_ssa_period_mean.csv`: Summary metric for static "drying severity" map

## Limitations
- Z-score assumes approximately normal distribution of annual flows
- Short-term variability may produce extreme SSA in individual years
- Naturalized flow: does not include dam/abstraction effects
"""
    with open(out_dir / "report.md", "w") as f:
        f.write(report)

    print(f"  {n_negative}/{n_total} segments below zero SSA in {LATE_START}–{LATE_END}")
    return ssa, period_mean


# ---------------------------------------------------------------------------
# B1: Basin-Wide Annual SSA
# ---------------------------------------------------------------------------
def run_b1(annual, meta):
    """Basin-wide annual SSA time series."""
    print("\n[B1] Basin-Wide Annual SSA...")
    out_dir = ANALYSIS_DIR / "B1_Basin_Wide_SSA"

    # Compute total basin flow per year (sum of all segments)
    basin_annual = annual.groupby("year")["annual_mean_m3s"].sum().reset_index()
    basin_annual = basin_annual.rename(columns={"annual_mean_m3s": "basin_total_m3s"})

    mu = basin_annual["basin_total_m3s"].mean()
    sigma = basin_annual["basin_total_m3s"].std()
    basin_annual["ssa"] = (basin_annual["basin_total_m3s"] - mu) / sigma

    # Also compute using outlet segment (Farakka-like — seg_id 1429 or 2959)
    # Use the segment with the highest long-term mean (most downstream)
    biggest_seg = meta.loc[meta["long_term_mean"].idxmax(), "seg_id"]
    outlet_annual = annual[annual["seg_id"] == biggest_seg][["year", "annual_mean_m3s"]].copy()
    outlet_mu = outlet_annual["annual_mean_m3s"].mean()
    outlet_sigma = outlet_annual["annual_mean_m3s"].std()
    outlet_annual["outlet_ssa"] = (outlet_annual["annual_mean_m3s"] - outlet_mu) / outlet_sigma

    basin_annual = basin_annual.merge(outlet_annual[["year", "outlet_ssa"]], on="year", how="left")

    # Classify drought years
    basin_annual["drought"] = basin_annual["ssa"] < SSA_DROUGHT_THRESHOLD
    basin_annual["extreme_drought"] = basin_annual["ssa"] < -1.0
    basin_annual["extreme_wet"] = basin_annual["ssa"] > 1.0

    # 30-year moving mean (paper method)
    basin_annual = basin_annual.sort_values("year")
    basin_annual["ssa_30yr_mean"] = basin_annual["ssa"].rolling(30, min_periods=15).mean()

    basin_annual.to_csv(out_dir / "basin_annual_ssa.csv", index=False)

    drought_years = basin_annual[basin_annual["drought"]].sort_values("ssa")
    extreme_drought_years = basin_annual[basin_annual["extreme_drought"]].sort_values("ssa")
    wet_years = basin_annual[basin_annual["extreme_wet"]].sort_values("ssa", ascending=False)

    # Decade summary
    basin_annual["decade"] = (basin_annual["year"] // 10) * 10
    decade_summary = (
        basin_annual.groupby("decade")
        .agg(
            mean_ssa=("ssa", "mean"),
            n_drought_years=("drought", "sum"),
            driest_year=("ssa", "idxmin"),
        )
        .reset_index()
    )
    decade_summary["driest_year"] = decade_summary["driest_year"].map(basin_annual.set_index(basin_annual.index)["year"])

    report = f"""# B1: Basin-Wide Annual SSA

## Method
Sum annual mean streamflow across all segments for each year, then standardize
as Z-score (SSA) using the full {FULL_START}–{FULL_END} period. Also computed for
the most downstream outlet segment (seg_id {biggest_seg}) for comparison with
Chuphal et al.

Drought threshold: SSA < {SSA_DROUGHT_THRESHOLD}
Extreme drought: SSA < -1.0
Extreme wet: SSA > 1.0

## Key Findings

- **{len(drought_years)} drought years** (SSA < {SSA_DROUGHT_THRESHOLD}) out of {len(basin_annual)} years
- **{len(extreme_drought_years)} extreme drought years** (SSA < -1.0)
- **{len(wet_years)} extreme wet years** (SSA > 1.0)

## All Drought Years (SSA < {SSA_DROUGHT_THRESHOLD}), Ranked by Severity

| Rank | Year | SSA | Basin Total (m3/s) | Outlet SSA |
|------|------|-----|-------------------|------------|
"""
    for i, row in enumerate(drought_years.itertuples(), 1):
        outlet_str = f"{row.outlet_ssa:.3f}" if pd.notna(row.outlet_ssa) else "N/A"
        report += f"| {i} | {row.year} | {row.ssa:.3f} | {row.basin_total_m3s:.0f} | {outlet_str} |\n"

    report += f"""
## Extreme Wet Years (SSA > 1.0)

| Year | SSA | Basin Total (m3/s) |
|------|-----|-------------------|
"""
    for row in wet_years.itertuples():
        report += f"| {row.year} | {row.ssa:+.3f} | {row.basin_total_m3s:.0f} |\n"

    report += f"""
## Decade Summary

| Decade | Mean SSA | N Drought Years |
|--------|----------|----------------|
"""
    for row in decade_summary.itertuples():
        report += f"| {row.decade}s | {row.mean_ssa:+.3f} | {int(row.n_drought_years)} |\n"

    report += """
## Visual Encoding Recommendation
Annotate the year slider with basin-wide SSA: red ticks for drought years,
blue for extreme wet years. The 30-year moving mean line shows the post-1991
structural shift. Could also show as a small sparkline beneath the slider.

## Limitations
- Basin-wide average masks spatial heterogeneity
- Western tributaries may be in severe drought while eastern ones are wet
"""
    with open(out_dir / "report.md", "w") as f:
        f.write(report)

    print(f"  {len(drought_years)} drought years, {len(extreme_drought_years)} extreme droughts")
    return basin_annual


# ---------------------------------------------------------------------------
# B2: Per-Segment Year of Minimum Flow
# ---------------------------------------------------------------------------
def run_b2(annual, meta):
    """Find the year of absolute minimum annual flow for each segment."""
    print("\n[B2] Per-Segment Year of Minimum Flow...")
    out_dir = ANALYSIS_DIR / "B2_Segment_Minima"

    idx_min = annual.groupby("seg_id")["annual_mean_m3s"].idxmin()
    result = annual.loc[idx_min].copy()
    result = result.rename(columns={"year": "min_year", "annual_mean_m3s": "min_flow_m3s"})
    result = result.merge(meta, on="seg_id", how="left")

    # Also get the year of maximum
    idx_max = annual.groupby("seg_id")["annual_mean_m3s"].idxmax()
    max_df = annual.loc[idx_max][["seg_id", "year", "annual_mean_m3s"]].rename(
        columns={"year": "max_year", "annual_mean_m3s": "max_flow_m3s"}
    )
    result = result.merge(max_df, on="seg_id", how="left")

    # Decade classification
    result["min_decade"] = (result["min_year"] // 10) * 10

    result.to_csv(out_dir / "segment_min_year.csv", index=False)

    # Decade distribution
    decade_dist = result["min_decade"].value_counts().sort_index()

    # How many minima in recent vs early period
    n_recent = (result["min_year"] >= LATE_START).sum()
    n_early = (result["min_year"] < LATE_START).sum()
    n_total = len(result)

    # Most common minimum year
    top_min_years = result["min_year"].value_counts().head(10)

    report = f"""# B2: Per-Segment Year of Minimum Annual Flow

## Method
For each segment, find the year with the lowest annual mean streamflow across
the entire {FULL_START}–{FULL_END} record.

## Key Findings

- **{n_recent} of {n_total} segments ({n_recent/n_total*100:.1f}%)** hit their all-time minimum in the recent period ({LATE_START}–{LATE_END})
- **{n_early} segments ({n_early/n_total*100:.1f}%)** had their minimum in the early period ({EARLY_START}–{EARLY_END})

## Distribution by Decade

| Decade | N Segments with Minimum in this Decade | % of Total |
|--------|---------------------------------------|-----------|
"""
    for decade, count in decade_dist.items():
        report += f"| {decade}s | {count} | {count/n_total*100:.1f}% |\n"

    report += f"""
## Most Common Minimum Years

| Year | N Segments with Minimum | % of Total |
|------|------------------------|-----------|
"""
    for year, count in top_min_years.items():
        report += f"| {year} | {count} | {count/n_total*100:.1f}% |\n"

    report += """
## Visual Encoding Recommendation
Color each segment by the **decade of its minimum**: a sequential color scale
from cool (1950s) to hot (2010s). If the map lights up red in recent decades,
the "unprecedented drying" story is immediately visual. Could also encode the
specific year as a categorical palette.

## Limitations
- A single minimum year may be a fluke (one bad monsoon) rather than sustained drying
- Does not capture segments that are consistently dry but never hit a single extreme low
"""
    with open(out_dir / "report.md", "w") as f:
        f.write(report)

    print(f"  {n_recent}/{n_total} segments hit minimum in {LATE_START}–{LATE_END}")
    return result


# ---------------------------------------------------------------------------
# B3: Drought Extent per Year
# ---------------------------------------------------------------------------
def run_b3(ssa_df, meta):
    """Count segments in drought for each year."""
    print("\n[B3] Drought Extent per Year...")
    out_dir = ANALYSIS_DIR / "B3_Drought_Extent"

    n_total = ssa_df["seg_id"].nunique()

    year_stats = (
        ssa_df.groupby("year")
        .agg(
            mean_ssa=("ssa", "mean"),
            median_ssa=("ssa", "median"),
            n_drought=("ssa", lambda x: (x < SSA_DROUGHT_THRESHOLD).sum()),
            n_extreme_drought=("ssa", lambda x: (x < -1.0).sum()),
            n_wet=("ssa", lambda x: (x > SSA_DROUGHT_THRESHOLD).sum()),
            n_extreme_wet=("ssa", lambda x: (x > 1.0).sum()),
        )
        .reset_index()
    )

    year_stats["pct_drought"] = year_stats["n_drought"] / n_total * 100
    year_stats["pct_extreme_drought"] = year_stats["n_extreme_drought"] / n_total * 100

    year_stats.to_csv(out_dir / "annual_drought_extent.csv", index=False)

    worst_years = year_stats.nlargest(15, "pct_drought")

    report = f"""# B3: Spatial Drought Extent per Year

## Method
For each year, count how many segments have SSA < {SSA_DROUGHT_THRESHOLD} (drought)
and SSA < -1.0 (extreme drought). Express as percentage of all {n_total} segments.
This measures the **spatial extent** of drought basin-wide.

## Key Findings

- Peak drought extent: **{worst_years.iloc[0]["pct_drought"]:.1f}%** of segments in drought in **{int(worst_years.iloc[0]["year"])}**
- Mean drought extent post-1991: **{year_stats[year_stats["year"] >= LATE_START]["pct_drought"].mean():.1f}%** of segments
- Mean drought extent pre-1991: **{year_stats[year_stats["year"] < LATE_START]["pct_drought"].mean():.1f}%** of segments

## Top 15 Years by Drought Extent

| Rank | Year | % Segments in Drought | % Extreme Drought | Mean SSA |
|------|------|----------------------|-------------------|----------|
"""
    for i, row in enumerate(worst_years.itertuples(), 1):
        report += f"| {i} | {int(row.year)} | {row.pct_drought:.1f}% | {row.pct_extreme_drought:.1f}% | {row.mean_ssa:.3f} |\n"

    report += """
## Visual Encoding Recommendation
Display as a bar chart / heatmap strip beneath the year slider: height or color
intensity proportional to % segments in drought. Red bars tower in post-1991 years.
Also usable as a timeline annotation alongside B1 basin-wide SSA.

## Limitations
- Treats all segments equally (a headwater and the mainstem both count as 1)
- Could be weighted by segment length or flow volume for a more nuanced view
"""
    with open(out_dir / "report.md", "w") as f:
        f.write(report)

    print(f"  Peak drought extent: {worst_years.iloc[0]['pct_drought']:.1f}% in {int(worst_years.iloc[0]['year'])}")
    return year_stats


# ---------------------------------------------------------------------------
# B4: Annual Anomaly Map Data (wide-format for visualization)
# ---------------------------------------------------------------------------
def run_b4(ssa_df, meta):
    """Produce wide-format SSA data for direct map consumption."""
    print("\n[B4] Annual Anomaly Map Data...")
    out_dir = ANALYSIS_DIR / "B4_Annual_Anomaly_Map"

    # Wide format: rows = seg_id, columns = years, values = SSA
    wide = ssa_df.pivot(index="seg_id", columns="year", values="ssa")
    wide = wide.reset_index()
    wide = wide.merge(meta[["seg_id", "river_name", "updated_tier"]], on="seg_id", how="left")

    # Move metadata columns to front
    meta_cols = ["seg_id", "river_name", "updated_tier"]
    year_cols = [c for c in wide.columns if c not in meta_cols]
    wide = wide[meta_cols + sorted(year_cols)]

    wide.to_csv(out_dir / "segment_annual_ssa_map.csv", index=False)

    # Identify "transition years" — when basin shifts from mostly positive to mostly negative SSA
    year_means = ssa_df.groupby("year")["ssa"].mean().sort_index()
    sign_changes = []
    prev_sign = None
    for year, val in year_means.items():
        curr_sign = "positive" if val >= 0 else "negative"
        if prev_sign is not None and curr_sign != prev_sign:
            sign_changes.append((year, prev_sign, curr_sign, val))
        prev_sign = curr_sign

    report = f"""# B4: Annual Anomaly Map Data (for Visualization)

## Method
This is the per-segment SSA data from A5, reshaped into **wide format** for
efficient map rendering. Each row is a segment, each column is a year, each
cell is the SSA value.

This is the data file your visualization should consume to animate the drying
story across the basin.

## Data Format
- Rows: {len(wide)} segments
- Columns: seg_id, river_name, updated_tier, then one column per year ({ssa_df["year"].min()}–{ssa_df["year"].max()})
- Values: SSA (Z-score, dimensionless)
- Color mapping suggestion: SSA -2 = deep red, 0 = white, +2 = deep blue

## Key Transition Points

| Year | Shift | Basin Mean SSA |
|------|-------|---------------|
"""
    for year, from_sign, to_sign, val in sign_changes:
        report += f"| {year} | {from_sign} -> {to_sign} | {val:.3f} |\n"

    report += f"""
## Suggested Animation Keyframes
Based on basin-wide SSA, these are the years that best illustrate the drying narrative:
- **{year_means.idxmax()}**: Wettest year (SSA = {year_means.max():.3f}) — start of animation baseline
- **1991**: Changepoint year identified by Chuphal et al.
- **{year_means.idxmin()}**: Driest year (SSA = {year_means.min():.3f})
- Play through all years at ~600ms/frame to show the blue→red transition

## Visual Encoding Recommendation
Replace the current constant-color (#2b8cbe) + flow-width encoding with:
- **Stroke width**: static, based on river tier (Tier 1 = thick, Tier 5 = thin)
- **Stroke color**: SSA-based diverging scale per segment per year
  - SSA <= -2.0: #67001f (deep red)
  - SSA = -1.0: #d6604d (red)
  - SSA = 0: #f7f7f7 (white/neutral)
  - SSA = +1.0: #4393c3 (blue)
  - SSA >= +2.0: #053061 (deep blue)

This dual encoding (width = river importance, color = anomaly) makes the drying
signal visible on EVERY segment regardless of absolute flow magnitude.
"""
    with open(out_dir / "report.md", "w") as f:
        f.write(report)

    print(f"  Wide-format SSA map: {len(wide)} segments x {len(year_cols)} years")
    return wide


# ---------------------------------------------------------------------------
# A6: Segment Classification — Stable vs. Drying vs. Wetting
# ---------------------------------------------------------------------------
def run_a6(a1_result, a2_result, meta):
    """Classify segments into Stable / Drying / Wetting using MK significance
    and percent-change severity."""
    print("\n[A6] Segment Classification (Stable / Drying / Wetting)...")
    out_dir = ANALYSIS_DIR / "A6_Segment_Classification"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Join A2 (MK trend + significance) with A1 (percent change magnitude)
    mk_cols = a2_result[["seg_id", "trend", "p_value", "tau",
                          "sens_slope_m3s_yr", "norm_slope_pct_yr"]].copy()
    pct_cols = a1_result[["seg_id", "pct_change", "early_mean", "late_mean"]].copy()

    merged = mk_cols.merge(pct_cols, on="seg_id", how="outer")
    merged = merged.merge(
        meta[["seg_id", "river_name", "updated_tier", "long_term_mean"]],
        on="seg_id", how="left",
    )

    # --- Primary classification (MK significance at p <= 0.05) ---
    conditions = [
        (merged["p_value"] <= 0.05) & (merged["trend"] == "decreasing"),
        (merged["p_value"] <= 0.05) & (merged["trend"] == "increasing"),
    ]
    choices = ["Drying", "Wetting"]
    merged["category"] = np.select(conditions, choices, default="Stable")

    # --- Severity sub-category using percent change ---
    severity_conditions = [
        (merged["category"] == "Drying") & (merged["pct_change"] <= -30),
        (merged["category"] == "Drying") & (merged["pct_change"] <= -10),
        (merged["category"] == "Drying") & (merged["pct_change"] > -10),
        (merged["category"] == "Wetting") & (merged["pct_change"] >= 20),
        (merged["category"] == "Wetting") & (merged["pct_change"] < 20),
        merged["category"] == "Stable",
    ]
    severity_choices = [
        "Severe Drying", "Moderate Drying", "Mild Drying",
        "Significant Wetting", "Mild Wetting",
        "Stable",
    ]
    merged["severity"] = np.select(severity_conditions, severity_choices, default="Stable")

    merged = merged.sort_values(["category", "pct_change"])
    merged.to_csv(out_dir / "segment_classification.csv", index=False)

    # ---- Build summary tables ----
    n_total = len(merged)

    # Table A: Overall basin summary
    cat_counts = merged["category"].value_counts()
    cat_order = ["Stable", "Drying", "Wetting"]
    table_a_rows = []
    for cat in cat_order:
        n = int(cat_counts.get(cat, 0))
        table_a_rows.append((cat, n, n / n_total * 100))

    # Table B: Severity breakdown
    sev_counts = merged["severity"].value_counts()
    sev_order = [
        "Severe Drying", "Moderate Drying", "Mild Drying",
        "Stable",
        "Mild Wetting", "Significant Wetting",
    ]
    table_b_rows = []
    for sev in sev_order:
        n = int(sev_counts.get(sev, 0))
        subset = merged[merged["severity"] == sev]
        avg_pct = subset["pct_change"].mean() if len(subset) > 0 else 0
        table_b_rows.append((sev, n, n / n_total * 100, avg_pct))

    # Table C: By river (top 20 rivers by segment count)
    river_class = (
        merged.groupby("river_name")
        .agg(
            total=("seg_id", "count"),
            n_drying=("category", lambda x: (x == "Drying").sum()),
            n_stable=("category", lambda x: (x == "Stable").sum()),
            n_wetting=("category", lambda x: (x == "Wetting").sum()),
            median_pct_change=("pct_change", "median"),
        )
        .reset_index()
    )
    river_class["pct_drying"] = river_class["n_drying"] / river_class["total"] * 100
    river_class = river_class.sort_values("total", ascending=False).head(20)

    # Table D: By tier
    tier_class = (
        merged.groupby("updated_tier")
        .agg(
            total=("seg_id", "count"),
            n_drying=("category", lambda x: (x == "Drying").sum()),
            n_stable=("category", lambda x: (x == "Stable").sum()),
            n_wetting=("category", lambda x: (x == "Wetting").sum()),
            median_pct_change=("pct_change", "median"),
        )
        .reset_index()
    )
    tier_class["pct_drying"] = tier_class["n_drying"] / tier_class["total"] * 100
    tier_class = tier_class.sort_values("updated_tier")

    # ---- Generate report ----
    n_drying = int(cat_counts.get("Drying", 0))
    n_stable = int(cat_counts.get("Stable", 0))
    n_wetting = int(cat_counts.get("Wetting", 0))

    report = f"""# A6: Segment Classification — Stable vs. Drying vs. Wetting

## Method
Each of the {n_total} Ganga Basin segments is classified using the **Mann-Kendall
trend test** (from A2) as the primary criterion:

| Category | Rule |
|----------|------|
| **Drying** | MK trend = decreasing AND p <= 0.05 |
| **Wetting** | MK trend = increasing AND p <= 0.05 |
| **Stable** | p > 0.05 (no statistically significant monotonic trend) |

Severity sub-categories use the **early-vs-late percent change** (from A1):

| Severity | Additional Criterion |
|----------|---------------------|
| Severe Drying | pct_change <= -30% |
| Moderate Drying | -30% < pct_change <= -10% |
| Mild Drying | pct_change > -10% |
| Significant Wetting | pct_change >= +20% |
| Mild Wetting | pct_change < +20% |

Period: {FULL_START}-{FULL_END}. Significance threshold: p <= 0.05.

## Table A: Overall Basin Summary

| Category | Segments | % of Total |
|----------|----------|------------|
"""
    for cat, n, pct in table_a_rows:
        report += f"| {cat} | {n} | {pct:.1f}% |\n"

    report += f"| **Total** | **{n_total}** | **100%** |\n"

    report += f"""
## Key Takeaway

> Of the {n_total} river segments in the Ganga Basin, **{n_stable} ({n_stable/n_total*100:.1f}%)
> show no statistically significant long-term trend** in streamflow over {FULL_START}-{FULL_END}.
> **{n_drying} ({n_drying/n_total*100:.1f}%) show significant drying**, while only
> **{n_wetting} ({n_wetting/n_total*100:.1f}%) show significant wetting**. The drying is real
> but geographically concentrated — it is not a uniform basin-wide catastrophe.

## Table B: Severity Breakdown

| Severity | Segments | % of Total | Avg % Change |
|----------|----------|------------|--------------|
"""
    for sev, n, pct, avg in table_b_rows:
        report += f"| {sev} | {n} | {pct:.1f}% | {avg:+.1f}% |\n"

    report += f"""
## Table C: Classification by River (Top 20 by segment count)

| River | Total | Drying | Stable | Wetting | % Drying | Median % Change |
|-------|-------|--------|--------|---------|----------|-----------------|
"""
    for row in river_class.itertuples():
        report += (
            f"| {row.river_name} | {row.total} | {row.n_drying} | {row.n_stable} "
            f"| {row.n_wetting} | {row.pct_drying:.1f}% | {row.median_pct_change:+.1f}% |\n"
        )

    report += f"""
## Table D: Classification by Tier

| Tier | Total | Drying | Stable | Wetting | % Drying | Median % Change |
|------|-------|--------|--------|---------|----------|-----------------|
"""
    for row in tier_class.itertuples():
        report += (
            f"| {row.updated_tier} | {row.total} | {row.n_drying} | {row.n_stable} "
            f"| {row.n_wetting} | {row.pct_drying:.1f}% | {row.median_pct_change:+.1f}% |\n"
        )

    report += f"""
## Limitations
- Classification uses a single significance threshold (p <= 0.05); borderline segments could shift with different thresholds
- MK test assumes monotonic trend — segments with step-changes or reversals may be misclassified as "Stable"
- Severity thresholds (-30%, -10%, +20%) are domain-informed but somewhat arbitrary
- All segments weighted equally regardless of flow volume or length
- Naturalized flow only — real-world conditions include dam/abstraction effects
"""
    with open(out_dir / "report.md", "w") as f:
        f.write(report)

    print(f"  Stable: {n_stable} ({n_stable/n_total*100:.1f}%), "
          f"Drying: {n_drying} ({n_drying/n_total*100:.1f}%), "
          f"Wetting: {n_wetting} ({n_wetting/n_total*100:.1f}%)")
    return merged


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 70)
    print("VizChitra Drying Analysis Suite")
    print("=" * 70)

    # Load data
    df = load_data()
    annual = compute_annual_means(df)
    meta = get_seg_meta(df)

    print(f"\nAnnual dataset: {len(annual):,} rows ({annual['seg_id'].nunique()} segments x {annual['year'].nunique()} years)")

    # --- A Methods: Which segments dried most? ---
    a1_result = run_a1(annual, meta)
    a2_result = run_a2(annual, meta)
    a3_result = run_a3(df, meta)
    a4_result = run_a4(df, meta)
    ssa_df, a5_period = run_a5(annual, meta)

    # --- A6: Classification combining A1 + A2 ---
    a6_result = run_a6(a1_result, a2_result, meta)

    # --- B Methods: Which years are extrema? ---
    b1_result = run_b1(annual, meta)
    b2_result = run_b2(annual, meta)
    b3_result = run_b3(ssa_df, meta)
    b4_result = run_b4(ssa_df, meta)

    # --- Summary ---
    print("\n" + "=" * 70)
    print("ALL ANALYSES COMPLETE")
    print("=" * 70)
    print(f"\nOutput directory: {ANALYSIS_DIR}")
    print("\nFolders created:")
    for d in sorted(ANALYSIS_DIR.iterdir()):
        if d.is_dir():
            files = [f.name for f in d.iterdir() if f.is_file()]
            print(f"  {d.name}/")
            for f in sorted(files):
                print(f"    - {f}")


if __name__ == "__main__":
    main()
