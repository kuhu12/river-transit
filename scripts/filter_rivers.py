#!/usr/bin/env python3
"""
Filter river_outlet_lookup.csv to final 35 target rivers.

Steps:
1. Fix duplicates by remapping to canonical names
2. Exclude garbage entries
3. Filter to final 35 target rivers
4. Output cleaned lookup CSV and report
"""

import pandas as pd
import re
from pathlib import Path

# Step 1: Duplicate remapping
DUPLICATE_REMAPPING = {
    "Ganges"               : "Ganga",
    "Koshi River"          : "Kosi",
    "Gomati"               : "Gomti",
    "Gomtī"                : "Gomti",
    "Kamla River"          : "Kamala",
    "Gambhiri"             : "Gambhir",
    "Gambhir;Parbati"      : "Gambhir",
    "Mahakali River"       : "Sharda",
    "Sipra"                : "Shipra",
    "Narayani"             : "Gandak",
    "Ghaghara River"       : "Ghaghara",
    "Chambal River"        : "Chambal",
    "Mahananda River"      : "Mahananda",
    "Kuno River"           : "Kuno",
    "North Koel River"     : "North Koel",
    "Gaula River"          : "Gaula",
    "Kamala River"         : "Kamala",
    "Bhagirathi River"     : "Bhagirathi",
    "Alaknanda River"      : "Alaknanda",
    "Hindon River"         : "Hindon",
    "Mechi River"          : "Mechi",
}

# Step 2: Garbage entries to remove
GARBAGE_ENTRIES = [
    "布抄老曲",
    "mAU nALA",
    "Mungeshpur Drain",
    "Gambhir",
    "Gambhir;Parbati",
]

# Step 3: Target rivers with tiers
TARGET_RIVERS = {
    # Tier 1 - Main Stem
    "Ganga": 1,

    # Tier 2 - Himalayan Headstreams
    "Bhagirathi": 2,
    "Alaknanda": 2,
    "Mandakini": 2,

    # Tier 3 - Left Bank Major Tributaries
    "Ramganga": 3,
    "Gomti": 3,
    "Ghaghara": 3,
    "Rapti": 3,
    "Sarju": 3,
    "Gandak": 3,
    "Burhi Gandak": 3,

    # Tier 4 - Left Bank Eastern Tributaries
    "Bagmati": 4,
    "Kamala": 4,
    "Kosi": 4,
    "Mechi": 4,
    "Mahananda": 4,

    # Tier 5 - Right Bank Major Tributaries
    "Yamuna": 5,
    "Tons": 5,
    "Chambal": 5,
    "Betwa": 5,
    "Ken": 5,
    "Son": 5,

    # Tier 6 - Right Bank Secondary Rivers
    "Sharda": 6,
    "Hindon": 6,
    "Sindh": 6,
    "Kali Sindh": 6,
    "Karmanasa": 6,
    "Punpun": 6,
    "North Koel": 6,

    # Tier 7 - Additional Significant Rivers
    "Gaula": 7,
    "Rihand": 7,
    "Gerua": 7,
    "Rohini": 7,
}


def has_non_latin(text: str) -> bool:
    """Check if text contains non-Latin characters."""
    if pd.isna(text):
        return False
    # Allow Latin letters, numbers, spaces, and common punctuation
    return bool(re.search(r'[^\x00-\x7F]', str(text)))


def is_garbage_name(name: str) -> bool:
    """Check if a river name should be excluded as garbage."""
    if pd.isna(name) or name.strip() == "":
        return True
    if name in GARBAGE_ENTRIES:
        return True
    if has_non_latin(name):
        return True
    # Check for drainage/canal patterns
    name_lower = name.lower()
    if any(pattern in name_lower for pattern in ['drain', 'nala', 'canal']):
        return True
    return False


def main():
    base_dir = Path(__file__).parent.parent
    input_path = base_dir / "Processed Data" / "river_outlet_lookup.csv"
    output_csv_path = base_dir / "Processed Data" / "river_final_lookup.csv"
    output_report_path = base_dir / "Processed Data" / "river_final_report.txt"

    # Load data
    df = pd.read_csv(input_path)
    original_count = len(df)
    print(f"Loaded {original_count} rivers from {input_path}")

    # Track statistics
    stats = {
        'original_count': original_count,
        'duplicates_resolved': 0,
        'garbage_removed': 0,
        'final_count': 0,
        'found_rivers': [],
        'missing_rivers': [],
        'duplicate_details': [],
        'garbage_details': [],
    }

    # Step 1: Remap duplicates
    df['river_name_original'] = df['river_name']
    for old_name, new_name in DUPLICATE_REMAPPING.items():
        mask = df['river_name'] == old_name
        if mask.any():
            stats['duplicates_resolved'] += mask.sum()
            stats['duplicate_details'].append(f"  {old_name} -> {new_name} ({mask.sum()} rows)")
            df.loc[mask, 'river_name'] = new_name

    # After remapping, if there are duplicate canonical names, keep the one with lowest seg_id
    # (or we could keep lowest BotElev, but seg_id is simpler)
    df_dedup = df.sort_values('seg_id').drop_duplicates(subset='river_name', keep='first')
    dedup_removed = len(df) - len(df_dedup)
    if dedup_removed > 0:
        print(f"  Removed {dedup_removed} duplicate rows after canonical remapping")
    df = df_dedup

    # Step 2: Remove garbage entries
    garbage_mask = df['river_name'].apply(is_garbage_name)
    garbage_rows = df[garbage_mask]
    for _, row in garbage_rows.iterrows():
        stats['garbage_details'].append(f"  {row['river_name']} (seg_id: {row['seg_id']})")
    stats['garbage_removed'] = garbage_mask.sum()
    df = df[~garbage_mask].copy()

    print(f"After removing garbage: {len(df)} rivers")

    # Step 3: Filter to target rivers
    df['is_target'] = df['river_name'].isin(TARGET_RIVERS.keys())
    df_final = df[df['is_target']].copy()

    # Add tier information
    df_final['tier'] = df_final['river_name'].map(TARGET_RIVERS)

    # Sort by tier, then by river name
    df_final = df_final.sort_values(['tier', 'river_name']).reset_index(drop=True)

    # Track found and missing rivers
    found_rivers = set(df_final['river_name'].unique())
    all_target_rivers = set(TARGET_RIVERS.keys())
    missing_rivers = all_target_rivers - found_rivers

    stats['found_rivers'] = sorted(found_rivers)
    stats['missing_rivers'] = sorted(missing_rivers)
    stats['final_count'] = len(df_final)

    # Step 4: Output files

    # 4a: CSV output
    df_final[['river_name', 'seg_id', 'tier']].to_csv(output_csv_path, index=False)
    print(f"\nSaved {len(df_final)} rivers to {output_csv_path}")

    # 4b: Report output
    report_lines = [
        "=" * 60,
        "RIVER FILTERING REPORT",
        "=" * 60,
        "",
        f"Input file: {input_path}",
        f"Original river count: {stats['original_count']}",
        "",
        "-" * 60,
        "STEP 1: DUPLICATE RESOLUTION",
        "-" * 60,
        f"Duplicates remapped: {stats['duplicates_resolved']}",
    ]

    if stats['duplicate_details']:
        report_lines.append("Details:")
        report_lines.extend(stats['duplicate_details'])
    else:
        report_lines.append("  (none found)")

    report_lines.extend([
        "",
        "-" * 60,
        "STEP 2: GARBAGE REMOVAL",
        "-" * 60,
        f"Garbage entries removed: {stats['garbage_removed']}",
    ])

    if stats['garbage_details']:
        report_lines.append("Details:")
        report_lines.extend(stats['garbage_details'])
    else:
        report_lines.append("  (none found)")

    report_lines.extend([
        "",
        "-" * 60,
        "STEP 3: TARGET RIVER FILTERING",
        "-" * 60,
        f"Target rivers requested: {len(TARGET_RIVERS)}",
        f"Target rivers found: {len(stats['found_rivers'])}",
        f"Target rivers missing: {len(stats['missing_rivers'])}",
        "",
        "FOUND RIVERS:",
    ])

    # Group found rivers by tier
    for tier in sorted(set(TARGET_RIVERS.values())):
        tier_rivers = [r for r in stats['found_rivers'] if TARGET_RIVERS.get(r) == tier]
        if tier_rivers:
            tier_names = {
                1: "Main Stem",
                2: "Himalayan Headstreams",
                3: "Left Bank Major Tributaries",
                4: "Left Bank Eastern Tributaries",
                5: "Right Bank Major Tributaries",
                6: "Right Bank Secondary Rivers",
                7: "Additional Significant Rivers",
            }
            report_lines.append(f"  Tier {tier} ({tier_names.get(tier, 'Unknown')}):")
            for river in tier_rivers:
                seg_id = df_final[df_final['river_name'] == river]['seg_id'].values[0]
                report_lines.append(f"    - {river} (seg_id: {seg_id})")

    report_lines.extend([
        "",
        "-" * 60,
        "FINAL OUTPUT",
        "-" * 60,
        f"Final river count: {stats['final_count']}",
        f"Output file: {output_csv_path}",
        "",
    ])

    # Step 5: Missing rivers warning
    if stats['missing_rivers']:
        report_lines.extend([
            "!" * 60,
            "WARNING: MISSING RIVERS",
            "!" * 60,
            "",
            "The following target rivers were NOT found in the data:",
        ])
        for river in stats['missing_rivers']:
            tier = TARGET_RIVERS[river]
            report_lines.append(f"  - {river} (Tier {tier})")
        report_lines.extend([
            "",
            "These need manual seg_id assignment or alternative name matching.",
            "",
        ])

    report_lines.append("=" * 60)

    report_text = "\n".join(report_lines)

    with open(output_report_path, 'w') as f:
        f.write(report_text)

    print(f"Saved report to {output_report_path}")

    # Print summary to console
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Found: {len(stats['found_rivers'])}/{len(TARGET_RIVERS)} target rivers")

    if stats['missing_rivers']:
        print(f"\nWARNING: MISSING RIVERS ({len(stats['missing_rivers'])}):")
        for river in stats['missing_rivers']:
            print(f"  - {river}")
        print("\nThese need manual seg_id assignment or alternative name matching.")
    else:
        print("\nAll target rivers found!")

    return df_final, stats


if __name__ == "__main__":
    df_final, stats = main()
