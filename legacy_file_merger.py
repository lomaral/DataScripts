"""
Legacy File Merger for Salesforce Data Migration
=================================================
Merges multiple legacy extract files (different column slices) into a single
unified file per object, ready for Data Loader import.

Usage:
    python legacy_file_merger.py <object_name>
    python legacy_file_merger.py accounts
    python legacy_file_merger.py contacts
    python legacy_file_merger.py --all  (processes all objects in config)

Outputs:
    - output/<object>_merged.csv        → Data Loader ready file
    - output/<object>_anomaly_report.csv → Duplicates and missing ID flags
"""

import pandas as pd
import json
import sys
import os
from datetime import datetime


def load_config(config_path: str = "config.json") -> dict:
    """Load object configurations from JSON file."""
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        return json.load(f)


def process_file(filepath: str, id_column: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load a single file, dedupe on id_column (keep first), return clean df and duplicates.
    
    Returns:
        - clean_df: deduplicated dataframe (first occurrence kept)
        - duplicates_df: rows that were duplicates (for reporting)
    """
    if not os.path.exists(filepath):
        print(f"  WARNING: File not found: {filepath}")
        return pd.DataFrame(), pd.DataFrame()
    
    df = pd.read_csv(filepath, dtype=str)  # Read all as strings to preserve data
    
    if id_column not in df.columns:
        print(f"  ERROR: ID column '{id_column}' not found in {filepath}")
        print(f"         Available columns: {list(df.columns)}")
        return pd.DataFrame(), pd.DataFrame()
    
    # Find duplicates BEFORE deduping
    duplicate_mask = df.duplicated(subset=[id_column], keep='first')
    duplicates_df = df[duplicate_mask].copy()
    if not duplicates_df.empty:
        duplicates_df['_source_file'] = os.path.basename(filepath)
        duplicates_df['_issue'] = 'duplicate_in_file'
    
    # Dedupe - keep first occurrence
    clean_df = df.drop_duplicates(subset=[id_column], keep='first')
    
    print(f"  {os.path.basename(filepath)}: {len(df)} rows → {len(clean_df)} unique ({len(duplicates_df)} duplicates)")
    
    return clean_df, duplicates_df


def merge_files(config: dict, id_column: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merge all files for an object using outer join on id_column.
    
    Returns:
        - merged_df: fully merged dataframe
        - anomalies_df: all anomalies (duplicates + missing from files)
    """
    files = config['files']
    all_duplicates = []
    dataframes = []
    file_id_sets = {}  # Track which IDs are in which files
    
    print(f"\nProcessing {len(files)} files...")
    
    for filepath in files:
        clean_df, duplicates_df = process_file(filepath, id_column)
        
        if clean_df.empty:
            continue
        
        # Track IDs per file for anomaly reporting
        file_id_sets[os.path.basename(filepath)] = set(clean_df[id_column].dropna())
        
        # Add source suffix to columns (except ID column) to track origin
        # We'll clean this up after merge
        dataframes.append((filepath, clean_df))
        
        if not duplicates_df.empty:
            all_duplicates.append(duplicates_df)
    
    if not dataframes:
        print("ERROR: No valid dataframes to merge!")
        return pd.DataFrame(), pd.DataFrame()
    
    # Start with first file as base
    print(f"\nMerging on '{id_column}'...")
    merged_df = dataframes[0][1].copy()
    
    # Outer join each subsequent file
    for filepath, df in dataframes[1:]:
        # Find overlapping columns (besides ID)
        overlap_cols = set(merged_df.columns) & set(df.columns) - {id_column}
        
        if overlap_cols:
            print(f"  Note: Overlapping columns with {os.path.basename(filepath)}: {overlap_cols}")
            print(f"        Keeping values from first file encountered")
        
        # Merge with suffixes for any overlapping columns
        merged_df = pd.merge(
            merged_df,
            df,
            on=id_column,
            how='outer',
            suffixes=('', f'__{os.path.basename(filepath)}')
        )
        
        # For overlapping columns, keep the original (non-null) value, fill with new if null
        for col in overlap_cols:
            dup_col = f"{col}__{os.path.basename(filepath)}"
            if dup_col in merged_df.columns:
                # Fill nulls in original with values from duplicate column
                merged_df[col] = merged_df[col].fillna(merged_df[dup_col])
                # Drop the duplicate column
                merged_df.drop(columns=[dup_col], inplace=True)
    
    # Identify which IDs are missing from some files
    all_ids = set(merged_df[id_column].dropna())
    all_files = set(file_id_sets.keys())
    
    complete_ids = set()  # IDs that exist in ALL files
    incomplete_records = []  # IDs missing from some files
    
    for id_val in all_ids:
        missing_from = []
        for filename, id_set in file_id_sets.items():
            if id_val not in id_set:
                missing_from.append(filename)
        
        if missing_from:
            incomplete_records.append({
                id_column: id_val,
                '_issue': 'missing_from_files',
                '_missing_from': ', '.join(missing_from)
            })
        else:
            complete_ids.add(id_val)
    
    # Split merged data: complete vs incomplete
    clean_df = merged_df[merged_df[id_column].isin(complete_ids)].copy()
    incomplete_df = merged_df[~merged_df[id_column].isin(complete_ids)].copy()
    
    # Add missing file info to incomplete records
    if not incomplete_df.empty:
        incomplete_lookup = {r[id_column]: r['_missing_from'] for r in incomplete_records}
        incomplete_df['_missing_from'] = incomplete_df[id_column].map(incomplete_lookup)
    
    # Combine all anomalies (duplicates only - incomplete records go to separate file)
    anomalies_df = pd.concat(all_duplicates, ignore_index=True) if all_duplicates else pd.DataFrame()
    
    print(f"\nMerge complete:")
    print(f"  Clean records (in all files): {len(clean_df)}")
    print(f"  Incomplete records (missing from some files): {len(incomplete_df)}")
    print(f"  Duplicates flagged: {len(anomalies_df)}")
    
    return clean_df, incomplete_df, anomalies_df


def process_object(object_name: str, config: dict, output_dir: str = "output"):
    """Process a single object: merge files and generate reports."""
    print(f"\n{'='*60}")
    print(f"Processing: {object_name.upper()}")
    print(f"{'='*60}")
    
    id_column = config['id_column']
    print(f"ID Column: {id_column}")
    print(f"Files: {config['files']}")
    
    # Merge all files
    clean_df, incomplete_df, duplicates_df = merge_files(config, id_column)
    
    if clean_df.empty and incomplete_df.empty:
        print(f"ERROR: No data merged for {object_name}")
        return
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Save clean merged file (records in ALL files - ready for Data Loader)
    if not clean_df.empty:
        merged_path = os.path.join(output_dir, f"{object_name}_merged.csv")
        clean_df.to_csv(merged_path, index=False)
        print(f"\n✓ Merged file saved: {merged_path}")
        print(f"  Records: {len(clean_df)} (complete - exist in all files)")
        print(f"  Columns: {list(clean_df.columns)}")
    else:
        print(f"\n⚠ No complete records found (no IDs exist in all files)")
    
    # Save incomplete records (missing from some files - needs review)
    if not incomplete_df.empty:
        incomplete_path = os.path.join(output_dir, f"{object_name}_incomplete.csv")
        incomplete_df.to_csv(incomplete_path, index=False)
        print(f"✓ Incomplete records saved: {incomplete_path}")
        print(f"  Records: {len(incomplete_df)} (missing from some files - review needed)")
    
    # Save duplicates report if any
    if not duplicates_df.empty:
        duplicates_path = os.path.join(output_dir, f"{object_name}_duplicates.csv")
        duplicates_df.to_csv(duplicates_path, index=False)
        print(f"✓ Duplicates report saved: {duplicates_path}")
        print(f"  Records: {len(duplicates_df)} (kept first occurrence, these were dropped)")


def main():
    if len(sys.argv) < 2:
        print("Usage: python legacy_file_merger.py <object_name>")
        print("       python legacy_file_merger.py --all")
        print("\nExample: python legacy_file_merger.py accounts")
        sys.exit(1)
    
    config = load_config()
    target = sys.argv[1].lower()
    
    if target == '--all':
        # Process all objects
        for object_name in config.keys():
            process_object(object_name, config[object_name])
    elif target in config:
        process_object(target, config[target])
    else:
        print(f"ERROR: Object '{target}' not found in config.json")
        print(f"Available objects: {list(config.keys())}")
        sys.exit(1)
    
    print(f"\n{'='*60}")
    print("Done!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()