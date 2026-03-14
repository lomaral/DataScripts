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


def process_file(filepath: str, id_column: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    """
    Load a single file, dedupe on id_column (keep first), return clean df and duplicates.
    
    Returns:
        - clean_df: deduplicated dataframe (first occurrence kept)
        - duplicates_df: rows that were duplicates (for reporting)
        - diagnostics: dict with row counts and issues
    """
    diagnostics = {
        'file': os.path.basename(filepath),
        'original_rows': 0,
        'null_ids': 0,
        'whitespace_ids': 0,
        'duplicates': 0,
        'final_rows': 0
    }
    
    if not os.path.exists(filepath):
        print(f"  WARNING: File not found: {filepath}")
        return pd.DataFrame(), pd.DataFrame(), diagnostics
    
    df = pd.read_csv(filepath, dtype=str)  # Read all as strings to preserve data
    diagnostics['original_rows'] = len(df)
    
    if id_column not in df.columns:
        print(f"  ERROR: ID column '{id_column}' not found in {filepath}")
        print(f"         Available columns: {list(df.columns)}")
        return pd.DataFrame(), pd.DataFrame(), diagnostics
    
    # Check for null/empty IDs
    null_mask = df[id_column].isna() | (df[id_column].str.strip() == '')
    null_ids_df = df[null_mask].copy()
    diagnostics['null_ids'] = len(null_ids_df)
    
    # Check for whitespace issues (IDs with leading/trailing spaces)
    whitespace_mask = df[id_column].str.strip() != df[id_column]
    diagnostics['whitespace_ids'] = whitespace_mask.sum()
    
    # Clean the ID column - strip whitespace
    df[id_column] = df[id_column].str.strip()
    
    # Remove null IDs
    df = df[~null_mask]
    
    # Find duplicates BEFORE deduping
    duplicate_mask = df.duplicated(subset=[id_column], keep='first')
    duplicates_df = df[duplicate_mask].copy()
    diagnostics['duplicates'] = len(duplicates_df)
    
    if not duplicates_df.empty:
        duplicates_df['_source_file'] = os.path.basename(filepath)
        duplicates_df['_issue'] = 'duplicate_in_file'
    
    # Dedupe - keep first occurrence
    clean_df = df.drop_duplicates(subset=[id_column], keep='first')
    diagnostics['final_rows'] = len(clean_df)
    
    print(f"  {os.path.basename(filepath)}: {diagnostics['original_rows']} rows → {len(clean_df)} unique")
    if diagnostics['null_ids'] > 0:
        print(f"    ⚠ {diagnostics['null_ids']} rows with NULL/empty ID (excluded)")
    if diagnostics['whitespace_ids'] > 0:
        print(f"    ⚠ {diagnostics['whitespace_ids']} IDs had whitespace (stripped)")
    if diagnostics['duplicates'] > 0:
        print(f"    ⚠ {diagnostics['duplicates']} duplicates (kept first)")
    
    return clean_df, duplicates_df, diagnostics


def merge_files(config: dict, id_column: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list]:
    """
    Merge all files for an object using outer join on id_column.
    
    Returns:
        - merged_df: fully merged dataframe
        - incomplete_df: records missing from some files
        - anomalies_df: duplicates
        - comparison_df: file comparison details
        - all_diagnostics: list of diagnostics per file
    """
    files = config['files']
    all_duplicates = []
    dataframes = []
    file_id_sets = {}  # Track which IDs are in which files
    all_diagnostics = []
    
    print(f"\nProcessing {len(files)} files...")
    
    for filepath in files:
        clean_df, duplicates_df, diagnostics = process_file(filepath, id_column)
        all_diagnostics.append(diagnostics)
        
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
    
    # Build file comparison summary
    file_comparison = []
    file_names = list(file_id_sets.keys())
    for i, file1 in enumerate(file_names):
        for file2 in file_names[i+1:]:
            ids_1 = file_id_sets[file1]
            ids_2 = file_id_sets[file2]
            only_in_1 = ids_1 - ids_2
            only_in_2 = ids_2 - ids_1
            in_both = ids_1 & ids_2
            
            file_comparison.append({
                'file_1': file1,
                'file_2': file2,
                'file_1_total': len(ids_1),
                'file_2_total': len(ids_2),
                'in_both': len(in_both),
                'only_in_file_1': len(only_in_1),
                'only_in_file_2': len(only_in_2),
            })
            
            # Store the actual IDs for detailed reporting
            for id_val in only_in_1:
                file_comparison.append({
                    'file_1': file1,
                    'file_2': file2,
                    'id_value': id_val,
                    'exists_in': file1,
                    'missing_from': file2
                })
            for id_val in only_in_2:
                file_comparison.append({
                    'file_1': file1,
                    'file_2': file2,
                    'id_value': id_val,
                    'exists_in': file2,
                    'missing_from': file1
                })
    
    comparison_df = pd.DataFrame(file_comparison) if file_comparison else pd.DataFrame()
    
    print(f"\nMerge complete:")
    print(f"  Clean records (in all files): {len(clean_df)}")
    print(f"  Incomplete records (missing from some files): {len(incomplete_df)}")
    print(f"  Duplicates flagged: {len(anomalies_df)}")
    
    # Print file comparison summary
    print(f"\nFile Comparison:")
    for fname, ids in file_id_sets.items():
        print(f"  {fname}: {len(ids)} unique IDs")
    
    return clean_df, incomplete_df, anomalies_df, comparison_df, all_diagnostics


def process_object(object_name: str, config: dict, output_dir: str = "output"):
    """Process a single object: merge files and generate reports."""
    print(f"\n{'='*60}")
    print(f"Processing: {object_name.upper()}")
    print(f"{'='*60}")
    
    id_column = config['id_column']
    external_id_prefix = config.get('external_id_prefix', object_name.upper())
    
    print(f"ID Column: {id_column}")
    print(f"External ID Prefix: {external_id_prefix}")
    print(f"Files: {config['files']}")
    
    # Merge all files
    clean_df, incomplete_df, duplicates_df, comparison_df, diagnostics = merge_files(config, id_column)
    
    if clean_df.empty and incomplete_df.empty:
        print(f"ERROR: No data merged for {object_name}")
        return
    
    # Generate External ID column
    if not clean_df.empty:
        clean_df['External_ID__c'] = external_id_prefix + '-' + clean_df[id_column].astype(str)
        # Move External_ID__c to first column
        cols = ['External_ID__c'] + [c for c in clean_df.columns if c != 'External_ID__c']
        clean_df = clean_df[cols]
    
    if not incomplete_df.empty:
        incomplete_df['External_ID__c'] = external_id_prefix + '-' + incomplete_df[id_column].astype(str)
        # Move External_ID__c to first column (before _missing_from)
        cols = ['External_ID__c'] + [c for c in incomplete_df.columns if c not in ['External_ID__c', '_missing_from']] + ['_missing_from']
        incomplete_df = incomplete_df[cols]
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Save diagnostics report (row counts, nulls, whitespace issues)
    if diagnostics:
        diag_path = os.path.join(output_dir, f"{object_name}_diagnostics.csv")
        pd.DataFrame(diagnostics).to_csv(diag_path, index=False)
        print(f"\n✓ Diagnostics saved: {diag_path}")
        print(f"  Shows row counts, null IDs, whitespace issues per file")
    
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
    
    # Save file comparison report
    if not comparison_df.empty:
        comparison_path = os.path.join(output_dir, f"{object_name}_file_comparison.csv")
        comparison_df.to_csv(comparison_path, index=False)
        print(f"✓ File comparison saved: {comparison_path}")
        print(f"  Shows which IDs exist in one file but not another")


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
