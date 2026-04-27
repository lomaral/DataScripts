"""
Table Data Pivot for Salesforce Data Migration
===============================================
Transforms table data with multiple rows per ID into single row with numbered columns.

Usage:
    python table_data_pivot.py

Input:
    - pivot_config.json with settings
    - Table data file with multiple rows per ID

Output:
    - Pivoted file with Name_1, Name_2, etc. columns
"""

import pandas as pd
import json
import os
import sys


def load_config(config_path: str = "pivot_config.json") -> dict:
    """Load pivot configuration from JSON file."""
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        return json.load(f)


def pivot_table_data(config: dict, output_dir: str = "output"):
    """Pivot table data from multiple rows to numbered columns."""
    print(f"\n{'='*60}")
    print(f"Pivoting Table Data")
    print(f"{'='*60}")
    
    data_file = config['data_file']
    id_column = config['id_column']
    pivot_columns = config.get('pivot_columns', [])  # Columns to pivot
    output_file = config.get('output_file', 'table_pivoted.csv')
    
    # Check file exists
    if not os.path.exists(data_file):
        print(f"ERROR: Data file not found: {data_file}")
        return
    
    # Load data
    df = pd.read_csv(data_file, dtype=str)
    print(f"Data file: {len(df)} rows")
    print(f"ID column: {id_column}")
    print(f"Pivot columns: {pivot_columns}")
    
    # If no pivot columns specified, use all except ID
    if not pivot_columns:
        pivot_columns = [col for col in df.columns if col != id_column]
        print(f"Auto-detected pivot columns: {pivot_columns}")
    
    # Group by ID and count max rows per ID
    max_rows = df.groupby(id_column).size().max()
    print(f"Max rows per ID: {max_rows}")
    
    # Build pivoted data
    pivoted_data = []
    
    for id_val, group in df.groupby(id_column):
        row_data = {id_column: id_val}
        
        # For each row in group, add numbered columns
        for i, (_, data_row) in enumerate(group.iterrows(), start=1):
            for col in pivot_columns:
                new_col = f"{col}_{i}"
                val = data_row.get(col, '')
                row_data[new_col] = val if pd.notna(val) else ''
        
        pivoted_data.append(row_data)
    
    # Create output dataframe
    pivoted_df = pd.DataFrame(pivoted_data)
    
    # Order columns: ID first, then numbered columns in order
    ordered_cols = [id_column]
    for i in range(1, max_rows + 1):
        for col in pivot_columns:
            col_name = f"{col}_{i}"
            if col_name in pivoted_df.columns:
                ordered_cols.append(col_name)
    
    pivoted_df = pivoted_df[ordered_cols]
    
    # Save output
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, output_file)
    pivoted_df.to_csv(output_path, index=False, encoding='utf-8')
    
    print(f"\n✓ Pivoted file saved: {output_path}")
    print(f"  Records: {len(pivoted_df)}")
    print(f"  Columns: {list(pivoted_df.columns)}")


def main():
    config = load_config()
    pivot_table_data(config)


if __name__ == "__main__":
    main()
