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


# US State mappings
STATE_ABBREV_TO_FULL = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
    'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'FL': 'Florida', 'GA': 'Georgia',
    'HI': 'Hawaii', 'ID': 'Idaho', 'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa',
    'KS': 'Kansas', 'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
    'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire', 'NJ': 'New Jersey',
    'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio',
    'OK': 'Oklahoma', 'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah', 'VT': 'Vermont',
    'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia', 'WI': 'Wisconsin', 'WY': 'Wyoming',
    'DC': 'District of Columbia', 'PR': 'Puerto Rico', 'VI': 'Virgin Islands', 'GU': 'Guam'
}
STATE_FULL_TO_ABBREV = {v.upper(): k for k, v in STATE_ABBREV_TO_FULL.items()}


def apply_transformations(df: pd.DataFrame, transformations: dict) -> pd.DataFrame:
    """Apply data transformations to dataframe."""
    if not transformations:
        return df
    
    df = df.copy()
    
    # 0. Filter rows (do this first)
    filter_rows = transformations.get('filter_rows', {})
    for col, allowed_values in filter_rows.items():
        if col not in df.columns:
            print(f"  WARNING: Filter column '{col}' not found")
            continue
        before_count = len(df)
        df = df[df[col].isin(allowed_values)]
        print(f"  Filtered '{col}' to {allowed_values}: {before_count} → {len(df)} rows")
    
    # 0a. Join column early (runs before filter_rows_exclude so you can filter on joined columns)
    join_column_early = transformations.get('join_column_early', {})
    for new_col, settings in join_column_early.items():
        from_file = settings.get('from_file')
        match_column = settings.get('match_column')
        match_to = settings.get('match_to')
        pull_column = settings.get('pull_column')
        
        if not os.path.exists(from_file):
            print(f"  ERROR: Join file not found: {from_file}")
            continue
        
        if match_column not in df.columns:
            print(f"  ERROR: Match column '{match_column}' not found in current file")
            continue
        
        other_df = pd.read_csv(from_file, dtype=str)
        
        if match_to not in other_df.columns:
            print(f"  ERROR: Match to column '{match_to}' not found in {from_file}")
            continue
        
        if pull_column not in other_df.columns:
            print(f"  ERROR: Pull column '{pull_column}' not found in {from_file}")
            continue
        
        join_map = {}
        for _, row in other_df.iterrows():
            key = row[match_to]
            val = row[pull_column]
            if pd.notna(key) and pd.notna(val):
                join_map[str(key).strip()] = str(val).strip()
        
        def do_join(input_val):
            if pd.isna(input_val) or str(input_val).strip() == '':
                return ''
            return join_map.get(str(input_val).strip(), '')
        
        df[new_col] = df[match_column].apply(do_join)
        matched = (df[new_col] != '').sum()
        print(f"  Joined early '{pull_column}' from {from_file} as '{new_col}' ({matched} matched)")
    
    # 0b. Filter rows exclude (exclude rows containing certain values)
    filter_rows_exclude = transformations.get('filter_rows_exclude', {})
    for col, exclude_values in filter_rows_exclude.items():
        if col not in df.columns:
            print(f"  WARNING: Filter exclude column '{col}' not found")
            continue
        before_count = len(df)
        # Exclude rows where column contains any of the exclude values
        mask = df[col].apply(lambda x: not any(str(ev) in str(x) if pd.notna(x) else False for ev in exclude_values))
        df = df[mask]
        print(f"  Excluded rows containing {exclude_values} in '{col}': {before_count} → {len(df)} rows")
    
    # 0c. Split rows (duplicate rows for values with delimiter)
    split_rows = transformations.get('split_rows', {})
    if split_rows:
        source_column = split_rows.get('source_column')
        delimiter = split_rows.get('delimiter', '/')
        output_column = split_rows.get('output_column', source_column)
        
        if source_column not in df.columns:
            print(f"  WARNING: Source column '{source_column}' not found for split_rows")
        else:
            before_count = len(df)
            new_rows = []
            
            for _, row in df.iterrows():
                val = row[source_column]
                if pd.isna(val) or str(val).strip() == '':
                    row_copy = row.copy()
                    row_copy[output_column] = ''
                    new_rows.append(row_copy)
                else:
                    parts = str(val).split(delimiter)
                    for part in parts:
                        row_copy = row.copy()
                        row_copy[output_column] = part.strip()
                        new_rows.append(row_copy)
            
            df = pd.DataFrame(new_rows)
            print(f"  Split rows on '{source_column}' by '{delimiter}': {before_count} → {len(df)} rows")
    
    # 0d. Split rows multi (map multiple columns to multiple output values)
    split_rows_multi = transformations.get('split_rows_multi', {})
    if split_rows_multi:
        source_columns = split_rows_multi.get('source_columns', [])
        output_column = split_rows_multi.get('output_column', 'output')
        mappings = split_rows_multi.get('mappings', {})
        
        missing_cols = [col for col in source_columns if col not in df.columns]
        if missing_cols:
            print(f"  WARNING: Source columns {missing_cols} not found for split_rows_multi")
        else:
            before_count = len(df)
            new_rows = []
            
            for _, row in df.iterrows():
                # Build key from source columns
                key_parts = []
                for col in source_columns:
                    val = row[col]
                    if pd.isna(val):
                        val = ''
                    key_parts.append(str(val).strip())
                key = '|'.join(key_parts)
                key_lower = key.lower()
                
                # Find matching mapping (case-insensitive)
                output_values = None
                for map_key, map_values in mappings.items():
                    if map_key.lower() == key_lower:
                        output_values = map_values
                        break
                
                if output_values:
                    # Create one row per output value
                    for out_val in output_values:
                        row_copy = row.copy()
                        row_copy[output_column] = out_val
                        new_rows.append(row_copy)
                else:
                    # No mapping found - keep original row with empty output
                    row_copy = row.copy()
                    row_copy[output_column] = ''
                    new_rows.append(row_copy)
            
            df = pd.DataFrame(new_rows)
            print(f"  Split rows multi on {source_columns}: {before_count} → {len(df)} rows")
    
    # 1. Merge columns
    merge_columns = transformations.get('merge_columns', {})
    for new_col, source_cols in merge_columns.items():
        def merge_values(row):
            values = []
            for col in source_cols:
                if col in row and pd.notna(row[col]) and str(row[col]).strip():
                    values.append(str(row[col]).strip())
            return ', '.join(values)
        df[new_col] = df.apply(merge_values, axis=1)
        print(f"  Merged columns {source_cols} → {new_col}")
    
    # 1b. Concat columns (prefix + columns + suffix)
    concat_columns = transformations.get('concat_columns', {})
    for new_col, settings in concat_columns.items():
        prefix = settings.get('prefix', '')
        suffix = settings.get('suffix', '')
        columns = settings.get('columns', [])
        separator = settings.get('separator', '')
        
        def concat_values(row):
            values = []
            for col in columns:
                if col in row and pd.notna(row[col]) and str(row[col]).strip():
                    values.append(str(row[col]).strip())
            return prefix + separator.join(values) + suffix
        
        df[new_col] = df.apply(concat_values, axis=1)
        print(f"  Created '{new_col}' = {prefix}{{values}}{suffix}")
    
    # 1c. Copy column (copy one column to a new column)
    copy_column = transformations.get('copy_column', {})
    for new_col, source_col in copy_column.items():
        if source_col not in df.columns:
            print(f"  WARNING: Source column '{source_col}' not found for copy_column")
            continue
        df[new_col] = df[source_col]
        print(f"  Copied '{source_col}' → '{new_col}'")
    
    # 1d. Map column (create new column from source column with value mapping)
    map_column = transformations.get('map_column', {})
    for new_col, settings in map_column.items():
        source_col = settings.get('source_column')
        mappings = settings.get('mappings', {})
        default = settings.get('default', '')
        
        if source_col not in df.columns:
            print(f"  WARNING: Source column '{source_col}' not found for map_column")
            continue
        
        def do_map(val):
            if pd.isna(val) or str(val).strip() == '':
                return default
            val_str = str(val).strip()
            # Case-insensitive lookup
            for k, v in mappings.items():
                if k.lower() == val_str.lower():
                    return v
            return default
        
        df[new_col] = df[source_col].apply(do_map)
        print(f"  Mapped '{source_col}' → '{new_col}'")
    
    # 1e. Map column multi (map based on multiple source columns)
    map_column_multi = transformations.get('map_column_multi', {})
    for new_col, settings in map_column_multi.items():
        source_columns = settings.get('source_columns', [])
        mappings = settings.get('mappings', {})
        default = settings.get('default', '')
        
        # Check all source columns exist
        missing_cols = [col for col in source_columns if col not in df.columns]
        if missing_cols:
            print(f"  WARNING: Source columns {missing_cols} not found for map_column_multi")
            continue
        
        def do_map_multi(row):
            # Build key from source columns: "value1|value2|..."
            values = []
            for col in source_columns:
                val = row[col]
                if pd.isna(val):
                    val = ''
                values.append(str(val).strip())
            key = '|'.join(values)
            key_lower = key.lower()
            
            # Try exact match first
            for k, v in mappings.items():
                if k.lower() == key_lower:
                    return v
            
            # Try wildcard match (e.g., "Draft|*")
            for k, v in mappings.items():
                parts = k.split('|')
                if len(parts) == len(values):
                    match = True
                    for i, part in enumerate(parts):
                        if part != '*' and part.lower() != values[i].lower():
                            match = False
                            break
                    if match:
                        return v
            
            return default
        
        df[new_col] = df.apply(do_map_multi, axis=1)
        print(f"  Mapped {source_columns} → '{new_col}'")
    
    # 2. Blank if value matches
    blank_if_value = transformations.get('blank_if_value', {})
    for col, values_to_blank in blank_if_value.items():
        if col in df.columns:
            mask = df[col].isin(values_to_blank)
            df.loc[mask, col] = ''
            print(f"  Blanked {mask.sum()} values in '{col}' matching {values_to_blank}")
    
    # 3. Blank if all zeros (for postal codes)
    blank_if_zeros = transformations.get('blank_if_zeros', [])
    for col in blank_if_zeros:
        if col in df.columns:
            def is_all_zeros(val):
                if pd.isna(val):
                    return False
                return str(val).strip().replace('0', '') == ''
            mask = df[col].apply(is_all_zeros)
            df.loc[mask, col] = ''
            print(f"  Blanked {mask.sum()} all-zero values in '{col}'")
    
    # 4. State format (abbreviation:full name)
    state_format = transformations.get('state_format', {})
    for col, format_type in state_format.items():
        if col in df.columns:
            def format_state(val):
                if pd.isna(val) or str(val).strip() == '':
                    return ''
                val_str = str(val).strip()
                val_upper = val_str.upper()
                
                # Check if abbreviation
                if val_upper in STATE_ABBREV_TO_FULL:
                    abbrev = val_upper
                    full = STATE_ABBREV_TO_FULL[abbrev]
                # Check if full name
                elif val_upper in STATE_FULL_TO_ABBREV:
                    abbrev = STATE_FULL_TO_ABBREV[val_upper]
                    full = STATE_ABBREV_TO_FULL[abbrev]
                else:
                    return val_str  # Return original if not recognized
                
                return f"{abbrev}: {full}"
            
            df[col] = df[col].apply(format_state)
            print(f"  Formatted state column '{col}' to abbreviation: full")
    
    # 5. Congressional district format (STATE:00)
    congressional_format = transformations.get('congressional_district_format', {})
    for col, settings in congressional_format.items():
        if col in df.columns:
            state_col = settings.get('state_column')
            pad_zeros = settings.get('pad_zeros', 2)
            
            def format_district(row):
                district_val = row[col]
                if pd.isna(district_val) or str(district_val).strip() == '':
                    return ''
                
                # Get state abbreviation
                state_val = row.get(state_col, '') if state_col else ''
                if pd.isna(state_val) or str(state_val).strip() == '':
                    return ''
                
                state_str = str(state_val).strip().upper()
                # Extract abbreviation if in format "XX:Full Name"
                if ':' in state_str:
                    state_abbrev = state_str.split(':')[0]
                elif state_str in STATE_ABBREV_TO_FULL:
                    state_abbrev = state_str
                elif state_str in STATE_FULL_TO_ABBREV:
                    state_abbrev = STATE_FULL_TO_ABBREV[state_str]
                else:
                    state_abbrev = state_str[:2]  # Fallback: first 2 chars
                
                # Format district number with padding
                try:
                    district_num = int(float(str(district_val).strip()))
                    district_str = str(district_num).zfill(pad_zeros)
                except:
                    district_str = str(district_val).strip()
                
                return f"{state_abbrev}:{district_str}"
            
            df[col] = df.apply(format_district, axis=1)
            print(f"  Formatted congressional district '{col}' using state from '{state_col}'")
    
    # 6. Set value (always set column to a specific value)
    set_value = transformations.get('set_value', {})
    for col, value in set_value.items():
        df[col] = value
        print(f"  Set '{col}' to '{value}' for all rows")
    
    # 6b. Replace text (simple find/replace characters in a column)
    replace_text = transformations.get('replace_text', {})
    for col, replacements in replace_text.items():
        if col not in df.columns:
            print(f"  WARNING: Column '{col}' not found for replace_text")
            continue
        
        def do_replace(val):
            if pd.isna(val):
                return ''
            val_str = str(val)
            for old_text, new_text in replacements.items():
                val_str = val_str.replace(old_text, new_text)
            return val_str
        
        df[col] = df[col].apply(do_replace)
        print(f"  Replaced text in '{col}': {replacements}")
    
    # 7. Map value from JSON file
    map_value = transformations.get('map_value', {})
    for col, settings in map_value.items():
        if col not in df.columns:
            print(f"  WARNING: Column '{col}' not found for map_value")
            continue
        
        mapping_file = settings.get('mapping_file')
        value_field = settings.get('value_field', 'value')
        match_type = settings.get('match_type', 'ends_with')
        delimiter = settings.get('delimiter', ';')
        
        if not os.path.exists(mapping_file):
            print(f"  ERROR: Mapping file not found: {mapping_file}")
            continue
        
        with open(mapping_file, 'r') as f:
            mapping_data = json.load(f)
        
        # Function to map a single value
        def map_single(input_str):
            input_str = input_str.strip()
            if input_str == '':
                return ''
            
            for entry in mapping_data:
                full_value = entry.get(value_field, '')
                
                if match_type == 'ends_with':
                    if full_value.endswith(f'- {input_str}') or full_value.endswith(f'-{input_str}'):
                        return full_value
                elif match_type == 'exact':
                    if full_value == input_str:
                        return full_value
                elif match_type == 'contains':
                    if input_str in full_value:
                        return full_value
            
            return input_str  # Return original if no match
        
        # Function to handle multiple values
        def map_val(input_val):
            if pd.isna(input_val) or str(input_val).strip() == '':
                return ''
            
            input_str = str(input_val).strip()
            
            # Split by delimiter, map each, rejoin
            parts = input_str.split(delimiter)
            mapped_parts = [map_single(p) for p in parts]
            return delimiter.join(mapped_parts)
        
        df[col] = df[col].apply(map_val)
        print(f"  Mapped values in '{col}' using {mapping_file}")
    
    # 8. Replace value (simple find/replace with delimiter support)
    replace_value = transformations.get('replace_value', {})
    for col, settings in replace_value.items():
        if col not in df.columns:
            print(f"  WARNING: Column '{col}' not found for replace_value")
            continue
        
        replacements = settings.get('mappings', settings)  # Support both formats
        delimiter = settings.get('delimiter', ';') if isinstance(settings, dict) and 'mappings' in settings else ';'
        
        # If settings is just the mappings dict directly
        if not isinstance(settings, dict) or 'mappings' not in settings:
            replacements = settings
        else:
            replacements = settings.get('mappings', {})
            delimiter = settings.get('delimiter', ';')
        
        def replace_val(input_val):
            if pd.isna(input_val) or str(input_val).strip() == '':
                return ''
            
            input_str = str(input_val).strip()
            parts = input_str.split(delimiter)
            replaced_parts = []
            
            for p in parts:
                p_stripped = p.strip()
                p_lower = p_stripped.lower()
                # Check for match (case-insensitive)
                matched = False
                for old_val, new_val in replacements.items():
                    if p_lower == old_val.lower():
                        replaced_parts.append(new_val)
                        matched = True
                        break
                if not matched:
                    replaced_parts.append(p_stripped)
            
            return delimiter.join(replaced_parts)
        
        df[col] = df[col].apply(replace_val)
        print(f"  Replaced values in '{col}'")
    
    # 9. Join column from another file
    join_column = transformations.get('join_column', {})
    for new_col, settings in join_column.items():
        from_file = settings.get('from_file')
        match_column = settings.get('match_column')  # Column in current df
        match_to = settings.get('match_to')  # Column in other file to match against
        pull_column = settings.get('pull_column')  # Column in other file to pull
        
        if not os.path.exists(from_file):
            print(f"  ERROR: Join file not found: {from_file}")
            continue
        
        if match_column not in df.columns:
            print(f"  ERROR: Match column '{match_column}' not found in current file")
            continue
        
        # Read the other file
        other_df = pd.read_csv(from_file, dtype=str)
        
        if match_to not in other_df.columns:
            print(f"  ERROR: Match to column '{match_to}' not found in {from_file}")
            continue
        
        if pull_column not in other_df.columns:
            print(f"  ERROR: Pull column '{pull_column}' not found in {from_file}")
            continue
        
        # Build lookup map: match_to -> pull_column
        join_map = {}
        for _, row in other_df.iterrows():
            key = row[match_to]
            val = row[pull_column]
            if pd.notna(key) and pd.notna(val):
                join_map[str(key).strip()] = str(val).strip()
        
        # Apply join
        def do_join(input_val):
            if pd.isna(input_val) or str(input_val).strip() == '':
                return ''
            return join_map.get(str(input_val).strip(), '')
        
        df[new_col] = df[match_column].apply(do_join)
        matched = (df[new_col] != '').sum()
        print(f"  Joined '{pull_column}' from {from_file} as '{new_col}' ({matched} matched)")
    
    # 10. Picklist overflow (if value in list -> picklist field, else -> overflow text field)
    picklist_overflow = transformations.get('picklist_overflow', {})
    for picklist_col, settings in picklist_overflow.items():
        source_column = settings.get('source_column')
        overflow_column = settings.get('overflow_column')
        valid_values = settings.get('valid_values', [])
        other_value = settings.get('other_value', 'Other')
        
        if source_column not in df.columns:
            print(f"  WARNING: Source column '{source_column}' not found for picklist_overflow")
            continue
        
        valid_values_lower = [v.lower() for v in valid_values]
        
        def assign_picklist(val):
            if pd.isna(val) or str(val).strip() == '':
                return ''
            if str(val).strip().lower() in valid_values_lower:
                idx = valid_values_lower.index(str(val).strip().lower())
                return valid_values[idx]
            return other_value
        
        def assign_overflow(val):
            if pd.isna(val) or str(val).strip() == '':
                return ''
            if str(val).strip().lower() in valid_values_lower:
                return ''
            return str(val).strip()
        
        df[picklist_col] = df[source_column].apply(assign_picklist)
        df[overflow_column] = df[source_column].apply(assign_overflow)
        print(f"  Picklist overflow '{source_column}' → '{picklist_col}' (overflow: '{overflow_column}')")
    
    return df


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
    
    # Apply transformations if defined
    transformations = config.get('transformations', {})
    if transformations:
        print(f"\nApplying transformations...")
        if not clean_df.empty:
            clean_df = apply_transformations(clean_df, transformations)
        if not incomplete_df.empty:
            incomplete_df = apply_transformations(incomplete_df, transformations)
    
    # Generate External ID column
    if not clean_df.empty:
        if external_id_prefix:
            clean_df['External_ID__c'] = external_id_prefix + '-' + clean_df[id_column].astype(str)
        else:
            clean_df['External_ID__c'] = clean_df[id_column].astype(str)
        # Move External_ID__c to first column
        cols = ['External_ID__c'] + [c for c in clean_df.columns if c != 'External_ID__c']
        clean_df = clean_df[cols]
    
    if not incomplete_df.empty:
        if external_id_prefix:
            incomplete_df['External_ID__c'] = external_id_prefix + '-' + incomplete_df[id_column].astype(str)
        else:
            incomplete_df['External_ID__c'] = incomplete_df[id_column].astype(str)
        # Move External_ID__c to first column (before _missing_from)
        cols = ['External_ID__c'] + [c for c in incomplete_df.columns if c not in ['External_ID__c', '_missing_from']] + ['_missing_from']
        incomplete_df = incomplete_df[cols]
    
    # Generate additional external IDs if defined
    additional_external_ids = config.get('additional_external_ids', {})
    if additional_external_ids:
        for field_name, prefix in additional_external_ids.items():
            if not clean_df.empty:
                if prefix:
                    clean_df[field_name] = prefix + '-' + clean_df[id_column].astype(str)
                else:
                    clean_df[field_name] = clean_df[id_column].astype(str)
            if not incomplete_df.empty:
                if prefix:
                    incomplete_df[field_name] = prefix + '-' + incomplete_df[id_column].astype(str)
                else:
                    incomplete_df[field_name] = incomplete_df[id_column].astype(str)
        print(f"Generated additional external IDs: {list(additional_external_ids.keys())}")
    
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
