"""
Form Element Filler for Salesforce Data Migration
==================================================
Fills form element templates with legacy data values.

Usage:
    python form_element_filler.py

Inputs:
    - form_config.json with templates, mappings, name_mapping
    - data file with Id and Name columns

Output:
    - application_form/ folder with filled templates (one per record)
"""

import pandas as pd
import json
import sys
import os
import re
from datetime import datetime


def load_config(config_path: str = "form_config.json") -> dict:
    """Load form configurations from JSON file."""
    if not os.path.exists(config_path):
        print(f"ERROR: Config file not found: {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        return json.load(f)


# Value columns in order of detection priority
VALUE_COLUMNS = [
    'EGMS_HF_Text_Value__c',
    'EGMS_HF_Number_Value__c',
    'EGMS_HF_Complex_Value__c',
    'EGMS_HF_Long_Text_Value__c',
    'EGMS_HF_Date_Value__c',
    'EGMS_HF_DateTime_Value__c',
    'EGMS_HF_Boolean_Value__c',
    'EGMS_HF_Rich_Text_Value__c',
    'EGMS_HF_Short_Text_Area_Value__c'
]


def detect_value_column(template_row: pd.Series) -> str:
    """Detect which value column has dummy data."""
    for col in VALUE_COLUMNS:
        if col in template_row.index:
            val = template_row[col]
            if pd.notna(val) and str(val).strip() != '':
                return col
    return 'EGMS_HF_Text_Value__c'  # Default to text


def format_value(value, column_name: str, delimiter: str = ';'):
    """Format value based on column type."""
    if pd.isna(value) or str(value).strip() == '':
        return ''
    
    val_str = str(value).strip()
    
    # Complex values need [] wrapper with each value quoted
    if column_name == 'EGMS_HF_Complex_Value__c':
        # If already JSON array, leave it
        if val_str.startswith('['):
            return val_str
        # Split by delimiter, wrap each value
        parts = [p.strip() for p in val_str.split(delimiter) if p.strip()]
        quoted_parts = [f'"{p}"' for p in parts]
        return f'[{", ".join(quoted_parts)}]'
    
    # Boolean values
    if column_name == 'EGMS_HF_Boolean_Value__c':
        val_lower = val_str.lower()
        if val_lower in ['true', 'yes', '1']:
            return 'true'
        return 'false'
    
    return val_str


def fill_table_value(complex_value: str, table_row: str, table_column: str, new_value) -> str:
    """Fill a value in a table (JSON array in complex value).
    
    Args:
        complex_value: The JSON string from EGMS_HF_Complex_Value__c
        table_row: Row identifier (e.g., 'ROW_1' or matches elementTemplateKey)
        table_column: Column key to update (e.g., 'AmountOfFunds')
        new_value: Value to insert
    """
    if pd.isna(new_value) or str(new_value).strip() == '':
        return complex_value
    
    try:
        # Parse existing JSON
        if pd.isna(complex_value) or str(complex_value).strip() == '':
            return complex_value
        
        data = json.loads(complex_value)
        
        if not isinstance(data, list):
            return complex_value
        
        # Find the row and update the column
        row_found = False
        for row in data:
            if isinstance(row, dict):
                # Check if this row matches by elementTemplateKey containing table_row
                element_key = row.get('elementTemplateKey', '')
                if table_row in element_key:
                    row[table_column] = str(new_value).strip()
                    row_found = True
                    break
        
        # If row not found, duplicate ROW_1 and create new row
        if not row_found and 'ROW_' in table_row:
            # Find ROW_1 template
            row_1_template = None
            for row in data:
                if isinstance(row, dict):
                    element_key = row.get('elementTemplateKey', '')
                    if 'ROW_1' in element_key:
                        row_1_template = row.copy()
                        break
            
            if row_1_template:
                # Create new row by replacing ROW_1 with target row
                new_row = {}
                for key, val in row_1_template.items():
                    if key == 'elementTemplateKey':
                        new_row[key] = val.replace('ROW_1', table_row)
                    else:
                        new_row[key] = ''  # Clear other values
                
                # Set the value for this column
                new_row[table_column] = str(new_value).strip()
                data.append(new_row)
        
        return json.dumps(data)
    
    except json.JSONDecodeError:
        print(f"    WARNING: Could not parse complex value as JSON")
        return complex_value


def extract_row_suffix(column_name: str) -> tuple:
    """Extract base column name and row number from Name_1, Name_2, etc.
    
    Returns:
        (base_name, row_number) or (column_name, None) if no suffix
    """
    import re
    match = re.match(r'^(.+)_(\d+)$', column_name)
    if match:
        return match.group(1), int(match.group(2))
    return column_name, None


def update_upsert_key(upsert_key: str, new_form_id: str) -> str:
    """Replace the form ID part of the upsert key with new_form_id.
    
    Format: OLD_FORM_ID_ELEMENT_xxxxx -> NEW_FORM_ID_ELEMENT_xxxxx
    """
    if pd.isna(upsert_key) or not upsert_key:
        return upsert_key
    
    upsert_str = str(upsert_key).strip()
    
    # Find _ELEMENT_ and replace everything before it
    if '_ELEMENT_' in upsert_str:
        element_part = upsert_str.split('_ELEMENT_', 1)[1]
        return f"{new_form_id}_ELEMENT_{element_part}"
    
    return upsert_str


def process_all_forms(config: dict, output_dir: str = "application_form"):
    """Process all records, selecting template based on Name column."""
    print(f"\n{'='*60}")
    print(f"Processing Form Elements")
    print(f"{'='*60}")
    
    data_path = config['data_file']
    name_column = config.get('name_column', 'Name')
    id_column = config.get('id_column', 'Id')
    templates = config.get('templates', {})
    
    # Check data file exists
    if not os.path.exists(data_path):
        print(f"ERROR: Data file not found: {data_path}")
        return
    
    # Load data file
    data_df = pd.read_csv(data_path, dtype=str)
    print(f"Data file: {len(data_df)} records")
    print(f"Name column: {name_column}")
    print(f"Id column: {id_column}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Track stats
    processed = 0
    skipped = 0
    all_filled_dfs = []
    
    # Process each record
    for idx, data_row in data_df.iterrows():
        # Get Name and Id
        name_value = data_row.get(name_column, '')
        form_id = data_row.get(id_column, '')
        
        if pd.isna(name_value) or str(name_value).strip() == '':
            print(f"  SKIP row {idx}: No Name")
            skipped += 1
            continue
        
        if pd.isna(form_id) or str(form_id).strip() == '':
            print(f"  SKIP row {idx}: No Id")
            skipped += 1
            continue
        
        name_value = str(name_value).strip()
        form_id = str(form_id).strip()
        
        # Get template config directly from Name
        if name_value not in templates:
            print(f"  SKIP row {idx}: No template for '{name_value}'")
            skipped += 1
            continue
        
        template_config = templates[name_value]
        template_path = template_config['template']
        mapping_path = template_config['mapping']
        template_transformations = template_config.get('transformations', {})
        
        # Check files exist
        if not os.path.exists(template_path):
            print(f"  SKIP row {idx}: Template file not found: {template_path}")
            skipped += 1
            continue
        if not os.path.exists(mapping_path):
            print(f"  SKIP row {idx}: Mapping file not found: {mapping_path}")
            skipped += 1
            continue
        
        print(f"\n  Processing: {form_id} ({name_value})")
        
        # Apply transformations to data row
        transformed_row = data_row.copy()
        
        # replace_text transformation
        replace_text = template_transformations.get('replace_text', {})
        for col, replacements in replace_text.items():
            if col in transformed_row.index:
                val = transformed_row[col]
                if pd.notna(val):
                    val_str = str(val)
                    for old_text, new_text in replacements.items():
                        val_str = val_str.replace(old_text, new_text)
                    transformed_row[col] = val_str
        
        # replace_value transformation
        replace_value = template_transformations.get('replace_value', {})
        for col, mappings in replace_value.items():
            if col in transformed_row.index:
                val = transformed_row[col]
                if pd.notna(val):
                    val_str = str(val).strip()
                    for old_val, new_val in mappings.items():
                        if val_str.lower() == old_val.lower():
                            transformed_row[col] = new_val
                            break
        
        # Use transformed_row instead of data_row for the rest
        data_row = transformed_row
        
        # Load template and mapping
        template_df = pd.read_csv(template_path, dtype=str)
        mapping_df = pd.read_csv(mapping_path, dtype=str)
        
        # Get column names from mapping
        mapping_cols = mapping_df.columns.tolist()
        legacy_field_col = mapping_cols[0]  # A = Legacy Field
        reporting_key_col = mapping_cols[1]  # B = Reporting Key
        element_key_col = mapping_cols[2] if len(mapping_cols) > 2 else None  # C = Element Key
        upsert_key_col = mapping_cols[3] if len(mapping_cols) > 3 else None  # D = Upsert Key
        data_type_col = mapping_cols[4] if len(mapping_cols) > 4 else None  # E = Data Type Value
        table_row_col = mapping_cols[5] if len(mapping_cols) > 5 else None  # F = Table Row
        table_col_col = mapping_cols[6] if len(mapping_cols) > 6 else None  # G = Table Column
        
        # Copy template
        filled_df = template_df.copy()
        
        # Set EGMS_HF_Form__c to the form Id
        filled_df['EGMS_HF_Form__c'] = form_id
        
        # Update ALL upsert keys with the form Id
        if 'EGMS_HF_Element_Upsert_Key__c' in filled_df.columns:
            filled_df['EGMS_HF_Element_Upsert_Key__c'] = filled_df['EGMS_HF_Element_Upsert_Key__c'].apply(
                lambda x: update_upsert_key(x, form_id)
            )
            print(f"    Updated upsert keys with Id: {form_id}")
        
        # Build mapping lookup: reporting_key -> list of entries
        mapping_lookup = {}
        for _, map_row in mapping_df.iterrows():
            rk = map_row[reporting_key_col]
            if pd.notna(rk) and str(rk).strip() != '':
                rk_str = str(rk).strip()
                entry = {
                    'legacy_field': str(map_row[legacy_field_col]).strip() if pd.notna(map_row[legacy_field_col]) else '',
                    'table_row': str(map_row[table_row_col]).strip() if table_row_col and pd.notna(map_row[table_row_col]) and str(map_row[table_row_col]).strip() != '' else None,
                    'table_column': str(map_row[table_col_col]).strip() if table_col_col and pd.notna(map_row[table_col_col]) and str(map_row[table_col_col]).strip() != '' else None,
                    'data_type_value': str(map_row[data_type_col]).strip() if data_type_col and pd.notna(map_row[data_type_col]) and str(map_row[data_type_col]).strip() != '' else None
                }
                if rk_str not in mapping_lookup:
                    mapping_lookup[rk_str] = []
                mapping_lookup[rk_str].append(entry)
        
        # Track filled fields
        filled_count = 0
        table_count = 0
        
        # Loop through each TEMPLATE row
        for t_idx, t_row in filled_df.iterrows():
            # Get reporting key from template
            reporting_key = t_row.get('EGMS_HF_Reporting_Key__c', '')
            if pd.isna(reporting_key) or str(reporting_key).strip() == '':
                continue
            
            rk = str(reporting_key).strip()
            
            # Look up in mapping
            if rk not in mapping_lookup:
                continue
            
            # Loop through ALL entries for this reporting key
            for entry in mapping_lookup[rk]:
                legacy_field = entry['legacy_field']
                table_row = entry['table_row']
                table_column = entry['table_column']
                data_type_value = entry['data_type_value']
                
                if not legacy_field:
                    continue
                
                # Check if table field with dynamic rows (table_column set but no table_row)
                # This handles Name_1, Name_2, etc. from pivoted data
                if table_column and not table_row:
                    # Look for all columns matching pattern: legacy_field_1, legacy_field_2, etc.
                    for col_name in data_row.index:
                        base_name, row_num = extract_row_suffix(col_name)
                        if base_name == legacy_field and row_num is not None:
                            value = data_row[col_name]
                            if pd.isna(value) or str(value).strip() == '':
                                continue
                            
                            # Build table_row from row number
                            dynamic_table_row = f"ROW_{row_num}"
                            
                            # Table field - update complex value JSON
                            complex_col = 'EGMS_HF_Complex_Value__c'
                            if complex_col in filled_df.columns:
                                current_value = filled_df.at[t_idx, complex_col]
                                new_value = fill_table_value(current_value, dynamic_table_row, table_column, value)
                                filled_df.at[t_idx, complex_col] = new_value
                                table_count += 1
                    continue
                
                # Get value from data file
                if legacy_field not in data_row.index:
                    continue
                
                value = data_row[legacy_field]
                if pd.isna(value) or str(value).strip() == '':
                    continue
                
                # Check if table field (has both table_row and table_column)
                if table_row and table_column:
                    # Table field - update complex value JSON
                    complex_col = 'EGMS_HF_Complex_Value__c'
                    if complex_col in filled_df.columns:
                        current_value = filled_df.at[t_idx, complex_col]
                        new_value = fill_table_value(current_value, table_row, table_column, value)
                        filled_df.at[t_idx, complex_col] = new_value
                        table_count += 1
                else:
                    # Use data_type_value from mapping column E if available, otherwise detect
                    if data_type_value:
                        value_col = data_type_value
                    else:
                        value_col = detect_value_column(t_row)
                    
                    formatted_value = format_value(value, value_col)
                    filled_df.at[t_idx, value_col] = formatted_value
                    filled_count += 1
        
        print(f"    Filled {filled_count} fields, {table_count} table values")
        
        # Append to combined output
        all_filled_dfs.append(filled_df)
        processed += 1
    
    # Save all to one file
    if all_filled_dfs:
        combined_df = pd.concat(all_filled_dfs, ignore_index=True)
        
        output_path = os.path.join(output_dir, "form_elements_import.csv")
        combined_df.to_csv(output_path, index=False)
        print(f"\n  Saved combined file: {output_path} ({len(combined_df)} rows)")
    
    print(f"\n{'='*60}")
    print(f"Completed: {processed} processed, {skipped} skipped")
    print(f"{'='*60}")


def main():
    config = load_config()
    process_all_forms(config)


if __name__ == "__main__":
    main()
