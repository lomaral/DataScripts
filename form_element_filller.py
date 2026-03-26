"""
Form Element Filler for Salesforce Data Migration
==================================================
Fills form element templates with legacy data values.

Usage:
    python form_element_filler.py <form_name>
    python form_element_filler.py ssvf_renewal
    python form_element_filler.py --all

Inputs:
    - templates/ folder with SF export templates
    - mappings/ folder with legacy-to-SF field mappings
    - output/ folder with merged legacy data

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


def format_value(value, column_name: str):
    """Format value based on column type."""
    if pd.isna(value) or str(value).strip() == '':
        return ''
    
    val_str = str(value).strip()
    
    # Complex values need [] wrapper
    if column_name == 'EGMS_HF_Complex_Value__c':
        # If already JSON array, leave it
        if val_str.startswith('['):
            return val_str
        # Wrap single value in array
        return f'["{val_str}"]'
    
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
        for row in data:
            if isinstance(row, dict):
                # Check if this row matches by elementTemplateKey containing table_row
                element_key = row.get('elementTemplateKey', '')
                if table_row in element_key:
                    row[table_column] = str(new_value).strip()
                    break
        
        return json.dumps(data)
    
    except json.JSONDecodeError:
        print(f"    WARNING: Could not parse complex value as JSON")
        return complex_value


def process_form(form_name: str, config: dict, output_dir: str = "application_form"):
    """Process a single form: fill template with data."""
    print(f"\n{'='*60}")
    print(f"Processing: {form_name.upper()}")
    print(f"{'='*60}")
    
    template_path = config['template']
    mapping_path = config['mapping']
    data_path = config['data_file']
    id_column = config.get('id_column', 'External_ID__c')
    
    # Check files exist
    if not os.path.exists(template_path):
        print(f"ERROR: Template not found: {template_path}")
        return
    if not os.path.exists(mapping_path):
        print(f"ERROR: Mapping not found: {mapping_path}")
        return
    if not os.path.exists(data_path):
        print(f"ERROR: Data file not found: {data_path}")
        return
    
    # Load files
    template_df = pd.read_csv(template_path, dtype=str)
    mapping_df = pd.read_csv(mapping_path, dtype=str)
    data_df = pd.read_csv(data_path, dtype=str)
    
    print(f"Template: {len(template_df)} rows")
    print(f"Mapping: {len(mapping_df)} fields")
    print(f"Data: {len(data_df)} records")
    
    # Get column names from mapping
    mapping_cols = mapping_df.columns.tolist()
    legacy_field_col = mapping_cols[0]  # A = Legacy Field
    reporting_key_col = mapping_cols[1]  # B = Reporting Key
    element_key_col = mapping_cols[2] if len(mapping_cols) > 2 else None  # C = Element Key
    upsert_key_col = mapping_cols[3] if len(mapping_cols) > 3 else None  # D = Upsert Key
    data_type_col = mapping_cols[4] if len(mapping_cols) > 4 else None  # E = Data Type Value
    table_row_col = mapping_cols[5] if len(mapping_cols) > 5 else None  # F = Table Row
    table_col_col = mapping_cols[6] if len(mapping_cols) > 6 else None  # G = Table Column
    
    print(f"Mapping columns: {mapping_cols}")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each record in data file
    for idx, data_row in data_df.iterrows():
        record_id = data_row.get(id_column, f"record_{idx}")
        print(f"\n  Processing record: {record_id}")
        
        # Copy template
        filled_df = template_df.copy()
        
        # Build mapping lookup: reporting_key -> {legacy_field, table_row, table_column, data_type_value}
        mapping_lookup = {}
        for _, map_row in mapping_df.iterrows():
            rk = map_row[reporting_key_col]
            if pd.notna(rk) and str(rk).strip() != '':
                rk_str = str(rk).strip()
                mapping_lookup[rk_str] = {
                    'legacy_field': str(map_row[legacy_field_col]).strip() if pd.notna(map_row[legacy_field_col]) else '',
                    'table_row': str(map_row[table_row_col]).strip() if table_row_col and pd.notna(map_row[table_row_col]) and str(map_row[table_row_col]).strip() != '' else None,
                    'table_column': str(map_row[table_col_col]).strip() if table_col_col and pd.notna(map_row[table_col_col]) and str(map_row[table_col_col]).strip() != '' else None,
                    'data_type_value': str(map_row[data_type_col]).strip() if data_type_col and pd.notna(map_row[data_type_col]) and str(map_row[data_type_col]).strip() != '' else None
                }
        
        print(f"    Mapping lookup: {len(mapping_lookup)} entries")
        
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
            
            # DEBUG
            print(f"    Template RK: '{rk}'")
            
            # Look up in mapping
            if rk not in mapping_lookup:
                print(f"      SKIP: not in mapping")
                continue
            
            legacy_field = mapping_lookup[rk]['legacy_field']
            table_row = mapping_lookup[rk]['table_row']
            table_column = mapping_lookup[rk]['table_column']
            data_type_value = mapping_lookup[rk]['data_type_value']
            
            print(f"      Legacy field: '{legacy_field}'")
            
            if not legacy_field:
                print(f"      SKIP: no legacy field")
                continue
            
            # Get value from data file
            if legacy_field not in data_row.index:
                print(f"      SKIP: legacy field not in data columns")
                continue
            
            value = data_row[legacy_field]
            if pd.isna(value) or str(value).strip() == '':
                print(f"      SKIP: no value in data")
                continue
            
            print(f"      Value: '{value}'")
            
            # Check if table field (has both table_row and table_column)
            if table_row and table_column:
                # Table field - update complex value JSON
                complex_col = 'EGMS_HF_Complex_Value__c'
                if complex_col in filled_df.columns:
                    current_value = filled_df.at[t_idx, complex_col]
                    new_value = fill_table_value(current_value, table_row, table_column, value)
                    filled_df.at[t_idx, complex_col] = new_value
                    table_count += 1
                    print(f"      INSERTED table value: row='{table_row}', col='{table_column}'")
            else:
                # Use data_type_value from mapping column E if available, otherwise detect
                if data_type_value:
                    value_col = data_type_value
                else:
                    value_col = detect_value_column(t_row)
                
                print(f"      Inserting into: '{value_col}'")
                
                formatted_value = format_value(value, value_col)
                filled_df.at[t_idx, value_col] = formatted_value
                filled_count += 1
                print(f"      INSERTED: '{formatted_value}'")
        
        print(f"    Filled {filled_count} fields, {table_count} table values")
        
        # Save filled template
        safe_id = re.sub(r'[^\w\-]', '_', str(record_id))
        output_path = os.path.join(output_dir, f"{form_name}_{safe_id}.csv")
        filled_df.to_csv(output_path, index=False)
        print(f"    Saved: {output_path}")
    
    print(f"\nCompleted {form_name}: {len(data_df)} files created")


def main():
    if len(sys.argv) < 2:
        print("Usage: python form_element_filler.py <form_name>")
        print("       python form_element_filler.py --all")
        sys.exit(1)
    
    config = load_config()
    
    if sys.argv[1] == '--all':
        for form_name in config.keys():
            process_form(form_name, config[form_name])
    else:
        form_name = sys.argv[1].lower().replace(' ', '_')
        if form_name not in config:
            print(f"ERROR: Form '{form_name}' not found in config")
            print(f"Available forms: {list(config.keys())}")
            sys.exit(1)
        process_form(form_name, config[form_name])


if __name__ == "__main__":
    main()
