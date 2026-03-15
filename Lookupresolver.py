# “””
Lookup Resolver for Salesforce Data Migration

Adds lookup columns to child objects so Data Loader can link them to parent records.

Usage:
python lookup_resolver.py <object_name>
python lookup_resolver.py individual_application
python lookup_resolver.py –all

Requires:
- Parent objects must be processed first (merged files must exist)
- Config must define lookups with source_column and parent_object

Outputs:
- output/<object>_import_ready.csv → Final file for Data Loader
“””

import pandas as pd
import json
import sys
import os

def load_config(config_path: str = “config.json”) -> dict:
“”“Load object configurations from JSON file.”””
if not os.path.exists(config_path):
print(f”ERROR: Config file not found: {config_path}”)
sys.exit(1)

```
with open(config_path, 'r') as f:
    return json.load(f)
```

def build_lookup_map(parent_object: str, parent_key: str, config: dict, output_dir: str = “output”) -> dict:
“””
Build a map from parent_key column to External_ID__c.

```
Returns:
    dict: {parent_key_value: External_ID__c_value}
"""
parent_config = config.get(parent_object)
if not parent_config:
    print(f"  ERROR: Parent object '{parent_object}' not found in config")
    return {}

# Look for parent's merged file
parent_file = os.path.join(output_dir, f"{parent_object}_merged.csv")

if not os.path.exists(parent_file):
    print(f"  ERROR: Parent merged file not found: {parent_file}")
    print(f"         Run merger on '{parent_object}' first")
    return {}

parent_df = pd.read_csv(parent_file, dtype=str)

if parent_key not in parent_df.columns:
    print(f"  ERROR: parent_key '{parent_key}' not found in {parent_file}")
    print(f"         Available columns: {list(parent_df.columns)}")
    return {}

if 'External_ID__c' not in parent_df.columns:
    print(f"  ERROR: External_ID__c not found in {parent_file}")
    return {}

# Build map: parent_key -> External_ID__c
lookup_map = {}
for _, row in parent_df.iterrows():
    key_val = row[parent_key]
    ext_id = row['External_ID__c']
    if pd.notna(key_val) and pd.notna(ext_id):
        lookup_map[str(key_val).strip()] = str(ext_id).strip()

print(f"  Built lookup map for '{parent_object}' using '{parent_key}': {len(lookup_map)} records")

return lookup_map
```

def resolve_lookups(object_name: str, config: dict, output_dir: str = “output”):
“”“Resolve all lookups for an object and create import-ready file.”””
print(f”\n{’=’*60}”)
print(f”Resolving lookups: {object_name.upper()}”)
print(f”{’=’*60}”)

```
obj_config = config.get(object_name)
if not obj_config:
    print(f"ERROR: Object '{object_name}' not found in config")
    return

lookups = obj_config.get('lookups', {})

if not lookups:
    print(f"No lookups defined for '{object_name}'. Skipping.")
    return

# Read the merged file
merged_file = os.path.join(output_dir, f"{object_name}_merged.csv")
if not os.path.exists(merged_file):
    print(f"ERROR: Merged file not found: {merged_file}")
    print(f"       Run merger on '{object_name}' first")
    return

df = pd.read_csv(merged_file, dtype=str)
print(f"Loaded {len(df)} records from {merged_file}")
print(f"Lookups to resolve: {list(lookups.keys())}")

# Track unmatched records
all_unmatched = []

# Process each lookup
for lookup_field, lookup_config in lookups.items():
    source_column = lookup_config['source_column']
    parent_object = lookup_config['parent_object']
    parent_key = lookup_config.get('parent_key', source_column)  # Default to same as source_column
    
    print(f"\nProcessing: {lookup_field}")
    print(f"  Source column (child): {source_column}")
    print(f"  Parent object: {parent_object}")
    print(f"  Parent key: {parent_key}")
    
    if source_column not in df.columns:
        print(f"  ERROR: Source column '{source_column}' not found in merged file")
        continue
    
    # Build lookup map from parent
    lookup_map = build_lookup_map(parent_object, parent_key, config, output_dir)
    
    if not lookup_map:
        print(f"  ERROR: Could not build lookup map. Skipping this lookup.")
        continue
    
    # Resolve lookup for each row
    def resolve(val):
        if pd.isna(val) or str(val).strip() == '':
            return ''
        val_str = str(val).strip()
        return lookup_map.get(val_str, '')
    
    df[lookup_field] = df[source_column].apply(resolve)
    
    # Track unmatched
    unmatched_mask = (df[source_column].notna()) & (df[source_column].str.strip() != '') & (df[lookup_field] == '')
    unmatched_count = unmatched_mask.sum()
    
    if unmatched_count > 0:
        print(f"  ⚠ {unmatched_count} records could not be matched")
        unmatched_values = df.loc[unmatched_mask, source_column].unique()
        print(f"    Unmatched values: {list(unmatched_values)[:10]}{'...' if len(unmatched_values) > 10 else ''}")
        
        # Save unmatched for reporting
        for val in unmatched_values:
            all_unmatched.append({
                'lookup_field': lookup_field,
                'source_column': source_column,
                'parent_object': parent_object,
                'unmatched_value': val
            })
    else:
        print(f"  ✓ All records matched")

# Save import-ready file
import_ready_path = os.path.join(output_dir, f"{object_name}_import_ready.csv")
df.to_csv(import_ready_path, index=False)
print(f"\n✓ Import-ready file saved: {import_ready_path}")
print(f"  Records: {len(df)}")
print(f"  Columns: {list(df.columns)}")

# Save unmatched report if any
if all_unmatched:
    unmatched_path = os.path.join(output_dir, f"{object_name}_unmatched_lookups.csv")
    pd.DataFrame(all_unmatched).to_csv(unmatched_path, index=False)
    print(f"✓ Unmatched lookups report saved: {unmatched_path}")
```

def main():
if len(sys.argv) < 2:
print(“Usage: python lookup_resolver.py <object_name>”)
print(”       python lookup_resolver.py –all”)
print(”\nExample: python lookup_resolver.py individual_application”)
sys.exit(1)

```
config = load_config()
target = sys.argv[1].lower()

if target == '--all':
    # Process all objects that have lookups
    for object_name, obj_config in config.items():
        if obj_config.get('lookups'):
            resolve_lookups(object_name, config)
elif target in config:
    resolve_lookups(target, config)
else:
    print(f"ERROR: Object '{target}' not found in config.json")
    print(f"Available objects: {list(config.keys())}")
    sys.exit(1)

print(f"\n{'='*60}")
print("Done!")
print(f"{'='*60}")
```

if **name** == “**main**”:
main()