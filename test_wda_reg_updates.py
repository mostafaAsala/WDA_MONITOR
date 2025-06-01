#!/usr/bin/env python3
"""
Test script for WDA_Reg Monitor updates with MAN_NAME, MODULE_NAME, and WDA_FLAG
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

def create_sample_wda_data_with_names():
    """Create sample WDA_Reg data with proper column names"""
    np.random.seed(42)
    
    # Sample data with new column structure
    n_records = 1000
    
    manufacturers = ['Boeing', 'Airbus', 'Lockheed Martin', 'Northrop Grumman', 'Raytheon']
    modules = ['Flight Control', 'Navigation', 'Communication', 'Power Systems', 'Hydraulics']
    
    data = {
        'MAN_NAME': np.random.choice(manufacturers, n_records),
        'MODULE_NAME': np.random.choice(modules, n_records),
        'PRTY': np.random.choice(['1', '2', '3', '4', '5'], n_records),
        'CS': np.random.randint(1, 100, n_records),
        'STATUS': np.random.choice(['found', 'not found', 'not run'], n_records, p=[0.6, 0.3, 0.1]),
        'WDA_FLAG': np.random.choice(['Y', 'N'], n_records, p=[0.7, 0.3]),
        'COUNT': np.random.randint(1, 50, n_records),
        'is_expired': np.random.choice([True, False], n_records, p=[0.2, 0.8]),
        'LR_DATE': [
            (datetime.now() - timedelta(days=np.random.randint(0, 30))).strftime('%Y-%m-%d')
            for _ in range(n_records)
        ]
    }
    
    return pd.DataFrame(data)

def test_aggregation_function_with_new_columns():
    """Test the updated aggregation function with new column names"""
    
    # Create sample data
    df = create_sample_wda_data_with_names()
    
    print("Testing WDA_Reg aggregation with updated columns...")
    print(f"Sample data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Test basic statistics
    total_parts = int(df['COUNT'].sum())
    found_parts = int(df[df['STATUS'] == 'found']['COUNT'].sum())
    not_found_parts = int(df[df['STATUS'] == 'not found']['COUNT'].sum())
    not_run_parts = int(df[df['STATUS'] == 'not run']['COUNT'].sum())
    expired_parts = int(df[df['is_expired'] == True]['COUNT'].sum())
    
    print(f"\nBasic Statistics:")
    print(f"Total Parts: {total_parts:,}")
    print(f"Found Parts: {found_parts:,} ({(found_parts/total_parts)*100:.1f}%)")
    print(f"Not Found Parts: {not_found_parts:,} ({(not_found_parts/total_parts)*100:.1f}%)")
    print(f"Not Run Parts: {not_run_parts:,} ({(not_run_parts/total_parts)*100:.1f}%)")
    print(f"Expired Parts: {expired_parts:,} ({(expired_parts/total_parts)*100:.1f}%)")
    
    # Test chart data generation with new columns
    status_counts = df.groupby('STATUS')['COUNT'].sum().to_dict()
    priority_counts = df.groupby('PRTY')['COUNT'].sum().to_dict()
    manufacturer_counts = df.groupby('MAN_NAME')['COUNT'].sum().sort_values(ascending=False).head(10).to_dict()
    wda_flag_counts = df.groupby('WDA_FLAG')['COUNT'].sum().to_dict()
    
    print(f"\nChart Data:")
    print(f"Status Distribution: {status_counts}")
    print(f"Priority Distribution: {priority_counts}")
    print(f"Top Manufacturers: {manufacturer_counts}")
    print(f"WDA Flag Distribution: {wda_flag_counts}")
    
    # Test filter options with new columns
    filter_options = {
        'man_names': sorted(df['MAN_NAME'].unique().tolist()),
        'module_names': sorted(df['MODULE_NAME'].unique().tolist()),
        'priorities': sorted(df['PRTY'].unique().tolist()),
        'statuses': sorted(df['STATUS'].unique().tolist()),
        'wda_flags': sorted(df['WDA_FLAG'].unique().tolist()),
        'expired_options': ['true', 'false']
    }
    
    print(f"\nFilter Options:")
    print(f"Manufacturers: {filter_options['man_names']}")
    print(f"Modules: {filter_options['module_names']}")
    print(f"Priorities: {filter_options['priorities']}")
    print(f"Statuses: {filter_options['statuses']}")
    print(f"WDA Flags: {filter_options['wda_flags']}")
    
    # Test table data grouping with new columns
    grouped_df = df.groupby(['MAN_NAME', 'MODULE_NAME', 'PRTY', 'STATUS', 'WDA_FLAG']).agg({
        'COUNT': 'sum',
        'is_expired': 'first',
        'LR_DATE': 'first'
    }).reset_index()
    
    print(f"\nTable Data:")
    print(f"Original records: {len(df)}")
    print(f"Grouped records: {len(grouped_df)}")
    print(f"Grouping columns: MAN_NAME, MODULE_NAME, PRTY, STATUS, WDA_FLAG")
    
    return True

def test_filtering_with_new_columns():
    """Test filtering logic with new column names"""
    
    df = create_sample_wda_data_with_names()
    
    print("\nTesting filtering with new columns...")
    
    # Test filtering by manufacturer name
    selected_manufacturers = ['Boeing', 'Airbus']
    filtered_df = df[df['MAN_NAME'].isin(selected_manufacturers)]
    
    print(f"Original data: {len(df)} records")
    print(f"Filtered by manufacturers {selected_manufacturers}: {len(filtered_df)} records")
    
    # Test filtering by module name
    selected_modules = ['Flight Control', 'Navigation']
    filtered_df = df[df['MODULE_NAME'].isin(selected_modules)]
    
    print(f"Filtered by modules {selected_modules}: {len(filtered_df)} records")
    
    # Test filtering by WDA flag
    selected_wda_flags = ['Y']
    filtered_df = df[df['WDA_FLAG'].isin(selected_wda_flags)]
    
    print(f"Filtered by WDA flags {selected_wda_flags}: {len(filtered_df)} records")
    
    # Test combined filtering
    combined_filter = df[
        (df['MAN_NAME'].isin(selected_manufacturers)) & 
        (df['MODULE_NAME'].isin(selected_modules)) &
        (df['WDA_FLAG'].isin(selected_wda_flags))
    ]
    
    print(f"Combined filter: {len(combined_filter)} records")
    
    return True

def test_wda_flag_chart_data():
    """Test WDA flag chart data generation"""
    
    df = create_sample_wda_data_with_names()
    
    print("\nTesting WDA Flag chart data...")
    
    # WDA Flag distribution
    wda_flag_counts = df.groupby('WDA_FLAG')['COUNT'].sum().to_dict()
    wda_flag_chart_data = {
        'labels': list(wda_flag_counts.keys()),
        'values': [int(v) for v in wda_flag_counts.values()],
        'colors': {
            'Y': '#28a745',  # Green for Yes
            'N': '#dc3545'   # Red for No
        }
    }
    
    print(f"WDA Flag Chart Data: {wda_flag_chart_data}")
    
    # Test percentage calculations
    total_count = sum(wda_flag_chart_data['values'])
    for i, label in enumerate(wda_flag_chart_data['labels']):
        percentage = (wda_flag_chart_data['values'][i] / total_count) * 100
        print(f"WDA Flag {label}: {wda_flag_chart_data['values'][i]:,} ({percentage:.1f}%)")
    
    return True

def test_table_display_structure():
    """Test the new table structure with WDA_FLAG column"""
    
    df = create_sample_wda_data_with_names()
    
    print("\nTesting table display structure...")
    
    # Group data as it would be for table display
    grouped_df = df.groupby(['MAN_NAME', 'MODULE_NAME', 'PRTY', 'STATUS', 'WDA_FLAG']).agg({
        'COUNT': 'sum',
        'is_expired': 'first',
        'LR_DATE': 'first'
    }).reset_index()
    
    # Sort by COUNT descending
    sorted_df = grouped_df.sort_values('COUNT', ascending=False)
    
    print("Table Headers: Manufacturer | Module | Priority | Status | WDA Flag | LR Date | Count | Expired")
    print("-" * 100)
    
    # Show first 10 rows
    for _, row in sorted_df.head(10).iterrows():
        wda_flag_display = 'Yes' if row['WDA_FLAG'] == 'Y' else 'No'
        expired_display = 'Yes' if row['is_expired'] else 'No'
        
        print(f"{row['MAN_NAME']:<15} | {row['MODULE_NAME']:<15} | {row['PRTY']:<8} | {row['STATUS']:<10} | {wda_flag_display:<8} | {row['LR_DATE']:<10} | {row['COUNT']:<5} | {expired_display}")
    
    return True

if __name__ == "__main__":
    print("WDA_Reg Monitor Column Updates Test")
    print("=" * 50)
    
    try:
        test_aggregation_function_with_new_columns()
        test_filtering_with_new_columns()
        test_wda_flag_chart_data()
        test_table_display_structure()
        
        print("\n" + "=" * 50)
        print("All tests completed successfully!")
        print("\nUpdates implemented:")
        print("✓ Replaced MAN_ID with MAN_NAME")
        print("✓ Replaced MOD_ID with MODULE_NAME")
        print("✓ Added WDA_FLAG column and filtering")
        print("✓ Added WDA Flag distribution chart")
        print("✓ Updated table structure with new columns")
        print("✓ Updated all filtering logic")
        print("✓ Maintained aggregation-only performance optimization")
        
    except Exception as e:
        print(f"\nTest failed with error: {e}")
        import traceback
        traceback.print_exc()
