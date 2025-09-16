#!/usr/bin/env python3
"""
Simple Schema Drift Detection Example

This example demonstrates the basic usage of dfdrift for detecting schema changes.

How to use:
1. Run this script the first time: python 1_simple_alert.py
   - Creates a baseline schema
   - No alert (first run)

2. Run this script the second time: python 1_simple_alert.py  
   - Detects schema change (new column added)
   - Triggers alert to stderr

3. Run this script the third time: python 1_simple_alert.py
   - Detects another schema change (column type changed)  
   - Triggers alert to stderr

Each run simulates a different data pipeline execution with schema evolution.
"""

import pandas as pd
import dfdrift

def load_data():
    """
    Simulate loading data with time-based schema changes.
    
    Uses current timestamp as column name to guarantee schema drift.
    """
    
    from datetime import datetime
    
    # Use current time as column name - guarantees schema change each run
    timestamp_col = f"timestamp_{datetime.now().strftime('%H%M%S')}"
    
    print(f"=== Data Pipeline with Time-based Schema ===")
    print(f"Dynamic column: {timestamp_col}")
    
    # Create DataFrame with timestamp-based column
    df = pd.DataFrame({
        'user_id': [1001, 1002, 1003, 1004],
        'name': ['Alice', 'Bob', 'Charlie', 'Diana'], 
        'age': [25, 30, 35, 28],
        timestamp_col: [1, 2, 3, 4]  # This column name changes every run!
    })
    
    print(f"Columns: {list(df.columns)}")
    print(f"Types: {dict(df.dtypes)}")
    
    return df


def main():
    print("🔍 DataFrame Schema Drift Detection - Simple Example")
    print("=" * 60)
    
    # Create validator with default settings (local file storage, stderr alerts)
    validator = dfdrift.DfValidator()
    
    # Load data (simulates real data pipeline)
    df = load_data()
    
    print(f"\nDataFrame shape: {df.shape}")
    print("\n--- Running Schema Validation ---")
    
    # This is where the magic happens!
    # The same line of code, but different data each time
    validator.validate(df)  # <- This line gets tracked by file:line
    
    print("\n✅ Validation complete!")
    print("\nNote: Run this script multiple times to see schema drift detection in action.")
    print("💡 Check the '.dfdrift_schemas' folder to see stored schemas.")


if __name__ == "__main__":
    main()