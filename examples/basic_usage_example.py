"""
Example: Basic usage of dfdrift for DataFrame schema validation

This example demonstrates how dfdrift detects schema changes while
ignoring row count variations, which is expected in real data processing.

Key Features:
- Only column names and data types are validated
- Row count changes are ignored (expected behavior)
- Null count changes are ignored (data-dependent)
- Alerts are triggered only for structural schema changes
"""

import pandas as pd
import dfdrift


def basic_validation_example():
    """Basic example showing schema validation ignoring row count changes"""
    
    print("=== Basic dfdrift Usage Example ===")
    print("Demonstrates that row count changes don't trigger alerts\n")
    
    # Use local storage for this example
    validator = dfdrift.DfValidator()
    
    print("--- Step 1: Initial DataFrame (3 rows) ---")
    # First DataFrame with 3 rows
    df1 = pd.DataFrame({
        'user_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35],
        'active': [True, False, True]
    })
    print(f"Shape: {df1.shape}")
    print(f"Columns: {list(df1.columns)}")
    print(f"Dtypes: {dict(df1.dtypes)}")
    
    validator.validate(df1)
    print("✓ Schema saved (baseline established)\n")
    
    print("--- Step 2: Same schema, different row count (5 rows) ---")
    # Same schema but different number of rows - should NOT trigger alert
    df2 = pd.DataFrame({
        'user_id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'age': [25, 30, 35, 28, 22],
        'active': [True, False, True, True, False]
    })
    print(f"Shape: {df2.shape}")
    print(f"Columns: {list(df2.columns)}")
    print(f"Dtypes: {dict(df2.dtypes)}")
    
    validator.validate(df2)
    print("✓ No alert - row count changes are ignored\n")
    
    print("--- Step 3: Schema change - new column (6 rows) ---")
    # Add a new column - should trigger alert
    df3 = pd.DataFrame({
        'user_id': [1, 2, 3, 4, 5, 6],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank'],
        'age': [25, 30, 35, 28, 22, 45],
        'active': [True, False, True, True, False, True],
        'department': ['IT', 'HR', 'IT', 'Sales', 'Marketing', 'IT']  # New column!
    })
    print(f"Shape: {df3.shape}")
    print(f"Columns: {list(df3.columns)}")
    print(f"Dtypes: {dict(df3.dtypes)}")
    
    validator.validate(df3)
    print("⚠️  Alert triggered - new column detected\n")
    
    print("--- Step 4: Schema change - dtype change (4 rows) ---")
    # Change data type - should trigger alert
    df4 = pd.DataFrame({
        'user_id': ['U001', 'U002', 'U003', 'U004'],  # Changed to string!
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'age': [25, 30, 35, 28],
        'active': [True, False, True, True],
        'department': ['IT', 'HR', 'IT', 'Sales']
    })
    print(f"Shape: {df4.shape}")
    print(f"Columns: {list(df4.columns)}")
    print(f"Dtypes: {dict(df4.dtypes)}")
    
    validator.validate(df4)
    print("⚠️  Alert triggered - user_id dtype changed from int64 to object\n")
    
    print("--- Step 5: Back to larger dataset, same schema (100 rows) ---")
    # Much larger dataset but same schema - should NOT trigger alert
    import numpy as np
    
    df5 = pd.DataFrame({
        'user_id': [f'U{i:03d}' for i in range(1, 101)],  # 100 rows
        'name': [f'User{i}' for i in range(1, 101)],
        'age': np.random.randint(18, 70, 100),
        'active': np.random.choice([True, False], 100),
        'department': np.random.choice(['IT', 'HR', 'Sales', 'Marketing'], 100)
    })
    print(f"Shape: {df5.shape}")
    print(f"Columns: {list(df5.columns)}")
    print(f"Dtypes: {dict(df5.dtypes)}")
    
    validator.validate(df5)
    print("✓ No alert - same schema, row count scaling up is normal\n")


def demonstrate_null_handling():
    """Demonstrate that null count changes don't trigger alerts"""
    
    print("=== Null Count Handling Example ===")
    print("Shows that changing null counts doesn't trigger schema alerts\n")
    
    # Use different storage location to avoid interference
    storage = dfdrift.LocalFileStorage(".dfdrift_null_example")
    validator = dfdrift.DfValidator(storage=storage)
    
    print("--- Dataset with no nulls ---")
    df_no_nulls = pd.DataFrame({
        'score': [85, 92, 78, 88, 95],
        'grade': ['A', 'A+', 'B', 'A-', 'A+']
    })
    print(f"Null counts: {df_no_nulls.isnull().sum().to_dict()}")
    
    validator.validate(df_no_nulls)
    print("✓ Baseline established\n")
    
    print("--- Same schema with some nulls ---")
    df_with_nulls = pd.DataFrame({
        'score': [85, None, 78, 88, None, 92],
        'grade': ['A', 'A+', None, 'A-', 'A+', 'A']
    })
    print(f"Null counts: {df_with_nulls.isnull().sum().to_dict()}")
    
    validator.validate(df_with_nulls)
    print("✓ No alert - null count changes are ignored\n")


if __name__ == "__main__":
    print("DataFrame Schema Drift Detection - Basic Usage Examples")
    print("=" * 60)
    print()
    
    basic_validation_example()
    demonstrate_null_handling()
    
    print("=" * 60)
    print("Summary:")
    print("✓ Row count changes are ignored (expected)")
    print("✓ Null count changes are ignored (data-dependent)")
    print("⚠️  Column additions/removals trigger alerts")
    print("⚠️  Data type changes trigger alerts")
    print("\nThis allows dfdrift to focus on structural schema changes")
    print("while being flexible about data volume and quality variations.")