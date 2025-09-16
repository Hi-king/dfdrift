#!/usr/bin/env python3
"""
Pandas Interface Schema Drift Detection Example

This example demonstrates using dfdrift through the pandas-compatible interface.
This approach automatically validates DataFrames when they are created, making
it seamless to integrate into existing pandas workflows.

How to use:
1. Run this script the first time: python 2_pandas_interface.py
   - Creates baseline schemas for each DataFrame creation
   - No alerts (first run)

2. Run this script the second time: python 2_pandas_interface.py
   - Detects schema changes
   - Triggers alerts automatically

The pandas interface tracks DataFrame creation locations automatically.
"""

import dfdrift.pandas as pd  # Use dfdrift's pandas interface

def setup_validation():
    """Configure automatic schema validation"""
    print("🔧 Configuring automatic schema validation...")
    
    # Configure validation with default settings
    # This enables automatic validation for all DataFrame operations
    pd.configure_validation()
    
    print("✅ Auto-validation enabled for all DataFrame operations")


def load_sales_data():
    """
    Simulate loading sales data with time-based dynamic columns.
    """
    
    from datetime import datetime
    
    # Dynamic column based on current time
    batch_col = f"batch_{datetime.now().strftime('%M%S')}"
    
    print(f"\n=== Sales Data Pipeline ===")
    print(f"Processing batch: {batch_col}")
    
    sales_df = pd.DataFrame({
        'product_id': [101, 102, 103, 104],
        'product_name': ['Widget A', 'Widget B', 'Gadget X', 'Gadget Y'],
        'amount': [100, 150, 200, 175],
        'sold_date': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18'],
        batch_col: [1, 1, 1, 1]  # Batch identifier changes each run
    })
    
    print(f"📊 Loaded {len(sales_df)} sales records")
    return sales_df


def analyze_user_behavior():
    """
    Simulate user behavior analysis with changing schema.
    """
    
    print("\n=== User Behavior Analysis ===")
    
    # This creates a different DataFrame at a different location
    # So it will have its own schema tracking
    user_df = pd.DataFrame({
        'user_id': [1, 2, 3, 4, 5],
        'page_views': [10, 25, 5, 30, 15],
        'session_duration': [120, 300, 60, 450, 180],
        'device_type': ['mobile', 'desktop', 'mobile', 'desktop', 'tablet']
    })
    
    print(f"📱 Analyzed {len(user_df)} user sessions")
    return user_df


def create_summary_report():
    """
    Create a summary report using pandas operations.
    """
    
    print("\n=== Creating Summary Report ===")
    
    # Using pandas operations that are automatically tracked
    summary_df = pd.DataFrame.from_dict({
        'metric': ['total_sales', 'avg_order_value', 'unique_customers'],
        'value': [1500.50, 87.25, 42],
        'period': ['Q1_2024', 'Q1_2024', 'Q1_2024']
    })
    
    print(f"📈 Generated summary with {len(summary_df)} metrics")
    return summary_df


def main():
    print("🐼 DataFrame Schema Drift Detection - Pandas Interface Example")
    print("=" * 70)
    
    # Setup automatic validation
    setup_validation()
    
    print("\n🔍 Creating DataFrames (automatically validated)...")
    
    # Each DataFrame creation is automatically validated
    sales_data = load_sales_data()
    user_data = analyze_user_behavior()  
    summary_data = create_summary_report()
    
    print(f"\n📋 Summary:")
    print(f"   Sales DataFrame: {sales_data.shape}")
    print(f"   User DataFrame: {user_data.shape}")
    print(f"   Summary DataFrame: {summary_data.shape}")
    
    print(f"\n✅ All DataFrames processed!")
    print(f"🔧 Schema validation was automatic - no explicit validate() calls needed")
    print(f"\n💡 Run this script again to see schema drift detection!")
    print(f"📂 Check '.dfdrift_schemas/schemas.json' to see stored schemas")
    
    # Show that these are real pandas DataFrames
    print(f"\n🐼 DataFrame types:")
    print(f"   Sales: {type(sales_data)}")
    print(f"   User: {type(user_data)}")
    print(f"   Summary: {type(summary_data)}")


if __name__ == "__main__":
    main()