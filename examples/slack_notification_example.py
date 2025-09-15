"""
Example: Using SlackAlerter for DataFrame schema drift notifications

This example demonstrates how to configure dfdrift to send schema change
notifications to Slack instead of stderr.

Requirements:
- Install slack support: pip install dfdrift[slack]
- Create a Slack Bot Token from https://api.slack.com/apps
- Set up the bot with chat:write permissions
- Add the bot to your desired channel
"""

import os
import pandas as pd
import dfdrift

# Method 1: Using environment variable (recommended)
# Set SLACK_BOT_TOKEN environment variable
# export SLACK_BOT_TOKEN="xoxb-your-bot-token-here"

def example_with_env_token():
    """Example using SLACK_BOT_TOKEN environment variable"""
    
    # Configure with Slack alerts using environment variable
    storage = dfdrift.LocalFileStorage("./slack_schemas")
    slack_alerter = dfdrift.SlackAlerter(channel="#data-alerts")  # token from env
    
    validator = dfdrift.DfValidator(storage=storage, alerter=slack_alerter)
    
    # First run - creates initial schema
    df1 = pd.DataFrame({
        'user_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    validator.validate(df1)
    print("First validation complete - schema saved")
    
    # Second run - schema change (dtype change)
    df2 = pd.DataFrame({
        'user_id': ['a', 'b', 'c'],  # Changed to string!
        'name': ['David', 'Eve', 'Frank'],
        'age': [28, 32, 27]
    })
    validator.validate(df2)
    print("Second validation complete - Slack alert sent!")


# Method 2: Using token argument
def example_with_token_argument():
    """Example passing token directly (not recommended for production)"""
    
    # Get token from secure storage in production
    token = os.getenv("SLACK_BOT_TOKEN", "xoxb-your-token-here")
    
    # Configure with Slack alerts using token argument
    storage = dfdrift.LocalFileStorage("./slack_schemas")
    slack_alerter = dfdrift.SlackAlerter(channel="#general", token=token)
    
    validator = dfdrift.DfValidator(storage=storage, alerter=slack_alerter)
    
    # Create DataFrame with schema change
    df = pd.DataFrame({
        'product_id': [101, 102, 103],
        'price': [29.99, 39.99, 49.99],
        'category': ['A', 'B', 'C']  # New column added
    })
    validator.validate(df)


# Method 3: Using with pandas module wrapper
def example_with_pandas_wrapper():
    """Example using dfdrift.pandas wrapper with Slack alerts"""
    
    import dfdrift.pandas as pd_drift
    
    # Configure global validation with Slack
    storage = dfdrift.LocalFileStorage("./pandas_slack_schemas")
    slack_alerter = dfdrift.SlackAlerter(channel="#dataframe-monitoring")
    
    pd_drift.configure_validation(storage=storage, alerter=slack_alerter)
    
    # Now all DataFrame creations will be monitored
    df = pd_drift.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=3),
        'value': [1.0, 2.0, 3.0],
        'status': ['OK', 'OK', 'ERROR']
    })
    
    print("DataFrame created and monitored automatically!")


if __name__ == "__main__":
    print("Slack notification examples for dfdrift")
    print("Make sure to set SLACK_BOT_TOKEN environment variable")
    
    # Check if token is available
    if not os.getenv("SLACK_BOT_TOKEN"):
        print("Warning: SLACK_BOT_TOKEN not set. Set it before running examples:")
        print("export SLACK_BOT_TOKEN='xoxb-your-bot-token-here'")
        exit(1)
    
    print("\n1. Running example with environment token...")
    example_with_env_token()
    
    print("\n2. Running example with token argument...")
    example_with_token_argument()
    
    print("\n3. Running example with pandas wrapper...")
    example_with_pandas_wrapper()
    
    print("\nAll examples completed! Check your Slack channel for notifications.")