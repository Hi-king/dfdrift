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

# Method 1: Using environment variables (recommended)
# Set SLACK_BOT_TOKEN and SLACK_CHANNEL environment variables
# export SLACK_BOT_TOKEN="xoxb-your-bot-token-here"
# export SLACK_CHANNEL="#data-alerts"

def example_with_env_vars():
    """Example using SLACK_BOT_TOKEN and SLACK_CHANNEL environment variables"""
    
    # Configure with Slack alerts using environment variables
    storage = dfdrift.LocalFileStorage("./slack_schemas")
    slack_alerter = dfdrift.SlackAlerter()  # token and channel from env vars
    
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
        'age2': [28, 32, 27]
    })
    validator.validate(df2)
    print("Second validation complete - Slack alert sent!")


# Method 2: Using partial environment variables
def example_with_env_token_only():
    """Example using SLACK_BOT_TOKEN env var with channel argument"""
    
    # Set SLACK_BOT_TOKEN environment variable, specify channel in code
    # export SLACK_BOT_TOKEN="xoxb-your-bot-token-here"
    
    # Configure with Slack alerts using token from env, channel from argument
    storage = dfdrift.LocalFileStorage("./slack_schemas")
    slack_alerter = dfdrift.SlackAlerter(channel="#general")  # token from env
    
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
    
    # Configure global validation with Slack using environment variables
    # export SLACK_BOT_TOKEN="xoxb-your-bot-token-here"
    # export SLACK_CHANNEL="#dataframe-monitoring"
    
    storage = dfdrift.LocalFileStorage("./pandas_slack_schemas")
    slack_alerter = dfdrift.SlackAlerter()  # token and channel from env vars
    
    pd_drift.configure_validation(storage=storage, alerter=slack_alerter)
    
    # Now all DataFrame creations will be monitored
    df = pd_drift.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=3),
        'value': [1.0, 2.0, 3.0],
        'status': ['OK', 'OK', 'ERROR']
    })
    
    print("DataFrame created and monitored automatically!")


# Method 4: Using direct arguments (not recommended for production)
def example_with_direct_arguments():
    """Example passing both token and channel directly (for testing)"""
    
    # Configure with Slack alerts using direct arguments
    storage = dfdrift.LocalFileStorage("./direct_slack_schemas")
    slack_alerter = dfdrift.SlackAlerter(
        channel="#test-channel",
        token="xoxb-your-bot-token-here"  # Should be from secure storage
    )
    
    validator = dfdrift.DfValidator(storage=storage, alerter=slack_alerter)
    
    # Create test DataFrame
    df = pd.DataFrame({
        'test_col': [1, 2, 3],
        'status': ['pass', 'fail', 'pass']
    })
    validator.validate(df)
    print("Direct arguments example completed")


if __name__ == "__main__":
    print("Slack notification examples for dfdrift")
    print("Make sure to set SLACK_BOT_TOKEN and SLACK_CHANNEL environment variables")
    
    # Check if token and channel are available
    if not os.getenv("SLACK_BOT_TOKEN"):
        print("Warning: SLACK_BOT_TOKEN not set. Set it before running examples:")
        print("export SLACK_BOT_TOKEN='xoxb-your-bot-token-here'")
        print("export SLACK_CHANNEL='#data-alerts'")
        exit(1)
    
    print("\n1. Running example with environment variables...")
    example_with_env_vars()
    
    print("\n2. Running example with partial environment variables...")
    example_with_env_token_only()
    
    print("\n3. Running example with pandas wrapper...")
    example_with_pandas_wrapper()
    
    print("\n4. Running example with direct arguments...")
    example_with_direct_arguments()
    
    print("\nAll examples completed! Check your Slack channel for notifications.")