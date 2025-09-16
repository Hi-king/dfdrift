"""
Example: Using SlackAlerter for DataFrame schema drift notifications

This example demonstrates how to configure dfdrift to send schema change
notifications to Slack instead of stderr.

Requirements:
- Install slack support: pip install dfdrift[slack]

Two authentication methods are supported:
1. Incoming Webhook (Recommended):
   - Create an Incoming Webhook from https://api.slack.com/apps
   - Simpler setup, no additional permissions needed
   - Set DFDRIFT_SLACK_WEBHOOK_URL environment variable

2. Bot Token (Advanced):
   - Create a Slack Bot Token from https://api.slack.com/apps
   - Set up the bot with chat:write permissions
   - Add the bot to your desired channel
   - Set DFDRIFT_SLACK_BOT_TOKEN and DFDRIFT_SLACK_CHANNEL environment variables
"""

import os
import pandas as pd
import dfdrift

# Method 1: Using Incoming Webhook (recommended)
# Set DFDRIFT_SLACK_WEBHOOK_URL environment variable
# export DFDRIFT_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"

def example_with_webhook():
    """Example using Slack Incoming Webhook (simplest configuration)"""
    
    # Configure with Slack webhook alerts using environment variable
    storage = dfdrift.LocalFileStorage("./webhook_slack_schemas")
    slack_alerter = dfdrift.SlackAlerter()  # webhook URL from DFDRIFT_SLACK_WEBHOOK_URL env var
    
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
        'age2': [28, 32, 27]  # Column name changed
    })
    validator.validate(df2)
    print("Second validation complete - Slack webhook alert sent!")


def example_with_webhook_direct():
    """Example using Slack Incoming Webhook with direct URL"""
    
    # Configure with webhook URL directly (for testing)
    storage = dfdrift.LocalFileStorage("./webhook_direct_schemas")
    slack_alerter = dfdrift.SlackAlerter(
        webhook_url="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
    )
    
    validator = dfdrift.DfValidator(storage=storage, alerter=slack_alerter)
    
    # Create DataFrame with schema change
    df = pd.DataFrame({
        'product_id': [101, 102, 103],
        'price': [29.99, 39.99, 49.99],
        'category': ['A', 'B', 'C'],
        'in_stock': [True, False, True]  # New column
    })
    validator.validate(df)
    print("Webhook direct example completed!")


# Method 2: Using Bot Token (advanced)
# Set DFDRIFT_SLACK_BOT_TOKEN and DFDRIFT_SLACK_CHANNEL environment variables
# export DFDRIFT_SLACK_BOT_TOKEN="xoxb-your-bot-token-here"
# export DFDRIFT_SLACK_CHANNEL="#data-alerts"

def example_with_bot_token_env_vars():
    """Example using DFDRIFT_SLACK_BOT_TOKEN and DFDRIFT_SLACK_CHANNEL environment variables"""
    
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


# Method 3: Using partial bot token environment variables
def example_with_bot_token_env_token_only():
    """Example using SLACK_BOT_TOKEN env var with channel argument"""
    
    # Set DFDRIFT_SLACK_BOT_TOKEN environment variable, specify channel in code
    # export DFDRIFT_SLACK_BOT_TOKEN="xoxb-your-bot-token-here"
    
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


# Method 4: Using with pandas module wrapper
def example_with_pandas_wrapper():
    """Example using dfdrift.pandas wrapper with Slack alerts"""
    
    import dfdrift.pandas as pd_drift
    
    # Configure global validation with Slack using environment variables
    # export DFDRIFT_SLACK_BOT_TOKEN="xoxb-your-bot-token-here"
    # export DFDRIFT_SLACK_CHANNEL="#dataframe-monitoring"
    
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


# Method 5: Using bot token direct arguments (not recommended for production)
def example_with_bot_token_direct_arguments():
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
    print("Two authentication methods supported:")
    print("1. Incoming Webhook (recommended): Set DFDRIFT_SLACK_WEBHOOK_URL")
    print("2. Bot Token (advanced): Set DFDRIFT_SLACK_BOT_TOKEN and DFDRIFT_SLACK_CHANNEL")
    
    # Check if webhook URL or bot token is available
    has_webhook = bool(os.getenv("DFDRIFT_SLACK_WEBHOOK_URL"))
    has_bot_token = bool(os.getenv("DFDRIFT_SLACK_BOT_TOKEN"))
    
    if not has_webhook and not has_bot_token:
        print("\nNo Slack configuration found. Choose one option:")
        print("\nOption 1 (Recommended) - Incoming Webhook:")
        print("export DFDRIFT_SLACK_WEBHOOK_URL='https://hooks.slack.com/services/YOUR/WEBHOOK/URL'")
        print("\nOption 2 (Advanced) - Bot Token:")
        print("export DFDRIFT_SLACK_BOT_TOKEN='xoxb-your-bot-token-here'")
        print("export DFDRIFT_SLACK_CHANNEL='#data-alerts'")
        exit(1)
    
    if has_webhook:
        print("\n1. Running webhook example with environment variable...")
        example_with_webhook()
        
        print("\n2. Running webhook example with direct URL...")
        example_with_webhook_direct()
    
    if has_bot_token:
        print("\n3. Running bot token example with environment variables...")
        example_with_bot_token_env_vars()
        
        print("\n4. Running bot token example with partial environment variables...")
        example_with_bot_token_env_token_only()
        
        print("\n5. Running example with pandas wrapper...")
        example_with_pandas_wrapper()
        
        print("\n6. Running bot token example with direct arguments...")
        example_with_bot_token_direct_arguments()
    
    print("\nAll examples completed! Check your Slack channel for notifications.")