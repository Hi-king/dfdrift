"""
Example: Complete integration with GCS storage and Slack notifications

This example demonstrates how to configure dfdrift with Google Cloud Storage
for schema persistence and Slack notifications for alerts, using environment
variables for production-ready configuration.

Requirements:
- Install all features: pip install dfdrift[gcs,slack]
- Set up Google Cloud authentication
- Create Slack Bot Token and configure bot permissions
- Set required environment variables

Environment Variables:
- DFDRIFT_GCS_BUCKET: Google Cloud Storage bucket name
- DFDRIFT_GCS_PREFIX: Optional prefix for schema files (defaults to "dfdrift")
- GOOGLE_APPLICATION_CREDENTIALS: Path to service account key file

Slack Configuration (choose one):
Option 1 (Recommended) - Incoming Webhook:
- DFDRIFT_SLACK_WEBHOOK_URL: Slack incoming webhook URL

Option 2 (Advanced) - Bot Token:
- DFDRIFT_SLACK_BOT_TOKEN: Slack bot token (xoxb-...)
- DFDRIFT_SLACK_CHANNEL: Slack channel for notifications (#channel-name)
"""

import datetime
import os
import pandas as pd
import dfdrift


def production_example():
    """Production-ready example using environment variables"""
    
    print("=== Production Integration Example ===")
    print("Using GCS for storage and Slack for notifications")
    
    # Check required environment variables
    gcs_bucket = os.getenv("DFDRIFT_GCS_BUCKET")
    has_webhook = bool(os.getenv("DFDRIFT_SLACK_WEBHOOK_URL"))
    has_bot_token = bool(os.getenv("DFDRIFT_SLACK_BOT_TOKEN") and os.getenv("DFDRIFT_SLACK_CHANNEL"))
    
    if not gcs_bucket:
        print("Missing required environment variable: DFDRIFT_GCS_BUCKET")
        print("export DFDRIFT_GCS_BUCKET='your-bucket-name'")
        return
        
    if not has_webhook and not has_bot_token:
        print("Missing Slack configuration. Choose one option:")
        print("\nOption 1 (Recommended) - Incoming Webhook:")
        print("export DFDRIFT_SLACK_WEBHOOK_URL='https://hooks.slack.com/services/YOUR/WEBHOOK/URL'")
        print("\nOption 2 (Advanced) - Bot Token:")
        print("export DFDRIFT_SLACK_BOT_TOKEN='xoxb-your-bot-token'")
        print("export DFDRIFT_SLACK_CHANNEL='#your-channel'")
        print("\nAlso set (for both options):")
        print("export DFDRIFT_GCS_BUCKET='your-bucket-name'")
        print("export GOOGLE_APPLICATION_CREDENTIALS='/path/to/service-account.json'")
        return
    
    # Configure storage and alerter using environment variables
    gcs_storage = dfdrift.GcsStorage()  # Uses DFDRIFT_GCS_BUCKET and DFDRIFT_GCS_PREFIX
    slack_alerter = dfdrift.SlackAlerter()  # Uses webhook URL or bot token from env vars
    
    validator = dfdrift.DfValidator(storage=gcs_storage, alerter=slack_alerter)
    
    print(f"✓ GCS Storage configured: gs://{gcs_storage.bucket}/{gcs_storage.prefix}")
    if has_webhook:
        print("✓ Slack Alerter configured: Using Incoming Webhook")
    else:
        print(f"✓ Slack Alerter configured: Using Bot Token -> {slack_alerter.channel}")
    
    current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # First run - establish baseline schema
    print("\n--- Creating baseline schema (4 users) ---")
    users_df = pd.DataFrame({
        f'user_id{current_time}': [1001, 1002, 1003, 1004],
        'username': ['alice', 'bob', 'charlie', 'diana'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'diana@example.com'],
        'age': [25, 30, 35, 28],
        'status': ['active', 'active', 'inactive', 'active']
    })
    
    validator.validate(users_df)
    print("✓ Baseline schema saved to GCS")
    
    # Second run - different row count, same schema (should NOT trigger alert)
    print("\n--- Processing larger dataset (10 users) ---")
    print("Note: Row count changes are ignored by design")
    users_df_larger = pd.DataFrame({
        f'user_id{current_time}': [1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009, 1010],
        'username': ['alice', 'bob', 'charlie', 'diana', 'eve', 'frank', 'grace', 'henry', 'iris', 'jack'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'diana@example.com',
                 'eve@example.com', 'frank@example.com', 'grace@example.com', 'henry@example.com', 
                 'iris@example.com', 'jack@example.com'],
        'age': [25, 30, 35, 28, 24, 45, 32, 38, 29, 41],
        'status': ['active', 'active', 'inactive', 'active', 'active', 'active', 'inactive', 'active', 'active', 'inactive']
    })
    
    validator.validate(users_df_larger)
    print("✓ No alert - row count changes are expected and ignored")
    
    # Third run - actual schema change (should trigger alert)
    print("\n--- Schema change detected (new column added) ---")
    users_df_new_col = pd.DataFrame({
        f'user_id{current_time}': [1001, 1002, 1003],
        'username': ['alice', 'bob', 'charlie'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
        'age': [25, 30, 35],
        'status': ['active', 'active', 'inactive'],
        'department': ['engineering', 'marketing', 'sales']  # New column!
    })
    
    validator.validate(users_df_new_col)
    print("✓ Schema change detected and Slack notification sent!")


if __name__ == "__main__":
    print("GCS + Slack Integration Examples for dfdrift")
    print("=" * 50)
    
    # Check if basic requirements are met
    gcs_bucket = os.getenv("DFDRIFT_GCS_BUCKET")
    has_webhook = bool(os.getenv("DFDRIFT_SLACK_WEBHOOK_URL"))
    has_bot_token = bool(os.getenv("DFDRIFT_SLACK_BOT_TOKEN") and os.getenv("DFDRIFT_SLACK_CHANNEL"))
    
    if not gcs_bucket or (not has_webhook and not has_bot_token):
        missing_components = []
        if not gcs_bucket:
            missing_components.append("DFDRIFT_GCS_BUCKET")
        if not has_webhook and not has_bot_token:
            missing_components.append("Slack configuration")
            
        print(f"\nMissing required components: {', '.join(missing_components)}")
        print("\nTo run these examples, please set:")
        print("export DFDRIFT_GCS_BUCKET='your-dfdrift-bucket'")
        print("export DFDRIFT_GCS_PREFIX='examples'  # Optional")
        print("export GOOGLE_APPLICATION_CREDENTIALS='/path/to/service-account.json'")
        print("\nFor Slack configuration, choose one option:")
        print("Option 1 (Recommended) - Incoming Webhook:")
        print("export DFDRIFT_SLACK_WEBHOOK_URL='https://hooks.slack.com/services/YOUR/WEBHOOK/URL'")
        print("\nOption 2 (Advanced) - Bot Token:")
        print("export DFDRIFT_SLACK_BOT_TOKEN='xoxb-your-bot-token'")
        print("export DFDRIFT_SLACK_CHANNEL='#data-monitoring'")
        exit(1)
    
    try:
        # Run examples
        production_example()
        
        print("\n" + "=" * 50)
        print("All examples completed successfully!")
        print(f"Check your GCS bucket: gs://{os.getenv('DFDRIFT_GCS_BUCKET')}")
        if has_webhook:
            print("Check your Slack channel for webhook notifications")
        else:
            print(f"Check your Slack channel: {os.getenv('DFDRIFT_SLACK_CHANNEL')}")
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        print("Please check your environment variables and credentials.")