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
- GCS_BUCKET: Google Cloud Storage bucket name
- GCS_PREFIX: Optional prefix for schema files (defaults to "dfdrift")
- GOOGLE_APPLICATION_CREDENTIALS: Path to service account key file
- SLACK_BOT_TOKEN: Slack bot token (xoxb-...)
- SLACK_CHANNEL: Slack channel for notifications (#channel-name)
"""

import os
import pandas as pd
import dfdrift


def production_example():
    """Production-ready example using environment variables"""
    
    print("=== Production Integration Example ===")
    print("Using GCS for storage and Slack for notifications")
    
    # Check required environment variables
    required_vars = ["GCS_BUCKET", "SLACK_BOT_TOKEN", "SLACK_CHANNEL"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"Missing required environment variables: {', '.join(missing_vars)}")
        print("\nPlease set the following environment variables:")
        print("export GCS_BUCKET='your-bucket-name'")
        print("export SLACK_BOT_TOKEN='xoxb-your-bot-token'")
        print("export SLACK_CHANNEL='#your-channel'")
        print("export GOOGLE_APPLICATION_CREDENTIALS='/path/to/service-account.json'")
        return
    
    # Configure storage and alerter using environment variables
    gcs_storage = dfdrift.GcsStorage()  # Uses GCS_BUCKET and GCS_PREFIX
    slack_alerter = dfdrift.SlackAlerter()  # Uses SLACK_BOT_TOKEN and SLACK_CHANNEL
    
    validator = dfdrift.DfValidator(storage=gcs_storage, alerter=slack_alerter)
    
    print(f"✓ GCS Storage configured: gs://{gcs_storage.bucket}/{gcs_storage.prefix}")
    print(f"✓ Slack Alerter configured: {slack_alerter.channel}")
    
    # First run - establish baseline schema
    print("\n--- Creating baseline schema ---")
    users_df = pd.DataFrame({
        'user_id': [1001, 1002, 1003, 1004],
        'username': ['alice', 'bob', 'charlie', 'diana'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'diana@example.com'],
        'age': [25, 30, 35, 28],
        'status': ['active', 'active', 'inactive', 'active']
    })
    
    validator.validate(users_df)
    print("✓ Baseline schema saved to GCS")
    
    # Second run - schema change (new column)
    print("\n--- Simulating schema drift (new column) ---")
    users_df_v2 = pd.DataFrame({
        'user_id': [1005, 1006, 1007],
        'username': ['eve', 'frank', 'grace'],
        'email': ['eve@example.com', 'frank@example.com', 'grace@example.com'],
        'age': [32, 29, 26],
        'status': ['active', 'active', 'pending'],
        'department': ['engineering', 'marketing', 'sales']  # New column!
    })
    
    validator.validate(users_df_v2)
    print("✓ Schema change detected and Slack notification sent!")
    
    # Third run - type change
    print("\n--- Simulating schema drift (type change) ---")
    users_df_v3 = pd.DataFrame({
        'user_id': ['U1008', 'U1009', 'U1010'],  # Changed to string!
        'username': ['henry', 'iris', 'jack'],
        'email': ['henry@example.com', 'iris@example.com', 'jack@example.com'],
        'age2': [31, 27, 34],
        'status': ['active', 'inactive', 'active'],
        'department': ['hr', 'finance', 'engineering']
    })
    
    validator.validate(users_df_v3)
    print("✓ Type change detected and Slack notification sent!")



if __name__ == "__main__":
    print("GCS + Slack Integration Examples for dfdrift")
    print("=" * 50)
    
    # Check if basic requirements are met
    required_vars = ["GCS_BUCKET", "SLACK_BOT_TOKEN", "SLACK_CHANNEL"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"\nMissing required environment variables: {', '.join(missing_vars)}")
        print("\nTo run these examples, please set:")
        print("export GCS_BUCKET='your-dfdrift-bucket'")
        print("export GCS_PREFIX='examples'  # Optional")
        print("export SLACK_BOT_TOKEN='xoxb-your-bot-token'")
        print("export SLACK_CHANNEL='#data-monitoring'")
        print("export GOOGLE_APPLICATION_CREDENTIALS='/path/to/service-account.json'")
        print("\nFor ML pipeline example, optionally set:")
        print("export GCS_BUCKET='ml-features-bucket'")
        print("export SLACK_CHANNEL='#ml-alerts'")
        exit(1)
    
    try:
        # Run examples
        production_example()
        
        print("\n" + "=" * 50)
        print("All examples completed successfully!")
        print(f"Check your GCS bucket: gs://{os.getenv('GCS_BUCKET')}")
        print(f"Check your Slack channel: {os.getenv('SLACK_CHANNEL')}")
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        print("Please check your environment variables and credentials.")