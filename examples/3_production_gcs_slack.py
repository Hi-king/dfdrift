#!/usr/bin/env python3
"""
Production Schema Drift Detection with GCS and Slack

This example demonstrates production-ready schema drift detection using:
- Google Cloud Storage for schema persistence (survives container restarts)  
- Slack notifications for team alerts

Setup Required:
1. Install with GCS and Slack support:
   pip install dfdrift[gcs,slack]

2. Set environment variables:
   export DFDRIFT_GCS_BUCKET="your-dfdrift-bucket"
   export DFDRIFT_GCS_PREFIX="production"  # optional
   export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

   # For Slack (choose one):
   # Option A (Recommended): Incoming Webhook  
   export DFDRIFT_SLACK_WEBHOOK_URL="https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
   
   # Option B: Bot Token
   export DFDRIFT_SLACK_BOT_TOKEN="xoxb-your-bot-token"
   export DFDRIFT_SLACK_CHANNEL="#data-alerts"

How to use:
1. Configure environment variables above
2. Run this script the first time: python 3_production_gcs_slack.py
   - Creates baseline schema in GCS
   - No Slack alert (first run)

3. Deploy updated version of this script to production
4. Run the second time: python 3_production_gcs_slack.py  
   - Detects schema changes
   - Sends Slack alert to your team
   - Updates schema in GCS

This simulates a real ML/data pipeline deployment scenario.
"""

import pandas as pd
import dfdrift
import os
from datetime import datetime

def check_environment():
    """Check if required environment variables are set"""
    
    print("🔧 Checking environment configuration...")
    
    # Check GCS configuration
    gcs_bucket = os.getenv("DFDRIFT_GCS_BUCKET")
    if not gcs_bucket:
        print("❌ Missing DFDRIFT_GCS_BUCKET environment variable")
        print("   Set with: export DFDRIFT_GCS_BUCKET='your-bucket-name'")
        return False
        
    # Check Slack configuration  
    has_webhook = bool(os.getenv("DFDRIFT_SLACK_WEBHOOK_URL"))
    has_bot_token = bool(os.getenv("DFDRIFT_SLACK_BOT_TOKEN") and os.getenv("DFDRIFT_SLACK_CHANNEL"))
    
    if not has_webhook and not has_bot_token:
        print("❌ Missing Slack configuration")
        print("   Option 1: export DFDRIFT_SLACK_WEBHOOK_URL='https://hooks.slack.com/services/...'")
        print("   Option 2: export DFDRIFT_SLACK_BOT_TOKEN='xoxb-...' and DFDRIFT_SLACK_CHANNEL='#channel'")
        return False
    
    print(f"✅ GCS Bucket: {gcs_bucket}")
    if has_webhook:
        print("✅ Slack: Incoming Webhook configured")
    else:
        print(f"✅ Slack: Bot Token configured for {os.getenv('DFDRIFT_SLACK_CHANNEL')}")
        
    return True


def simulate_ml_feature_pipeline():
    """
    Simulate an ML feature engineering pipeline with time-based features.
    """
    
    from datetime import datetime
    
    print("\n=== ML Feature Pipeline ===")
    
    # Create time-based feature columns
    model_version = f"model_v{datetime.now().strftime('%H%M')}"
    feature_hash = f"feat_{datetime.now().strftime('%S%f')[:6]}"
    
    print(f"🚀 Generating features for {model_version}")
    
    features_df = pd.DataFrame({
        'user_id': [1001, 1002, 1003, 1004, 1005],
        'age': [25, 30, 35, 28, 42],
        'total_purchases': [5, 12, 3, 8, 15],
        'account_age_days': [365, 180, 90, 1200, 2000],
        model_version: [0.1, 0.8, 0.3, 0.6, 0.9],  # Dynamic column name
        feature_hash: [1, 2, 3, 4, 5]  # Another dynamic column
    })
    
    print(f"📊 Generated {len(features_df)} feature vectors")
    print(f"🔧 Dynamic features: {model_version}, {feature_hash}")
    
    return features_df


def main():
    print("🏭 Production Schema Drift Detection - GCS + Slack Example")
    print("=" * 65)
    
    # Check environment setup
    if not check_environment():
        print("\n❌ Environment not configured. Please set required variables and try again.")
        return
    
    print("\n🔧 Initializing production monitoring...")
    
    try:
        # Create production-ready validator
        # GCS storage for persistence, Slack for alerts
        gcs_storage = dfdrift.GcsStorage()  # Uses DFDRIFT_GCS_BUCKET env var
        slack_alerter = dfdrift.SlackAlerter()  # Uses Slack env vars
        
        validator = dfdrift.DfValidator(storage=gcs_storage, alerter=slack_alerter)
        
        print("✅ Production validator initialized")
        print(f"   📦 Storage: GCS bucket '{gcs_storage.bucket}/{gcs_storage.prefix}'")
        print(f"   📢 Alerts: Slack notifications enabled")
        
    except Exception as e:
        print(f"❌ Failed to initialize production components: {e}")
        print("💡 Make sure GCS credentials and Slack configuration are correct")
        return
    
    print("\n🔄 Running feature pipeline...")
    
    try:
        # Run the ML pipeline
        features = simulate_ml_feature_pipeline()
        
        print(f"\n📊 Feature matrix shape: {features.shape}")
        print("\n🔍 Running schema validation...")
        
        # Critical line: validate features before feeding to ML model
        validator.validate(features)  # This line is tracked across deployments
        
        print("✅ Schema validation complete!")
        print("📂 Schema stored in GCS for team access")
        
        print("📢 If schema changes were detected, your team received a Slack alert!")
        
    except Exception as e:
        print(f"❌ Pipeline failed: {e}")
        return
    
    print(f"\n🎯 Production pipeline completed successfully!")
    print(f"💡 In real production:")
    print(f"   - This would run on schedule (cron, Airflow, etc.)")
    print(f"   - Schema changes alert the entire data team via Slack")
    print(f"   - GCS storage ensures schema history survives deployments")
    print(f"   - Multiple pipelines can share the same schema store")


if __name__ == "__main__":
    main()