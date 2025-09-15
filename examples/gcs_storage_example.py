"""
Example: Using GcsStorage for DataFrame schema drift detection with Google Cloud Storage

This example demonstrates how to configure dfdrift to store schemas in Google Cloud Storage
instead of local files, which is useful for serverless environments like Cloud Functions,
Lambda, or Kubernetes deployments.

Requirements:
- Install GCS support: pip install dfdrift[gcs]
- Set up Google Cloud authentication:
  - Service account key file (set GOOGLE_APPLICATION_CREDENTIALS)
  - OR Application Default Credentials (gcloud auth application-default login)
- Create a GCS bucket and configure permissions
"""

import os
import pandas as pd
import dfdrift

# Method 1: Using environment variables (recommended for production)
def example_with_env_variables():
    """Example using GCS_BUCKET environment variable"""
    
    # Set environment variables:
    # export GCS_BUCKET="my-dfdrift-bucket"
    # export GCS_PREFIX="schemas/production"  # Optional, defaults to "dfdrift"
    # export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"
    
    # Configure with GCS storage using environment variables
    gcs_storage = dfdrift.GcsStorage()  # bucket and prefix from env vars
    slack_alerter = dfdrift.SlackAlerter(channel="#data-monitoring")
    
    validator = dfdrift.DfValidator(storage=gcs_storage, alerter=slack_alerter)
    
    # First run - creates initial schema in GCS
    df1 = pd.DataFrame({
        'user_id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    validator.validate(df1)
    print("First validation complete - schema saved to GCS")
    
    # Second run - schema change (dtype change)
    df2 = pd.DataFrame({
        'user_id': ['a', 'b', 'c'],  # Changed to string!
        'name': ['David', 'Eve', 'Frank'],
        'age': [28, 32, 27]
    })
    validator.validate(df2)
    print("Second validation complete - Slack alert sent for schema drift!")


# Method 2: Using constructor arguments
def example_with_constructor_args():
    """Example passing bucket and prefix directly"""
    
    # Configure with GCS storage using constructor arguments
    gcs_storage = dfdrift.GcsStorage(
        bucket="my-company-dfdrift-bucket", 
        prefix="schemas/staging"
    )
    
    validator = dfdrift.DfValidator(storage=gcs_storage)
    
    # Create DataFrame with schema to track
    df = pd.DataFrame({
        'product_id': [101, 102, 103],
        'price': [29.99, 39.99, 49.99],
        'category': ['Electronics', 'Books', 'Clothing']
    })
    validator.validate(df)
    print("Schema stored in GCS: gs://my-company-dfdrift-bucket/schemas/staging/schemas.json")


# Method 3: Using with pandas module wrapper
def example_with_pandas_wrapper():
    """Example using dfdrift.pandas wrapper with GCS storage"""
    
    import dfdrift.pandas as pd_drift
    
    # Configure global validation with GCS
    gcs_storage = dfdrift.GcsStorage(
        bucket="ml-pipeline-schemas",
        prefix="dataframes/v1"
    )
    
    pd_drift.configure_validation(storage=gcs_storage)
    
    # Now all DataFrame creations will be monitored and stored in GCS
    df = pd_drift.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=5, freq='D'),
        'metric_value': [1.2, 1.5, 1.8, 1.1, 1.6],
        'source': ['api'] * 5
    })
    
    print("DataFrame created and schema automatically stored in GCS!")


# Method 4: Cloud Function / Serverless example
def cloud_function_example():
    """Example for serverless environments (Cloud Functions, Lambda, etc.)"""
    
    # In serverless environments, use environment variables for configuration
    # The service will automatically use the environment's service account
    
    # These would be set in the Cloud Function environment:
    # GCS_BUCKET=my-function-schemas
    # GCS_PREFIX=cloud-functions/data-processing
    
    gcs_storage = dfdrift.GcsStorage()  # Configuration from environment
    
    # Use stderr alerter for Cloud Function logs
    stderr_alerter = dfdrift.StderrAlerter()
    
    validator = dfdrift.DfValidator(storage=gcs_storage, alerter=stderr_alerter)
    
    # Process data in cloud function
    df = pd.DataFrame({
        'event_id': ['e1', 'e2', 'e3'],
        'event_type': ['click', 'view', 'purchase'],
        'user_id': [123, 456, 789]
    })
    
    validator.validate(df)
    print("Cloud Function: Schema validation completed with GCS storage")


# Method 5: Kubernetes deployment example
def kubernetes_example():
    """Example for Kubernetes deployments"""
    
    # In Kubernetes, use Workload Identity or service account secrets
    # Configure via environment variables in the deployment manifest:
    #
    # env:
    # - name: GCS_BUCKET
    #   value: "k8s-ml-schemas"
    # - name: GCS_PREFIX
    #   value: "pipelines/production"
    # - name: SLACK_BOT_TOKEN
    #   valueFrom:
    #     secretKeyRef:
    #       name: slack-credentials
    #       key: bot-token
    
    gcs_storage = dfdrift.GcsStorage()
    slack_alerter = dfdrift.SlackAlerter(channel="#ml-monitoring")
    
    validator = dfdrift.DfValidator(storage=gcs_storage, alerter=slack_alerter)
    
    # ML pipeline processing
    df = pd.DataFrame({
        'feature_1': [0.1, 0.2, 0.3],
        'feature_2': [1.0, 2.0, 3.0],
        'label': [0, 1, 0]
    })
    
    validator.validate(df)
    print("Kubernetes: ML pipeline schema validation with GCS + Slack")


if __name__ == "__main__":
    print("GCS storage examples for dfdrift")
    
    # Check if GCS bucket is configured
    if not os.getenv("GCS_BUCKET"):
        print("Warning: GCS_BUCKET not set. Set it before running examples:")
        print("export GCS_BUCKET='your-bucket-name'")
        print("export GOOGLE_APPLICATION_CREDENTIALS='/path/to/service-account.json'")
        print("\nRunning constructor argument examples instead...")
        
        print("\n1. Running example with constructor arguments...")
        example_with_constructor_args()
        
        print("\n2. Running pandas wrapper example...")
        example_with_pandas_wrapper()
        
        print("\n3. Running cloud function example (simulated)...")
        cloud_function_example()
        
    else:
        print("\n1. Running example with environment variables...")
        example_with_env_variables()
        
        print("\n2. Running example with constructor arguments...")
        example_with_constructor_args()
        
        print("\n3. Running pandas wrapper example...")
        example_with_pandas_wrapper()
        
        print("\n4. Running cloud function example...")
        cloud_function_example()
        
        print("\n5. Running kubernetes example...")
        kubernetes_example()
    
    print("\nAll examples completed! Check your GCS bucket for stored schemas.")
    print("Schema location: gs://{bucket}/{prefix}/schemas.json")