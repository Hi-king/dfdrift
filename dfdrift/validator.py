import inspect
import json
import os
import pandas as pd
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Union, Optional

from .alerters import Alerter, StderrAlerter


class SchemaStorage(ABC):
    @abstractmethod
    def save_schema(self, location_key: str, schema: Dict[str, Any]) -> None:
        pass
    
    @abstractmethod
    def load_schemas(self) -> Dict[str, Any]:
        pass


class LocalFileStorage(SchemaStorage):
    def __init__(self, storage_path: Union[str, Path] = ".dfdrift_schemas"):
        self.storage_path = Path(storage_path)
        self.schema_file = self.storage_path / "schemas.json"
    
    def save_schema(self, location_key: str, schema: Dict[str, Any]) -> None:
        self.storage_path.mkdir(exist_ok=True)
        
        all_schemas = self.load_schemas()
        all_schemas[location_key] = schema
        
        with open(self.schema_file, "w", encoding="utf-8") as f:
            json.dump(all_schemas, f, indent=2, ensure_ascii=False)
    
    def load_schemas(self) -> Dict[str, Any]:
        if self.schema_file.exists():
            with open(self.schema_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}


class GcsStorage(SchemaStorage):
    def __init__(self, bucket: Optional[str] = None, prefix: Optional[str] = None):
        self.bucket = bucket or os.getenv("GCS_BUCKET")
        self.prefix = prefix or os.getenv("GCS_PREFIX", "dfdrift")
        
        if not self.bucket:
            raise ValueError("GCS bucket must be provided either as argument or GCS_BUCKET environment variable")
        
        # Ensure prefix doesn't start with / and ends with /
        if self.prefix.startswith("/"):
            self.prefix = self.prefix[1:]
        if not self.prefix.endswith("/"):
            self.prefix += "/"
            
        self.schema_blob_name = f"{self.prefix}schemas.json"
        self.client = self._import_gcs_client()
    
    def _import_gcs_client(self):
        """Import Google Cloud Storage client"""
        try:
            from google.cloud import storage
            return storage.Client()
        except ImportError:
            raise ImportError("google-cloud-storage package is required for GcsStorage. Install with: pip install dfdrift[gcs]")
    
    def save_schema(self, location_key: str, schema: Dict[str, Any]) -> None:
        try:
            # Load existing schemas
            all_schemas = self.load_schemas()
            all_schemas[location_key] = schema
            
            # Save to GCS
            bucket = self.client.bucket(self.bucket)
            blob = bucket.blob(self.schema_blob_name)
            
            schema_json = json.dumps(all_schemas, indent=2, ensure_ascii=False)
            blob.upload_from_string(schema_json, content_type="application/json")
            
        except Exception as e:
            raise RuntimeError(f"Failed to save schema to GCS: {e}")
    
    def load_schemas(self) -> Dict[str, Any]:
        try:
            bucket = self.client.bucket(self.bucket)
            blob = bucket.blob(self.schema_blob_name)
            
            if blob.exists():
                schema_json = blob.download_as_text()
                return json.loads(schema_json)
            return {}
            
        except Exception as e:
            # Return empty dict if file doesn't exist or other errors
            return {}



class DfValidator:
    def __init__(self, storage: SchemaStorage = None, alerter: Alerter = None):
        self.storage = storage if storage is not None else LocalFileStorage()
        self.alerter = alerter if alerter is not None else StderrAlerter()
    
    def validate(self, df: pd.DataFrame) -> None:
        frame = inspect.currentframe()
        if frame is None:
            return
        
        caller_frame = frame.f_back
        if caller_frame is None:
            return
        
        filename = caller_frame.f_code.co_filename
        line_number = caller_frame.f_lineno
        location_key = f"{filename}:{line_number}"
        
        current_schema = self._get_dataframe_schema(df)
        
        existing_schemas = self.storage.load_schemas()
        if location_key in existing_schemas:
            previous_schema = existing_schemas[location_key]
            if not self._schemas_equal(previous_schema, current_schema):
                differences = self._get_schema_differences(previous_schema, current_schema)
                self.alerter.alert(
                    f"DataFrame schema changed at {location_key}. Changes: {differences}",
                    location_key,
                    previous_schema,
                    current_schema
                )
        
        self.storage.save_schema(location_key, current_schema)
    
    def _get_dataframe_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        schema = {}
        for column in df.columns:
            schema[column] = {
                "dtype": str(df[column].dtype),
                "null_count": int(df[column].isnull().sum()),
                "total_count": len(df)
            }
        
        return {
            "columns": schema,
            "shape": list(df.shape)
        }
    
    def _schemas_equal(self, schema1: Dict[str, Any], schema2: Dict[str, Any]) -> bool:
        return schema1 == schema2
    
    def _get_schema_differences(self, old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> str:
        differences = []
        
        old_columns = set(old_schema.get("columns", {}).keys())
        new_columns = set(new_schema.get("columns", {}).keys())
        
        added_columns = new_columns - old_columns
        removed_columns = old_columns - new_columns
        common_columns = old_columns & new_columns
        
        if added_columns:
            differences.append(f"Added columns: {list(added_columns)}")
        if removed_columns:
            differences.append(f"Removed columns: {list(removed_columns)}")
        
        for column in common_columns:
            old_col = old_schema["columns"][column]
            new_col = new_schema["columns"][column]
            if old_col["dtype"] != new_col["dtype"]:
                differences.append(f"Column '{column}' dtype changed: {old_col['dtype']} → {new_col['dtype']}")
        
        old_shape = old_schema.get("shape", [])
        new_shape = new_schema.get("shape", [])
        if old_shape != new_shape:
            differences.append(f"Shape changed: {old_shape} → {new_shape}")
        
        return "; ".join(differences) if differences else "Unknown change"


