import inspect
import json
import os
import pandas as pd
import sys
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Union, Optional


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


class Alerter(ABC):
    @abstractmethod
    def alert(self, message: str, location_key: str, old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> None:
        pass


class StderrAlerter(Alerter):
    def alert(self, message: str, location_key: str, old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> None:
        print(f"WARNING: {message}", file=sys.stderr)
        print(f"Location: {location_key}", file=sys.stderr)


class SlackAlerter(Alerter):
    def __init__(self, channel: str, token: Optional[str] = None):
        self.token = token or os.getenv("SLACK_BOT_TOKEN")
        self.channel = channel
        
        if not self.token:
            raise ValueError("Slack token must be provided either as argument or SLACK_BOT_TOKEN environment variable")
        
        if not self.channel:
            raise ValueError("Slack channel must be provided")
        
        self.client = self._import_slack_sdk()
    
    def _import_slack_sdk(self):
        """Import slack SDK and create client"""
        try:
            from slack_sdk import WebClient
            return WebClient(token=self.token)
        except ImportError:
            raise ImportError("slack-sdk package is required for SlackAlerter. Install with: pip install slack-sdk")
    
    def alert(self, message: str, location_key: str, old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> None:
        try:
            # Format message for Slack
            slack_message = f"🚨 *DataFrame Schema Drift Detected*\n\n"
            slack_message += f"*Location:* `{location_key}`\n"
            slack_message += f"*Details:* {message}\n\n"
            
            # Add schema comparison details
            old_columns = set(old_schema.get("columns", {}).keys())
            new_columns = set(new_schema.get("columns", {}).keys())
            
            if old_columns != new_columns:
                added = new_columns - old_columns
                removed = old_columns - new_columns
                if added:
                    slack_message += f"*Added columns:* {', '.join(f'`{col}`' for col in added)}\n"
                if removed:
                    slack_message += f"*Removed columns:* {', '.join(f'`{col}`' for col in removed)}\n"
            
            # Send to Slack
            response = self.client.chat_postMessage(
                channel=self.channel,
                text=slack_message,
                mrkdwn=True
            )
            
            if not response["ok"]:
                print(f"Failed to send Slack message: {response.get('error', 'Unknown error')}", file=sys.stderr)
                # Fallback to stderr
                print(f"WARNING: {message}", file=sys.stderr)
                print(f"Location: {location_key}", file=sys.stderr)
                
        except Exception as e:
            print(f"Error sending Slack notification: {e}", file=sys.stderr)
            # Fallback to stderr
            print(f"WARNING: {message}", file=sys.stderr)
            print(f"Location: {location_key}", file=sys.stderr)


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


