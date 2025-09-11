import inspect
import json
import pandas as pd
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Union


class SchemaStorage(ABC):
    @abstractmethod
    def save_schema(self, location_key: str, schema: Dict[str, Any]) -> None:
        pass
    
    @abstractmethod
    def load_schemas(self) -> Dict[str, Any]:
        pass


class LocalFileStorage(SchemaStorage):
    def __init__(self, storage_path: Union[str, Path] = ".dfvalidate_schemas"):
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


class DfValidator:
    def __init__(self, storage: SchemaStorage = None):
        self.storage = storage if storage is not None else LocalFileStorage()
    
    def validate(self, df: pd.DataFrame) -> None:
        frame = inspect.currentframe()
        if frame is None:
            return
        
        caller_frame = frame.f_back
        if caller_frame is None:
            return
        
        filename = caller_frame.f_code.co_filename
        line_number = caller_frame.f_lineno
        
        schema = self._get_dataframe_schema(df)
        location_key = f"{filename}:{line_number}"
        
        self.storage.save_schema(location_key, schema)
    
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


