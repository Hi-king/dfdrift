import inspect
import json
import os
import pandas as pd
from pathlib import Path
from typing import Dict, Any


def validate(df: pd.DataFrame) -> None:
    frame = inspect.currentframe()
    if frame is None:
        return
    
    caller_frame = frame.f_back
    if caller_frame is None:
        return
    
    filename = caller_frame.f_code.co_filename
    line_number = caller_frame.f_lineno
    
    schema = _get_dataframe_schema(df)
    
    location_key = f"{filename}:{line_number}"
    
    _save_schema(location_key, schema)


def _get_dataframe_schema(df: pd.DataFrame) -> Dict[str, Any]:
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


def _save_schema(location_key: str, schema: Dict[str, Any]) -> None:
    schema_dir = Path(".dfvalidate_schemas")
    schema_dir.mkdir(exist_ok=True)
    
    schema_file = schema_dir / "schemas.json"
    
    if schema_file.exists():
        with open(schema_file, "r", encoding="utf-8") as f:
            all_schemas = json.load(f)
    else:
        all_schemas = {}
    
    all_schemas[location_key] = schema
    
    with open(schema_file, "w", encoding="utf-8") as f:
        json.dump(all_schemas, f, indent=2, ensure_ascii=False)