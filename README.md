# dfvalidate

A DataFrame schema validation and change detection library for pandas DataFrames.

## Features

- **Schema Tracking**: Automatically save DataFrame schemas with location information (file:line)
- **Change Detection**: Detect schema changes between executions and alert when differences are found
- **Configurable Storage**: Support for local file storage with extensible interface for future cloud storage (GCS, etc.)
- **Configurable Alerting**: Built-in stderr alerter with extensible interface for future integrations (Slack, etc.)

## Installation

```bash
# Install in development mode
uv pip install -e .
```

## Usage

dfvalidate offers two ways to validate DataFrames:

### 1. Import Replacement

Simply replace your pandas import with dfvalidate.pandas:

```python
import dfvalidate.pandas as pd

# Configure validation (optional - uses default settings if omitted)
pd.configure_validation()

# All DataFrame operations are automatically validated
df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'city': ['Tokyo', 'Osaka', 'Kyoto']
})
# Schema automatically saved with location info
```

### 2. Explicit Validation

```python
import pandas as pd
import dfvalidate

# Create a validator instance
validator = dfvalidate.DfValidator()

# Validate a DataFrame manually
df = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
    'city': ['Tokyo', 'Osaka', 'Kyoto']
})

validator.validate(df)
```

## Configuration

### Custom Storage Path

```python
import dfvalidate

# Method 1: Import replacement with custom storage
import dfvalidate.pandas as pd
pd.configure_validation(
    storage=dfvalidate.LocalFileStorage("./my_schemas")
)

# Method 2: Explicit validation with custom storage
validator = dfvalidate.DfValidator(
    storage=dfvalidate.LocalFileStorage("./my_schemas")
)
```

### Custom Alerter

```python
import dfvalidate

# Built-in stderr alerter (default)
import dfvalidate.pandas as pd
pd.configure_validation(alerter=dfvalidate.StderrAlerter())

# Or implement your own alerter
class SlackAlerter(dfvalidate.Alerter):
    def alert(self, message, location_key, old_schema, new_schema):
        # Send to Slack
        pass

pd.configure_validation(alerter=SlackAlerter())
```

## Schema Change Detection

When a DataFrame schema changes between executions, dfvalidate will automatically detect and alert:

- **Added columns**: New columns that weren't in the previous schema
- **Removed columns**: Columns that existed before but are now missing
- **Type changes**: When a column's dtype changes (e.g., int64 → object)
- **Shape changes**: When the DataFrame dimensions change

Example alert output:
```
WARNING: DataFrame schema changed at /path/to/file.py:25. Changes: Added columns: ['new_col']; Column 'age' dtype changed: int64 → object
Location: /path/to/file.py:25
```

## Examples

See the `samples/` directory for usage examples:

- `samples/sample.py`: Explicit validation
- `samples/sample_custom_path.py`: Custom storage path
- `samples/sample_changing_schema.py`: Schema change detection demo
- `samples/sample_pandas_import.py`: Import replacement

## Architecture

### Storage Interface

```python
class SchemaStorage(ABC):
    def save_schema(self, location_key: str, schema: Dict[str, Any]) -> None:
        pass
    
    def load_schemas(self) -> Dict[str, Any]:
        pass
```

### Alerter Interface

```python
class Alerter(ABC):
    def alert(self, message: str, location_key: str, old_schema: Dict[str, Any], new_schema: Dict[str, Any]) -> None:
        pass
```

## Schema Format

Schemas are stored as JSON with the following structure:

```json
{
  "/path/to/file.py:line_number": {
    "columns": {
      "column_name": {
        "dtype": "int64",
        "null_count": 0,
        "total_count": 100
      }
    },
    "shape": [100, 3]
  }
}
```

## Development

Run the samples to test functionality:

```bash
# Import replacement
uv run python samples/sample_pandas_import.py  # Run twice to see alerts

# Explicit validation
uv run python samples/sample.py

# Test schema change detection
uv run python samples/sample_changing_schema.py  # Run twice to see alerts
```