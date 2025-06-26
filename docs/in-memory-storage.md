# In-Memory Storage Backend

## Overview

The in-memory storage backend allows datasets to be stored in memory using Polars DataFrames instead of saving them to disk. This provides several benefits:

- **Faster Access**: No disk I/O for reading datasets
- **Lower Latency**: Direct memory access for data operations
- **Simplified Deployment**: No need for persistent storage configuration
- **Better for Development**: Quick testing without file system artifacts

## Configuration

To enable the in-memory storage backend, set the following environment variable:

```bash
export STORAGE_BACKEND=memory
```

Or add it to your `.env` file:

```
STORAGE_BACKEND=memory
```

You can also copy the provided configuration template:

```bash
cp .env.memory .env
```

## How It Works

When the memory backend is enabled:

1. **File Upload**: Uploaded datasets are loaded into Polars DataFrames and stored in memory
2. **Data Access**: Data is read directly from the in-memory DataFrames
3. **Schema Extraction**: Schema information is extracted from Polars DataFrame metadata
4. **Statistics**: Statistics are calculated using Polars built-in functions
5. **Persistence**: Data persists only for the lifetime of the application process

## Supported File Types

The memory backend supports the same file types as the disk-based backend:
- CSV files (`.csv`)
- Excel files (`.xlsx`, `.xls`, `.xlsm`)
- Parquet files (`.parquet`)

## API Usage

The API remains exactly the same - no changes are needed to client code:

```python
# Upload a dataset
response = requests.post(
    "http://localhost:8000/api/datasets/upload",
    files={"file": open("data.csv", "rb")},
    data={"name": "My Dataset"}
)

# Get dataset data
data = requests.get(
    f"http://localhost:8000/api/datasets/{dataset_id}/versions/{version_id}/data"
)
```

## Limitations

- **Memory Usage**: All datasets are stored in RAM, so total size is limited by available memory
- **No Persistence**: Data is lost when the application restarts
- **Single Instance**: Data is not shared between multiple application instances

## Use Cases

The in-memory backend is ideal for:
- Development and testing
- Small to medium datasets
- Real-time data processing
- Temporary data analysis
- Environments where disk I/O is expensive

## Performance Considerations

- Polars DataFrames are highly optimized for in-memory operations
- Column-oriented storage provides efficient memory usage
- Lazy evaluation capabilities for complex queries
- Multi-threaded operations for better performance

## Switching Between Backends

You can easily switch between storage backends without code changes:

```bash
# Use local file storage (default)
export STORAGE_BACKEND=local

# Use in-memory storage
export STORAGE_BACKEND=memory
```

The application will use the appropriate backend based on the configuration.