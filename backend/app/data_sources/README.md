# Adding New Data Sources

## Quick Start
1. Copy the `example_source` folder with a new name
2. Implement the required methods in `source.py`:
   - `_validate_config()`
   - `fetch_data()`
   - `close()`
3. Define your configuration in `config.py`
4. Add your source to `app/config/sources.py`

## Example Implementation
The Danish WFS source provides a complete example of a working implementation.

## Testing
1. Create test file: `tests/data_sources/test_your_source.py`
2. Test your implementation:
```python
from your_source import YourDataSource

async def test_your_source():
    config = {
        "name": "Test Source",
        "type": "your_type",
        # Add required config
    }
    source = YourDataSource(config)
    result = await source.fetch_data()
    assert result.data is not None
```

## Adding to Configuration
Add your source to `app/config/sources.py`:
```python
SOURCES = {
    "your_source_id": YourSourceConfig(
        name="Your Source Name",
        description="Description of your source",
        # Add required config
    )
}
```
