# Longbow SDK Tests

This directory contains unit tests for the Longbow Python SDK.

## Running Tests

```bash
# Install test dependencies
pip install pytest pytest-asyncio

# Run all tests
pytest tests/ -v

# Run specific test file
pytest tests/test_sdk_filters.py -v

# Run specific test
pytest tests/test_sdk_filters.py::TestSDKFilters::test_search_with_single_filter -v
```

## Test Requirements

- Running Longbow server on `localhost:3000`
- Python dependencies: `pytest`, `pandas`, `numpy`, `longbow`

## Test Coverage

- `test_sdk_filters.py`: Filter functionality for search and insert operations
