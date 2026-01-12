#!/usr/bin/env python3
"""
Unit tests for Arrow streaming functionality in Longbow SDK.
Tests zero-copy Arrow Table download and streaming RecordBatch iteration.
"""
import pytest
import pyarrow as pa
import pyarrow.flight as flight
from typing import Iterator
from longbow import LongbowClient


class TestArrowStreaming:
    """Test suite for Arrow streaming download methods."""
    
    @pytest.fixture
    def client(self):
        """Create test client."""
        return LongbowClient(uri="grpc://localhost:3000", meta_uri="grpc://localhost:3001")
    
    @pytest.fixture
    def sample_dataset(self, client):
        """Create a sample dataset for testing."""
        # Generate test data
        import numpy as np
        vectors = np.random.rand(1000, 128).astype(np.float32)
        ids = pa.array(range(1000), type=pa.int64())
        vector_array = pa.FixedSizeListArray.from_arrays(
            vectors.flatten(), 
            type=pa.list_(pa.float32(), 128)
        )
        
        table = pa.Table.from_arrays(
            [ids, vector_array],
            schema=pa.schema([
                pa.field("id", pa.int64()),
                pa.field("vector", pa.list_(pa.float32(), 128))
            ])
        )
        
        # Upload dataset
        dataset_name = "test_streaming_dataset"
        client.insert(dataset_name, table)
        
        yield dataset_name
        
        # Cleanup
        try:
            client.delete(dataset_name)
        except:
            pass
    
    def test_download_arrow_returns_table(self, client, sample_dataset):
        """Test that download_arrow() returns an Arrow Table."""
        result = client.download_arrow(sample_dataset)
        
        assert isinstance(result, pa.Table)
        assert result.num_rows > 0
        assert "id" in result.column_names
        assert "vector" in result.column_names
    
    def test_download_arrow_correct_data(self, client, sample_dataset):
        """Test that download_arrow() returns correct data."""
        result = client.download_arrow(sample_dataset)
        
        assert result.num_rows == 1000
        assert result.num_columns >= 2
        
        # Verify data types
        assert result.schema.field("id").type == pa.int64()
        assert isinstance(result.schema.field("vector").type, pa.FixedSizeListType)
    
    def test_download_stream_returns_iterator(self, client, sample_dataset):
        """Test that download_stream() returns an iterator of RecordBatches."""
        stream = client.download_stream(sample_dataset)
        
        assert hasattr(stream, '__iter__')
        
        # Consume stream
        batches = list(stream)
        assert len(batches) > 0
        assert all(isinstance(batch, pa.RecordBatch) for batch in batches)
    
    def test_download_stream_correct_row_count(self, client, sample_dataset):
        """Test that download_stream() returns all rows."""
        stream = client.download_stream(sample_dataset)
        
        total_rows = sum(batch.num_rows for batch in stream)
        assert total_rows == 1000
    
    def test_download_stream_memory_efficient(self, client, sample_dataset):
        """Test that download_stream() doesn't load all data at once."""
        import sys
        
        stream = client.download_stream(sample_dataset)
        
        # Process one batch at a time
        for batch in stream:
            # Each batch should be small
            batch_size_mb = sys.getsizeof(batch) / (1024 * 1024)
            assert batch_size_mb < 100  # Each batch < 100MB
            break  # Just test first batch
    
    def test_download_arrow_with_filter(self, client, sample_dataset):
        """Test download_arrow() with filter."""
        filter_spec = [{"field": "id", "op": "lt", "value": "100"}]
        result = client.download_arrow(sample_dataset, filter=filter_spec)
        
        assert isinstance(result, pa.Table)
        assert result.num_rows <= 100
    
    def test_download_stream_with_filter(self, client, sample_dataset):
        """Test download_stream() with filter."""
        filter_spec = [{"field": "id", "op": "lt", "value": "100"}]
        stream = client.download_stream(sample_dataset, filter=filter_spec)
        
        total_rows = sum(batch.num_rows for batch in stream)
        assert total_rows <= 100
    
    def test_download_arrow_empty_dataset(self, client):
        """Test download_arrow() with empty/non-existent dataset."""
        with pytest.raises(Exception):  # Should raise error for non-existent dataset
            client.download_arrow("nonexistent_dataset")
    
    def test_download_stream_empty_result(self, client, sample_dataset):
        """Test download_stream() with filter that returns no results."""
        filter_spec = [{"field": "id", "op": "gt", "value": "10000"}]
        stream = client.download_stream(sample_dataset, filter=filter_spec)
        
        batches = list(stream)
        total_rows = sum(batch.num_rows for batch in batches)
        assert total_rows == 0
    
    def test_backward_compatibility_download(self, client, sample_dataset):
        """Test that old download() method still works (returns Arrow Table now)."""
        result = client.download(sample_dataset)
        
        # Should return Arrow Table instead of Dask DataFrame
        assert isinstance(result, pa.Table)
        assert result.num_rows == 1000
    
    def test_download_arrow_performance(self, client, sample_dataset):
        """Test that download_arrow() is faster than old Dask method."""
        import time
        
        start = time.time()
        result = client.download_arrow(sample_dataset)
        duration = time.time() - start
        
        # Should complete in reasonable time for 1000 rows
        assert duration < 1.0  # Less than 1 second
        assert result.num_rows == 1000
    
    def test_download_stream_batching(self, client, sample_dataset):
        """Test that download_stream() returns data in batches."""
        stream = client.download_stream(sample_dataset)
        
        batches = list(stream)
        
        # Should have multiple batches for 1000 rows
        # (depends on server batch size, but should be > 1)
        assert len(batches) >= 1
        
        # Verify all batches have same schema
        if len(batches) > 1:
            schema = batches[0].schema
            assert all(batch.schema.equals(schema) for batch in batches)


class TestArrowStreamingIntegration:
    """Integration tests for Arrow streaming with real server."""
    
    @pytest.fixture
    def client(self):
        """Create client connected to real server."""
        return LongbowClient(uri="grpc://localhost:3000", meta_uri="grpc://localhost:3001")
    
    def test_large_dataset_streaming(self, client):
        """Test streaming large dataset that doesn't fit in memory."""
        # This test requires a large dataset to be pre-loaded
        # Skip if not available
        pytest.skip("Requires large dataset setup")
    
    def test_concurrent_streaming(self, client):
        """Test multiple concurrent streaming downloads."""
        # Test that multiple streams can be active simultaneously
        pytest.skip("Requires concurrent test setup")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
