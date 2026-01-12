"""Unit tests for Longbow SDK filter functionality."""
import pytest
import pandas as pd
from longbow import LongbowClient


class TestSDKFilters:
    """Test suite for SDK filter operations."""
    
    def test_search_with_single_filter(self, sample_documents, sample_embeddings, test_dataset_name):
        """Test search with a single equality filter."""
        client = LongbowClient(uri="grpc://localhost:3000")
        
        # Create dataset and insert documents
        df = pd.DataFrame(sample_documents)
        df['vector'] = sample_embeddings
        
        try:
            client.delete_namespace(test_dataset_name)
        except:
            pass  # Dataset might not exist
        
        client.insert(test_dataset_name, df)
        
        # Search with filter for category="tech"
        query_vector = sample_embeddings[0]
        results = client.search(
            test_dataset_name,
            vector=query_vector,
            k=10,
            filters=[{"field": "category", "op": "eq", "value": "tech"}]
        ).compute()
        
        # Verify all results have category="tech"
        assert len(results) > 0, "Should return at least one result"
        assert all(results['category'] == 'tech'), "All results should have category='tech'"
        
        # Cleanup
        client.delete_namespace(test_dataset_name)
    
    def test_search_with_multiple_filters_and(self, sample_documents, sample_embeddings, test_dataset_name):
        """Test search with multiple AND filters."""
        client = LongbowClient(uri="grpc://localhost:3000")
        
        df = pd.DataFrame(sample_documents)
        df['vector'] = sample_embeddings
        
        try:
            client.delete_namespace(test_dataset_name)
        except:
            pass
        
        client.insert(test_dataset_name, df)
        
        # Search with filters: category="tech" AND priority=1
        query_vector = sample_embeddings[0]
        results = client.search(
            test_dataset_name,
            vector=query_vector,
            k=10,
            filters=[
                {"field": "category", "op": "eq", "value": "tech"},
                {"field": "priority", "op": "eq", "value": 1}
            ]
        ).compute()
        
        # Verify results match both conditions
        if len(results) > 0:
            assert all(results['category'] == 'tech'), "All results should have category='tech'"
            assert all(results['priority'] == 1), "All results should have priority=1"
        
        # Cleanup
        client.delete_namespace(test_dataset_name)
    
    def test_search_without_filters(self, sample_documents, sample_embeddings, test_dataset_name):
        """Test search without any filters (baseline)."""
        client = LongbowClient(uri="grpc://localhost:3000")
        
        df = pd.DataFrame(sample_documents)
        df['vector'] = sample_embeddings
        
        try:
            client.delete_namespace(test_dataset_name)
        except:
            pass
        
        client.insert(test_dataset_name, df)
        
        # Search without filters
        query_vector = sample_embeddings[0]
        results = client.search(
            test_dataset_name,
            vector=query_vector,
            k=5,
            filters=None
        ).compute()
        
        # Should return up to k results
        assert len(results) > 0, "Should return at least one result"
        assert len(results) <= 5, "Should not return more than k results"
        
        # Cleanup
        client.delete_namespace(test_dataset_name)
    
    def test_insert_with_metadata(self, sample_documents, sample_embeddings, test_dataset_name):
        """Test that metadata fields are correctly stored and retrievable."""
        client = LongbowClient(uri="grpc://localhost:3000")
        
        df = pd.DataFrame(sample_documents)
        df['vector'] = sample_embeddings
        
        try:
            client.delete_namespace(test_dataset_name)
        except:
            pass
        
        # Insert with metadata
        client.insert(test_dataset_name, df)
        
        # Search and verify metadata is present
        query_vector = sample_embeddings[0]
        results = client.search(test_dataset_name, vector=query_vector, k=5).compute()
        
        assert 'category' in results.columns, "Results should include 'category' field"
        assert 'priority' in results.columns, "Results should include 'priority' field"
        assert 'text' in results.columns, "Results should include 'text' field"
        
        # Cleanup
        client.delete_namespace(test_dataset_name)
    
    def test_filter_with_no_matches(self, sample_documents, sample_embeddings, test_dataset_name):
        """Test search with filter that matches no documents."""
        client = LongbowClient(uri="grpc://localhost:3000")
        
        df = pd.DataFrame(sample_documents)
        df['vector'] = sample_embeddings
        
        try:
            client.delete_namespace(test_dataset_name)
        except:
            pass
        
        client.insert(test_dataset_name, df)
        
        # Search with filter that matches nothing
        query_vector = sample_embeddings[0]
        results = client.search(
            test_dataset_name,
            vector=query_vector,
            k=10,
            filters=[{"field": "category", "op": "eq", "value": "nonexistent"}]
        ).compute()
        
        # Should return empty results
        assert len(results) == 0, "Should return no results for non-matching filter"
        
        # Cleanup
        client.delete_namespace(test_dataset_name)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
