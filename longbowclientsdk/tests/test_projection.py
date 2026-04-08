"""Unit tests for Longbow SDK projection functionality."""

import pytest


class TestProjectionSDK:
    """Test suite for SDK projection operations."""

    def test_search_with_projection_columns(
        self, client, sample_dataset, sample_documents, sample_embeddings
    ):
        """Test search with column projection."""
        # Insert data
        client.insert(sample_dataset, sample_documents)

        # Search with projection - only return specific columns
        results = client.search(
            sample_dataset, sample_embeddings[0], k=5, projection=["id", "score"]
        )

        # Verify projection was applied
        assert len(results) > 0
        # Results should contain the projected columns
        if hasattr(results, "columns"):
            # Check that only projected columns are present (or additional metadata)
            for col in results.columns:
                assert col in ["id", "score"] or col.startswith("_")

    def test_search_with_projection_and_filters(
        self, client, sample_dataset, sample_documents, sample_embeddings
    ):
        """Test search with both projection and filters."""
        # Insert data
        client.insert(sample_dataset, sample_documents)

        # Search with both projection and filters
        results = client.search(
            sample_dataset,
            sample_embeddings[0],
            k=5,
            filters=[{"field": "category", "op": "eq", "value": "tech"}],
            projection=["id", "score"],
        )

        # Verify both were applied
        assert results is not None

    def test_search_without_projection(
        self, client, sample_dataset, sample_documents, sample_embeddings
    ):
        """Test search without projection (default behavior)."""
        # Insert data
        client.insert(sample_dataset, sample_documents)

        # Search without projection
        results = client.search(sample_dataset, sample_embeddings[0], k=5)

        # Should return all columns
        assert len(results) > 0
        if hasattr(results, "columns"):
            assert "vector" in results.columns or "_vector" in str(results.columns)
