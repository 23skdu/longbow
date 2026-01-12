"""Pytest configuration and fixtures for Longbow SDK tests."""
import pytest
import pandas as pd
import numpy as np


@pytest.fixture
def sample_documents():
    """Sample documents with filterable metadata."""
    return [
        {"id": 1, "text": "Vector databases store embeddings", "category": "tech", "priority": 1},
        {"id": 2, "text": "GraphRAG combines graphs with RAG", "category": "tech", "priority": 2},
        {"id": 3, "text": "Embeddings capture semantic meaning", "category": "ai", "priority": 1},
        {"id": 4, "text": "PostgreSQL is a relational database", "category": "tech", "priority": 3},
        {"id": 5, "text": "Transformers revolutionized NLP", "category": "ai", "priority": 1},
    ]


@pytest.fixture
def sample_embeddings():
    """Sample 384-dimensional embeddings (matching all-MiniLM-L6-v2)."""
    np.random.seed(42)
    return [np.random.randn(384).tolist() for _ in range(5)]


@pytest.fixture
def test_dataset_name():
    """Unique dataset name for testing."""
    import time
    return f"test_dataset_{int(time.time())}"
