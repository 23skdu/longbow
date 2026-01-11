from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field

class Vector(BaseModel):
    """Represents a single vector record."""
    id: int
    values: List[float]
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    timestamp: Optional[int] = None

class SearchResult(BaseModel):
    """Represents a single search result item."""
    id: int
    score: float
    values: Optional[List[float]] = None
    metadata: Optional[Dict[str, Any]] = None

class IndexStats(BaseModel):
    """Statistics about a specific index/dataset."""
    name: str
    dimension: int
    count: int
    segments: int
    memory_usage_bytes: int

class NamespaceInfo(BaseModel):
    """Information about a namespace."""
    name: str
    status: str
    vector_count: int
    created_at: int
