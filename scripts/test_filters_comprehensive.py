#!/usr/bin/env python3
"""Comprehensive filter testing script.

Consolidates functionality from:
- cap_test.py: Tests capitalized operators
- debug_search.py: Debug search request flow
- quick_test.py: Quick filter verification
- test_ingest_schema.py: Schema validation

Tests various filter operators and edge cases.
"""
import time
import pandas as pd
import numpy as np
from longbow import LongbowClient
import pyarrow.flight as flight
import json


def test_schema_validation():
    """Test that schema includes all expected fields."""
    from longbow.ingest import to_arrow_table
    
    df = pd.DataFrame([{
        "id": 1,
        "vector": [0.1, 0.2],
        "text": "hello",
        "category": "A",
        "timestamp": pd.Timestamp.now()
    }])
    table = to_arrow_table(df)
    
    print("Schema validation:")
    print(table.schema)
    
    if "category" in table.schema.names:
        print("✓ PASS: category field found in schema")
    else:
        print("✗ FAIL: category field missing from schema")
        return False
    return True


def test_filter_operators(client, dataset_name):
    """Test various filter operators including case variations."""
    np.random.seed(42)
    
    # Create test data
    docs = [
        {"id": 1, "text": "Tech doc", "category": "tech", "score": 10},
        {"id": 2, "text": "AI doc", "category": "ai", "score": 20},
        {"id": 3, "text": "Another tech", "category": "tech", "score": 15},
    ]
    embeddings = [np.random.randn(384).tolist() for _ in range(3)]
    
    df = pd.DataFrame(docs)
    df['vector'] = embeddings
    
    # Clean up and insert
    try:
        client.delete_namespace(dataset_name)
        time.sleep(1)
    except:
        pass
    
    print(f"\nInserting {len(docs)} documents...")
    client.insert(dataset_name, df)
    time.sleep(3)  # Wait for indexing
    
    # Test 1: No filter
    print("\n1. Testing search without filters...")
    results = client.search(dataset_name, vector=embeddings[0], k=5).compute()
    print(f"   Results: {len(results)} rows")
    assert len(results) > 0, "No results without filter"
    print("   ✓ PASS")
    
    # Test 2: Lowercase operator
    print("\n2. Testing with lowercase 'eq' operator...")
    results = client.search(
        dataset_name,
        vector=embeddings[0],
        k=5,
        filters=[{"field": "category", "op": "eq", "value": "tech"}]
    ).compute()
    print(f"   Results: {len(results)} rows")
    assert len(results) == 2, f"Expected 2 tech docs, got {len(results)}"
    print("   ✓ PASS")
    
    # Test 3: Capitalized operator (Eq)
    print("\n3. Testing with capitalized 'Eq' operator...")
    results = client.search(
        dataset_name,
        vector=embeddings[0],
        k=5,
        filters=[{"field": "category", "op": "Eq", "value": "tech"}]
    ).compute()
    print(f"   Results: {len(results)} rows")
    assert len(results) == 2, f"Expected 2 tech docs, got {len(results)}"
    print("   ✓ PASS")
    
    # Test 4: Uppercase operator (EQ)
    print("\n4. Testing with uppercase 'EQ' operator...")
    results = client.search(
        dataset_name,
        vector=embeddings[0],
        k=5,
        filters=[{"field": "category", "op": "EQ", "value": "ai"}]
    ).compute()
    print(f"   Results: {len(results)} rows")
    assert len(results) == 1, f"Expected 1 ai doc, got {len(results)}"
    print("   ✓ PASS")
    
    # Test 5: Numeric filter
    print("\n5. Testing numeric filter (score > 12)...")
    results = client.search(
        dataset_name,
        vector=embeddings[0],
        k=5,
        filters=[{"field": "score", "op": "gt", "value": 12}]
    ).compute()
    print(f"   Results: {len(results)} rows")
    assert len(results) == 2, f"Expected 2 docs with score > 12, got {len(results)}"
    print("   ✓ PASS")


def test_debug_request_flow(client, dataset_name):
    """Debug test to trace search request flow."""
    print("\n6. Testing raw request flow (debug mode)...")
    
    np.random.seed(42)
    docs = [{"id": 1, "text": "Test", "category": "tech"}]
    embeddings = [np.random.randn(384).tolist()]
    
    df = pd.DataFrame(docs)
    df['vector'] = embeddings
    
    try:
        client.delete_namespace(dataset_name + "_debug")
        time.sleep(1)
    except:
        pass
    
    client.insert(dataset_name + "_debug", df)
    time.sleep(3)
    
    # Build request manually
    req = {
        "dataset": dataset_name + "_debug",
        "vector": embeddings[0],
        "k": 5,
        "filters": [{"field": "category", "op": "Eq", "value": "tech"}]
    }
    
    ticket_bytes = json.dumps({"search": req}).encode("utf-8")
    ticket = flight.Ticket(ticket_bytes)
    
    print(f"   Ticket length: {len(ticket_bytes)} bytes")
    print(f"   Ticket preview: {ticket_bytes[:100]}...")
    
    try:
        reader = client._data_client.do_get(ticket, options=client._get_call_options())
        table = reader.read_all()
        print(f"   Got table with {table.num_rows} rows")
        assert table.num_rows > 0, "No results from raw request"
        print("   ✓ PASS")
    except Exception as e:
        print(f"   ✗ FAIL: {e}")
        raise
    finally:
        client.delete_namespace(dataset_name + "_debug")


def main():
    """Run all filter tests."""
    print("=" * 60)
    print("Comprehensive Filter Testing Suite")
    print("=" * 60)
    
    # Test 1: Schema validation
    if not test_schema_validation():
        print("\n✗ Schema validation failed, aborting")
        return 1
    
    # Setup client
    client = LongbowClient(uri="grpc://localhost:3000")
    dataset_name = "filter_test_dataset"
    
    try:
        # Test 2-5: Filter operators
        test_filter_operators(client, dataset_name)
        
        # Test 6: Debug request flow
        test_debug_request_flow(client, dataset_name)
        
        print("\n" + "=" * 60)
        print("✓ All tests PASSED")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        # Cleanup
        try:
            client.delete_namespace(dataset_name)
        except:
            pass
    
    return 0


if __name__ == "__main__":
    exit(main())
