#!/usr/bin/env python3
"""
Longbow Vector Search Test Script

Generates Lorem Ipsum text blurbs, creates embeddings, uploads to Longbow,
and performs vector search to verify keyword relevancy.

Requirements:
    pip install pyarrow sentence-transformers lorem-text numpy
"""

import pyarrow as pa
import pyarrow.flight as flight
import numpy as np
from sentence_transformers import SentenceTransformer
from lorem_text import lorem
import time
import sys
import psutil
import os
import json
import math

# Configuration
LONGBOW_HOSTS = [
    "grpc://0.0.0.0:3000",
    "grpc://0.0.0.0:3010",
    "grpc://0.0.0.0:3020"
]
DATASET_PREFIX = "lorem_test"
# Configuration: NUM_BLURBS can be set via env or first command line arg
NUM_BLURBS = int(os.getenv("NUM_BLURBS", sys.argv[1] if len(sys.argv) > 1 else 10000))
EMBEDDING_MODEL = "all-MiniLM-L6-v2"  # 384 dimensions, fast
SEARCH_K = 5
NUM_SEARCH_ITERATIONS = 10  # Run each query 10 times

# Test keywords/phrases
TEST_QUERIES = [
    "technology and innovation",
    "business strategy",
    "health and wellness",
    "education learning",
    "environmental sustainability"
]

class TestMetrics:
    """Helper to track and report test performance metrics."""
    def __init__(self):
        self.start_time = time.time()
        self.phases = {}
        self.mem_start = self._get_mem()

    def _get_mem(self):
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / (1024 * 1024)  # MB

    def start_phase(self, name):
        self.phases[name] = {"start": time.time(), "mem_before": self._get_mem()}

    def end_phase(self, name, extra_info=None):
        if name in self.phases:
            p = self.phases[name]
            p["end"] = time.time()
            p["duration"] = p["end"] - p["start"]
            p["mem_after"] = self._get_mem()
            p["mem_delta"] = p["mem_after"] - p["mem_before"]
            if extra_info:
                p.update(extra_info)

    def report(self):
        print("\n" + "=" * 80)
        print("PERFORMANCE REPORT")
        print("=" * 80)
        print(f"{'Phase':<25} {'Duration (s)':<15} {'Mem (MB)':<15} {'Notes'}")
        print("-" * 80)
        for name, p in self.phases.items():
            duration = f"{p['duration']:.2f}"
            mem = f"{p['mem_after']:.1f} (+{p['mem_delta']:.1f})"
            notes = p.get("notes", "")
            print(f"{name:<25} {duration:<15} {mem:<15} {notes}")
        
        total_duration = time.time() - self.start_time
        print("-" * 80)
        print(f"Total Execution Time: {total_duration:.2f} seconds")
        print(f"Peak Memory Usage: {self._get_mem():.1f} MB")
        print("=" * 80)

class LongbowClient:
    """Simple Longbow client for testing."""
    
    def __init__(self, host):
        self.client = flight.FlightClient(host)
        # Assuming meta server is always at data_port + 1
        port = int(host.split(":")[-1])
        meta_host = host.rsplit(":", 1)[0] + ":" + str(port + 1)
        self.meta_client = flight.FlightClient(meta_host)
        print(f"Connected to Longbow at {host} (Meta: {meta_host})")

    def do_put(self, dataset_name, table, metrics=None, chunk_size=10000):
        """Upload data using DoPut, chunking to avoid size limits."""
        if metrics: metrics.start_phase("Data Upload")
        descriptor = flight.FlightDescriptor.for_path(dataset_name)
        writer, _ = self.client.do_put(descriptor, table.schema)
        
        # Manually chunk the table into record batches
        batches = table.to_batches(max_chunksize=chunk_size)
        total_bytes = table.nbytes
        
        for i, batch in enumerate(batches):
            writer.write_batch(batch)
            if (i + 1) % 10 == 0 or i == len(batches) - 1:
                print(f"Uploaded batch {i + 1}/{len(batches)} ({((i+1)/len(batches))*100:.1f}%)")
        
        writer.close()
        throughput_rows = len(table)
        if metrics:
            duration = metrics.phases["Data Upload"]["duration"] if "Data Upload" in metrics.phases and "duration" in metrics.phases["Data Upload"] else 0
            mb_per_sec = (total_bytes / (1024 * 1024 * duration)) if duration > 0 else 0
            metrics.end_phase("Data Upload", {"notes": f"{throughput_rows} rows, {total_bytes/(1024*1024):.1f} MB ({mb_per_sec:.2f} MB/s)"})
        print(f"Uploaded {len(table)} rows to '{dataset_name}'")
    
    def vector_search(self, dataset_name, query_vector, k=5):
        """Perform vector search using DoAction."""
        # Create search request
        request = {
            "dataset": dataset_name,
            "k": k,
            "vector": query_vector.tolist()
        }
        
        import json
        payload = json.dumps(request).encode('utf-8')
        
        # Execute search via DoAction on meta server
        action = flight.Action("VectorSearch", payload)
        
        # Get results
        results = list(self.meta_client.do_action(action))
        if not results:
            return pa.table({})
        
        # Deserialize result
        result_data = json.loads(results[0].body.to_pybytes().decode('utf-8'))
        
        # VectorSearch returns {"ids": [...], "scores": [...]}
        if not result_data or 'ids' not in result_data:
            return pa.table({})
        
        ids = result_data['ids']
        scores = result_data['scores']
        
        if not ids:
            return pa.table({})
        
        try:
            # To support global search, we need to fetch from all nodes
            all_node_tables = []
            for host in LONGBOW_HOSTS:
                try:
                    node_client = flight.FlightClient(host)
                    query = {"name": dataset_name}
                    ticket = flight.Ticket(json.dumps(query).encode('utf-8'))
                    reader = node_client.do_get(ticket)
                    all_node_tables.append(reader.read_all())
                except Exception as node_e:
                    # Skip nodes that don't have the dataset yet or are down
                    pass
            
            if not all_node_tables:
                return pa.table({'id': ids, 'score': scores})
                
            full_table = pa.concat_tables(all_node_tables)
            
            # Map IDs to their scores for sorting
            id_to_score = {int(id_val): score_val for id_val, score_val in zip(ids, scores)}
            
            # Filter to only the IDs we got from search
            id_set = set(ids)
            mask = [val.as_py() in id_set for val in full_table['id']]
            filtered_table = full_table.filter(mask)
            
            # Add scores to the table for sorting
            row_scores = []
            for row_id in filtered_table['id']:
                row_scores.append(id_to_score.get(row_id.as_py(), 0.0))
            
            table_with_scores = filtered_table.append_column('score', pa.array(row_scores, type=pa.float32()))
            
            # Sort by score ascending (distances)
            if hasattr(table_with_scores, 'sort_by'):
                return table_with_scores.sort_by([('score', 'ascending')])
            else:
                indices = np.argsort(row_scores)
                return table_with_scores.take(indices)

        except Exception as e:
            print(f"  Warning: Could not fetch full records: {e}")
            return pa.table({
                'id': ids,
                'score': scores
            })

def generate_lorem_blurbs(num_blurbs, metrics=None):
    """Generate Lorem Ipsum text blurbs with varied content and time the process."""
    if metrics: metrics.start_phase("Text Generation")
    print(f"\nGenerating {num_blurbs} Lorem Ipsum blurbs...")
    
    blurbs = []
    topics = [
        "technology", "business", "health", "education", "environment",
        "science", "art", "sports", "travel", "food"
    ]
    
    for i in range(num_blurbs):
        # Generate 5 paragraphs, each with 3-5 sentences
        paragraphs = []
        for _ in range(5):
            num_sentences = np.random.randint(3, 6)
            sentences = [lorem.sentence() for _ in range(num_sentences)]
            paragraph = ' '.join(sentences)
            paragraphs.append(paragraph)
        
        text = '\n\n'.join(paragraphs)
        
        # Add a topic keyword for relevancy testing
        topic = topics[i % len(topics)]
        text = f"{topic.capitalize()}: {text}"
        
        blurbs.append({
            'id': i,
            'text': text,
            'topic': topic,
            'label': f"embed_{(i % 10) + 1}"  # Labels: embed_1 to embed_10
        })
    
    if metrics:
        metrics.end_phase("Text Generation", {"notes": f"{len(blurbs)} items"})
    return blurbs

def create_embeddings(blurbs, model_name, metrics=None):
    """Create vector embeddings using SentenceTransformer."""
    if metrics: metrics.start_phase("Embedding Creation")
    print(f"\nCreating embeddings using {model_name}...")
    
    model = SentenceTransformer(model_name)
    texts = [b['text'] for b in blurbs]
    
    # Generate embeddings
    embeddings = model.encode(texts, show_progress_bar=True, convert_to_numpy=True)
    
    if metrics:
        metrics.end_phase("Embedding Creation", {"notes": f"dim={embeddings.shape[1]}"})
    print(f"Created {len(embeddings)} embeddings of dimension {embeddings.shape[1]}")
    return embeddings, model

def create_arrow_table(blurbs, embeddings, metrics=None):
    """Create PyArrow table with embeddings."""
    if metrics: metrics.start_phase("Arrow Table Creation")
    print("\nCreating Arrow table...")
    
    # Prepare data
    ids = [b['id'] for b in blurbs]
    texts = [b['text'] for b in blurbs]
    topics = [b['topic'] for b in blurbs]
    labels = [b['label'] for b in blurbs]
    
    # Create schema
    vector_dim = embeddings.shape[1]
    schema = pa.schema([
        ('id', pa.int64()),
        ('text', pa.string()),
        ('topic', pa.string()),
        ('label', pa.string()),
        ('vector', pa.list_(pa.float32(), vector_dim))
    ])
    
    # Convert embeddings to list of lists
    vector_data = [emb.astype(np.float32).tolist() for emb in embeddings]
    
    # Create table
    table = pa.table({
        'id': ids,
        'text': texts,
        'topic': topics,
        'label': labels,
        'vector': vector_data
    }, schema=schema)
    
    if metrics:
        metrics.end_phase("Arrow Table Creation", {"notes": f"{table.nbytes/(1024*1024):.1f} MB"})
    print(f"Created Arrow table: {len(table)} rows, {len(table.schema)} columns")
    return table

def test_vector_search(client, dataset_name, model, queries, k=5, num_iterations=10):
    """Test vector search with sample queries and collect performance metrics."""
    print(f"\nTesting Vector Search (k={k}, iterations={num_iterations})...")
    print("=" * 80)
    
    results_summary = []
    all_latencies = []
    
    for i, query in enumerate(queries, 1):
        print(f"\n[Query {i}] '{query}'")
        print("-" * 80)
        
        # Create query embedding once
        query_embedding = model.encode([query], convert_to_numpy=True)[0]
        
        print(f"  Searching dataset: '{dataset_name}'")
        
        # Run multiple iterations to measure latency
        iteration_latencies = []
        first_result = None
        
        for iteration in range(num_iterations):
            try:
                # Measure search latency
                search_start = time.time()
                result_table = client.vector_search(dataset_name, query_embedding, k=k)
                search_latency = (time.time() - search_start) * 1000  # Convert to ms
                
                iteration_latencies.append(search_latency)
                
                if iteration == 0:
                    first_result = result_table
                    
            except Exception as e:
                print(f"  Iteration {iteration+1} failed: {e}")
                continue
        
        # Calculate statistics
        if iteration_latencies:
            avg_latency = np.mean(iteration_latencies)
            p50_latency = np.percentile(iteration_latencies, 50)
            p95_latency = np.percentile(iteration_latencies, 95)
            p99_latency = np.percentile(iteration_latencies, 99)
            min_latency = np.min(iteration_latencies)
            max_latency = np.max(iteration_latencies)
            throughput = 1000.0 / avg_latency  # queries per second
            
            all_latencies.extend(iteration_latencies)
            
            print(f"\n  Performance Metrics ({num_iterations} iterations):")
            print(f"     Avg Latency:  {avg_latency:.2f} ms")
            print(f"     P50 Latency:  {p50_latency:.2f} ms")
            print(f"     P95 Latency:  {p95_latency:.2f} ms")
            print(f"     P99 Latency:  {p99_latency:.2f} ms")
            print(f"     Min Latency:  {min_latency:.2f} ms")
            print(f"     Max Latency:  {max_latency:.2f} ms")
            print(f"     Throughput:   {throughput:.2f} queries/sec")
        
        # Display first result
        if first_result is not None and len(first_result) > 0:
            print(f"\n  Top Results:")
            for j in range(min(3, len(first_result))):  # Show top 3
                row_id = first_result['id'][j].as_py()
                text = first_result['text'][j].as_py()
                topic = first_result['topic'][j].as_py()
                label = first_result['label'][j].as_py()
                
                # Calculate relevancy score
                result_vector = np.array(first_result['vector'][j].as_py())
                similarity = np.dot(query_embedding, result_vector) / (
                    np.linalg.norm(query_embedding) * np.linalg.norm(result_vector)
                )
                
                print(f"     {j+1}. [Score: {similarity:.4f}] ID={row_id}, Label={label}, Topic={topic}")
                if j == 0:
                    print(f"        Text: {text[:150]}...")
                
                if j == 0:
                    results_summary.append({
                        'query': query,
                        'top_score': similarity,
                        'top_topic': topic,
                        'avg_latency_ms': avg_latency if iteration_latencies else 0,
                        'throughput_qps': throughput if iteration_latencies else 0
                    })
        elif first_result is not None:
            print("  No results found")
            results_summary.append({
                'query': query,
                'top_score': 0.0,
                'top_topic': 'NO_RESULTS',
                'avg_latency_ms': avg_latency if iteration_latencies else 0,
                'throughput_qps': throughput if iteration_latencies else 0
            })
    
    return results_summary, all_latencies

def print_summary(results_summary, all_latencies):
    """Print test summary with performance metrics."""
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    
    for i, result in enumerate(results_summary, 1):
        print(f"{i}. Query: '{result['query']}'")
        print(f"   Top Score:    {result['top_score']:.4f}")
        print(f"   Top Topic:    {result['top_topic']}")
        print(f"   Avg Latency:  {result['avg_latency_ms']:.2f} ms")
        print(f"   Throughput:   {result['throughput_qps']:.2f} qps")
        print()
    
    if all_latencies:
        avg_latency = np.mean(all_latencies)
        p50_latency = np.percentile(all_latencies, 50)
        p95_latency = np.percentile(all_latencies, 95)
        p99_latency = np.percentile(all_latencies, 99)
        avg_throughput = 1000.0 / avg_latency
        
        print("=" * 80)
        print("OVERALL PERFORMANCE")
        print("=" * 80)
        print(f"Total Queries:     {len(all_latencies)}")
        print(f"Avg Latency:       {avg_latency:.2f} ms")
        print(f"P50 Latency:       {p50_latency:.2f} ms")
        print(f"P95 Latency:       {p95_latency:.2f} ms")
        print(f"P99 Latency:       {p99_latency:.2f} ms")
        print(f"Avg Throughput:    {avg_throughput:.2f} queries/sec")
    
    avg_score = np.mean([r['top_score'] for r in results_summary])
    print(f"\nAverage Top Score: {avg_score:.4f}")
    print("=" * 80)

def main():
    """Main test workflow."""
    metrics = TestMetrics()
    print("=" * 80)
    print("LONGBOW VECTOR SEARCH TEST")
    print("=" * 80)
    
    try:
        # Step 1: Generate Lorem Ipsum blurbs
        blurbs = generate_lorem_blurbs(NUM_BLURBS, metrics)
        
        # Step 2: Create embeddings
        embeddings, model = create_embeddings(blurbs, EMBEDDING_MODEL, metrics)
        
        # Step 3: Create Arrow table
        table = create_arrow_table(blurbs, embeddings, metrics)
        
        # Step 4: Shard and Upload Data
        dataset_name = f"{DATASET_PREFIX}_{int(time.time())}"
        num_nodes = len(LONGBOW_HOSTS)
        rows_per_shard = len(table) // num_nodes
        
        print(f"\nDistributing {len(table)} rows across {num_nodes} nodes...")
        
        for i, host in enumerate(LONGBOW_HOSTS):
            start_row = i * rows_per_shard
            end_row = (i + 1) * rows_per_shard if i < num_nodes - 1 else len(table)
            shard = table.slice(start_row, end_row - start_row)
            
            print(f"\nConnecting to node {i+1} at {host}...")
            client = LongbowClient(host)
            print(f"Uploading {len(shard)} rows to '{dataset_name}' on node {i+1}...")
            client.do_put(dataset_name, shard, metrics=metrics)
        
        # Wait for indexing across cluster
        metrics.start_phase("Indexing Wait")
        print("\nWaiting 15 seconds for cluster-wide indexing...")
        time.sleep(15)
        metrics.end_phase("Indexing Wait")
        
        # Step 5: Test global vector search (via node 1)
        print("\nStarting global search performance testing (via Node 1)...")
        primary_client = LongbowClient(LONGBOW_HOSTS[0])
        results_summary, all_latencies = test_vector_search(
            primary_client, dataset_name, model, TEST_QUERIES, k=SEARCH_K, num_iterations=NUM_SEARCH_ITERATIONS
        )
        
        # Step 6: Print reports
        print_summary(results_summary, all_latencies)
        metrics.report()
        
        print("\nDistributed test completed successfully!")
        return 0
        
    except Exception as e:
        print(f"\nTest failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
