import argparse
import json
import random
import time
import pyarrow.flight as flight

def get_client(uri):
    return flight.FlightClient(uri)

def add_edges(client, dataset, count):
    print(f"Seeding {count} edges to dataset '{dataset}'...")
    start_time = time.time()
    
    # Simple chain: 0->1, 1->2...
    # Also some random edges for complexity
    
    success = 0
    errors = 0
    
    for i in range(count):
        subject = i
        object_id = i + 1
        predicate = "knows"
        weight = 1.0
        
        req = {
            "dataset": dataset,
            "subject": subject,
            "predicate": predicate,
            "object": object_id,
            "weight": weight
        }
        
        try:
            action = flight.Action("add-edge", json.dumps(req).encode("utf-8"))
            results = list(client.do_action(action))
            success += 1
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"Error adding edge {i}: {e}")
                
        if (i+1) % 100 == 0:
            print(f"Added {i+1} edges...")

    duration = time.time() - start_time
    print(f"Seeding complete in {duration:.2f}s. Success: {success}, Errors: {errors}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--meta-uri", default="grpc://0.0.0.0:3001")
    args = parser.parse_args()
    
    client = get_client(args.meta_uri)
    add_edges(client, args.dataset, args.count)
