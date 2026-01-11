import sys
import logging
import pandas as pd
import numpy as np
import dask.dataframe as dd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("verify_sdk")

try:
    from longbow import LongbowClient
    logger.info("Successfully imported longbow package.")
except ImportError as e:
    logger.error(f"Failed to import longbow: {e}")
    sys.exit(1)

def verify_dask_ingestion():
    logger.info("Verifying Dask ingestion logic...")
    
    # Create synthetic data
    rows = 100
    dim = 4
    df = pd.DataFrame({
        "id": range(rows),
        "vector": [np.random.rand(dim).astype(np.float32).tolist() for _ in range(rows)],
        "timestamp": pd.Timestamp.now()
    })
    ddf = dd.from_pandas(df, npartitions=2)
    
    # Mock upload by intercepting the client (since we might not have a running server or want to isolate)
    # Actually, let's just use the ingest helper directly to verify conversion logic
    from longbow.ingest import to_arrow_table
    # import pyarrow as pa
    
    try:
        # Test converting pandas
        tbl = to_arrow_table(df)
        logger.info(f"Pandas -> Arrow conversion successful: {tbl.num_rows} rows")
        
        # Test converting Dask (should compute to pandas if passed to helper, 
        # but Client handles the partitions. Let's verify helper handles compute())
        tbl_dask = to_arrow_table(ddf)
        logger.info(f"Dask -> Arrow conversion successful: {tbl_dask.num_rows} rows")
        
    except Exception as e:
        logger.error(f"Ingestion verification failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def verify_client_connectivity():
    logger.info("Verifying Client connectivity (assuming server at localhost:3000)...")
    client = LongbowClient(uri="grpc://localhost:3000")
    try:
        # Try to connect (FlightClient init doesn't always dial immediately, but list_flights does)
        client.connect()
        namespaces = client.list_namespaces()
        logger.info(f"Connected! Found namespaces: {namespaces}")
    except Exception as e:
        logger.warning(f"Connection test failed (Server might be down, expected): {e}")

if __name__ == "__main__":
    verify_dask_ingestion()
    verify_client_connectivity()
    logger.info("SDK verification completed.")
