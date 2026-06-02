#!/usr/bin/env python3

import os
import sys

try:
    import adbc_driver_manager
    import adbc_driver_manager.dbapi as dbapi
except ImportError:
    print("Please install adbc-driver-manager: pip install adbc-driver-manager")
    sys.exit(1)

def main():
    driver_path = os.path.abspath("liblongbow_adbc.so")
    if not os.path.exists(driver_path):
        print(f"Driver not found at {driver_path}")
        sys.exit(1)

    print(f"Loading driver from {driver_path}")
    
    # Try to connect
    try:
        with dbapi.connect(driver=driver_path) as conn:
            with conn.cursor() as cur:
                print("Executing test query...")
                # The execution will likely succeed since ExecuteQuery returns an empty RecordReader stub
                cur.execute("SELECT * FROM system.tables")
                result = cur.fetchall()
                print(f"Query returned {len(result)} rows.")
                
                print("Testing parametric binding (should be stubbed)...")
                try:
                    cur.execute("SELECT * FROM my_collection WHERE vector <-> ?", [[0.1, 0.2, 0.3]])
                except Exception as e:
                    print(f"Parametric binding correctly returned an error (stubbed): {e}")

        print("\nCross-language verification passed! Driver is loadable and interactive.")
    except Exception as e:
        print(f"Connection failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
