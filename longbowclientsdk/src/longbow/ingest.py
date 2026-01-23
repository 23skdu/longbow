import pyarrow as pa
import pandas as pd
import numpy as np
from typing import Union, List, Dict, Any, Optional

def _infer_schema(dim: int, with_meta: bool = False) -> pa.Schema:
    """Creates standard Longbow Arrow schema."""
    fields = [
        pa.field("id", pa.int64()),
        pa.field("vector", pa.list_(pa.float32(), dim)),
        pa.field("timestamp", pa.timestamp("ns")),
    ]
    if with_meta:
        fields.append(pa.field("metadata", pa.string())) # JSON serialized
    return pa.schema(fields)

def to_arrow_table(
    data: Union[List[Dict[str, Any]], pd.DataFrame, Dict[str, Any], pa.Table],
    dim: Optional[int] = None
) -> pa.Table:
    """
    Converts various input formats to a PyArrow Table suitable for Longbow ingestion.
    
    Args:
        data: Input data (List of Dicts, Pandas DataFrame).
        dim: Vector dimension (optional if can be inferred).
        
    Returns:
        pa.Table: Arrow table ready for flight.
    """
    
    # 0. Handle Arrow Table (Pass-through)
    if isinstance(data, pa.Table):
        return data

    # 2. Handle List of Dicts
    if isinstance(data, list):
        if not data:
            raise ValueError("Input list is empty")
        
        # Check first item to guess structure or use dim
        first = data[0]
        if 'vector' in first:
            if dim is None:
                dim = len(first['vector'])
        
        # Convert to Pandas first for easier Arrow conversion (handling missing cols)
        df = pd.DataFrame(data)
        return to_arrow_table(df, dim)

    # 3. Handle Pandas DataFrame
    if isinstance(data, pd.DataFrame):
        # Ensure required columns
        if 'id' not in data.columns:
            # Auto-generate IDs if missing? Or raise error. Longbow requires explicit IDs currently.
            # Let's fallback to index if it's integer
            if pd.api.types.is_integer_dtype(data.index):
                data['id'] = data.index
            else:
                 raise ValueError("Data must have an 'id' column or integer index")
        
        if 'vector' not in data.columns:
             raise ValueError("Data must have a 'vector' column")

        # Ensure vector column is list of floats
        # Pandas often stores lists as objects.
        
        # Infer dimension if needed
        if dim is None:
            # Peek at first vector
            first_vec = data['vector'].iloc[0]
            if isinstance(first_vec, (list, np.ndarray)):
                dim = len(first_vec)
            elif isinstance(first_vec, str):
                try:
                    import json
                    parsed = json.loads(first_vec)
                    if isinstance(parsed, list):
                        dim = len(parsed)
                    else:
                         raise ValueError(f"String vector is not a list: {first_vec}")
                except Exception:
                     raise ValueError(f"Could not parse string vector: {first_vec}")
            else:
                raise ValueError(f"Could not infer vector dimension. First element type: {type(first_vec)}, Value: {first_vec}")
        
        # Timestamp handling
        if 'timestamp' not in data.columns:
            data['timestamp'] = pd.Timestamp.now()
            
        # Metadata handling
        # If there are extra columns, assume they are metadata?
        # Longbow expects a single "metadata" column or specific schema.
        # The schema expected is: id, vector, timestamp, (optional) metadata [string or map?]
        # The go server expects "meta" as string? Wait, let's check ops_test.py
        # ops_test.py uses: id, vector, timestamp, meta (string)
        # We need to serialize dict to string for 'meta' column if provided as dict
        
        # If 'metadata' column exists
        if 'metadata' in data.columns or 'meta' in data.columns:
            # meta_col = 'metadata' if 'metadata' in data.columns else 'meta'
            # Serialize if needed
            # ... implementation detail ...
            pass
            
        # Construct Arrow Arrays efficiently
        # Vector column need special handling to FixedSizeList
        
        # Fast path using stack for vectors
        # Check if we need to parse strings
        vec_sample = data['vector'].iloc[0]
        if isinstance(vec_sample, str):
             import json
             # Apply parsing to the whole column. 
             data['vector'] = data['vector'].apply(json.loads)

        vecs = np.stack(data['vector'].values) # matrix [N, dim]
        
        # Support various dtypes without forced float32 cast if compatible
        # Supported: float32, float64, int8/16/32/64, complex64/128
        # We only cast if it's an unsupported type or 'object' (e.g. list of floats)
        
        orig_dtype = str(vecs.dtype)
        is_complex = False
        
        if vecs.dtype == np.complex64:
            # view as float32: (N, dim) -> (N, 2*dim)
            vecs = vecs.view(np.float32).reshape(vecs.shape[0], -1)
            is_complex = True
        elif vecs.dtype == np.complex128:
            # view as float64: (N, dim) -> (N, 2*dim)
            vecs = vecs.view(np.float64).reshape(vecs.shape[0], -1)
            is_complex = True
        elif vecs.dtype == np.object_:
            vecs = vecs.astype(np.float32)
            
        # Update dim if it's complex (physical dim is 2x logical dim)
        current_dim = vecs.shape[1]
            
        # Create FixedSizeListArray
        flat_vecs = vecs.flatten()
        # Create the value array with appropriate type
        value_arr = pa.array(flat_vecs)
        arrow_vecs = pa.FixedSizeListArray.from_arrays(value_arr, current_dim)
        
        # arrow_ids = pa.array(data['id'].values, type=pa.int64())
        # Allow string IDs for test compatibility
        arrow_ids = pa.array(data['id'].values)
        arrow_ts = pa.array(data['timestamp'].values, type=pa.timestamp("ns"))
        
        arrays = [arrow_ids, arrow_vecs, arrow_ts]
        names = ['id', 'vector', 'timestamp']
        
        # Handle metadata/extra columns
        reserved = {'id', 'vector', 'timestamp'}
        for col in data.columns:
            if col in reserved:
                continue
            
            # Simple type mapping: Convert to string mostly, or let Arrow infer
            # For this demo/SDK, we rely on pyarrow inference but careful with object types
            col_data = data[col]
            if pd.api.types.is_object_dtype(col_data) and not isinstance(col_data.iloc[0], (str, bytes)):
                 # JSON serialize dictionaries/lists if found in object column
                 import json
                 col_data = col_data.apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else str(x))
                 
            arrays.append(pa.array(col_data))
            names.append(col)
             
        table = pa.Table.from_arrays(arrays, names=names)
        
        # Add metadata for type inference
        meta = table.schema.metadata or {}
        typed_meta = meta.copy()
        typed_meta[b"longbow.vector_type"] = orig_dtype.encode()
        if is_complex:
             typed_meta[b"longbow.complex"] = b"true"
        
        return table.replace_schema_metadata(typed_meta)

    raise TypeError(f"Unsupported data type: {type(data)}")
