import json

def format_table(data, dims):
    # Header
    print(f"| Type       |   Dim | DoPut (MB/s) | DoGet (MB/s) | Dense QPS | Status     |")
    print(f"|:-----------|------:|-------------:|-------------:|----------:|:-----------|")
    
    # Filter and sort
    filtered = [r for r in data if r['dim'] in dims]
    # Sort order: int8, int16, int32, int64, float32, float16, float64, complex128
    order = ["int8", "int16", "int32", "int64", "float32", "float16", "float64", "complex64", "complex128"]
    
    # helper to get index
    def get_order(dtype):
        try:
            return order.index(dtype)
        except:
            return 99

    filtered.sort(key=lambda x: (x['dim'], get_order(x['dtype'])))

    for r in filtered:
        dtype = r['dtype']
        dim = r['dim']
        put = r['put_mb']
        get = r['get_mb']
        qps = r['search_qps']
        status = r['status']
        
        # Bold first col
        dtype_str = f"**{dtype}**"
        
        print(f"| {dtype_str:<10} | {dim:>5} | {put:>12.2f} | {get:>12.2f} | {qps:>10.1f} | **{status}** |")

def main():
    try:
        with open("matrix_results.json", "r") as f:
            data = json.load(f)
    except FileNotFoundError:
        print("matrix_results.json not found")
        return

    print("### Data Type Matrix (Up to 50k Vectors)\n")
    format_table(data, [128, 384])
    
    print("\n### High-Dimensional Matrix (OpenAI Models)\n")
    format_table(data, [1536, 3072])

if __name__ == "__main__":
    main()
