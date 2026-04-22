import numpy as np
import time

count = 10000
dim = 384

data = np.random.rand(count, dim).astype(np.float32)

start = time.time()
l = data.tolist()
print(f"tolist() took {(time.time() - start)*1000:.2f}ms")

start = time.time()
# simulate list of dicts creation
records = [{"id": str(i), "vector": l[i]} for i in range(count)]
print(f"record creation took {(time.time() - start)*1000:.2f}ms")
