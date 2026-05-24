import re

with open('internal/store/index/navigation.go', 'r') as f:
    content = f.read()

# Define the boundaries of what to extract.
# I'll manually split by looking for the start of `type parallelSearchHostF32` and `func (h *ArrowHNSW) SearchForParallel` etc.
# Actually, since it's 1400 lines, I'll extract these chunks by matching functions.
