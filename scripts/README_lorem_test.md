# Longbow Lorem Ipsum Vector Search Test

## Overview

Test script that generates Lorem Ipsum text, creates embeddings, and validates Longbow's vector search functionality with keyword relevancy scoring.

## Requirements

```bash
pip install pyarrow sentence-transformers lorem-text numpy
```

## Usage

### 1. Start Longbow Server

```bash
./longbow-server
```

### 2. Run Test Script

```bash
python3 scripts/lorem_vector_test.py
```

## What It Does

1. **Generates 100 Lorem Ipsum Blurbs**
   - Each blurb contains 2-5 sentences
   - Tagged with topics: technology, business, health, education, environment, etc.
   - Labeled as `embed_1` through `embed_10` (cycling)

2. **Creates Vector Embeddings**
   - Uses `all-MiniLM-L6-v2` model (384 dimensions)
   - Fast and accurate for semantic search
   - Converts text to dense vectors

3. **Uploads to Longbow**
   - Creates Arrow table with schema: id, text, topic, label, vector
   - Uses DoPut to upload data
   - Dataset name: `lorem_test_<timestamp>`

4. **Performs Vector Search**
   - Tests 5 sample queries:
     - "technology and innovation"
     - "business strategy"
     - "health and wellness"
     - "education learning"
     - "environmental sustainability"
   - Returns top 5 results for each query
   - Calculates cosine similarity scores

5. **Displays Results**
   - Shows relevancy scores (0.0 to 1.0)
   - Displays matched text snippets
   - Prints summary statistics

## Expected Output

```
================================================================================
LONGBOW VECTOR SEARCH TEST
================================================================================

Generating 100 Lorem Ipsum blurbs...
Generated 100 blurbs

Creating embeddings using all-MiniLM-L6-v2...
Created 100 embeddings of dimension 384

Creating Arrow table...
Created Arrow table: 100 rows, 5 columns

Connecting to Longbow at grpc://0.0.0.0:3000...
Connected to Longbow at grpc://0.0.0.0:3000

Uploading to dataset 'lorem_test_1735194000'...
Uploaded 100 rows to 'lorem_test_1735194000'

Waiting 5 seconds for indexing...

Testing Vector Search (k=5)...
================================================================================

[Query 1] 'technology and innovation'
--------------------------------------------------------------------------------
  1. [Score: 0.8234] ID=42, Label=embed_3, Topic=technology
     Text: Technology: Lorem ipsum dolor sit amet, consectetur adipiscing...
  2. [Score: 0.7891] ID=12, Label=embed_3, Topic=technology
     Text: Technology: Sed do eiusmod tempor incididunt ut labore...
  ...

================================================================================
TEST SUMMARY
================================================================================
1. Query: 'technology and innovation'
   Top Score: 0.8234
   Top Topic: technology

2. Query: 'business strategy'
   Top Score: 0.7956
   Top Topic: business
...

Average Top Score: 0.7845
================================================================================

Test completed successfully!
```

## Customization

Edit the script to customize:

```python
# Configuration
NUM_BLURBS = 100  # Number of text blurbs
SEARCH_K = 5      # Top-K results
EMBEDDING_MODEL = "all-MiniLM-L6-v2"  # Embedding model

# Test queries
TEST_QUERIES = [
    "your custom query here",
    ...
]
```

## Troubleshooting

**Server not running:**

```
Test failed: Flight returned unavailable error
```

Solution: Start `./longbow-server` first

**Missing dependencies:**

```
ModuleNotFoundError: No module named 'sentence_transformers'
```

Solution: `pip install sentence-transformers lorem-text`

**Out of memory:**
Reduce `NUM_BLURBS` or use smaller embedding model

## Performance Notes

- Embedding generation: ~2-5 seconds for 100 blurbs
- Upload time: <1 second
- Search time: <100ms per query
- Total test time: ~15-20 seconds
