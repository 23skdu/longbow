# Web UI Plan for Longbow

## Overview
Add a web-based management interface for Longbow vector store operations.

## Architecture

```
Web UI (Vue.js 3 + Tailwind CSS)
    ↓
HTTP API Server (Go)
    ↓
Longbow gRPC/Arrow Flight
```

## Features

### Dashboard
- Cluster overview (nodes, memory, storage)
- Real-time metrics (Prometheus integration)
- Health status indicators

### Dataset Management
- List/Create/Delete datasets
- View dataset statistics (size, vector count, dimensions)
- Search preview (input vectors, view results)

### Query Playground
- Vector search with live results
- Filter configuration
- Result visualization

### Monitoring
- Memory usage charts
- Query latency histograms
- Throughput metrics

## Implementation Steps

### Step 1: HTTP API Server ✅ COMPLETED
- Add HTTP endpoints alongside gRPC/Flight
- Endpoints: `/api/datasets`, `/api/search`, `/api/metrics`, `/api/health`
- File: `cmd/longbow/webapi.go`

### Step 2: Frontend Setup ✅ IN PROGRESS
- Vue.js 3 SPA with Composition API
- Tailwind CSS for styling (via CDN for simplicity)
- Embedded static files with `//go:embed`
- File: `cmd/longbow/static/index.html`

### Step 3: API Integration ⏳ PENDING
- Fetch datasets from store
- Execute search queries via Arrow Flight
- Display Prometheus metrics

### Step 4: Polish ⏳ PENDING
- Dark mode support
- Mobile responsive
- Error handling

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | /api/datasets | List all datasets |
| GET | /api/dataset?name=XXX | Get dataset details |
| POST | /api/search | Execute vector search |
| GET | /api/health | Health check |
| GET | /api/metrics | Runtime metrics |

## Frontend Components

```
static/
└── index.html          # Vue.js 3 SPA with Tailwind CSS
```

## Usage

```bash
# Start Longbow with Web UI
./longbow

# Access Web UI
http://localhost:8080

# Access API directly
http://localhost:8080/api/datasets
http://localhost:8080/api/health
```

## Configuration

| Environment Variable | Default | Description |
|----------------------|---------|-------------|
| WEBUI_ADDR | 0.0.0.0:8080 | Web UI server address |
| LISTEN_ADDR | 0.0.0.0:3000 | gRPC/Flight server |

## Next Steps

1. ✅ Create HTTP API handlers
2. ✅ Set up frontend framework (Vue.js 3 + Tailwind)
3. ⏳ Implement dataset listing
4. ⏳ Build search playground
5. ⏳ Add create/delete dataset functionality
6. ⏳ Add memory charts and metrics visualization
7. ⏳ Mobile responsive design
8. ⏳ Error handling and loading states
