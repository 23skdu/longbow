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

### Step 3: API Integration ✅ COMPLETED
- Fetch datasets from store (`/api/datasets`)
- Execute search queries via VectorStore (`/api/search`)
- Display Prometheus metrics (`/api/metrics`)
- Create dataset endpoint (`/api/dataset/create`)
- Delete dataset endpoint (`/api/dataset/delete`)
- Enhanced metrics with totals and averages

### Step 4: Polish ✅ COMPLETED
- Dark mode support with theme toggle
- Mobile responsive design (grid adapts to screen size)
- Error handling with toast notifications
- Loading states and spinners
- Dataset creation modal with form validation
- Chart.js integration for memory/records visualization
- Keyboard shortcuts (Ctrl+R refresh, Ctrl+S search)
- Real-time metrics auto-refresh (5s interval)

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | /api/datasets | List all datasets |
| GET | /api/dataset?name=XXX | Get dataset details |
| POST | /api/dataset/create | Create new dataset |
| DELETE | /api/dataset/delete?name=XXX | Delete dataset |
| POST | /api/search | Execute vector search |
| GET | /api/health | Health check |
| GET | /api/metrics | Extended metrics |

## Frontend Components

```
static/
└── index.html          # Vue.js 3 SPA with Tailwind CSS + Chart.js
```

## Features Implemented

| Feature | Status |
|---------|--------|
| Dataset listing with stats | ✅ |
| Create dataset modal | ✅ |
| Search playground | ✅ |
| Memory visualization (Chart.js) | ✅ |
| Theme toggle (light/dark) | ✅ |
| Responsive mobile design | ✅ |
| Loading states & spinners | ✅ |
| Toast notifications | ✅ |
| Keyboard shortcuts | ✅ |
| Auto-refresh metrics | ✅ |

## Usage

```bash
# Start Longbow
./longbow

# Access Web UI
http://localhost:8080

# Keyboard Shortcuts
# Ctrl+R - Refresh all data
# Ctrl+S - Execute search
```

## Configuration

| Environment Variable | Default | Description |
|----------------------|---------|-------------|
| WEBUI_ADDR | 0.0.0.0:8080 | Web UI server address |
| LISTEN_ADDR | 0.0.0.0:3000 | gRPC/Flight server |

## Web UI Roadmap (Future Enhancements)

1. Add dataset detail view with vector preview
2. Add vector ingestion UI (upload CSV/JSON)
3. Add Prometheus query builder for custom charts
4. Add cluster health monitoring
5. Add user authentication
6. Add audit logging viewer
7. Add performance benchmarking UI
8. Multi-language support
