package main

import (
	"encoding/json"
	"net/http"

	"github.com/23skdu/longbow/internal/store"
	"github.com/rs/zerolog"
)

type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type DatasetInfo struct {
	Name        string `json:"name"`
	RecordCount int64  `json:"record_count"`
	VectorSize  int    `json:"vector_size"`
	Status      string `json:"status"`
	MemoryBytes int64  `json:"memory_bytes"`
	Dimensions  int    `json:"dimensions"`
}

type SearchRequest struct {
	Dataset string    `json:"dataset"`
	Query   []float32 `json:"query"`
	K       int       `json:"k"`
	Filter  string    `json:"filter,omitempty"`
}

type SearchResponse struct {
	Results []store.SearchResult `json:"results"`
	TookMs  int64                `json:"took_ms"`
}

type APIHandler struct {
	vs     *store.VectorStore
	logger zerolog.Logger
}

func NewAPIHandler(vs *store.VectorStore, logger zerolog.Logger) *APIHandler {
	return &APIHandler{
		vs:     vs,
		logger: logger,
	}
}

func (h *APIHandler) HandleListDatasets(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var dsList []DatasetInfo
	h.vs.IterateDatasets(func(name string, ds *store.Dataset) {
		dim := 0
		if ds.Schema != nil && len(ds.Schema.Fields()) > 0 {
			if fl, ok := ds.Schema.Fields()[0].Type.(interface{ Len() int }); ok {
				dim = fl.Len()
			}
		}
		info := DatasetInfo{
			Name:        name,
			RecordCount: int64(len(ds.Records)),
			VectorSize:  dim,
			Status:      "active",
			MemoryBytes: ds.SizeBytes.Load(),
			Dimensions:  ds.Index.Len(),
		}
		dsList = append(dsList, info)
	})

	resp := APIResponse{
		Success: true,
		Data:    dsList,
	}
	json.NewEncoder(w).Encode(resp)
}

func (h *APIHandler) HandleGetDataset(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	name := r.URL.Query().Get("name")
	if name == "" {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "name parameter required",
		})
		return
	}

	var foundDS *store.Dataset
	h.vs.IterateDatasets(func(dsName string, ds *store.Dataset) {
		if dsName == name {
			foundDS = ds
		}
	})

	if foundDS == nil {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "dataset not found",
		})
		return
	}

	info := DatasetInfo{
		Name:        name,
		RecordCount: int64(len(foundDS.Records)),
		VectorSize:  0,
		Status:      "active",
		MemoryBytes: foundDS.SizeBytes.Load(),
		Dimensions:  foundDS.Index.Len(),
	}

	json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Data:    info,
	})
}

func (h *APIHandler) HandleSearch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "POST required",
		})
		return
	}

	var req SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "invalid request body",
		})
		return
	}

	if req.Dataset == "" {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "dataset required",
		})
		return
	}

	if len(req.Query) == 0 {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "query vector required",
		})
		return
	}

	if req.K <= 0 {
		req.K = 10
	}

	var foundDS *store.Dataset
	h.vs.IterateDatasets(func(dsName string, ds *store.Dataset) {
		if dsName == req.Dataset {
			foundDS = ds
		}
	})

	if foundDS == nil {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "dataset not found",
		})
		return
	}

	results, err := foundDS.Index.SearchVectors(r.Context(), req.Query, req.K, nil, store.SearchOptions{})
	if err != nil {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   err.Error(),
		})
		return
	}

	resp := APIResponse{
		Success: true,
		Data: SearchResponse{
			Results: results,
			TookMs:  0,
		},
	}
	json.NewEncoder(w).Encode(resp)
}

func (h *APIHandler) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var datasetCount int
	h.vs.IterateDatasets(func(_ string, _ *store.Dataset) {
		datasetCount++
	})

	health := map[string]interface{}{
		"status":       "healthy",
		"datasets":     datasetCount,
		"memory_usage": 0,
	}

	json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Data:    health,
	})
}

func (h *APIHandler) HandleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var datasetCount int
	h.vs.IterateDatasets(func(_ string, _ *store.Dataset) {
		datasetCount++
	})

	metrics := map[string]interface{}{
		"current_memory": 0,
		"peak_memory":    0,
		"dataset_count":  datasetCount,
	}

	json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Data:    metrics,
	})
}

func SetupAPIEndpoints(mux *http.ServeMux, vs *store.VectorStore, logger zerolog.Logger) {
	handler := NewAPIHandler(vs, logger)

	mux.HandleFunc("/api/health", handler.HandleHealth)
	mux.HandleFunc("/api/metrics", handler.HandleMetrics)
	mux.HandleFunc("/api/datasets", handler.HandleListDatasets)
	mux.HandleFunc("/api/dataset", handler.HandleGetDataset)
	mux.HandleFunc("/api/search", handler.HandleSearch)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" || r.URL.Path == "/index.html" {
			w.Header().Set("Content-Type", "text/html")
			w.Write([]byte(`<!DOCTYPE html>
<html>
<head>
    <title>Longbow Vector Store</title>
    <style>
        body { font-family: system-ui, sans-serif; margin: 40px; background: #1a1a2e; color: #eee; }
        h1 { color: #00d9ff; }
        .card { background: #16213e; padding: 20px; border-radius: 8px; margin: 10px 0; }
        button { background: #00d9ff; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; }
        input { padding: 8px; border-radius: 4px; border: 1px solid #333; background: #0f3460; color: #fff; }
    </style>
</head>
<body>
    <h1>Longbow Vector Store</h1>
    <div class="card">
        <h2>Quick Actions</h2>
        <button onclick="fetchDatasets()">Refresh Datasets</button>
        <button onclick="checkHealth()">Health Check</button>
    </div>
    <div id="output"></div>
    <script>
        async function fetchDatasets() {
            const res = await fetch('/api/datasets');
            const data = await res.json();
            document.getElementById('output').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
        }
        async function checkHealth() {
            const res = await fetch('/api/health');
            const data = await res.json();
            document.getElementById('output').innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
        }
    </script>
</body>
</html>`))
		}
	})
}
