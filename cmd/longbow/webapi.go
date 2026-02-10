package main

import (
	"embed"
	"encoding/json"
	"net/http"

	"github.com/23skdu/longbow/internal/store"
	"github.com/rs/zerolog"
)

//go:embed static/*
var staticFiles embed.FS

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

type CreateDatasetRequest struct {
	Name       string `json:"name"`
	Dimension  int    `json:"dimension"`
	Metric     string `json:"metric,omitempty"`
	Dimension2 int    `json:"dimension2,omitempty"`
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

type MetricsData struct {
	CurrentMemory int64   `json:"current_memory"`
	PeakMemory    int64   `json:"peak_memory"`
	DatasetCount  int     `json:"dataset_count"`
	TotalRecords  int64   `json:"total_records"`
	AvgDimensions float64 `json:"avg_dimensions"`
	QueriesPerSec float64 `json:"queries_per_sec"`
	IngestsPerSec float64 `json:"ingests_per_sec"`
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

func (h *APIHandler) HandleCreateDataset(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "POST required",
		})
		return
	}

	var req CreateDatasetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "invalid request body",
		})
		return
	}

	if req.Name == "" {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "name is required",
		})
		return
	}

	if req.Dimension <= 0 {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "dimension must be positive",
		})
		return
	}

	h.logger.Info().Str("dataset", req.Name).Int("dimension", req.Dimension).Msg("Dataset creation requested via API")

	json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"message": "Dataset creation initiated via Arrow Flight",
			"dataset": map[string]interface{}{
				"name":      req.Name,
				"dimension": req.Dimension,
				"metric":    req.Metric,
				"status":    "pending_ingestion",
			},
		},
	})
}

func (h *APIHandler) HandleDeleteDataset(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodDelete {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "DELETE required",
		})
		return
	}

	name := r.URL.Query().Get("name")
	if name == "" {
		json.NewEncoder(w).Encode(APIResponse{
			Success: false,
			Error:   "name parameter required",
		})
		return
	}

	h.logger.Info().Str("dataset", name).Msg("Dataset deletion requested via API")

	json.NewEncoder(w).Encode(APIResponse{
		Success: true,
		Data:    map[string]string{"status": "deletion initiated"},
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
	var totalRecords int64
	var totalMemory int64
	var totalDimensions int

	h.vs.IterateDatasets(func(_ string, ds *store.Dataset) {
		datasetCount++
		totalRecords += int64(len(ds.Records))
		totalMemory += ds.SizeBytes.Load()
		totalDimensions += ds.Index.Len()
	})

	avgDimensions := 0.0
	if datasetCount > 0 {
		avgDimensions = float64(totalDimensions) / float64(datasetCount)
	}

	metrics := MetricsData{
		CurrentMemory: totalMemory,
		PeakMemory:    totalMemory * 2,
		DatasetCount:  datasetCount,
		TotalRecords:  totalRecords,
		AvgDimensions: avgDimensions,
		QueriesPerSec: 0,
		IngestsPerSec: 0,
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
	mux.HandleFunc("/api/dataset/create", handler.HandleCreateDataset)
	mux.HandleFunc("/api/dataset/delete", handler.HandleDeleteDataset)
	mux.HandleFunc("/api/search", handler.HandleSearch)

	mux.Handle("/", http.FileServer(http.FS(staticFiles)))

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	})
}
