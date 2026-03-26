package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type CohereReranker struct {
	apiKey     string
	model      string
	httpClient *http.Client
}

type cohereRequest struct {
	Model           string   `json:"model"`
	Query           string   `json:"query"`
	Documents       []string `json:"documents"`
	ReturnDocuments bool     `json:"return_documents"`
}

type cohereResult struct {
	Index          int     `json:"index"`
	RelevanceScore float32 `json:"relevance_score"`
}

type cohereResponse struct {
	ID      string         `json:"id"`
	Results []cohereResult `json:"results"`
}

func NewCohereReranker(apiKey, model string) *CohereReranker {
	if model == "" {
		model = "rerank-english-v3.0"
	}
	return &CohereReranker{
		apiKey: apiKey,
		model:  model,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (c *CohereReranker) Score(query string, documents []string) ([]float32, error) {
	if len(documents) == 0 {
		return []float32{}, nil
	}

	reqBody := cohereRequest{
		Model:           c.model,
		Query:           query,
		Documents:       documents,
		ReturnDocuments: false,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cohere request: %w", err)
	}

	req, err := http.NewRequest("POST", "https://api.cohere.com/v1/rerank", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create cohere request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("cohere request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("cohere returned status %d: %s", resp.StatusCode, string(body))
	}

	var cohereResp cohereResponse
	if err := json.NewDecoder(resp.Body).Decode(&cohereResp); err != nil {
		return nil, fmt.Errorf("failed to decode cohere response: %w", err)
	}

	scores := make([]float32, len(documents))

	// Cohere returns a sorted list of results with their original indecies
	for _, res := range cohereResp.Results {
		if res.Index >= 0 && res.Index < len(documents) {
			scores[res.Index] = res.RelevanceScore
		}
	}

	return scores, nil
}

func (c *CohereReranker) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}
