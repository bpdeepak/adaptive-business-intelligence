package predict

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ScoreClient talks to the Python model sidecar (ml/serve.py). It is an
// optional dependency: when the sidecar is down, the read APIs still serve the
// persisted predictions and POST /score fails with a clear 503.
type ScoreClient struct {
	baseURL string
	http    *http.Client
}

// NewScoreClient builds a client for a sidecar base URL (e.g.
// http://127.0.0.1:8093). An empty URL yields nil, which disables live scoring.
func NewScoreClient(baseURL string) *ScoreClient {
	if strings.TrimSpace(baseURL) == "" {
		return nil
	}
	return &ScoreClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 5 * time.Second},
	}
}

// ScoreResponse is the sidecar's reply for one scoring request.
type ScoreResponse struct {
	Status      string          `json:"status"`
	Model       string          `json:"model"`
	Version     string          `json:"version"`
	Task        string          `json:"task"`
	Grain       string          `json:"grain"`
	Prediction  float64         `json:"prediction"`
	Confidence  float64         `json:"confidence"`
	Explanation json.RawMessage `json:"explanation"`
	Error       string          `json:"error"`
}

// Score sends one feature vector to the sidecar for the named model.
func (c *ScoreClient) Score(ctx context.Context, name string, features map[string]float64) (*ScoreResponse, error) {
	body, err := json.Marshal(map[string]any{"model": name, "features": features})
	if err != nil {
		return nil, fmt.Errorf("marshal score request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/score", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("score request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("model sidecar unreachable: %w", err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	var out ScoreResponse
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, fmt.Errorf("score response %d: %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		if out.Error != "" {
			return nil, fmt.Errorf("sidecar: %s", out.Error)
		}
		return nil, fmt.Errorf("sidecar returned %d", resp.StatusCode)
	}
	return &out, nil
}

// Health probes the sidecar's /health endpoint. A nil client is reported as
// unhealthy (scoring disabled).
func (c *ScoreClient) Health(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("scoring disabled")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("sidecar health %d", resp.StatusCode)
	}
	return nil
}