package agent

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

// AgentClient talks to the Python NL→BI agent sidecar (ml/agent/server.py).
// It is an optional dependency: when the sidecar is down the read/query API
// fails with a clear 503 and the /models/health-style gauge flips to 0.
type AgentClient struct {
	baseURL string
	http    *http.Client
}

// NewAgentClient builds a client for a sidecar base URL (e.g.
// http://127.0.0.1:8094). An empty URL yields nil, which disables the agent.
func NewAgentClient(baseURL string) *AgentClient {
	if strings.TrimSpace(baseURL) == "" {
		return nil
	}
	return &AgentClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		// The agent runs a bounded tool loop (≤1 revision, fixed tool set), so
		// a single answer can legitimately take a few seconds; stay generous.
		http: &http.Client{Timeout: 60 * time.Second},
	}
}

// ToolUse records one tool invocation the agent made while answering.
type ToolUse struct {
	Tool   string         `json:"tool"`
	Params map[string]any `json:"params"`
	Label  string         `json:"label"`
	Note   string         `json:"note"`
}

// QueryResponse is the agent sidecar's full response contract for one question
// (mirrors ml/agent/agent.py). `grounded` and `refused` are mutually exclusive
// views of the same outcome: a refused answer is never grounded.
type QueryResponse struct {
	Question  string    `json:"question"`
	Answer    string    `json:"answer"`
	Grounded  bool      `json:"grounded"`
	Refused   bool      `json:"refused"`
	Revisions int       `json:"revisions"`
	Turns     int       `json:"turns"`
	ToolUses  []ToolUse `json:"tool_uses"`
	Sources   []string  `json:"sources"`
}

// Query sends one natural-language question to the agent sidecar and returns
// the full response contract, including provenance labels and tool uses.
func (c *AgentClient) Query(ctx context.Context, question string) (*QueryResponse, error) {
	if c == nil {
		return nil, fmt.Errorf("agent disabled (ABI_AGENT_URL empty)")
	}
	body, err := json.Marshal(map[string]string{"question": question})
	if err != nil {
		return nil, fmt.Errorf("marshal query request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/query", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("agent query request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("agent sidecar unreachable: %w", err)
	}
	defer resp.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	var out QueryResponse
	if err := json.Unmarshal(payload, &out); err != nil {
		return nil, fmt.Errorf("agent query response %d: %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("agent sidecar returned %d", resp.StatusCode)
	}
	if out.Answer == "" {
		return nil, fmt.Errorf("agent returned an empty answer")
	}
	return &out, nil
}

// Health probes the sidecar's /health endpoint. A nil client is reported as
// unhealthy (agent disabled).
func (c *AgentClient) Health(ctx context.Context) error {
	if c == nil {
		return fmt.Errorf("agent disabled")
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
		return fmt.Errorf("agent sidecar health %d", resp.StatusCode)
	}
	return nil
}
