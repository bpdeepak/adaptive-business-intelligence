package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"abi/internal/telemetry"
)

func newSvc(client *AgentClient) (*Service, *telemetry.Registry) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := telemetry.NewRegistry()
	svc := NewService(client, log)
	svc.Instrument(reg)
	return svc, reg
}

func TestClientQueryParsesFullContract(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/query" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		var in struct {
			Question string `json:"question"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if in.Question != "how many orders?" {
			t.Fatalf("question = %q", in.Question)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"question":  in.Question,
			"answer":    "The dataset has 99441 orders.",
			"grounded":  true,
			"refused":   false,
			"revisions": 0,
			"turns":     2,
			"tool_uses": []map[string]any{{"tool": "batch_overview", "params": map[string]any{}, "label": "batch", "note": ""}},
			"sources":   []string{"batch"},
		})
	}))
	defer sidecar.Close()

	c := NewAgentClient(sidecar.URL)
	resp, err := c.Query(context.Background(), "how many orders?")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if !resp.Grounded || resp.Refused || resp.Answer != "The dataset has 99441 orders." {
		t.Fatalf("contract mismatch: %+v", resp)
	}
	if len(resp.ToolUses) != 1 || resp.ToolUses[0].Label != "batch" {
		t.Fatalf("tool_uses mismatch: %+v", resp.ToolUses)
	}
	if len(resp.Sources) != 1 || resp.Sources[0] != "batch" {
		t.Fatalf("sources mismatch: %+v", resp.Sources)
	}
}

func TestClientHealth(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer sidecar.Close()
	if err := NewAgentClient(sidecar.URL).Health(context.Background()); err != nil {
		t.Fatalf("health: %v", err)
	}
}

func TestNilClient(t *testing.T) {
	if NewAgentClient("  ") != nil {
		t.Fatal("empty URL must yield nil client")
	}
	if NewAgentClient("http://x:1").Health(context.Background()) == nil {
		t.Fatal("nil client health must fail")
	}
}

func TestServiceQueryDisabledReturns503(t *testing.T) {
	svc, reg := newSvc(nil)
	mux := http.NewServeMux()
	svc.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/agent/query",
		bytes.NewBufferString(`{"question":"hi"}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if reg.Counter("abi_agent_errors_total", "").Value() != 1 {
		t.Fatalf("error counter not incremented")
	}
}

func TestServiceQueryContractPassthrough(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"question": "q", "answer": "a", "grounded": false, "refused": true,
			"revisions": 1, "turns": 3, "tool_uses": []any{}, "sources": []string{"batch"},
		})
	}))
	defer sidecar.Close()

	svc, reg := newSvc(NewAgentClient(sidecar.URL))
	mux := http.NewServeMux()
	svc.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/v1/agent/query",
		bytes.NewBufferString(`{"question":"q"}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	data, _ := body["data"].(map[string]any)
	if data["answer"] != "a" {
		t.Fatalf("answer not passed through verbatim: %v", data["answer"])
	}
	if data["refused"] != true {
		t.Fatalf("refused flag not passed through: %v", data)
	}
	if reg.Counter("abi_agent_query_total", "").Value() != 1 {
		t.Fatalf("query counter not incremented")
	}
	if reg.Counter("abi_agent_grounding_refused_total", "").Value() != 1 {
		t.Fatalf("refusal counter not incremented")
	}
	if reg.Counter("abi_agent_turns_total", "").Value() != 3 {
		t.Fatalf("turns counter not equal to 3")
	}
}

func TestServiceRefreshGauges(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer sidecar.Close()

	svc, reg := newSvc(NewAgentClient(sidecar.URL))
	svc.RefreshGauges(context.Background())
	if reg.Gauge("abi_agent_up", "").Value() != 1 {
		t.Fatalf("abi_agent_up should be 1 when sidecar healthy")
	}

	svcDown, regDown := newSvc(nil)
	svcDown.RefreshGauges(context.Background())
	if regDown.Gauge("abi_agent_up", "").Value() != 0 {
		t.Fatalf("abi_agent_up should be 0 when disabled")
	}
}
