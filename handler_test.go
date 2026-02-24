package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// ─── Fake ClickHouse HTTP server ────────────────────────────────────────────

type fakeCH struct {
	server       *httptest.Server
	mu           sync.Mutex
	pingOK       bool
	queryHandler func(sql string) (int, string)
}

func newFakeCH() *fakeCH {
	f := &fakeCH{pingOK: true}
	f.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Health check
		if r.URL.Path == "/ping" {
			f.mu.Lock()
			ok := f.pingOK
			f.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			fmt.Fprint(w, "Ok.\n")
			return
		}

		// Detect INSERT via query param — accept silently
		qParam := r.URL.Query().Get("query")
		if strings.HasPrefix(strings.TrimSpace(strings.ToUpper(qParam)), "INSERT") {
			w.WriteHeader(http.StatusOK)
			return
		}

		// SELECT queries come in the POST body
		body, _ := io.ReadAll(r.Body)
		sql := string(body)

		f.mu.Lock()
		handler := f.queryHandler
		f.mu.Unlock()

		if handler != nil {
			code, resp := handler(sql)
			w.WriteHeader(code)
			fmt.Fprint(w, resp)
			return
		}

		// Default: return empty result set
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[],"rows":0,"statistics":{"elapsed":0.001,"rows_read":0,"bytes_read":0}}`)
	}))
	return f
}

func (f *fakeCH) close() {
	f.server.Close()
}

func (f *fakeCH) setPingOK(ok bool) {
	f.mu.Lock()
	f.pingOK = ok
	f.mu.Unlock()
}

func (f *fakeCH) setQueryHandler(h func(sql string) (int, string)) {
	f.mu.Lock()
	f.queryHandler = h
	f.mu.Unlock()
}

// ─── Test setup ─────────────────────────────────────────────────────────────

func setupTestServer(authToken string) (*http.ServeMux, *fakeCH, func()) {
	fake := newFakeCH()
	ch := NewClickHouseClient(fake.server.URL, "default", "", "")
	mux := buildMux(ch, authToken)

	return mux, fake, func() {
		fake.close()
		resetBuffers()
	}
}

// ─── Auth middleware tests ──────────────────────────────────────────────────

func TestAuth_NoTokenConfigured(t *testing.T) {
	mux, _, cleanup := setupTestServer("")
	defer cleanup()

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestAuth_MissingHeader(t *testing.T) {
	mux, _, cleanup := setupTestServer("secret")
	defer cleanup()

	req := httptest.NewRequest("GET", "/v0/pipes/list_traces.json", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["error"] != "unauthorized" {
		t.Errorf("expected {\"error\":\"unauthorized\"}, got %v", body)
	}
}

func TestAuth_WrongToken(t *testing.T) {
	mux, _, cleanup := setupTestServer("secret")
	defer cleanup()

	req := httptest.NewRequest("GET", "/v0/pipes/list_traces.json", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", w.Code)
	}
}

func TestAuth_CorrectToken(t *testing.T) {
	mux, _, cleanup := setupTestServer("secret")
	defer cleanup()

	req := httptest.NewRequest("GET", "/health", nil)
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

// ─── Health endpoint tests ──────────────────────────────────────────────────

func TestHealth_Up(t *testing.T) {
	mux, _, cleanup := setupTestServer("")
	defer cleanup()

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("expected status=ok, got %v", body)
	}
}

func TestHealth_Down(t *testing.T) {
	mux, fake, cleanup := setupTestServer("")
	defer cleanup()

	fake.setPingOK(false)
	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "unhealthy") {
		t.Errorf("expected body to contain 'unhealthy', got %q", w.Body.String())
	}
}

// ─── Ingest endpoint tests ──────────────────────────────────────────────────

func TestIngest_MissingName(t *testing.T) {
	mux, _, cleanup := setupTestServer("")
	defer cleanup()

	req := httptest.NewRequest("POST", "/v0/events", strings.NewReader(`{"key":"val"}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestIngest_UnknownDatasource(t *testing.T) {
	mux, _, cleanup := setupTestServer("")
	defer cleanup()

	req := httptest.NewRequest("POST", "/v0/events?name=nonexistent", strings.NewReader(`{"key":"val"}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestIngest_ValidNDJSON(t *testing.T) {
	mux, _, cleanup := setupTestServer("")
	defer cleanup()

	ndjson := `{"OrgId":"org1","Body":"line1"}
{"OrgId":"org1","Body":"line2"}`
	req := httptest.NewRequest("POST", "/v0/events?name=logs", strings.NewReader(ndjson))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["successful_rows"] != float64(2) {
		t.Errorf("expected successful_rows=2, got %v", body["successful_rows"])
	}
	if body["quarantined_rows"] != float64(0) {
		t.Errorf("expected quarantined_rows=0, got %v", body["quarantined_rows"])
	}
}

func TestIngest_InvalidJSON(t *testing.T) {
	mux, _, cleanup := setupTestServer("")
	defer cleanup()

	req := httptest.NewRequest("POST", "/v0/events?name=logs", strings.NewReader(`{broken json`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestIngest_EmptyBody(t *testing.T) {
	mux, _, cleanup := setupTestServer("")
	defer cleanup()

	req := httptest.NewRequest("POST", "/v0/events?name=logs", strings.NewReader(""))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if body["successful_rows"] != float64(0) {
		t.Errorf("expected successful_rows=0, got %v", body["successful_rows"])
	}
}

func TestIngest_AllDatasources(t *testing.T) {
	mux, _, cleanup := setupTestServer("")
	defer cleanup()

	for dsName := range validDatasources {
		t.Run(dsName, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/v0/events?name="+dsName, strings.NewReader(`{"key":"val"}`))
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != http.StatusOK {
				t.Errorf("datasource %q: expected 200, got %d", dsName, w.Code)
			}
		})
	}
}

// ─── Pipe endpoint tests ────────────────────────────────────────────────────

func TestHandlePipe_UnknownPipe(t *testing.T) {
	mux, _, cleanup := setupTestServer("")
	defer cleanup()

	req := httptest.NewRequest("GET", "/v0/pipes/nonexistent.json", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}

func TestHandlePipe_TinybirdEnvelope(t *testing.T) {
	mux, fake, cleanup := setupTestServer("")
	defer cleanup()

	fake.setQueryHandler(func(sql string) (int, string) {
		return 200, `{"data":[{"traceId":"abc123","spanCount":5}],"rows":1,"statistics":{"elapsed":0.05,"rows_read":1000,"bytes_read":50000}}`
	})

	req := httptest.NewRequest("GET", "/v0/pipes/list_traces.json?org_id=test_org", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var envelope map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&envelope); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	// Verify Tinybird envelope keys
	for _, key := range []string{"data", "rows", "rows_before_limit_at_least", "statistics"} {
		if _, ok := envelope[key]; !ok {
			t.Errorf("missing key %q in Tinybird envelope", key)
		}
	}

	// Verify statistics shape
	stats, ok := envelope["statistics"].(map[string]interface{})
	if !ok {
		t.Fatal("statistics is not an object")
	}
	for _, key := range []string{"elapsed", "rows_read", "bytes_read"} {
		if _, ok := stats[key]; !ok {
			t.Errorf("missing statistics key %q", key)
		}
	}

	// Verify data
	data, ok := envelope["data"].([]interface{})
	if !ok {
		t.Fatal("data is not an array")
	}
	if len(data) != 1 {
		t.Errorf("expected 1 row, got %d", len(data))
	}
}

func TestHandlePipe_QueryParamsPassedToSQL(t *testing.T) {
	mux, fake, cleanup := setupTestServer("")
	defer cleanup()

	var capturedSQL string
	fake.setQueryHandler(func(sql string) (int, string) {
		capturedSQL = sql
		return 200, `{"data":[],"rows":0,"statistics":{"elapsed":0.001,"rows_read":0,"bytes_read":0}}`
	})

	req := httptest.NewRequest("GET", "/v0/pipes/list_traces.json?org_id=org_abc&service=payment-svc&limit=25", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if !strings.Contains(capturedSQL, "'org_abc'") {
		t.Errorf("expected SQL to contain 'org_abc', got:\n%s", capturedSQL)
	}
	if !strings.Contains(capturedSQL, "'payment-svc'") {
		t.Errorf("expected SQL to contain 'payment-svc', got:\n%s", capturedSQL)
	}
	if !strings.Contains(capturedSQL, "LIMIT 25") {
		t.Errorf("expected SQL to contain 'LIMIT 25', got:\n%s", capturedSQL)
	}
}

func TestHandlePipe_ClickHouseError(t *testing.T) {
	mux, fake, cleanup := setupTestServer("")
	defer cleanup()

	fake.setQueryHandler(func(sql string) (int, string) {
		return 500, "Code: 60. DB::Exception: Table default.traces doesn't exist."
	})

	req := httptest.NewRequest("GET", "/v0/pipes/list_traces.json?org_id=test", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHandlePipe_BarePathWithoutJSON(t *testing.T) {
	mux, fake, cleanup := setupTestServer("")
	defer cleanup()

	fake.setQueryHandler(func(sql string) (int, string) {
		return 200, `{"data":[],"rows":0,"statistics":{"elapsed":0.001,"rows_read":0,"bytes_read":0}}`
	})

	req := httptest.NewRequest("GET", "/v0/pipes/list_traces?org_id=test", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for bare path (no .json suffix), got %d: %s", w.Code, w.Body.String())
	}
}
