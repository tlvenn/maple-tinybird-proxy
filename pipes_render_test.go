package main

import (
	"strings"
	"testing"
)

func TestPipeRender_ListTraces(t *testing.T) {
	pipe := pipeRegistry["list_traces"]
	sql, err := buildQuery(pipe, Params{
		"org_id":     "org_abc",
		"service":    "payment-svc",
		"start_time": "2024-06-01T00:00:00Z",
		"end_time":   "2024-06-02T00:00:00Z",
		"limit":      "50",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"'org_abc'", "'payment-svc'", "LIMIT 50", "WITH"} {
		if !strings.Contains(sql, want) {
			t.Errorf("expected SQL to contain %q, got:\n%s", want, sql)
		}
	}

	// Multi-node pipe should produce a CTE
	if !strings.Contains(sql, "base_traces AS (") {
		t.Errorf("expected CTE with base_traces, got:\n%s", sql)
	}
}

func TestPipeRender_ListLogs(t *testing.T) {
	pipe := pipeRegistry["list_logs"]
	sql, err := buildQuery(pipe, Params{
		"org_id":   "org_abc",
		"service":  "api-gateway",
		"severity": "ERROR",
		"search":   "timeout",
		"limit":    "100",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"'ERROR'", "'timeout'", "LIMIT 100"} {
		if !strings.Contains(sql, want) {
			t.Errorf("expected SQL to contain %q, got:\n%s", want, sql)
		}
	}
}

func TestPipeRender_SpanHierarchy(t *testing.T) {
	pipe := pipeRegistry["span_hierarchy"]
	sql, err := buildQuery(pipe, Params{
		"trace_id": "abc123def456",
		"org_id":   "org_abc",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(sql, "TraceId") {
		t.Errorf("expected SQL to contain 'TraceId', got:\n%s", sql)
	}
	if !strings.Contains(sql, "'abc123def456'") {
		t.Errorf("expected SQL to contain trace_id value, got:\n%s", sql)
	}
	// Single node pipe — no WITH clause
	if strings.Contains(sql, "WITH") {
		t.Errorf("span_hierarchy is single-node, should not have WITH clause, got:\n%s", sql)
	}
}

func TestPipeRender_ServiceOverview(t *testing.T) {
	pipe := pipeRegistry["service_overview"]
	sql, err := buildQuery(pipe, Params{
		"org_id":     "org_abc",
		"start_time": "2024-06-01T00:00:00Z",
		"end_time":   "2024-06-02T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"quantile(0.50)", "quantile(0.95)", "quantile(0.99)"} {
		if !strings.Contains(sql, want) {
			t.Errorf("expected SQL to contain %q, got:\n%s", want, sql)
		}
	}
}

func TestPipeRender_CustomTracesTimeseries_GroupByService(t *testing.T) {
	pipe := pipeRegistry["custom_traces_timeseries"]
	sql, err := buildQuery(pipe, Params{
		"org_id":           "org_abc",
		"start_time":       "2024-06-01T00:00:00Z",
		"end_time":         "2024-06-02T00:00:00Z",
		"group_by_service": "1",
		"bucket_seconds":   "300",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(sql, "ServiceName") {
		t.Errorf("expected SQL to contain 'ServiceName' for group_by_service, got:\n%s", sql)
	}
	if !strings.Contains(sql, "INTERVAL 300 SECOND") {
		t.Errorf("expected SQL to contain 'INTERVAL 300 SECOND', got:\n%s", sql)
	}
}

func TestPipeRender_CustomTracesTimeseries_NoGroupBy(t *testing.T) {
	pipe := pipeRegistry["custom_traces_timeseries"]
	sql, err := buildQuery(pipe, Params{
		"org_id":     "org_abc",
		"start_time": "2024-06-01T00:00:00Z",
		"end_time":   "2024-06-02T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(sql, "'all'") {
		t.Errorf("expected SQL to contain \"'all'\" as default groupName, got:\n%s", sql)
	}
}

func TestPipeRender_SQLInjectionPrevention(t *testing.T) {
	pipe := pipeRegistry["list_logs"]
	sql, err := buildQuery(pipe, Params{
		"org_id": "test'; DROP TABLE--",
	})
	if err != nil {
		t.Fatal(err)
	}

	// The single quote must be escaped
	if strings.Contains(sql, "test'; DROP") {
		t.Errorf("raw SQL injection found in output:\n%s", sql)
	}
	// Escaped version should be present
	if !strings.Contains(sql, `test\'; DROP TABLE--`) {
		t.Errorf("expected escaped quote in SQL, got:\n%s", sql)
	}
}

func TestPipeRender_DateTimeSQLInjection(t *testing.T) {
	pipe := pipeRegistry["list_traces"]
	sql, err := buildQuery(pipe, Params{
		"org_id":     "org_abc",
		"start_time": "') OR 1=1--",
		"end_time":   "2024-06-02'; DROP TABLE traces--",
	})
	if err != nil {
		t.Fatal(err)
	}

	// The escapeString function turns ' into \' so the injection can't
	// break out of the SQL string literal. Verify the backslash-escaped
	// forms are present (proving escapeString was applied).
	if !strings.Contains(sql, `\') OR 1=1--`) {
		t.Errorf("expected backslash-escaped start_time injection value, got:\n%s", sql)
	}
	if !strings.Contains(sql, `\'; DROP TABLE traces--`) {
		t.Errorf("expected backslash-escaped end_time injection value, got:\n%s", sql)
	}

	// Also verify via the template engine directly that DateTime escapes properly
	rendered, err := RenderSQL(`{{ DateTime(val) }}`, Params{"val": "'); DROP TABLE--"})
	if err != nil {
		t.Fatal(err)
	}
	// Must produce '...\''... not a bare closing quote
	if rendered != `'\'); DROP TABLE--'` {
		t.Errorf("DateTime escaping mismatch: got %q, want %q", rendered, `'\'); DROP TABLE--'`)
	}
}

func TestPipeRender_DefaultValues(t *testing.T) {
	tests := []struct {
		name     string
		pipe     string
		params   Params
		contains []string
	}{
		{
			name:     "list_logs default limit",
			pipe:     "list_logs",
			params:   Params{},
			contains: []string{"LIMIT 50"},
		},
		{
			name:     "list_traces default limit",
			pipe:     "list_traces",
			params:   Params{},
			contains: []string{"LIMIT 100", "OFFSET 0"},
		},
		{
			name:     "errors_by_type default limit",
			pipe:     "errors_by_type",
			params:   Params{},
			contains: []string{"LIMIT 50"},
		},
		{
			name:     "custom_traces_timeseries default bucket",
			pipe:     "custom_traces_timeseries",
			params:   Params{},
			contains: []string{"INTERVAL 60 SECOND"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipe, ok := pipeRegistry[tt.pipe]
			if !ok {
				t.Fatalf("pipe %q not found in registry", tt.pipe)
			}
			sql, err := buildQuery(pipe, tt.params)
			if err != nil {
				t.Fatal(err)
			}
			for _, want := range tt.contains {
				if !strings.Contains(sql, want) {
					t.Errorf("expected SQL to contain %q, got:\n%s", want, sql)
				}
			}
		})
	}
}
