package main

import (
	"strings"
	"testing"
)

func TestRenderSQL_NoTemplate(t *testing.T) {
	sql := "SELECT 1"
	got, err := RenderSQL(sql, Params{})
	if err != nil {
		t.Fatal(err)
	}
	if got != sql {
		t.Errorf("expected %q, got %q", sql, got)
	}
}

func TestRenderSQL_StringSubstitution(t *testing.T) {
	sql := `SELECT * FROM t WHERE x = {{String(org_id, "")}}`
	got, err := RenderSQL(sql, Params{"org_id": "myorg"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, `'myorg'`) {
		t.Errorf("expected 'myorg' in %q", got)
	}
}

func TestRenderSQL_StringDefault(t *testing.T) {
	sql := `{{String(missing, "default_val")}}`
	got, err := RenderSQL(sql, Params{})
	if err != nil {
		t.Fatal(err)
	}
	if got != `'default_val'` {
		t.Errorf("expected 'default_val', got %q", got)
	}
}

func TestRenderSQL_StringEscaping(t *testing.T) {
	sql := `{{String(val)}}`
	got, err := RenderSQL(sql, Params{"val": "it's a test"})
	if err != nil {
		t.Fatal(err)
	}
	if got != `'it\'s a test'` {
		t.Errorf("unexpected escaping: %q", got)
	}
}

func TestRenderSQL_IntSubstitution(t *testing.T) {
	sql := `LIMIT {{Int32(limit, 100)}}`
	got, err := RenderSQL(sql, Params{"limit": "50"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "50") {
		t.Errorf("expected 50 in %q", got)
	}
}

func TestRenderSQL_IntDefault(t *testing.T) {
	sql := `LIMIT {{Int32(limit, 100)}}`
	got, err := RenderSQL(sql, Params{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "100") {
		t.Errorf("expected 100 in %q", got)
	}
}

func TestRenderSQL_IfDefined(t *testing.T) {
	sql := `WHERE 1=1 {% if defined(svc) %} AND svc = {{String(svc)}} {% end %}`

	// Without param
	got, err := RenderSQL(sql, Params{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "AND svc") {
		t.Errorf("should not contain 'AND svc' when param absent: %q", got)
	}

	// With param
	got, err = RenderSQL(sql, Params{"svc": "api"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "'api'") {
		t.Errorf("expected 'api' in %q", got)
	}
}

func TestRenderSQL_IfNotDefined(t *testing.T) {
	sql := `{% if not defined(x) %}default_branch{% end %}`
	got, err := RenderSQL(sql, Params{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "default_branch") {
		t.Errorf("expected default_branch in %q", got)
	}

	got, err = RenderSQL(sql, Params{"x": "val"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, "default_branch") {
		t.Errorf("should not contain default_branch when x defined: %q", got)
	}
}

func TestRenderSQL_IfElse(t *testing.T) {
	sql := `{% if defined(x) %}YES{% else %}NO{% end %}`

	got, _ := RenderSQL(sql, Params{"x": "1"})
	if !strings.Contains(got, "YES") {
		t.Errorf("expected YES, got %q", got)
	}

	got, _ = RenderSQL(sql, Params{})
	if !strings.Contains(got, "NO") {
		t.Errorf("expected NO, got %q", got)
	}
}

func TestRenderSQL_IfElif(t *testing.T) {
	sql := `{% if defined(group_by_service) %}ServiceName{% elif defined(group_by_span_name) %}SpanName{% else %}'all'{% end %} AS groupName`

	got, _ := RenderSQL(sql, Params{"group_by_service": "1"})
	if !strings.Contains(got, "ServiceName") {
		t.Errorf("expected ServiceName, got %q", got)
	}

	got, _ = RenderSQL(sql, Params{"group_by_span_name": "1"})
	if !strings.Contains(got, "SpanName") {
		t.Errorf("expected SpanName, got %q", got)
	}

	got, _ = RenderSQL(sql, Params{})
	if !strings.Contains(got, "'all'") {
		t.Errorf("expected 'all', got %q", got)
	}
}

func TestRenderSQL_NotDefinedOrEquals(t *testing.T) {
	sql := `{% if not defined(metric_type) or metric_type == 'sum' %}SUM_BLOCK{% else %}EMPTY{% end %}`

	// Not defined → include SUM_BLOCK
	got, _ := RenderSQL(sql, Params{})
	if !strings.Contains(got, "SUM_BLOCK") {
		t.Errorf("expected SUM_BLOCK when not defined, got %q", got)
	}

	// metric_type=sum → include SUM_BLOCK
	got, _ = RenderSQL(sql, Params{"metric_type": "sum"})
	if !strings.Contains(got, "SUM_BLOCK") {
		t.Errorf("expected SUM_BLOCK when metric_type=sum, got %q", got)
	}

	// metric_type=gauge → EMPTY
	got, _ = RenderSQL(sql, Params{"metric_type": "gauge"})
	if strings.Contains(got, "SUM_BLOCK") {
		t.Errorf("should not contain SUM_BLOCK when metric_type=gauge, got %q", got)
	}
}

func TestRenderSQL_NestedIf(t *testing.T) {
	sql := `WHERE 1=1 {% if defined(a) %} AND a=1 {% if defined(b) %} AND b=2 {% end %} {% end %}`

	got, _ := RenderSQL(sql, Params{"a": "x", "b": "y"})
	if !strings.Contains(got, "AND a=1") || !strings.Contains(got, "AND b=2") {
		t.Errorf("expected both conditions, got %q", got)
	}

	got, _ = RenderSQL(sql, Params{"a": "x"})
	if !strings.Contains(got, "AND a=1") || strings.Contains(got, "AND b=2") {
		t.Errorf("expected only a condition, got %q", got)
	}

	got, _ = RenderSQL(sql, Params{})
	if strings.Contains(got, "AND a=1") {
		t.Errorf("expected no conditions, got %q", got)
	}
}

func TestRenderSQL_DateTime(t *testing.T) {
	sql := `AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}}`

	got, _ := RenderSQL(sql, Params{"start_time": "2024-03-15T10:00:00Z"})
	if !strings.Contains(got, "'2024-03-15 10:00:00'") {
		t.Errorf("expected formatted datetime, got %q", got)
	}

	got, _ = RenderSQL(sql, Params{})
	if !strings.Contains(got, "'2023-01-01 00:00:00'") {
		t.Errorf("expected default datetime, got %q", got)
	}
}

func TestBuildQuery_SingleNode(t *testing.T) {
	pipe := Pipe{
		Name: "test",
		Nodes: []PipeNode{
			{Name: "n1", SQL: "SELECT 1"},
		},
	}
	got, err := buildQuery(pipe, Params{})
	if err != nil {
		t.Fatal(err)
	}
	if got != "SELECT 1" {
		t.Errorf("expected 'SELECT 1', got %q", got)
	}
}

func TestBuildQuery_MultiNode(t *testing.T) {
	pipe := Pipe{
		Name: "test",
		Nodes: []PipeNode{
			{Name: "base", SQL: "SELECT id FROM t"},
			{Name: "final", SQL: "SELECT * FROM base LIMIT 10"},
		},
	}
	got, err := buildQuery(pipe, Params{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "WITH\nbase AS (") {
		t.Errorf("expected CTE header, got %q", got)
	}
	if !strings.Contains(got, "SELECT * FROM base LIMIT 10") {
		t.Errorf("expected final query, got %q", got)
	}
}

func TestAllPipesRender(t *testing.T) {
	// Smoke test: every pipe must render without error with empty params
	for _, pipe := range allPipes {
		t.Run(pipe.Name, func(t *testing.T) {
			_, err := buildQuery(pipe, Params{})
			if err != nil {
				t.Errorf("pipe %q render error: %v", pipe.Name, err)
			}
		})
	}
}
