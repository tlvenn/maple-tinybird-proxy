package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// ─── Pipe types ────────────────────────────────────────────────────────────────

type PipeNode struct {
	Name string
	SQL  string
}

type Pipe struct {
	Name  string
	Nodes []PipeNode
}

// ─── Registry ─────────────────────────────────────────────────────────────────

var pipeRegistry = map[string]Pipe{}

func init() {
	for _, p := range allPipes {
		pipeRegistry[p.Name] = p
	}
}

// ─── CTE Compiler ─────────────────────────────────────────────────────────────

// buildQuery compiles a multi-node pipe into a single SQL statement.
// Nodes 0..N-2 become CTEs; node N-1 is the outer SELECT.
func buildQuery(pipe Pipe, params Params) (string, error) {
	if len(pipe.Nodes) == 0 {
		return "", fmt.Errorf("pipe %q has no nodes", pipe.Name)
	}

	last := pipe.Nodes[len(pipe.Nodes)-1]

	if len(pipe.Nodes) == 1 {
		return RenderSQL(last.SQL, params)
	}

	var sb strings.Builder
	sb.WriteString("WITH\n")

	for i, node := range pipe.Nodes[:len(pipe.Nodes)-1] {
		rendered, err := RenderSQL(node.SQL, params)
		if err != nil {
			return "", fmt.Errorf("pipe %q node %q: %w", pipe.Name, node.Name, err)
		}
		if i > 0 {
			sb.WriteString(",\n")
		}
		fmt.Fprintf(&sb, "%s AS (\n%s\n)", node.Name, rendered)
	}

	finalSQL, err := RenderSQL(last.SQL, params)
	if err != nil {
		return "", fmt.Errorf("pipe %q node %q: %w", pipe.Name, last.Name, err)
	}
	sb.WriteString("\n")
	sb.WriteString(finalSQL)
	return sb.String(), nil
}

// ─── Query Handler ────────────────────────────────────────────────────────────

func handlePipe(ch *ClickHouseClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")

		pipe, ok := pipeRegistry[name]
		if !ok {
			writeJSONError(w, http.StatusNotFound, fmt.Sprintf("pipe not found: %s", name))
			return
		}

		params := Params{}
		for k, vs := range r.URL.Query() {
			if len(vs) > 0 {
				params[k] = vs[0]
			}
		}

		sql, err := buildQuery(pipe, params)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, err.Error())
			return
		}

		result, err := ch.Query(sql)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, err.Error())
			return
		}

		// Tinybird response envelope
		resp := map[string]interface{}{
			"data":                      result.Data,
			"rows":                      result.Rows,
			"rows_before_limit_at_least": result.Rows,
			"statistics": map[string]interface{}{
				"elapsed":    result.Statistics.Elapsed,
				"rows_read":  result.Statistics.RowsRead,
				"bytes_read": result.Statistics.BytesRead,
			},
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}

// ─── All Pipes ────────────────────────────────────────────────────────────────

var allPipes = []Pipe{

	// ── 1. list_traces ───────────────────────────────────────────────────────
	{
		Name: "list_traces",
		Nodes: []PipeNode{
			{Name: "base_traces", SQL: `
        SELECT
          TraceId AS traceId,
          min(Timestamp) AS startTime,
          fromUnixTimestamp64Nano(max(toUnixTimestamp64Nano(Timestamp) + Duration)) AS endTime,
          intDiv(max(toUnixTimestamp64Nano(Timestamp) + Duration) - min(toUnixTimestamp64Nano(Timestamp)), 1000) AS durationMicros,
          count() AS spanCount,
          groupUniqArray(ServiceName) AS services,
          argMin(
            if(
              SpanName LIKE 'http.server %' AND SpanAttributes['http.route'] != '',
              concat(replaceOne(SpanName, 'http.server ', ''), ' ', SpanAttributes['http.route']),
              SpanName
            ),
            if(ParentSpanId = '', 0, 1)
          ) AS rootSpanName,
          max(if(
            StatusCode = 'Error'
            OR (SpanAttributes['http.status_code'] != '' AND toUInt16OrZero(SpanAttributes['http.status_code']) >= 500),
            1, 0
          )) AS hasError,
          groupUniqArrayIf(SpanAttributes['http.method'], SpanAttributes['http.method'] != '') AS httpMethods,
          groupUniqArrayIf(SpanAttributes['http.status_code'], SpanAttributes['http.status_code'] != '') AS httpStatusCodes,
          groupUniqArrayIf(ResourceAttributes['deployment.environment'], ResourceAttributes['deployment.environment'] != '') AS deploymentEnvs,
          {% if defined(attribute_filter_key) %}
          max(if(
            SpanAttributes[{{String(attribute_filter_key)}}] = {{String(attribute_filter_value, "")}},
            1, 0
          )) AS matchesAttributeFilter,
          {% else %}
          1 AS matchesAttributeFilter,
          {% end %}
          {% if defined(resource_filter_key) %}
          max(if(
            ResourceAttributes[{{String(resource_filter_key)}}] = {{String(resource_filter_value, "")}},
            1, 0
          )) AS matchesResourceFilter,
          {% else %}
          1 AS matchesResourceFilter,
          {% end %}
          countIf(ParentSpanId = '') AS rootSpanCount
        FROM traces
        WHERE TraceId != ''
          AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %}
          AND ServiceName = {{String(service, "")}}
        {% end %}
        {% if defined(start_time) %}
          AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}}
        {% end %}
        {% if defined(end_time) %}
          AND Timestamp <= {{DateTime(end_time, "2023-12-31 23:59:59")}}
        {% end %}
        GROUP BY TraceId
`},
			{Name: "list_traces_node", SQL: `
        SELECT
          traceId, startTime, endTime, durationMicros,
          spanCount, services, rootSpanName, hasError
        FROM base_traces
        WHERE 1=1
        {% if not defined(root_only) or root_only %}
          AND rootSpanCount > 0
        {% end %}
        {% if defined(span_name) %}
          AND rootSpanName = {{String(span_name, "")}}
        {% end %}
        {% if defined(has_error) and has_error %}
          AND hasError = 1
        {% end %}
        {% if defined(min_duration_ms) %}
          AND durationMicros >= {{Float64(min_duration_ms, 0)}} * 1000
        {% end %}
        {% if defined(max_duration_ms) %}
          AND durationMicros <= {{Float64(max_duration_ms, 999999999)}} * 1000
        {% end %}
        {% if defined(http_method) %}
          AND has(httpMethods, {{String(http_method, "")}})
        {% end %}
        {% if defined(http_status_code) %}
          AND has(httpStatusCodes, {{String(http_status_code, "")}})
        {% end %}
        {% if defined(deployment_env) %}
          AND has(deploymentEnvs, {{String(deployment_env, "")}})
        {% end %}
        {% if defined(attribute_filter_key) %}
          AND matchesAttributeFilter = 1
        {% end %}
        {% if defined(resource_filter_key) %}
          AND matchesResourceFilter = 1
        {% end %}
        ORDER BY startTime DESC
        LIMIT {{Int32(limit, 100)}}
        OFFSET {{Int32(offset, 0)}}
`},
		},
	},

	// ── 2. span_hierarchy ────────────────────────────────────────────────────
	{
		Name: "span_hierarchy",
		Nodes: []PipeNode{
			{Name: "span_hierarchy_node", SQL: `
        SELECT
          TraceId AS traceId,
          SpanId AS spanId,
          ParentSpanId AS parentSpanId,
          if(
            SpanName LIKE 'http.server %' AND SpanAttributes['http.route'] != '',
            concat(replaceOne(SpanName, 'http.server ', ''), ' ', SpanAttributes['http.route']),
            SpanName
          ) AS spanName,
          ServiceName AS serviceName,
          SpanKind AS spanKind,
          Duration / 1000000 AS durationMs,
          Timestamp AS startTime,
          StatusCode AS statusCode,
          StatusMessage AS statusMessage,
          toJSONString(SpanAttributes) AS spanAttributes,
          toJSONString(ResourceAttributes) AS resourceAttributes,
          {% if defined(span_id) %}
          if(SpanId = {{String(span_id, "")}}, 'target', 'related') AS relationship
          {% else %}
          'related' AS relationship
          {% end %}
        FROM traces
        WHERE TraceId = {{String(trace_id)}}
          AND OrgId = {{String(org_id, "")}}
        ORDER BY Timestamp ASC
`},
		},
	},

	// ── 3. list_logs ─────────────────────────────────────────────────────────
	{
		Name: "list_logs",
		Nodes: []PipeNode{
			{Name: "list_logs_node", SQL: `
        SELECT
          Timestamp AS timestamp,
          SeverityText AS severityText,
          SeverityNumber AS severityNumber,
          ServiceName AS serviceName,
          Body AS body,
          TraceId AS traceId,
          SpanId AS spanId,
          toJSONString(LogAttributes) AS logAttributes,
          toJSONString(ResourceAttributes) AS resourceAttributes
        FROM logs
        WHERE 1=1
        AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %}
          AND ServiceName = {{String(service, "")}}
        {% end %}
        {% if defined(severity) %}
          AND SeverityText = {{String(severity, "")}}
        {% end %}
        {% if defined(min_severity) %}
          AND SeverityNumber >= {{UInt8(min_severity, 0)}}
        {% end %}
        {% if defined(start_time) %}
          AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}}
        {% end %}
        {% if defined(end_time) %}
          AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}}
        {% end %}
        {% if defined(trace_id) %}
          AND TraceId = {{String(trace_id, "")}}
        {% end %}
        {% if defined(span_id) %}
          AND SpanId = {{String(span_id, "")}}
        {% end %}
        {% if defined(cursor) %}
          AND Timestamp < {{DateTime(cursor)}}
        {% end %}
        {% if defined(search) %}
          AND Body ILIKE concat('%', {{String(search, "")}}, '%')
        {% end %}
        ORDER BY Timestamp DESC
        LIMIT {{Int32(limit, 50)}}
`},
		},
	},

	// ── 4. logs_count ────────────────────────────────────────────────────────
	{
		Name: "logs_count",
		Nodes: []PipeNode{
			{Name: "logs_count_node", SQL: `
        SELECT count() as total
        FROM logs
        WHERE 1=1
        AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %}
          AND ServiceName = {{String(service, "")}}
        {% end %}
        {% if defined(severity) %}
          AND SeverityText = {{String(severity, "")}}
        {% end %}
        {% if defined(start_time) %}
          AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}}
        {% end %}
        {% if defined(end_time) %}
          AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}}
        {% end %}
        {% if defined(trace_id) %}
          AND TraceId = {{String(trace_id, "")}}
        {% end %}
        {% if defined(search) %}
          AND Body ILIKE concat('%', {{String(search, "")}}, '%')
        {% end %}
`},
		},
	},

	// ── 5. logs_facets ───────────────────────────────────────────────────────
	{
		Name: "logs_facets",
		Nodes: []PipeNode{
			{Name: "severity_facets", SQL: `
        SELECT SeverityText AS severityText, '' AS serviceName,
               count() AS count, 'severity' AS facetType
        FROM logs
        WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY SeverityText ORDER BY count DESC
`},
			{Name: "service_facets", SQL: `
        SELECT '' AS severityText, ServiceName AS serviceName,
               count() AS count, 'service' AS facetType
        FROM logs
        WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(severity) %} AND SeverityText = {{String(severity, "")}} {% end %}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY ServiceName ORDER BY count DESC
`},
			{Name: "combined_facets", SQL: `
        SELECT * FROM severity_facets
        UNION ALL
        SELECT * FROM service_facets
`},
		},
	},

	// ── 6. error_rate_by_service ─────────────────────────────────────────────
	{
		Name: "error_rate_by_service",
		Nodes: []PipeNode{
			{Name: "error_rate_by_service_node", SQL: `
        SELECT
          ServiceName AS serviceName,
          count() AS totalLogs,
          countIf(SeverityText IN ('ERROR', 'FATAL')) AS errorLogs,
          round(errorLogs / totalLogs * 100, 2) AS errorRatePercent
        FROM logs
        WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY ServiceName
        ORDER BY errorRatePercent DESC
`},
		},
	},

	// ── 7. get_service_usage ─────────────────────────────────────────────────
	{
		Name: "get_service_usage",
		Nodes: []PipeNode{
			{Name: "service_usage_node", SQL: `
        SELECT
          ServiceName AS serviceName,
          sum(LogCount) AS totalLogCount, sum(LogSizeBytes) AS totalLogSizeBytes,
          sum(TraceCount) AS totalTraceCount, sum(TraceSizeBytes) AS totalTraceSizeBytes,
          sum(SumMetricCount) AS totalSumMetricCount, sum(SumMetricSizeBytes) AS totalSumMetricSizeBytes,
          sum(GaugeMetricCount) AS totalGaugeMetricCount, sum(GaugeMetricSizeBytes) AS totalGaugeMetricSizeBytes,
          sum(HistogramMetricCount) AS totalHistogramMetricCount, sum(HistogramMetricSizeBytes) AS totalHistogramMetricSizeBytes,
          sum(ExpHistogramMetricCount) AS totalExpHistogramMetricCount, sum(ExpHistogramMetricSizeBytes) AS totalExpHistogramMetricSizeBytes,
          sum(LogSizeBytes) + sum(TraceSizeBytes) + sum(SumMetricSizeBytes) + sum(GaugeMetricSizeBytes) + sum(HistogramMetricSizeBytes) + sum(ExpHistogramMetricSizeBytes) AS totalSizeBytes
        FROM service_usage
        WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND Hour >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Hour <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY ServiceName ORDER BY totalSizeBytes DESC
`},
		},
	},

	// ── 8. list_metrics ──────────────────────────────────────────────────────
	{
		Name: "list_metrics",
		Nodes: []PipeNode{
			{Name: "all_metrics", SQL: `
        {% if not defined(metric_type) or metric_type == 'sum' %}
        SELECT OrgId, MetricName AS metricName, 'sum' AS metricType,
               ServiceName AS serviceName, MetricDescription AS metricDescription,
               MetricUnit AS metricUnit, count() AS dataPointCount,
               min(TimeUnix) AS firstSeen, max(TimeUnix) AS lastSeen
        FROM metrics_sum
        WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        {% if defined(search) %} AND MetricName ILIKE concat('%', {{String(search, "")}}, '%') {% end %}
        GROUP BY OrgId, MetricName, ServiceName, MetricDescription, MetricUnit
        {% else %}
        SELECT '' AS OrgId, '' AS metricName, '' AS metricType, '' AS serviceName,
               '' AS metricDescription, '' AS metricUnit, 0 AS dataPointCount,
               now() AS firstSeen, now() AS lastSeen WHERE 1=0
        {% end %}

        UNION ALL

        {% if not defined(metric_type) or metric_type == 'gauge' %}
        SELECT OrgId, MetricName AS metricName, 'gauge' AS metricType,
               ServiceName AS serviceName, MetricDescription AS metricDescription,
               MetricUnit AS metricUnit, count() AS dataPointCount,
               min(TimeUnix) AS firstSeen, max(TimeUnix) AS lastSeen
        FROM metrics_gauge
        WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        {% if defined(search) %} AND MetricName ILIKE concat('%', {{String(search, "")}}, '%') {% end %}
        GROUP BY OrgId, MetricName, ServiceName, MetricDescription, MetricUnit
        {% else %}
        SELECT '' AS OrgId, '' AS metricName, '' AS metricType, '' AS serviceName,
               '' AS metricDescription, '' AS metricUnit, 0 AS dataPointCount,
               now() AS firstSeen, now() AS lastSeen WHERE 1=0
        {% end %}

        UNION ALL

        {% if not defined(metric_type) or metric_type == 'histogram' %}
        SELECT OrgId, MetricName AS metricName, 'histogram' AS metricType,
               ServiceName AS serviceName, MetricDescription AS metricDescription,
               MetricUnit AS metricUnit, count() AS dataPointCount,
               min(TimeUnix) AS firstSeen, max(TimeUnix) AS lastSeen
        FROM metrics_histogram
        WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        {% if defined(search) %} AND MetricName ILIKE concat('%', {{String(search, "")}}, '%') {% end %}
        GROUP BY OrgId, MetricName, ServiceName, MetricDescription, MetricUnit
        {% else %}
        SELECT '' AS OrgId, '' AS metricName, '' AS metricType, '' AS serviceName,
               '' AS metricDescription, '' AS metricUnit, 0 AS dataPointCount,
               now() AS firstSeen, now() AS lastSeen WHERE 1=0
        {% end %}

        UNION ALL

        {% if not defined(metric_type) or metric_type == 'exponential_histogram' %}
        SELECT OrgId, MetricName AS metricName, 'exponential_histogram' AS metricType,
               ServiceName AS serviceName, MetricDescription AS metricDescription,
               MetricUnit AS metricUnit, count() AS dataPointCount,
               min(TimeUnix) AS firstSeen, max(TimeUnix) AS lastSeen
        FROM metrics_exponential_histogram
        WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        {% if defined(search) %} AND MetricName ILIKE concat('%', {{String(search, "")}}, '%') {% end %}
        GROUP BY OrgId, MetricName, ServiceName, MetricDescription, MetricUnit
        {% else %}
        SELECT '' AS OrgId, '' AS metricName, '' AS metricType, '' AS serviceName,
               '' AS metricDescription, '' AS metricUnit, 0 AS dataPointCount,
               now() AS firstSeen, now() AS lastSeen WHERE 1=0
        {% end %}
`},
			{Name: "filtered_metrics", SQL: `
        SELECT metricName, metricType, serviceName, metricDescription,
               metricUnit, dataPointCount, firstSeen, lastSeen
        FROM all_metrics
        WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(metric_type) %} AND metricType = {{String(metric_type, "")}} {% end %}
        ORDER BY lastSeen DESC
        LIMIT {{Int32(limit, 100)}}
        OFFSET {{Int32(offset, 0)}}
`},
		},
	},

	// ── 9. metric_time_series_sum ─────────────────────────────────────────────
	{
		Name: "metric_time_series_sum",
		Nodes: []PipeNode{
			{Name: "time_series", SQL: `
        SELECT
          toStartOfInterval(TimeUnix, INTERVAL {{Int32(bucket_seconds, 60)}} SECOND) AS bucket,
          ServiceName AS serviceName,
          avg(Value) AS avgValue, min(Value) AS minValue,
          max(Value) AS maxValue, sum(Value) AS sumValue,
          count() AS dataPointCount
        FROM metrics_sum
        WHERE MetricName = {{String(metric_name)}}
          AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY bucket, ServiceName ORDER BY bucket ASC
`},
		},
	},

	// ── 10. metric_time_series_gauge ──────────────────────────────────────────
	{
		Name: "metric_time_series_gauge",
		Nodes: []PipeNode{
			{Name: "time_series", SQL: `
        SELECT
          toStartOfInterval(TimeUnix, INTERVAL {{Int32(bucket_seconds, 60)}} SECOND) AS bucket,
          ServiceName AS serviceName,
          avg(Value) AS avgValue, min(Value) AS minValue,
          max(Value) AS maxValue, sum(Value) AS sumValue,
          count() AS dataPointCount
        FROM metrics_gauge
        WHERE MetricName = {{String(metric_name)}}
          AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY bucket, ServiceName ORDER BY bucket ASC
`},
		},
	},

	// ── 11. metric_time_series_histogram ──────────────────────────────────────
	{
		Name: "metric_time_series_histogram",
		Nodes: []PipeNode{
			{Name: "time_series", SQL: `
        SELECT
          toStartOfInterval(TimeUnix, INTERVAL {{Int32(bucket_seconds, 60)}} SECOND) AS bucket,
          ServiceName AS serviceName,
          if(sum(Count) > 0, sum(Sum) / sum(Count), 0) AS avgValue,
          min(Min) AS minValue, max(Max) AS maxValue,
          sum(Sum) AS sumValue, sum(Count) AS dataPointCount
        FROM metrics_histogram
        WHERE MetricName = {{String(metric_name)}}
          AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY bucket, ServiceName ORDER BY bucket ASC
`},
		},
	},

	// ── 12. metric_time_series_exp_histogram ──────────────────────────────────
	{
		Name: "metric_time_series_exp_histogram",
		Nodes: []PipeNode{
			{Name: "time_series", SQL: `
        SELECT
          toStartOfInterval(TimeUnix, INTERVAL {{Int32(bucket_seconds, 60)}} SECOND) AS bucket,
          ServiceName AS serviceName,
          if(sum(Count) > 0, sum(Sum) / sum(Count), 0) AS avgValue,
          min(Min) AS minValue, max(Max) AS maxValue,
          sum(Sum) AS sumValue, sum(Count) AS dataPointCount
        FROM metrics_exponential_histogram
        WHERE MetricName = {{String(metric_name)}}
          AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY bucket, ServiceName ORDER BY bucket ASC
`},
		},
	},

	// ── 13. metrics_summary ───────────────────────────────────────────────────
	{
		Name: "metrics_summary",
		Nodes: []PipeNode{
			{Name: "sum_count", SQL: `
        SELECT 'sum' AS metricType, count(DISTINCT MetricName) AS metricCount, count() AS dataPointCount
        FROM metrics_sum WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
`},
			{Name: "gauge_count", SQL: `
        SELECT 'gauge' AS metricType, count(DISTINCT MetricName) AS metricCount, count() AS dataPointCount
        FROM metrics_gauge WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
`},
			{Name: "histogram_count", SQL: `
        SELECT 'histogram' AS metricType, count(DISTINCT MetricName) AS metricCount, count() AS dataPointCount
        FROM metrics_histogram WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
`},
			{Name: "exponential_histogram_count", SQL: `
        SELECT 'exponential_histogram' AS metricType, count(DISTINCT MetricName) AS metricCount, count() AS dataPointCount
        FROM metrics_exponential_histogram WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(service) %} AND ServiceName = {{String(service, "")}} {% end %}
        {% if defined(start_time) %} AND TimeUnix >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND TimeUnix <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
`},
			{Name: "metrics_summary_result", SQL: `
        SELECT * FROM sum_count UNION ALL SELECT * FROM gauge_count
        UNION ALL SELECT * FROM histogram_count UNION ALL SELECT * FROM exponential_histogram_count
`},
		},
	},

	// ── 14. traces_facets ─────────────────────────────────────────────────────
	{
		Name: "traces_facets",
		Nodes: []PipeNode{
			{Name: "trace_summaries", SQL: `
        SELECT
          TraceId,
          groupUniqArray(ServiceName) AS services,
          argMin(
            if(SpanName LIKE 'http.server %' AND SpanAttributes['http.route'] != '',
               concat(replaceOne(SpanName, 'http.server ', ''), ' ', SpanAttributes['http.route']),
               SpanName),
            if(ParentSpanId = '', 0, 1)
          ) AS rootSpanName,
          groupUniqArrayIf(SpanAttributes['http.method'], SpanAttributes['http.method'] != '') AS httpMethods,
          groupUniqArrayIf(SpanAttributes['http.status_code'], SpanAttributes['http.status_code'] != '') AS httpStatusCodes,
          groupUniqArrayIf(ResourceAttributes['deployment.environment'], ResourceAttributes['deployment.environment'] != '') AS deploymentEnvs,
          max(if(StatusCode = 'Error' OR (SpanAttributes['http.status_code'] != '' AND toUInt16OrZero(SpanAttributes['http.status_code']) >= 500), 1, 0)) AS hasError,
          {% if defined(attribute_filter_key) %}
          max(if(SpanAttributes[{{String(attribute_filter_key)}}] = {{String(attribute_filter_value, "")}}, 1, 0)) AS matchesAttributeFilter,
          {% else %}
          1 AS matchesAttributeFilter,
          {% end %}
          {% if defined(resource_filter_key) %}
          max(if(ResourceAttributes[{{String(resource_filter_key)}}] = {{String(resource_filter_value, "")}}, 1, 0)) AS matchesResourceFilter,
          {% else %}
          1 AS matchesResourceFilter,
          {% end %}
          (max(toUnixTimestamp64Nano(Timestamp) + Duration) - min(toUnixTimestamp64Nano(Timestamp))) / 1000000.0 AS durationMs
        FROM traces
        WHERE TraceId != '' AND OrgId = {{String(org_id, "")}}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY TraceId
`},
			{Name: "service_facets", SQL: `
        SELECT service AS name, count() AS count, 'service' AS facetType
        FROM trace_summaries ARRAY JOIN services AS service
        WHERE 1=1
        {% if defined(attribute_filter_key) %} AND matchesAttributeFilter = 1 {% end %}
        {% if defined(resource_filter_key) %} AND matchesResourceFilter = 1 {% end %}
        {% if defined(span_name) %} AND rootSpanName = {{String(span_name)}} {% end %}
        {% if defined(has_error) and has_error %} AND hasError = 1 {% end %}
        {% if defined(min_duration_ms) %} AND durationMs >= {{Float64(min_duration_ms)}} {% end %}
        {% if defined(max_duration_ms) %} AND durationMs <= {{Float64(max_duration_ms)}} {% end %}
        {% if defined(http_method) %} AND has(httpMethods, {{String(http_method)}}) {% end %}
        {% if defined(http_status_code) %} AND has(httpStatusCodes, {{String(http_status_code)}}) {% end %}
        {% if defined(deployment_env) %} AND has(deploymentEnvs, {{String(deployment_env)}}) {% end %}
        GROUP BY service ORDER BY count DESC LIMIT 50
`},
			{Name: "span_name_facets", SQL: `
        SELECT rootSpanName AS name, count() AS count, 'spanName' AS facetType
        FROM trace_summaries WHERE 1=1
        {% if defined(attribute_filter_key) %} AND matchesAttributeFilter = 1 {% end %}
        {% if defined(resource_filter_key) %} AND matchesResourceFilter = 1 {% end %}
        {% if defined(service) %} AND has(services, {{String(service)}}) {% end %}
        {% if defined(has_error) and has_error %} AND hasError = 1 {% end %}
        {% if defined(min_duration_ms) %} AND durationMs >= {{Float64(min_duration_ms)}} {% end %}
        {% if defined(max_duration_ms) %} AND durationMs <= {{Float64(max_duration_ms)}} {% end %}
        {% if defined(http_method) %} AND has(httpMethods, {{String(http_method)}}) {% end %}
        {% if defined(http_status_code) %} AND has(httpStatusCodes, {{String(http_status_code)}}) {% end %}
        {% if defined(deployment_env) %} AND has(deploymentEnvs, {{String(deployment_env)}}) {% end %}
        GROUP BY rootSpanName ORDER BY count DESC LIMIT 50
`},
			{Name: "http_method_facets", SQL: `
        SELECT method AS name, count() AS count, 'httpMethod' AS facetType
        FROM trace_summaries ARRAY JOIN httpMethods AS method WHERE 1=1
        {% if defined(attribute_filter_key) %} AND matchesAttributeFilter = 1 {% end %}
        {% if defined(resource_filter_key) %} AND matchesResourceFilter = 1 {% end %}
        {% if defined(service) %} AND has(services, {{String(service)}}) {% end %}
        {% if defined(span_name) %} AND rootSpanName = {{String(span_name)}} {% end %}
        {% if defined(has_error) and has_error %} AND hasError = 1 {% end %}
        {% if defined(min_duration_ms) %} AND durationMs >= {{Float64(min_duration_ms)}} {% end %}
        {% if defined(max_duration_ms) %} AND durationMs <= {{Float64(max_duration_ms)}} {% end %}
        {% if defined(http_status_code) %} AND has(httpStatusCodes, {{String(http_status_code)}}) {% end %}
        {% if defined(deployment_env) %} AND has(deploymentEnvs, {{String(deployment_env)}}) {% end %}
        GROUP BY method ORDER BY count DESC LIMIT 20
`},
			{Name: "http_status_facets", SQL: `
        SELECT status AS name, count() AS count, 'httpStatus' AS facetType
        FROM trace_summaries ARRAY JOIN httpStatusCodes AS status WHERE 1=1
        {% if defined(attribute_filter_key) %} AND matchesAttributeFilter = 1 {% end %}
        {% if defined(resource_filter_key) %} AND matchesResourceFilter = 1 {% end %}
        {% if defined(service) %} AND has(services, {{String(service)}}) {% end %}
        {% if defined(span_name) %} AND rootSpanName = {{String(span_name)}} {% end %}
        {% if defined(has_error) and has_error %} AND hasError = 1 {% end %}
        {% if defined(min_duration_ms) %} AND durationMs >= {{Float64(min_duration_ms)}} {% end %}
        {% if defined(max_duration_ms) %} AND durationMs <= {{Float64(max_duration_ms)}} {% end %}
        {% if defined(http_method) %} AND has(httpMethods, {{String(http_method)}}) {% end %}
        {% if defined(deployment_env) %} AND has(deploymentEnvs, {{String(deployment_env)}}) {% end %}
        GROUP BY status ORDER BY count DESC LIMIT 20
`},
			{Name: "deployment_env_facets", SQL: `
        SELECT env AS name, count() AS count, 'deploymentEnv' AS facetType
        FROM trace_summaries ARRAY JOIN deploymentEnvs AS env WHERE 1=1
        {% if defined(attribute_filter_key) %} AND matchesAttributeFilter = 1 {% end %}
        {% if defined(resource_filter_key) %} AND matchesResourceFilter = 1 {% end %}
        {% if defined(service) %} AND has(services, {{String(service)}}) {% end %}
        {% if defined(span_name) %} AND rootSpanName = {{String(span_name)}} {% end %}
        {% if defined(has_error) and has_error %} AND hasError = 1 {% end %}
        {% if defined(min_duration_ms) %} AND durationMs >= {{Float64(min_duration_ms)}} {% end %}
        {% if defined(max_duration_ms) %} AND durationMs <= {{Float64(max_duration_ms)}} {% end %}
        {% if defined(http_method) %} AND has(httpMethods, {{String(http_method)}}) {% end %}
        {% if defined(http_status_code) %} AND has(httpStatusCodes, {{String(http_status_code)}}) {% end %}
        GROUP BY env ORDER BY count DESC LIMIT 20
`},
			{Name: "error_count", SQL: `
        SELECT 'error' AS name, countIf(hasError = 1) AS count, 'errorCount' AS facetType
        FROM trace_summaries WHERE 1=1
        {% if defined(attribute_filter_key) %} AND matchesAttributeFilter = 1 {% end %}
        {% if defined(resource_filter_key) %} AND matchesResourceFilter = 1 {% end %}
        {% if defined(service) %} AND has(services, {{String(service)}}) {% end %}
        {% if defined(span_name) %} AND rootSpanName = {{String(span_name)}} {% end %}
        {% if defined(min_duration_ms) %} AND durationMs >= {{Float64(min_duration_ms)}} {% end %}
        {% if defined(max_duration_ms) %} AND durationMs <= {{Float64(max_duration_ms)}} {% end %}
        {% if defined(http_method) %} AND has(httpMethods, {{String(http_method)}}) {% end %}
        {% if defined(http_status_code) %} AND has(httpStatusCodes, {{String(http_status_code)}}) {% end %}
        {% if defined(deployment_env) %} AND has(deploymentEnvs, {{String(deployment_env)}}) {% end %}
`},
			{Name: "combined_facets", SQL: `
        SELECT * FROM service_facets UNION ALL SELECT * FROM span_name_facets
        UNION ALL SELECT * FROM http_method_facets UNION ALL SELECT * FROM http_status_facets
        UNION ALL SELECT * FROM deployment_env_facets UNION ALL SELECT * FROM error_count
`},
		},
	},

	// ── 15. traces_duration_stats ─────────────────────────────────────────────
	{
		Name: "traces_duration_stats",
		Nodes: []PipeNode{
			{Name: "duration_stats_node", SQL: `
        SELECT
          min(durationMs) AS minDurationMs, max(durationMs) AS maxDurationMs,
          quantile(0.50)(durationMs) AS p50DurationMs, quantile(0.95)(durationMs) AS p95DurationMs
        FROM (
          SELECT TraceId,
            (max(toUnixTimestamp64Nano(Timestamp) + Duration) - min(toUnixTimestamp64Nano(Timestamp))) / 1000000.0 AS durationMs
          FROM traces WHERE TraceId != '' AND OrgId = {{String(org_id, "")}}
          {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
          {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
          {% if defined(service) %} AND ServiceName = {{String(service)}} {% end %}
          {% if defined(span_name) %} AND ParentSpanId = '' AND SpanName = {{String(span_name)}} {% end %}
          {% if defined(has_error) and has_error %}
            AND TraceId IN (
              SELECT TraceId FROM traces WHERE TraceId != '' AND OrgId = {{String(org_id, "")}}
              AND (StatusCode = 'Error' OR (SpanAttributes['http.status_code'] != '' AND toUInt16OrZero(SpanAttributes['http.status_code']) >= 500))
              {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
              {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
              GROUP BY TraceId
            )
          {% end %}
          {% if defined(http_method) %} AND SpanAttributes['http.method'] = {{String(http_method)}} {% end %}
          {% if defined(http_status_code) %} AND SpanAttributes['http.status_code'] = {{String(http_status_code)}} {% end %}
          {% if defined(deployment_env) %} AND ResourceAttributes['deployment.environment'] = {{String(deployment_env)}} {% end %}
          GROUP BY TraceId
        )
`},
		},
	},

	// ── 16. service_overview ──────────────────────────────────────────────────
	{
		Name: "service_overview",
		Nodes: []PipeNode{
			{Name: "service_overview_node", SQL: `
        SELECT
          ServiceName AS serviceName,
          ResourceAttributes['deployment.environment'] AS environment,
          ResourceAttributes['deployment.commit_sha'] AS commitSha,
          count() AS throughput, countIf(StatusCode = 'Error') AS errorCount,
          count() AS spanCount,
          quantile(0.50)(Duration / 1000000) AS p50LatencyMs,
          quantile(0.95)(Duration / 1000000) AS p95LatencyMs,
          quantile(0.99)(Duration / 1000000) AS p99LatencyMs,
          countIf(TraceState LIKE '%th:%') AS sampledSpanCount,
          countIf(TraceState = '' OR TraceState NOT LIKE '%th:%') AS unsampledSpanCount,
          anyIf(extract(TraceState, 'th:([0-9a-f]+)'), TraceState LIKE '%th:%') AS dominantThreshold
        FROM traces
        WHERE ParentSpanId = '' AND OrgId = {{String(org_id, "")}}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        {% if defined(environments) %} AND ResourceAttributes['deployment.environment'] IN splitByChar(',', {{String(environments, "")}}) {% end %}
        {% if defined(commit_shas) %} AND ResourceAttributes['deployment.commit_sha'] IN splitByChar(',', {{String(commit_shas, "")}}) {% end %}
        GROUP BY serviceName, environment, commitSha ORDER BY throughput DESC LIMIT 100
`},
		},
	},

	// ── 17. services_facets ───────────────────────────────────────────────────
	{
		Name: "services_facets",
		Nodes: []PipeNode{
			{Name: "environment_facets", SQL: `
        SELECT ResourceAttributes['deployment.environment'] AS name, count() AS count, 'environment' AS facetType
        FROM traces WHERE ResourceAttributes['deployment.environment'] != '' AND OrgId = {{String(org_id, "")}}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY name ORDER BY count DESC LIMIT 50
`},
			{Name: "commit_sha_facets", SQL: `
        SELECT ResourceAttributes['deployment.commit_sha'] AS name, count() AS count, 'commitSha' AS facetType
        FROM traces WHERE ResourceAttributes['deployment.commit_sha'] != '' AND OrgId = {{String(org_id, "")}}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY name ORDER BY count DESC LIMIT 50
`},
			{Name: "combined_facets", SQL: `
        SELECT * FROM environment_facets UNION ALL SELECT * FROM commit_sha_facets
`},
		},
	},

	// ── 18. errors_by_type ────────────────────────────────────────────────────
	{
		Name: "errors_by_type",
		Nodes: []PipeNode{
			{Name: "errors_by_type_node", SQL: `
        SELECT
          if(StatusMessage = '', 'Unknown Error', StatusMessage) AS errorType,
          count() AS count,
          uniq(ServiceName) AS affectedServicesCount,
          min(Timestamp) AS firstSeen, max(Timestamp) AS lastSeen,
          groupUniqArray(ServiceName) AS affectedServices
        FROM traces
        WHERE StatusCode = 'Error' AND OrgId = {{String(org_id, "")}}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        {% if defined(services) %} AND ServiceName IN splitByChar(',', {{String(services, "")}}) {% end %}
        {% if defined(deployment_envs) %} AND ResourceAttributes['deployment.environment'] IN splitByChar(',', {{String(deployment_envs, "")}}) {% end %}
        {% if defined(error_types) %} AND if(StatusMessage = '', 'Unknown Error', StatusMessage) IN splitByChar(',', {{String(error_types, "")}}) {% end %}
        {% if defined(exclude_spam_patterns) %}
          AND NOT arrayExists(x -> positionCaseInsensitive(if(StatusMessage = '', 'Unknown Error', StatusMessage), x) > 0, splitByChar(',', {{String(exclude_spam_patterns, "")}}))
        {% end %}
        GROUP BY errorType ORDER BY count DESC LIMIT {{Int32(limit, 50)}}
`},
		},
	},

	// ── 19. error_detail_traces ───────────────────────────────────────────────
	{
		Name: "error_detail_traces",
		Nodes: []PipeNode{
			{Name: "error_trace_ids", SQL: `
        SELECT DISTINCT TraceId
        FROM traces
        WHERE StatusCode = 'Error' AND OrgId = {{String(org_id, "")}}
          AND if(StatusMessage = '', 'Unknown Error', StatusMessage) = {{String(error_type)}}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        {% if defined(services) %} AND ServiceName IN splitByChar(',', {{String(services, "")}}) {% end %}
        {% if defined(exclude_spam_patterns) %}
          AND NOT arrayExists(x -> positionCaseInsensitive(if(StatusMessage = '', 'Unknown Error', StatusMessage), x) > 0, splitByChar(',', {{String(exclude_spam_patterns, "")}}))
        {% end %}
        ORDER BY Timestamp DESC LIMIT {{Int32(limit, 10)}}
`},
			{Name: "error_detail_traces_node", SQL: `
        SELECT
          t.TraceId AS traceId,
          min(t.Timestamp) AS startTime,
          intDiv(max(toUnixTimestamp64Nano(t.Timestamp) + t.Duration) - min(toUnixTimestamp64Nano(t.Timestamp)), 1000) AS durationMicros,
          count() AS spanCount,
          groupUniqArray(t.ServiceName) AS services,
          argMin(
            if(t.SpanName LIKE 'http.server %' AND t.SpanAttributes['http.route'] != '',
               concat(replaceOne(t.SpanName, 'http.server ', ''), ' ', t.SpanAttributes['http.route']),
               t.SpanName),
            if(t.ParentSpanId = '', 0, 1)
          ) AS rootSpanName,
          anyIf(t.StatusMessage, t.StatusCode = 'Error' AND t.StatusMessage != '') AS errorMessage
        FROM traces AS t
        INNER JOIN error_trace_ids AS e ON t.TraceId = e.TraceId AND t.OrgId = {{String(org_id, "")}}
        GROUP BY t.TraceId ORDER BY startTime DESC
`},
		},
	},

	// ── 20. errors_facets ─────────────────────────────────────────────────────
	{
		Name: "errors_facets",
		Nodes: []PipeNode{
			{Name: "error_base", SQL: `
        SELECT ServiceName AS serviceName,
               ResourceAttributes['deployment.environment'] AS deploymentEnv,
               if(StatusMessage = '', 'Unknown Error', StatusMessage) AS errorType
        FROM traces
        WHERE StatusCode = 'Error' AND OrgId = {{String(org_id, "")}}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        {% if defined(exclude_spam_patterns) %}
          AND NOT arrayExists(x -> positionCaseInsensitive(if(StatusMessage = '', 'Unknown Error', StatusMessage), x) > 0, splitByChar(',', {{String(exclude_spam_patterns, "")}}))
        {% end %}
`},
			{Name: "service_error_facets", SQL: `
        SELECT serviceName AS name, count() AS count, 'service' AS facetType
        FROM error_base WHERE 1=1
        {% if defined(deployment_envs) %} AND deploymentEnv IN splitByChar(',', {{String(deployment_envs, "")}}) {% end %}
        {% if defined(error_types) %} AND errorType IN splitByChar(',', {{String(error_types, "")}}) {% end %}
        GROUP BY serviceName ORDER BY count DESC
`},
			{Name: "environment_error_facets", SQL: `
        SELECT deploymentEnv AS name, count() AS count, 'deploymentEnv' AS facetType
        FROM error_base WHERE deploymentEnv != ''
        {% if defined(services) %} AND serviceName IN splitByChar(',', {{String(services, "")}}) {% end %}
        {% if defined(error_types) %} AND errorType IN splitByChar(',', {{String(error_types, "")}}) {% end %}
        GROUP BY deploymentEnv ORDER BY count DESC
`},
			{Name: "error_type_facets", SQL: `
        SELECT errorType AS name, count() AS count, 'errorType' AS facetType
        FROM error_base WHERE 1=1
        {% if defined(services) %} AND serviceName IN splitByChar(',', {{String(services, "")}}) {% end %}
        {% if defined(deployment_envs) %} AND deploymentEnv IN splitByChar(',', {{String(deployment_envs, "")}}) {% end %}
        GROUP BY errorType ORDER BY count DESC LIMIT 50
`},
			{Name: "combined_error_facets", SQL: `
        SELECT * FROM service_error_facets
        UNION ALL SELECT * FROM environment_error_facets
        UNION ALL SELECT * FROM error_type_facets
`},
		},
	},

	// ── 21. errors_summary ────────────────────────────────────────────────────
	{
		Name: "errors_summary",
		Nodes: []PipeNode{
			{Name: "errors_summary_node", SQL: `
        SELECT
          countIf(StatusCode = 'Error') AS totalErrors,
          count() AS totalSpans,
          if(totalSpans > 0, round(totalErrors / totalSpans * 100, 2), 0) AS errorRate,
          uniqIf(ServiceName, StatusCode = 'Error') AS affectedServicesCount,
          uniqIf(TraceId, StatusCode = 'Error') AS affectedTracesCount
        FROM traces WHERE 1=1 AND OrgId = {{String(org_id, "")}}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        {% if defined(services) %} AND ServiceName IN splitByChar(',', {{String(services, "")}}) {% end %}
        {% if defined(deployment_envs) %} AND ResourceAttributes['deployment.environment'] IN splitByChar(',', {{String(deployment_envs, "")}}) {% end %}
        {% if defined(error_types) %} AND (StatusCode != 'Error' OR if(StatusMessage = '', 'Unknown Error', StatusMessage) IN splitByChar(',', {{String(error_types, "")}})  ) {% end %}
        {% if defined(exclude_spam_patterns) %}
          AND (StatusCode != 'Error' OR NOT arrayExists(x -> positionCaseInsensitive(if(StatusMessage = '', 'Unknown Error', StatusMessage), x) > 0, splitByChar(',', {{String(exclude_spam_patterns, "")}})))
        {% end %}
`},
		},
	},

	// ── 22. service_apdex_time_series ─────────────────────────────────────────
	{
		Name: "service_apdex_time_series",
		Nodes: []PipeNode{
			{Name: "service_apdex_time_series_node", SQL: `
        SELECT
          toStartOfInterval(Timestamp, INTERVAL {{Int32(bucket_seconds, 60)}} second) AS bucket,
          count() AS totalCount,
          countIf(Duration / 1000000 < {{Float64(apdex_threshold_ms, 500)}}) AS satisfiedCount,
          countIf(Duration / 1000000 >= {{Float64(apdex_threshold_ms, 500)}} AND Duration / 1000000 < {{Float64(apdex_threshold_ms, 500)}} * 4) AS toleratingCount,
          if(count() > 0,
            round((countIf(Duration / 1000000 < {{Float64(apdex_threshold_ms, 500)}})
              + countIf(Duration / 1000000 >= {{Float64(apdex_threshold_ms, 500)}} AND Duration / 1000000 < {{Float64(apdex_threshold_ms, 500)}} * 4) * 0.5
            ) / count(), 4), 0) AS apdexScore
        FROM traces
        WHERE ParentSpanId = '' AND OrgId = {{String(org_id, "")}} AND ServiceName = {{String(service_name)}}
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        GROUP BY bucket ORDER BY bucket ASC
`},
		},
	},

	// ── 23. custom_traces_timeseries ──────────────────────────────────────────
	{
		Name: "custom_traces_timeseries",
		Nodes: []PipeNode{
			{Name: "custom_traces_ts_node", SQL: `
        SELECT
          toStartOfInterval(Timestamp, INTERVAL {{Int32(bucket_seconds, 60)}} SECOND) AS bucket,
          {% if defined(group_by_service) %}ServiceName
          {% elif defined(group_by_span_name) %}SpanName
          {% elif defined(group_by_status_code) %}StatusCode
          {% elif defined(group_by_attribute) %}SpanAttributes[{{String(group_by_attribute)}}]
          {% else %}'all'
          {% end %} AS groupName,
          count() AS count,
          avg(Duration) / 1000000 AS avgDuration,
          quantile(0.5)(Duration) / 1000000 AS p50Duration,
          quantile(0.95)(Duration) / 1000000 AS p95Duration,
          quantile(0.99)(Duration) / 1000000 AS p99Duration,
          if(count() > 0, countIf(StatusCode = 'Error') * 100.0 / count(), 0) AS errorRate
        FROM traces
        WHERE Timestamp >= {{DateTime(start_time)}}
          AND OrgId = {{String(org_id, "")}}
          AND Timestamp <= {{DateTime(end_time)}}
          {% if defined(service_name) %}AND ServiceName = {{String(service_name)}}{% end %}
          {% if defined(span_name) %}AND SpanName = {{String(span_name)}}{% end %}
          {% if defined(root_only) %}AND ParentSpanId = ''{% end %}
          {% if defined(environments) %}AND ResourceAttributes['deployment.environment'] IN splitByChar(',', {{String(environments, "")}}){% end %}
          {% if defined(commit_shas) %}AND ResourceAttributes['deployment.commit_sha'] IN splitByChar(',', {{String(commit_shas, "")}}){% end %}
          {% if defined(attribute_filter_key) %}AND SpanAttributes[{{String(attribute_filter_key)}}] = {{String(attribute_filter_value, '')}}{% end %}
          {% if defined(resource_filter_key) %}AND ResourceAttributes[{{String(resource_filter_key)}}] = {{String(resource_filter_value, '')}}{% end %}
        GROUP BY bucket, groupName ORDER BY bucket ASC, groupName ASC
`},
		},
	},

	// ── 24. custom_traces_breakdown ───────────────────────────────────────────
	{
		Name: "custom_traces_breakdown",
		Nodes: []PipeNode{
			{Name: "custom_traces_breakdown_node", SQL: `
        SELECT
          {% if defined(group_by_service) %}ServiceName
          {% elif defined(group_by_span_name) %}SpanName
          {% elif defined(group_by_status_code) %}StatusCode
          {% elif defined(group_by_http_method) %}SpanAttributes['http.method']
          {% elif defined(group_by_attribute) %}SpanAttributes[{{String(group_by_attribute)}}]
          {% else %}ServiceName
          {% end %} AS name,
          count() AS count,
          avg(Duration) / 1000000 AS avgDuration,
          quantile(0.5)(Duration) / 1000000 AS p50Duration,
          quantile(0.95)(Duration) / 1000000 AS p95Duration,
          quantile(0.99)(Duration) / 1000000 AS p99Duration,
          if(count() > 0, countIf(StatusCode = 'Error') * 100.0 / count(), 0) AS errorRate
        FROM traces
        WHERE Timestamp >= {{DateTime(start_time)}}
          AND OrgId = {{String(org_id, "")}}
          AND Timestamp <= {{DateTime(end_time)}}
          {% if defined(service_name) %}AND ServiceName = {{String(service_name)}}{% end %}
          {% if defined(span_name) %}AND SpanName = {{String(span_name)}}{% end %}
          {% if defined(root_only) %}AND ParentSpanId = ''{% end %}
          {% if defined(environments) %}AND ResourceAttributes['deployment.environment'] IN splitByChar(',', {{String(environments, "")}}){% end %}
          {% if defined(commit_shas) %}AND ResourceAttributes['deployment.commit_sha'] IN splitByChar(',', {{String(commit_shas, "")}}){% end %}
          {% if defined(attribute_filter_key) %}AND SpanAttributes[{{String(attribute_filter_key)}}] = {{String(attribute_filter_value, '')}}{% end %}
          {% if defined(resource_filter_key) %}AND ResourceAttributes[{{String(resource_filter_key)}}] = {{String(resource_filter_value, '')}}{% end %}
        GROUP BY name ORDER BY count DESC LIMIT {{Int32(limit, 10)}}
`},
		},
	},

	// ── 25. custom_logs_timeseries ────────────────────────────────────────────
	{
		Name: "custom_logs_timeseries",
		Nodes: []PipeNode{
			{Name: "custom_logs_ts_node", SQL: `
        SELECT
          toStartOfInterval(Timestamp, INTERVAL {{Int32(bucket_seconds, 60)}} SECOND) AS bucket,
          {% if defined(group_by_service) %}ServiceName
          {% elif defined(group_by_severity) %}SeverityText
          {% else %}'all'
          {% end %} AS groupName,
          count() AS count
        FROM logs
        WHERE Timestamp >= {{DateTime(start_time)}}
          AND OrgId = {{String(org_id, "")}}
          AND Timestamp <= {{DateTime(end_time)}}
          {% if defined(service_name) %}AND ServiceName = {{String(service_name)}}{% end %}
          {% if defined(severity) %}AND SeverityText = {{String(severity)}}{% end %}
        GROUP BY bucket, groupName ORDER BY bucket ASC, groupName ASC
`},
		},
	},

	// ── 26. custom_logs_breakdown ─────────────────────────────────────────────
	{
		Name: "custom_logs_breakdown",
		Nodes: []PipeNode{
			{Name: "custom_logs_breakdown_node", SQL: `
        SELECT
          {% if defined(group_by_service) %}ServiceName
          {% elif defined(group_by_severity) %}SeverityText
          {% else %}ServiceName
          {% end %} AS name,
          count() AS count
        FROM logs
        WHERE Timestamp >= {{DateTime(start_time)}}
          AND OrgId = {{String(org_id, "")}}
          AND Timestamp <= {{DateTime(end_time)}}
          {% if defined(service_name) %}AND ServiceName = {{String(service_name)}}{% end %}
          {% if defined(severity) %}AND SeverityText = {{String(severity)}}{% end %}
        GROUP BY name ORDER BY count DESC LIMIT {{Int32(limit, 10)}}
`},
		},
	},

	// ── 27. custom_metrics_breakdown ──────────────────────────────────────────
	{
		Name: "custom_metrics_breakdown",
		Nodes: []PipeNode{
			{Name: "sum_breakdown", SQL: `
        SELECT ServiceName AS name, avg(Value) AS avgValue, sum(Value) AS sumValue, count() AS count
        FROM metrics_sum
        WHERE MetricName = {{String(metric_name)}} AND OrgId = {{String(org_id, "")}}
          AND TimeUnix >= {{DateTime(start_time)}} AND TimeUnix <= {{DateTime(end_time)}}
        GROUP BY ServiceName
`},
			{Name: "gauge_breakdown", SQL: `
        SELECT ServiceName AS name, avg(Value) AS avgValue, sum(Value) AS sumValue, count() AS count
        FROM metrics_gauge
        WHERE MetricName = {{String(metric_name)}} AND OrgId = {{String(org_id, "")}}
          AND TimeUnix >= {{DateTime(start_time)}} AND TimeUnix <= {{DateTime(end_time)}}
        GROUP BY ServiceName
`},
			{Name: "histogram_breakdown", SQL: `
        SELECT ServiceName AS name, if(sum(Count) > 0, sum(Sum) / sum(Count), 0) AS avgValue, sum(Sum) AS sumValue, sum(Count) AS count
        FROM metrics_histogram
        WHERE MetricName = {{String(metric_name)}} AND OrgId = {{String(org_id, "")}}
          AND TimeUnix >= {{DateTime(start_time)}} AND TimeUnix <= {{DateTime(end_time)}}
        GROUP BY ServiceName
`},
			{Name: "exp_histogram_breakdown", SQL: `
        SELECT ServiceName AS name, if(sum(Count) > 0, sum(Sum) / sum(Count), 0) AS avgValue, sum(Sum) AS sumValue, sum(Count) AS count
        FROM metrics_exponential_histogram
        WHERE MetricName = {{String(metric_name)}} AND OrgId = {{String(org_id, "")}}
          AND TimeUnix >= {{DateTime(start_time)}} AND TimeUnix <= {{DateTime(end_time)}}
        GROUP BY ServiceName
`},
			{Name: "combined_breakdown", SQL: `
        SELECT name, avgValue, sumValue, count FROM (
          {% if not defined(metric_type) or metric_type == 'sum' %}SELECT * FROM sum_breakdown{% else %}SELECT name, avgValue, sumValue, count FROM sum_breakdown WHERE 1=0{% end %}
          UNION ALL
          {% if not defined(metric_type) or metric_type == 'gauge' %}SELECT * FROM gauge_breakdown{% else %}SELECT name, avgValue, sumValue, count FROM gauge_breakdown WHERE 1=0{% end %}
          UNION ALL
          {% if not defined(metric_type) or metric_type == 'histogram' %}SELECT * FROM histogram_breakdown{% else %}SELECT name, avgValue, sumValue, count FROM histogram_breakdown WHERE 1=0{% end %}
          UNION ALL
          {% if not defined(metric_type) or metric_type == 'exponential_histogram' %}SELECT * FROM exp_histogram_breakdown{% else %}SELECT name, avgValue, sumValue, count FROM exp_histogram_breakdown WHERE 1=0{% end %}
        )
        ORDER BY count DESC LIMIT {{Int32(limit, 10)}}
`},
		},
	},

	// ── 28. service_dependencies ──────────────────────────────────────────────
	{
		Name: "service_dependencies",
		Nodes: []PipeNode{
			{Name: "peer_service_edges", SQL: `
        SELECT
          ServiceName AS sourceService, SpanAttributes['peer.service'] AS targetService,
          count() AS callCount, countIf(StatusCode = 'Error') AS errorCount,
          avg(Duration / 1000000) AS avgDurationMs, quantile(0.95)(Duration / 1000000) AS p95DurationMs,
          countIf(TraceState LIKE '%th:%') AS sampledSpanCount,
          countIf(TraceState = '' OR TraceState NOT LIKE '%th:%') AS unsampledSpanCount,
          anyIf(extract(TraceState, 'th:([0-9a-f]+)'), TraceState LIKE '%th:%') AS dominantThreshold
        FROM traces
        WHERE OrgId = {{String(org_id, "")}} AND SpanKind = 'Client' AND SpanAttributes['peer.service'] != ''
        {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
        {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
        {% if defined(deployment_env) %} AND ResourceAttributes['deployment.environment'] = {{String(deployment_env)}} {% end %}
        GROUP BY sourceService, targetService
`},
			{Name: "join_edges", SQL: `
        SELECT
          p.ServiceName AS sourceService, c.ServiceName AS targetService,
          count() AS callCount, countIf(c.StatusCode = 'Error') AS errorCount,
          avg(c.Duration / 1000000) AS avgDurationMs, quantile(0.95)(c.Duration / 1000000) AS p95DurationMs,
          countIf(c.TraceState LIKE '%th:%') AS sampledSpanCount,
          countIf(c.TraceState = '' OR c.TraceState NOT LIKE '%th:%') AS unsampledSpanCount,
          anyIf(extract(c.TraceState, 'th:([0-9a-f]+)'), c.TraceState LIKE '%th:%') AS dominantThreshold
        FROM (
          SELECT TraceId, SpanId, ServiceName
          FROM traces WHERE OrgId = {{String(org_id, "")}} AND SpanKind IN ('Client', 'Producer') AND SpanAttributes['peer.service'] = ''
          {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
          {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
          {% if defined(deployment_env) %} AND ResourceAttributes['deployment.environment'] = {{String(deployment_env)}} {% end %}
        ) AS p
        INNER JOIN (
          SELECT TraceId, ParentSpanId, ServiceName, Duration, StatusCode, TraceState
          FROM traces WHERE OrgId = {{String(org_id, "")}} AND ParentSpanId != ''
          {% if defined(start_time) %} AND Timestamp >= {{DateTime(start_time, "2023-01-01 00:00:00")}} {% end %}
          {% if defined(end_time) %} AND Timestamp <= {{DateTime(end_time, "2099-12-31 23:59:59")}} {% end %}
          {% if defined(deployment_env) %} AND ResourceAttributes['deployment.environment'] = {{String(deployment_env)}} {% end %}
        ) AS c ON p.SpanId = c.ParentSpanId AND p.TraceId = c.TraceId
        WHERE p.ServiceName != c.ServiceName
        GROUP BY sourceService, targetService
`},
			{Name: "merged_edges", SQL: `
        SELECT sourceService, targetService,
          sum(callCount) AS callCount, sum(errorCount) AS errorCount,
          avg(avgDurationMs) AS avgDurationMs, max(p95DurationMs) AS p95DurationMs,
          sum(sampledSpanCount) AS sampledSpanCount, sum(unsampledSpanCount) AS unsampledSpanCount,
          any(dominantThreshold) AS dominantThreshold
        FROM (SELECT * FROM peer_service_edges UNION ALL SELECT * FROM join_edges)
        GROUP BY sourceService, targetService ORDER BY callCount DESC LIMIT 200
`},
		},
	},

	// ── 29. span_attribute_keys ───────────────────────────────────────────────
	{
		Name: "span_attribute_keys",
		Nodes: []PipeNode{
			{Name: "span_attribute_keys_node", SQL: `
        SELECT arrayJoin(mapKeys(SpanAttributes)) AS attributeKey, count() AS usageCount
        FROM traces
        WHERE OrgId = {{String(org_id, "")}}
          AND Timestamp >= {{DateTime(start_time)}} AND Timestamp <= {{DateTime(end_time)}}
          AND SpanAttributes != map()
        GROUP BY attributeKey ORDER BY usageCount DESC LIMIT {{Int32(limit, 200)}}
`},
		},
	},

	// ── 30. span_attribute_values ─────────────────────────────────────────────
	{
		Name: "span_attribute_values",
		Nodes: []PipeNode{
			{Name: "span_attribute_values_node", SQL: `
        SELECT SpanAttributes[{{String(attribute_key)}}] AS attributeValue, count() AS usageCount
        FROM traces
        WHERE OrgId = {{String(org_id, "")}}
          AND Timestamp >= {{DateTime(start_time)}} AND Timestamp <= {{DateTime(end_time)}}
          AND SpanAttributes[{{String(attribute_key)}}] != ''
        GROUP BY attributeValue ORDER BY usageCount DESC LIMIT {{Int32(limit, 50)}}
`},
		},
	},

	// ── 31. resource_attribute_keys ───────────────────────────────────────────
	{
		Name: "resource_attribute_keys",
		Nodes: []PipeNode{
			{Name: "resource_attribute_keys_node", SQL: `
        SELECT arrayJoin(mapKeys(ResourceAttributes)) AS attributeKey, count() AS usageCount
        FROM traces
        WHERE OrgId = {{String(org_id, "")}}
          AND Timestamp >= {{DateTime(start_time)}} AND Timestamp <= {{DateTime(end_time)}}
          AND ResourceAttributes != map()
        GROUP BY attributeKey ORDER BY usageCount DESC LIMIT {{Int32(limit, 200)}}
`},
		},
	},

	// ── 32. resource_attribute_values ─────────────────────────────────────────
	{
		Name: "resource_attribute_values",
		Nodes: []PipeNode{
			{Name: "resource_attribute_values_node", SQL: `
        SELECT ResourceAttributes[{{String(attribute_key)}}] AS attributeValue, count() AS usageCount
        FROM traces
        WHERE OrgId = {{String(org_id, "")}}
          AND Timestamp >= {{DateTime(start_time)}} AND Timestamp <= {{DateTime(end_time)}}
          AND ResourceAttributes[{{String(attribute_key)}}] != ''
        GROUP BY attributeValue ORDER BY usageCount DESC LIMIT {{Int32(limit, 50)}}
`},
		},
	},
}
