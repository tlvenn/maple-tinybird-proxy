package main

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

// ─── Config ───────────────────────────────────────────────────────────────────

type Config struct {
	ListenAddr    string // e.g. ":7181"
	ClickHouseURL string // e.g. "http://localhost:8123"
	ClickHouseDB  string // e.g. "default"
	CHUser        string
	CHPassword    string
	AuthToken     string // Bearer token; empty = no auth
}

func configFromEnv() Config {
	return Config{
		ListenAddr:    env("LISTEN_ADDR", ":7181"),
		ClickHouseURL: env("CLICKHOUSE_URL", "http://localhost:8123"),
		ClickHouseDB:  env("CLICKHOUSE_DB", "default"),
		CHUser:        env("CLICKHOUSE_USER", "default"),
		CHPassword:    env("CLICKHOUSE_PASSWORD", ""),
		AuthToken:     env("AUTH_TOKEN", ""),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	cfg := configFromEnv()

	ch := NewClickHouseClient(cfg.ClickHouseURL, cfg.ClickHouseDB, cfg.CHUser, cfg.CHPassword)

	// Wait for ClickHouse to be ready (useful in Docker Compose startup)
	for i := 0; i < 30; i++ {
		if err := ch.Ping(); err == nil {
			break
		}
		log.Printf("waiting for ClickHouse... (%d/30)", i+1)
		time.Sleep(2 * time.Second)
	}

	mux := buildMux(ch, cfg.AuthToken)

	log.Printf("maple-tinybird-proxy listening on %s (db=%s)", cfg.ListenAddr, cfg.ClickHouseDB)
	log.Printf("registered %d pipes", len(pipeRegistry))
	log.Fatal(http.ListenAndServe(cfg.ListenAddr, mux))
}

// ─── Mux Builder ─────────────────────────────────────────────────────────────

func buildMux(ch *ClickHouseClient, authToken string) *http.ServeMux {
	mux := http.NewServeMux()
	auth := authMiddleware(authToken)

	// ── Tinybird-compatible endpoints ────────────────────────────────────────
	mux.HandleFunc("POST /v0/events", auth(handleIngest(ch)))
	mux.HandleFunc("GET /v0/pipes/{name}", auth(handlePipe(ch)))

	// ── Admin / utility ──────────────────────────────────────────────────────
	mux.HandleFunc("GET /health", handleHealth(ch))

	mux.HandleFunc("POST /admin/schema", auth(func(w http.ResponseWriter, r *http.Request) {
		applySchema(ch, w)
	}))

	mux.HandleFunc("GET /admin/pipes", auth(func(w http.ResponseWriter, r *http.Request) {
		names := make([]string, 0, len(pipeRegistry))
		for k := range pipeRegistry {
			names = append(names, k)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"pipes": names, "count": len(names)})
	}))

	return mux
}

// ─── Health ──────────────────────────────────────────────────────────────────

func handleHealth(ch *ClickHouseClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := ch.Ping(); err != nil {
			log.Printf("health check failed: %v", err)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"status":"unhealthy"}`)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"ok"}`)
	}
}

// ─── Auth Middleware ──────────────────────────────────────────────────────────

func authMiddleware(token string) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if token == "" {
				next(w, r)
				return
			}
			expected := "Bearer " + token
			actual := r.Header.Get("Authorization")
			if subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) != 1 {
				writeJSONError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			next(w, r)
		}
	}
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	b, _ := json.Marshal(map[string]string{"error": msg})
	w.Write(b)
}

// ─── Schema ───────────────────────────────────────────────────────────────────

func applySchema(ch *ClickHouseClient, w http.ResponseWriter) {
	statements := splitStatements(schemaSQL)
	var errs []string
	for _, stmt := range statements {
		if _, err := ch.Query(stmt); err != nil {
			errs = append(errs, err.Error())
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if len(errs) > 0 {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{"errors": errs})
		return
	}
	fmt.Fprint(w, `{"ok":true}`)
}

// schemaSQL is the full ClickHouse DDL embedded at compile time.
// Generated from Maple's datasources.ts + materializations.ts.
const schemaSQL = `
-- ─────────────────────────────────────────────────────────────────
--  logs
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS logs (
    OrgId           LowCardinality(String),
    Timestamp       DateTime64(9),
    TimestampTime   DateTime,
    TraceId         String,
    SpanId          String,
    TraceFlags      UInt8,
    SeverityText    LowCardinality(String),
    SeverityNumber  UInt8,
    ServiceName     LowCardinality(String),
    Body            String,
    ResourceSchemaUrl String,
    ResourceAttributes Map(LowCardinality(String), String),
    ScopeSchemaUrl  String,
    ScopeName       String,
    ScopeVersion    String,
    ScopeAttributes Map(LowCardinality(String), String),
    LogAttributes   Map(LowCardinality(String), String)
) ENGINE = MergeTree()
PARTITION BY toDate(TimestampTime)
ORDER BY (OrgId, ServiceName, TimestampTime, Timestamp)
TTL toDate(TimestampTime) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ─────────────────────────────────────────────────────────────────
--  traces
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS traces (
    OrgId           LowCardinality(String),
    Timestamp       DateTime64(9),
    TraceId         String,
    SpanId          String,
    ParentSpanId    String,
    TraceState      String,
    SpanName        LowCardinality(String),
    SpanKind        LowCardinality(String),
    ServiceName     LowCardinality(String),
    ResourceSchemaUrl String,
    ResourceAttributes Map(LowCardinality(String), String),
    ScopeSchemaUrl  String,
    ScopeName       String,
    ScopeVersion    String,
    ScopeAttributes Map(LowCardinality(String), String),
    Duration        UInt64 DEFAULT 0,
    StatusCode      LowCardinality(String),
    StatusMessage   String,
    SpanAttributes  Map(LowCardinality(String), String),
    EventsTimestamp Array(DateTime64(9)),
    EventsName      Array(LowCardinality(String)),
    EventsAttributes Array(Map(LowCardinality(String), String)),
    LinksTraceId    Array(String),
    LinksSpanId     Array(String),
    LinksTraceState Array(String),
    LinksAttributes Array(Map(LowCardinality(String), String))
) ENGINE = MergeTree()
PARTITION BY toDate(Timestamp)
ORDER BY (OrgId, ServiceName, SpanName, toDateTime(Timestamp))
TTL toDate(Timestamp) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ─────────────────────────────────────────────────────────────────
--  service_usage  (SummingMergeTree — populated by MVs below)
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS service_usage (
    OrgId                       LowCardinality(String),
    ServiceName                 LowCardinality(String),
    Hour                        DateTime,
    LogCount                    UInt64,
    LogSizeBytes                UInt64,
    TraceCount                  UInt64,
    TraceSizeBytes              UInt64,
    SumMetricCount              UInt64,
    SumMetricSizeBytes          UInt64,
    GaugeMetricCount            UInt64,
    GaugeMetricSizeBytes        UInt64,
    HistogramMetricCount        UInt64,
    HistogramMetricSizeBytes    UInt64,
    ExpHistogramMetricCount     UInt64,
    ExpHistogramMetricSizeBytes UInt64
) ENGINE = SummingMergeTree()
ORDER BY (OrgId, ServiceName, Hour)
TTL Hour + INTERVAL 365 DAY;

-- ─────────────────────────────────────────────────────────────────
--  metrics_sum
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metrics_sum (
    OrgId           LowCardinality(String),
    ResourceAttributes Map(LowCardinality(String), String),
    ResourceSchemaUrl String,
    ScopeName       String,
    ScopeVersion    String,
    ScopeAttributes Map(LowCardinality(String), String),
    ScopeSchemaUrl  String,
    ServiceName     LowCardinality(String),
    MetricName      LowCardinality(String),
    MetricDescription LowCardinality(String),
    MetricUnit      LowCardinality(String),
    Attributes      Map(LowCardinality(String), String),
    StartTimeUnix   DateTime64(9),
    TimeUnix        DateTime64(9),
    Value           Float64,
    Flags           UInt32,
    ExemplarsTraceId Array(String),
    ExemplarsSpanId  Array(String),
    ExemplarsTimestamp Array(DateTime64(9)),
    ExemplarsValue   Array(Float64),
    ExemplarsFilteredAttributes Array(Map(LowCardinality(String), String)),
    AggregationTemporality Int32,
    IsMonotonic     Bool
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (OrgId, ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDate(TimeUnix) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- ─────────────────────────────────────────────────────────────────
--  metrics_gauge
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metrics_gauge (
    OrgId           LowCardinality(String),
    ResourceAttributes Map(LowCardinality(String), String),
    ResourceSchemaUrl String,
    ScopeName       String,
    ScopeVersion    String,
    ScopeAttributes Map(LowCardinality(String), String),
    ScopeSchemaUrl  String,
    ServiceName     LowCardinality(String),
    MetricName      LowCardinality(String),
    MetricDescription LowCardinality(String),
    MetricUnit      LowCardinality(String),
    Attributes      Map(LowCardinality(String), String),
    StartTimeUnix   DateTime64(9),
    TimeUnix        DateTime64(9),
    Value           Float64,
    Flags           UInt32,
    ExemplarsTraceId Array(String),
    ExemplarsSpanId  Array(String),
    ExemplarsTimestamp Array(DateTime64(9)),
    ExemplarsValue   Array(Float64),
    ExemplarsFilteredAttributes Array(Map(LowCardinality(String), String))
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (OrgId, ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDate(TimeUnix) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- ─────────────────────────────────────────────────────────────────
--  metrics_histogram
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metrics_histogram (
    OrgId           LowCardinality(String),
    ResourceAttributes Map(LowCardinality(String), String),
    ResourceSchemaUrl String,
    ScopeName       String,
    ScopeVersion    String,
    ScopeAttributes Map(LowCardinality(String), String),
    ScopeSchemaUrl  String,
    ServiceName     LowCardinality(String),
    MetricName      LowCardinality(String),
    MetricDescription LowCardinality(String),
    MetricUnit      LowCardinality(String),
    Attributes      Map(LowCardinality(String), String),
    StartTimeUnix   DateTime64(9),
    TimeUnix        DateTime64(9),
    Count           UInt64,
    Sum             Float64,
    BucketCounts    Array(UInt64),
    ExplicitBounds  Array(Float64),
    ExemplarsTraceId Array(String),
    ExemplarsSpanId  Array(String),
    ExemplarsTimestamp Array(DateTime64(9)),
    ExemplarsValue   Array(Float64),
    ExemplarsFilteredAttributes Array(Map(LowCardinality(String), String)),
    Flags           UInt32,
    Min             Nullable(Float64),
    Max             Nullable(Float64),
    AggregationTemporality Int32
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (OrgId, ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDate(TimeUnix) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- ─────────────────────────────────────────────────────────────────
--  metrics_exponential_histogram
-- ─────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS metrics_exponential_histogram (
    OrgId           LowCardinality(String),
    ResourceAttributes Map(LowCardinality(String), String),
    ResourceSchemaUrl String,
    ScopeName       String,
    ScopeVersion    String,
    ScopeAttributes Map(LowCardinality(String), String),
    ScopeSchemaUrl  String,
    ServiceName     LowCardinality(String),
    MetricName      LowCardinality(String),
    MetricDescription LowCardinality(String),
    MetricUnit      LowCardinality(String),
    Attributes      Map(LowCardinality(String), String),
    StartTimeUnix   DateTime64(9),
    TimeUnix        DateTime64(9),
    Count           UInt64,
    Sum             Float64,
    Scale           Int32,
    ZeroCount       UInt64,
    PositiveOffset  Int32,
    PositiveBucketCounts Array(UInt64),
    NegativeOffset  Int32,
    NegativeBucketCounts Array(UInt64),
    ExemplarsTraceId Array(String),
    ExemplarsSpanId  Array(String),
    ExemplarsTimestamp Array(DateTime64(9)),
    ExemplarsValue   Array(Float64),
    ExemplarsFilteredAttributes Array(Map(LowCardinality(String), String)),
    Flags           UInt32,
    Min             Nullable(Float64),
    Max             Nullable(Float64),
    AggregationTemporality Int32
) ENGINE = MergeTree()
PARTITION BY toDate(TimeUnix)
ORDER BY (OrgId, ServiceName, MetricName, Attributes, toUnixTimestamp64Nano(TimeUnix))
TTL toDate(TimeUnix) + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;

-- ─────────────────────────────────────────────────────────────────
--  Materialized Views → service_usage
-- ─────────────────────────────────────────────────────────────────
CREATE MATERIALIZED VIEW IF NOT EXISTS service_usage_logs_mv TO service_usage AS
SELECT OrgId, ServiceName, toStartOfHour(TimestampTime) AS Hour,
       count() AS LogCount, sum(length(Body) + 200) AS LogSizeBytes,
       0 AS TraceCount, 0 AS TraceSizeBytes,
       0 AS SumMetricCount, 0 AS SumMetricSizeBytes,
       0 AS GaugeMetricCount, 0 AS GaugeMetricSizeBytes,
       0 AS HistogramMetricCount, 0 AS HistogramMetricSizeBytes,
       0 AS ExpHistogramMetricCount, 0 AS ExpHistogramMetricSizeBytes
FROM logs GROUP BY OrgId, ServiceName, Hour;

CREATE MATERIALIZED VIEW IF NOT EXISTS service_usage_traces_mv TO service_usage AS
SELECT OrgId, ServiceName, toStartOfHour(toDateTime(Timestamp)) AS Hour,
       0 AS LogCount, 0 AS LogSizeBytes,
       count() AS TraceCount, sum(length(SpanName) + 300) AS TraceSizeBytes,
       0 AS SumMetricCount, 0 AS SumMetricSizeBytes,
       0 AS GaugeMetricCount, 0 AS GaugeMetricSizeBytes,
       0 AS HistogramMetricCount, 0 AS HistogramMetricSizeBytes,
       0 AS ExpHistogramMetricCount, 0 AS ExpHistogramMetricSizeBytes
FROM traces GROUP BY OrgId, ServiceName, Hour;

CREATE MATERIALIZED VIEW IF NOT EXISTS service_usage_metrics_sum_mv TO service_usage AS
SELECT OrgId, ServiceName, toStartOfHour(toDateTime(TimeUnix)) AS Hour,
       0 AS LogCount, 0 AS LogSizeBytes, 0 AS TraceCount, 0 AS TraceSizeBytes,
       count() AS SumMetricCount, count() * 150 AS SumMetricSizeBytes,
       0 AS GaugeMetricCount, 0 AS GaugeMetricSizeBytes,
       0 AS HistogramMetricCount, 0 AS HistogramMetricSizeBytes,
       0 AS ExpHistogramMetricCount, 0 AS ExpHistogramMetricSizeBytes
FROM metrics_sum GROUP BY OrgId, ServiceName, Hour;

CREATE MATERIALIZED VIEW IF NOT EXISTS service_usage_metrics_gauge_mv TO service_usage AS
SELECT OrgId, ServiceName, toStartOfHour(toDateTime(TimeUnix)) AS Hour,
       0 AS LogCount, 0 AS LogSizeBytes, 0 AS TraceCount, 0 AS TraceSizeBytes,
       0 AS SumMetricCount, 0 AS SumMetricSizeBytes,
       count() AS GaugeMetricCount, count() * 150 AS GaugeMetricSizeBytes,
       0 AS HistogramMetricCount, 0 AS HistogramMetricSizeBytes,
       0 AS ExpHistogramMetricCount, 0 AS ExpHistogramMetricSizeBytes
FROM metrics_gauge GROUP BY OrgId, ServiceName, Hour;

CREATE MATERIALIZED VIEW IF NOT EXISTS service_usage_metrics_histogram_mv TO service_usage AS
SELECT OrgId, ServiceName, toStartOfHour(toDateTime(TimeUnix)) AS Hour,
       0 AS LogCount, 0 AS LogSizeBytes, 0 AS TraceCount, 0 AS TraceSizeBytes,
       0 AS SumMetricCount, 0 AS SumMetricSizeBytes,
       0 AS GaugeMetricCount, 0 AS GaugeMetricSizeBytes,
       count() AS HistogramMetricCount, count() * 250 AS HistogramMetricSizeBytes,
       0 AS ExpHistogramMetricCount, 0 AS ExpHistogramMetricSizeBytes
FROM metrics_histogram GROUP BY OrgId, ServiceName, Hour;

CREATE MATERIALIZED VIEW IF NOT EXISTS service_usage_metrics_exp_histogram_mv TO service_usage AS
SELECT OrgId, ServiceName, toStartOfHour(toDateTime(TimeUnix)) AS Hour,
       0 AS LogCount, 0 AS LogSizeBytes, 0 AS TraceCount, 0 AS TraceSizeBytes,
       0 AS SumMetricCount, 0 AS SumMetricSizeBytes,
       0 AS GaugeMetricCount, 0 AS GaugeMetricSizeBytes,
       0 AS HistogramMetricCount, 0 AS HistogramMetricSizeBytes,
       count() AS ExpHistogramMetricCount, count() * 300 AS ExpHistogramMetricSizeBytes
FROM metrics_exponential_histogram GROUP BY OrgId, ServiceName, Hour;
`
