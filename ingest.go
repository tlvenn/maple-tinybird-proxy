package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// validDatasources maps Tinybird datasource names → ClickHouse table names.
var validDatasources = map[string]string{
	"logs":                          "logs",
	"traces":                        "traces",
	"metrics_sum":                   "metrics_sum",
	"metrics_gauge":                 "metrics_gauge",
	"metrics_histogram":             "metrics_histogram",
	"metrics_exponential_histogram": "metrics_exponential_histogram",
}

// ─── Batch Ingest Buffer ──────────────────────────────────────────────────────

const (
	batchMaxRows     = 5000
	batchMaxBytes    = 8 * 1024 * 1024 // 8 MB
	batchFlushPeriod = 500 * time.Millisecond
)

type batchBuffer struct {
	mu      sync.Mutex
	rows    []json.RawMessage
	size    int
	table   string
	ch      *ClickHouseClient
	flushCh chan struct{}
	done    chan struct{}
}

var (
	buffers   = map[string]*batchBuffer{}
	buffersMu sync.RWMutex
)

func getOrCreateBuffer(table string, ch *ClickHouseClient) *batchBuffer {
	buffersMu.RLock()
	b, ok := buffers[table]
	buffersMu.RUnlock()
	if ok {
		return b
	}

	buffersMu.Lock()
	defer buffersMu.Unlock()
	if b, ok = buffers[table]; ok {
		return b
	}
	b = &batchBuffer{
		table:   table,
		ch:      ch,
		flushCh: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	go b.flushLoop()
	buffers[table] = b
	return b
}

func (b *batchBuffer) add(rows []json.RawMessage) {
	b.mu.Lock()
	b.rows = append(b.rows, rows...)
	for _, r := range rows {
		b.size += len(r) + 1
	}
	shouldFlush := len(b.rows) >= batchMaxRows || b.size >= batchMaxBytes
	b.mu.Unlock()

	if shouldFlush {
		select {
		case b.flushCh <- struct{}{}:
		default:
		}
	}
}

func (b *batchBuffer) stop() {
	close(b.done)
}

func (b *batchBuffer) flushLoop() {
	ticker := time.NewTicker(batchFlushPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-b.done:
			b.flush()
			return
		case <-ticker.C:
			b.flush()
		case <-b.flushCh:
			b.flush()
		}
	}
}

func resetBuffers() {
	buffersMu.Lock()
	defer buffersMu.Unlock()
	for _, b := range buffers {
		b.stop()
	}
	buffers = map[string]*batchBuffer{}
}

func (b *batchBuffer) flush() {
	b.mu.Lock()
	if len(b.rows) == 0 {
		b.mu.Unlock()
		return
	}
	rows := b.rows
	b.rows = nil
	b.size = 0
	b.mu.Unlock()

	var buf bytes.Buffer
	for _, r := range rows {
		buf.Write(r)
		buf.WriteByte('\n')
	}

	if err := b.ch.Insert(b.table, buf.Bytes()); err != nil {
		log.Printf("ERROR ingest flush %s: %v (%d rows dropped)", b.table, err, len(rows))
	}
}

// ─── Handler ──────────────────────────────────────────────────────────────────

func handleIngest(ch *ClickHouseClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 64*1024*1024) // 64 MB max

		dsName := r.URL.Query().Get("name")
		if dsName == "" {
			writeJSONError(w, http.StatusBadRequest, "missing 'name' query parameter")
			return
		}

		tableName, ok := validDatasources[dsName]
		if !ok {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("unknown datasource: %s", dsName))
			return
		}

		// Parse NDJSON body
		var rows []json.RawMessage
		scanner := bufio.NewScanner(r.Body)
		scanner.Buffer(make([]byte, 512*1024), 16*1024*1024) // 16 MB max line

		for scanner.Scan() {
			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}
			// Validate JSON
			if !json.Valid(line) {
				writeJSONError(w, http.StatusBadRequest, "invalid JSON in NDJSON body")
				return
			}
			cp := make([]byte, len(line))
			copy(cp, line)
			rows = append(rows, json.RawMessage(cp))
		}

		if err := scanner.Err(); err != nil {
			writeJSONError(w, http.StatusBadRequest, "failed to read request body")
			return
		}

		if len(rows) == 0 {
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"successful_rows":0,"quarantined_rows":0}`)
			return
		}

		buf := getOrCreateBuffer(tableName, ch)
		buf.add(rows)

		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"successful_rows":%d,"quarantined_rows":0}`, len(rows))
	}
}

func splitStatements(sql string) []string {
	// Split on semicolons that end a line (simplistic but works for our schema)
	var stmts []string
	var cur strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		cur.WriteString(line)
		cur.WriteByte('\n')
		trimmed := strings.TrimSpace(line)
		if strings.HasSuffix(trimmed, ";") {
			stmts = append(stmts, strings.TrimSuffix(strings.TrimSpace(cur.String()), ";"))
			cur.Reset()
		}
	}
	if s := strings.TrimSpace(cur.String()); s != "" {
		stmts = append(stmts, s)
	}
	return stmts
}
