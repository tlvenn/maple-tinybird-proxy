package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// ClickHouseClient is a thin HTTP client for ClickHouse.
type ClickHouseClient struct {
	baseURL  string
	database string
	user     string
	password string
	client   *http.Client
}

func NewClickHouseClient(baseURL, database, user, password string) *ClickHouseClient {
	return &ClickHouseClient{
		baseURL:  strings.TrimSuffix(baseURL, "/"),
		database: database,
		user:     user,
		password: password,
		client: &http.Client{
			Timeout: 120 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        200,
				MaxIdleConnsPerHost: 200,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// CHQueryResponse is the ClickHouse FORMAT JSON response.
type CHQueryResponse struct {
	Data       []map[string]interface{} `json:"data"`
	Rows       int64                    `json:"rows"`
	Statistics struct {
		Elapsed   float64 `json:"elapsed"`
		RowsRead  int64   `json:"rows_read"`
		BytesRead int64   `json:"bytes_read"`
	} `json:"statistics"`
}

// Query executes a SELECT and returns JSON-parsed rows.
func (c *ClickHouseClient) Query(sql string) (*CHQueryResponse, error) {
	fullSQL := sql + "\nFORMAT JSON"
	return c.execQuery(fullSQL)
}

func (c *ClickHouseClient) execQuery(sql string) (*CHQueryResponse, error) {
	reqURL := c.buildURL(map[string]string{
		"database": c.database,
	})

	req, err := http.NewRequest(http.MethodPost, reqURL, strings.NewReader(sql))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result CHQueryResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("parse response: %w (body: %.200s)", err, body)
	}
	return &result, nil
}

// Insert bulk-inserts NDJSON rows into a table.
func (c *ClickHouseClient) Insert(table string, ndjson []byte) error {
	sql := fmt.Sprintf("INSERT INTO `%s`.`%s` FORMAT JSONEachRow", c.database, table)
	reqURL := c.buildURL(map[string]string{
		"database": c.database,
		"query":    sql,
	})

	req, err := http.NewRequest(http.MethodPost, reqURL, bytes.NewReader(ndjson))
	if err != nil {
		return fmt.Errorf("build insert request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	if c.user != "" {
		req.SetBasicAuth(c.user, c.password)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("execute insert: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse insert %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (c *ClickHouseClient) buildURL(queryParams map[string]string) string {
	q := url.Values{}
	for k, v := range queryParams {
		q.Set(k, v)
	}
	return c.baseURL + "/?" + q.Encode()
}

// Ping checks ClickHouse connectivity.
func (c *ClickHouseClient) Ping() error {
	resp, err := c.client.Get(c.baseURL + "/ping")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping: %d", resp.StatusCode)
	}
	return nil
}
