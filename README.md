# maple-tinybird-proxy

A drop-in replacement for [Tinybird](https://www.tinybird.co/) that lets [Maple](https://github.com/Makisuo/maple) run against a self-hosted ClickHouse instance. The proxy translates Tinybird's Events API and Pipes API into native ClickHouse queries, so Maple works without any Tinybird account or cloud dependency.

## How It Works

The proxy exposes two Tinybird-compatible endpoints:

- **`POST /v0/events?name=<datasource>`** -- accepts NDJSON and bulk-inserts into ClickHouse via buffered batches (5 000 rows / 8 MB / 500 ms flush).
- **`GET /v0/pipes/<name>.json`** -- compiles Maple's pipe definitions (parameterized SQL with Jinja-style templating) into ClickHouse queries and returns results in the Tinybird response envelope.

Maple's frontend talks to these endpoints exactly as it would to Tinybird -- no code changes required.

## Quickstart

```sh
docker compose up -d
```

This starts ClickHouse and the proxy. The proxy waits for ClickHouse to become healthy before accepting traffic.

Apply the schema:

```sh
curl -X POST http://localhost:7181/admin/schema
```

The proxy is now ready on port **7181**. Point Maple's Tinybird URL at `http://localhost:7181`.

## Configuration

All settings are read from environment variables:

| Variable              | Default                   | Description                                  |
|-----------------------|---------------------------|----------------------------------------------|
| `LISTEN_ADDR`         | `:7181`                   | Address and port the proxy listens on        |
| `CLICKHOUSE_URL`      | `http://localhost:8123`   | ClickHouse HTTP API URL                      |
| `CLICKHOUSE_DB`       | `default`                 | ClickHouse database name                     |
| `CLICKHOUSE_USER`     | `default`                 | ClickHouse user                              |
| `CLICKHOUSE_PASSWORD` | (empty)                   | ClickHouse password                          |
| `AUTH_TOKEN`          | (empty)                   | Bearer token for API auth; empty = no auth   |

## Endpoints

### Ingest

```
POST /v0/events?name=<datasource>
```

Accepts NDJSON. Valid datasource names: `logs`, `traces`, `metrics_sum`, `metrics_gauge`, `metrics_histogram`, `metrics_exponential_histogram`.

### Pipes

```
GET /v0/pipes/<pipe_name>.json?param1=value1&param2=value2
```

31 pipes are registered covering traces, logs, metrics, errors, services, and custom dashboards. Query parameters are passed through to the SQL template engine. List all registered pipes:

```sh
curl http://localhost:7181/admin/pipes
```

### Admin

| Endpoint               | Method | Description                              |
|------------------------|--------|------------------------------------------|
| `/health`              | GET    | ClickHouse connectivity check            |
| `/admin/schema`        | POST   | Creates tables and materialized views    |
| `/admin/pipes`         | GET    | Lists all registered pipe names          |

## Schema

`POST /admin/schema` creates:

- **Tables** -- `logs`, `traces`, `metrics_sum`, `metrics_gauge`, `metrics_histogram`, `metrics_exponential_histogram`, `service_usage`
- **Materialized views** -- 6 MVs that aggregate into `service_usage` (SummingMergeTree) for hourly usage tracking
- **TTLs** -- 90 days for logs/traces, 365 days for metrics and usage

## Building

Without Docker:

```sh
go build -o proxy .
./proxy
```

Docker image:

```sh
docker build -t maple-tinybird-proxy .
```

## License

MIT
