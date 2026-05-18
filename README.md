# schemamaker

Generate ClickHouse migration SQL from a JSON file. Replaces the `create_ddl_for_kafka.sh` shell script without requiring a ClickHouse binary.

## Install

Download the binary for your platform from the [v0.2.0 release](https://github.com/Maksim-Gr/schemamaker/releases/tag/v0.2.0).

**macOS (Apple Silicon)**
```bash
curl -L https://github.com/Maksim-Gr/schemamaker/releases/download/v0.2.0/schemamaker-macos-arm64.tar.gz | tar -xz
chmod +x schemamaker && mv schemamaker /usr/local/bin/schemamaker
```

**macOS (Intel)**
```bash
curl -L https://github.com/Maksim-Gr/schemamaker/releases/download/v0.2.0/schemamaker-macos-x86_64.tar.gz | tar -xz
chmod +x schemamaker && mv schemamaker /usr/local/bin/schemamaker
```

**Linux (x86_64)**
```bash
curl -L https://github.com/Maksim-Gr/schemamaker/releases/download/v0.2.0/schemamaker-linux-x86_64.tar.gz | tar -xz
chmod +x schemamaker && mv schemamaker /usr/local/bin/schemamaker
```

Verify:
```bash
schemamaker --version
```

## Build from source

```bash
cargo build --release
```

The binary is at `./target/release/schemamaker`.

## Usage

```bash
schemamaker <COMMAND> [OPTIONS] <INPUT>
```

### Commands

| Command   | Description |
|-----------|-------------|
| `kafka`   | Generate full Kafka→ClickHouse pipeline migrations (streams, raw, datalake) |
| `scan`    | Scan JSON fields and suggest suitable ClickHouse table engines |
| `table`   | Generate a simple `CREATE TABLE` migration from JSON |
| `explain` | Explain index utilization for a SQL query against a live ClickHouse |

---

### `kafka`

```bash
schemamaker kafka [OPTIONS] <INPUT>
```

| Flag | Default | Description |
|------|---------|-------------|
| `-n, --name <NAME>` | input filename stem | Override the table name |
| `-c, --cluster <CLUSTER>` | `clickhouse_datalake` | ClickHouse cluster name |
| `-k, --kafka <KAFKA>` | `kafka` | Kafka collection name |
| `-o, --output-dir <DIR>` | `.` | Output directory for generated SQL files |

```bash
schemamaker kafka video_events.json
schemamaker kafka video_events.json -n my_table -c my_cluster -k my_kafka -o migrations/
```

Writes `{name}_up.sql` (creates streams table, raw table, datalake table, raw_mv, streams_mv) and `{name}_down.sql` (drops all 5 in reverse order).

---

### `scan`

Analyzes JSON fields, classifies them (Timestamp-like, ID-like, Numeric), and prints engine suggestions with `ORDER BY` recommendations.

```bash
schemamaker scan [OPTIONS] <INPUT>
```

| Flag | Default | Description |
|------|---------|-------------|
| `-n, --name <NAME>` | input filename stem | Override the table name |
| `-c, --cluster <CLUSTER>` | — | If set, suggests `ReplicatedMergeTree` variants |

```bash
schemamaker scan video_events.json
schemamaker scan video_events.json -c my_cluster
```

Example output (truncated):
```
Field analysis: video_events.json  (4 records, 13 fields)

  event_id              String            required → ID-like
  event_time            String            required → Timestamp-like
  amount                Float64           nullable → Numeric

Suggested engines:

  1. MergeTree
     ORDER BY (event_time)

  2. ReplacingMergeTree
     ORDER BY (event_id, event_time)
     → deduplicates rows by `event_id`

Run with chosen engine:
  schemamaker table video_events.json --engine MergeTree
```


---

### `table`

Generates a single `CREATE TABLE` / `DROP TABLE` migration. Use `scan` first to pick the right engine.

```bash
schemamaker table [OPTIONS] <INPUT>
```

| Flag | Default | Description |
|------|---------|-------------|
| `-n, --name <NAME>` | input filename stem | Override the table name |
| `-e, --engine <ENGINE>` | inferred (MergeTree) | `MergeTree`, `ReplicatedMergeTree`, `ReplacingMergeTree`, `SummingMergeTree` |
| `--order-by <FIELDS>` | inferred from field names | Comma-separated `ORDER BY` fields |
| `-c, --cluster <CLUSTER>` | — | Adds `ON CLUSTER` clause; required for `ReplicatedMergeTree` |
| `-o, --output-dir <DIR>` | `.` | Output directory for generated SQL files |

```bash
schemamaker table video_events.json
schemamaker table video_events.json --engine ReplicatedMergeTree -c my_cluster
```


---

### `explain`

Runs `EXPLAIN indexes = 1` against a live ClickHouse instance and prints both the raw plan and a parsed summary showing how many parts and granules each index scans.

```bash
schemamaker explain [OPTIONS] [SQL]
```

| Flag | Default | Description |
|------|---------|-------------|
| `[SQL]` | — | SQL query to explain (mutually exclusive with `--file`) |
| `--file <FILE>` | — | Read SQL from a file instead of an inline argument |
| `--host <HOST>` | `localhost` | ClickHouse host |
| `--port <PORT>` | `8123` | ClickHouse HTTP port |
| `--user <USER>` | `default` | ClickHouse user |
| `--password <PASSWORD>` | — | ClickHouse password |
| `--database <DATABASE>` | `default` | ClickHouse database |

```bash
schemamaker explain "SELECT * FROM events WHERE user_id = 123"
schemamaker explain --file query.sql --host ch.prod --database analytics
```

Example output:
```
ReadFromMergeTree (events)
  PrimaryKey  (user_id in [123, 123])
  Parts: 3/10  Granules: 5/1000

--- Index Analysis ---
PrimaryKey
  Parts     : 3 / 10  (30.0%)
  Granules  : 5 / 1000  (0.5%)
  Verdict   : index effective
```

Verdict thresholds: `index effective` < 10% granules scanned, `partial scan` 10–50%, `full scan` > 50%.

---

## Why the Kafka migration flow

Ingesting Kafka events into ClickHouse reliably requires three layers and two materialized views connecting them:

```
Kafka topic
    ↓
streams.{name}     # Kafka engine cursor — holds no data, just reads from the topic
    ↓ (streams_mv)
raw.{name}         # durable replay buffer — original message string + Kafka metadata
    ↓ (raw_mv)
datalake.{name}    # typed, queryable table — fields JSONExtracted at write time
```

`raw` exists so you can rebuild `datalake` without re-consuming from Kafka if the schema changes.

## Type Inference

Types are inferred by scanning every record and widening as needed:

| JSON value | ClickHouse type |
|------------|-----------------|
| string | `Nullable(String)` |
| integer | `Nullable(Int64)` |
| float | `Nullable(Float64)` |
| boolean | `Nullable(Bool)` |
| null / array / object | `Nullable(String)` |

If the same field appears as `Int64` in one record and `Float64` in another, it widens to `Nullable(Float64)`. Any other type conflict widens to `Nullable(String)`.

A field is non-nullable only if it is present in every record. Field order in the output matches the first record that introduced each field.
