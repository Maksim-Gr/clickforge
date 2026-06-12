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
  event_time            DateTime64(3)     required → Timestamp-like
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
| ISO-8601 datetime string (`YYYY-MM-DDThh:mm:ss…`) | `Nullable(DateTime64(3))` |
| integer | `Nullable(Int64)` |
| float | `Nullable(Float64)` |
| boolean | `Nullable(Bool)` |
| array | `Array(T)` — element type inferred |
| object with scalar values | `Map(String, V)` |
| null / nested object | `Nullable(String)` |

`Array` and `Map` columns are never wrapped in `Nullable` (ClickHouse forbids it). Date-only strings (`2024-03-01`) are left as `String`.

If the same field appears as `Int64` in one record and `Float64` in another, it widens to `Nullable(Float64)`. Any other type conflict widens to `Nullable(String)`.

A field is non-nullable only if it is present in every record. Field order in the output matches the first record that introduced each field.
