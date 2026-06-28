# clickforge

Generate ClickHouse migration SQL from a JSON file. Replaces the `create_ddl_for_kafka.sh` shell script without requiring a ClickHouse binary.

![clickforge demo — scan a JSON sample and generate a migration](assets/demo.gif)

## Install

Download the prebuilt binary for your platform from the [latest release](https://github.com/Maksim-Gr/clickforge/releases/latest). The `latest/download` URLs below always resolve to the newest release, so they never go stale.

**macOS (Apple Silicon)**
```bash
curl -L https://github.com/Maksim-Gr/clickforge/releases/latest/download/clickforge-macos-arm64.tar.gz | tar -xz
chmod +x clickforge && sudo mv clickforge /usr/local/bin/clickforge
```

**macOS (Intel)**
```bash
curl -L https://github.com/Maksim-Gr/clickforge/releases/latest/download/clickforge-macos-x86_64.tar.gz | tar -xz
chmod +x clickforge && sudo mv clickforge /usr/local/bin/clickforge
```

**Linux (x86_64)**
```bash
curl -L https://github.com/Maksim-Gr/clickforge/releases/latest/download/clickforge-linux-x86_64.tar.gz | tar -xz
chmod +x clickforge && sudo mv clickforge /usr/local/bin/clickforge
```

To install a specific version, replace `latest/download` with `download/<tag>`, e.g. `download/v0.5.0`.

> **macOS:** the binary is unsigned, so Gatekeeper may block the first run. If you see *"cannot be opened because the developer cannot be verified"*, clear the quarantine flag:
> ```bash
> xattr -d com.apple.quarantine /usr/local/bin/clickforge
> ```

Verify:
```bash
clickforge --version
```

## Build from source

```bash
cargo build --release
```

The binary is at `./target/release/clickforge`.

## Usage

```bash
clickforge <COMMAND> [OPTIONS] <INPUT>
```

`<INPUT>` is a path to a JSON/NDJSON file, or `-` to read from stdin. When reading from stdin, pass `--name` to set the table name (it defaults to `table`):

```bash
cat video_events.json | clickforge scan -
cat video_events.json | clickforge table - --name video_events -o migrations/
```

### Commands

| Command   | Description |
|-----------|-------------|
| `kafka`   | Generate full Kafka→ClickHouse pipeline migrations (streams, raw, datalake) |
| `scan`    | Scan JSON fields and suggest suitable ClickHouse table engines |
| `table`   | Generate a simple `CREATE TABLE` migration from JSON |
| `diff`    | Generate `ALTER TABLE` migrations from the diff between two JSON samples |

---

### `kafka`

```bash
clickforge kafka [OPTIONS] <INPUT>
```

| Flag | Default | Description |
|------|---------|-------------|
| `-n, --name <NAME>` | input filename stem | Override the table name |
| `-c, --cluster <CLUSTER>` | `clickhouse_datalake` | ClickHouse cluster name |
| `-k, --kafka <KAFKA>` | `kafka` | Kafka collection name |
| `-o, --output-dir <DIR>` | `.` | Output directory for generated SQL files |
| `--stdout` | off | Print migrations to stdout instead of writing files |

```bash
clickforge kafka video_events.json
clickforge kafka video_events.json -n my_table -c my_cluster -k my_kafka -o migrations/
```

Writes `{name}_up.sql` (creates streams table, raw table, datalake table, raw_mv, streams_mv) and `{name}_down.sql` (drops all 5 in reverse order).

---

### `scan`

Analyzes JSON fields, classifies them (Timestamp-like, ID-like, Numeric), and prints engine suggestions with `ORDER BY` recommendations. When numeric metrics and a dimension (id/timestamp) are present, it also suggests `SummingMergeTree` with the metric columns to sum.

```bash
clickforge scan [OPTIONS] <INPUT>
```

| Flag | Default | Description |
|------|---------|-------------|
| `-n, --name <NAME>` | input filename stem | Override the table name (used to name files if you generate one from the prompt) |
| `-c, --cluster <CLUSTER>` | — | If set, suggests `ReplicatedMergeTree` variants |

```bash
clickforge scan video_events.json
clickforge scan video_events.json -c my_cluster
```

When run in a terminal, `scan` finishes by offering to generate a migration from one of
its suggestions, so you don't have to re-type a `table` command:

```
Pick an engine to generate [1-3, Enter to skip]: 2
Written: ./video_events_up.sql
Written: ./video_events_down.sql
```

Picking a number writes `{name}_up.sql`/`{name}_down.sql` (using that suggestion's engine
and `ORDER BY`) to the current directory; press Enter to skip. The prompt only appears in
an interactive terminal — piped or scripted runs just print the suggestions as before.

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
  clickforge table video_events.json --engine MergeTree
```


---

### `table`

Generates a single `CREATE TABLE` / `DROP TABLE` migration. Use `scan` first to pick the right engine.

```bash
clickforge table [OPTIONS] <INPUT>
```

| Flag | Default | Description |
|------|---------|-------------|
| `-n, --name <NAME>` | input filename stem | Override the table name |
| `-e, --engine <ENGINE>` | inferred (MergeTree) | `MergeTree`, `ReplicatedMergeTree`, `ReplacingMergeTree`, `SummingMergeTree` |
| `--order-by <FIELDS>` | inferred from field names | Comma-separated `ORDER BY` fields |
| `-c, --cluster <CLUSTER>` | — | Adds `ON CLUSTER` clause; required for `ReplicatedMergeTree` |
| `-o, --output-dir <DIR>` | `.` | Output directory for generated SQL files |
| `--stdout` | off | Print migrations to stdout instead of writing files |

```bash
clickforge table video_events.json
clickforge table video_events.json --engine ReplicatedMergeTree -c my_cluster
```


---

### `diff`

Infers a schema from two JSON samples (an old and a new one) and generates additive `ALTER TABLE` migrations for the columns that appear in the new sample but not the old.

```bash
clickforge diff [OPTIONS] <OLD> <NEW>
```

| Flag | Default | Description |
|------|---------|-------------|
| `<OLD>` | — | Existing/old JSON sample (or `-` for stdin) |
| `<NEW>` | — | New JSON sample (or `-` for stdin) |
| `-n, --name <NAME>` | new file stem | Override the table name |
| `-c, --cluster <CLUSTER>` | — | Adds `ON CLUSTER` to the statements |
| `-o, --output-dir <DIR>` | `.` | Output directory for generated SQL files |
| `--stdout` | off | Print migrations to stdout instead of writing files |

```bash
clickforge diff video_events.json video_events_v2.json -n video_events
```

Writes `{name}_alter_up.sql` (`ADD COLUMN`) and `{name}_alter_down.sql` (`DROP COLUMN`, reverse order). Removed columns and type changes are **not** migrated automatically — they are reported as warnings on stderr so you can review them by hand (dropping or retyping a populated column is destructive).

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

String columns with few distinct values across a large-enough sample are emitted as `LowCardinality(String)` (a ClickHouse storage optimization). This is conservative: it stays off for small samples and high-distinct columns.

If the same field appears as `Int64` in one record and `Float64` in another, it widens to `Nullable(Float64)`. Any other type conflict widens to `Nullable(String)`.

A field is non-nullable only if it is present in every record. Field order in the output matches the first record that introduced each field.
