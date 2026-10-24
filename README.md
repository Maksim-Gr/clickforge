# clickforge

Generate the **Kafka→ClickHouse ingestion pipeline** — the `streams`, `raw`, and `datalake` tables plus the materialized views that connect them — from a single JSON sample. A drop-in replacement for the `create_ddl_for_kafka.sh` script that needs no ClickHouse binary.

![clickforge kafka demo — generate the streams→raw→datalake pipeline from a JSON sample](assets/kafka.gif)

## The Kafka ingestion flow

Reliably ingesting Kafka events into ClickHouse takes three layers and two materialized views connecting them — exactly what `clickforge kafka` generates in one shot:

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

## Quickstart

Point `clickforge kafka` at a JSON sample of your Kafka messages to generate the whole pipeline:

```bash
clickforge kafka video_events.json
```

This writes `video_events_up.sql` (the `streams` Kafka table, the `raw` replay buffer, the typed `datalake` table, and both materialized views) and `video_events_down.sql` (drops them in reverse). Target your cluster and Kafka collection as needed:

```bash
clickforge kafka video_events.json -c clickhouse_datalake -k kafka -o migrations/
```

## Usage

```bash
clickforge <COMMAND> [OPTIONS] <INPUT>
```

The `kafka`, `scan`, and `table` commands take a single `<INPUT>`. The `diff` command takes two positionals, `<OLD> <NEW>` (see the [`diff`](#diff) section).

`<INPUT>` is a path to a JSON/NDJSON file, or `-` to read from stdin. When reading from stdin, pass `--name` to set the table name (it defaults to `table`):

```bash
cat video_events.json | clickforge scan -
cat video_events.json | clickforge table - --name video_events -o migrations/
```

### Commands

`kafka` is the primary command — it generates the whole ingestion pipeline. The rest are helpers around it:

| Command   | Role | Description |
|-----------|------|-------------|
| `kafka`   | **primary** | Generate the full Kafka→ClickHouse pipeline (streams, raw, datalake + materialized views) |
| `scan`    | helper | Inspect JSON fields and pick a ClickHouse engine for a one-off table |
| `table`   | helper | Generate a single `CREATE TABLE` migration from JSON |
| `diff`    | helper | Generate `ALTER TABLE` migrations to evolve a table as your JSON schema changes |
| `contract` | helper | Create a schema contract from a sample, check new samples against it, and migrate it as it changes |

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

## Helper commands

`scan`, `table`, and `diff` support the pipeline above — choose an engine, generate a one-off table, or evolve the `datalake` schema as your messages change. You don't need them for the basic `kafka` flow.

### `scan`

Analyzes JSON fields, classifies them (Timestamp-like, ID-like, Numeric), and prints engine suggestions with `ORDER BY` recommendations. When numeric metrics and a dimension (id/timestamp) are present, it also suggests `SummingMergeTree` with the metric columns to sum.

![clickforge scan demo — inspect fields and generate a one-off table](assets/demo.gif)

```bash
clickforge scan [OPTIONS] <INPUT>
```

| Flag | Default | Description |
|------|---------|-------------|
| `-n, --name <NAME>` | input filename stem | Names the generated `{name}_up.sql`/`{name}_down.sql` files if you pick an engine at the prompt; otherwise no effect on output |
| `-c, --cluster <CLUSTER>` | — | If set, suggests `ReplicatedMergeTree` instead of `MergeTree` |

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
  playback_position_ms  Int64             required → Numeric
  timestamp             DateTime64(3)     required → Timestamp-like
  client_ip             Nullable(String)  nullable

Suggested engines:

  1. MergeTree
     ORDER BY (timestamp)
     → general purpose time-series table

  2. ReplacingMergeTree
     ORDER BY (event_id, timestamp)
     → deduplicates rows by `event_id` — good for upsert-like data

To generate a migration with the chosen engine, run:
  clickforge table video_events.json --engine MergeTree --order-by timestamp
  clickforge table video_events.json --engine ReplacingMergeTree --order-by event_id,timestamp
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
| `-e, --engine <ENGINE>` | inferred (MergeTree, or ReplicatedMergeTree with `-c`) | `MergeTree`, `ReplicatedMergeTree`, `ReplacingMergeTree`, `SummingMergeTree` |
| `--order-by <FIELDS>` | inferred from field names | Comma-separated `ORDER BY` fields |
| `-c, --cluster <CLUSTER>` | — | Adds `ON CLUSTER` clause; required for `ReplicatedMergeTree`, and makes it the default engine |
| `-o, --output-dir <DIR>` | `.` | Output directory for generated SQL files |
| `--stdout` | off | Print migrations to stdout instead of writing files |

```bash
clickforge table video_events.json
clickforge table video_events.json -c my_cluster                  # ReplicatedMergeTree ON CLUSTER
clickforge table video_events.json --order-by event_id,timestamp  # override the inferred ORDER BY
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
| `-n, --name <NAME>` | new file stem (old file stem if `<NEW>` is `-`) | Override the table name |
| `-c, --cluster <CLUSTER>` | — | Adds `ON CLUSTER` to the statements |
| `-o, --output-dir <DIR>` | `.` | Output directory for generated SQL files |
| `--stdout` | off | Print migrations to stdout instead of writing files |

```bash
clickforge diff video_events.json video_events_v2.json -n video_events
```

Writes `{name}_alter_up.sql` (`ADD COLUMN`) and `{name}_alter_down.sql` (`DROP COLUMN`, reverse order). Removed columns and type changes are **not** migrated automatically — they are reported as warnings on stderr so you can review them by hand (dropping or retyping a populated column is destructive).

> **Kafka pipelines:** `diff` alters a single, unqualified table and does not touch the `raw` → `datalake` materialized view. Applied to a `kafka` pipeline's `datalake` table, the new columns are added but stay empty, because the view never extracts them. Recreate the view with the new fields yourself.

---

### `contract`

A **schema contract** (`{name}.contract.json`) is the committed statement of the schema a table or Kafka pipeline expects. Generate it once from a sample, review and edit it by hand where inference guessed wrong, then check new samples against it.

```bash
clickforge contract init <INPUT> --target <kafka|table> [OPTIONS]
clickforge contract check <CONTRACT> <SAMPLE>
clickforge contract apply <CONTRACT> <SAMPLE> [--accept <FIELDS> | --accept-all] [-o <DIR>]
clickforge contract diff <OLD> <NEW> [-o <DIR>]
```

`init` writes the contract plus `{name}_v1_up.sql`/`{name}_v1_down.sql` (the same SQL `kafka` or `table` would generate). It refuses to overwrite an existing contract. It takes the `kafka` command's `-n`, `-c`, `-k`, `-o` for `--target kafka`, and the `table` command's `-n`, `-e`, `--order-by`, `-c`, `-o` for `--target table`.

`check` reads the sample (or `-` for stdin), classifies every difference and writes nothing:

| Contract vs sample | Class |
|---|---|
| New field, missing or `null` in some records | compatible |
| New field present in every record | attention |
| Integers where the contract has `Int64`/`UInt64` (widening to `Float64`) | attention |
| Required field missing or `null` in some records | attention |
| Required field absent from the sample | breaking |
| Values that don't fit the contract's type (e.g. `"abc"` for `Int64`) | breaking |
| An attention change on an `ORDER BY` column | breaking |

A nullable field absent from the sample is not a change: samples are incomplete by nature. The contract's `unknown_fields` setting decides what new fields are: `propose` (default, as above), `ignore` (reported as info) or `reject` (breaking).

```
Checking new_events.json against video_events.contract.json (v1)

  breaking    user_id               sample has String values that don't fit Int64; they would be stored as defaults
  attention   playback_position_ms  Int64 -> Float64 (widening; values are truncated until then)
  compatible  device                new field Nullable(String)
```

Exit codes: `0` no changes or compatible only, `2` attention, `3` breaking, `1` error — so CI can gate on them.

`apply` runs the same check, then writes `{name}_v{N}_up.sql`/`_down.sql` and rewrites the contract as version `N` (hand-edited settings such as `unknown_fields` are kept):

- **Compatible** changes are always applied.
- **Attention** changes are applied only for fields named in `--accept` (comma-separated) or with `--accept-all`; the rest are skipped and `apply` exits `2`. A new field present in every record is still added without `--accept`, as `Nullable`; accepting it makes it required.
- **Breaking** changes are never applied: while there is one, `apply` writes nothing and exits `3`. Fix the producer, or edit the contract by hand.

New fields become `ADD COLUMN`, widened or newly nullable fields `MODIFY COLUMN`; columns are never dropped, and existing rows are not backfilled (they keep the column's default). For a `kafka` contract the migration alters `datalake` and then swaps in the new `raw` → `datalake` view with `streams` detached meanwhile, so Kafka holds the messages and none are lost:

```sql
ALTER TABLE `datalake`.`video_events` ON CLUSTER … ADD COLUMN IF NOT EXISTS `device` Nullable(String) AFTER `platform`;

DETACH TABLE `streams`.`video_events` ON CLUSTER …;
DROP VIEW IF EXISTS `raw`.`video_events_mv` ON CLUSTER … SYNC;
CREATE MATERIALIZED VIEW IF NOT EXISTS `raw`.`video_events_mv` …;   -- now extracts `device`
ATTACH TABLE `streams`.`video_events` ON CLUSTER …;
```

The down script restores the previous view first, then drops added columns and reverts `MODIFY`s, with a `-- WARNING` comment where reverting truncates values or turns `NULL`s into defaults. Migrations are written next to the contract unless `-o` is given, and `apply` refuses to overwrite an existing one.

`diff` is how a person changes the contract on purpose — including breaking changes, which `apply` never makes. Edit the contract, bump its `version` by one, and diff it against the deployed version; every edit is applied, and the report shows what each one risks:

```bash
git show HEAD:video_events.contract.json > /tmp/old.json
$EDITOR video_events.contract.json
clickforge contract diff /tmp/old.json video_events.contract.json
```

- **Removing a field:** mark it `"retired": true` instead of deleting it. It stops being extracted (for `kafka`, the view is swapped), but its column is kept — columns are never dropped — and new rows get its default. `check` reports a retired field as info, and `apply` never re-adds it. Delete the flag to revive it; a changed type becomes a `MODIFY`.
- **Type and nullability changes** become `MODIFY COLUMN`. Narrowing ones (`String` → `Int64`, `Float64` → `Int64`) carry a `-- WARNING`: values that don't fit are truncated, or the mutation fails — and blocks the table's later mutations until killed — if a value doesn't convert. Making a field required turns existing `NULL`s into defaults.
- **Not migratable:** editing `table`, `target`, `cluster`, `kafka`, `engine`, `order_by` or `sum_columns`, or the type of an `ORDER BY` field, is an error — that needs a new table or pipeline.

`diff` never writes the contracts, only `{name}_v{N}_up.sql`/`_down.sql`. When nothing changes in ClickHouse (e.g. only `unknown_fields` changed), it writes nothing and `version` stays as it is.

---

## Type Inference

Types are inferred by scanning every record and widening as needed:

| JSON value | ClickHouse type |
|------------|-----------------|
| string | `Nullable(String)` |
| ISO-8601 datetime string (`YYYY-MM-DDThh:mm:ss…`) | `Nullable(DateTime64(3))` |
| integer | `Nullable(Int64)` (or `Nullable(UInt64)` for values beyond `Int64`'s range) |
| float | `Nullable(Float64)` |
| boolean | `Nullable(Bool)` |
| array | `Array(T)` — element type inferred |
| object with scalar values | `Map(String, V)` |
| null / nested object | `Nullable(String)` |

`Array` and `Map` columns are never wrapped in `Nullable` (ClickHouse forbids it). Date-only strings (`2024-03-01`) are left as `String`.

String columns with few distinct values across a large-enough sample are emitted as `LowCardinality(String)` (a ClickHouse storage optimization). This is conservative: it stays off for small samples and high-distinct columns.

If the same field appears as `Int64` in one record and `Float64` (or `UInt64`) in another, it widens to `Float64`. Any other type conflict widens to `String`.

A field is non-nullable only if it is present in every record and is never JSON `null`. Field order in the output matches the first record that introduced each field.
