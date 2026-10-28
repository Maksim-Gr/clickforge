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

Download the prebuilt binary from the [latest release](https://github.com/Maksim-Gr/clickforge/releases/latest), replacing `macos-arm64` with `macos-x86_64` or `linux-x86_64` as needed:

```bash
curl -L https://github.com/Maksim-Gr/clickforge/releases/latest/download/clickforge-macos-arm64.tar.gz | tar -xz
chmod +x clickforge && sudo mv clickforge /usr/local/bin/clickforge
clickforge --version
```

- **Specific version:** replace `latest/download` with `download/<tag>`, e.g. `download/v0.5.0`.
- **macOS Gatekeeper:** the binary is unsigned; if it's blocked, run `xattr -d com.apple.quarantine /usr/local/bin/clickforge`.
- **From source:** `cargo build --release` puts the binary at `./target/release/clickforge`.

## Quickstart

Point `clickforge kafka` at a JSON sample of your Kafka messages to generate the whole pipeline:

```bash
clickforge kafka video_events.json
```

This writes `video_events_up.sql` (the `streams` Kafka table, the `raw` replay buffer, the typed `datalake` table, and both materialized views) and `video_events_down.sql` (drops them in reverse). Target your cluster and Kafka collection as needed:

```bash
clickforge kafka video_events.json -c clickhouse_datalake -k kafka -o migrations/
```

To keep evolving the pipeline as your messages change, start it from a [schema contract](docs/contract.md) instead — the same SQL, plus a contract that new samples are checked and migrated against:

```bash
clickforge contract init video_events.json --target kafka
clickforge contract apply video_events.contract.json new_events.json
```

## Commands

`kafka` is the primary command — it generates the whole ingestion pipeline. The rest are helpers around it:

| Command   | Role | Description |
|-----------|------|-------------|
| [`kafka`](docs/commands.md#kafka)   | **primary** | Generate the full Kafka→ClickHouse pipeline (streams, raw, datalake + materialized views) |
| [`scan`](docs/commands.md#scan)    | helper | Inspect JSON fields and pick a ClickHouse engine for a one-off table |
| [`table`](docs/commands.md#table)   | helper | Generate a single `CREATE TABLE` migration from JSON |
| [`diff`](docs/commands.md#diff)    | helper | Generate `ALTER TABLE` migrations adding the columns new in a JSON sample |
| [`contract`](docs/contract.md) | helper | Keep a reviewed schema contract, check new samples against it, and migrate it version by version |

`<INPUT>` is a JSON/NDJSON file, or `-` to read from stdin ([details](docs/commands.md#input)).

## Documentation

- [Commands](docs/commands.md) — input handling, `kafka`, `scan`, `table`, `diff`
- [Schema contracts](docs/contract.md) — change classes, exit codes, versioned migrations
- [Type inference](docs/type-inference.md) — how JSON values map to ClickHouse types

Every command lists its flags and defaults with `clickforge <COMMAND> --help`.
