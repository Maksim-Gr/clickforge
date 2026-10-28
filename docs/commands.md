# Commands

`kafka` is the primary command — it generates the whole ingestion pipeline. `scan`, `table`, and `diff` support it — choose an engine, generate a one-off table, or evolve the `datalake` schema as your messages change. For schema contracts, see [contract.md](contract.md).

Every command lists its flags and defaults with `clickforge <COMMAND> --help`.

## Input

```bash
clickforge <COMMAND> [OPTIONS] <INPUT>
```

The `kafka`, `scan`, and `table` commands take a single `<INPUT>`; `diff` and `contract` take their own positionals (see their sections).

`<INPUT>` is a path to a JSON/NDJSON file, or `-` to read from stdin. When reading from stdin, pass `--name` to set the table name (it defaults to `table`):

```bash
cat video_events.json | clickforge scan -
cat video_events.json | clickforge table - --name video_events -o migrations/
```

## `kafka`

```bash
clickforge kafka video_events.json
clickforge kafka video_events.json -n my_table -c my_cluster -k my_kafka -o migrations/
```

Writes `{name}_up.sql` (creates streams table, raw table, datalake table, raw_mv, streams_mv) and `{name}_down.sql` (drops all 5 in reverse order).

## `scan`

Analyzes JSON fields, classifies them (Timestamp-like, ID-like, Numeric), and prints engine suggestions with `ORDER BY` recommendations. When numeric metrics and a dimension (id/timestamp) are present, it also suggests `SummingMergeTree` with the metric columns to sum.

![clickforge scan demo — inspect fields and generate a one-off table](../assets/demo.gif)

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

## `table`

Generates a single `CREATE TABLE` / `DROP TABLE` migration. Use `scan` first to pick the right engine.

```bash
clickforge table video_events.json
clickforge table video_events.json -c my_cluster                  # ReplicatedMergeTree ON CLUSTER
clickforge table video_events.json --order-by event_id,timestamp  # override the inferred ORDER BY
```

## `diff`

Infers a schema from two JSON samples (an old and a new one) and generates additive `ALTER TABLE` migrations for the columns that appear in the new sample but not the old.

```bash
clickforge diff video_events.json video_events_v2.json -n video_events
```

Writes `{name}_alter_up.sql`/`_down.sql` that add the columns new in `<NEW>`, always as `Nullable`. Every other difference is reported and classified as by [`contract check`](contract.md), with the same exit codes, but not migrated — retyping or dropping a populated column is destructive.

> **Kafka pipelines:** `diff` alters a single, unqualified table, not the `raw` → `datalake` view, so new columns would stay empty. Use [`contract apply`](contract.md), which swaps the view too.
