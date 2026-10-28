# Schema contracts

A **schema contract** (`{name}.contract.json`) is the committed, reviewed record of the schema a table or Kafka pipeline expects. New samples are checked against it, and each change becomes a versioned migration.

```bash
clickforge contract init  <SAMPLE> --target kafka|table   # contract + {name}_v1_up/down.sql
clickforge contract check <CONTRACT> <SAMPLE>             # report only
clickforge contract apply <CONTRACT> <SAMPLE> [--accept <FIELDS> | --accept-all]
clickforge contract diff  <OLD> <NEW>                     # migrate a hand-edited contract
```

Each subcommand lists its flags with `clickforge contract help <SUBCOMMAND>`.

Every difference between the contract and a sample is classified:

| Class | Examples | `apply` |
|---|---|---|
| compatible | new field, missing or `null` in some records | applied |
| attention | new field in every record · floats in an `Int64` field (widens to `Float64`) · required field sometimes `null` | only with `--accept`; a new field is still added, as `Nullable` |
| breaking | required field absent · values that don't fit the type (`"abc"` for `Int64`, which `JSONExtract` would silently store as `0`) · any change to an `ORDER BY` field | never; nothing is written |

A nullable field absent from a sample is not a change: samples are incomplete. The contract's `"unknown_fields"` decides what new fields are: `propose` (default), `ignore` or `reject`. Exit codes: `0` none or compatible, `2` attention, `3` breaking, `1` error.

`apply` writes `{name}_v{N}_up/down.sql` next to the contract and bumps its `version`. Columns are only added or modified, never dropped, and existing rows aren't backfilled. For `kafka`, the `raw` → `datalake` view is swapped with `streams` detached, so Kafka holds the messages meanwhile:

```sql
ALTER TABLE `datalake`.`video_events` … ADD COLUMN IF NOT EXISTS `device` Nullable(String) AFTER `platform`;
DETACH TABLE `streams`.`video_events` …;
DROP VIEW IF EXISTS `raw`.`video_events_mv` … SYNC;
CREATE MATERIALIZED VIEW IF NOT EXISTS `raw`.`video_events_mv` …;   -- now extracts `device`
ATTACH TABLE `streams`.`video_events` …;
```

Down scripts restore the old view first, with a `-- WARNING` wherever reverting truncates values or turns `NULL`s into defaults.

**Changing the contract by hand** is how breaking changes are made, since `apply` never makes them: edit it, bump `version`, and `diff` it against the deployed version. To remove a field, mark it `"retired": true` rather than deleting it — it stops being extracted and its column is kept. Narrowing type changes carry a `-- WARNING` (the mutation fails if a value doesn't convert). The engine, `ORDER BY`, cluster and target can't be migrated.

```bash
git show HEAD:video_events.contract.json > /tmp/old.json
$EDITOR video_events.contract.json        # e.g. "retired": true on a field, "version": 2
clickforge contract diff /tmp/old.json video_events.contract.json
```
