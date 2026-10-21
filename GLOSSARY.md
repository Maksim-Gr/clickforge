# clickforge

Generates ClickHouse migrations for a Kafka→ClickHouse ingestion flow from a sample of the JSON messages that flow carries.

## Language

**JSON sample**:
A set of example JSON records (a JSON array or NDJSON, from a file or stdin) that a table's schema is inferred from. It names the table unless a name is given explicitly.
_Avoid_: input, fixture, payload

**Kafka pipeline**:
The full chain for one table: the `streams` Kafka table, the `raw` and `datalake` tables, and the materialized views connecting them.
_Avoid_: ingestion tables, DDL bundle

**Engine suggestion**:
One candidate table engine for a JSON sample, with its ORDER BY, SUM columns and a rationale. A scan produces a ranked list of them.
_Avoid_: recommendation, engine option

**Engine selection**:
The final engine, ORDER BY and SUM columns chosen for a single table, from the engine suggestions plus any explicit user choices.
_Avoid_: engine config, engine resolution

**Migration**:
A named pair of SQL scripts: an "up" script that applies a change and a "down" script that reverts it.
_Avoid_: up/down pair, DDL, SQL files
