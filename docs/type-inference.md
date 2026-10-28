# Type Inference

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
