use crate::schema::{Column, ColumnType, InferredSchema};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Stop counting distinct values once a column exceeds this many — past it the
/// column is clearly not low-cardinality, so the exact count no longer matters.
const CARDINALITY_CAP: usize = 100;
/// Don't flag low-cardinality on small samples — too little signal.
const MIN_RECORDS_FOR_LC: usize = 20;

/// Parses input as a JSON array or as a stream of JSON objects (NDJSON or
/// concatenated pretty-printed). Shared by `infer_schema` and `record_count` so
/// the two always agree on how many records the input contains.
fn parse_values(content: &str) -> Result<Vec<Value>, String> {
    let trimmed = content.trim();
    if trimmed.starts_with('[') {
        serde_json::from_str(trimmed).map_err(|e| format!("Error parsing JSON array: {}", e))
    } else {
        serde_json::Deserializer::from_str(trimmed)
            .into_iter::<Value>()
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("Error parsing JSON: {}", e))
    }
}

pub fn infer_schema(content: &str, table_name: &str) -> Result<InferredSchema, String> {
    let mut field_order: Vec<String> = Vec::new();
    let mut field_types: HashMap<String, ColumnType> = HashMap::new();
    let mut field_seen: HashMap<String, usize> = HashMap::new(); // how many records contained this field
    let mut field_has_null: HashSet<String> = HashSet::new(); // fields that had a JSON `null` in at least one record
    let mut distinct_values: HashMap<String, HashSet<String>> = HashMap::new(); // capped distinct string values
    let mut record_count = 0;

    let values: Vec<Value> = parse_values(content)?;

    for (i, value) in values.into_iter().enumerate() {
        let obj = value.as_object().ok_or_else(|| {
            format!(
                "record {}: expected a JSON object, got something else",
                i + 1
            )
        })?;

        for (key, value) in obj {
            if !field_seen.contains_key(key) {
                field_order.push(key.clone());
            }
            if value.is_null() {
                // Don't let a null occurrence widen/override the type inferred from
                // this field's real values elsewhere; just remember it happened so
                // the field is marked nullable below.
                field_has_null.insert(key.clone());
            } else {
                let inferred = infer_type(value);
                if let Some(existing) = field_types.get(key) {
                    field_types.insert(key.clone(), merge_types(existing, &inferred));
                } else {
                    field_types.insert(key.clone(), inferred);
                }
            }
            *field_seen.entry(key.clone()).or_insert(0) += 1;
            // Track distinct values for plain string fields, capped.
            if let Value::String(s) = value {
                let set = distinct_values.entry(key.clone()).or_default();
                if set.len() <= CARDINALITY_CAP {
                    set.insert(s.clone());
                }
            }
        }
        record_count += 1;
    }
    if record_count == 0 {
        return Err("input file is empty or contains no JSON objects".to_string());
    }

    let columns: Vec<Column> = field_order
        .into_iter()
        .map(|name| {
            // Absent only when every occurrence of this field was JSON `null`.
            let ch_type = field_types.remove(&name).unwrap_or(ColumnType::String);
            let seen = *field_seen.get(&name).unwrap_or(&0);
            // Nullable if the field was missing from at least one record, or was
            // present but null at least once.
            let nullable = seen < record_count || field_has_null.contains(&name);
            let low_cardinality = is_low_cardinality(
                &ch_type,
                distinct_values.get(&name).map(|s| s.len()),
                record_count,
            );
            Column {
                name,
                ch_type,
                nullable,
                low_cardinality,
            }
        })
        .collect();

    Ok(InferredSchema {
        table_name: table_name.to_string(),
        columns,
    })
}

pub fn record_count(content: &str) -> usize {
    parse_values(content).map(|v| v.len()).unwrap_or(0)
}

fn infer_type(val: &Value) -> ColumnType {
    match val {
        Value::Bool(_) => ColumnType::Bool,
        Value::String(s) => {
            if looks_like_datetime(s) {
                ColumnType::DateTime64
            } else {
                ColumnType::String
            }
        }
        Value::Number(n) => {
            if n.as_i64().is_some() {
                ColumnType::Int64
            } else if n.as_u64().is_some() {
                // Fits u64 but not i64: values beyond i64::MAX. Without this, they'd
                // silently truncate/wrap once cast to Int64 downstream.
                ColumnType::UInt64
            } else {
                ColumnType::Float64
            }
        }
        Value::Array(items) => {
            let elem = items
                .iter()
                .map(infer_type)
                .reduce(|a, b| merge_types(&a, &b))
                // No elements observed; String is an arbitrary but harmless default
                // (documented limitation — see README's Type Inference section).
                .unwrap_or(ColumnType::String);
            ColumnType::Array(Box::new(elem))
        }
        Value::Object(map) => {
            // Map(String, V) only when every value is a homogeneous scalar; nested
            // objects and empty objects fall back to String, discarding structure.
            // This is a deliberate, documented simplification (see README), not an
            // oversight.
            let mut value_type: Option<ColumnType> = None;
            for v in map.values() {
                if !v.is_string() && !v.is_number() && !v.is_boolean() {
                    return ColumnType::String;
                }
                let t = infer_type(v);
                value_type = Some(match value_type {
                    Some(existing) => merge_types(&existing, &t),
                    None => t,
                });
            }
            match value_type {
                Some(vt) => ColumnType::Map(Box::new(ColumnType::String), Box::new(vt)),
                None => ColumnType::String, // empty object
            }
        }
        // Callers must not merge a null occurrence's type into a field's inferred
        // type (see infer_schema); this arm only matters for a field that is *only*
        // ever seen as null within an array element.
        Value::Null => ColumnType::String,
    }
}

/// Conservative ISO-8601 check: matches `YYYY-MM-DDThh:mm:ss` or `YYYY-MM-DD hh:mm:ss`
/// (optionally followed by fractional seconds / timezone). Date-only strings are left as String.
fn looks_like_datetime(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() < 19 {
        return false;
    }
    let digit = |i: usize| b[i].is_ascii_digit();
    (0..4).all(digit)                          // YYYY
        && b[4] == b'-' && digit(5) && digit(6)  // -MM
        && b[7] == b'-' && digit(8) && digit(9)  // -DD
        && (b[10] == b'T' || b[10] == b' ')      // T or space
        && digit(11) && digit(12) && b[13] == b':'  // hh:
        && digit(14) && digit(15) && b[16] == b':'  // mm:
        && digit(17) && digit(18) // ss
}

/// A `String` column is low-cardinality when, across a large-enough sample, it has
/// few distinct values that are clearly fewer than the number of records. Conservative:
/// stays off for small samples or when distinct counting hit the cap.
fn is_low_cardinality(ch_type: &ColumnType, distinct: Option<usize>, record_count: usize) -> bool {
    if !matches!(ch_type, ColumnType::String) || record_count < MIN_RECORDS_FOR_LC {
        return false;
    }
    match distinct {
        Some(d) => d <= CARDINALITY_CAP && d * 2 < record_count,
        None => false,
    }
}

fn merge_types(a: &ColumnType, b: &ColumnType) -> ColumnType {
    if a == b {
        return a.clone();
    }
    match (a, b) {
        (ColumnType::Float64, ColumnType::Int64)
        | (ColumnType::Int64, ColumnType::Float64)
        | (ColumnType::Float64, ColumnType::UInt64)
        | (ColumnType::UInt64, ColumnType::Float64)
        // Int64 and UInt64 ranges don't nest in each other; widen to Float64
        // rather than silently truncating either side.
        | (ColumnType::Int64, ColumnType::UInt64)
        | (ColumnType::UInt64, ColumnType::Int64) => ColumnType::Float64,
        (ColumnType::Array(x), ColumnType::Array(y)) => {
            ColumnType::Array(Box::new(merge_types(x, y)))
        }
        (ColumnType::Map(_, x), ColumnType::Map(_, y)) => {
            ColumnType::Map(Box::new(ColumnType::String), Box::new(merge_types(x, y)))
        }
        // Any other conflict (e.g. Bool vs String, or mixed-type array elements)
        // widens to String — lossy but documented (see README's Type Inference
        // section) rather than silently picking one side.
        _ => ColumnType::String,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infer_schema_json_array() {
        let schema = infer_schema(r#"[{"id":1,"name":"alice"}]"#, "users").unwrap();
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].name, "id");
        assert_eq!(schema.columns[1].name, "name");
    }

    #[test]
    fn infer_schema_ndjson() {
        let schema = infer_schema("{\"id\":1}\n{\"id\":2}", "t").unwrap();
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].name, "id");
    }

    #[test]
    fn infer_schema_nullable_when_field_missing() {
        let schema = infer_schema("{\"a\":1}\n{\"b\":2}", "t").unwrap();
        let a = schema.columns.iter().find(|c| c.name == "a").unwrap();
        let b = schema.columns.iter().find(|c| c.name == "b").unwrap();
        assert!(a.nullable);
        assert!(b.nullable);
    }

    #[test]
    fn merge_int_and_float_widens_to_float() {
        let result = merge_types(&ColumnType::Int64, &ColumnType::Float64);
        assert_eq!(result, ColumnType::Float64);
    }

    #[test]
    fn merge_conflicting_types_widens_to_string() {
        let result = merge_types(&ColumnType::Bool, &ColumnType::Int64);
        assert_eq!(result, ColumnType::String);
    }

    #[test]
    fn infer_iso8601_string_as_datetime() {
        let schema = infer_schema(r#"[{"ts":"2024-03-01T12:00:00Z"}]"#, "t").unwrap();
        assert_eq!(schema.columns[0].ch_type, ColumnType::DateTime64);
    }

    #[test]
    fn plain_string_is_not_datetime() {
        let schema = infer_schema(r#"[{"name":"alice"}]"#, "t").unwrap();
        assert_eq!(schema.columns[0].ch_type, ColumnType::String);
        // a date-only string stays String (conservative)
        let d = infer_schema(r#"[{"d":"2024-03-01"}]"#, "t").unwrap();
        assert_eq!(d.columns[0].ch_type, ColumnType::String);
    }

    #[test]
    fn infer_array_element_type() {
        let schema = infer_schema(r#"[{"tags":["a","b"]}]"#, "t").unwrap();
        assert_eq!(
            schema.columns[0].ch_type,
            ColumnType::Array(Box::new(ColumnType::String))
        );
        assert_eq!(schema.columns[0].ch_type.as_ch_str(true), "Array(String)");
    }

    #[test]
    fn infer_scalar_object_as_map() {
        let schema = infer_schema(r#"[{"attrs":{"a":1,"b":2}}]"#, "t").unwrap();
        assert_eq!(
            schema.columns[0].ch_type,
            ColumnType::Map(Box::new(ColumnType::String), Box::new(ColumnType::Int64))
        );
    }

    #[test]
    fn infer_nested_object_falls_back_to_string() {
        let schema = infer_schema(r#"[{"meta":{"inner":{"x":1}}}]"#, "t").unwrap();
        assert_eq!(schema.columns[0].ch_type, ColumnType::String);
    }

    #[test]
    fn low_cardinality_flagged_for_few_distinct_strings() {
        // 40 records, only 2 distinct values → low cardinality.
        let records: String = (0..40)
            .map(|i| {
                format!(
                    r#"{{"status":"{}"}}"#,
                    if i % 2 == 0 { "ok" } else { "err" }
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        let schema = infer_schema(&records, "t").unwrap();
        let status = schema.columns.iter().find(|c| c.name == "status").unwrap();
        assert!(status.low_cardinality);
        assert_eq!(status.ch_type_str(), "LowCardinality(String)");
    }

    #[test]
    fn high_cardinality_string_not_flagged() {
        // 40 records, all distinct → not low cardinality.
        let records: String = (0..40)
            .map(|i| format!(r#"{{"id":"u{}"}}"#, i))
            .collect::<Vec<_>>()
            .join("\n");
        let schema = infer_schema(&records, "t").unwrap();
        let id = schema.columns.iter().find(|c| c.name == "id").unwrap();
        assert!(!id.low_cardinality);
    }

    #[test]
    fn low_cardinality_off_for_small_sample() {
        let schema = infer_schema(r#"[{"status":"ok"},{"status":"ok"}]"#, "t").unwrap();
        let status = schema.columns.iter().find(|c| c.name == "status").unwrap();
        assert!(!status.low_cardinality);
    }

    #[test]
    fn merge_arrays_merges_element_types() {
        let result = merge_types(
            &ColumnType::Array(Box::new(ColumnType::Int64)),
            &ColumnType::Array(Box::new(ColumnType::Float64)),
        );
        assert_eq!(result, ColumnType::Array(Box::new(ColumnType::Float64)));
    }

    #[test]
    fn field_always_null_is_nullable_string() {
        let schema = infer_schema(r#"[{"a":null},{"a":null}]"#, "t").unwrap();
        let a = &schema.columns[0];
        assert_eq!(a.ch_type, ColumnType::String);
        assert!(a.nullable);
    }

    #[test]
    fn field_sometimes_null_keeps_real_type_and_is_nullable() {
        let schema = infer_schema(r#"[{"a":1},{"a":null}]"#, "t").unwrap();
        let a = &schema.columns[0];
        assert_eq!(a.ch_type, ColumnType::Int64);
        assert!(a.nullable);
    }

    #[test]
    fn field_present_every_record_never_null_is_not_nullable() {
        let schema = infer_schema(r#"[{"a":1},{"a":2}]"#, "t").unwrap();
        assert!(!schema.columns[0].nullable);
    }

    #[test]
    fn infer_large_u64_as_uint64() {
        let schema = infer_schema(r#"[{"a":18446744073709551615}]"#, "t").unwrap();
        assert_eq!(schema.columns[0].ch_type, ColumnType::UInt64);
    }

    #[test]
    fn merge_int64_and_uint64_widens_to_float64() {
        let result = merge_types(&ColumnType::Int64, &ColumnType::UInt64);
        assert_eq!(result, ColumnType::Float64);
    }

    #[test]
    fn record_count_matches_infer_schema_record_count() {
        let content = "{\"a\":1}\n{\"a\":2}\n{\"a\":3}";
        assert_eq!(record_count(content), 3);
        assert_eq!(infer_schema(content, "t").unwrap().columns.len(), 1);
    }
}
