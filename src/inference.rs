use crate::schema::{Column, ColumnType, InferredSchema};
use serde_json::Value;
use std::collections::{HashMap, HashSet};

/// Stop counting distinct values once a column exceeds this many — past it the
/// column is clearly not low-cardinality, so the exact count no longer matters.
const CARDINALITY_CAP: usize = 100;
/// Don't flag low-cardinality on small samples — too little signal.
const MIN_RECORDS_FOR_LC: usize = 20;

pub fn infer_schema(content: &str, table_name: &str) -> Result<InferredSchema, String> {
    let mut field_order: Vec<String> = Vec::new();
    let mut field_types: HashMap<String, ColumnType> = HashMap::new();
    let mut field_seen: HashMap<String, usize> = HashMap::new(); // how many records contained this field
    let mut distinct_values: HashMap<String, HashSet<String>> = HashMap::new(); // capped distinct string values
    let mut record_count = 0;

    let values: Vec<Value> = {
        let trimmed = content.trim();
        if trimmed.starts_with('[') {
            // JSON array
            serde_json::from_str(trimmed).map_err(|e| format!("Error parsing JSON array: {}", e))?
        } else {
            // Stream of JSON objects (NDJSON or concatenated pretty-printed)
            serde_json::Deserializer::from_str(trimmed)
                .into_iter::<Value>()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("Error parsing JSON: {}", e))?
        }
    };

    for (i, value) in values.into_iter().enumerate() {
        let obj = value.as_object().ok_or_else(|| {
            format!(
                "record {}: expected a JSON object, got something else",
                i + 1
            )
        })?;

        for (key, value) in obj {
            let inferred = infer_type(value);
            if let Some(existing) = field_types.get(key) {
                field_types.insert(key.clone(), merge_types(existing, &inferred));
            } else {
                field_order.push(key.clone());
                field_types.insert(key.clone(), inferred);
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
            let ch_type = field_types.remove(&name).unwrap();
            let seen = *field_seen.get(&name).unwrap_or(&0);
            // A field is nullable if it was missing from at least one record
            let nullable = seen < record_count;
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
    let trimmed = content.trim();
    if trimmed.starts_with('[') {
        serde_json::from_str::<Vec<Value>>(trimmed)
            .map(|v| v.len())
            .unwrap_or(0)
    } else {
        serde_json::Deserializer::from_str(trimmed)
            .into_iter::<Value>()
            .count()
    }
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
            if n.as_i64().is_some() || n.as_u64().is_some() {
                ColumnType::Int64
            } else {
                ColumnType::Float64
            }
        }
        Value::Array(items) => {
            let elem = items
                .iter()
                .map(infer_type)
                .reduce(|a, b| merge_types(&a, &b))
                .unwrap_or(ColumnType::String);
            ColumnType::Array(Box::new(elem))
        }
        Value::Object(map) => {
            // Map(String, V) only when every value is a homogeneous scalar; otherwise String.
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
        (ColumnType::Float64, ColumnType::Int64) | (ColumnType::Int64, ColumnType::Float64) => {
            ColumnType::Float64
        }
        (ColumnType::Array(x), ColumnType::Array(y)) => {
            ColumnType::Array(Box::new(merge_types(x, y)))
        }
        (ColumnType::Map(_, x), ColumnType::Map(_, y)) => {
            ColumnType::Map(Box::new(ColumnType::String), Box::new(merge_types(x, y)))
        }
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
}
