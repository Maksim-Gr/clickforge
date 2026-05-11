use crate::schema::{Column, ColumnType, InferredSchema};
use serde_json::Value;
use std::collections::HashMap;

pub fn infer_schema(content: &str, table_name: &str) -> Result<InferredSchema, String> {
    let mut field_order: Vec<String> = Vec::new();
    let mut field_types: HashMap<String, ColumnType> = HashMap::new();
    let mut field_seen: HashMap<String, usize> = HashMap::new(); // how many records contained this field
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
            Column {
                name,
                ch_type,
                nullable,
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
        Value::String(_) => ColumnType::String,
        Value::Number(n) => {
            if n.as_i64().is_some() || n.as_u64().is_some() {
                ColumnType::Int64
            } else {
                ColumnType::Float64
            }
        }
        _ => ColumnType::String,
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
}
