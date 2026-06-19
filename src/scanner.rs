use crate::schema::{ColumnType, EngineConfig, InferredSchema, TableEngine};

const TIMESTAMP_SUFFIXES: &[&str] = &["_at", "_time", "_date"];
const TIMESTAMP_EXACT: &[&str] = &[
    "timestamp",
    "date",
    "created_at",
    "updated_at",
    "event_time",
];
const ID_EXACT: &[&str] = &["id", "uuid"];
const ID_SUFFIXES: &[&str] = &["_id", "_uuid"];

fn is_timestamp(name: &str) -> bool {
    let lower = name.to_lowercase();
    if TIMESTAMP_EXACT.contains(&lower.as_str()) {
        return true;
    }
    TIMESTAMP_SUFFIXES.iter().any(|s| lower.ends_with(s))
}

fn is_id(name: &str) -> bool {
    let lower = name.to_lowercase();
    if ID_EXACT.contains(&lower.as_str()) {
        return true;
    }
    ID_SUFFIXES.iter().any(|s| lower.ends_with(s))
}

fn is_numeric(col_type: &ColumnType) -> bool {
    matches!(col_type, ColumnType::Int64 | ColumnType::Float64)
}

pub struct FieldRole {
    pub name: String,
    pub ch_type: String,
    pub nullable: bool,
    pub timestamp: bool,
    pub id: bool,
    pub numeric: bool,
}

pub struct ScanResult {
    pub fields: Vec<FieldRole>,
    pub suggestions: Vec<EngineSuggestion>,
}

pub struct EngineSuggestion {
    pub engine: TableEngine,
    pub order_by: Vec<String>,
    pub sum_columns: Vec<String>,
    pub rationale: String,
}

impl EngineSuggestion {
    pub fn to_engine_config(&self) -> EngineConfig {
        EngineConfig {
            engine: self.engine.clone(),
            order_by: self.order_by.clone(),
            sum_columns: self.sum_columns.clone(),
        }
    }
}

pub fn scan(schema: &InferredSchema, replicated: bool) -> ScanResult {
    let fields: Vec<FieldRole> = schema
        .columns
        .iter()
        .map(|col| {
            let id = is_id(&col.name);
            FieldRole {
                name: col.name.clone(),
                ch_type: col.ch_type.as_ch_str(col.nullable),
                nullable: col.nullable,
                timestamp: is_timestamp(&col.name),
                id,
                numeric: is_numeric(&col.ch_type) && !id,
            }
        })
        .collect();

    let timestamp_fields: Vec<&str> = fields
        .iter()
        .filter(|f| f.timestamp)
        .map(|f| f.name.as_str())
        .collect();

    let id_fields: Vec<&str> = fields
        .iter()
        .filter(|f| f.id)
        .map(|f| f.name.as_str())
        .collect();

    let mut suggestions: Vec<EngineSuggestion> = Vec::new();

    // 1. MergeTree — always
    let mt_order: Vec<String> = if let Some(ts) = timestamp_fields.first() {
        vec![ts.to_string()]
    } else {
        vec![]
    };
    let mt_engine = if replicated {
        TableEngine::ReplicatedMergeTree
    } else {
        TableEngine::MergeTree
    };
    suggestions.push(EngineSuggestion {
        engine: mt_engine,
        order_by: mt_order,
        sum_columns: vec![],
        rationale: "general purpose time-series table".to_string(),
    });

    // 2. ReplacingMergeTree — if id + timestamp
    // ReplacingMergeTree has no separate Replicated variant in this tool's scope;
    // always suggest ReplacingMergeTree and let the user pair it with ON CLUSTER.
    if !id_fields.is_empty() && !timestamp_fields.is_empty() {
        let mut order_by: Vec<String> = id_fields.iter().take(1).map(|s| s.to_string()).collect();
        order_by.push(timestamp_fields[0].to_string());
        let rationale = format!(
            "deduplicates rows by `{}` — good for upsert-like data",
            id_fields[0]
        );
        suggestions.push(EngineSuggestion {
            engine: TableEngine::ReplacingMergeTree,
            order_by,
            sum_columns: vec![],
            rationale,
        });
    }

    // 3. SummingMergeTree — if there are numeric metrics plus a dimension to group by.
    // Keep the grouping key narrow (one primary id + one timestamp) so it stays a
    // sensible default; a wide ORDER BY across every id would rarely be what you want.
    let numeric_fields: Vec<&str> = fields
        .iter()
        .filter(|f| f.numeric)
        .map(|f| f.name.as_str())
        .collect();
    let dimensions: Vec<&str> = id_fields
        .iter()
        .take(1)
        .chain(timestamp_fields.iter().take(1))
        .copied()
        .collect();
    if !numeric_fields.is_empty() && !dimensions.is_empty() {
        let order_by: Vec<String> = dimensions.iter().map(|s| s.to_string()).collect();
        let sum_columns: Vec<String> = numeric_fields.iter().map(|s| s.to_string()).collect();
        let rationale = format!(
            "pre-aggregates ({}) grouped by ({})",
            sum_columns.join(", "),
            order_by.join(", ")
        );
        suggestions.push(EngineSuggestion {
            engine: TableEngine::SummingMergeTree,
            order_by,
            sum_columns,
            rationale,
        });
    }

    ScanResult {
        fields,
        suggestions,
    }
}

pub fn print_scan(result: &ScanResult, source: &str, record_count: usize) {
    println!(
        "Field analysis: {}  ({} records, {} fields)\n",
        source,
        record_count,
        result.fields.len()
    );

    let name_w = result
        .fields
        .iter()
        .map(|f| f.name.len())
        .max()
        .unwrap_or(0);
    let type_w = result
        .fields
        .iter()
        .map(|f| f.ch_type.len())
        .max()
        .unwrap_or(0);

    for f in &result.fields {
        let req = if f.nullable { "nullable " } else { "required " };
        let role = if f.timestamp {
            "→ Timestamp-like"
        } else if f.id {
            "→ ID-like"
        } else if f.numeric {
            "→ Numeric"
        } else {
            ""
        };
        println!(
            "  {:<nw$}  {:<tw$}  {}{}",
            f.name,
            f.ch_type,
            req,
            role,
            nw = name_w,
            tw = type_w,
        );
    }

    println!("\nSuggested engines:\n");

    for (i, s) in result.suggestions.iter().enumerate() {
        println!("  {}. {}", i + 1, s.engine);
        let order_str = if s.order_by.is_empty() {
            "tuple()".to_string()
        } else {
            format!("({})", s.order_by.join(", "))
        };
        println!("     ORDER BY {}", order_str);
        if !s.sum_columns.is_empty() {
            println!("     SUM COLUMNS ({})", s.sum_columns.join(", "));
        }
        println!("     → {}\n", s.rationale);
    }

    println!("To generate a migration with the chosen engine, run:");
    let input = source;
    for s in &result.suggestions {
        let engine_name = match &s.engine {
            TableEngine::MergeTree => "MergeTree",
            TableEngine::ReplicatedMergeTree => "ReplicatedMergeTree",
            TableEngine::ReplacingMergeTree => "ReplacingMergeTree",
            TableEngine::SummingMergeTree => "SummingMergeTree",
        };
        if s.order_by.is_empty() {
            println!("  clickforge table {} --engine {}", input, engine_name);
        } else {
            println!(
                "  clickforge table {} --engine {} --order-by {}",
                input,
                engine_name,
                s.order_by.join(",")
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::infer_schema;

    #[test]
    fn suggests_summing_for_metrics_with_dimensions() {
        let schema = infer_schema(
            r#"[{"user_id":"u1","event_time":"2024-03-01T00:00:00Z","amount":5}]"#,
            "t",
        )
        .unwrap();
        let result = scan(&schema, false);
        let summing = result
            .suggestions
            .iter()
            .find(|s| s.engine == TableEngine::SummingMergeTree)
            .expect("expected a SummingMergeTree suggestion");
        assert_eq!(summing.sum_columns, vec!["amount".to_string()]);
        assert!(summing.order_by.contains(&"user_id".to_string()));
        assert!(summing.order_by.contains(&"event_time".to_string()));
    }

    #[test]
    fn summing_order_by_uses_one_id_plus_one_timestamp() {
        // Multiple id fields present — only the first should be in the grouping key.
        let schema = infer_schema(
            r#"[{"user_id":"u1","session_id":"s1","video_id":"v1","event_time":"2024-03-01T00:00:00Z","amount":5}]"#,
            "t",
        )
        .unwrap();
        let result = scan(&schema, false);
        let summing = result
            .suggestions
            .iter()
            .find(|s| s.engine == TableEngine::SummingMergeTree)
            .expect("expected a SummingMergeTree suggestion");
        assert_eq!(
            summing.order_by,
            vec!["user_id".to_string(), "event_time".to_string()]
        );
    }

    #[test]
    fn no_summing_without_numeric_metric() {
        let schema = infer_schema(
            r#"[{"user_id":"u1","event_time":"2024-03-01T00:00:00Z"}]"#,
            "t",
        )
        .unwrap();
        let result = scan(&schema, false);
        assert!(
            !result
                .suggestions
                .iter()
                .any(|s| s.engine == TableEngine::SummingMergeTree)
        );
    }
}
