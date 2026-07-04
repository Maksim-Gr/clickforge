#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    String,
    Int64,
    UInt64,
    Float64,
    Bool,
    DateTime64,
    Array(Box<ColumnType>),
    Map(Box<ColumnType>, Box<ColumnType>),
}

impl ColumnType {
    /// The ClickHouse type name, without any `Nullable(...)` wrapper.
    pub fn as_str(&self) -> String {
        match self {
            ColumnType::String => "String".to_string(),
            ColumnType::Int64 => "Int64".to_string(),
            ColumnType::UInt64 => "UInt64".to_string(),
            ColumnType::Float64 => "Float64".to_string(),
            ColumnType::Bool => "Bool".to_string(),
            ColumnType::DateTime64 => "DateTime64(3)".to_string(),
            // Array/Map elements are kept non-nullable for simplicity.
            ColumnType::Array(inner) => format!("Array({})", inner.as_str()),
            ColumnType::Map(key, val) => format!("Map({}, {})", key.as_str(), val.as_str()),
        }
    }

    /// ClickHouse forbids wrapping `Array`/`Map` in `Nullable(...)`.
    fn nullable_allowed(&self) -> bool {
        !matches!(self, ColumnType::Array(_) | ColumnType::Map(_, _))
    }

    pub fn as_ch_str(&self, nullable: bool) -> String {
        self.as_ch_str_with(nullable, false)
    }

    /// Renders the ClickHouse type, optionally wrapping a `String` in `LowCardinality(...)`.
    /// ClickHouse nests these as `LowCardinality(Nullable(String))`.
    pub fn as_ch_str_with(&self, nullable: bool, low_cardinality: bool) -> String {
        let nullable = nullable && self.nullable_allowed();
        let inner = if nullable {
            format!("Nullable({})", self.as_str())
        } else {
            self.as_str()
        };
        if low_cardinality && matches!(self, ColumnType::String) {
            format!("LowCardinality({})", inner)
        } else {
            inner
        }
    }
}

pub struct Column {
    pub name: String,
    pub ch_type: ColumnType,
    pub nullable: bool,
    pub low_cardinality: bool,
}

impl Column {
    /// The full ClickHouse column type, applying the `LowCardinality` wrapper when flagged.
    pub fn ch_type_str(&self) -> String {
        self.ch_type
            .as_ch_str_with(self.nullable, self.low_cardinality)
    }
}

pub struct InferredSchema {
    pub table_name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)] // names match ClickHouse engine names intentionally
pub enum TableEngine {
    MergeTree,
    ReplicatedMergeTree,
    ReplacingMergeTree,
    SummingMergeTree,
}

impl std::str::FromStr for TableEngine {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "MergeTree" => Ok(TableEngine::MergeTree),
            "ReplicatedMergeTree" => Ok(TableEngine::ReplicatedMergeTree),
            "ReplacingMergeTree" => Ok(TableEngine::ReplacingMergeTree),
            "SummingMergeTree" => Ok(TableEngine::SummingMergeTree),
            other => Err(format!(
                "unknown engine '{}'; valid options: MergeTree, ReplicatedMergeTree, ReplacingMergeTree, SummingMergeTree",
                other
            )),
        }
    }
}

impl std::fmt::Display for TableEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            TableEngine::MergeTree => "MergeTree",
            TableEngine::ReplicatedMergeTree => "ReplicatedMergeTree",
            TableEngine::ReplacingMergeTree => "ReplacingMergeTree",
            TableEngine::SummingMergeTree => "SummingMergeTree",
        };
        f.write_str(s)
    }
}

pub struct EngineConfig {
    pub engine: TableEngine,
    pub order_by: Vec<String>,
    pub sum_columns: Vec<String>,
}

/// Quotes a ClickHouse identifier (column or table name), escaping embedded
/// backticks by doubling them so names derived from arbitrary JSON keys can't
/// break out of the identifier or (worse) inject SQL.
pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Quotes a `database.table` qualified identifier, quoting each part independently.
pub fn quote_qualified(database: &str, table: &str) -> String {
    format!("{}.{}", quote_ident(database), quote_ident(table))
}

/// Escapes a value for embedding inside a single-quoted ClickHouse string literal
/// (distinct from `quote_ident`: this doubles single quotes, not backticks).
pub fn quote_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_from_str_valid() {
        assert_eq!(
            "MergeTree".parse::<TableEngine>().unwrap(),
            TableEngine::MergeTree
        )
    }

    #[test]
    fn engine_from_str_invalid() {
        assert!("InvalidEngine".parse::<TableEngine>().is_err())
    }

    #[test]
    fn quote_ident_plain_name() {
        assert_eq!(quote_ident("user_id"), "`user_id`");
    }

    #[test]
    fn quote_ident_escapes_embedded_backtick() {
        assert_eq!(quote_ident("a`b"), "`a``b`");
    }

    #[test]
    fn quote_ident_handles_reserved_word() {
        assert_eq!(quote_ident("order"), "`order`");
    }

    #[test]
    fn quote_qualified_quotes_both_parts() {
        assert_eq!(quote_qualified("raw", "events"), "`raw`.`events`");
    }

    #[test]
    fn quote_string_literal_escapes_single_quote() {
        assert_eq!(quote_string_literal("it's"), "it''s");
    }
}
