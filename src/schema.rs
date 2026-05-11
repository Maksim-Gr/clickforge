#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnType {
    String,
    Int64,
    Float64,
    Bool,
}

impl ColumnType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ColumnType::String => "String",
            ColumnType::Int64 => "Int64",
            ColumnType::Float64 => "Float64",
            ColumnType::Bool => "Bool",
        }
    }

    pub fn as_nullable_str(&self) -> &'static str {
        match self {
            ColumnType::String => "Nullable(String)",
            ColumnType::Int64 => "Nullable(Int64)",
            ColumnType::Float64 => "Nullable(Float64)",
            ColumnType::Bool => "Nullable(Bool)",
        }
    }

    pub fn as_ch_str(&self, nullable: bool) -> &'static str {
        if nullable {
            self.as_nullable_str()
        } else {
            self.as_str()
        }
    }
}

pub struct Column {
    pub name: String,
    pub ch_type: ColumnType,
    pub nullable: bool,
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
}
