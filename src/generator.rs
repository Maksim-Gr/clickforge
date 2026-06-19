use crate::schema::{ColumnType, EngineConfig, InferredSchema, TableEngine};

pub struct Generator<'a> {
    schema: &'a InferredSchema,
    cluster: String,
    kafka: String,
}

impl<'a> Generator<'a> {
    pub fn new(schema: &'a InferredSchema, cluster: String, kafka: String) -> Self {
        Generator {
            schema,
            cluster,
            kafka,
        }
    }

    pub fn generate_up(&self) -> String {
        [
            self.streams_table(),
            self.raw_table(),
            self.datalake_table(),
            self.raw_mv(),
            self.streams_mv(),
        ]
        .join("\n\n")
    }

    pub fn generate_down(&self) -> String {
        let t = &self.schema.table_name;
        let c = &self.cluster;
        format!(
            "DROP TABLE IF EXISTS streams.{t} ON CLUSTER {c} SYNC;\n\
             DROP VIEW IF EXISTS streams.{t}_mv ON CLUSTER {c} SYNC;\n\
             DROP TABLE IF EXISTS raw.{t} ON CLUSTER {c} SYNC;\n\
             DROP VIEW IF EXISTS raw.{t}_mv ON CLUSTER {c} SYNC;\n\
             DROP TABLE IF EXISTS datalake.{t} ON CLUSTER {c} SYNC;"
        )
    }

    fn streams_table(&self) -> String {
        let t = &self.schema.table_name;
        let c = &self.cluster;
        let k = &self.kafka;
        format!(
            "CREATE TABLE IF NOT EXISTS streams.{t} ON CLUSTER {c}\n\
             (\n\
             \t`message` String\n\
             )\n\
             \tENGINE = Kafka({k}) SETTINGS kafka_topic_list =\n\
             \t'private.{{environment}}.{t}.v1', kafka_group_name =\n\
             \t'clickhouse-{{environment}}xdcl-{t}-shard-1', kafka_format = 'RawBLOB';"
        )
    }

    fn raw_table(&self) -> String {
        let t = &self.schema.table_name;
        let c = &self.cluster;
        format!(
            "CREATE TABLE IF NOT EXISTS raw.{t} ON CLUSTER {c}\n\
             (\n\
             \t`message`       String,\n\
             \t`_key`          String,\n\
             \t`_offset`       UInt64,\n\
             \t`_partition`    UInt64,\n\
             \t`_timestamp_ms` DateTime64(3),\n\
             \t`_topic`        LowCardinality(String),\n\
             \t`_row_created`  DateTime DEFAULT nowInBlock()\n\
             )\n\
             \tENGINE = ReplicatedMergeTree('/clickhouse/{{cluster}}/tables/raw/{t}/{{shard}}', '{{replica}}')\n\
             \tPARTITION BY toYYYYMM(_row_created)\n\
             \tORDER BY _row_created\n\
             \tSETTINGS index_granularity = 8192;"
        )
    }

    fn datalake_table(&self) -> String {
        let t = &self.schema.table_name;
        let c = &self.cluster;
        let cols: String = self
            .schema
            .columns
            .iter()
            .map(|col| format!("\t`{}` {},\n", col.name, col.ch_type_str()))
            .collect();
        format!(
            "CREATE TABLE IF NOT EXISTS datalake.{t} ON CLUSTER {c}\n\
             (\n\
             {cols}\
             \t`_timestamp_ms` DateTime64(3),\n\
             \t`_topic`        LowCardinality(String),\n\
             \t`_row_created`  DateTime DEFAULT nowInBlock()\n\
             )\n\
             \tENGINE = ReplicatedMergeTree('/clickhouse/{{cluster}}/tables/datalake/{t}/{{shard}}', '{{replica}}')\n\
             \tPARTITION BY toYYYYMM(_timestamp_ms)\n\
             \tORDER BY _timestamp_ms\n\
             \tSETTINGS index_granularity = 8192;"
        )
    }

    fn raw_mv(&self) -> String {
        let t = &self.schema.table_name;
        let c = &self.cluster;
        let extracts: String = self
            .schema
            .columns
            .iter()
            .map(|col| {
                format!(
                    "\t\tJSONExtract(message, '{}', '{}') AS {},\n",
                    col.name,
                    col.ch_type_str(),
                    col.name
                )
            })
            .collect();
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS raw.{t}_mv\n\
             \tON CLUSTER {c} TO datalake.{t} AS\n\
             SELECT * FROM (\n\
             \tSELECT\n\
             {extracts}\
             \t\t`_timestamp_ms`,\n\
             \t\t`_topic`,\n\
             \t\t`_row_created`\n\
             \tFROM raw.{t}\n\
             ) SETTINGS cast_keep_nullable = 1;"
        )
    }

    fn streams_mv(&self) -> String {
        let t = &self.schema.table_name;
        let c = &self.cluster;
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS streams.{t}_mv\n\
             \tON CLUSTER {c}\n\
             \tTO raw.{t} (\n\
             \t\t`message`        String,\n\
             \t\t`_key`           String,\n\
             \t\t`_offset`        UInt64,\n\
             \t\t`_partition`     UInt64,\n\
             \t\t`_timestamp_ms`  DateTime64(3),\n\
             \t\t`_topic`         LowCardinality(String),\n\
             \t\t`_row_created`   DateTime\n\
             \t) AS\n\
             SELECT\n\
             \tmessage,\n\
             \t_key,\n\
             \t_offset,\n\
             \t_partition,\n\
             \tassumeNotNull(_timestamp_ms) AS _timestamp_ms,\n\
             \t_topic,\n\
             \tnowInBlock() AS _row_created\n\
             FROM streams.{t};"
        )
    }
}

pub struct TableGenerator<'a> {
    schema: &'a InferredSchema,
    config: EngineConfig,
    cluster: Option<String>,
}

impl<'a> TableGenerator<'a> {
    pub fn new(schema: &'a InferredSchema, config: EngineConfig, cluster: Option<String>) -> Self {
        TableGenerator {
            schema,
            config,
            cluster,
        }
    }

    pub fn generate_up(&self) -> String {
        let t = &self.schema.table_name;

        let cluster_clause = self
            .cluster
            .as_ref()
            .map(|c| format!(" ON CLUSTER {c}"))
            .unwrap_or_default();

        let cols: String = self
            .schema
            .columns
            .iter()
            .map(|col| format!("\t`{}` {},\n", col.name, col.ch_type_str()))
            .collect();
        // strip trailing comma+newline from last column
        let cols = cols.trim_end_matches(",\n").to_string() + "\n";

        let engine_str = self.engine_str();

        let order_str = if self.config.order_by.is_empty() {
            "tuple()".to_string()
        } else {
            format!("({})", self.config.order_by.join(", "))
        };

        // Add PARTITION BY only when the first ORDER BY field is an actual DateTime
        // column. `toYYYYMM` requires a date/datetime argument, so partitioning by a
        // field that merely looks like a timestamp by name (but inferred as String)
        // would produce SQL that ClickHouse rejects.
        let partition_clause = self
            .config
            .order_by
            .first()
            .filter(|first| {
                self.schema
                    .columns
                    .iter()
                    .any(|c| &c.name == *first && c.ch_type == ColumnType::DateTime64)
            })
            .map(|first| format!("\tPARTITION BY toYYYYMM({first})\n"))
            .unwrap_or_default();

        format!(
            "CREATE TABLE IF NOT EXISTS {t}{cluster_clause}\n\
             (\n\
             {cols}\
             )\n\
             \tENGINE = {engine_str}\n\
             {partition_clause}\
             \tORDER BY {order_str}\n\
             \tSETTINGS index_granularity = 8192;"
        )
    }

    pub fn generate_down(&self) -> String {
        let t = &self.schema.table_name;
        match &self.cluster {
            Some(c) => format!("DROP TABLE IF EXISTS {t} ON CLUSTER {c} SYNC;"),
            None => format!("DROP TABLE IF EXISTS {t};"),
        }
    }

    fn engine_str(&self) -> String {
        let t = &self.schema.table_name;
        match &self.config.engine {
            TableEngine::MergeTree => "MergeTree()".to_string(),
            TableEngine::ReplicatedMergeTree => {
                format!(
                    "ReplicatedMergeTree('/clickhouse/{{cluster}}/tables/{t}/{{shard}}', '{{replica}}')"
                )
            }
            TableEngine::ReplacingMergeTree => "ReplacingMergeTree()".to_string(),
            TableEngine::SummingMergeTree => {
                if self.config.sum_columns.is_empty() {
                    "SummingMergeTree()".to_string()
                } else {
                    format!("SummingMergeTree({})", self.config.sum_columns.join(", "))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::Column;

    fn schema_with(columns: Vec<Column>) -> InferredSchema {
        InferredSchema {
            table_name: "t".to_string(),
            columns,
        }
    }

    fn col(name: &str, ch_type: ColumnType) -> Column {
        Column {
            name: name.to_string(),
            ch_type,
            nullable: false,
            low_cardinality: false,
        }
    }

    #[test]
    fn partition_by_emitted_for_datetime_order_key() {
        let schema = schema_with(vec![col("event_time", ColumnType::DateTime64)]);
        let config = EngineConfig {
            engine: TableEngine::MergeTree,
            order_by: vec!["event_time".to_string()],
            sum_columns: vec![],
        };
        let sql = TableGenerator::new(&schema, config, None).generate_up();
        assert!(sql.contains("PARTITION BY toYYYYMM(event_time)"));
    }

    #[test]
    fn no_partition_by_for_string_named_like_timestamp() {
        // `event_date` is a String (date-only strings stay String); toYYYYMM would be invalid.
        let schema = schema_with(vec![col("event_date", ColumnType::String)]);
        let config = EngineConfig {
            engine: TableEngine::MergeTree,
            order_by: vec!["event_date".to_string()],
            sum_columns: vec![],
        };
        let sql = TableGenerator::new(&schema, config, None).generate_up();
        assert!(!sql.contains("PARTITION BY"));
    }
}
