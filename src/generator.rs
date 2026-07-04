use crate::schema::{
    ColumnType, EngineConfig, InferredSchema, TableEngine, quote_ident, quote_qualified,
    quote_string_literal,
};

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
        let streams_t = quote_qualified("streams", t);
        let streams_mv_name = quote_qualified("streams", &format!("{t}_mv"));
        let raw_t = quote_qualified("raw", t);
        let raw_mv_name = quote_qualified("raw", &format!("{t}_mv"));
        let datalake_t = quote_qualified("datalake", t);
        format!(
            "DROP TABLE IF EXISTS {streams_t} ON CLUSTER {c} SYNC;\n\
             DROP VIEW IF EXISTS {streams_mv_name} ON CLUSTER {c} SYNC;\n\
             DROP TABLE IF EXISTS {raw_t} ON CLUSTER {c} SYNC;\n\
             DROP VIEW IF EXISTS {raw_mv_name} ON CLUSTER {c} SYNC;\n\
             DROP TABLE IF EXISTS {datalake_t} ON CLUSTER {c} SYNC;"
        )
    }

    fn streams_table(&self) -> String {
        let t = &self.schema.table_name;
        let c = &self.cluster;
        let k = &self.kafka;
        let qt = quote_qualified("streams", t);
        let topic = quote_string_literal(t);
        format!(
            "CREATE TABLE IF NOT EXISTS {qt} ON CLUSTER {c}\n\
             (\n\
             \t`message` String\n\
             )\n\
             \tENGINE = Kafka({k}) SETTINGS kafka_topic_list =\n\
             \t'private.{{environment}}.{topic}.v1', kafka_group_name =\n\
             \t'clickhouse-{{environment}}xdcl-{topic}-shard-1', kafka_format = 'RawBLOB';"
        )
    }

    fn raw_table(&self) -> String {
        let t = &self.schema.table_name;
        let c = &self.cluster;
        let qt = quote_qualified("raw", t);
        let path_t = quote_string_literal(t);
        format!(
            "CREATE TABLE IF NOT EXISTS {qt} ON CLUSTER {c}\n\
             (\n\
             \t`message`       String,\n\
             \t`_key`          String,\n\
             \t`_offset`       UInt64,\n\
             \t`_partition`    UInt64,\n\
             \t`_timestamp_ms` DateTime64(3),\n\
             \t`_topic`        LowCardinality(String),\n\
             \t`_row_created`  DateTime DEFAULT nowInBlock()\n\
             )\n\
             \tENGINE = ReplicatedMergeTree('/clickhouse/{{cluster}}/tables/raw/{path_t}/{{shard}}', '{{replica}}')\n\
             \tPARTITION BY toYYYYMM(_row_created)\n\
             \tORDER BY _row_created\n\
             \tSETTINGS index_granularity = 8192;"
        )
    }

    fn datalake_table(&self) -> String {
        let t = &self.schema.table_name;
        let c = &self.cluster;
        let qt = quote_qualified("datalake", t);
        let path_t = quote_string_literal(t);
        let cols: String = self
            .schema
            .columns
            .iter()
            .map(|col| format!("\t{} {},\n", quote_ident(&col.name), col.ch_type_str()))
            .collect();
        format!(
            "CREATE TABLE IF NOT EXISTS {qt} ON CLUSTER {c}\n\
             (\n\
             {cols}\
             \t`_timestamp_ms` DateTime64(3),\n\
             \t`_topic`        LowCardinality(String),\n\
             \t`_row_created`  DateTime DEFAULT nowInBlock()\n\
             )\n\
             \tENGINE = ReplicatedMergeTree('/clickhouse/{{cluster}}/tables/datalake/{path_t}/{{shard}}', '{{replica}}')\n\
             \tPARTITION BY toYYYYMM(_timestamp_ms)\n\
             \tORDER BY _timestamp_ms\n\
             \tSETTINGS index_granularity = 8192;"
        )
    }

    fn raw_mv(&self) -> String {
        let t = &self.schema.table_name;
        let c = &self.cluster;
        let mv_name = quote_qualified("raw", &format!("{t}_mv"));
        let datalake_t = quote_qualified("datalake", t);
        let raw_t = quote_qualified("raw", t);
        let extracts: String = self
            .schema
            .columns
            .iter()
            .map(|col| {
                format!(
                    "\t\tJSONExtract(message, '{}', '{}') AS {},\n",
                    quote_string_literal(&col.name),
                    col.ch_type_str(),
                    quote_ident(&col.name)
                )
            })
            .collect();
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name}\n\
             \tON CLUSTER {c} TO {datalake_t} AS\n\
             SELECT * FROM (\n\
             \tSELECT\n\
             {extracts}\
             \t\t`_timestamp_ms`,\n\
             \t\t`_topic`,\n\
             \t\t`_row_created`\n\
             \tFROM {raw_t}\n\
             ) SETTINGS cast_keep_nullable = 1;"
        )
    }

    fn streams_mv(&self) -> String {
        let t = &self.schema.table_name;
        let c = &self.cluster;
        let mv_name = quote_qualified("streams", &format!("{t}_mv"));
        let raw_t = quote_qualified("raw", t);
        let streams_t = quote_qualified("streams", t);
        format!(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS {mv_name}\n\
             \tON CLUSTER {c}\n\
             \tTO {raw_t} (\n\
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
             FROM {streams_t};"
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
        let qt = quote_ident(t);

        let cluster_clause = self
            .cluster
            .as_ref()
            .map(|c| format!(" ON CLUSTER {c}"))
            .unwrap_or_default();

        let cols: String = self
            .schema
            .columns
            .iter()
            .map(|col| format!("\t{} {},\n", quote_ident(&col.name), col.ch_type_str()))
            .collect();
        // strip trailing comma+newline from last column
        let cols = cols.trim_end_matches(",\n").to_string() + "\n";

        let engine_str = self.engine_str();

        let order_str = if self.config.order_by.is_empty() {
            "tuple()".to_string()
        } else {
            let quoted: Vec<String> = self
                .config
                .order_by
                .iter()
                .map(|c| quote_ident(c))
                .collect();
            format!("({})", quoted.join(", "))
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
            .map(|first| format!("\tPARTITION BY toYYYYMM({})\n", quote_ident(first)))
            .unwrap_or_default();

        format!(
            "CREATE TABLE IF NOT EXISTS {qt}{cluster_clause}\n\
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
        let qt = quote_ident(t);
        match &self.cluster {
            Some(c) => format!("DROP TABLE IF EXISTS {qt} ON CLUSTER {c} SYNC;"),
            None => format!("DROP TABLE IF EXISTS {qt};"),
        }
    }

    fn engine_str(&self) -> String {
        let t = &self.schema.table_name;
        match &self.config.engine {
            TableEngine::MergeTree => "MergeTree()".to_string(),
            TableEngine::ReplicatedMergeTree => {
                let path_t = quote_string_literal(t);
                format!(
                    "ReplicatedMergeTree('/clickhouse/{{cluster}}/tables/{path_t}/{{shard}}', '{{replica}}')"
                )
            }
            TableEngine::ReplacingMergeTree => "ReplacingMergeTree()".to_string(),
            TableEngine::SummingMergeTree => {
                if self.config.sum_columns.is_empty() {
                    "SummingMergeTree()".to_string()
                } else {
                    let quoted: Vec<String> = self
                        .config
                        .sum_columns
                        .iter()
                        .map(|c| quote_ident(c))
                        .collect();
                    format!("SummingMergeTree({})", quoted.join(", "))
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
        assert!(sql.contains("PARTITION BY toYYYYMM(`event_time`)"));
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

    #[test]
    fn column_name_with_backtick_is_escaped_in_table_ddl() {
        let schema = schema_with(vec![col("a`b", ColumnType::String)]);
        let config = EngineConfig {
            engine: TableEngine::MergeTree,
            order_by: vec![],
            sum_columns: vec![],
        };
        let sql = TableGenerator::new(&schema, config, None).generate_up();
        assert!(sql.contains("`a``b` String"));
    }

    #[test]
    fn column_name_with_quote_is_escaped_in_kafka_pipeline() {
        let schema = schema_with(vec![col("it's", ColumnType::String)]);
        let generator = Generator::new(&schema, "cluster".to_string(), "kafka".to_string());
        let sql = generator.generate_up();
        // identifier position: backtick-quoted, unescaped since no backtick in name
        assert!(sql.contains("`it's`"));
        // JSONExtract path argument: single-quote doubled inside the string literal
        assert!(sql.contains("JSONExtract(message, 'it''s', 'String') AS `it's`"));
    }
}
