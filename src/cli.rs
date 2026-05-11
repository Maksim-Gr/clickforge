use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "schemamaker",
    about = "Generate ClickHouse migrations from a JSON file",
    long_about = "Generate ClickHouse migrations from a JSON file.\n\nSubcommands:\n  scan     Analyze fields and pick an engine\n  table    Generate a CREATE TABLE migration\n  kafka    Generate a full Kafka→ClickHouse pipeline\n  explain  Explain index usage for a SQL query against a live ClickHouse\n\nTip: start with `schemamaker scan <file.ndjson>` if unsure which engine to use.",
    version = "0.1.3-beta"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Generate full Kafka→ClickHouse pipeline migrations (streams, raw, datalake)
    Kafka(KafkaArgs),
    /// Scan JSON fields and suggest suitable ClickHouse table engines
    Scan(ScanArgs),
    /// Generate a simple CREATE TABLE migration from JSON
    Table(TableArgs),
    /// Explain how ClickHouse uses indexes for a SQL query
    Explain(ExplainArgs),
}

#[derive(Parser, Debug)]
pub struct KafkaArgs {
    /// Path to a NDJSON file (one JSON object per line)
    pub input: PathBuf,
    /// Override table name (defaults to input file stem)
    #[arg(short, long)]
    pub name: Option<String>,
    /// ClickHouse cluster name
    #[arg(short, long, default_value = "clickhouse_datalake")]
    pub cluster: String,
    /// Kafka collection name
    #[arg(short, long, default_value = "kafka")]
    pub kafka: String,
    /// Output directory for migration files
    #[arg(short, long, default_value = ".")]
    pub output_dir: PathBuf,
}

#[derive(Parser, Debug)]
pub struct ScanArgs {
    /// Path to a NDJSON file (one JSON object per line)
    pub input: PathBuf,
    /// Override table name (defaults to input file stem)
    #[arg(short, long)]
    pub name: Option<String>,
    /// Cluster name; when provided, includes ReplicatedMergeTree in suggestions
    #[arg(short, long)]
    pub cluster: Option<String>,
}

#[derive(Parser, Debug)]
pub struct TableArgs {
    /// Path to a NDJSON file (one JSON object per line)
    pub input: PathBuf,
    /// Override table name (defaults to input file stem)
    #[arg(short, long)]
    pub name: Option<String>,
    /// Table engine: MergeTree, ReplicatedMergeTree, ReplacingMergeTree, SummingMergeTree
    /// If omitted, inferred automatically from JSON fields
    #[arg(short, long)]
    pub engine: Option<String>,
    /// Comma-separated ORDER BY fields, e.g. 'id,created_at' (overrides inference)
    #[arg(long)]
    pub order_by: Option<String>,
    /// ClickHouse cluster name (required for ReplicatedMergeTree)
    #[arg(short, long)]
    pub cluster: Option<String>,
    /// Output directory for migration files
    #[arg(short, long, default_value = ".")]
    pub output_dir: PathBuf,
}

#[derive(Parser, Debug)]
pub struct ExplainArgs {
    /// SQL query to explain (mutually exclusive with --file)
    #[arg(conflicts_with = "file")]
    pub sql: Option<String>,

    /// Read SQL from a file instead of an inline argument
    #[arg(long, conflicts_with = "sql")]
    pub file: Option<PathBuf>,

    /// ClickHouse host
    #[arg(long, default_value = "localhost")]
    pub host: String,

    /// ClickHouse HTTP port
    #[arg(long, default_value_t = 8123)]
    pub port: u16,

    /// ClickHouse user
    #[arg(long, default_value = "default")]
    pub user: String,

    /// ClickHouse password
    #[arg(long, default_value = "")]
    pub password: String,

    /// ClickHouse database
    #[arg(long, default_value = "default")]
    pub database: String,
}
