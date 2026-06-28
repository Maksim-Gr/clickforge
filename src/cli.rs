use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "clickforge",
    about = "Generate ClickHouse migrations from a JSON file",
    long_about = "Generate ClickHouse migrations from a JSON file.\n\nSubcommands:\n  scan     Analyze fields and pick an engine\n  table    Generate a CREATE TABLE migration\n  kafka    Generate a full Kafka→ClickHouse pipeline\n\nTip: start with `clickforge scan <file.ndjson>` if unsure which engine to use.",
    version = env!("CARGO_PKG_VERSION")
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
    /// Generate ALTER TABLE migrations from the diff between two JSON samples
    Diff(DiffArgs),
}

#[derive(Parser, Debug)]
pub struct KafkaArgs {
    /// Path to a JSON array or NDJSON file (or `-` for stdin)
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
    /// Print migrations to stdout instead of writing files
    #[arg(long)]
    pub stdout: bool,
}

#[derive(Parser, Debug)]
pub struct DiffArgs {
    /// Path to the existing/old JSON sample (or `-` for stdin)
    pub old: PathBuf,
    /// Path to the new JSON sample (or `-` for stdin)
    pub new: PathBuf,
    /// Override table name (defaults to the new file's stem)
    #[arg(short, long)]
    pub name: Option<String>,
    /// ClickHouse cluster name; adds `ON CLUSTER`
    #[arg(short, long)]
    pub cluster: Option<String>,
    /// Output directory for migration files
    #[arg(short, long, default_value = ".")]
    pub output_dir: PathBuf,
    /// Print migrations to stdout instead of writing files
    #[arg(long)]
    pub stdout: bool,
}

#[derive(Parser, Debug)]
pub struct ScanArgs {
    /// Path to a JSON array or NDJSON file (or `-` for stdin)
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
    /// Path to a JSON array or NDJSON file (or `-` for stdin)
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
    /// Print migrations to stdout instead of writing files
    #[arg(long)]
    pub stdout: bool,
}
