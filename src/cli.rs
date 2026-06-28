use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "clickforge",
    about = "Generate a Kafka→ClickHouse ingestion pipeline (and helper migrations) from a JSON sample",
    long_about = "Generate the Kafka→ClickHouse ingestion pipeline from a JSON sample — the streams/raw/datalake tables and the materialized views that connect them.\n\nCommands:\n  kafka    Generate the full Kafka→ClickHouse ingestion pipeline (primary)\n  scan     Helper: inspect fields and pick an engine\n  table    Helper: generate a single CREATE TABLE migration\n  diff     Helper: generate ALTER TABLE migrations as the schema evolves\n\nTip: run `clickforge kafka <file>` to generate the whole pipeline.",
    version = env!("CARGO_PKG_VERSION"),
    after_help = "EXAMPLES:\n  clickforge kafka video_events.json          Generate the full Kafka→ClickHouse ingestion pipeline\n  clickforge scan video_events.json           Helper: inspect fields, suggest engines, then pick one to generate\n  clickforge table video_events.json          Helper: generate a single CREATE TABLE migration\n  clickforge diff old.json new.json -n events Helper: generate an additive ALTER TABLE migration\n\nNew here? Run `clickforge kafka <file>` to generate the pipeline."
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
#[command(
    after_help = "EXAMPLES:\n  clickforge kafka video_events.json\n  clickforge kafka video_events.json -n my_table -c my_cluster -k my_kafka -o migrations/"
)]
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
#[command(
    after_help = "EXAMPLES:\n  clickforge diff video_events.json video_events_v2.json -n video_events"
)]
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
#[command(
    after_help = "EXAMPLES:\n  clickforge scan video_events.json\n  clickforge scan video_events.json -c my_cluster\n\nIn a terminal, scan ends by offering to generate a migration from a suggested engine."
)]
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
#[command(
    after_help = "EXAMPLES:\n  clickforge table video_events.json\n  clickforge table video_events.json --engine ReplicatedMergeTree -c my_cluster"
)]
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
