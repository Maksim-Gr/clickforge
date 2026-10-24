use clap::builder::styling::{AnsiColor, Styles};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Green.on_default().bold())
    .usage(AnsiColor::Green.on_default().bold())
    .literal(AnsiColor::Cyan.on_default().bold())
    .placeholder(AnsiColor::Cyan.on_default());

#[derive(Parser, Debug)]
#[command(
    name = "clickforge",
    about = "Generate a Kafka→ClickHouse ingestion pipeline (and helper migrations) from a JSON sample",
    long_about = "Generate the Kafka→ClickHouse ingestion pipeline from a JSON sample — the streams/raw/datalake tables and the materialized views that connect them.\n\nCommands:\n  kafka    Generate the full Kafka→ClickHouse ingestion pipeline (primary)\n  scan     Helper: inspect fields and pick an engine\n  table    Helper: generate a single CREATE TABLE migration\n  diff     Helper: generate ALTER TABLE migrations as the schema evolves\n  contract Create a schema contract, check samples against it, apply changes\n\nTip: run `clickforge kafka <file>` to generate the whole pipeline.",
    version = env!("CARGO_PKG_VERSION"),
    after_help = "EXAMPLES:\n  clickforge kafka video_events.json          Generate the full Kafka→ClickHouse ingestion pipeline\n  clickforge scan video_events.json           Helper: inspect fields, suggest engines, then pick one to generate\n  clickforge table video_events.json          Helper: generate a single CREATE TABLE migration\n  clickforge diff old.json new.json -n events Helper: generate an additive ALTER TABLE migration\n  clickforge contract init events.json --target kafka\n                                              Create a schema contract and its first migration\n\nNew here? Run `clickforge kafka <file>` to generate the pipeline.",
    styles = STYLES
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
    /// Create a schema contract, check a JSON sample against it, or apply the changes
    #[command(subcommand)]
    Contract(ContractCommands),
}

#[derive(Subcommand, Debug)]
pub enum ContractCommands {
    /// Create a schema contract and its first (v1) migration from a JSON sample
    Init(ContractInitArgs),
    /// Classify how a JSON sample differs from a schema contract; writes nothing
    Check(ContractCheckArgs),
    /// Write the next contract version and the migration to it from a JSON sample
    Apply(ContractApplyArgs),
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContractTarget {
    /// The full Kafka→ClickHouse pipeline
    Kafka,
    /// A single table
    Table,
}

#[derive(Parser, Debug)]
#[command(
    after_help = "EXAMPLES:\n  clickforge contract init video_events.json --target kafka             Contract for the Kafka pipeline, plus video_events_v1_up/down.sql\n  clickforge contract init video_events.json --target table -c my_cluster\n                                                                          Contract for a single table\n\nWrites {name}.contract.json; commit it and edit it by hand where inference guessed wrong."
)]
pub struct ContractInitArgs {
    /// Path to a JSON array or NDJSON file (or `-` for stdin)
    pub input: PathBuf,
    /// What the contract deploys
    #[arg(long, value_enum)]
    pub target: ContractTarget,
    /// Override table name (defaults to input file stem)
    #[arg(short, long)]
    pub name: Option<String>,
    /// ClickHouse cluster name (defaults to `clickhouse_datalake` for the kafka target)
    #[arg(short, long)]
    pub cluster: Option<String>,
    /// Kafka collection name (kafka target only; defaults to `kafka`)
    #[arg(short, long)]
    pub kafka: Option<String>,
    /// Table engine (table target only); inferred if omitted
    #[arg(short, long)]
    pub engine: Option<String>,
    /// Comma-separated ORDER BY fields (table target only); inferred if omitted
    #[arg(long)]
    pub order_by: Option<String>,
    /// Output directory for the contract and migration files
    #[arg(short, long, default_value = ".")]
    pub output_dir: PathBuf,
}

#[derive(Parser, Debug)]
#[command(
    after_help = "EXAMPLES:\n  clickforge contract check video_events.contract.json new_events.json\n  cat new_events.ndjson | clickforge contract check video_events.contract.json -\n\nEXIT CODES:\n  0  no changes, or compatible changes only\n  1  error\n  2  changes that need attention\n  3  breaking changes"
)]
pub struct ContractCheckArgs {
    /// Path to the schema contract
    pub contract: PathBuf,
    /// Path to a JSON array or NDJSON file (or `-` for stdin)
    pub sample: PathBuf,
}

#[derive(Parser, Debug)]
#[command(
    after_help = "EXAMPLES:\n  clickforge kafka video_events.json                              Generate the pipeline, writing migration files\n  clickforge kafka video_events.json -n my_table -c my_cluster -k my_kafka -o migrations/\n                                                                   Override table/cluster/kafka names and output dir\n  clickforge kafka video_events.json --stdout                     Print migrations instead of writing files\n  cat events.ndjson | clickforge kafka - -n events                Read NDJSON from stdin"
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
    after_help = "EXAMPLES:\n  clickforge diff video_events.json video_events_v2.json -n video_events  Generate an additive ALTER TABLE migration\n  clickforge diff old.json new.json -c my_cluster                        Add ON CLUSTER to the ALTER statements\n  clickforge diff old.json new.json --stdout                             Print migrations instead of writing files"
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
    after_help = "EXAMPLES:\n  clickforge scan video_events.json               Inspect fields and suggest engines\n  clickforge scan video_events.json -c my_cluster  Include ReplicatedMergeTree in the suggestions\n\nIn a terminal, scan ends by offering to generate a migration from a suggested engine."
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
    after_help = "EXAMPLES:\n  clickforge table video_events.json                                        Generate a CREATE TABLE migration, engine inferred\n  clickforge table video_events.json --engine ReplicatedMergeTree -c my_cluster\n                                                                             Force an engine, required for ReplicatedMergeTree\n  clickforge table video_events.json --order-by 'id,created_at'             Override the inferred ORDER BY\n  clickforge table video_events.json --stdout                              Print migrations instead of writing files"
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
    /// ClickHouse cluster name (required for ReplicatedMergeTree, which becomes the default engine)
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
    after_help = "EXAMPLES:\n  clickforge contract apply video_events.contract.json new_events.json                 Apply compatible changes\n  clickforge contract apply video_events.contract.json new_events.json --accept amount Also apply the attention change to `amount`\n  clickforge contract apply video_events.contract.json new_events.json --accept-all    Apply every attention change\n\nWrites {name}_v{N}_up.sql/_down.sql and rewrites the contract as version N. Breaking changes are never applied.\n\nEXIT CODES:\n  0  applied, or nothing to apply\n  1  error\n  2  attention changes left out (not accepted)\n  3  breaking changes; nothing written"
)]
pub struct ContractApplyArgs {
    /// Path to the schema contract (rewritten in place)
    pub contract: PathBuf,
    /// Path to a JSON array or NDJSON file (or `-` for stdin)
    pub sample: PathBuf,
    /// Apply the attention change to these fields (comma-separated or repeated)
    #[arg(long, value_delimiter = ',', conflicts_with = "accept_all")]
    pub accept: Vec<String>,
    /// Apply every attention change
    #[arg(long)]
    pub accept_all: bool,
    /// Output directory for migration files (defaults to the contract's directory)
    #[arg(short, long)]
    pub output_dir: Option<PathBuf>,
}
