use clap::builder::styling::{AnsiColor, Styles};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Green.on_default().bold())
    .usage(AnsiColor::Green.on_default().bold())
    .literal(AnsiColor::Cyan.on_default().bold())
    .placeholder(AnsiColor::Cyan.on_default());

const CLI_LONG_ABOUT: &str = "\
Generate the Kafka→ClickHouse ingestion pipeline from a JSON sample — the streams/raw/datalake tables and the materialized views that connect them.

Commands:
  kafka    Generate the full Kafka→ClickHouse ingestion pipeline (primary)
  scan     Helper: inspect fields and pick an engine
  table    Helper: generate a single CREATE TABLE migration
  diff     Helper: generate ALTER TABLE migrations as the schema evolves
  contract Create a schema contract, check samples against it, apply or diff changes

Tip: run `clickforge kafka <file>` to generate the whole pipeline.";

const CLI_AFTER_HELP: &str = "\
EXAMPLES:
  clickforge kafka video_events.json          Generate the full Kafka→ClickHouse ingestion pipeline
  clickforge scan video_events.json           Helper: inspect fields, suggest engines, then pick one to generate
  clickforge table video_events.json          Helper: generate a single CREATE TABLE migration
  clickforge diff old.json new.json -n events Helper: generate an additive ALTER TABLE migration
  clickforge contract init events.json --target kafka
                                              Create a schema contract and its first migration

New here? Run `clickforge kafka <file>` to generate the pipeline.";

#[derive(Parser, Debug)]
#[command(
    name = "clickforge",
    about = "Generate a Kafka→ClickHouse ingestion pipeline (and helper migrations) from a JSON sample",
    long_about = CLI_LONG_ABOUT,
    version = env!("CARGO_PKG_VERSION"),
    after_help = CLI_AFTER_HELP,
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
    /// Create a schema contract, check a JSON sample against it, or migrate it to a new version
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
    /// Write the migration from a deployed contract to a hand-edited version of it
    Diff(ContractDiffArgs),
}

#[derive(ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContractTarget {
    /// The full Kafka→ClickHouse pipeline
    Kafka,
    /// A single table
    Table,
}

const CONTRACT_INIT_AFTER_HELP: &str = "\
EXAMPLES:
  clickforge contract init video_events.json --target kafka             Contract for the Kafka pipeline, plus video_events_v1_up/down.sql
  clickforge contract init video_events.json --target table -c my_cluster
                                                                          Contract for a single table

Writes {name}.contract.json; commit it and edit it by hand where inference guessed wrong.";

#[derive(Parser, Debug)]
#[command(
    after_help = CONTRACT_INIT_AFTER_HELP
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

const CONTRACT_CHECK_AFTER_HELP: &str = "\
EXAMPLES:
  clickforge contract check video_events.contract.json new_events.json
  cat new_events.ndjson | clickforge contract check video_events.contract.json -

EXIT CODES:
  0  no changes, or compatible changes only
  1  error
  2  changes that need attention
  3  breaking changes";

#[derive(Parser, Debug)]
#[command(
    after_help = CONTRACT_CHECK_AFTER_HELP
)]
pub struct ContractCheckArgs {
    /// Path to the schema contract
    pub contract: PathBuf,
    /// Path to a JSON array or NDJSON file (or `-` for stdin)
    pub sample: PathBuf,
}

const KAFKA_AFTER_HELP: &str = "\
EXAMPLES:
  clickforge kafka video_events.json                              Generate the pipeline, writing migration files
  clickforge kafka video_events.json -n my_table -c my_cluster -k my_kafka -o migrations/
                                                                   Override table/cluster/kafka names and output dir
  clickforge kafka video_events.json --stdout                     Print migrations instead of writing files
  cat events.ndjson | clickforge kafka - -n events                Read NDJSON from stdin";

#[derive(Parser, Debug)]
#[command(
    after_help = KAFKA_AFTER_HELP
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

const DIFF_AFTER_HELP: &str = "\
EXAMPLES:
  clickforge diff video_events.json video_events_v2.json -n video_events  Generate an additive ALTER TABLE migration
  clickforge diff old.json new.json -c my_cluster                        Add ON CLUSTER to the ALTER statements
  clickforge diff old.json new.json --stdout                             Print migrations instead of writing files";

#[derive(Parser, Debug)]
#[command(
    after_help = DIFF_AFTER_HELP
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

const SCAN_AFTER_HELP: &str = "\
EXAMPLES:
  clickforge scan video_events.json               Inspect fields and suggest engines
  clickforge scan video_events.json -c my_cluster  Include ReplicatedMergeTree in the suggestions

In a terminal, scan ends by offering to generate a migration from a suggested engine.";

#[derive(Parser, Debug)]
#[command(
    after_help = SCAN_AFTER_HELP
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

const TABLE_AFTER_HELP: &str = "\
EXAMPLES:
  clickforge table video_events.json                                        Generate a CREATE TABLE migration, engine inferred
  clickforge table video_events.json --engine ReplicatedMergeTree -c my_cluster
                                                                             Force an engine, required for ReplicatedMergeTree
  clickforge table video_events.json --order-by 'id,created_at'             Override the inferred ORDER BY
  clickforge table video_events.json --stdout                              Print migrations instead of writing files";

#[derive(Parser, Debug)]
#[command(
    after_help = TABLE_AFTER_HELP
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

const CONTRACT_APPLY_AFTER_HELP: &str = "\
EXAMPLES:
  clickforge contract apply video_events.contract.json new_events.json                 Apply compatible changes
  clickforge contract apply video_events.contract.json new_events.json --accept amount Also apply the attention change to `amount`
  clickforge contract apply video_events.contract.json new_events.json --accept-all    Apply every attention change

Writes {name}_v{N}_up.sql/_down.sql and rewrites the contract as version N. Breaking changes are never applied.

EXIT CODES:
  0  applied, or nothing to apply
  1  error
  2  attention changes left out (not accepted)
  3  breaking changes; nothing written";

#[derive(Parser, Debug)]
#[command(
    after_help = CONTRACT_APPLY_AFTER_HELP
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

const CONTRACT_DIFF_AFTER_HELP: &str = "\
EXAMPLES:
  git show HEAD:video_events.contract.json > /tmp/old.json   The deployed version
  $EDITOR video_events.contract.json                          Edit it, and bump `version`
  clickforge contract diff /tmp/old.json video_events.contract.json

Writes {name}_v{N}_up.sql/_down.sql; never touches the contracts. Every edit is applied: this is how breaking changes are made.
Delete a field by marking it \"retired\": true; its column is kept.";

#[derive(Parser, Debug)]
#[command(
    after_help = CONTRACT_DIFF_AFTER_HELP
)]
pub struct ContractDiffArgs {
    /// Path to the deployed contract
    pub old: PathBuf,
    /// Path to the hand-edited contract, with `version` one higher
    pub new: PathBuf,
    /// Output directory for migration files (defaults to the new contract's directory)
    #[arg(short, long)]
    pub output_dir: Option<PathBuf>,
}
