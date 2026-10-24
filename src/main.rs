mod cli;
mod contract;
mod diff;
mod evolve;
mod generator;
mod inference;
mod sample;
mod scanner;
mod schema;

use clap::Parser;
use owo_colors::{OwoColorize, Stream::Stderr, Stream::Stdout, Style};
use std::fs;
use std::path::Path;

/// Prints `Error: {msg}` to stderr and exits the process with status 1.
fn die(msg: impl std::fmt::Display) -> ! {
    let error = "Error:"
        .if_supports_color(Stderr, |t| t.style(Style::new().red().bold()))
        .to_string();
    eprintln!("{} {}", error, msg);
    std::process::exit(1);
}

fn print_schema_summary(schema: &schema::InferredSchema) {
    let max_len = schema
        .columns
        .iter()
        .map(|c| c.name.len())
        .max()
        .unwrap_or(0);
    eprintln!(
        "{} columns inferred from {}\n",
        schema.columns.len(),
        schema.table_name.if_supports_color(Stderr, |t| t.bold())
    );
    for col in &schema.columns {
        let name = format!("{:<width$}", col.name, width = max_len);
        eprintln!(
            "  {}  {}",
            name.if_supports_color(Stderr, |t| t.bold()),
            col.ch_type.as_str().if_supports_color(Stderr, |t| t.cyan())
        );
    }
    eprintln!();
}

/// Loads a JSON sample from `input` (stdin when `-`), or prints an error and exits(1).
fn load_sample(input: &Path, name: Option<String>) -> sample::Sample {
    let sample = sample::load(input, name, std::io::stdin()).unwrap_or_else(|e| die(e));
    if sample.defaulted_name {
        let note = "Note:"
            .if_supports_color(Stderr, |t| t.style(Style::new().cyan().bold()))
            .to_string();
        eprintln!(
            "{} reading from stdin; defaulting table name to `{}` (pass --name to override).",
            note, sample.schema.table_name
        );
    }
    sample
}

/// Prints both scripts to stdout, separated by `-- up` / `-- down` headers.
fn print_migrations(migration: &schema::Migration) {
    println!("-- up\n{}\n\n-- down\n{}", migration.up, migration.down);
}

/// Either prints a migration to stdout or writes it to files.
fn emit_migrations(migration: schema::Migration, output_dir: &Path, stdout: bool) {
    if stdout {
        print_migrations(&migration);
    } else {
        write_migrations(migration, output_dir);
    }
}

/// After `scan`, offer to generate a migration from one of the suggestions so the
/// user doesn't have to re-type a `table` command. Only prompts in an interactive
/// terminal reading a real file; piped/scripted runs are left untouched.
fn maybe_generate_interactively(
    result: &scanner::ScanResult,
    sample: &sample::Sample,
    cluster: Option<String>,
) {
    use std::io::{IsTerminal, Write};

    if result.suggestions.is_empty() || sample.from_stdin || !std::io::stdin().is_terminal() {
        return;
    }

    let n = result.suggestions.len();
    let prompt = format!("\nPick an engine to generate [1-{}, Enter to skip]: ", n);
    print!("{}", prompt.if_supports_color(Stdout, |t| t.bold()));
    let _ = std::io::stdout().flush();

    let mut line = String::new();
    if std::io::stdin().read_line(&mut line).is_err() {
        return;
    }
    let choice = match line.trim().parse::<usize>() {
        Ok(k) if (1..=n).contains(&k) => k,
        _ => return, // empty / invalid / out of range → skip
    };

    let engine = result.suggestions[choice - 1].engine.clone();
    let schema = &sample.schema;
    let engine_config = scanner::select_engine(schema, Some(engine), None, cluster.as_deref())
        .unwrap_or_else(|e| die(e));
    emit_migrations(
        generator::table_migration(schema, engine_config, cluster),
        Path::new("."),
        false,
    );
}

fn write_migrations(migration: schema::Migration, output_dir: &Path) {
    let up_path = output_dir.join(format!("{}_up.sql", migration.name));
    let down_path = output_dir.join(format!("{}_down.sql", migration.name));

    fs::write(&up_path, migration.up)
        .unwrap_or_else(|e| die(format!("writing {:?}: {}", up_path, e)));
    fs::write(&down_path, migration.down)
        .unwrap_or_else(|e| die(format!("writing {:?}: {}", down_path, e)));

    let written = "Written:"
        .if_supports_color(Stderr, |t| t.style(Style::new().green().bold()))
        .to_string();
    eprintln!("{} {}", written, up_path.display());
    eprintln!("{} {}", written, down_path.display());
}

fn main() {
    let raw_args: Vec<String> = std::env::args().collect();
    if raw_args.iter().skip(1).any(|a| a == "--help" || a == "-h") {
        let mut help_args = vec![raw_args[0].clone()];
        if let Some(sub) = raw_args.iter().skip(1).find(|a| !a.starts_with('-')) {
            help_args.push(sub.clone());
        }
        help_args.push("--help".to_string());
        cli::Cli::parse_from(help_args);
    }

    let cli = cli::Cli::parse();

    match cli.command {
        cli::Commands::Kafka(args) => {
            let sample = load_sample(&args.input, args.name);
            print_schema_summary(&sample.schema);
            emit_migrations(
                generator::kafka_pipeline(&sample.schema, args.cluster, args.kafka),
                &args.output_dir,
                args.stdout,
            );
        }

        cli::Commands::Scan(args) => {
            let sample = load_sample(&args.input, args.name);
            let replicated = args.cluster.is_some();
            let result = scanner::scan(&sample.schema, replicated);
            scanner::print_scan(&result, &sample.label, sample.schema.record_count);
            maybe_generate_interactively(&result, &sample, args.cluster);
        }

        cli::Commands::Table(args) => {
            let schema = load_sample(&args.input, args.name).schema;

            let engine: Option<schema::TableEngine> =
                args.engine.map(|e| e.parse().unwrap_or_else(|e| die(e)));
            let order_by = args
                .order_by
                .map(|ob| ob.split(',').map(|s| s.trim().to_string()).collect());
            let engine_config =
                scanner::select_engine(&schema, engine, order_by, args.cluster.as_deref())
                    .unwrap_or_else(|e| die(e));

            print_schema_summary(&schema);
            emit_migrations(
                generator::table_migration(&schema, engine_config, args.cluster),
                &args.output_dir,
                args.stdout,
            );
        }

        cli::Commands::Diff(args) => {
            let (old, new) = sample::load_pair(&args.old, &args.new, args.name, std::io::stdin())
                .unwrap_or_else(|e| die(e));
            let result = diff::diff_schemas(
                &old.schema,
                &new.schema,
                &new.schema.table_name,
                args.cluster.as_deref(),
            );
            for w in &result.warnings {
                let warning = "Warning:"
                    .if_supports_color(Stderr, |t| t.style(Style::new().yellow().bold()))
                    .to_string();
                eprintln!("{} {}", warning, w);
            }
            match result.migration {
                Some(migration) => emit_migrations(migration, &args.output_dir, args.stdout),
                None if result.warnings.is_empty() => eprintln!("No changes detected."),
                None => {}
            }
        }

        cli::Commands::Contract(cli::ContractCommands::Init(args)) => contract_init(args),

        cli::Commands::Contract(cli::ContractCommands::Check(args)) => contract_check(args),

        cli::Commands::Contract(cli::ContractCommands::Apply(args)) => contract_apply(args),
    }
}

/// Writes `{name}.contract.json` and the v1 migration that deploys it.
fn contract_init(args: cli::ContractInitArgs) {
    let sample = load_sample(&args.input, args.name);
    let schema = &sample.schema;

    let contract_path = args
        .output_dir
        .join(format!("{}.contract.json", schema.table_name));
    if contract_path.exists() {
        die(format!(
            "{} already exists; edit it instead of re-initializing",
            contract_path.display()
        ));
    }

    let (contract, migration) = match args.target {
        cli::ContractTarget::Kafka => {
            if args.engine.is_some() || args.order_by.is_some() {
                die("--engine and --order-by only apply to --target table");
            }
            let cluster = args
                .cluster
                .unwrap_or_else(|| "clickhouse_datalake".to_string());
            let kafka = args.kafka.unwrap_or_else(|| "kafka".to_string());
            // The pipeline's datalake table is always this engine and key.
            let engine_config = schema::EngineConfig {
                engine: schema::TableEngine::ReplicatedMergeTree,
                order_by: vec!["_timestamp_ms".to_string()],
                sum_columns: vec![],
            };
            let migration = generator::kafka_pipeline(schema, cluster.clone(), kafka.clone());
            let contract = contract::Contract::from_schema(
                schema,
                contract::Target::Kafka,
                Some(cluster),
                Some(kafka),
                engine_config,
            );
            (contract, migration)
        }
        cli::ContractTarget::Table => {
            if args.kafka.is_some() {
                die("--kafka only applies to --target kafka");
            }
            let engine: Option<schema::TableEngine> =
                args.engine.map(|e| e.parse().unwrap_or_else(|e| die(e)));
            let order_by = args
                .order_by
                .map(|ob| ob.split(',').map(|s| s.trim().to_string()).collect());
            let engine_config =
                scanner::select_engine(schema, engine, order_by, args.cluster.as_deref())
                    .unwrap_or_else(|e| die(e));
            let migration =
                generator::table_migration(schema, engine_config.clone(), args.cluster.clone());
            let contract = contract::Contract::from_schema(
                schema,
                contract::Target::Table,
                args.cluster,
                None,
                engine_config,
            );
            (contract, migration)
        }
    };

    print_schema_summary(schema);
    fs::write(&contract_path, contract.to_json())
        .unwrap_or_else(|e| die(format!("writing {:?}: {}", contract_path, e)));
    let written = "Written:"
        .if_supports_color(Stderr, |t| t.style(Style::new().green().bold()))
        .to_string();
    eprintln!("{} {}", written, contract_path.display());
    write_migrations(
        schema::Migration {
            name: format!("{}_v{}", migration.name, contract.version),
            ..migration
        },
        &args.output_dir,
    );
}

/// Prints how a sample differs from a contract and exits with the code of the
/// worst change: 0 (none/compatible), 2 (attention) or 3 (breaking).
fn contract_check(args: cli::ContractCheckArgs) {
    let (_, changes) = report_check(&args.contract, &args.sample);
    let code = match changes.iter().map(|c| c.class).max() {
        Some(contract::ChangeClass::Breaking) => 3,
        Some(contract::ChangeClass::Attention) => 2,
        _ => 0,
    };
    std::process::exit(code);
}

/// Writes the next contract version and the migration to it, then exits 0, or 2 when
/// attention changes were left out. Refuses (exit 3) while any change is breaking.
fn contract_apply(args: cli::ContractApplyArgs) {
    let (contract, changes) = report_check(&args.contract, &args.sample);
    if changes
        .iter()
        .any(|c| c.class == contract::ChangeClass::Breaking)
    {
        eprintln!(
            "\nNothing applied: breaking changes need the producer fixed, or the contract edited by hand."
        );
        std::process::exit(3);
    }

    let accept = if args.accept_all {
        contract::Accept::All
    } else {
        contract::Accept::Fields(&args.accept)
    };
    let next = contract::next_version(&contract, &changes, accept).unwrap_or_else(|e| die(e));
    let skipped_code = if next.skipped.is_empty() { 0 } else { 2 };
    let print_skipped = || {
        if !next.skipped.is_empty() {
            let skipped = "Skipped:"
                .if_supports_color(Stderr, |t| t.style(Style::new().yellow().bold()))
                .to_string();
            eprintln!(
                "{} {}; rerun with --accept {} to apply",
                skipped,
                next.skipped.join(", "),
                next.skipped.join(",")
            );
        }
    };
    let Some(new) = &next.contract else {
        eprintln!("\nNothing to apply.");
        print_skipped();
        std::process::exit(skipped_code);
    };

    let migration = evolve::migration(&contract, new);
    let output_dir = args.output_dir.clone().unwrap_or_else(|| {
        args.contract
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_path_buf()
    });
    for script in ["up", "down"] {
        let path = output_dir.join(format!("{}_{}.sql", migration.name, script));
        if path.exists() {
            die(format!(
                "{} already exists; nothing written",
                path.display()
            ));
        }
    }

    eprintln!();
    write_migrations(migration, &output_dir);
    fs::write(&args.contract, new.to_json())
        .unwrap_or_else(|e| die(format!("writing {:?}: {}", args.contract, e)));
    let written = "Written:"
        .if_supports_color(Stderr, |t| t.style(Style::new().green().bold()))
        .to_string();
    eprintln!("{} {} (v{})", written, args.contract.display(), new.version);
    print_skipped();
    std::process::exit(skipped_code);
}

/// Reads the contract at `path`, checks the sample at `sample_path` against it and
/// prints the report, worst change first.
fn report_check(path: &Path, sample_path: &Path) -> (contract::Contract, Vec<contract::Change>) {
    let content = fs::read_to_string(path)
        .unwrap_or_else(|e| die(format!("reading {}: {}", path.display(), e)));
    let contract = contract::Contract::from_json(&content)
        .unwrap_or_else(|e| die(format!("{}: {}", path.display(), e)));
    let sample = load_sample(sample_path, Some(contract.table.clone()));

    let mut changes = contract::check(&contract, &sample.schema);
    // Worst first; the sort is stable, so contract order is kept within a class.
    changes.sort_by_key(|c| std::cmp::Reverse(c.class));

    eprintln!(
        "Checking {} against {} (v{})\n",
        sample.label.if_supports_color(Stderr, |t| t.bold()),
        path.display().if_supports_color(Stderr, |t| t.bold()),
        contract.version
    );
    if changes.is_empty() {
        eprintln!("No changes: the sample fits the contract.");
    }
    let max_len = changes.iter().map(|c| c.field.len()).max().unwrap_or(0);
    for change in &changes {
        let (label, style) = match change.class {
            contract::ChangeClass::Breaking => ("breaking", Style::new().red().bold()),
            contract::ChangeClass::Attention => ("attention", Style::new().yellow().bold()),
            contract::ChangeClass::Compatible => ("compatible", Style::new().green().bold()),
            contract::ChangeClass::Info => ("info", Style::new().cyan().bold()),
        };
        let label = format!("{:<10}", label);
        let field = format!("{:<width$}", change.field, width = max_len);
        eprintln!(
            "  {}  {}  {}",
            label.if_supports_color(Stderr, |t| t.style(style)),
            field.if_supports_color(Stderr, |t| t.bold()),
            change.message
        );
    }
    (contract, changes)
}
