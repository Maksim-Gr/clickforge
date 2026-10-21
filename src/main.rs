mod cli;
mod diff;
mod generator;
mod inference;
mod sample;
mod scanner;
mod schema;

use clap::Parser;
use generator::{Generator, TableGenerator};
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

/// Prints both migrations to stdout, separated by `-- up` / `-- down` headers.
fn print_migrations(up: &str, down: &str) {
    println!("-- up\n{}\n\n-- down\n{}", up, down);
}

/// Either prints migrations to stdout or writes them to files.
fn emit_migrations(up: String, down: String, table_name: &str, output_dir: &Path, stdout: bool) {
    if stdout {
        print_migrations(&up, &down);
    } else {
        write_migrations(up, down, table_name, output_dir);
    }
}

/// Builds a CREATE/DROP TABLE migration from a chosen engine config and emits it.
fn generate_table_migration(
    schema: &schema::InferredSchema,
    engine_config: schema::EngineConfig,
    cluster: Option<String>,
    table_name: &str,
    output_dir: &Path,
    stdout: bool,
) {
    let generator = TableGenerator::new(schema, engine_config, cluster);
    emit_migrations(
        generator.generate_up(),
        generator.generate_down(),
        table_name,
        output_dir,
        stdout,
    );
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
    generate_table_migration(
        schema,
        engine_config,
        cluster,
        &schema.table_name,
        Path::new("."),
        false,
    );
}

fn write_migrations(up: String, down: String, table_name: &str, output_dir: &Path) {
    let up_path = output_dir.join(format!("{}_up.sql", table_name));
    let down_path = output_dir.join(format!("{}_down.sql", table_name));

    fs::write(&up_path, up).unwrap_or_else(|e| die(format!("writing {:?}: {}", up_path, e)));
    fs::write(&down_path, down).unwrap_or_else(|e| die(format!("writing {:?}: {}", down_path, e)));

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
            let schema = sample.schema;
            let table_name = &schema.table_name;
            print_schema_summary(&schema);
            let generator = Generator::new(&schema, args.cluster, args.kafka);
            emit_migrations(
                generator.generate_up(),
                generator.generate_down(),
                table_name,
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
            generate_table_migration(
                &schema,
                engine_config,
                args.cluster,
                &schema.table_name,
                &args.output_dir,
                args.stdout,
            );
        }

        cli::Commands::Diff(args) => {
            let (old, new) = sample::load_pair(&args.old, &args.new, args.name, std::io::stdin())
                .unwrap_or_else(|e| die(e));
            let table_name = &new.schema.table_name;

            let result = diff::diff_schemas(
                &old.schema,
                &new.schema,
                table_name,
                args.cluster.as_deref(),
            );
            for w in &result.warnings {
                let warning = "Warning:"
                    .if_supports_color(Stderr, |t| t.style(Style::new().yellow().bold()))
                    .to_string();
                eprintln!("{} {}", warning, w);
            }
            if result.up.is_empty() {
                if result.warnings.is_empty() {
                    eprintln!("No changes detected.");
                }
            } else {
                emit_migrations(
                    result.up,
                    result.down,
                    &format!("{}_alter", table_name),
                    &args.output_dir,
                    args.stdout,
                );
            }
        }
    }
}
