mod cli;
mod diff;
mod generator;
mod inference;
mod scanner;
mod schema;

use clap::Parser;
use generator::{Generator, TableGenerator};
use std::fs;
use std::path::Path;

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
        schema.table_name
    );
    for col in &schema.columns {
        eprintln!(
            "  {:<width$}  {}",
            col.name,
            col.ch_type.as_str(),
            width = max_len
        );
    }
    eprintln!();
}

fn table_name_from(name: Option<String>, input: &Path) -> String {
    name.unwrap_or_else(|| {
        if input.as_os_str() == "-" {
            eprintln!("Note: reading from stdin; defaulting table name to `table` (pass --name to override).");
            return "table".to_string();
        }
        input
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| {
                eprintln!("Error: could not derive a table name from {:?}; pass --name", input);
                std::process::exit(1);
            })
    })
}

/// Reads the input source: stdin when `path` is `-`, otherwise the file at `path`.
fn read_input(path: &Path) -> String {
    if path.as_os_str() == "-" {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .unwrap_or_else(|e| {
                eprintln!("Error reading stdin: {}", e);
                std::process::exit(1);
            });
        buf
    } else {
        read_file(path)
    }
}

fn read_file(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("Error reading {:?}: {}", path, e);
        std::process::exit(1);
    })
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
    schema: &schema::InferredSchema,
    cluster: Option<String>,
    table_name: &str,
    input: &Path,
) {
    use std::io::{IsTerminal, Write};

    if result.suggestions.is_empty() || input.as_os_str() == "-" || !std::io::stdin().is_terminal()
    {
        return;
    }

    let n = result.suggestions.len();
    print!("\nPick an engine to generate [1-{}, Enter to skip]: ", n);
    let _ = std::io::stdout().flush();

    let mut line = String::new();
    if std::io::stdin().read_line(&mut line).is_err() {
        return;
    }
    let choice = match line.trim().parse::<usize>() {
        Ok(k) if (1..=n).contains(&k) => k,
        _ => return, // empty / invalid / out of range → skip
    };

    let engine_config = result.suggestions[choice - 1].to_engine_config();
    generate_table_migration(
        schema,
        engine_config,
        cluster,
        table_name,
        Path::new("."),
        false,
    );
}

fn write_migrations(up: String, down: String, table_name: &str, output_dir: &Path) {
    let up_path = output_dir.join(format!("{}_up.sql", table_name));
    let down_path = output_dir.join(format!("{}_down.sql", table_name));

    fs::write(&up_path, up).unwrap_or_else(|e| {
        eprintln!("Error writing {:?}: {}", up_path, e);
        std::process::exit(1);
    });
    fs::write(&down_path, down).unwrap_or_else(|e| {
        eprintln!("Error writing {:?}: {}", down_path, e);
        std::process::exit(1);
    });

    eprintln!("Written: {}", up_path.display());
    eprintln!("Written: {}", down_path.display());
}

fn main() {
    let cli = cli::Cli::parse();

    match cli.command {
        cli::Commands::Kafka(args) => {
            let table_name = table_name_from(args.name, &args.input);
            let content = read_input(&args.input);
            let schema = inference::infer_schema(&content, &table_name).unwrap_or_else(|e| {
                eprintln!("Error inferring schema: {}", e);
                std::process::exit(1);
            });
            print_schema_summary(&schema);
            let generator = Generator::new(&schema, args.cluster, args.kafka);
            emit_migrations(
                generator.generate_up(),
                generator.generate_down(),
                &table_name,
                &args.output_dir,
                args.stdout,
            );
        }

        cli::Commands::Scan(args) => {
            let table_name = table_name_from(args.name, &args.input);
            let content = read_input(&args.input);
            let schema = inference::infer_schema(&content, &table_name).unwrap_or_else(|e| {
                eprintln!("Error inferring schema: {}", e);
                std::process::exit(1);
            });
            let replicated = args.cluster.is_some();
            let result = scanner::scan(&schema, replicated);
            let source = if args.input.as_os_str() == "-" {
                "<stdin>".to_string()
            } else {
                args.input.display().to_string()
            };
            scanner::print_scan(&result, &source, inference::record_count(&content));
            maybe_generate_interactively(&result, &schema, args.cluster, &table_name, &args.input);
        }

        cli::Commands::Table(args) => {
            let table_name = table_name_from(args.name, &args.input);
            let content = read_input(&args.input);
            let schema = inference::infer_schema(&content, &table_name).unwrap_or_else(|e| {
                eprintln!("Error inferring schema: {}", e);
                std::process::exit(1);
            });

            let replicated = matches!(args.engine.as_deref(), Some("ReplicatedMergeTree"));
            let scan_result = scanner::scan(&schema, replicated);

            let engine_config = if let Some(engine_str) = args.engine {
                let engine: schema::TableEngine = engine_str.parse().unwrap_or_else(|e| {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                });
                // order_by: use --order-by flag if given, else take from scanner suggestion
                let order_by = if let Some(ob) = args.order_by {
                    ob.split(',').map(|s| s.trim().to_string()).collect()
                } else {
                    // find the suggestion with matching engine, fall back to first
                    scan_result
                        .suggestions
                        .iter()
                        .find(|s| s.engine == engine)
                        .or_else(|| scan_result.suggestions.first())
                        .map(|s| s.order_by.clone())
                        .unwrap_or_default()
                };
                let sum_columns = scan_result
                    .suggestions
                    .iter()
                    .find(|s| s.engine == schema::TableEngine::SummingMergeTree)
                    .map(|s| s.sum_columns.clone())
                    .unwrap_or_default();
                schema::EngineConfig {
                    engine,
                    order_by,
                    sum_columns,
                }
            } else {
                // no --engine: use first suggestion (MergeTree)
                scan_result
                    .suggestions
                    .into_iter()
                    .next()
                    .map(|s| s.to_engine_config())
                    .unwrap_or_else(|| schema::EngineConfig {
                        engine: schema::TableEngine::MergeTree,
                        order_by: vec![],
                        sum_columns: vec![],
                    })
            };

            print_schema_summary(&schema);
            generate_table_migration(
                &schema,
                engine_config,
                args.cluster,
                &table_name,
                &args.output_dir,
                args.stdout,
            );
        }

        cli::Commands::Diff(args) => {
            if args.old.as_os_str() == "-" && args.new.as_os_str() == "-" {
                eprintln!("Error: only one of <OLD> and <NEW> can be '-' (stdin).");
                std::process::exit(1);
            }
            let name_input = if args.new.as_os_str() != "-" {
                &args.new
            } else {
                &args.old
            };
            let table_name = table_name_from(args.name, name_input);
            let infer = |content: &str| {
                inference::infer_schema(content, &table_name).unwrap_or_else(|e| {
                    eprintln!("Error inferring schema: {}", e);
                    std::process::exit(1);
                })
            };
            let old_schema = infer(&read_input(&args.old));
            let new_schema = infer(&read_input(&args.new));

            let result = diff::diff_schemas(
                &old_schema,
                &new_schema,
                &table_name,
                args.cluster.as_deref(),
            );
            for w in &result.warnings {
                eprintln!("Warning: {}", w);
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
