mod cli;
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
        input
            .file_stem()
            .expect("input file has no stem")
            .to_string_lossy()
            .to_string()
    })
}

fn read_file(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("Error reading {:?}: {}", path, e);
        std::process::exit(1);
    })
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
            let content = read_file(&args.input);
            let schema = inference::infer_schema(&content, &table_name).unwrap_or_else(|e| {
                eprintln!("Error inferring schema: {}", e);
                std::process::exit(1);
            });
            print_schema_summary(&schema);
            let generator = Generator::new(&schema, args.cluster, args.kafka);
            write_migrations(
                generator.generate_up(),
                generator.generate_down(),
                &table_name,
                &args.output_dir,
            );
        }

        cli::Commands::Scan(args) => {
            let table_name = table_name_from(args.name, &args.input);
            let content = read_file(&args.input);
            let schema = inference::infer_schema(&content, &table_name).unwrap_or_else(|e| {
                eprintln!("Error inferring schema: {}", e);
                std::process::exit(1);
            });
            let replicated = args.cluster.is_some();
            let result = scanner::scan(&schema, replicated);
            let source = args.input.display().to_string();
            scanner::print_scan(&result, &source, inference::record_count(&content));
        }

        cli::Commands::Table(args) => {
            let table_name = table_name_from(args.name, &args.input);
            let content = read_file(&args.input);
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
            let generator = TableGenerator::new(&schema, engine_config, args.cluster);
            write_migrations(
                generator.generate_up(),
                generator.generate_down(),
                &table_name,
                &args.output_dir,
            );
        }
    }
}
