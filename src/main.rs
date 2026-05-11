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

struct IndexBlock {
    name: String,
    condition: Option<String>,
    parts_read: Option<u64>,
    parts_total: Option<u64>,
    granules_read: Option<u64>,
    granules_total: Option<u64>,
}

fn parse_index_summary(output: &str) {
    let mut blocks: Vec<IndexBlock> = Vec::new();
    let mut in_indexes = false;
    let mut current: Option<IndexBlock> = None;
    let index_types = ["PrimaryKey", "MinMax"];

    for line in output.lines() {
        let trimmed = line.trim();

        if trimmed == "Indexes:" {
            in_indexes = true;
            continue;
        }
        if !in_indexes {
            continue;
        }

        if index_types.contains(&trimmed) || (trimmed.starts_with("Skip") && !trimmed.contains(':'))
        {
            if let Some(block) = current.take() {
                blocks.push(block);
            }
            current = Some(IndexBlock {
                name: trimmed.to_string(),
                condition: None,
                parts_read: None,
                parts_total: None,
                granules_read: None,
                granules_total: None,
            });
            continue;
        }

        if let Some(ref mut block) = current {
            if let Some(rest) = trimmed.strip_prefix("Condition: ") {
                block.condition = Some(rest.to_string());
            } else if let Some(rest) = trimmed.strip_prefix("Parts: ") {
                if let Some((a, b)) = rest.split_once('/') {
                    block.parts_read = a.trim().parse().ok();
                    block.parts_total = b.trim().parse().ok();
                }
            } else if let Some(rest) = trimmed.strip_prefix("Granules: ") {
                if let Some((a, b)) = rest.split_once('/') {
                    block.granules_read = a.trim().parse().ok();
                    block.granules_total = b.trim().parse().ok();
                }
            }
        }
    }
    if let Some(block) = current {
        blocks.push(block);
    }

    if blocks.is_empty() {
        println!("No index statistics found in EXPLAIN output.");
        return;
    }

    println!("\n--- Index Analysis ---");
    for block in &blocks {
        println!("{}", block.name);
        if let Some(ref cond) = block.condition {
            println!("  Condition : {}", cond);
        }
        if let (Some(r), Some(t)) = (block.parts_read, block.parts_total) {
            let pct = if t > 0 {
                r as f64 / t as f64 * 100.0
            } else {
                0.0
            };
            println!("  Parts     : {} / {}  ({:.1}%)", r, t, pct);
        }
        if let (Some(r), Some(t)) = (block.granules_read, block.granules_total) {
            let pct = if t > 0 {
                r as f64 / t as f64 * 100.0
            } else {
                0.0
            };
            let verdict = if pct < 10.0 {
                "index effective"
            } else if pct <= 50.0 {
                "partial scan"
            } else {
                "full scan"
            };
            println!("  Granules  : {} / {}  ({:.1}%)", r, t, pct);
            println!("  Verdict   : {}", verdict);
        }
    }
}

fn run_explain(args: cli::ExplainArgs) {
    let query = if let Some(sql) = args.sql {
        sql
    } else if let Some(path) = args.file {
        read_file(&path)
    } else {
        eprintln!("Error: provide a SQL query as an argument or via --file");
        std::process::exit(1);
    };

    let explain_query = format!("EXPLAIN indexes = 1 {}", query.trim());
    let url = format!("http://{}:{}/", args.host, args.port);

    let result = ureq::post(&url)
        .set("X-ClickHouse-User", &args.user)
        .set("X-ClickHouse-Key", &args.password)
        .set("X-ClickHouse-Database", &args.database)
        .send_string(&explain_query);

    match result {
        Ok(response) => {
            let body = response.into_string().unwrap_or_else(|e| {
                eprintln!("Error reading response: {}", e);
                std::process::exit(1);
            });
            print!("{}", body);
            parse_index_summary(&body);
        }
        Err(ureq::Error::Status(code, response)) => {
            let body = response.into_string().unwrap_or_default();
            eprintln!("ClickHouse error (HTTP {}): {}", code, body.trim());
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Connection error: {}", e);
            std::process::exit(1);
        }
    }
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

        cli::Commands::Explain(args) => run_explain(args),

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
