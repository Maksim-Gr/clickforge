//! Opt-in SQL execution tests: cargo test --test clickhouse -- --ignored
//! Requires Docker and the clickhouse/clickhouse-server:26.3 image locally.
use serde_json::{Value, json};
use std::io::Write;
use std::process::{Command, Stdio};

fn run_with_input(command: &mut Command, input: &str) -> String {
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(input.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn pipeline_rows(sample: &Value, messages: &[Value], table: &str) -> Vec<Value> {
    let generated = run_with_input(
        Command::new(env!("CARGO_BIN_EXE_clickforge"))
            .args(["kafka", "-", "--name", table, "--stdout"]),
        &sample.to_string(),
    );
    let mut sql = String::from(
        "CREATE DATABASE raw; CREATE DATABASE datalake; SET session_timezone = 'UTC';\n",
    );
    for statement in generated.split(';').map(str::trim) {
        if statement.starts_with("CREATE TABLE IF NOT EXISTS `raw`.")
            || statement.starts_with("CREATE TABLE IF NOT EXISTS `datalake`.")
        {
            // Keep generated columns; local execution needs neither Keeper nor a cluster.
            sql.push_str(
                statement
                    .split("\n\tENGINE =")
                    .next()
                    .unwrap()
                    .replace(" ON CLUSTER clickhouse_datalake", "")
                    .as_str(),
            );
            sql.push_str(" ENGINE = Memory;\n");
        } else if statement.starts_with("CREATE MATERIALIZED VIEW IF NOT EXISTS `raw`.") {
            sql.push_str(&statement.replace("ON CLUSTER clickhouse_datalake ", ""));
            sql.push_str(";\n");
        }
    }
    let table = format!("`{}`", table.replace('\\', "\\\\").replace('`', "``"));
    for message in messages {
        // Hex encoding keeps the test input independent of SQL escaping under test.
        let hex: String = message
            .to_string()
            .bytes()
            .map(|b| format!("{b:02x}"))
            .collect();
        sql.push_str(&format!(
            "INSERT INTO raw.{table} (message) SELECT unhex('{hex}');\n"
        ));
    }
    sql.push_str(&format!(
        "SELECT * FROM datalake.{table} FORMAT JSONEachRow;"
    ));
    run_with_input(
        Command::new("docker").args([
            "run",
            "--rm",
            "-i",
            "--network",
            "none",
            "--entrypoint",
            "clickhouse",
            "clickhouse/clickhouse-server:26.3",
            "local",
            "--multiquery",
        ]),
        &sql,
    )
    .lines()
    .map(|line| serde_json::from_str(line).unwrap())
    .collect()
}

#[test]
#[ignore = "requires Docker and a local ClickHouse image"]
fn iso_timestamps_survive_materialized_view_ingestion() {
    let timestamp = "2024-03-01T14:34:56.123+02:00";
    let first = json!({"required": timestamp, "optional": timestamp,
        "times": [[timestamp]], "by_name": {"start": timestamp}});
    let second = json!({"required": "2024-03-01T12:34:56.123Z", "optional": null});
    let missing = json!({"required": timestamp});
    let mut invalid = first.clone();
    invalid["optional"] = json!("invalid");
    let mut rows = pipeline_rows(
        &json!([first, second, missing]),
        &[first.clone(), second, missing, invalid],
        "events",
    );
    assert_eq!(rows.len(), 4);
    // ClickHouse may return rows in any order without ORDER BY.
    rows.sort_by_key(|row| row["optional"].is_null());
    for row in &rows {
        assert_eq!(row["required"], "2024-03-01 12:34:56.123");
    }
    assert_eq!(rows[0]["optional"], "2024-03-01 12:34:56.123");
    assert_eq!(rows[0]["times"], json!([["2024-03-01 12:34:56.123"]]));
    assert_eq!(
        rows[0]["by_name"],
        json!({"start": "2024-03-01 12:34:56.123"})
    );
    for row in &rows[1..] {
        assert_eq!(row["optional"], Value::Null);
    }
}

#[test]
#[ignore = "requires Docker and a local ClickHouse image"]
fn message_alias_does_not_shadow_raw_json() {
    let message = json!({"message": "hello", "other": 42, "source": "also a field"});
    let rows = pipeline_rows(&message, std::slice::from_ref(&message), "events");
    assert_eq!(rows[0]["message"], "hello");
    assert_eq!(rows[0]["other"], 42);
    assert_eq!(rows[0]["source"], "also a field");
}

#[test]
#[ignore = "requires Docker and a local ClickHouse image"]
fn backslashes_in_field_and_table_names_round_trip() {
    let message = json!({"a\\b": "control escape", "trailing\\": "end",
        "slash\\'quote": "single quote", "slash\\`tick": "backtick"});
    let rows = pipeline_rows(&message, std::slice::from_ref(&message), "table\\'`name\\");
    for (key, value) in message.as_object().unwrap() {
        assert_eq!(&rows[0][key], value, "field {key:?}");
    }
}
