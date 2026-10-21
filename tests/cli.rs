use std::io::Write;
use std::process::{Command, Stdio};

fn clickforge() -> Command {
    Command::new(env!("CARGO_BIN_EXE_clickforge"))
}

fn fixture(name: &str) -> String {
    format!("{}/tests/fixtures/{}", env!("CARGO_MANIFEST_DIR"), name)
}

#[test]
fn kafka_subcommand_generates_pipeline_sql() {
    let output = clickforge()
        .args(["kafka", &fixture("sample.json"), "--stdout"])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("CREATE TABLE"));
    assert!(stdout.contains("MATERIALIZED VIEW"));
}

#[test]
fn scan_subcommand_prints_field_analysis() {
    let output = clickforge()
        .args(["scan", &fixture("sample.json")])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Suggested engines"));
}

#[test]
fn table_subcommand_generates_create_table() {
    let output = clickforge()
        .args(["table", &fixture("sample.json"), "--stdout"])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("CREATE TABLE"));
}

#[test]
fn diff_subcommand_generates_alter() {
    let output = clickforge()
        .args([
            "diff",
            &fixture("old.json"),
            &fixture("new.json"),
            "--stdout",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ALTER TABLE"));
}

#[test]
fn stdin_input_is_read_via_dash() {
    let content = std::fs::read_to_string(fixture("sample.json")).unwrap();
    let mut child = clickforge()
        .args(["table", "-", "--name", "t", "--stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn clickforge");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(content.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("CREATE TABLE"));
}

#[test]
fn diff_rejects_both_inputs_as_stdin() {
    let output = clickforge()
        .args(["diff", "-", "-", "--stdout"])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("only one of"));
}

#[test]
fn table_name_derived_from_file_stem() {
    let output = clickforge()
        .args(["table", &fixture("sample.json"), "--stdout"])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("sample"));
}

#[test]
fn table_name_override_via_flag() {
    let output = clickforge()
        .args([
            "table",
            &fixture("sample.json"),
            "--name",
            "custom",
            "--stdout",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("custom"));
    assert!(!stdout.contains("sample"));
}

#[test]
fn table_subcommand_without_stdout_writes_migration_files() {
    let dir = std::env::temp_dir().join(format!("clickforge_test_write_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let output = clickforge()
        .args([
            "table",
            &fixture("sample.json"),
            "--name",
            "written",
            "--output-dir",
            dir.to_str().unwrap(),
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());

    let up = std::fs::read_to_string(dir.join("written_up.sql")).unwrap();
    let down = std::fs::read_to_string(dir.join("written_down.sql")).unwrap();
    assert!(up.contains("CREATE TABLE"));
    assert!(down.contains("DROP TABLE"));

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn malformed_json_input_reports_schema_error() {
    let output = clickforge()
        .args(["table", &fixture("malformed.json"), "--stdout"])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Error: inferring schema"));
}

#[test]
fn empty_array_input_reports_schema_error() {
    let output = clickforge()
        .args(["table", &fixture("empty.json"), "--stdout"])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("empty or contains no JSON objects"));
}

#[test]
fn nonexistent_input_file_reports_read_error() {
    let output = clickforge()
        .args(["table", "does-not-exist.json", "--stdout"])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Error: reading"));
}

#[test]
fn invalid_engine_flag_reports_error() {
    let output = clickforge()
        .args([
            "table",
            &fixture("sample.json"),
            "--engine",
            "NotAnEngine",
            "--stdout",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown engine"));
}

#[test]
fn top_level_help_flag_prints_usage() {
    let output = clickforge()
        .args(["--help"])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Usage"));
}

#[test]
fn subcommand_help_flag_prints_subcommand_usage() {
    let output = clickforge()
        .args(["table", "--help"])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--engine"));
}

#[test]
fn diff_with_no_changes_reports_no_changes_detected() {
    let output = clickforge()
        .args([
            "diff",
            &fixture("old.json"),
            &fixture("old.json"),
            "--stdout",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stderr.contains("No changes detected."));
    assert!(!stdout.contains("ALTER TABLE"));
}

#[test]
fn table_engine_flag_selects_replicated_merge_tree() {
    let output = clickforge()
        .args([
            "table",
            &fixture("sample.json"),
            "--engine",
            "ReplicatedMergeTree",
            "-c",
            "ck",
            "--stdout",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ReplicatedMergeTree('/clickhouse/"));
}

#[test]
fn table_replicated_engine_without_cluster_reports_error() {
    let output = clickforge()
        .args([
            "table",
            &fixture("sample.json"),
            "--engine",
            "ReplicatedMergeTree",
            "--stdout",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("requires a cluster"));
}

#[test]
fn table_cluster_flag_adds_on_cluster_clause() {
    let output = clickforge()
        .args([
            "table",
            &fixture("sample.json"),
            "--cluster",
            "ck",
            "--stdout",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ON CLUSTER ck"));
    assert!(stdout.contains("ReplicatedMergeTree('/clickhouse/"));
}

#[test]
fn table_order_by_flag_overrides_suggested_order() {
    let output = clickforge()
        .args([
            "table",
            &fixture("sample.json"),
            "--engine",
            "MergeTree",
            "--order-by",
            "user_id",
            "--stdout",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ORDER BY (`user_id`)"));
}

#[test]
fn table_order_by_flag_applies_without_engine_flag() {
    let output = clickforge()
        .args([
            "table",
            &fixture("sample.json"),
            "--order-by",
            "user_id",
            "--stdout",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ORDER BY (`user_id`)"));
}
