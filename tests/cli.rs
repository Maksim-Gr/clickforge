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
