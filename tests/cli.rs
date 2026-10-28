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
    // `b` is in every new record: attention (exit 2), still added as nullable.
    assert_eq!(output.status.code(), Some(2), "{:?}", output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("ADD COLUMN IF NOT EXISTS `b` Nullable(String)"),
        "{stdout}"
    );
}

/// Runs `diff` of the `old.json` fixture against `new` fed on stdin.
fn diff_against_old(new: &str) -> std::process::Output {
    let mut child = clickforge()
        .args(["diff", &fixture("old.json"), "-", "--stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run clickforge");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(new.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn diff_compatible_change_exits_0() {
    let output = diff_against_old("{\"a\": 1, \"b\": \"x\"}\n{\"a\": 2}");
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("compatible"));
}

#[test]
fn diff_breaking_change_exits_3_and_reports_it() {
    let output = diff_against_old(r#"[{"a": "one"}]"#);
    assert_eq!(output.status.code(), Some(3), "{:?}", output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("breaking") && stderr.contains("only adds columns"),
        "{stderr}"
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("ALTER"));
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
fn nested_subcommand_help_flag_prints_nested_usage() {
    let output = clickforge()
        .args(["contract", "init", "--help"])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--target"));
}

#[test]
fn help_flag_after_value_taking_flag_prints_help() {
    let output = clickforge()
        .args(["kafka", "-n", "--help"])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--kafka"));
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

/// A fresh, empty temp directory unique to `test`.
fn temp_dir(test: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("clickforge_test_{}_{}", test, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Runs `contract init` for the `sample.json` fixture into `dir`.
fn contract_init(dir: &std::path::Path, target: &str) -> std::process::Output {
    clickforge()
        .args([
            "contract",
            "init",
            &fixture("sample.json"),
            "--target",
            target,
            "-o",
            dir.to_str().unwrap(),
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge")
}

/// Runs `contract check` against `dir/sample.contract.json`, feeding `sample` on stdin.
fn contract_check(dir: &std::path::Path, sample: &str) -> std::process::Output {
    let contract = dir.join("sample.contract.json");
    let mut child = clickforge()
        .args(["contract", "check", contract.to_str().unwrap(), "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run clickforge");
    // An invalid contract exits before stdin is read, so the write may hit a broken pipe.
    let _ = child.stdin.take().unwrap().write_all(sample.as_bytes());
    child.wait_with_output().unwrap()
}

const SAMPLE_V1: &str = r#"[
  {"user_id": "u3", "event_time": "2024-03-03T10:00:00Z", "amount": 1},
  {"user_id": "u4", "event_time": "2024-03-04T10:00:00Z", "amount": 2}
]"#;

#[test]
fn contract_init_kafka_writes_contract_and_v1_migration() {
    let dir = temp_dir("contract_init_kafka");
    let output = contract_init(&dir, "kafka");
    assert!(output.status.success(), "{:?}", output);

    let contract: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(dir.join("sample.contract.json")).unwrap())
            .unwrap();
    assert_eq!(contract["version"], 1);
    assert_eq!(contract["target"], "kafka");
    assert_eq!(contract["cluster"], "clickhouse_datalake");
    assert_eq!(contract["kafka"], "kafka");
    assert_eq!(contract["fields"][2]["name"], "amount");

    let up = std::fs::read_to_string(dir.join("sample_v1_up.sql")).unwrap();
    let down = std::fs::read_to_string(dir.join("sample_v1_down.sql")).unwrap();
    assert!(up.contains("MATERIALIZED VIEW"));
    assert!(down.contains("DROP TABLE"));

    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_init_table_writes_create_table() {
    let dir = temp_dir("contract_init_table");
    let output = contract_init(&dir, "table");
    assert!(output.status.success(), "{:?}", output);
    let up = std::fs::read_to_string(dir.join("sample_v1_up.sql")).unwrap();
    assert!(up.contains("CREATE TABLE"));
    assert!(!up.contains("MATERIALIZED VIEW"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_init_refuses_to_overwrite_contract() {
    let dir = temp_dir("contract_init_overwrite");
    std::fs::write(dir.join("sample.contract.json"), "edited by hand").unwrap();
    let output = contract_init(&dir, "kafka");
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        std::fs::read_to_string(dir.join("sample.contract.json")).unwrap(),
        "edited by hand"
    );
    assert!(!dir.join("sample_v1_up.sql").exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_init_kafka_rejects_engine_flag() {
    let output = clickforge()
        .args([
            "contract",
            "init",
            &fixture("sample.json"),
            "--target",
            "kafka",
            "--engine",
            "MergeTree",
            "-o",
            "/nonexistent",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge");
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--engine"));
}

#[test]
fn contract_check_matching_sample_exits_0() {
    let dir = temp_dir("contract_check_match");
    contract_init(&dir, "kafka");
    let output = contract_check(&dir, SAMPLE_V1);
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_check_new_nullable_field_exits_0_and_reports_it() {
    let dir = temp_dir("contract_check_compatible");
    contract_init(&dir, "kafka");
    let sample = r#"[
      {"user_id": "u3", "event_time": "2024-03-03T10:00:00Z", "amount": 1, "device": "tv"},
      {"user_id": "u4", "event_time": "2024-03-04T10:00:00Z", "amount": 2}
    ]"#;
    let output = contract_check(&dir, sample);
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("compatible") && stderr.contains("device"),
        "{stderr}"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_check_attention_exits_2() {
    let dir = temp_dir("contract_check_attention");
    contract_init(&dir, "kafka");
    let output = contract_check(&dir, &SAMPLE_V1.replace("\"amount\": 1", "\"amount\": 1.5"));
    assert_eq!(output.status.code(), Some(2), "{:?}", output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("amount"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_check_breaking_exits_3() {
    let dir = temp_dir("contract_check_breaking");
    contract_init(&dir, "kafka");
    let output = contract_check(
        &dir,
        &SAMPLE_V1.replace("\"amount\": 1", "\"amount\": \"one\""),
    );
    assert_eq!(output.status.code(), Some(3), "{:?}", output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("breaking"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_check_invalid_contract_exits_1() {
    let dir = temp_dir("contract_check_invalid");
    std::fs::write(dir.join("sample.contract.json"), "{\"version\": 1}").unwrap();
    let output = contract_check(&dir, SAMPLE_V1);
    assert_eq!(output.status.code(), Some(1), "{:?}", output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("sample.contract.json"));
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Runs `contract apply` against `dir/sample.contract.json` with `extra` args,
/// feeding `sample` on stdin.
fn contract_apply(dir: &std::path::Path, sample: &str, extra: &[&str]) -> std::process::Output {
    let contract = dir.join("sample.contract.json");
    let mut child = clickforge()
        .args(["contract", "apply", contract.to_str().unwrap(), "-"])
        .args(extra)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to run clickforge");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(sample.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn read_contract(dir: &std::path::Path) -> serde_json::Value {
    serde_json::from_str(&std::fs::read_to_string(dir.join("sample.contract.json")).unwrap())
        .unwrap()
}

const SAMPLE_NEW_NULLABLE_FIELD: &str = r#"[
  {"user_id": "u3", "event_time": "2024-03-03T10:00:00Z", "amount": 1, "device": "tv"},
  {"user_id": "u4", "event_time": "2024-03-04T10:00:00Z", "amount": 2}
]"#;

#[test]
fn contract_apply_compatible_writes_v2_and_bumps_contract() {
    let dir = temp_dir("contract_apply_compatible");
    contract_init(&dir, "kafka");
    let output = contract_apply(&dir, SAMPLE_NEW_NULLABLE_FIELD, &[]);
    assert_eq!(output.status.code(), Some(0), "{:?}", output);

    let contract = read_contract(&dir);
    assert_eq!(contract["version"], 2);
    assert_eq!(contract["fields"][3]["name"], "device");
    assert_eq!(contract["fields"][3]["nullable"], true);

    let up = std::fs::read_to_string(dir.join("sample_v2_up.sql")).unwrap();
    let down = std::fs::read_to_string(dir.join("sample_v2_down.sql")).unwrap();
    assert!(up.contains("ADD COLUMN IF NOT EXISTS `device`"), "{up}");
    assert!(up.contains("DETACH TABLE `streams`.`sample`"), "{up}");
    assert!(down.contains("DROP COLUMN IF EXISTS `device`"), "{down}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_apply_breaking_exits_3_and_writes_nothing() {
    let dir = temp_dir("contract_apply_breaking");
    contract_init(&dir, "kafka");
    let before = read_contract(&dir);
    let sample = SAMPLE_NEW_NULLABLE_FIELD.replace("\"amount\": 1", "\"amount\": \"one\"");
    let output = contract_apply(&dir, &sample, &[]);
    assert_eq!(output.status.code(), Some(3), "{:?}", output);
    assert_eq!(read_contract(&dir), before);
    assert!(!dir.join("sample_v2_up.sql").exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_apply_unaccepted_attention_exits_2_and_writes_nothing() {
    let dir = temp_dir("contract_apply_unaccepted");
    contract_init(&dir, "kafka");
    let before = read_contract(&dir);
    let output = contract_apply(
        &dir,
        &SAMPLE_V1.replace("\"amount\": 1", "\"amount\": 1.5"),
        &[],
    );
    assert_eq!(output.status.code(), Some(2), "{:?}", output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("--accept"));
    assert_eq!(read_contract(&dir), before);
    assert!(!dir.join("sample_v2_up.sql").exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_apply_accepted_widening_modifies_column() {
    let dir = temp_dir("contract_apply_accepted");
    contract_init(&dir, "kafka");
    let sample = SAMPLE_V1.replace("\"amount\": 1", "\"amount\": 1.5");
    let output = contract_apply(&dir, &sample, &["--accept", "amount"]);
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    assert_eq!(read_contract(&dir)["fields"][2]["type"], "Float64");
    let up = std::fs::read_to_string(dir.join("sample_v2_up.sql")).unwrap();
    assert!(up.contains("MODIFY COLUMN `amount` Float64"), "{up}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_apply_keeps_hand_edited_settings() {
    let dir = temp_dir("contract_apply_settings");
    contract_init(&dir, "kafka");
    let mut contract = read_contract(&dir);
    contract["unknown_fields"] = serde_json::json!("ignore");
    std::fs::write(dir.join("sample.contract.json"), contract.to_string()).unwrap();

    let sample = SAMPLE_NEW_NULLABLE_FIELD.replace("\"amount\": 1", "\"amount\": 1.5");
    let output = contract_apply(&dir, &sample, &["--accept-all"]);
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    let contract = read_contract(&dir);
    assert_eq!(contract["unknown_fields"], "ignore");
    assert_eq!(contract["fields"].as_array().unwrap().len(), 3); // `device` ignored
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_apply_rejects_accepting_unchanged_field() {
    let dir = temp_dir("contract_apply_bad_accept");
    contract_init(&dir, "kafka");
    let output = contract_apply(&dir, SAMPLE_NEW_NULLABLE_FIELD, &["--accept", "amount"]);
    assert_eq!(output.status.code(), Some(1), "{:?}", output);
    assert!(!dir.join("sample_v2_up.sql").exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_apply_refuses_to_overwrite_migration() {
    let dir = temp_dir("contract_apply_overwrite");
    contract_init(&dir, "kafka");
    std::fs::write(dir.join("sample_v2_up.sql"), "-- hand written").unwrap();
    let output = contract_apply(&dir, SAMPLE_NEW_NULLABLE_FIELD, &[]);
    assert_eq!(output.status.code(), Some(1), "{:?}", output);
    assert_eq!(read_contract(&dir)["version"], 1);
    assert_eq!(
        std::fs::read_to_string(dir.join("sample_v2_up.sql")).unwrap(),
        "-- hand written"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_apply_without_changes_writes_nothing() {
    let dir = temp_dir("contract_apply_noop");
    contract_init(&dir, "kafka");
    let output = contract_apply(&dir, SAMPLE_V1, &[]);
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    assert_eq!(read_contract(&dir)["version"], 1);
    assert!(!dir.join("sample_v2_up.sql").exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Copies `dir/sample.contract.json` to `dir/old.contract.json`, then applies `edit`
/// to the original, as someone editing the deployed contract by hand would.
fn edit_contract(dir: &std::path::Path, edit: impl FnOnce(&mut serde_json::Value)) {
    std::fs::copy(
        dir.join("sample.contract.json"),
        dir.join("old.contract.json"),
    )
    .unwrap();
    let mut contract = read_contract(dir);
    edit(&mut contract);
    std::fs::write(dir.join("sample.contract.json"), contract.to_string()).unwrap();
}

fn contract_diff(dir: &std::path::Path) -> std::process::Output {
    clickforge()
        .args([
            "contract",
            "diff",
            dir.join("old.contract.json").to_str().unwrap(),
            dir.join("sample.contract.json").to_str().unwrap(),
        ])
        .stdin(Stdio::null())
        .output()
        .expect("failed to run clickforge")
}

#[test]
fn contract_diff_retiring_a_field_swaps_the_view() {
    let dir = temp_dir("contract_diff_retire");
    contract_init(&dir, "kafka");
    edit_contract(&dir, |c| {
        c["version"] = serde_json::json!(2);
        c["fields"][2]["retired"] = serde_json::json!(true);
    });
    let output = contract_diff(&dir);
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    let up = std::fs::read_to_string(dir.join("sample_v2_up.sql")).unwrap();
    assert!(up.contains("DETACH TABLE `streams`.`sample`"), "{up}");
    assert!(!up.contains("ALTER") && !up.contains("'amount'"), "{up}");
    let down = std::fs::read_to_string(dir.join("sample_v2_down.sql")).unwrap();
    assert!(down.contains("'amount'"), "{down}");
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_diff_narrowing_warns_in_up_script() {
    let dir = temp_dir("contract_diff_narrow");
    contract_init(&dir, "table");
    edit_contract(&dir, |c| {
        c["version"] = serde_json::json!(2);
        c["fields"][0]["type"] = serde_json::json!("Int64"); // user_id: String -> Int64
    });
    let output = contract_diff(&dir);
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("breaking"));
    let up = std::fs::read_to_string(dir.join("sample_v2_up.sql")).unwrap();
    assert!(
        up.contains("-- WARNING") && up.contains("MODIFY COLUMN `user_id` Int64"),
        "{up}"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_diff_requires_version_bump() {
    let dir = temp_dir("contract_diff_version");
    contract_init(&dir, "kafka");
    edit_contract(&dir, |c| {
        c["fields"][2]["retired"] = serde_json::json!(true)
    });
    let output = contract_diff(&dir);
    assert_eq!(output.status.code(), Some(1), "{:?}", output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("version"));
    assert!(!dir.join("sample_v2_up.sql").exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_diff_rejects_deleted_field() {
    let dir = temp_dir("contract_diff_deleted");
    contract_init(&dir, "kafka");
    edit_contract(&dir, |c| {
        c["version"] = serde_json::json!(2);
        c["fields"].as_array_mut().unwrap().pop();
    });
    let output = contract_diff(&dir);
    assert_eq!(output.status.code(), Some(1), "{:?}", output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("retired"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_diff_without_sql_writes_nothing() {
    let dir = temp_dir("contract_diff_no_sql");
    contract_init(&dir, "kafka");
    edit_contract(&dir, |c| c["unknown_fields"] = serde_json::json!("reject"));
    let output = contract_diff(&dir);
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("No migration needed"));
    assert!(!dir.join("sample_v2_up.sql").exists());
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_check_reports_retired_field_as_info() {
    let dir = temp_dir("contract_check_retired");
    contract_init(&dir, "kafka");
    let mut contract = read_contract(&dir);
    contract["fields"][2]["retired"] = serde_json::json!(true);
    std::fs::write(dir.join("sample.contract.json"), contract.to_string()).unwrap();
    let output = contract_check(&dir, SAMPLE_V1);
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    assert!(String::from_utf8_lossy(&output.stderr).contains("retired field"));
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn contract_apply_reports_new_field_added_as_nullable() {
    let dir = temp_dir("contract_apply_added_nullable");
    contract_init(&dir, "kafka");
    let sample = SAMPLE_V1.replace("\"amount\": ", "\"os\": \"ios\", \"amount\": ");
    let output = contract_apply(&dir, &sample, &[]);
    assert_eq!(output.status.code(), Some(0), "{:?}", output);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Added as Nullable: os"), "{stderr}");
    std::fs::remove_dir_all(&dir).unwrap();
}
