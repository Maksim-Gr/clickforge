use crate::inference::infer_schema;
use crate::schema::InferredSchema;
use std::fs;
use std::io::Read;
use std::path::Path;

/// A JSON sample loaded from a file, or from stdin when the path is `-`.
pub struct Sample {
    pub schema: InferredSchema,
    /// The path, or `<stdin>`, for display.
    pub label: String,
    pub from_stdin: bool,
    /// True when no name was given and the sample came from stdin, so the table
    /// name fell back to `table`.
    pub defaulted_name: bool,
}

/// Loads a sample, naming the table `name`, else after the file stem, else
/// (for stdin) `table`.
pub fn load(path: &Path, name: Option<String>, stdin: impl Read) -> Result<Sample, String> {
    let (table_name, defaulted_name) = match name {
        Some(name) => (name, false),
        None if is_stdin(path) => ("table".to_string(), true),
        None => (name_from_stem(path)?, false),
    };
    read(path, table_name, defaulted_name, stdin)
}

/// Loads an old and a new sample under one table name: `name`, else the new
/// file's stem, else (when new is stdin) the old file's. At most one may be stdin.
pub fn load_pair(
    old: &Path,
    new: &Path,
    name: Option<String>,
    mut stdin: impl Read,
) -> Result<(Sample, Sample), String> {
    if is_stdin(old) && is_stdin(new) {
        return Err("only one of <OLD> and <NEW> can be '-' (stdin).".to_string());
    }
    let table_name = match name {
        Some(name) => name,
        None => name_from_stem(if is_stdin(new) { old } else { new })?,
    };
    let old = read(old, table_name.clone(), false, &mut stdin)?;
    let new = read(new, table_name, false, &mut stdin)?;
    Ok((old, new))
}

fn is_stdin(path: &Path) -> bool {
    path.as_os_str() == "-"
}

fn name_from_stem(path: &Path) -> Result<String, String> {
    path.file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .ok_or_else(|| format!("could not derive a table name from {:?}; pass --name", path))
}

fn read(
    path: &Path,
    table_name: String,
    defaulted_name: bool,
    mut stdin: impl Read,
) -> Result<Sample, String> {
    let from_stdin = is_stdin(path);
    let content = if from_stdin {
        let mut buf = String::new();
        stdin
            .read_to_string(&mut buf)
            .map_err(|e| format!("reading stdin: {}", e))?;
        buf
    } else {
        fs::read_to_string(path).map_err(|e| format!("reading {:?}: {}", path, e))?
    };
    let schema =
        infer_schema(&content, &table_name).map_err(|e| format!("inferring schema: {}", e))?;
    let label = if from_stdin {
        "<stdin>".to_string()
    } else {
        path.display().to_string()
    };
    Ok(Sample {
        schema,
        label,
        from_stdin,
        defaulted_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = "tests/fixtures/sample.json";
    const NDJSON: &[u8] = b"{\"a\":1}\n{\"a\":2}";

    fn no_stdin() -> &'static [u8] {
        b""
    }

    #[test]
    fn load_file_names_table_after_stem() {
        let s = load(Path::new(FIXTURE), None, no_stdin()).unwrap();
        assert_eq!(s.schema.table_name, "sample");
        assert_eq!(s.schema.record_count, 2);
        assert_eq!(s.label, FIXTURE);
        assert!(!s.from_stdin);
        assert!(!s.defaulted_name);
    }

    #[test]
    fn load_name_override_wins() {
        let s = load(Path::new(FIXTURE), Some("ev".to_string()), no_stdin()).unwrap();
        assert_eq!(s.schema.table_name, "ev");
    }

    #[test]
    fn load_stdin_defaults_name_to_table() {
        let s = load(Path::new("-"), None, NDJSON).unwrap();
        assert_eq!(s.schema.table_name, "table");
        assert_eq!(s.schema.record_count, 2);
        assert_eq!(s.label, "<stdin>");
        assert!(s.from_stdin);
        assert!(s.defaulted_name);
    }

    #[test]
    fn load_stdin_with_name_is_not_defaulted() {
        let s = load(Path::new("-"), Some("ev".to_string()), NDJSON).unwrap();
        assert_eq!(s.schema.table_name, "ev");
        assert!(!s.defaulted_name);
    }

    #[test]
    fn load_missing_file_errors() {
        let Err(e) = load(Path::new("tests/fixtures/nope.json"), None, no_stdin()) else {
            panic!("expected an error");
        };
        assert!(e.starts_with("reading \"tests/fixtures/nope.json\""));
    }

    #[test]
    fn load_malformed_input_errors() {
        let Err(e) = load(Path::new("-"), None, &b"{a}"[..]) else {
            panic!("expected an error");
        };
        assert!(e.starts_with("inferring schema:"));
    }

    #[test]
    fn load_pair_names_after_new_file() {
        let (old, new) = load_pair(
            Path::new("tests/fixtures/old.json"),
            Path::new("tests/fixtures/new.json"),
            None,
            no_stdin(),
        )
        .unwrap();
        assert_eq!(old.schema.table_name, "new");
        assert_eq!(new.schema.table_name, "new");
    }

    #[test]
    fn load_pair_names_after_old_file_when_new_is_stdin() {
        let (old, new) = load_pair(
            Path::new("tests/fixtures/old.json"),
            Path::new("-"),
            None,
            NDJSON,
        )
        .unwrap();
        assert_eq!(old.schema.table_name, "old");
        assert_eq!(new.schema.table_name, "old");
        assert!(new.from_stdin);
    }

    #[test]
    fn load_pair_rejects_both_stdin() {
        let Err(e) = load_pair(Path::new("-"), Path::new("-"), None, no_stdin()) else {
            panic!("expected an error");
        };
        assert!(e.contains("only one of <OLD> and <NEW>"));
    }
}
