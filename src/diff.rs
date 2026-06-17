use crate::schema::{Column, InferredSchema};
use std::collections::{HashMap, HashSet};

pub struct Diff {
    pub up: String,
    pub down: String,
    pub warnings: Vec<String>,
}

/// Compares two inferred schemas and produces additive `ALTER TABLE` migrations.
///
/// Only columns present in `new` but missing from `old` produce `ADD COLUMN` (up) and
/// `DROP COLUMN` (down). Removed columns and type changes are reported as warnings rather
/// than emitting destructive `DROP`/`MODIFY` statements.
pub fn diff_schemas(
    old: &InferredSchema,
    new: &InferredSchema,
    table_name: &str,
    cluster: Option<&str>,
) -> Diff {
    let t = table_name;
    let on_cluster = cluster
        .map(|c| format!(" ON CLUSTER `{c}`"))
        .unwrap_or_default();

    let old_by_name: HashMap<&str, &Column> =
        old.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let new_names: HashSet<&str> = new.columns.iter().map(|c| c.name.as_str()).collect();

    let mut up_lines: Vec<String> = Vec::new();
    let mut down_lines: Vec<String> = Vec::new();
    let mut warnings: Vec<String> = Vec::new();

    for col in &new.columns {
        match old_by_name.get(col.name.as_str()) {
            None => {
                let ty = col.ch_type.as_ch_str(col.nullable);
                up_lines.push(format!(
                    "ALTER TABLE `{t}`{on_cluster} ADD COLUMN IF NOT EXISTS `{}` {};",
                    col.name, ty
                ));
                down_lines.push(format!(
                    "ALTER TABLE `{t}`{on_cluster} DROP COLUMN IF EXISTS `{}`;",
                    col.name
                ));
            }
            Some(old_col) if old_col.ch_type != col.ch_type || old_col.nullable != col.nullable => {
                warnings.push(format!(
                    "column `{}` changed type {} -> {} (no MODIFY emitted; review manually)",
                    col.name,
                    old_col.ch_type.as_ch_str(old_col.nullable),
                    col.ch_type.as_ch_str(col.nullable)
                ));
            }
            Some(_) => {}
        }
    }

    for col in &old.columns {
        if !new_names.contains(col.name.as_str()) {
            warnings.push(format!(
                "column `{}` exists in old but not new (no DROP emitted; review manually)",
                col.name
            ));
        }
    }

    // Drop columns in reverse of the order they were added.
    down_lines.reverse();

    Diff {
        up: up_lines.join("\n"),
        down: down_lines.join("\n"),
        warnings,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::infer_schema;

    #[test]
    fn added_field_produces_add_and_drop() {
        let old = infer_schema(r#"[{"a":1}]"#, "t").unwrap();
        let new = infer_schema(r#"[{"a":1,"b":"x"}]"#, "t").unwrap();
        let d = diff_schemas(&old, &new, "t", None);
        assert!(d.up.contains("ADD COLUMN IF NOT EXISTS `b`"));
        assert!(d.down.contains("DROP COLUMN IF EXISTS `b`"));
        assert!(!d.up.contains("`a`")); // unchanged column untouched
        assert!(d.warnings.is_empty());
    }

    #[test]
    fn removed_field_warns_not_dropped() {
        let old = infer_schema(r#"[{"a":1,"b":2}]"#, "t").unwrap();
        let new = infer_schema(r#"[{"a":1}]"#, "t").unwrap();
        let d = diff_schemas(&old, &new, "t", None);
        assert!(d.up.is_empty());
        assert!(
            d.warnings
                .iter()
                .any(|w| w.contains("`b`") && w.contains("not new"))
        );
    }

    #[test]
    fn type_change_warns() {
        let old = infer_schema(r#"[{"a":1}]"#, "t").unwrap(); // Int64
        let new = infer_schema(r#"[{"a":"x"}]"#, "t").unwrap(); // String
        let d = diff_schemas(&old, &new, "t", None);
        assert!(d.up.is_empty());
        assert!(d.warnings.iter().any(|w| w.contains("changed type")));
    }

    #[test]
    fn nullability_change_warns() {
        // a is nullable in old (absent from one record), non-nullable in new
        let old = infer_schema("{\"a\":1}\n{}", "t").unwrap();
        let new = infer_schema(r#"[{"a":1},{"a":2}]"#, "t").unwrap();
        let d = diff_schemas(&old, &new, "t", None);
        assert!(d.up.is_empty());
        assert!(
            d.warnings
                .iter()
                .any(|w| w.contains("`a`") && w.contains("changed type"))
        );
    }

    #[test]
    fn cluster_adds_on_cluster() {
        let old = infer_schema(r#"[{"a":1}]"#, "t").unwrap();
        let new = infer_schema(r#"[{"a":1,"b":2}]"#, "t").unwrap();
        let d = diff_schemas(&old, &new, "t", Some("ck"));
        assert!(d.up.contains("ON CLUSTER `ck`"));
    }
}
