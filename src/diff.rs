use crate::contract::{self, Accept, Change, Contract, Target};
use crate::evolve;
use crate::schema::{EngineConfig, InferredSchema, Migration, TableEngine};

pub struct Diff {
    /// `None` when there are no columns to add.
    pub migration: Option<Migration>,
    /// Every difference, classified as `contract check` would.
    pub changes: Vec<Change>,
}

/// Compares two inferred schemas and produces additive `ALTER TABLE` migrations.
///
/// The old schema stands in for a contract, so differences are classified exactly as
/// `contract check` classifies them. Only new fields are migrated, as `ADD COLUMN`
/// (up) and `DROP COLUMN` (down), always nullable: `diff` has no `--accept` to make
/// one required. Type changes and missing fields are only reported, since retyping or
/// dropping a populated column is destructive. The migration is named
/// `<table_name>_alter`.
pub fn diff_schemas(
    old: &InferredSchema,
    new: &InferredSchema,
    table_name: &str,
    cluster: Option<&str>,
) -> Diff {
    let mut baseline = Contract::from_schema(
        old,
        Target::Table,
        cluster.map(String::from),
        None,
        // Never read: no column is modified, so sorting keys don't matter here.
        EngineConfig {
            engine: TableEngine::MergeTree,
            order_by: vec![],
            sum_columns: vec![],
        },
    );
    baseline.table = table_name.to_string();

    let changes = contract::check(&baseline, new);
    // Nothing is accepted, so existing columns are left alone and only additions apply.
    let next = contract::next_version(&baseline, &changes, Accept::Fields(&[]))
        .expect("accepting nothing can't name an unknown field");
    let migration = next
        .contract
        .and_then(|next| evolve::migration(&baseline, &next))
        .map(|m| Migration {
            name: format!("{table_name}_alter"),
            ..m
        });
    Diff { migration, changes }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::ChangeClass;
    use crate::inference::infer_schema;

    fn run(old: &str, new: &str, cluster: Option<&str>) -> Diff {
        let old = infer_schema(old, "t").unwrap();
        let new = infer_schema(new, "t").unwrap();
        diff_schemas(&old, &new, "t", cluster)
    }

    fn classes(d: &Diff) -> Vec<(ChangeClass, &str)> {
        d.changes
            .iter()
            .map(|c| (c.class, c.field.as_str()))
            .collect()
    }

    #[test]
    fn added_nullable_field_is_compatible_add_and_drop() {
        let d = run(r#"[{"a":1}]"#, "{\"a\":1,\"b\":\"x\"}\n{\"a\":2}", None);
        assert_eq!(classes(&d), vec![(ChangeClass::Compatible, "b")]);
        let m = d.migration.expect("expected a migration");
        assert_eq!(m.name, "t_alter");
        assert_eq!(
            m.up,
            "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `b` Nullable(String);"
        );
        assert_eq!(m.down, "ALTER TABLE `t` DROP COLUMN IF EXISTS `b`;");
    }

    #[test]
    fn added_always_present_field_is_added_as_nullable() {
        let d = run(r#"[{"a":1}]"#, r#"[{"a":1,"b":"x"}]"#, None);
        assert_eq!(classes(&d), vec![(ChangeClass::Attention, "b")]);
        let m = d.migration.expect("expected a migration");
        assert!(m.up.contains("`b` Nullable(String);"), "{}", m.up);
    }

    #[test]
    fn required_field_missing_from_new_is_breaking_and_not_dropped() {
        let d = run(r#"[{"a":1,"b":2}]"#, r#"[{"a":1}]"#, None);
        assert_eq!(classes(&d), vec![(ChangeClass::Breaking, "b")]);
        assert!(d.migration.is_none());
    }

    #[test]
    fn nullable_field_missing_from_new_is_no_change() {
        let d = run("{\"a\":1,\"b\":2}\n{\"a\":1}", r#"[{"a":1}]"#, None);
        assert!(d.changes.is_empty());
        assert!(d.migration.is_none());
    }

    #[test]
    fn widening_is_reported_not_modified() {
        let d = run(r#"[{"a":1}]"#, r#"[{"a":1.5}]"#, None);
        assert_eq!(classes(&d), vec![(ChangeClass::Attention, "a")]);
        assert!(d.migration.is_none());
    }

    #[test]
    fn incompatible_type_is_breaking() {
        let d = run(r#"[{"a":1}]"#, r#"[{"a":"x"}]"#, None);
        assert_eq!(classes(&d), vec![(ChangeClass::Breaking, "a")]);
        assert!(d.migration.is_none());
    }

    #[test]
    fn field_becoming_nullable_needs_attention() {
        let d = run(r#"[{"a":1},{"a":2}]"#, "{\"a\":1}\n{}", None);
        assert_eq!(classes(&d), vec![(ChangeClass::Attention, "a")]);
        assert!(d.migration.is_none());
    }

    #[test]
    fn identical_schemas_produce_no_changes() {
        let d = run(r#"[{"a":1,"b":"x"}]"#, r#"[{"a":2,"b":"y"}]"#, None);
        assert!(d.changes.is_empty());
        assert!(d.migration.is_none());
    }

    #[test]
    fn combined_add_remove_and_type_change() {
        // old: a (kept), b (removed, required), c (Int64 -> String)
        // new: a, c (String), d (added, nullable)
        let d = run(
            r#"[{"a":1,"b":2,"c":3}]"#,
            "{\"a\":1,\"c\":\"x\",\"d\":true}\n{\"a\":1,\"c\":\"y\"}",
            None,
        );
        assert_eq!(
            classes(&d),
            vec![
                (ChangeClass::Breaking, "b"),
                (ChangeClass::Breaking, "c"),
                (ChangeClass::Compatible, "d"),
            ]
        );
        let m = d.migration.expect("expected a migration");
        assert_eq!(
            m.up,
            "ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `d` Nullable(Bool);"
        );
    }

    #[test]
    fn cluster_adds_on_cluster() {
        let d = run(r#"[{"a":1}]"#, r#"[{"a":1,"b":2}]"#, Some("ck"));
        assert!(d.migration.unwrap().up.contains("ON CLUSTER ck"));
    }
}
