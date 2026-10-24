use crate::contract::{Contract, Field, Target, is_numeric_widening};
use crate::generator;
use crate::inference::merge_types;
use crate::schema::{Migration, quote_ident, quote_qualified, quote_string_literal};

/// The migration that moves a deployed `old` contract to `new`: `ADD COLUMN` for new
/// fields and `MODIFY COLUMN` for changed ones. For a kafka pipeline it also recreates
/// the `raw` → `datalake` view, with `streams` detached meanwhile so Kafka holds the
/// messages and none are lost. Retiring a field only drops it from the view; columns
/// are never dropped. Named `<table>_v<new version>`; `None` when nothing in
/// ClickHouse changes.
pub fn migration(old: &Contract, new: &Contract) -> Option<Migration> {
    let kafka = new.target == Target::Kafka;
    let table = if kafka {
        quote_qualified("datalake", &new.table)
    } else {
        quote_ident(&new.table)
    };
    let alter = format!("ALTER TABLE {table}{}", on_cluster(new));

    let mut up: Vec<String> = Vec::new();
    let mut down: Vec<String> = Vec::new();
    let mut previous: Option<&str> = None;
    for field in &new.fields {
        let name = quote_ident(&field.name);
        match old.fields.iter().find(|f| f.name == field.name) {
            None => {
                // Keep datalake's fields ahead of its Kafka metadata columns, where a
                // freshly generated pipeline has them.
                let position = match (kafka, previous) {
                    (false, _) => String::new(),
                    (true, Some(p)) => format!(" AFTER {}", quote_ident(p)),
                    (true, None) => " FIRST".to_string(),
                };
                up.push(format!(
                    "{alter} ADD COLUMN IF NOT EXISTS {name} {}{position};",
                    field.column().ch_type_str()
                ));
                down.push(format!("{alter} DROP COLUMN IF EXISTS {name};"));
            }
            Some(old_field) if !same_column(old_field, field) => {
                up.push(modify_column(&alter, old_field, field));
                down.push(modify_column(&alter, field, old_field));
            }
            Some(_) => {}
        }
        previous = Some(&field.name);
    }
    let extracted = |c: &Contract| {
        c.fields
            .iter()
            .filter(|f| !f.retired)
            .cloned()
            .collect::<Vec<_>>()
    };
    let view_changed = kafka && extracted(old) != extracted(new);
    if up.is_empty() && !view_changed {
        return None;
    }
    down.reverse();
    let mut up = up.join("\n");
    let mut down = down.join("\n");

    // The view must extract only columns that exist: recreate it after adding columns,
    // and restore the old one before dropping them.
    if kafka {
        up = format!("{up}\n\n{}", recreate_raw_mv(new))
            .trim_start()
            .to_string();
        down = format!("{}\n\n{down}", recreate_raw_mv(old))
            .trim_end()
            .to_string();
    }

    Some(Migration {
        name: format!("{}_v{}", new.table, new.version),
        up,
        down,
    })
}

/// Whether two versions of a field are the same ClickHouse column: retiring or
/// reviving a field only changes what the view extracts.
fn same_column(a: &Field, b: &Field) -> bool {
    a.ty == b.ty && a.nullable == b.nullable && a.low_cardinality == b.low_cardinality
}

/// `MODIFY COLUMN` from `from` to `to`, preceded by a warning where it can lose data
/// or fail.
fn modify_column(alter: &str, from: &Field, to: &Field) -> String {
    let name = quote_ident(&to.name);
    let (from_col, to_col) = (from.column(), to.column());
    let to_type = to_col.ch_type_str();
    let mut sql = String::new();
    if from_col.ch_type != to_col.ch_type
        && merge_types(&from_col.ch_type, &to_col.ch_type) != to_col.ch_type
    {
        let risk = if is_numeric_widening(&to_col.ch_type, &from_col.ch_type) {
            "values that don't fit are truncated"
        } else {
            "the mutation fails, and blocks this table's later mutations, if any value doesn't convert"
        };
        sql.push_str(&format!(
            "-- WARNING: converts {name} from {} to {}; {risk}.\n",
            from_col.ch_type.as_str(),
            to_col.ch_type.as_str()
        ));
    }
    if from.nullable && !to.nullable {
        // ClickHouse refuses Nullable(T) -> T without a DEFAULT, even when there are
        // no NULLs. Remove it again so the column ends up exactly as declared.
        sql.push_str(&format!(
            "-- WARNING: makes {name} non-nullable; NULLs become {to_type} defaults.\n\
             {alter} MODIFY COLUMN {name} {to_type} DEFAULT defaultValueOfTypeName('{}');\n\
             {alter} MODIFY COLUMN {name} REMOVE DEFAULT;",
            quote_string_literal(&to_type)
        ));
    } else {
        sql.push_str(&format!("{alter} MODIFY COLUMN {name} {to_type};"));
    }
    sql
}

fn on_cluster(contract: &Contract) -> String {
    // Cluster names are config-defined labels, left unquoted as in generator.rs.
    contract
        .cluster
        .as_ref()
        .map(|c| format!(" ON CLUSTER {c}"))
        .unwrap_or_default()
}

/// Swaps the `raw` → `datalake` view for `contract`'s, with `streams` detached so it
/// stops consuming: Kafka keeps the messages and the offset doesn't move meanwhile.
fn recreate_raw_mv(contract: &Contract) -> String {
    let t = &contract.table;
    let c = on_cluster(contract);
    let streams_t = quote_qualified("streams", t);
    let raw_mv = quote_qualified("raw", &format!("{t}_mv"));
    // `Contract::from_json` rejects kafka contracts without them.
    let view = generator::kafka_raw_mv(
        &contract.schema(),
        contract
            .cluster
            .clone()
            .expect("kafka contract has a cluster"),
        contract
            .kafka
            .clone()
            .expect("kafka contract has a kafka collection"),
    );
    format!(
        "DETACH TABLE {streams_t}{c};\n\
         DROP VIEW IF EXISTS {raw_mv}{c} SYNC;\n\
         {view}\n\
         ATTACH TABLE {streams_t}{c};"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::{FieldType, Target};
    use crate::inference::infer_schema;
    use crate::schema::{EngineConfig, TableEngine};

    fn contract(json: &str, target: Target, cluster: Option<&str>) -> Contract {
        let schema = infer_schema(json, "t").unwrap();
        let engine = EngineConfig {
            engine: TableEngine::MergeTree,
            order_by: vec![],
            sum_columns: vec![],
        };
        let kafka = (target == Target::Kafka).then(|| "kafka".to_string());
        Contract::from_schema(&schema, target, cluster.map(String::from), kafka, engine)
    }

    /// `old` with `a` widened to Float64 and a new nullable `b`, as version 2.
    fn evolved(old: &Contract) -> Contract {
        let mut new = old.clone();
        new.version = 2;
        new.fields[0].ty = FieldType::Float64;
        let mut b = new.fields[0].clone();
        b.name = "b".to_string();
        b.ty = FieldType::String;
        b.nullable = true;
        new.fields.push(b);
        new
    }

    /// Byte offset of `needle` in `haystack`, failing the test when absent.
    fn pos(haystack: &str, needle: &str) -> usize {
        haystack
            .find(needle)
            .unwrap_or_else(|| panic!("`{needle}` not found in:\n{haystack}"))
    }

    #[test]
    fn named_after_the_new_version() {
        let old = contract(r#"[{"a":1}]"#, Target::Table, None);
        assert_eq!(migration(&old, &evolved(&old)).unwrap().name, "t_v2");
    }

    #[test]
    fn table_adds_and_modifies_columns() {
        let old = contract(r#"[{"a":1}]"#, Target::Table, None);
        let m = migration(&old, &evolved(&old)).unwrap();
        assert_eq!(
            m.up,
            "ALTER TABLE `t` MODIFY COLUMN `a` Float64;\n\
             ALTER TABLE `t` ADD COLUMN IF NOT EXISTS `b` Nullable(String);"
        );
        // Reverse order: drop the added column, then revert the modify.
        assert!(
            pos(&m.down, "DROP COLUMN IF EXISTS `b`") < pos(&m.down, "MODIFY COLUMN `a` Int64")
        );
    }

    #[test]
    fn reverting_a_widening_warns_about_truncation() {
        let old = contract(r#"[{"a":1}]"#, Target::Table, None);
        let down = migration(&old, &evolved(&old)).unwrap().down;
        let warning = pos(&down, "-- WARNING");
        assert!(down[warning..].contains("truncat"), "{down}");
        assert!(warning < pos(&down, "MODIFY COLUMN `a` Int64"));
    }

    #[test]
    fn reverting_nullability_warns() {
        let old = contract(r#"[{"a":1}]"#, Target::Table, None);
        let mut new = old.clone();
        new.version = 2;
        new.fields[0].nullable = true;
        let m = migration(&old, &new).unwrap();
        assert_eq!(m.up, "ALTER TABLE `t` MODIFY COLUMN `a` Nullable(Int64);");
        // ClickHouse refuses Nullable(T) -> T without a DEFAULT, even with no NULLs;
        // the default is removed again so the column ends up exactly as before.
        assert_eq!(
            m.down,
            "-- WARNING: makes `a` non-nullable; NULLs become Int64 defaults.\n\
             ALTER TABLE `t` MODIFY COLUMN `a` Int64 DEFAULT defaultValueOfTypeName('Int64');\n\
             ALTER TABLE `t` MODIFY COLUMN `a` REMOVE DEFAULT;"
        );
    }

    #[test]
    fn modify_keeps_low_cardinality() {
        let mut old = contract(r#"[{"a":"x"}]"#, Target::Table, None);
        old.fields[0].low_cardinality = true;
        let mut new = old.clone();
        new.version = 2;
        new.fields[0].nullable = true;
        let up = migration(&old, &new).unwrap().up;
        assert!(up.contains("LowCardinality(Nullable(String))"), "{up}");
    }

    #[test]
    fn table_cluster_adds_on_cluster() {
        let old = contract(r#"[{"a":1}]"#, Target::Table, Some("ck"));
        let m = migration(&old, &evolved(&old)).unwrap();
        assert!(m.up.contains("ALTER TABLE `t` ON CLUSTER ck MODIFY COLUMN"));
        assert!(m.down.contains("ALTER TABLE `t` ON CLUSTER ck DROP COLUMN"));
    }

    #[test]
    fn table_does_not_touch_views() {
        let old = contract(r#"[{"a":1}]"#, Target::Table, None);
        let m = migration(&old, &evolved(&old)).unwrap();
        assert!(!m.up.contains("VIEW") && !m.down.contains("VIEW"));
    }

    #[test]
    fn kafka_alters_datalake_after_existing_fields() {
        let old = contract(r#"[{"a":1}]"#, Target::Kafka, Some("ck"));
        let up = migration(&old, &evolved(&old)).unwrap().up;
        assert!(up.contains(
            "ALTER TABLE `datalake`.`t` ON CLUSTER ck ADD COLUMN IF NOT EXISTS `b` Nullable(String) AFTER `a`;"
        ), "{up}");
    }

    #[test]
    fn kafka_up_alters_then_recreates_view_with_streams_detached() {
        let old = contract(r#"[{"a":1}]"#, Target::Kafka, Some("ck"));
        let up = migration(&old, &evolved(&old)).unwrap().up;
        let alter = pos(&up, "ADD COLUMN");
        let detach = pos(&up, "DETACH TABLE `streams`.`t` ON CLUSTER ck;");
        let drop = pos(&up, "DROP VIEW IF EXISTS `raw`.`t_mv` ON CLUSTER ck SYNC;");
        let create = pos(&up, "CREATE MATERIALIZED VIEW IF NOT EXISTS `raw`.`t_mv`");
        let attach = pos(&up, "ATTACH TABLE `streams`.`t` ON CLUSTER ck;");
        assert!(alter < detach && detach < drop && drop < create && create < attach);
        assert!(up.contains("JSONExtract(message, 'b', 'Nullable(String)') AS `b`"));
        assert!(up.contains("JSONExtract(message, 'a', 'Float64') AS `a`"));
    }

    #[test]
    fn kafka_down_restores_old_view_before_dropping_columns() {
        let old = contract(r#"[{"a":1}]"#, Target::Kafka, Some("ck"));
        let down = migration(&old, &evolved(&old)).unwrap().down;
        let detach = pos(&down, "DETACH TABLE");
        let create = pos(&down, "CREATE MATERIALIZED VIEW");
        let attach = pos(&down, "ATTACH TABLE");
        let drop_col = pos(&down, "DROP COLUMN IF EXISTS `b`");
        assert!(detach < create && create < attach && attach < drop_col);
        assert!(
            !down.contains("'b'"),
            "old view must not extract `b`:\n{down}"
        );
        assert!(down.contains("JSONExtract(message, 'a', 'Int64') AS `a`"));
    }

    #[test]
    fn no_migration_when_only_settings_change() {
        for target in [Target::Table, Target::Kafka] {
            let old = contract(r#"[{"a":1}]"#, target, Some("ck"));
            let mut new = old.clone();
            new.unknown_fields = crate::contract::UnknownFields::Ignore;
            assert!(migration(&old, &new).is_none());
        }
    }

    #[test]
    fn retiring_in_table_mode_needs_no_migration() {
        let old = contract(r#"[{"a":1,"b":2}]"#, Target::Table, None);
        let mut new = old.clone();
        new.fields[1].retired = true;
        assert!(migration(&old, &new).is_none());
    }

    #[test]
    fn retiring_in_kafka_mode_only_swaps_the_view() {
        let old = contract(r#"[{"a":1,"b":2}]"#, Target::Kafka, Some("ck"));
        let mut new = old.clone();
        new.version = 2;
        new.fields[1].retired = true;
        let m = migration(&old, &new).unwrap();
        assert!(
            !m.up.contains("ALTER") && !m.down.contains("ALTER"),
            "{}",
            m.up
        );
        assert!(
            !m.up.contains("'b'"),
            "new view must not extract `b`:\n{}",
            m.up
        );
        assert!(m.down.contains("JSONExtract(message, 'b', 'Int64') AS `b`"));
    }

    #[test]
    fn reviving_with_new_type_modifies_instead_of_adding() {
        let mut old = contract(r#"[{"a":1,"b":2}]"#, Target::Kafka, Some("ck"));
        old.fields[1].retired = true;
        let mut new = old.clone();
        new.version = 2;
        new.fields[1].retired = false;
        new.fields[1].ty = FieldType::Float64;
        let up = migration(&old, &new).unwrap().up;
        assert!(up.contains("MODIFY COLUMN `b` Float64;"), "{up}");
        assert!(!up.contains("ADD COLUMN"), "{up}");
        assert!(up.contains("JSONExtract(message, 'b', 'Float64') AS `b`"));
    }

    #[test]
    fn narrowing_up_warns_that_the_mutation_can_fail() {
        let old = contract(r#"[{"a":"x"}]"#, Target::Table, None);
        let mut new = old.clone();
        new.version = 2;
        new.fields[0].ty = FieldType::Int64;
        let m = migration(&old, &new).unwrap();
        assert_eq!(
            m.up,
            "-- WARNING: converts `a` from String to Int64; the mutation fails, and blocks \
             this table's later mutations, if any value doesn't convert.\n\
             ALTER TABLE `t` MODIFY COLUMN `a` Int64;"
        );
        // Back to String always converts.
        assert_eq!(m.down, "ALTER TABLE `t` MODIFY COLUMN `a` String;");
    }

    #[test]
    fn making_a_field_required_uses_a_temporary_default() {
        let old = contract("{\"a\":1}\n{}", Target::Table, None); // a nullable
        let mut new = old.clone();
        new.version = 2;
        new.fields[0].nullable = false;
        let m = migration(&old, &new).unwrap();
        assert!(
            m.up.contains("DEFAULT defaultValueOfTypeName('Int64');"),
            "{}",
            m.up
        );
        assert!(
            m.up.contains("MODIFY COLUMN `a` REMOVE DEFAULT;"),
            "{}",
            m.up
        );
        assert_eq!(m.down, "ALTER TABLE `t` MODIFY COLUMN `a` Nullable(Int64);");
    }

    #[test]
    fn toggling_low_cardinality_modifies_column() {
        let old = contract(r#"[{"a":"x"}]"#, Target::Table, None);
        let mut new = old.clone();
        new.version = 2;
        new.fields[0].low_cardinality = true;
        let m = migration(&old, &new).unwrap();
        assert_eq!(
            m.up,
            "ALTER TABLE `t` MODIFY COLUMN `a` LowCardinality(String);"
        );
        assert_eq!(m.down, "ALTER TABLE `t` MODIFY COLUMN `a` String;");
    }
}
