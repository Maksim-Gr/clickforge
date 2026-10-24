use crate::contract::{Contract, Target};
use crate::generator;
use crate::schema::{Migration, quote_ident, quote_qualified, quote_string_literal};

/// The migration that moves a deployed `old` contract to `new`: `ADD COLUMN` for new
/// fields and `MODIFY COLUMN` for changed ones. For a kafka pipeline it also recreates
/// the `raw` → `datalake` view, with `streams` detached meanwhile so Kafka holds the
/// messages and none are lost. Named `<table>_v<new version>`.
pub fn migration(old: &Contract, new: &Contract) -> Migration {
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
            Some(old_field) if old_field != field => {
                up.push(format!(
                    "{alter} MODIFY COLUMN {name} {};",
                    field.column().ch_type_str()
                ));
                let mut revert = String::new();
                if old_field.ty != field.ty {
                    revert.push_str(&format!(
                        "-- WARNING: reverts {name} to {}; values that don't fit are truncated.\n",
                        old_field.column().ch_type.as_str()
                    ));
                }
                let old_type = old_field.column().ch_type_str();
                if !old_field.nullable && field.nullable {
                    // ClickHouse refuses Nullable(T) -> T without a DEFAULT, even when
                    // there are no NULLs. Remove it again so the column ends up as it was.
                    revert.push_str(&format!(
                        "-- WARNING: reverts {name} to non-nullable; NULLs become {old_type} defaults.\n\
                         {alter} MODIFY COLUMN {name} {old_type} DEFAULT defaultValueOfTypeName('{}');\n\
                         {alter} MODIFY COLUMN {name} REMOVE DEFAULT;",
                        quote_string_literal(&old_type)
                    ));
                } else {
                    revert.push_str(&format!("{alter} MODIFY COLUMN {name} {old_type};"));
                }
                down.push(revert);
            }
            Some(_) => {}
        }
        previous = Some(&field.name);
    }
    down.reverse();
    let mut up = up.join("\n");
    let mut down = down.join("\n");

    // The view must extract only columns that exist: recreate it after adding columns,
    // and restore the old one before dropping them.
    if kafka {
        up = format!("{up}\n\n{}", recreate_raw_mv(new));
        down = format!("{}\n\n{down}", recreate_raw_mv(old));
    }

    Migration {
        name: format!("{}_v{}", new.table, new.version),
        up,
        down,
    }
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
        assert_eq!(migration(&old, &evolved(&old)).name, "t_v2");
    }

    #[test]
    fn table_adds_and_modifies_columns() {
        let old = contract(r#"[{"a":1}]"#, Target::Table, None);
        let m = migration(&old, &evolved(&old));
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
        let down = migration(&old, &evolved(&old)).down;
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
        let m = migration(&old, &new);
        assert_eq!(m.up, "ALTER TABLE `t` MODIFY COLUMN `a` Nullable(Int64);");
        // ClickHouse refuses Nullable(T) -> T without a DEFAULT, even with no NULLs;
        // the default is removed again so the column ends up exactly as before.
        assert_eq!(
            m.down,
            "-- WARNING: reverts `a` to non-nullable; NULLs become Int64 defaults.\n\
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
        let up = migration(&old, &new).up;
        assert!(up.contains("LowCardinality(Nullable(String))"), "{up}");
    }

    #[test]
    fn table_cluster_adds_on_cluster() {
        let old = contract(r#"[{"a":1}]"#, Target::Table, Some("ck"));
        let m = migration(&old, &evolved(&old));
        assert!(m.up.contains("ALTER TABLE `t` ON CLUSTER ck MODIFY COLUMN"));
        assert!(m.down.contains("ALTER TABLE `t` ON CLUSTER ck DROP COLUMN"));
    }

    #[test]
    fn table_does_not_touch_views() {
        let old = contract(r#"[{"a":1}]"#, Target::Table, None);
        let m = migration(&old, &evolved(&old));
        assert!(!m.up.contains("VIEW") && !m.down.contains("VIEW"));
    }

    #[test]
    fn kafka_alters_datalake_after_existing_fields() {
        let old = contract(r#"[{"a":1}]"#, Target::Kafka, Some("ck"));
        let up = migration(&old, &evolved(&old)).up;
        assert!(up.contains(
            "ALTER TABLE `datalake`.`t` ON CLUSTER ck ADD COLUMN IF NOT EXISTS `b` Nullable(String) AFTER `a`;"
        ), "{up}");
    }

    #[test]
    fn kafka_up_alters_then_recreates_view_with_streams_detached() {
        let old = contract(r#"[{"a":1}]"#, Target::Kafka, Some("ck"));
        let up = migration(&old, &evolved(&old)).up;
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
        let down = migration(&old, &evolved(&old)).down;
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
}
