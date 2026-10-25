use crate::inference::merge_types;
use crate::schema::{Column, ColumnType, EngineConfig, InferredSchema, TableEngine};
use serde::{Deserialize, Serialize};

/// The committed statement of the schema a table or Kafka pipeline expects.
/// Stored as `{table}.contract.json` and checked against new JSON samples.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Contract {
    pub version: u32,
    pub table: String,
    pub target: Target,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    /// Kafka collection name; required for (and only used by) the `kafka` target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka: Option<String>,
    pub engine: TableEngine,
    pub order_by: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sum_columns: Vec<String>,
    #[serde(default)]
    pub unknown_fields: UnknownFields,
    pub fields: Vec<Field>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    Kafka,
    Table,
}

/// What `check` does with a sample field the contract doesn't list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnknownFields {
    /// Treat it as a new field.
    #[default]
    Propose,
    /// Leave it out of the table and only mention it.
    Ignore,
    /// Treat it as breaking.
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(flatten)]
    pub ty: FieldType,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub low_cardinality: bool,
    /// No longer extracted, but its column is kept: columns are never dropped.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub retired: bool,
}

/// `ColumnType` as it is spelled in a contract: `{"type": "Int64"}`,
/// `{"type": "Array", "of": {...}}`, `{"type": "Map", "key": {...}, "value": {...}}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FieldType {
    String,
    Int64,
    UInt64,
    Float64,
    Bool,
    DateTime64,
    Array {
        of: Box<FieldType>,
    },
    Map {
        key: Box<FieldType>,
        value: Box<FieldType>,
    },
}

impl From<&ColumnType> for FieldType {
    fn from(t: &ColumnType) -> Self {
        match t {
            ColumnType::String => FieldType::String,
            ColumnType::Int64 => FieldType::Int64,
            ColumnType::UInt64 => FieldType::UInt64,
            ColumnType::Float64 => FieldType::Float64,
            ColumnType::Bool => FieldType::Bool,
            ColumnType::DateTime64 => FieldType::DateTime64,
            ColumnType::Array(of) => FieldType::Array {
                of: Box::new(of.as_ref().into()),
            },
            ColumnType::Map(key, value) => FieldType::Map {
                key: Box::new(key.as_ref().into()),
                value: Box::new(value.as_ref().into()),
            },
        }
    }
}

impl From<&FieldType> for ColumnType {
    fn from(t: &FieldType) -> Self {
        match t {
            FieldType::String => ColumnType::String,
            FieldType::Int64 => ColumnType::Int64,
            FieldType::UInt64 => ColumnType::UInt64,
            FieldType::Float64 => ColumnType::Float64,
            FieldType::Bool => ColumnType::Bool,
            FieldType::DateTime64 => ColumnType::DateTime64,
            FieldType::Array { of } => ColumnType::Array(Box::new(of.as_ref().into())),
            FieldType::Map { key, value } => ColumnType::Map(
                Box::new(key.as_ref().into()),
                Box::new(value.as_ref().into()),
            ),
        }
    }
}

impl Field {
    pub fn column(&self) -> Column {
        Column {
            name: self.name.clone(),
            ch_type: (&self.ty).into(),
            nullable: self.nullable,
            low_cardinality: self.low_cardinality,
            type_observed: true,
        }
    }
}

impl Contract {
    /// The schema of the fields this contract extracts (retired ones are left out).
    pub fn schema(&self) -> InferredSchema {
        InferredSchema {
            table_name: self.table.clone(),
            columns: self
                .fields
                .iter()
                .filter(|f| !f.retired)
                .map(Field::column)
                .collect(),
            record_count: 0,
        }
    }

    /// Version 1 of a contract for `schema`, deployed as `target` with `engine`.
    pub fn from_schema(
        schema: &InferredSchema,
        target: Target,
        cluster: Option<String>,
        kafka: Option<String>,
        engine: EngineConfig,
    ) -> Contract {
        Contract {
            version: 1,
            table: schema.table_name.clone(),
            target,
            cluster,
            kafka,
            engine: engine.engine,
            order_by: engine.order_by,
            sum_columns: engine.sum_columns,
            unknown_fields: UnknownFields::default(),
            fields: schema
                .columns
                .iter()
                .map(|c| Field {
                    name: c.name.clone(),
                    ty: (&c.ch_type).into(),
                    nullable: c.nullable,
                    low_cardinality: c.low_cardinality,
                    retired: false,
                })
                .collect(),
        }
    }

    pub fn to_json(&self) -> String {
        // Serializing plain data with string keys can't fail.
        serde_json::to_string_pretty(self).expect("contract serializes") + "\n"
    }

    pub fn from_json(content: &str) -> Result<Contract, String> {
        let contract: Contract = serde_json::from_str(content).map_err(|e| e.to_string())?;
        if contract.target == Target::Kafka
            && (contract.cluster.is_none() || contract.kafka.is_none())
        {
            return Err("a kafka contract needs both `cluster` and `kafka`".to_string());
        }
        Ok(contract)
    }
}

/// How a difference between a contract and a JSON sample is judged. Ordered from
/// least to most severe, so the worst change in a check is the `max()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChangeClass {
    Info,
    Compatible,
    Attention,
    Breaking,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub class: ChangeClass,
    pub field: String,
    pub message: String,
    /// The field as the next contract version would have it; `None` when the change
    /// can't be applied (breaking, info).
    pub proposed: Option<Field>,
}

/// Classifies every difference between `contract` and the schema inferred from a
/// JSON sample. An empty result means the sample fits the contract.
pub fn check(contract: &Contract, sample: &InferredSchema) -> Vec<Change> {
    let mut changes = Vec::new();

    for field in &contract.fields {
        if field.retired {
            if sample.columns.iter().any(|c| c.name == field.name) {
                changes.push(Change {
                    class: ChangeClass::Info,
                    field: field.name.clone(),
                    message: "retired field, ignored".to_string(),
                    proposed: None,
                });
            }
            continue;
        }
        let mut found: Vec<(ChangeClass, String)> = Vec::new();
        let expected: ColumnType = (&field.ty).into();
        let mut proposed = field.clone();
        match sample.columns.iter().find(|c| c.name == field.name) {
            None if !field.nullable => found.push((
                ChangeClass::Breaking,
                "required field missing from the sample".to_string(),
            )),
            // A sample is incomplete by nature: absence is not a deletion.
            None => {}
            Some(col) => {
                if col.type_observed {
                    match fit_type(&expected, &col.ch_type) {
                        TypeFit::Fits => {}
                        TypeFit::Widens(widened) => {
                            found.push((
                                ChangeClass::Attention,
                                format!(
                                    "{} -> {} (widening); {} truncates these values",
                                    expected.as_str(),
                                    widened.as_str(),
                                    expected.as_str()
                                ),
                            ));
                            proposed.ty = (&widened).into();
                        }
                        TypeFit::Breaks => found.push((
                            ChangeClass::Breaking,
                            format!(
                                "sample has {} values that don't fit {}; they would be stored as defaults",
                                col.ch_type.as_str(),
                                expected.as_str()
                            ),
                        )),
                    }
                }
                if !field.nullable && col.nullable {
                    proposed.nullable = true;
                    found.push((
                        ChangeClass::Attention,
                        "required field missing or null in some records".to_string(),
                    ));
                }
            }
        }
        let Some(mut class) = found.iter().map(|(class, _)| *class).max() else {
            continue;
        };
        let mut message = found
            .into_iter()
            .map(|(_, m)| m)
            .collect::<Vec<_>>()
            .join("; ");
        // ClickHouse can't MODIFY a sorting-key column.
        if class == ChangeClass::Attention && contract.order_by.contains(&field.name) {
            class = ChangeClass::Breaking;
            message.push_str("; it is in ORDER BY, which can't be modified");
        }
        changes.push(Change {
            class,
            field: field.name.clone(),
            message,
            proposed: (class == ChangeClass::Attention).then_some(proposed),
        });
    }

    for col in &sample.columns {
        if contract.fields.iter().any(|f| f.name == col.name) {
            continue;
        }
        let (class, message) = match contract.unknown_fields {
            UnknownFields::Propose if col.nullable => (
                ChangeClass::Compatible,
                format!("new field {}", col.ch_type.as_ch_str(true)),
            ),
            UnknownFields::Propose => (
                ChangeClass::Attention,
                format!(
                    "new field {}, present in every record",
                    col.ch_type.as_ch_str(false)
                ),
            ),
            UnknownFields::Ignore => (ChangeClass::Info, "unknown field, ignored".to_string()),
            UnknownFields::Reject => (
                ChangeClass::Breaking,
                "unknown field (unknown_fields: reject)".to_string(),
            ),
        };
        let proposed = (contract.unknown_fields == UnknownFields::Propose).then(|| Field {
            name: col.name.clone(),
            ty: (&col.ch_type).into(),
            nullable: col.nullable,
            low_cardinality: col.low_cardinality,
            retired: false,
        });
        changes.push(Change {
            class,
            field: col.name.clone(),
            message,
            proposed,
        });
    }

    changes
}

/// Which attention changes `next_version` may apply.
pub enum Accept<'a> {
    Fields(&'a [String]),
    All,
}

pub struct NextVersion {
    /// `None` when there is nothing to apply.
    pub contract: Option<Contract>,
    /// Fields whose attention change was left out because it wasn't accepted.
    pub skipped: Vec<String>,
    /// New fields present in every record that were added as nullable, not accepted.
    pub added_nullable: Vec<String>,
}

/// The next version of `contract`, with `changes` (from `check`) applied: every
/// compatible change, and attention changes whose field is accepted. A new field that
/// isn't accepted is still added, as nullable. Breaking changes are never applied;
/// callers refuse to apply anything while one is present.
///
/// Errors when an accepted field has no attention change.
pub fn next_version(
    contract: &Contract,
    changes: &[Change],
    accept: Accept,
) -> Result<NextVersion, String> {
    let needs_attention = |name: &str| {
        changes
            .iter()
            .any(|c| c.class == ChangeClass::Attention && c.field == name)
    };
    if let Accept::Fields(names) = accept
        && let Some(name) = names.iter().find(|n| !needs_attention(n))
    {
        return Err(format!(
            "--accept {name}: no change to `{name}` needs attention"
        ));
    }
    let accepted = |name: &str| match accept {
        Accept::Fields(names) => names.iter().any(|n| n == name),
        Accept::All => true,
    };

    let mut next = contract.clone();
    let mut applied = false;
    let mut skipped = Vec::new();
    let mut added_nullable = Vec::new();
    for change in changes {
        let Some(proposed) = &change.proposed else {
            continue;
        };
        let existing = next.fields.iter_mut().find(|f| f.name == proposed.name);
        let mut proposed = proposed.clone();
        if change.class == ChangeClass::Attention && !accepted(&change.field) {
            if existing.is_some() {
                skipped.push(change.field.clone());
                continue;
            }
            // A new field is always safe to add as nullable; accepting it makes it required.
            proposed.nullable = true;
            added_nullable.push(change.field.clone());
        }
        match existing {
            Some(field) => *field = proposed,
            None => next.fields.push(proposed),
        }
        applied = true;
    }

    next.version += 1;
    Ok(NextVersion {
        contract: applied.then_some(next),
        skipped,
        added_nullable,
    })
}

/// Classifies every hand edit from the deployed contract `old` to `new`, for the
/// report of `contract diff`; every edit is applied, whatever its class.
///
/// Errors on edits a migration can't make: a field deleted instead of retired, a
/// changed table, target, cluster, kafka, engine, ORDER BY or SUM columns, a type or
/// nullability change on an ORDER BY field, or a new field that starts retired.
pub fn diff(old: &Contract, new: &Contract) -> Result<Vec<Change>, String> {
    let settings = [
        ("table", old.table != new.table),
        ("target", old.target != new.target),
        ("cluster", old.cluster != new.cluster),
        ("kafka", old.kafka != new.kafka),
        ("engine", old.engine != new.engine),
        ("order_by", old.order_by != new.order_by),
        ("sum_columns", old.sum_columns != new.sum_columns),
    ];
    if let Some((setting, _)) = settings.iter().find(|(_, changed)| *changed) {
        return Err(format!(
            "`{setting}` changed; that needs a new table or pipeline, not a migration"
        ));
    }
    if let Some(deleted) = old
        .fields
        .iter()
        .find(|f| !new.fields.iter().any(|n| n.name == f.name))
    {
        return Err(format!(
            "field `{}` was deleted; mark it \"retired\": true instead (its column is kept)",
            deleted.name
        ));
    }

    let mut changes = Vec::new();
    for field in &new.fields {
        let ty: ColumnType = (&field.ty).into();
        let Some(old_field) = old.fields.iter().find(|f| f.name == field.name) else {
            if field.retired {
                return Err(format!("new field `{}` can't start retired", field.name));
            }
            let (class, message) = if field.nullable {
                (
                    ChangeClass::Compatible,
                    format!("new field {}", field.column().ch_type_str()),
                )
            } else {
                (
                    ChangeClass::Attention,
                    format!(
                        "new required field {}; existing rows get its default",
                        field.column().ch_type_str()
                    ),
                )
            };
            changes.push(Change {
                class,
                field: field.name.clone(),
                message,
                proposed: None,
            });
            continue;
        };

        if (old_field.ty != field.ty || old_field.nullable != field.nullable)
            && old.order_by.contains(&field.name)
        {
            return Err(format!(
                "`{}` is in ORDER BY; ClickHouse can't change its type or nullability",
                field.name
            ));
        }
        let mut found: Vec<(ChangeClass, String)> = Vec::new();
        if old_field.ty != field.ty {
            let old_ty: ColumnType = (&old_field.ty).into();
            let (from, to) = (old_ty.as_str(), ty.as_str());
            found.push(if merge_types(&old_ty, &ty) == ty {
                (
                    ChangeClass::Attention,
                    format!("{from} -> {to} (rewrites the column)"),
                )
            } else if is_numeric_widening(&ty, &old_ty) {
                (
                    ChangeClass::Breaking,
                    format!("{from} -> {to}; values that don't fit are truncated"),
                )
            } else {
                (
                    ChangeClass::Breaking,
                    format!("{from} -> {to}; the mutation fails if any value doesn't convert"),
                )
            });
        }
        match (old_field.nullable, field.nullable) {
            (false, true) => found.push((ChangeClass::Attention, "becomes nullable".to_string())),
            (true, false) => found.push((
                ChangeClass::Breaking,
                "becomes required; existing NULLs become defaults".to_string(),
            )),
            _ => {}
        }
        if old_field.low_cardinality != field.low_cardinality {
            let state = if field.low_cardinality { "on" } else { "off" };
            found.push((ChangeClass::Info, format!("LowCardinality {state}")));
        }
        match (old_field.retired, field.retired) {
            (false, true) => found.push((
                ChangeClass::Info,
                "retired: no longer extracted; its column is kept".to_string(),
            )),
            (true, false) => found.push((
                ChangeClass::Compatible,
                "revived: extracted again".to_string(),
            )),
            _ => {}
        }

        let Some(class) = found.iter().map(|(class, _)| *class).max() else {
            continue;
        };
        changes.push(Change {
            class,
            field: field.name.clone(),
            message: found
                .into_iter()
                .map(|(_, m)| m)
                .collect::<Vec<_>>()
                .join("; "),
            proposed: None,
        });
    }

    if old.unknown_fields != new.unknown_fields {
        // Serializing a unit enum variant can't fail.
        let name = |u: &UnknownFields| serde_json::to_string(u).expect("serializes");
        changes.push(Change {
            class: ChangeClass::Info,
            field: "unknown_fields".to_string(),
            message: format!(
                "{} -> {}",
                name(&old.unknown_fields),
                name(&new.unknown_fields)
            ),
            proposed: None,
        });
    }
    Ok(changes)
}

enum TypeFit {
    Fits,
    /// Fits once integers widen the column to this type.
    Widens(ColumnType),
    Breaks,
}

/// Whether values of type `observed` fit a column of type `expected`.
fn fit_type(expected: &ColumnType, observed: &ColumnType) -> TypeFit {
    let widened = merge_types(expected, observed);
    if &widened == expected {
        TypeFit::Fits
    } else if is_numeric_widening(expected, &widened) {
        TypeFit::Widens(widened)
    } else {
        TypeFit::Breaks
    }
}

/// True when `to` only differs from `from` by integers widening to `Float64`.
pub fn is_numeric_widening(from: &ColumnType, to: &ColumnType) -> bool {
    match (from, to) {
        (ColumnType::Int64 | ColumnType::UInt64, ColumnType::Float64) => true,
        (ColumnType::Array(a), ColumnType::Array(b))
        | (ColumnType::Map(_, a), ColumnType::Map(_, b)) => a == b || is_numeric_widening(a, b),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::infer_schema;

    fn engine(order_by: &[&str]) -> EngineConfig {
        EngineConfig {
            engine: TableEngine::MergeTree,
            order_by: order_by.iter().map(|s| s.to_string()).collect(),
            sum_columns: vec![],
        }
    }

    /// A table contract inferred from `json`, with `order_by` as its sorting key.
    fn contract(json: &str, order_by: &[&str]) -> Contract {
        let schema = infer_schema(json, "t").unwrap();
        Contract::from_schema(&schema, Target::Table, None, None, engine(order_by))
    }

    fn changes(contract: &Contract, json: &str) -> Vec<(ChangeClass, String)> {
        let sample = infer_schema(json, "t").unwrap();
        check(contract, &sample)
            .into_iter()
            .map(|c| (c.class, c.field))
            .collect()
    }

    fn next(contract: &Contract, json: &str, accept: Accept) -> Result<NextVersion, String> {
        let sample = infer_schema(json, "t").unwrap();
        next_version(contract, &check(contract, &sample), accept)
    }

    fn field<'a>(contract: &'a Contract, name: &str) -> &'a Field {
        contract.fields.iter().find(|f| f.name == name).unwrap()
    }

    #[test]
    fn next_version_adds_compatible_field_and_bumps_version() {
        let mut c = contract(r#"[{"a":1}]"#, &[]);
        c.unknown_fields = UnknownFields::Propose;
        let n = next(&c, "{\"a\":1,\"b\":\"x\"}\n{\"a\":2}", Accept::Fields(&[])).unwrap();
        let n = n.contract.expect("a next version");
        assert_eq!(n.version, 2);
        assert_eq!(n.fields.len(), 2);
        assert_eq!(field(&n, "b").ty, FieldType::String);
        assert!(field(&n, "b").nullable);
    }

    #[test]
    fn next_version_keeps_settings_and_untouched_fields() {
        let mut c = contract(r#"[{"a":1,"z":true}]"#, &["a"]);
        c.cluster = Some("ck".to_string());
        c.fields[1].low_cardinality = true;
        let n = next(
            &c,
            "{\"a\":1,\"z\":true,\"b\":1}\n{\"a\":1,\"z\":false}",
            Accept::Fields(&[]),
        )
        .unwrap()
        .contract
        .unwrap();
        assert_eq!(n.cluster, c.cluster);
        assert_eq!(n.order_by, c.order_by);
        assert_eq!(n.unknown_fields, c.unknown_fields);
        assert_eq!(n.fields[..2], c.fields[..]);
    }

    #[test]
    fn next_version_is_none_without_changes() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let n = next(&c, r#"[{"a":2}]"#, Accept::Fields(&[])).unwrap();
        assert!(n.contract.is_none());
        assert!(n.skipped.is_empty());
    }

    #[test]
    fn next_version_skips_unaccepted_widening() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let n = next(&c, r#"[{"a":1.5}]"#, Accept::Fields(&[])).unwrap();
        assert!(n.contract.is_none());
        assert_eq!(n.skipped, vec!["a".to_string()]);
    }

    #[test]
    fn next_version_applies_accepted_widening() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let n = next(&c, r#"[{"a":1.5}]"#, Accept::Fields(&["a".to_string()]))
            .unwrap()
            .contract
            .unwrap();
        assert_eq!(field(&n, "a").ty, FieldType::Float64);
        assert!(!field(&n, "a").nullable);
    }

    #[test]
    fn next_version_combines_widening_and_nullability() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let n = next(&c, "{\"a\":1.5}\n{}", Accept::All)
            .unwrap()
            .contract
            .unwrap();
        assert_eq!(field(&n, "a").ty, FieldType::Float64);
        assert!(field(&n, "a").nullable);
    }

    #[test]
    fn next_version_adds_unaccepted_required_field_as_nullable() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let n = next(&c, r#"[{"a":1,"b":"x"}]"#, Accept::Fields(&[])).unwrap();
        assert!(n.skipped.is_empty());
        assert_eq!(n.added_nullable, vec!["b".to_string()]);
        assert!(field(&n.contract.unwrap(), "b").nullable);
    }

    #[test]
    fn next_version_adds_accepted_required_field_as_required() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let n = next(
            &c,
            r#"[{"a":1,"b":"x"}]"#,
            Accept::Fields(&["b".to_string()]),
        )
        .unwrap();
        assert!(n.added_nullable.is_empty());
        let n = n.contract.unwrap();
        assert!(!field(&n, "b").nullable);
    }

    #[test]
    fn next_version_applies_compatible_and_skips_unaccepted() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let n = next(
            &c,
            "{\"a\":1.5,\"b\":\"x\"}\n{\"a\":2}",
            Accept::Fields(&[]),
        )
        .unwrap();
        assert_eq!(n.skipped, vec!["a".to_string()]);
        let n = n.contract.unwrap();
        assert_eq!(field(&n, "a").ty, FieldType::Int64);
        assert!(field(&n, "b").nullable);
    }

    #[test]
    fn next_version_rejects_accepting_a_field_without_attention_change() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let err = next(
            &c,
            r#"[{"a":1.5}]"#,
            Accept::Fields(&["a_typo".to_string()]),
        )
        .err()
        .expect("an error");
        assert!(err.contains("a_typo"), "{err}");
    }

    #[test]
    fn next_version_ignores_info_changes() {
        let mut c = contract(r#"[{"a":1}]"#, &[]);
        c.unknown_fields = UnknownFields::Ignore;
        let n = next(&c, r#"[{"a":1,"b":2}]"#, Accept::All).unwrap();
        assert!(n.contract.is_none());
    }

    #[test]
    fn retired_field_in_sample_is_info() {
        let mut c = contract(r#"[{"a":1,"b":2}]"#, &[]);
        c.fields[1].retired = true;
        assert_eq!(
            changes(&c, r#"[{"a":1,"b":"now a string"}]"#),
            vec![(ChangeClass::Info, "b".to_string())]
        );
    }

    #[test]
    fn required_retired_field_absent_from_sample_is_no_change() {
        let mut c = contract(r#"[{"a":1,"b":2}]"#, &[]);
        c.fields[1].retired = true;
        assert!(changes(&c, r#"[{"a":1}]"#).is_empty());
    }

    #[test]
    fn schema_leaves_out_retired_fields() {
        let mut c = contract(r#"[{"a":1,"b":2}]"#, &[]);
        c.fields[0].retired = true;
        let names: Vec<_> = c.schema().columns.into_iter().map(|c| c.name).collect();
        assert_eq!(names, vec!["b".to_string()]);
    }

    #[test]
    fn retired_is_written_only_when_set() {
        let mut c = contract(r#"[{"a":1,"b":2}]"#, &[]);
        c.fields[1].retired = true;
        let v: serde_json::Value = serde_json::from_str(&c.to_json()).unwrap();
        assert!(v["fields"][0].get("retired").is_none());
        assert_eq!(v["fields"][1]["retired"], true);
        assert_eq!(Contract::from_json(&c.to_json()).unwrap(), c);
    }

    /// `old`'s edits as (class, field), or the error.
    fn diff_of(
        old: &Contract,
        edit: impl FnOnce(&mut Contract),
    ) -> Result<Vec<(ChangeClass, String)>, String> {
        let mut new = old.clone();
        new.version += 1;
        edit(&mut new);
        diff(old, &new).map(|cs| cs.into_iter().map(|c| (c.class, c.field)).collect())
    }

    fn new_field(name: &str, ty: FieldType, nullable: bool) -> Field {
        Field {
            name: name.to_string(),
            ty,
            nullable,
            low_cardinality: false,
            retired: false,
        }
    }

    #[test]
    fn diff_of_identical_contracts_is_empty() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        assert_eq!(diff_of(&c, |_| {}), Ok(vec![]));
    }

    #[test]
    fn diff_rejects_deleted_field() {
        let c = contract(r#"[{"a":1,"b":2}]"#, &[]);
        let err = diff_of(&c, |n| {
            n.fields.pop();
        })
        .unwrap_err();
        assert!(err.contains("`b`") && err.contains("retired"), "{err}");
    }

    #[test]
    fn diff_rejects_setting_changes() {
        let c = contract(r#"[{"a":1,"b":2}]"#, &["a"]);
        type Edit = fn(&mut Contract);
        let edits: Vec<(&str, Edit)> = vec![
            ("table", |n| n.table = "other".to_string()),
            ("target", |n| n.target = Target::Kafka),
            ("cluster", |n| n.cluster = Some("ck".to_string())),
            ("kafka", |n| n.kafka = Some("k".to_string())),
            ("engine", |n| n.engine = TableEngine::ReplacingMergeTree),
            ("order_by", |n| n.order_by = vec!["b".to_string()]),
            ("sum_columns", |n| n.sum_columns = vec!["b".to_string()]),
        ];
        for (setting, edit) in edits {
            let err = diff_of(&c, edit).unwrap_err();
            assert!(err.contains(setting), "{setting}: {err}");
        }
    }

    #[test]
    fn diff_rejects_type_change_on_order_by_field() {
        let c = contract(r#"[{"a":1}]"#, &["a"]);
        let err = diff_of(&c, |n| n.fields[0].ty = FieldType::Float64).unwrap_err();
        assert!(err.contains("ORDER BY"), "{err}");
        let err = diff_of(&c, |n| n.fields[0].nullable = true).unwrap_err();
        assert!(err.contains("ORDER BY"), "{err}");
    }

    #[test]
    fn diff_rejects_new_field_that_starts_retired() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let err = diff_of(&c, |n| {
            let mut b = new_field("b", FieldType::String, true);
            b.retired = true;
            n.fields.push(b);
        })
        .unwrap_err();
        assert!(err.contains("`b`"), "{err}");
    }

    #[test]
    fn diff_classifies_added_fields() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let result = diff_of(&c, |n| {
            n.fields.push(new_field("b", FieldType::String, true));
            n.fields.push(new_field("c", FieldType::Int64, false));
        });
        assert_eq!(
            result,
            Ok(vec![
                (ChangeClass::Compatible, "b".to_string()),
                (ChangeClass::Attention, "c".to_string()),
            ])
        );
    }

    #[test]
    fn diff_classifies_retiring_and_reviving() {
        let c = contract(r#"[{"a":1,"b":2}]"#, &[]);
        assert_eq!(
            diff_of(&c, |n| n.fields[1].retired = true),
            Ok(vec![(ChangeClass::Info, "b".to_string())])
        );
        let mut retired = c.clone();
        retired.fields[1].retired = true;
        assert_eq!(
            diff_of(&retired, |n| n.fields[1].retired = false),
            Ok(vec![(ChangeClass::Compatible, "b".to_string())])
        );
    }

    #[test]
    fn diff_classifies_type_changes() {
        let c = contract(r#"[{"a":1,"s":"x","f":1.5}]"#, &[]);
        assert_eq!(
            diff_of(&c, |n| {
                n.fields[0].ty = FieldType::Float64; // widening
                n.fields[1].ty = FieldType::Int64; // String -> Int64 may not convert
                n.fields[2].ty = FieldType::Int64; // narrowing truncates
            }),
            Ok(vec![
                (ChangeClass::Attention, "a".to_string()),
                (ChangeClass::Breaking, "s".to_string()),
                (ChangeClass::Breaking, "f".to_string()),
            ])
        );
    }

    #[test]
    fn diff_classifies_nullability_changes() {
        let c = contract("{\"a\":1,\"b\":2}\n{\"a\":1}", &[]); // b nullable
        assert_eq!(
            diff_of(&c, |n| {
                n.fields[0].nullable = true;
                n.fields[1].nullable = false;
            }),
            Ok(vec![
                (ChangeClass::Attention, "a".to_string()),
                (ChangeClass::Breaking, "b".to_string()),
            ])
        );
    }

    #[test]
    fn diff_classifies_low_cardinality_and_unknown_fields_as_info() {
        let c = contract(r#"[{"a":"x"}]"#, &[]);
        assert_eq!(
            diff_of(&c, |n| {
                n.fields[0].low_cardinality = true;
                n.unknown_fields = UnknownFields::Ignore;
            }),
            Ok(vec![
                (ChangeClass::Info, "a".to_string()),
                (ChangeClass::Info, "unknown_fields".to_string()),
            ])
        );
    }

    #[test]
    fn from_schema_starts_at_version_1() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        assert_eq!(c.version, 1);
        assert_eq!(c.table, "t");
        assert_eq!(c.unknown_fields, UnknownFields::Propose);
        assert_eq!(c.fields.len(), 1);
        assert_eq!(c.fields[0].ty, FieldType::Int64);
        assert!(!c.fields[0].nullable);
    }

    #[test]
    fn json_round_trips() {
        let c = contract(
            r#"[{"a":1,"tags":["x"],"attrs":{"k":1.5},"b":null}]"#,
            &["a"],
        );
        assert_eq!(Contract::from_json(&c.to_json()).unwrap(), c);
    }

    #[test]
    fn json_spells_types_as_tagged_objects() {
        let json = contract(r#"[{"a":1,"tags":[1]}]"#, &[]).to_json();
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            v["fields"][0],
            serde_json::json!({"name": "a", "type": "Int64", "nullable": false})
        );
        assert_eq!(v["fields"][1]["of"], serde_json::json!({"type": "Int64"}));
        assert_eq!(v["target"], "table");
        assert_eq!(v["engine"], "MergeTree");
    }

    #[test]
    fn from_json_rejects_unknown_top_level_key() {
        let mut v: serde_json::Value =
            serde_json::from_str(&contract(r#"[{"a":1}]"#, &[]).to_json()).unwrap();
        v["oder_by"] = serde_json::json!([]);
        assert!(Contract::from_json(&v.to_string()).is_err());
    }

    #[test]
    fn from_json_requires_cluster_and_kafka_for_kafka_target() {
        let mut c = contract(r#"[{"a":1}]"#, &[]);
        c.target = Target::Kafka;
        let err = Contract::from_json(&c.to_json()).unwrap_err();
        assert!(err.contains("cluster"), "{err}");
    }

    #[test]
    fn identical_sample_has_no_changes() {
        let c = contract(r#"[{"a":1,"b":"x"}]"#, &[]);
        assert!(changes(&c, r#"[{"a":2,"b":"y"}]"#).is_empty());
    }

    #[test]
    fn new_nullable_field_is_compatible() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        assert_eq!(
            changes(&c, "{\"a\":1,\"b\":\"x\"}\n{\"a\":2}"),
            vec![(ChangeClass::Compatible, "b".to_string())]
        );
    }

    #[test]
    fn new_always_present_field_needs_attention() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        assert_eq!(
            changes(&c, r#"[{"a":1,"b":"x"}]"#),
            vec![(ChangeClass::Attention, "b".to_string())]
        );
    }

    #[test]
    fn nullable_field_absent_from_sample_is_no_change() {
        let c = contract("{\"a\":1,\"b\":2}\n{\"a\":1}", &[]);
        assert!(changes(&c, r#"[{"a":1}]"#).is_empty());
    }

    #[test]
    fn required_field_absent_from_sample_is_breaking() {
        let c = contract(r#"[{"a":1,"b":2}]"#, &[]);
        assert_eq!(
            changes(&c, r#"[{"a":1}]"#),
            vec![(ChangeClass::Breaking, "b".to_string())]
        );
    }

    #[test]
    fn required_field_sometimes_missing_needs_attention() {
        let c = contract(r#"[{"a":1,"b":2}]"#, &[]);
        assert_eq!(
            changes(&c, "{\"a\":1,\"b\":2}\n{\"a\":1,\"b\":null}"),
            vec![(ChangeClass::Attention, "b".to_string())]
        );
    }

    #[test]
    fn nullable_field_always_present_is_no_change() {
        let c = contract("{\"a\":1}\n{}", &[]);
        assert!(changes(&c, r#"[{"a":1},{"a":2}]"#).is_empty());
    }

    #[test]
    fn messages_describe_the_sample_not_an_action() {
        // `diff` reports these too, and it can neither accept nor modify.
        let c = contract(r#"[{"a":1,"r":1}]"#, &[]);
        let sample = infer_schema(
            "{\"a\":1.5,\"r\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}",
            "t",
        )
        .unwrap();
        let changes = check(&c, &sample);
        assert_eq!(changes.len(), 3, "{changes:?}");
        for change in changes {
            for phrase in ["accept", "until", "becomes", "added as"] {
                assert!(
                    !change.message.contains(phrase),
                    "{}: {}",
                    change.field,
                    change.message
                );
            }
        }
    }

    #[test]
    fn int_to_float_needs_attention() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let sample = infer_schema(r#"[{"a":1.5}]"#, "t").unwrap();
        let result = check(&c, &sample);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].class, ChangeClass::Attention);
        assert!(
            result[0].message.contains("Float64"),
            "{}",
            result[0].message
        );
    }

    #[test]
    fn int64_to_uint64_widens_to_float64() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        let sample = infer_schema(r#"[{"a":18446744073709551615}]"#, "t").unwrap();
        let result = check(&c, &sample);
        assert_eq!(result[0].class, ChangeClass::Attention);
        assert!(
            result[0].message.contains("Float64"),
            "{}",
            result[0].message
        );
    }

    #[test]
    fn float_contract_accepts_ints() {
        let c = contract(r#"[{"a":1.5}]"#, &[]);
        assert!(changes(&c, r#"[{"a":1}]"#).is_empty());
    }

    #[test]
    fn string_contract_accepts_any_type() {
        let c = contract(r#"[{"a":"x"}]"#, &[]);
        assert!(changes(&c, r#"[{"a":5}]"#).is_empty());
    }

    #[test]
    fn incompatible_type_is_breaking() {
        let c = contract(r#"[{"a":1}]"#, &[]);
        assert_eq!(
            changes(&c, r#"[{"a":"abc"}]"#),
            vec![(ChangeClass::Breaking, "a".to_string())]
        );
    }

    #[test]
    fn widening_inside_array_needs_attention() {
        let c = contract(r#"[{"a":[1]}]"#, &[]);
        assert_eq!(
            changes(&c, r#"[{"a":[1.5]}]"#),
            vec![(ChangeClass::Attention, "a".to_string())]
        );
    }

    #[test]
    fn attention_on_order_by_column_is_breaking() {
        let c = contract(r#"[{"a":1,"b":2}]"#, &["a"]);
        assert_eq!(
            changes(&c, r#"[{"a":1.5,"b":2.5}]"#),
            vec![
                (ChangeClass::Breaking, "a".to_string()),
                (ChangeClass::Attention, "b".to_string()),
            ]
        );
    }

    #[test]
    fn field_only_ever_null_in_sample_skips_type_check() {
        let c = contract("{\"a\":1,\"b\":5}\n{\"a\":1}", &[]);
        assert!(changes(&c, r#"[{"a":1,"b":null}]"#).is_empty());
    }

    #[test]
    fn low_cardinality_difference_is_not_reported() {
        let mut c = contract(r#"[{"a":"x"}]"#, &[]);
        c.fields[0].low_cardinality = true;
        assert!(changes(&c, r#"[{"a":"y"}]"#).is_empty());
    }

    #[test]
    fn unknown_field_ignore_is_info() {
        let mut c = contract(r#"[{"a":1}]"#, &[]);
        c.unknown_fields = UnknownFields::Ignore;
        assert_eq!(
            changes(&c, r#"[{"a":1,"b":2}]"#),
            vec![(ChangeClass::Info, "b".to_string())]
        );
    }

    #[test]
    fn unknown_field_reject_is_breaking() {
        let mut c = contract(r#"[{"a":1}]"#, &[]);
        c.unknown_fields = UnknownFields::Reject;
        assert_eq!(
            changes(&c, "{\"a\":1,\"b\":2}\n{\"a\":1}"),
            vec![(ChangeClass::Breaking, "b".to_string())]
        );
    }
}
