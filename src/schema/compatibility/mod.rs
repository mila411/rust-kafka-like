use crate::schema::registry::Schema;
use serde_json::Value;

/// Enum representing the compatibility modes for schema evolution.
///
/// # Variants
///
/// * `BACKWARD` - Ensures that new schemas can read data written by old schemas.
/// * `FORWARD` - Ensures that old schemas can read data written by new schemas.
/// * `FULL` - Ensures both backward and forward compatibility.
/// * `NONE` - No compatibility checks.
#[derive(Debug, Clone, Copy)]
pub enum Compatibility {
    BACKWARD,
    FORWARD,
    FULL,
    NONE,
}

impl Compatibility {
    /// Checks the compatibility between a new schema and an old schema.
    ///
    /// # Arguments
    ///
    /// * `new_schema` - The new schema to be checked.
    /// * `old_schema` - The existing schema to check against.
    ///
    /// # Returns
    ///
    /// * `true` if the schemas are compatible according to the specified mode.
    /// * `false` otherwise.
    pub fn check(&self, new_schema: &Schema, old_schema: &Schema) -> bool {
        match self {
            Compatibility::BACKWARD => Self::check_backward(new_schema, old_schema),
            Compatibility::FORWARD => Self::check_forward(new_schema, old_schema),
            Compatibility::FULL => {
                Self::check_backward(new_schema, old_schema)
                    && Self::check_forward(new_schema, old_schema)
            }
            Compatibility::NONE => true,
        }
    }

    /// Checks backward compatibility between a new schema and an old schema.
    ///
    /// # Arguments
    ///
    /// * `new_schema` - The new schema to be checked.
    /// * `old_schema` - The existing schema to check against.
    ///
    /// # Returns
    ///
    /// * `true` if the new schema is backward compatible with the old schema.
    /// * `false` otherwise.
    fn check_backward(new_schema: &Schema, old_schema: &Schema) -> bool {
        if let (Ok(new_json), Ok(old_json)) = (
            serde_json::from_str::<Value>(&new_schema.definition),
            serde_json::from_str::<Value>(&old_schema.definition),
        ) {
            // The new schema must contain all the fields of the old schema.
            Self::contains_all_required_fields(&new_json, &old_json)
        } else {
            false
        }
    }

    /// Checks forward compatibility between a new schema and an old schema.
    ///
    /// # Arguments
    ///
    /// * `new_schema` - The new schema to be checked.
    /// * `old_schema` - The existing schema to check against.
    ///
    /// # Returns
    ///
    /// * `true` if the new schema is forward compatible with the old schema.
    /// * `false` otherwise.
    fn check_forward(new_schema: &Schema, old_schema: &Schema) -> bool {
        if let (Ok(new_json), Ok(old_json)) = (
            serde_json::from_str::<Value>(&new_schema.definition),
            serde_json::from_str::<Value>(&old_schema.definition),
        ) {
            // New fields must be optional
            Self::new_fields_are_optional(&new_json, &old_json)
        } else {
            false
        }
    }

    /// Ensures that all required fields in the old schema are present in the new schema.
    ///
    /// # Arguments
    ///
    /// * `new_schema` - The new schema to be checked.
    /// * `old_schema` - The existing schema to check against.
    ///
    /// # Returns
    ///
    /// * `true` if all required fields are present.
    /// * `false` otherwise.
    fn contains_all_required_fields(new_schema: &Value, old_schema: &Value) -> bool {
        if let (Some(new_fields), Some(old_fields)) = (
            new_schema.get("fields").and_then(Value::as_array),
            old_schema.get("fields").and_then(Value::as_array),
        ) {
            old_fields.iter().all(|old_field| {
                let old_name = old_field.get("name").and_then(Value::as_str);
                new_fields.iter().any(|new_field| {
                    let new_name = new_field.get("name").and_then(Value::as_str);
                    old_name == new_name
                })
            })
        } else {
            true // If there is no field, it is determined to be compatible.
        }
    }

    /// Ensures that new fields in the new schema are optional.
    ///
    /// # Arguments
    ///
    /// * `new_schema` - The new schema to be checked.
    /// * `old_schema` - The existing schema to check against.
    ///
    /// # Returns
    ///
    /// * `true` if new fields are optional.
    /// * `false` otherwise.
    fn new_fields_are_optional(new_schema: &Value, old_schema: &Value) -> bool {
        if let (Some(new_fields), Some(old_fields)) = (
            new_schema.get("fields").and_then(Value::as_array),
            old_schema.get("fields").and_then(Value::as_array),
        ) {
            new_fields.iter().all(|new_field| {
                let new_name = new_field.get("name").and_then(Value::as_str);
                old_fields.iter().any(|old_field| {
                    let old_name = old_field.get("name").and_then(Value::as_str);
                    new_name == old_name || new_field.get("default").is_some()
                })
            })
        } else {
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backward_compatibility() {
        let old_schema = Schema {
            id: 1,
            version: crate::schema::version::SchemaVersion::new(1),
            definition: r#"{"type":"record","fields":[{"name":"id","type":"string"}]}"#.to_string(),
        };

        let new_schema = Schema {
            id: 2,
            version: crate::schema::version::SchemaVersion::new(2),
            definition: r#"{"type":"record","fields":[{"name":"id","type":"string"},{"name":"value","type":"string","default":""}]}"#.to_string(),
        };

        let compatibility = Compatibility::BACKWARD;
        assert!(compatibility.check(&new_schema, &old_schema));
    }
}
