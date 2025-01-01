use crate::schema::compatibility::Compatibility;
use crate::schema::version::SchemaVersion;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub id: u32,
    pub version: SchemaVersion,
    pub definition: String,
}

/// Schema Registry Implementation
/// Manage the schema version for each topic and perform compatibility checks.
pub struct SchemaRegistry {
    schemas: Arc<RwLock<HashMap<String, Vec<Schema>>>>,
    compatibility: Compatibility,
}

impl SchemaRegistry {
    /// Creates a new SchemaRegistry instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::registry::SchemaRegistry;
    ///
    /// let registry = SchemaRegistry::new();
    /// ```
    pub fn new() -> Self {
        SchemaRegistry {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            compatibility: Compatibility::BACKWARD,
        }
    }

    /// Registers a new schema for a given topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic.
    /// * `definition` - The schema definition as a JSON string.
    ///
    /// # Returns
    ///
    /// * `Ok(Schema)` if the schema is successfully registered.
    /// * `Err(String)` if the schema fails compatibility checks.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::registry::SchemaRegistry;
    ///
    /// let registry = SchemaRegistry::new();
    /// let schema_def = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;
    /// let result = registry.register_schema("test_topic", schema_def);
    /// assert!(result.is_ok());
    /// ```
    pub fn register_schema(&self, topic: &str, definition: &str) -> Result<Schema, String> {
        let mut schemas = self.schemas.write().unwrap();
        let topic_schemas = schemas.entry(topic.to_string()).or_default();

        let new_schema = Schema {
            id: topic_schemas.len() as u32,
            version: SchemaVersion::new(topic_schemas.len() as u32 + 1),
            definition: definition.to_string(),
        };

        if self.check_compatibility(&new_schema, topic_schemas) {
            topic_schemas.push(new_schema.clone());
            Ok(new_schema)
        } else {
            Err("The schema failed the compatibility check.".to_string())
        }
    }

    /// Checks the compatibility of a new schema with existing schemas.
    ///
    /// # Arguments
    ///
    /// * `new_schema` - The new schema to be checked.
    /// * `existing_schemas` - The list of existing schemas for the topic.
    ///
    /// # Returns
    ///
    /// * `true` if the new schema is compatible with the existing schemas.
    /// * `false` otherwise.
    fn check_compatibility(&self, new_schema: &Schema, existing_schemas: &[Schema]) -> bool {
        if existing_schemas.is_empty() {
            return true;
        }

        let latest_schema = existing_schemas.last().unwrap();
        self.compatibility.check(new_schema, latest_schema)
    }

    /// Retrieves a schema for a given topic and version.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic.
    /// * `version` - The version of the schema to retrieve (optional).
    ///
    /// # Returns
    ///
    /// * `Some(Schema)` if the schema is found.
    /// * `None` if the schema is not found.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::registry::SchemaRegistry;
    ///
    /// let registry = SchemaRegistry::new();
    /// let schema_def = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;
    /// registry.register_schema("test_topic", schema_def).unwrap();
    /// let schema = registry.get_schema("test_topic", Some(1));
    /// assert!(schema.is_some());
    /// ```
    pub fn get_schema(&self, topic: &str, version: Option<u32>) -> Option<Schema> {
        let schemas = self.schemas.read().unwrap();
        let topic_schemas = schemas.get(topic)?;

        match version {
            Some(v) => topic_schemas.iter().find(|s| s.version.major == v).cloned(),
            None => topic_schemas.last().cloned(),
        }
    }

    /// Sets the compatibility mode for the schema registry.
    ///
    /// # Arguments
    ///
    /// * `compatibility` - The compatibility mode to set.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::registry::SchemaRegistry;
    /// use pilgrimage::schema::compatibility::Compatibility;
    ///
    /// let mut registry = SchemaRegistry::new();
    /// registry.set_compatibility(Compatibility::FULL);
    /// ```
    pub fn set_compatibility(&mut self, compatibility: Compatibility) {
        self.compatibility = compatibility;
    }

    /// Retrieves all schemas for a given topic.
    ///
    /// # Arguments
    ///
    /// * `topic` - The name of the topic.
    ///
    /// # Returns
    ///
    /// * `Some(Vec<Schema>)` if schemas are found.
    /// * `None` if no schemas are found.
    ///
    /// # Examples
    ///
    /// ```
    /// use pilgrimage::schema::registry::SchemaRegistry;
    ///
    /// let registry = SchemaRegistry::new();
    /// let schema_def = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;
    /// registry.register_schema("test_topic", schema_def).unwrap();
    /// let schemas = registry.get_all_schemas("test_topic");
    /// assert!(schemas.is_some());
    /// ```
    pub fn get_all_schemas(&self, topic: &str) -> Option<Vec<Schema>> {
        let schemas = self.schemas.read().unwrap();
        schemas.get(topic).cloned()
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_registration() {
        let registry = SchemaRegistry::new();
        let schema_def =
            r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;

        let result = registry.register_schema("test_topic", schema_def);
        assert!(result.is_ok());

        let schema = result.unwrap();
        assert_eq!(schema.id, 0);
        assert_eq!(schema.version.major, 1);
    }

    #[test]
    fn test_schema_compatibility() {
        let registry = SchemaRegistry::new();

        // Register the initial schema
        let schema1 = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"}]}"#;
        let result1 = registry.register_schema("test_topic", schema1);
        assert!(result1.is_ok());

        // Add compatible schema
        let schema2 = r#"{"type":"record","name":"test","fields":[{"name":"id","type":"string"},{"name":"value","type":"string"}]}"#;
        let result2 = registry.register_schema("test_topic", schema2);
        assert!(result2.is_ok());
    }
}
