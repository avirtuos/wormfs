//! Configuration provider trait and registry for automatic config display.
//!
//! This module provides a trait-based system for automatically including
//! component configurations in the Admin UI without manual updates.

use serde_json::{json, Value};
use std::collections::HashMap;

/// Trait for providing component configuration with descriptions.
///
/// Components implement this trait to make their configuration automatically
/// available in the Admin UI Config tab without requiring manual updates to
/// the config handler.
pub trait ConfigProvider {
    /// Returns the configuration name (e.g., "admin", "metrics", "network")
    fn name(&self) -> &'static str;

    /// Returns the configuration values with their descriptions.
    ///
    /// The returned `ConfigWithDescriptions` contains:
    /// - `values`: The actual configuration values as JSON
    /// - `descriptions`: Human-readable descriptions for each field
    fn get_config_with_descriptions(&self) -> ConfigWithDescriptions;
}

/// Configuration values with field descriptions.
#[derive(Debug, Clone)]
pub struct ConfigWithDescriptions {
    /// Configuration values as JSON
    pub values: Value,

    /// Field descriptions (field_name -> description)
    pub descriptions: HashMap<String, String>,
}

impl ConfigWithDescriptions {
    /// Create a new config with descriptions
    pub fn new(values: Value, descriptions: HashMap<String, String>) -> Self {
        Self {
            values,
            descriptions,
        }
    }
}

/// Registry for all configuration providers.
///
/// The registry collects configurations from all registered components
/// and provides a unified JSON response for the Admin UI.
pub struct ConfigRegistry {
    providers: Vec<Box<dyn ConfigProvider + Send + Sync>>,
}

impl ConfigRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    /// Register a configuration provider
    pub fn register<P: ConfigProvider + Send + Sync + 'static>(&mut self, provider: P) {
        self.providers.push(Box::new(provider));
    }

    /// Get all configurations as a unified JSON response.
    ///
    /// Returns JSON in the format expected by the Admin UI:
    /// ```json
    /// {
    ///   "component_name": {
    ///     "values": { ... },
    ///     "descriptions": { ... }
    ///   }
    /// }
    /// ```
    pub fn get_all_configs(&self) -> Value {
        let mut config_json = serde_json::Map::new();

        for provider in &self.providers {
            let name = provider.name();
            let config_with_desc = provider.get_config_with_descriptions();

            config_json.insert(
                name.to_string(),
                json!({
                    "values": config_with_desc.values,
                    "descriptions": config_with_desc.descriptions,
                }),
            );
        }

        Value::Object(config_json)
    }
}

impl Default for ConfigRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestConfigProvider;

    impl ConfigProvider for TestConfigProvider {
        fn name(&self) -> &'static str {
            "test"
        }

        fn get_config_with_descriptions(&self) -> ConfigWithDescriptions {
            let values = json!({
                "enabled": true,
                "port": 9090,
            });

            let mut descriptions = HashMap::new();
            descriptions.insert("enabled".to_string(), "Enable the test feature".to_string());
            descriptions.insert("port".to_string(), "Port number".to_string());

            ConfigWithDescriptions::new(values, descriptions)
        }
    }

    #[test]
    fn test_config_registry() {
        let mut registry = ConfigRegistry::new();
        registry.register(TestConfigProvider);

        let all_configs = registry.get_all_configs();

        assert!(all_configs.is_object());
        assert!(all_configs["test"].is_object());
        assert!(all_configs["test"]["values"].is_object());
        assert!(all_configs["test"]["descriptions"].is_object());
    }
}
