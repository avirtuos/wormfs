//! # WormValidator Component
//!
//! WormValidator is a standalone binary that provides integration testing capabilities
//! for WormFS by embedding a single-node storage cluster and acting as a simulated
//! FUSE client.
//!
//! ## Architecture
//!
//! The validator consists of several key components:
//! - **ClusterManager**: Bootstraps and manages the embedded storage cluster
//! - **FuseClientSimulator**: gRPC client that mimics FUSE operations
//! - **TestScenarioRunner**: Orchestrates test execution
//! - **ValidationEngine**: Verifies expected outcomes
//! - **ReportGenerator**: Creates detailed test reports
//!
//! ## Usage
//!
//! ```no_run
//! use wormfs::worm_validator::{WormValidator, ValidatorConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = ValidatorConfig::default();
//!     let mut validator = WormValidator::new(config).await?;
//!     let results = validator.run_all_tests().await;
//!     validator.cleanup().await?;
//!     Ok(())
//! }
//! ```

pub mod cluster_manager;
pub mod client_simulator;
pub mod report;
pub mod scenario_runner;
pub mod scenarios;
pub mod types;
pub mod validation;

use async_trait::async_trait;
pub use types::{TestResults, ValidatorConfig, ValidatorError};

/// WormValidator trait defines the interface for the integration testing system.
///
/// Implementations manage an embedded storage cluster and execute test scenarios
/// to validate end-to-end system behavior.
#[async_trait]
pub trait WormValidator: Send + Sync {
    /// Create and initialize a new WormValidator with the given configuration.
    ///
    /// This method sets up the validator but does not start the embedded cluster.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the validator and embedded cluster
    ///
    /// # Returns
    ///
    /// Returns `Ok(Self)` if initialization succeeds, or an error describing what failed.
    ///
    /// # Errors
    ///
    /// - Configuration validation errors
    /// - Temporary directory creation failures
    async fn new(config: ValidatorConfig) -> Result<Self, ValidatorError>
    where
        Self: Sized;

    /// Run all configured test scenarios.
    ///
    /// This method:
    /// - Starts the embedded storage cluster
    /// - Connects the client simulator
    /// - Executes all test scenarios (or filtered subset)
    /// - Collects results and metrics
    /// - Stops the cluster
    ///
    /// # Returns
    ///
    /// Returns a `TestResults` object containing the outcome of all scenarios.
    async fn run_all_tests(&mut self) -> TestResults;

    /// Run a specific set of test scenarios by name or category.
    ///
    /// # Arguments
    ///
    /// * `scenarios` - List of scenario names or categories to run
    ///
    /// # Returns
    ///
    /// Returns a `TestResults` object containing the outcome of the specified scenarios.
    async fn run_scenarios(&mut self, scenarios: &[String]) -> TestResults;

    /// Clean up test resources and temporary directories.
    ///
    /// This method:
    /// - Stops the embedded cluster if running
    /// - Removes temporary directories (unless keep_data is true)
    /// - Releases any held resources
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup cannot be completed. This is typically
    /// a non-fatal error that can be logged.
    async fn cleanup(&mut self) -> Result<(), ValidatorError>;

    /// Check if the embedded cluster is currently running.
    ///
    /// # Returns
    ///
    /// `true` if the cluster is running, `false` otherwise.
    fn is_cluster_running(&self) -> bool;

    /// Get the current validator configuration.
    ///
    /// # Returns
    ///
    /// A reference to the validator's configuration.
    fn get_config(&self) -> &ValidatorConfig;
}
