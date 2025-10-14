//! # TestScenarioRunner
//!
//! Orchestrates execution of test scenarios.

use crate::worm_validator::client_simulator::FuseClientSimulator;
use crate::worm_validator::types::{ScenarioResult, TestResults, TestStatus};
use async_trait::async_trait;
use std::time::{Duration, Instant};

/// Orchestrates test scenario execution.
pub struct TestScenarioRunner {
    /// List of scenarios to execute
    scenarios: Vec<Box<dyn TestScenario>>,
    /// Collected results
    results: Vec<ScenarioResult>,
}

impl TestScenarioRunner {
    /// Create a new TestScenarioRunner.
    pub fn new() -> Self {
        Self {
            scenarios: Vec::new(),
            results: Vec::new(),
        }
    }

    /// Load test scenarios based on an optional filter.
    ///
    /// # Arguments
    ///
    /// * `filter` - Optional list of scenario names or categories to load
    pub fn load_scenarios(&mut self, filter: Option<&[String]>) {
        // TODO: Implement scenario loading
        // 1. Instantiate all available scenarios
        // 2. Apply filter if provided
        // 3. Store in scenarios vector
        unimplemented!("TestScenarioRunner::load_scenarios")
    }

    /// Run all loaded scenarios.
    ///
    /// # Arguments
    ///
    /// * `client` - Client simulator to use for test operations
    ///
    /// # Returns
    ///
    /// Returns aggregated test results.
    pub async fn run_scenarios(&mut self, client: &mut FuseClientSimulator) -> TestResults {
        let start = Instant::now();
        self.results.clear();

        for scenario in &self.scenarios {
            let result = self.run_single_scenario(scenario.as_ref(), client).await;
            self.results.push(result);
        }

        let duration = start.elapsed();
        self.aggregate_results(duration)
    }

    /// Run a single test scenario.
    async fn run_single_scenario(
        &self,
        scenario: &dyn TestScenario,
        client: &mut FuseClientSimulator,
    ) -> ScenarioResult {
        let start = Instant::now();
        let result = scenario.execute(client).await;
        let duration = start.elapsed();

        ScenarioResult {
            name: scenario.name().to_string(),
            category: scenario.category().to_string(),
            status: if result.is_ok() {
                TestStatus::Passed
            } else {
                TestStatus::Failed
            },
            duration,
            error: result.err().map(|e| e.to_string()),
            metrics: Default::default(),
        }
    }

    /// Aggregate scenario results into a TestResults object.
    fn aggregate_results(&self, duration: Duration) -> TestResults {
        let total_scenarios = self.results.len();
        let passed = self
            .results
            .iter()
            .filter(|r| r.status == TestStatus::Passed)
            .count();
        let failed = self
            .results
            .iter()
            .filter(|r| r.status == TestStatus::Failed)
            .count();
        let skipped = self
            .results
            .iter()
            .filter(|r| r.status == TestStatus::Skipped)
            .count();

        TestResults {
            total_scenarios,
            passed,
            failed,
            skipped,
            duration,
            scenario_results: self.results.clone(),
        }
    }
}

impl Default for TestScenarioRunner {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for test scenarios.
///
/// Each scenario represents a specific test case or workflow to validate.
#[async_trait]
pub trait TestScenario: Send + Sync {
    /// Get the scenario name.
    fn name(&self) -> &str;

    /// Get the scenario category.
    fn category(&self) -> &str;

    /// Execute the test scenario.
    ///
    /// # Arguments
    ///
    /// * `client` - Client simulator for filesystem operations
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if the scenario passes, or an error describing the failure.
    async fn execute(&self, client: &mut FuseClientSimulator) -> Result<(), Box<dyn std::error::Error>>;
}
