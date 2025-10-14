//! # ReportGenerator
//!
//! Creates detailed test reports from test results.

use crate::worm_validator::types::{TestResults, TestStatus};
use std::io::Write;

/// Generates test reports in various formats.
pub struct ReportGenerator;

impl ReportGenerator {
    /// Create a new ReportGenerator.
    pub fn new() -> Self {
        Self
    }

    /// Generate a plain text report.
    ///
    /// # Arguments
    ///
    /// * `results` - Test results to report on
    ///
    /// # Returns
    ///
    /// Returns a formatted text report.
    pub fn generate_text_report(&self, results: &TestResults) -> String {
        let mut report = String::new();

        report.push_str("=====================================\n");
        report.push_str("      WormFS Validator Report\n");
        report.push_str("=====================================\n\n");

        report.push_str(&format!("Total Scenarios: {}\n", results.total_scenarios));
        report.push_str(&format!("Passed: {}\n", results.passed));
        report.push_str(&format!("Failed: {}\n", results.failed));
        report.push_str(&format!("Skipped: {}\n", results.skipped));
        report.push_str(&format!("Success Rate: {:.2}%\n", results.success_rate()));
        report.push_str(&format!(
            "Duration: {:.2}s\n\n",
            results.duration.as_secs_f64()
        ));

        report.push_str("Scenario Results:\n");
        report.push_str("-------------------------------------\n");

        for result in &results.scenario_results {
            let status_str = match result.status {
                TestStatus::Passed => "✓ PASS",
                TestStatus::Failed => "✗ FAIL",
                TestStatus::Skipped => "- SKIP",
            };

            report.push_str(&format!(
                "{} | {} | {} | {:.2}s\n",
                status_str,
                result.category,
                result.name,
                result.duration.as_secs_f64()
            ));

            if let Some(error) = &result.error {
                report.push_str(&format!("      Error: {}\n", error));
            }
        }

        report.push_str("\n=====================================\n");

        report
    }

    /// Generate a JSON report.
    ///
    /// # Arguments
    ///
    /// * `results` - Test results to report on
    ///
    /// # Returns
    ///
    /// Returns a JSON-formatted report.
    pub fn generate_json_report(&self, results: &TestResults) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(results)
    }

    /// Write a report to a file.
    ///
    /// # Arguments
    ///
    /// * `report` - Report content
    /// * `path` - Path to write to
    pub fn write_report<P: AsRef<std::path::Path>>(
        &self,
        report: &str,
        path: P,
    ) -> std::io::Result<()> {
        let mut file = std::fs::File::create(path)?;
        file.write_all(report.as_bytes())?;
        Ok(())
    }
}

impl Default for ReportGenerator {
    fn default() -> Self {
        Self::new()
    }
}
