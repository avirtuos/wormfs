//! # ValidationEngine
//!
//! Verifies expected outcomes and validates test results.

use crate::worm_validator::types::ValidatorError;

/// Validates test outcomes and verifies data integrity.
pub struct ValidationEngine;

impl ValidationEngine {
    /// Create a new ValidationEngine.
    pub fn new() -> Self {
        Self
    }

    /// Compare actual data with expected data.
    ///
    /// # Arguments
    ///
    /// * `actual` - Actual data
    /// * `expected` - Expected data
    ///
    /// # Returns
    ///
    /// Returns Ok(()) if data matches, or an error describing the mismatch.
    pub fn validate_data(&self, actual: &[u8], expected: &[u8]) -> Result<(), ValidatorError> {
        if actual == expected {
            Ok(())
        } else {
            Err(ValidatorError::TestScenarioFailed(format!(
                "Data mismatch: expected {} bytes, got {} bytes",
                expected.len(),
                actual.len()
            )))
        }
    }

    /// Validate data integrity using checksums.
    ///
    /// # Arguments
    ///
    /// * `data` - Data to validate
    /// * `expected_checksum` - Expected CRC32 checksum
    pub fn validate_checksum(&self, data: &[u8], expected_checksum: u32) -> Result<(), ValidatorError> {
        let checksum = crc32fast::hash(data);
        if checksum == expected_checksum {
            Ok(())
        } else {
            Err(ValidatorError::TestScenarioFailed(format!(
                "Checksum mismatch: expected {}, got {}",
                expected_checksum, checksum
            )))
        }
    }

    /// Validate that a condition is true.
    ///
    /// # Arguments
    ///
    /// * `condition` - Condition to validate
    /// * `message` - Error message if condition is false
    pub fn validate_condition(&self, condition: bool, message: &str) -> Result<(), ValidatorError> {
        if condition {
            Ok(())
        } else {
            Err(ValidatorError::TestScenarioFailed(message.to_string()))
        }
    }
}

impl Default for ValidationEngine {
    fn default() -> Self {
        Self::new()
    }
}
