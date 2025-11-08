//! Test fixtures for integration tests
//!
//! This module provides utilities for managing test data files
//! and validating parsing results.

use std::path::PathBuf;

/// Test file metadata for validation
#[derive(Debug)]
pub struct TestFileMetadata {
    pub filename: &'static str,
    pub description: &'static str,
    pub expected_rows: Option<usize>,
    pub expected_cols: Option<usize>,
    pub min_rows: Option<usize>,
    pub min_cols: Option<usize>,
    pub sha256: Option<&'static str>,
}

impl TestFileMetadata {
    /// Check if the test file exists
    pub fn exists(&self) -> bool {
        self.path().exists()
    }

    /// Get the full path to the test file
    pub fn path(&self) -> PathBuf {
        PathBuf::from("testlog").join(self.filename)
    }

    /// Read the test file contents
    pub fn read(&self) -> std::io::Result<Vec<u8>> {
        std::fs::read(self.path())
    }

    /// Validate SHA256 checksum if provided
    ///
    /// Note: Requires the `sha256-validation` feature and sha2 dependency
    #[allow(dead_code)]
    #[cfg(feature = "sha256-validation")]
    pub fn validate_checksum(&self, data: &[u8]) -> bool {
        use sha2::{Digest, Sha256};

        if let Some(expected_hash) = self.sha256 {
            let mut hasher = Sha256::new();
            hasher.update(data);
            let result = hasher.finalize();
            let actual_hash = format!("{:x}", result);
            actual_hash == expected_hash
        } else {
            true // No checksum to validate
        }
    }

    /// Skip test with message if file doesn't exist
    pub fn skip_if_missing(&self) {
        if !self.exists() {
            println!("\n⚠️  Skipping test - file not found: {}", self.filename);
            println!("   To run this test, place the file in: testlog/");
            println!("   See tests/README.md for more information.\n");
        }
    }
}

/// Known test files
pub mod test_files {
    use super::TestFileMetadata;

    /// Large real-world robotics log with struct arrays
    pub const AKIT_LOG: TestFileMetadata = TestFileMetadata {
        filename: "akit_25-03-21_17-17-36_txfor_q43.wpilog",
        description:
            "Real robotics log from FRC team with struct arrays, nested structs, and sparse data",
        expected_rows: Some(17577),
        expected_cols: Some(391),
        min_rows: Some(17000), // Allow some tolerance
        min_cols: Some(390),
        sha256: None, // TODO: Add checksum if desired
    };

    /// List all known test files
    pub fn all() -> Vec<TestFileMetadata> {
        vec![AKIT_LOG]
    }
}

/// Helper to check if any test files are available
pub fn has_test_files() -> bool {
    test_files::all().iter().any(|f| f.exists())
}

/// Print test data status
pub fn print_test_data_status() {
    println!("\n📊 Test Data Status:");
    println!("─────────────────────────────────────");

    for file in test_files::all() {
        let status = if file.exists() {
            "✅ Available"
        } else {
            "❌ Missing"
        };
        println!("{}: {}", status, file.filename);
        if file.exists() {
            if let Ok(metadata) = std::fs::metadata(file.path()) {
                println!("   Size: {:.2} MB", metadata.len() as f64 / 1_000_000.0);
            }
        }
    }

    println!("─────────────────────────────────────");

    if !has_test_files() {
        println!("\n💡 To enable large file tests:");
        println!("   1. Place .wpilog files in testlog/ directory");
        println!("   2. Run: cargo test -- --ignored --nocapture");
        println!("   See tests/README.md for details.\n");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_path() {
        let file = test_files::AKIT_LOG;
        let path = file.path();
        assert_eq!(
            path.to_str().unwrap(),
            "testlog/akit_25-03-21_17-17-36_txfor_q43.wpilog"
        );
    }

    #[test]
    fn test_file_listing() {
        let files = test_files::all();
        assert!(!files.is_empty());
    }
}
