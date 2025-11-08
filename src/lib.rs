//! High-performance conversion of WPILog binary data to Polars DataFrames.
//!
//! This library provides a simple API for parsing WPILog files (binary datalog files from
//! WPILib robotics) and converting them directly to Polars DataFrames with zero intermediate
//! formats.
//!
//! # Features
//!
//! - Direct conversion to Polars DataFrames
//! - Support for all WPILog data types (scalars and arrays)
//! - Automatic sparse data handling (null-filling for missing values)
//! - Memory-mapped file I/O for maximum performance
//! - UTF-8 fallback for binary data marked as strings
//!
//! # Example
//!
//! ```no_run
//! use wpilog_polars::WpilogParser;
//!
//! // Parse from file (uses memory mapping)
//! let df = WpilogParser::from_file("robot.wpilog")?;
//! println!("{}", df);
//!
//! // Parse from bytes
//! let data = std::fs::read("robot.wpilog")?;
//! let df = WpilogParser::from_bytes(data)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Data Types
//!
//! The library supports the following WPILog data types:
//!
//! | WPILog Type | Polars Type |
//! |-------------|-------------|
//! | `double` | `Float64` |
//! | `float` | `Float32` |
//! | `int64` | `Int64` |
//! | `boolean` | `Boolean` |
//! | `string` | `String` |
//! | `double[]` | `List(Float64)` |
//! | `float[]` | `List(Float32)` |
//! | `int64[]` | `List(Int64)` |
//! | `boolean[]` | `List(Boolean)` |
//! | `string[]` | `List(String)` |
//!
//! # Sparse Data
//!
//! WPILog files contain sparse data where not every column is updated at every timestamp.
//! This library automatically handles this by filling missing values with nulls in the
//! resulting DataFrame.
//!
//! # Performance
//!
//! The library uses several techniques for high performance:
//! - Two-pass algorithm: schema inference then data accumulation
//! - Pre-allocated column builders with capacity estimation
//! - Memory-mapped file I/O for `from_file()`
//! - Zero-copy where possible
//! - Direct columnar format (no row-to-column conversion)

pub mod builders;
pub mod converter;
pub mod datalog;
pub mod error;
pub mod schema;
pub mod struct_support;
pub mod types;

pub use error::{Result, WpilogError};
pub use polars::prelude::DataFrame;

use converter::WpilogConverter;
use std::fs::File;
use std::path::Path;

/// Main entry point for parsing WPILog files.
pub struct WpilogParser;

impl WpilogParser {
    /// Parses a WPILog file from a byte vector and returns a Polars DataFrame.
    ///
    /// # Arguments
    ///
    /// * `data` - The WPILog file data as a byte vector
    ///
    /// # Returns
    ///
    /// A `Result` containing either:
    /// - A `DataFrame` with a "timestamp" column (Int64) followed by columns for each entry
    /// - A `WpilogError` if parsing fails
    ///
    /// # Example
    ///
    /// ```no_run
    /// use wpilog_polars::WpilogParser;
    ///
    /// let data = std::fs::read("robot.wpilog")?;
    /// let df = WpilogParser::from_bytes(data)?;
    /// println!("Parsed {} rows and {} columns", df.height(), df.width());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_bytes(data: Vec<u8>) -> Result<DataFrame> {
        WpilogConverter::from_bytes(&data)
    }

    /// Parses a WPILog file from disk using memory mapping for maximum performance.
    ///
    /// This method uses memory-mapped I/O which is significantly faster than reading
    /// the entire file into memory, especially for large files.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the WPILog file
    ///
    /// # Returns
    ///
    /// A `Result` containing either:
    /// - A `DataFrame` with a "timestamp" column (Int64) followed by columns for each entry
    /// - A `WpilogError` if parsing fails or the file cannot be opened
    ///
    /// # Example
    ///
    /// ```no_run
    /// use wpilog_polars::WpilogParser;
    ///
    /// let df = WpilogParser::from_file("robot.wpilog")?;
    /// println!("Parsed {} rows and {} columns", df.height(), df.width());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<DataFrame> {
        let file = File::open(path)?;
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        WpilogConverter::from_bytes(&mmap)
    }
}

/// Infers the schema of a WPILog file without parsing all the data.
///
/// This function only reads the START control records to determine the schema,
/// which is much faster than parsing the entire file.
///
/// # Arguments
///
/// * `data` - The WPILog file data as a byte slice
///
/// # Returns
///
/// A `Result` containing either:
/// - A Polars `Schema` describing the columns and their types
/// - A `WpilogError` if schema inference fails
///
/// # Example
///
/// ```no_run
/// use wpilog_polars::infer_schema;
///
/// let data = std::fs::read("robot.wpilog")?;
/// let schema = infer_schema(&data)?;
/// println!("Schema: {:?}", schema);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn infer_schema(data: &[u8]) -> Result<polars::prelude::Schema> {
    use datalog::DataLogReader;
    use schema::WpilogSchema;

    let reader = DataLogReader::new(data);
    if !reader.is_valid() {
        return Err(WpilogError::InvalidFormat(
            "Invalid WPILog file header".to_string(),
        ));
    }

    let schema = WpilogSchema::infer_from_records(reader.records()?)?;
    Ok(schema.to_polars_schema())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a minimal WPILog file for testing
    fn create_test_wpilog() -> Vec<u8> {
        let mut data = Vec::new();

        // Header: "WPILOG" + version 0x0100 + extra header length 0
        data.extend_from_slice(b"WPILOG");
        data.extend_from_slice(&[0x00, 0x01]); // Version 1.0
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Extra header length = 0

        // Start control record for entry ID 1
        data.push(0x00); // Header byte
        data.push(0x00); // Entry ID = 0 (control)
        data.push(27); // Payload size (1+4+4+4+4+6+4=27)
        data.push(0x01); // Timestamp = 1

        // Start control payload
        data.push(0x00); // Control type = Start
        data.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // Entry ID = 1
        data.extend_from_slice(&[0x04, 0x00, 0x00, 0x00]); // Name length = 4
        data.extend_from_slice(b"test"); // Name = "test"
        data.extend_from_slice(&[0x06, 0x00, 0x00, 0x00]); // Type length = 6
        data.extend_from_slice(b"double"); // Type = "double"
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Metadata length = 0

        // Data record for entry ID 1 with value 3.14
        data.push(0x00); // Header byte
        data.push(0x01); // Entry ID = 1
        data.push(0x08); // Payload size = 8
        data.push(0x02); // Timestamp = 2
                         // 3.14 as f64 in little endian
        data.extend_from_slice(&[0x1f, 0x85, 0xeb, 0x51, 0xb8, 0x1e, 0x09, 0x40]);

        data
    }

    #[test]
    fn test_from_bytes() {
        let data = create_test_wpilog();
        let df = WpilogParser::from_bytes(data).unwrap();

        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 2); // timestamp + test

        let col_names = df.get_column_names();
        assert!(col_names.iter().any(|s| s.as_str() == "timestamp"));
        assert!(col_names.iter().any(|s| s.as_str() == "test"));
    }

    #[test]
    fn test_infer_schema() {
        let data = create_test_wpilog();
        let schema = infer_schema(&data).unwrap();

        assert!(schema.get("timestamp").is_some());
        assert!(schema.get("test").is_some());
    }
}
