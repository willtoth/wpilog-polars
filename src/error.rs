//! Error types for the WPILog parser library.

use thiserror::Error;

/// Result type alias for WPILog operations.
pub type Result<T> = std::result::Result<T, WpilogError>;

/// Errors that can occur when parsing WPILog files.
#[derive(Error, Debug)]
pub enum WpilogError {
    /// Invalid WPILog file format (e.g., wrong magic bytes, unsupported version)
    #[error("Invalid WPILOG format: {0}")]
    InvalidFormat(String),

    /// I/O error occurred while reading or writing
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Entry not found or invalid entry ID
    #[error("Invalid entry: {0}")]
    InvalidEntry(String),

    /// Data parsing error (e.g., wrong data type, corrupted data)
    #[error("Parse error: {0}")]
    ParseError(String),

    /// Schema inference or validation error
    #[error("Schema error: {0}")]
    SchemaError(String),

    /// Polars error during DataFrame construction
    #[error("Polars error: {0}")]
    PolarsError(#[from] polars::error::PolarsError),

    /// UTF-8 encoding/decoding error
    #[error("UTF-8 error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),

    /// Generic error with message
    #[error("{0}")]
    Other(String),
}
