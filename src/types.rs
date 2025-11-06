//! Type system mapping between WPILog and Polars.
//!
//! This module provides:
//! - `PolarsDataType`: enum representing supported data types
//! - Mapping from WPILog type strings to PolarsDataType
//! - Conversion from PolarsDataType to Polars DataType
//! - `PolarsValue`: enum for storing typed values during accumulation

use crate::error::{Result, WpilogError};
use polars::prelude::*;

/// Data types supported by the WPILog to Polars converter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolarsDataType {
    Float64,
    Float32,
    Int64,
    Boolean,
    String,
    BooleanArray,
    Int64Array,
    Float32Array,
    Float64Array,
    StringArray,
}

impl PolarsDataType {
    /// Maps a WPILog type string to a PolarsDataType.
    /// Unknown types are treated as String (binary/serialized data).
    pub fn from_wpilog_type(type_name: &str) -> Result<Self> {
        match type_name {
            "double" => Ok(PolarsDataType::Float64),
            "float" => Ok(PolarsDataType::Float32),
            "int64" => Ok(PolarsDataType::Int64),
            "boolean" => Ok(PolarsDataType::Boolean),
            "string" => Ok(PolarsDataType::String),
            "raw" => Ok(PolarsDataType::String), // Treat raw as string (binary data)
            "boolean[]" => Ok(PolarsDataType::BooleanArray),
            "int64[]" => Ok(PolarsDataType::Int64Array),
            "float[]" => Ok(PolarsDataType::Float32Array),
            "double[]" => Ok(PolarsDataType::Float64Array),
            "string[]" => Ok(PolarsDataType::StringArray),
            "msgpack" => Ok(PolarsDataType::String), // Serialize msgpack as string
            "struct" => Ok(PolarsDataType::String),  // Serialize struct as string
            "json" => Ok(PolarsDataType::String),    // JSON as string
            "protobuf" => Ok(PolarsDataType::String), // Protobuf as string
            // Unknown/custom types: treat as string (binary/serialized data)
            // This allows graceful handling of custom WPILog types
            _ => {
                eprintln!(
                    "Warning: Unknown WPILog type '{}', treating as binary string",
                    type_name
                );
                Ok(PolarsDataType::String)
            }
        }
    }

    /// Converts to a Polars DataType.
    pub fn to_polars_dtype(&self) -> DataType {
        match self {
            PolarsDataType::Float64 => DataType::Float64,
            PolarsDataType::Float32 => DataType::Float32,
            PolarsDataType::Int64 => DataType::Int64,
            PolarsDataType::Boolean => DataType::Boolean,
            PolarsDataType::String => DataType::String,
            PolarsDataType::BooleanArray => DataType::List(Box::new(DataType::Boolean)),
            PolarsDataType::Int64Array => DataType::List(Box::new(DataType::Int64)),
            PolarsDataType::Float32Array => DataType::List(Box::new(DataType::Float32)),
            PolarsDataType::Float64Array => DataType::List(Box::new(DataType::Float64)),
            PolarsDataType::StringArray => DataType::List(Box::new(DataType::String)),
        }
    }

    /// Returns true if this is an array type.
    pub fn is_array(&self) -> bool {
        matches!(
            self,
            PolarsDataType::BooleanArray
                | PolarsDataType::Int64Array
                | PolarsDataType::Float32Array
                | PolarsDataType::Float64Array
                | PolarsDataType::StringArray
        )
    }
}

/// A typed value used during data accumulation.
#[derive(Debug, Clone)]
pub enum PolarsValue {
    Float64(f64),
    Float32(f32),
    Int64(i64),
    Boolean(bool),
    String(String),
    BooleanArray(Vec<bool>),
    Int64Array(Vec<i64>),
    Float32Array(Vec<f32>),
    Float64Array(Vec<f64>),
    StringArray(Vec<String>),
    Null,
}

impl PolarsValue {
    /// Returns the PolarsDataType of this value.
    pub fn dtype(&self) -> PolarsDataType {
        match self {
            PolarsValue::Float64(_) => PolarsDataType::Float64,
            PolarsValue::Float32(_) => PolarsDataType::Float32,
            PolarsValue::Int64(_) => PolarsDataType::Int64,
            PolarsValue::Boolean(_) => PolarsDataType::Boolean,
            PolarsValue::String(_) => PolarsDataType::String,
            PolarsValue::BooleanArray(_) => PolarsDataType::BooleanArray,
            PolarsValue::Int64Array(_) => PolarsDataType::Int64Array,
            PolarsValue::Float32Array(_) => PolarsDataType::Float32Array,
            PolarsValue::Float64Array(_) => PolarsDataType::Float64Array,
            PolarsValue::StringArray(_) => PolarsDataType::StringArray,
            PolarsValue::Null => PolarsDataType::String, // Default to string for null
        }
    }

    /// Creates a null value for the given data type.
    pub fn null_for_type(_dtype: &PolarsDataType) -> Self {
        PolarsValue::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_mapping() {
        assert_eq!(
            PolarsDataType::from_wpilog_type("double").unwrap(),
            PolarsDataType::Float64
        );
        assert_eq!(
            PolarsDataType::from_wpilog_type("float").unwrap(),
            PolarsDataType::Float32
        );
        assert_eq!(
            PolarsDataType::from_wpilog_type("int64").unwrap(),
            PolarsDataType::Int64
        );
        assert_eq!(
            PolarsDataType::from_wpilog_type("boolean").unwrap(),
            PolarsDataType::Boolean
        );
        assert_eq!(
            PolarsDataType::from_wpilog_type("string").unwrap(),
            PolarsDataType::String
        );
    }

    #[test]
    fn test_array_type_mapping() {
        assert_eq!(
            PolarsDataType::from_wpilog_type("double[]").unwrap(),
            PolarsDataType::Float64Array
        );
        assert_eq!(
            PolarsDataType::from_wpilog_type("int64[]").unwrap(),
            PolarsDataType::Int64Array
        );
    }

    #[test]
    fn test_is_array() {
        assert!(!PolarsDataType::Float64.is_array());
        assert!(PolarsDataType::Float64Array.is_array());
        assert!(PolarsDataType::Int64Array.is_array());
    }

    #[test]
    fn test_to_polars_dtype() {
        assert_eq!(PolarsDataType::Float64.to_polars_dtype(), DataType::Float64);
        assert_eq!(
            PolarsDataType::Int64Array.to_polars_dtype(),
            DataType::List(Box::new(DataType::Int64))
        );
    }
}
