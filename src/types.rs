//! Type system mapping between WPILog and Polars.
//!
//! This module provides:
//! - `PolarsDataType`: enum representing supported data types
//! - Mapping from WPILog type strings to PolarsDataType
//! - Conversion from PolarsDataType to Polars DataType
//! - `PolarsValue`: enum for storing typed values during accumulation

use crate::error::Result;
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
    Struct(String),      // Struct with type name (e.g., "Pose2d")
    StructArray(String), // Array of structs (e.g., "SwerveModuleState[]")
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
            "json" => Ok(PolarsDataType::String),    // JSON as string
            "protobuf" => Ok(PolarsDataType::String), // Protobuf as string
            // Check for struct array types (format: "struct:TypeName[]")
            _ if type_name.starts_with("struct:") && type_name.ends_with("[]") => {
                let struct_name = type_name
                    .strip_prefix("struct:")
                    .unwrap()
                    .strip_suffix("[]")
                    .unwrap()
                    .to_string();
                Ok(PolarsDataType::StructArray(struct_name))
            }
            // Check for struct types (format: "struct:TypeName")
            _ if type_name.starts_with("struct:") => {
                let struct_name = type_name.strip_prefix("struct:").unwrap().to_string();
                Ok(PolarsDataType::Struct(struct_name))
            }
            // Struct schema entries - store as strings for now (contain text schema definitions)
            "structschema" => Ok(PolarsDataType::String),
            // Generic "struct" without type name - treat as binary string
            "struct" => {
                eprintln!("Warning: Generic 'struct' type without name, treating as binary string");
                Ok(PolarsDataType::String)
            }
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
            // Structs and struct arrays will be properly converted in the builders
            PolarsDataType::Struct(_) => DataType::String,
            PolarsDataType::StructArray(_) => DataType::String,
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
                | PolarsDataType::StructArray(_)
        )
    }

    /// Returns true if this is a struct type.
    pub fn is_struct(&self) -> bool {
        matches!(self, PolarsDataType::Struct(_))
    }

    /// Returns true if this is a struct array type.
    pub fn is_struct_array(&self) -> bool {
        matches!(self, PolarsDataType::StructArray(_))
    }

    /// Gets the struct name if this is a struct type.
    pub fn struct_name(&self) -> Option<&str> {
        match self {
            PolarsDataType::Struct(name) => Some(name.as_str()),
            _ => None,
        }
    }

    /// Gets the struct name if this is a struct array type.
    pub fn struct_array_name(&self) -> Option<&str> {
        match self {
            PolarsDataType::StructArray(name) => Some(name.as_str()),
            _ => None,
        }
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
    Struct(crate::struct_support::StructValue), // Store deserialized struct value
    StructArray(Vec<crate::struct_support::StructValue>), // Store array of deserialized structs
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
            PolarsValue::Struct(sv) => PolarsDataType::Struct(sv.struct_name.clone()),
            PolarsValue::StructArray(svs) => {
                // Get struct name from first element, or default to empty string
                let struct_name = svs
                    .first()
                    .map(|sv| sv.struct_name.clone())
                    .unwrap_or_default();
                PolarsDataType::StructArray(struct_name)
            }
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

    #[test]
    fn test_struct_type_detection() {
        // Test struct type detection
        let dtype = PolarsDataType::from_wpilog_type("struct:Pose2d").unwrap();
        assert!(dtype.is_struct());
        assert_eq!(dtype.struct_name(), Some("Pose2d"));

        let dtype2 = PolarsDataType::from_wpilog_type("struct:Translation2d").unwrap();
        assert_eq!(dtype2.struct_name(), Some("Translation2d"));

        // Non-struct types should not be structs
        assert!(!PolarsDataType::Float64.is_struct());
        assert!(!PolarsDataType::String.is_struct());

        // Struct types should map to String for now
        assert_eq!(dtype.to_polars_dtype(), DataType::String);
    }

    #[test]
    fn test_struct_array_type_detection() {
        // Test struct array type detection
        let dtype = PolarsDataType::from_wpilog_type("struct:SwerveModuleState[]").unwrap();
        assert!(dtype.is_struct_array());
        assert!(dtype.is_array());
        assert!(!dtype.is_struct());
        assert_eq!(dtype.struct_array_name(), Some("SwerveModuleState"));

        let dtype2 = PolarsDataType::from_wpilog_type("struct:ChassisSpeeds[]").unwrap();
        assert!(dtype2.is_struct_array());
        assert_eq!(dtype2.struct_array_name(), Some("ChassisSpeeds"));

        // Non-struct-array types should not be struct arrays
        assert!(!PolarsDataType::Float64.is_struct_array());
        assert!(!PolarsDataType::Int64Array.is_struct_array());
        assert!(!PolarsDataType::Struct("Pose2d".to_string()).is_struct_array());

        // Struct array types should map to String for now
        assert_eq!(dtype.to_polars_dtype(), DataType::String);
    }

    #[test]
    fn test_struct_array_name_extraction() {
        // Test extracting struct name from struct array types
        let dtype = PolarsDataType::from_wpilog_type("struct:MyCustomType[]").unwrap();
        if let PolarsDataType::StructArray(name) = dtype {
            assert_eq!(name, "MyCustomType");
        } else {
            panic!("Expected StructArray variant");
        }
    }

    #[test]
    fn test_polars_value_struct_array_dtype() {
        // Test that PolarsValue::StructArray correctly reports its dtype
        use crate::struct_support::StructValue;

        let struct_val1 = StructValue {
            struct_name: "TestStruct".to_string(),
            fields: std::collections::HashMap::new(),
        };
        let struct_val2 = StructValue {
            struct_name: "TestStruct".to_string(),
            fields: std::collections::HashMap::new(),
        };

        let array_value = PolarsValue::StructArray(vec![struct_val1, struct_val2]);
        let dtype = array_value.dtype();

        assert!(dtype.is_struct_array());
        assert_eq!(dtype.struct_array_name(), Some("TestStruct"));
    }

    #[test]
    fn test_polars_value_empty_struct_array() {
        // Test empty struct array
        let empty_array = PolarsValue::StructArray(vec![]);
        let dtype = empty_array.dtype();

        assert!(dtype.is_struct_array());
        // Empty array should have empty struct name
        assert_eq!(dtype.struct_array_name(), Some(""));
    }
}
