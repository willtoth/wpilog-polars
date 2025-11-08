//! Parser for WPILib struct schema definitions.

use super::types::*;
use crate::error::{Result, WpilogError};
use std::collections::HashMap;

/// Parser for struct schema definitions.
pub struct SchemaParser;

impl SchemaParser {
    /// Parse a schema definition string.
    /// Example: "double x; double y; double theta"
    pub fn parse(schema_text: &str) -> Result<Vec<StructField>> {
        let mut fields = Vec::new();

        // Split by semicolons and parse each declaration
        for decl in schema_text.split(';') {
            let trimmed = decl.trim();
            if trimmed.is_empty() {
                continue; // Skip empty declarations
            }

            let field = Self::parse_declaration(trimmed)?;
            fields.push(field);
        }

        Ok(fields)
    }

    /// Parse a single declaration.
    fn parse_declaration(decl: &str) -> Result<StructField> {
        let decl = decl.trim();

        // Check for bit-field (contains ':')
        if decl.contains(':') {
            Self::parse_bitfield_declaration(decl)
        } else {
            Self::parse_standard_declaration(decl)
        }
    }

    /// Parse a standard (non-bit-field) declaration.
    fn parse_standard_declaration(decl: &str) -> Result<StructField> {
        // Format: [enum {...}] type name[size]
        let (enum_spec, rest) = Self::extract_enum_spec(decl)?;
        let rest = rest.trim();

        // Split into tokens
        let tokens: Vec<&str> = rest.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(WpilogError::ParseError(format!(
                "Invalid declaration: {}",
                decl
            )));
        }

        let type_str = tokens[0];
        let name_and_array = tokens[1..].join(" ");

        // Check for array syntax: name[size]
        let (name, field_type) = if let Some(bracket_pos) = name_and_array.find('[') {
            let name = name_and_array[..bracket_pos].trim();
            let array_part = &name_and_array[bracket_pos..];

            // Extract array size
            if !array_part.ends_with(']') {
                return Err(WpilogError::ParseError(format!(
                    "Invalid array syntax: {}",
                    name_and_array
                )));
            }

            let size_str = array_part[1..array_part.len() - 1].trim();
            let length = size_str.parse::<usize>().map_err(|_| {
                WpilogError::ParseError(format!("Invalid array size: {}", size_str))
            })?;

            let elem_type = Self::parse_type(type_str)?;
            let field_type = FieldType::Array {
                elem_type: Box::new(elem_type),
                length,
            };

            (name.to_string(), field_type)
        } else {
            let name = name_and_array.trim();
            let field_type = Self::parse_type(type_str)?;
            (name.to_string(), field_type)
        };

        Ok(StructField::Standard(StandardField {
            name,
            field_type,
            offset: 0, // Will be calculated by registry
            size: 0,   // Will be calculated by registry
            enum_spec,
        }))
    }

    /// Parse a bit-field declaration.
    fn parse_bitfield_declaration(decl: &str) -> Result<StructField> {
        // Format: [enum {...}] type name : bits
        let (enum_spec, rest) = Self::extract_enum_spec(decl)?;
        let rest = rest.trim();

        // Split by ':'
        let parts: Vec<&str> = rest.split(':').collect();
        if parts.len() != 2 {
            return Err(WpilogError::ParseError(format!(
                "Invalid bit-field syntax: {}",
                decl
            )));
        }

        let left = parts[0].trim();
        let bit_width_str = parts[1].trim();

        // Parse bit width
        let bit_width = bit_width_str.parse::<u8>().map_err(|_| {
            WpilogError::ParseError(format!("Invalid bit width: {}", bit_width_str))
        })?;

        if bit_width == 0 {
            return Err(WpilogError::ParseError(
                "Bit width must be at least 1".to_string(),
            ));
        }

        // Parse type and name
        let tokens: Vec<&str> = left.split_whitespace().collect();
        if tokens.len() < 2 {
            return Err(WpilogError::ParseError(format!(
                "Invalid bit-field declaration: {}",
                left
            )));
        }

        let type_str = tokens[0];
        let name = tokens[1..].join(" ");

        let int_type = Self::parse_integer_type(type_str)?;

        // Validate bit width
        if bit_width > int_type.max_bits() {
            return Err(WpilogError::ParseError(format!(
                "Bit width {} exceeds maximum for type {:?} ({})",
                bit_width,
                int_type,
                int_type.max_bits()
            )));
        }

        Ok(StructField::BitField(BitFieldDecl {
            name,
            int_type,
            bit_width: bit_width as usize,
            storage_offset: 0,  // Will be calculated by registry
            bit_offset: 0,      // Will be calculated by registry
            spans_units: false, // Will be calculated by registry
            enum_spec,
        }))
    }

    /// Extract enum specification from the beginning of a declaration.
    /// Returns (enum_spec, remaining_text).
    fn extract_enum_spec(decl: &str) -> Result<(Option<EnumSpec>, &str)> {
        let trimmed = decl.trim();

        // Check if starts with 'enum' or '{'
        let start_pos = if trimmed.starts_with("enum") {
            trimmed
                .find('{')
                .ok_or_else(|| WpilogError::ParseError("enum keyword without braces".to_string()))?
        } else if trimmed.starts_with('{') {
            0
        } else {
            return Ok((None, trimmed));
        };

        // Find matching '}'
        let end_pos = trimmed
            .find('}')
            .ok_or_else(|| WpilogError::ParseError("Unclosed enum specification".to_string()))?;

        let enum_text = &trimmed[start_pos..=end_pos];
        let remaining = &trimmed[end_pos + 1..];

        let enum_spec = Self::parse_enum_spec(enum_text)?;
        Ok((Some(enum_spec), remaining))
    }

    /// Parse enum specification: { name=value, name=value, ... }
    fn parse_enum_spec(text: &str) -> Result<EnumSpec> {
        let text = text.trim();

        // Remove braces
        if !text.starts_with('{') || !text.ends_with('}') {
            return Err(WpilogError::ParseError(format!(
                "Invalid enum spec: {}",
                text
            )));
        }

        let inner = &text[1..text.len() - 1];
        let mut values = HashMap::new();

        if inner.trim().is_empty() {
            return Ok(EnumSpec { values });
        }

        // Split by commas
        for entry in inner.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }

            // Parse name=value
            let parts: Vec<&str> = entry.split('=').collect();
            if parts.len() != 2 {
                return Err(WpilogError::ParseError(format!(
                    "Invalid enum entry: {}",
                    entry
                )));
            }

            let name = parts[0].trim().to_string();
            let value_str = parts[1].trim();
            let value = value_str.parse::<i64>().map_err(|_| {
                WpilogError::ParseError(format!("Invalid enum value: {}", value_str))
            })?;

            values.insert(value, name);
        }

        Ok(EnumSpec { values })
    }

    /// Parse a type name into FieldType.
    fn parse_type(type_str: &str) -> Result<FieldType> {
        match type_str {
            "bool" => Ok(FieldType::Bool),
            "char" => Ok(FieldType::Char),
            "int8" => Ok(FieldType::Int8),
            "int16" => Ok(FieldType::Int16),
            "int32" => Ok(FieldType::Int32),
            "int64" => Ok(FieldType::Int64),
            "uint8" => Ok(FieldType::UInt8),
            "uint16" => Ok(FieldType::UInt16),
            "uint32" => Ok(FieldType::UInt32),
            "uint64" => Ok(FieldType::UInt64),
            "float" | "float32" => Ok(FieldType::Float32),
            "double" | "float64" => Ok(FieldType::Float64),
            _ => {
                // Assume it's a struct reference
                Ok(FieldType::Struct(type_str.to_string()))
            }
        }
    }

    /// Parse an integer type for bit-fields.
    fn parse_integer_type(type_str: &str) -> Result<IntegerType> {
        match type_str {
            "bool" => Ok(IntegerType::Bool),
            "int8" => Ok(IntegerType::Int8),
            "int16" => Ok(IntegerType::Int16),
            "int32" => Ok(IntegerType::Int32),
            "int64" => Ok(IntegerType::Int64),
            "uint8" => Ok(IntegerType::UInt8),
            "uint16" => Ok(IntegerType::UInt16),
            "uint32" => Ok(IntegerType::UInt32),
            "uint64" => Ok(IntegerType::UInt64),
            _ => Err(WpilogError::ParseError(format!(
                "Invalid integer type for bit-field: {}",
                type_str
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_schema() {
        let fields = SchemaParser::parse("double x; double y").unwrap();
        assert_eq!(fields.len(), 2);

        match &fields[0] {
            StructField::Standard(f) => {
                assert_eq!(f.name, "x");
                assert_eq!(f.field_type, FieldType::Float64);
            }
            _ => panic!("Expected standard field"),
        }
    }

    #[test]
    fn test_parse_array() {
        let fields = SchemaParser::parse("double arr[4]").unwrap();
        assert_eq!(fields.len(), 1);

        match &fields[0] {
            StructField::Standard(f) => {
                assert_eq!(f.name, "arr");
                match &f.field_type {
                    FieldType::Array { elem_type, length } => {
                        assert_eq!(**elem_type, FieldType::Float64);
                        assert_eq!(*length, 4);
                    }
                    _ => panic!("Expected array type"),
                }
            }
            _ => panic!("Expected standard field"),
        }
    }

    #[test]
    fn test_parse_bitfield() {
        let fields = SchemaParser::parse("int8 a:4; int16 b:4").unwrap();
        assert_eq!(fields.len(), 2);

        match &fields[0] {
            StructField::BitField(bf) => {
                assert_eq!(bf.name, "a");
                assert_eq!(bf.int_type, IntegerType::Int8);
                assert_eq!(bf.bit_width, 4);
            }
            _ => panic!("Expected bit-field"),
        }
    }

    #[test]
    fn test_parse_enum_spec() {
        let fields = SchemaParser::parse("enum{a=1,b=2} int8 val").unwrap();
        assert_eq!(fields.len(), 1);

        match &fields[0] {
            StructField::Standard(f) => {
                assert_eq!(f.name, "val");
                assert!(f.enum_spec.is_some());
                let enum_spec = f.enum_spec.as_ref().unwrap();
                assert_eq!(enum_spec.values.get(&1), Some(&"a".to_string()));
                assert_eq!(enum_spec.values.get(&2), Some(&"b".to_string()));
            }
            _ => panic!("Expected standard field"),
        }
    }

    #[test]
    fn test_parse_struct_reference() {
        let fields = SchemaParser::parse("Translation2d translation").unwrap();
        assert_eq!(fields.len(), 1);

        match &fields[0] {
            StructField::Standard(f) => {
                assert_eq!(f.name, "translation");
                assert_eq!(f.field_type, FieldType::Struct("Translation2d".to_string()));
            }
            _ => panic!("Expected standard field"),
        }
    }

    #[test]
    fn test_empty_declarations() {
        let fields = SchemaParser::parse("double x;; ; double y;").unwrap();
        assert_eq!(fields.len(), 2);
    }
}
