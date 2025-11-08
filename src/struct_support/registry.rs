use super::types::*;
use crate::error::{Result, WpilogError};
use std::collections::HashMap;

/// Registry for managing struct schemas and calculating layouts
#[derive(Clone)]
pub struct StructRegistry {
    schemas: HashMap<String, StructSchema>,
}

impl StructRegistry {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Register a struct schema from its text definition
    pub fn register(&mut self, name: String, schema_text: &str) -> Result<()> {
        let fields = super::parser::SchemaParser::parse(schema_text)?;
        let layout = self.calculate_layout(&fields)?;

        let schema = StructSchema {
            name: name.clone(),
            fields: layout.fields,
            total_size: layout.total_size,
        };

        self.schemas.insert(name, schema);
        Ok(())
    }

    /// Get a registered schema by name
    pub fn get(&self, name: &str) -> Option<&StructSchema> {
        self.schemas.get(name)
    }

    /// Calculate the layout (offsets and size) for a set of fields
    fn calculate_layout(&self, fields: &[StructField]) -> Result<LayoutResult> {
        let mut offset = 0;
        let mut result_fields = Vec::new();
        let mut pending_bitfields: Vec<BitFieldDecl> = Vec::new();

        for field in fields {
            match field {
                StructField::Standard(std_field) => {
                    // Flush any pending bit-fields first
                    if !pending_bitfields.is_empty() {
                        let packed = self.pack_bitfields(&pending_bitfields, offset)?;
                        result_fields.extend(packed.fields);
                        offset = packed.next_offset;
                        pending_bitfields.clear();
                    }

                    // Calculate size and offset for standard field
                    let size = self.calculate_field_size(&std_field.field_type)?;
                    let field_with_offset = StandardField {
                        name: std_field.name.clone(),
                        field_type: std_field.field_type.clone(),
                        enum_spec: std_field.enum_spec.clone(),
                        offset,
                        size,
                    };

                    result_fields.push(StructField::Standard(field_with_offset));
                    offset += size;
                }
                StructField::BitField(bitfield) => {
                    // Accumulate bit-fields for packing
                    pending_bitfields.push(bitfield.clone());
                }
            }
        }

        // Flush any remaining bit-fields
        if !pending_bitfields.is_empty() {
            let packed = self.pack_bitfields(&pending_bitfields, offset)?;
            result_fields.extend(packed.fields);
            offset = packed.next_offset;
        }

        Ok(LayoutResult {
            fields: result_fields,
            total_size: offset,
        })
    }

    /// Calculate the size in bytes of a field type
    fn calculate_field_size(&self, field_type: &FieldType) -> Result<usize> {
        match field_type {
            FieldType::Bool | FieldType::Int8 | FieldType::UInt8 => Ok(1),
            FieldType::Int16 | FieldType::UInt16 => Ok(2),
            FieldType::Int32 | FieldType::UInt32 | FieldType::Float32 => Ok(4),
            FieldType::Int64 | FieldType::UInt64 | FieldType::Float64 => Ok(8),
            FieldType::Char => Ok(1),
            FieldType::Array { elem_type, length } => {
                let elem_size = self.calculate_field_size(elem_type)?;
                Ok(elem_size * length)
            }
            FieldType::Struct(name) => {
                let schema = self.schemas.get(name).ok_or_else(|| {
                    WpilogError::SchemaError(format!(
                        "Nested struct '{}' not found in registry",
                        name
                    ))
                })?;
                Ok(schema.total_size)
            }
        }
    }

    /// Pack consecutive bit-fields according to WPILib rules
    ///
    /// Rules from struct.adoc:
    /// 1. Bit-fields are packed in little-endian order
    /// 2. Multiple adjacent bit-fields of the same underlying type are packed together
    /// 3. If total bits <= type width, pack into one storage unit
    /// 4. If total bits > type width, split across multiple units
    /// 5. Bit-fields of different types are not packed together
    fn pack_bitfields(
        &self,
        bitfields: &[BitFieldDecl],
        start_offset: usize,
    ) -> Result<PackedBitFields> {
        let mut result_fields = Vec::new();
        let mut offset = start_offset;
        let mut i = 0;

        while i < bitfields.len() {
            // Find all consecutive bit-fields with the same underlying type
            let current_type = &bitfields[i].int_type;
            let mut group = vec![&bitfields[i]];
            let mut j = i + 1;

            while j < bitfields.len() && bitfields[j].int_type == *current_type {
                group.push(&bitfields[j]);
                j += 1;
            }

            // Pack this group
            let type_width = Self::integer_type_width(current_type);
            let type_size = Self::integer_type_size(current_type);

            // Calculate total bits needed
            let total_bits: usize = group.iter().map(|bf| bf.bit_width).sum();

            // Calculate how many storage units we need
            let num_units = (total_bits + type_width - 1) / type_width;

            // Create bit-field entries for each field in the group
            let mut bit_offset = 0;
            for bitfield in &group {
                // Determine which storage unit(s) this field spans
                let start_unit = bit_offset / type_width;
                let end_bit = bit_offset + bitfield.bit_width;
                let end_unit = (end_bit - 1) / type_width;

                // For simplicity, we store each bit-field with its storage offset
                // The deserializer will handle extracting bits across units
                let field_with_offset = BitFieldDecl {
                    name: bitfield.name.clone(),
                    int_type: bitfield.int_type.clone(),
                    bit_width: bitfield.bit_width,
                    enum_spec: bitfield.enum_spec.clone(),
                    storage_offset: offset + start_unit * type_size,
                    bit_offset: bit_offset % type_width,
                    spans_units: end_unit > start_unit,
                };

                result_fields.push(StructField::BitField(field_with_offset));
                bit_offset += bitfield.bit_width;
            }

            offset += num_units * type_size;
            i = j;
        }

        Ok(PackedBitFields {
            fields: result_fields,
            next_offset: offset,
        })
    }

    /// Get the width in bits of an integer type
    fn integer_type_width(int_type: &IntegerType) -> usize {
        match int_type {
            IntegerType::Bool | IntegerType::Int8 | IntegerType::UInt8 => 8,
            IntegerType::Int16 | IntegerType::UInt16 => 16,
            IntegerType::Int32 | IntegerType::UInt32 => 32,
            IntegerType::Int64 | IntegerType::UInt64 => 64,
        }
    }

    /// Get the size in bytes of an integer type
    fn integer_type_size(int_type: &IntegerType) -> usize {
        Self::integer_type_width(int_type) / 8
    }
}

impl Default for StructRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Result of layout calculation
struct LayoutResult {
    fields: Vec<StructField>,
    total_size: usize,
}

/// Result of bit-field packing
struct PackedBitFields {
    fields: Vec<StructField>,
    next_offset: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_struct_layout() {
        let mut registry = StructRegistry::new();

        // double x; double y;
        registry
            .register("Translation2d".to_string(), "double x; double y")
            .unwrap();

        let schema = registry.get("Translation2d").unwrap();
        assert_eq!(schema.total_size, 16); // 2 * 8 bytes
        assert_eq!(schema.fields.len(), 2);

        match &schema.fields[0] {
            StructField::Standard(f) => {
                assert_eq!(f.name, "x");
                assert_eq!(f.offset, 0);
                assert_eq!(f.size, 8);
            }
            _ => panic!("Expected standard field"),
        }

        match &schema.fields[1] {
            StructField::Standard(f) => {
                assert_eq!(f.name, "y");
                assert_eq!(f.offset, 8);
                assert_eq!(f.size, 8);
            }
            _ => panic!("Expected standard field"),
        }
    }

    #[test]
    fn test_mixed_types() {
        let mut registry = StructRegistry::new();

        // int8 a; int16 b; int32 c; double d;
        registry
            .register("Mixed".to_string(), "int8 a; int16 b; int32 c; double d")
            .unwrap();

        let schema = registry.get("Mixed").unwrap();
        assert_eq!(schema.total_size, 15); // 1 + 2 + 4 + 8

        let offsets: Vec<usize> = schema
            .fields
            .iter()
            .map(|f| match f {
                StructField::Standard(sf) => sf.offset,
                _ => panic!("Expected standard field"),
            })
            .collect();

        assert_eq!(offsets, vec![0, 1, 3, 7]);
    }

    #[test]
    fn test_array_field() {
        let mut registry = StructRegistry::new();

        // double values[4];
        registry
            .register("Vector".to_string(), "double values[4]")
            .unwrap();

        let schema = registry.get("Vector").unwrap();
        assert_eq!(schema.total_size, 32); // 4 * 8 bytes

        match &schema.fields[0] {
            StructField::Standard(f) => {
                assert_eq!(f.name, "values");
                assert_eq!(f.size, 32);
                match &f.field_type {
                    FieldType::Array { elem_type, length } => {
                        assert_eq!(*length, 4);
                        assert!(matches!(**elem_type, FieldType::Float64));
                    }
                    _ => panic!("Expected array type"),
                }
            }
            _ => panic!("Expected standard field"),
        }
    }

    #[test]
    fn test_simple_bitfield_packing() {
        let mut registry = StructRegistry::new();

        // Two 4-bit fields should pack into one byte
        registry
            .register("Packed".to_string(), "int8 a:4; int8 b:4")
            .unwrap();

        let schema = registry.get("Packed").unwrap();
        assert_eq!(schema.total_size, 1); // Packed into 1 byte
        assert_eq!(schema.fields.len(), 2);

        match &schema.fields[0] {
            StructField::BitField(bf) => {
                assert_eq!(bf.name, "a");
                assert_eq!(bf.storage_offset, 0);
                assert_eq!(bf.bit_offset, 0);
                assert_eq!(bf.bit_width, 4);
            }
            _ => panic!("Expected bit-field"),
        }

        match &schema.fields[1] {
            StructField::BitField(bf) => {
                assert_eq!(bf.name, "b");
                assert_eq!(bf.storage_offset, 0);
                assert_eq!(bf.bit_offset, 4);
                assert_eq!(bf.bit_width, 4);
            }
            _ => panic!("Expected bit-field"),
        }
    }

    #[test]
    fn test_bitfield_overflow() {
        let mut registry = StructRegistry::new();

        // Three 4-bit fields need 12 bits = 2 bytes for int8
        registry
            .register("Overflow".to_string(), "int8 a:4; int8 b:4; int8 c:4")
            .unwrap();

        let schema = registry.get("Overflow").unwrap();
        assert_eq!(schema.total_size, 2); // 2 bytes needed
    }

    #[test]
    fn test_bitfield_different_types_not_packed() {
        let mut registry = StructRegistry::new();

        // Different types should not pack together
        registry
            .register("DiffTypes".to_string(), "int8 a:4; int16 b:4")
            .unwrap();

        let schema = registry.get("DiffTypes").unwrap();
        // int8:4 = 1 byte, int16:4 = 2 bytes (even though only 4 bits used)
        assert_eq!(schema.total_size, 3);
    }

    #[test]
    fn test_mixed_standard_and_bitfield() {
        let mut registry = StructRegistry::new();

        // Standard field, then bit-fields, then another standard field
        registry
            .register("Mixed".to_string(), "double x; int8 a:4; int8 b:4; float y")
            .unwrap();

        let schema = registry.get("Mixed").unwrap();
        // double(8) + int8:4+int8:4(1) + float(4) = 13 bytes
        assert_eq!(schema.total_size, 13);

        let offsets: Vec<usize> = schema
            .fields
            .iter()
            .map(|f| match f {
                StructField::Standard(sf) => sf.offset,
                StructField::BitField(bf) => bf.storage_offset,
            })
            .collect();

        assert_eq!(offsets, vec![0, 8, 8, 9]);
    }

    #[test]
    fn test_nested_struct() {
        let mut registry = StructRegistry::new();

        // Register inner struct first
        registry
            .register("Translation2d".to_string(), "double x; double y")
            .unwrap();

        // Register outer struct that uses inner
        registry
            .register(
                "Pose2d".to_string(),
                "Translation2d translation; double rotation",
            )
            .unwrap();

        let schema = registry.get("Pose2d").unwrap();
        assert_eq!(schema.total_size, 24); // 16 + 8

        match &schema.fields[0] {
            StructField::Standard(f) => {
                assert_eq!(f.name, "translation");
                assert_eq!(f.offset, 0);
                assert_eq!(f.size, 16);
            }
            _ => panic!("Expected standard field"),
        }
    }

    #[test]
    fn test_nested_struct_not_found() {
        let mut registry = StructRegistry::new();

        // Try to register struct with unknown nested type
        let result = registry.register(
            "Pose2d".to_string(),
            "Translation2d translation; double rotation",
        );

        assert!(result.is_err());
        match result {
            Err(WpilogError::SchemaError(msg)) => {
                assert!(msg.contains("not found in registry"));
            }
            _ => panic!("Expected SchemaError"),
        }
    }

    #[test]
    fn test_bitfield_spanning_units() {
        let mut registry = StructRegistry::new();

        // Create a bit-field that spans storage units
        // int16 a:10; int16 b:10 = 20 bits total, needs 2 int16 units (32 bits)
        registry
            .register("Spanning".to_string(), "int16 a:10; int16 b:10")
            .unwrap();

        let schema = registry.get("Spanning").unwrap();
        assert_eq!(schema.total_size, 4); // 2 * 2 bytes

        match &schema.fields[0] {
            StructField::BitField(bf) => {
                assert_eq!(bf.name, "a");
                assert_eq!(bf.bit_width, 10);
                assert_eq!(bf.storage_offset, 0);
            }
            _ => panic!("Expected bit-field"),
        }

        match &schema.fields[1] {
            StructField::BitField(bf) => {
                assert_eq!(bf.name, "b");
                assert_eq!(bf.bit_width, 10);
                // This should start at bit 10, which is in the second unit
                assert!(bf.bit_offset == 10 || bf.storage_offset == 2);
            }
            _ => panic!("Expected bit-field"),
        }
    }
}
