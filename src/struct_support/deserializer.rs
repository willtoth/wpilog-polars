//! Binary deserializer for WPILib packed structs.

use byteorder::{ByteOrder, LittleEndian};
use std::cell::RefCell;
use std::collections::HashMap;

use super::registry::StructRegistry;
use super::types::*;
use crate::error::{Result, WpilogError};

/// Deserializer for binary struct data.
pub struct StructDeserializer<'a> {
    registry: &'a StructRegistry,
    /// Cache for last schema lookup to avoid repeated HashMap lookups
    last_schema_cache: RefCell<Option<(String, &'a StructSchema)>>,
}

impl<'a> StructDeserializer<'a> {
    /// Create a new deserializer with the given registry.
    pub fn new(registry: &'a StructRegistry) -> Self {
        Self {
            registry,
            last_schema_cache: RefCell::new(None),
        }
    }

    /// Get a reference to the registry.
    pub fn registry(&self) -> &StructRegistry {
        self.registry
    }

    /// Get schema with caching to avoid repeated HashMap lookups
    fn get_schema(&self, struct_name: &str) -> Result<&'a StructSchema> {
        // Check cache first
        if let Some((cached_name, cached_schema)) = self.last_schema_cache.borrow().as_ref() {
            if cached_name == struct_name {
                return Ok(*cached_schema);
            }
        }

        // Cache miss - look up in registry
        let schema = self.registry.get(struct_name).ok_or_else(|| {
            WpilogError::SchemaError(format!("Struct '{}' not found in registry", struct_name))
        })?;

        // Cache the result
        *self.last_schema_cache.borrow_mut() = Some((struct_name.to_string(), schema));

        Ok(schema)
    }

    /// Deserialize a struct from binary data.
    pub fn deserialize(&self, struct_name: &str, data: &[u8]) -> Result<StructValue> {
        let schema = self.get_schema(struct_name)?;

        // Check that data is large enough
        if data.len() < schema.total_size {
            return Err(WpilogError::ParseError(format!(
                "Data too short for struct '{}': expected {} bytes, got {}",
                struct_name,
                schema.total_size,
                data.len()
            )));
        }

        // Pre-allocate HashMap with exact capacity to avoid reallocations
        let mut fields = HashMap::with_capacity(schema.fields.len());

        for field in &schema.fields {
            match field {
                StructField::Standard(std_field) => {
                    let value = self.deserialize_standard_field(std_field, data)?;
                    fields.insert(std_field.name.clone(), value);
                }
                StructField::BitField(bitfield) => {
                    let value = self.deserialize_bitfield(bitfield, data)?;
                    fields.insert(bitfield.name.clone(), value);
                }
            }
        }

        Ok(StructValue {
            struct_name: struct_name.to_string(),
            fields,
        })
    }

    /// Deserialize a standard field.
    fn deserialize_standard_field(&self, field: &StandardField, data: &[u8]) -> Result<FieldValue> {
        let offset = field.offset;

        match &field.field_type {
            FieldType::Bool => {
                let val = data[offset] != 0;
                Ok(FieldValue::Bool(val))
            }
            FieldType::Char => {
                let val = data[offset] as char;
                Ok(FieldValue::Char(val))
            }
            FieldType::Int8 => {
                let val = data[offset] as i8;
                Ok(FieldValue::Int8(val))
            }
            FieldType::Int16 => {
                let val = LittleEndian::read_i16(&data[offset..offset + 2]);
                Ok(FieldValue::Int16(val))
            }
            FieldType::Int32 => {
                let val = LittleEndian::read_i32(&data[offset..offset + 4]);
                Ok(FieldValue::Int32(val))
            }
            FieldType::Int64 => {
                let val = LittleEndian::read_i64(&data[offset..offset + 8]);
                Ok(FieldValue::Int64(val))
            }
            FieldType::UInt8 => {
                let val = data[offset];
                Ok(FieldValue::UInt8(val))
            }
            FieldType::UInt16 => {
                let val = LittleEndian::read_u16(&data[offset..offset + 2]);
                Ok(FieldValue::UInt16(val))
            }
            FieldType::UInt32 => {
                let val = LittleEndian::read_u32(&data[offset..offset + 4]);
                Ok(FieldValue::UInt32(val))
            }
            FieldType::UInt64 => {
                let val = LittleEndian::read_u64(&data[offset..offset + 8]);
                Ok(FieldValue::UInt64(val))
            }
            FieldType::Float32 => {
                let val = LittleEndian::read_f32(&data[offset..offset + 4]);
                Ok(FieldValue::Float32(val))
            }
            FieldType::Float64 => {
                let val = LittleEndian::read_f64(&data[offset..offset + 8]);
                Ok(FieldValue::Float64(val))
            }
            FieldType::Array { elem_type, length } => {
                let mut values = Vec::with_capacity(*length);
                let elem_size = self.calculate_elem_size(elem_type)?;

                for i in 0..*length {
                    let elem_offset = offset + i * elem_size;
                    let value = self.deserialize_array_element(elem_type, &data[elem_offset..])?;
                    values.push(value);
                }

                Ok(FieldValue::Array(values))
            }
            FieldType::Struct(struct_name) => {
                let nested_schema = self.get_schema(struct_name)?;

                let struct_data = &data[offset..offset + nested_schema.total_size];
                let nested_value = self.deserialize(struct_name, struct_data)?;
                Ok(FieldValue::Struct(Box::new(nested_value)))
            }
        }
    }

    /// Deserialize an array element (non-recursive primitive types).
    fn deserialize_array_element(&self, elem_type: &FieldType, data: &[u8]) -> Result<FieldValue> {
        match elem_type {
            FieldType::Bool => Ok(FieldValue::Bool(data[0] != 0)),
            FieldType::Char => Ok(FieldValue::Char(data[0] as char)),
            FieldType::Int8 => Ok(FieldValue::Int8(data[0] as i8)),
            FieldType::Int16 => Ok(FieldValue::Int16(LittleEndian::read_i16(&data[0..2]))),
            FieldType::Int32 => Ok(FieldValue::Int32(LittleEndian::read_i32(&data[0..4]))),
            FieldType::Int64 => Ok(FieldValue::Int64(LittleEndian::read_i64(&data[0..8]))),
            FieldType::UInt8 => Ok(FieldValue::UInt8(data[0])),
            FieldType::UInt16 => Ok(FieldValue::UInt16(LittleEndian::read_u16(&data[0..2]))),
            FieldType::UInt32 => Ok(FieldValue::UInt32(LittleEndian::read_u32(&data[0..4]))),
            FieldType::UInt64 => Ok(FieldValue::UInt64(LittleEndian::read_u64(&data[0..8]))),
            FieldType::Float32 => Ok(FieldValue::Float32(LittleEndian::read_f32(&data[0..4]))),
            FieldType::Float64 => Ok(FieldValue::Float64(LittleEndian::read_f64(&data[0..8]))),
            FieldType::Struct(struct_name) => {
                let nested_value = self.deserialize(struct_name, data)?;
                Ok(FieldValue::Struct(Box::new(nested_value)))
            }
            FieldType::Array { .. } => Err(WpilogError::ParseError(
                "Nested arrays not supported".to_string(),
            )),
        }
    }

    /// Calculate the size of an array element.
    fn calculate_elem_size(&self, elem_type: &FieldType) -> Result<usize> {
        match elem_type {
            FieldType::Bool | FieldType::Char | FieldType::Int8 | FieldType::UInt8 => Ok(1),
            FieldType::Int16 | FieldType::UInt16 => Ok(2),
            FieldType::Int32 | FieldType::UInt32 | FieldType::Float32 => Ok(4),
            FieldType::Int64 | FieldType::UInt64 | FieldType::Float64 => Ok(8),
            FieldType::Struct(struct_name) => {
                let schema = self.get_schema(struct_name)?;
                Ok(schema.total_size)
            }
            FieldType::Array { .. } => Err(WpilogError::ParseError(
                "Nested arrays not supported".to_string(),
            )),
        }
    }

    /// Deserialize a bit-field.
    fn deserialize_bitfield(&self, bitfield: &BitFieldDecl, data: &[u8]) -> Result<FieldValue> {
        let type_width = bitfield.int_type.width();
        let type_size = bitfield.int_type.size();

        // Read the storage unit(s)
        let storage_val = if bitfield.spans_units {
            // Field spans multiple storage units, need to read both
            let unit1 = self.read_integer_at(data, bitfield.storage_offset, &bitfield.int_type)?;
            let unit2 = self.read_integer_at(
                data,
                bitfield.storage_offset + type_size,
                &bitfield.int_type,
            )?;

            // Extract bits from both units
            let bits_in_first = type_width - bitfield.bit_offset;
            let mask1 = ((1u64 << bits_in_first) - 1) << bitfield.bit_offset;
            let val1 = (unit1 & mask1) >> bitfield.bit_offset;

            let bits_in_second = bitfield.bit_width - bits_in_first;
            let mask2 = (1u64 << bits_in_second) - 1;
            let val2 = unit2 & mask2;

            val1 | (val2 << bits_in_first)
        } else {
            // Field is contained in a single storage unit
            let storage =
                self.read_integer_at(data, bitfield.storage_offset, &bitfield.int_type)?;

            // Extract the bits
            let mask = (1u64 << bitfield.bit_width) - 1;
            (storage >> bitfield.bit_offset) & mask
        };

        // According to WPILib spec, bit-fields are not sign-extended
        // They are simply extracted as unsigned values
        let value = storage_val as i64;

        // Convert to appropriate FieldValue
        Ok(FieldValue::Int64(value))
    }

    /// Read an integer value at a specific offset.
    fn read_integer_at(&self, data: &[u8], offset: usize, int_type: &IntegerType) -> Result<u64> {
        match int_type {
            IntegerType::Bool => Ok(data[offset] as u64),
            IntegerType::Int8 => Ok(data[offset] as i8 as i64 as u64),
            IntegerType::UInt8 => Ok(data[offset] as u64),
            IntegerType::Int16 => {
                Ok(LittleEndian::read_i16(&data[offset..offset + 2]) as i64 as u64)
            }
            IntegerType::UInt16 => Ok(LittleEndian::read_u16(&data[offset..offset + 2]) as u64),
            IntegerType::Int32 => {
                Ok(LittleEndian::read_i32(&data[offset..offset + 4]) as i64 as u64)
            }
            IntegerType::UInt32 => Ok(LittleEndian::read_u32(&data[offset..offset + 4]) as u64),
            IntegerType::Int64 => Ok(LittleEndian::read_i64(&data[offset..offset + 8]) as u64),
            IntegerType::UInt64 => Ok(LittleEndian::read_u64(&data[offset..offset + 8])),
        }
    }
}

impl IntegerType {
    /// Get the width in bits of this integer type.
    fn width(&self) -> usize {
        match self {
            IntegerType::Bool | IntegerType::Int8 | IntegerType::UInt8 => 8,
            IntegerType::Int16 | IntegerType::UInt16 => 16,
            IntegerType::Int32 | IntegerType::UInt32 => 32,
            IntegerType::Int64 | IntegerType::UInt64 => 64,
        }
    }
}

/// A deserialized struct value.
#[derive(Debug, Clone, PartialEq)]
pub struct StructValue {
    pub struct_name: String,
    pub fields: HashMap<String, FieldValue>,
}

/// A field value (primitive, array, or nested struct).
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Bool(bool),
    Char(char),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Array(Vec<FieldValue>),
    Struct(Box<StructValue>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_simple_struct() {
        let mut registry = StructRegistry::new();
        registry
            .register("Translation2d".to_string(), "double x; double y")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);

        // Create binary data: x=1.5, y=2.5 (little-endian doubles)
        let mut data = vec![0u8; 16];
        LittleEndian::write_f64(&mut data[0..8], 1.5);
        LittleEndian::write_f64(&mut data[8..16], 2.5);

        let result = deserializer.deserialize("Translation2d", &data).unwrap();

        assert_eq!(result.struct_name, "Translation2d");
        assert_eq!(result.fields.len(), 2);

        match result.fields.get("x").unwrap() {
            FieldValue::Float64(v) => assert_eq!(*v, 1.5),
            _ => panic!("Expected Float64"),
        }

        match result.fields.get("y").unwrap() {
            FieldValue::Float64(v) => assert_eq!(*v, 2.5),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_deserialize_mixed_types() {
        let mut registry = StructRegistry::new();
        registry
            .register("Mixed".to_string(), "int8 a; int16 b; int32 c; double d")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);

        // Create binary data: a=10, b=1000, c=100000, d=3.14
        let mut data = vec![0u8; 15];
        data[0] = 10i8 as u8;
        LittleEndian::write_i16(&mut data[1..3], 1000);
        LittleEndian::write_i32(&mut data[3..7], 100000);
        LittleEndian::write_f64(&mut data[7..15], 3.14);

        let result = deserializer.deserialize("Mixed", &data).unwrap();

        match result.fields.get("a").unwrap() {
            FieldValue::Int8(v) => assert_eq!(*v, 10),
            _ => panic!("Expected Int8"),
        }

        match result.fields.get("b").unwrap() {
            FieldValue::Int16(v) => assert_eq!(*v, 1000),
            _ => panic!("Expected Int16"),
        }

        match result.fields.get("c").unwrap() {
            FieldValue::Int32(v) => assert_eq!(*v, 100000),
            _ => panic!("Expected Int32"),
        }

        match result.fields.get("d").unwrap() {
            FieldValue::Float64(v) => assert!((v - 3.14).abs() < 1e-10),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_deserialize_array() {
        let mut registry = StructRegistry::new();
        registry
            .register("Vector".to_string(), "double values[4]")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);

        // Create binary data: [1.0, 2.0, 3.0, 4.0]
        let mut data = vec![0u8; 32];
        LittleEndian::write_f64(&mut data[0..8], 1.0);
        LittleEndian::write_f64(&mut data[8..16], 2.0);
        LittleEndian::write_f64(&mut data[16..24], 3.0);
        LittleEndian::write_f64(&mut data[24..32], 4.0);

        let result = deserializer.deserialize("Vector", &data).unwrap();

        match result.fields.get("values").unwrap() {
            FieldValue::Array(arr) => {
                assert_eq!(arr.len(), 4);
                for (i, val) in arr.iter().enumerate() {
                    match val {
                        FieldValue::Float64(v) => assert_eq!(*v, (i + 1) as f64),
                        _ => panic!("Expected Float64"),
                    }
                }
            }
            _ => panic!("Expected Array"),
        }
    }

    #[test]
    fn test_deserialize_bitfield() {
        let mut registry = StructRegistry::new();
        registry
            .register("Packed".to_string(), "int8 a:4; int8 b:4")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);

        // Create binary data: a=5 (0101), b=10 (1010)
        // Packed as: bbbbaaaa = 10100101 = 0xA5
        let data = vec![0xA5u8];

        let result = deserializer.deserialize("Packed", &data).unwrap();

        match result.fields.get("a").unwrap() {
            FieldValue::Int64(v) => assert_eq!(*v, 5),
            _ => panic!("Expected Int64"),
        }

        match result.fields.get("b").unwrap() {
            FieldValue::Int64(v) => assert_eq!(*v, 10),
            _ => panic!("Expected Int64"),
        }
    }

    #[test]
    fn test_deserialize_nested_struct() {
        let mut registry = StructRegistry::new();

        // Register inner struct
        registry
            .register("Translation2d".to_string(), "double x; double y")
            .unwrap();

        // Register outer struct
        registry
            .register(
                "Pose2d".to_string(),
                "Translation2d translation; double rotation",
            )
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);

        // Create binary data: translation.x=1.0, translation.y=2.0, rotation=3.14
        let mut data = vec![0u8; 24];
        LittleEndian::write_f64(&mut data[0..8], 1.0);
        LittleEndian::write_f64(&mut data[8..16], 2.0);
        LittleEndian::write_f64(&mut data[16..24], 3.14);

        let result = deserializer.deserialize("Pose2d", &data).unwrap();

        match result.fields.get("translation").unwrap() {
            FieldValue::Struct(nested) => {
                assert_eq!(nested.struct_name, "Translation2d");

                match nested.fields.get("x").unwrap() {
                    FieldValue::Float64(v) => assert_eq!(*v, 1.0),
                    _ => panic!("Expected Float64"),
                }

                match nested.fields.get("y").unwrap() {
                    FieldValue::Float64(v) => assert_eq!(*v, 2.0),
                    _ => panic!("Expected Float64"),
                }
            }
            _ => panic!("Expected Struct"),
        }

        match result.fields.get("rotation").unwrap() {
            FieldValue::Float64(v) => assert!((v - 3.14).abs() < 1e-10),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_data_too_short() {
        let mut registry = StructRegistry::new();
        registry
            .register("Translation2d".to_string(), "double x; double y")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);

        // Only 8 bytes, but need 16
        let data = vec![0u8; 8];

        let result = deserializer.deserialize("Translation2d", &data);
        assert!(result.is_err());
    }

    #[test]
    fn test_deserialize_struct_array() {
        // Test deserializing multiple structs from a single buffer
        let mut registry = StructRegistry::new();
        registry
            .register("Point".to_string(), "double x; double y")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);

        // Get the struct schema to determine size
        let schema = deserializer.registry().get("Point").unwrap();
        let struct_size = schema.total_size;
        assert_eq!(struct_size, 16); // 2 doubles = 16 bytes

        // Create binary data for 3 points: (1.0, 2.0), (3.0, 4.0), (5.0, 6.0)
        let mut data = vec![0u8; struct_size * 3];
        LittleEndian::write_f64(&mut data[0..8], 1.0);
        LittleEndian::write_f64(&mut data[8..16], 2.0);
        LittleEndian::write_f64(&mut data[16..24], 3.0);
        LittleEndian::write_f64(&mut data[24..32], 4.0);
        LittleEndian::write_f64(&mut data[32..40], 5.0);
        LittleEndian::write_f64(&mut data[40..48], 6.0);

        // Deserialize each struct from the array
        let num_structs = data.len() / struct_size;
        assert_eq!(num_structs, 3);

        let mut structs = Vec::new();
        for i in 0..num_structs {
            let start = i * struct_size;
            let end = start + struct_size;
            let struct_data = &data[start..end];
            let result = deserializer.deserialize("Point", struct_data).unwrap();
            structs.push(result);
        }

        // Verify all three structs
        assert_eq!(structs.len(), 3);

        // First point (1.0, 2.0)
        match structs[0].fields.get("x").unwrap() {
            FieldValue::Float64(v) => assert_eq!(*v, 1.0),
            _ => panic!("Expected Float64"),
        }
        match structs[0].fields.get("y").unwrap() {
            FieldValue::Float64(v) => assert_eq!(*v, 2.0),
            _ => panic!("Expected Float64"),
        }

        // Second point (3.0, 4.0)
        match structs[1].fields.get("x").unwrap() {
            FieldValue::Float64(v) => assert_eq!(*v, 3.0),
            _ => panic!("Expected Float64"),
        }
        match structs[1].fields.get("y").unwrap() {
            FieldValue::Float64(v) => assert_eq!(*v, 4.0),
            _ => panic!("Expected Float64"),
        }

        // Third point (5.0, 6.0)
        match structs[2].fields.get("x").unwrap() {
            FieldValue::Float64(v) => assert_eq!(*v, 5.0),
            _ => panic!("Expected Float64"),
        }
        match structs[2].fields.get("y").unwrap() {
            FieldValue::Float64(v) => assert_eq!(*v, 6.0),
            _ => panic!("Expected Float64"),
        }
    }

    #[test]
    fn test_struct_array_size_validation() {
        // Test that struct array size must be a multiple of struct size
        let mut registry = StructRegistry::new();
        registry
            .register("Point".to_string(), "double x; double y")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);
        let schema = deserializer.registry().get("Point").unwrap();
        let struct_size = schema.total_size;

        // Create data that's not a multiple of struct size (17 bytes instead of 16 or 32)
        let invalid_data = vec![0u8; struct_size + 1];

        // The last element shouldn't be deserializable since it's incomplete
        // We test by trying to deserialize at the invalid offset
        let result = deserializer.deserialize("Point", &invalid_data[struct_size..]);
        assert!(result.is_err()); // Should fail due to insufficient data
    }

    #[test]
    fn test_struct_array_with_nested_structs() {
        // Test struct arrays where each element contains nested structs
        let mut registry = StructRegistry::new();

        // Register nested struct
        registry
            .register("Vector2d".to_string(), "double x; double y")
            .unwrap();

        // Register struct with nested field
        registry
            .register("Velocity".to_string(), "Vector2d linear; double angular")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);
        let schema = deserializer.registry().get("Velocity").unwrap();
        let struct_size = schema.total_size;
        assert_eq!(struct_size, 24); // 2 doubles + 1 double = 24 bytes

        // Create binary data for 2 velocities
        let mut data = vec![0u8; struct_size * 2];

        // First velocity: linear=(1.0, 2.0), angular=0.5
        LittleEndian::write_f64(&mut data[0..8], 1.0);
        LittleEndian::write_f64(&mut data[8..16], 2.0);
        LittleEndian::write_f64(&mut data[16..24], 0.5);

        // Second velocity: linear=(3.0, 4.0), angular=1.5
        LittleEndian::write_f64(&mut data[24..32], 3.0);
        LittleEndian::write_f64(&mut data[32..40], 4.0);
        LittleEndian::write_f64(&mut data[40..48], 1.5);

        // Deserialize both structs
        let mut structs = Vec::new();
        for i in 0..2 {
            let start = i * struct_size;
            let end = start + struct_size;
            let struct_data = &data[start..end];
            let result = deserializer.deserialize("Velocity", struct_data).unwrap();
            structs.push(result);
        }

        assert_eq!(structs.len(), 2);

        // Verify first velocity
        match structs[0].fields.get("linear").unwrap() {
            FieldValue::Struct(nested) => {
                match nested.fields.get("x").unwrap() {
                    FieldValue::Float64(v) => assert_eq!(*v, 1.0),
                    _ => panic!("Expected Float64"),
                }
                match nested.fields.get("y").unwrap() {
                    FieldValue::Float64(v) => assert_eq!(*v, 2.0),
                    _ => panic!("Expected Float64"),
                }
            }
            _ => panic!("Expected nested Struct"),
        }

        // Verify second velocity angular component
        match structs[1].fields.get("angular").unwrap() {
            FieldValue::Float64(v) => assert_eq!(*v, 1.5),
            _ => panic!("Expected Float64"),
        }
    }
}
