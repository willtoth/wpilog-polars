//! Conversion from struct values to Polars types.

use polars::prelude::*;

use super::deserializer::{FieldValue, StructValue};
use super::registry::StructRegistry;
use super::types::*;
use crate::error::{Result, WpilogError};

/// Converter for struct values to Polars types.
pub struct PolarsConverter<'a> {
    registry: &'a StructRegistry,
}

impl<'a> PolarsConverter<'a> {
    /// Create a new converter with the given registry.
    pub fn new(registry: &'a StructRegistry) -> Self {
        Self { registry }
    }

    /// Convert a struct schema to a Polars DataType (Struct).
    pub fn schema_to_dtype(&self, struct_name: &str) -> Result<DataType> {
        let schema = self.registry.get(struct_name).ok_or_else(|| {
            WpilogError::SchemaError(format!("Struct '{}' not found in registry", struct_name))
        })?;

        let mut fields = Vec::new();

        for field in &schema.fields {
            match field {
                StructField::Standard(std_field) => {
                    let dtype = self.field_type_to_dtype(&std_field.field_type)?;
                    fields.push(Field::new(std_field.name.as_str().into(), dtype));
                }
                StructField::BitField(bitfield) => {
                    // Bit-fields are always represented as Int64
                    fields.push(Field::new(bitfield.name.as_str().into(), DataType::Int64));
                }
            }
        }

        Ok(DataType::Struct(fields))
    }

    /// Convert a FieldType to a Polars DataType.
    fn field_type_to_dtype(&self, field_type: &FieldType) -> Result<DataType> {
        match field_type {
            FieldType::Bool => Ok(DataType::Boolean),
            FieldType::Char => Ok(DataType::UInt8),
            FieldType::Int8 => Ok(DataType::Int8),
            FieldType::Int16 => Ok(DataType::Int16),
            FieldType::Int32 => Ok(DataType::Int32),
            FieldType::Int64 => Ok(DataType::Int64),
            FieldType::UInt8 => Ok(DataType::UInt8),
            FieldType::UInt16 => Ok(DataType::UInt16),
            FieldType::UInt32 => Ok(DataType::UInt32),
            FieldType::UInt64 => Ok(DataType::UInt64),
            FieldType::Float32 => Ok(DataType::Float32),
            FieldType::Float64 => Ok(DataType::Float64),
            FieldType::Array { elem_type, .. } => {
                // Use List instead of Array for compatibility
                let elem_dtype = self.field_type_to_dtype(elem_type)?;
                Ok(DataType::List(Box::new(elem_dtype)))
            }
            FieldType::Struct(struct_name) => self.schema_to_dtype(struct_name),
        }
    }

    /// Convert a StructValue to a Polars StructChunked (single value).
    pub fn value_to_series(&self, value: &StructValue) -> Result<Series> {
        let schema = self.registry.get(&value.struct_name).ok_or_else(|| {
            WpilogError::SchemaError(format!(
                "Struct '{}' not found in registry",
                value.struct_name
            ))
        })?;

        let mut series_vec = Vec::new();

        for field in &schema.fields {
            let field_name = match field {
                StructField::Standard(f) => &f.name,
                StructField::BitField(f) => &f.name,
            };

            let field_value = value.fields.get(field_name).ok_or_else(|| {
                WpilogError::ParseError(format!("Field '{}' not found in struct value", field_name))
            })?;

            let series = self.field_value_to_series(field_name, field_value)?;
            series_vec.push(series);
        }

        // Create a StructChunked with a single row
        // The second parameter should be the length of the series (number of rows), not number of fields
        let len = series_vec.first().map(|s| s.len()).unwrap_or(0);
        let series_refs: Vec<&Series> = series_vec.iter().collect();
        let struct_chunked =
            StructChunked::from_series(PlSmallStr::from(""), len, series_refs.into_iter())?;

        Ok(struct_chunked.into_series())
    }

    /// Convert a FieldValue to a Polars Series (single value).
    fn field_value_to_series(&self, name: &str, value: &FieldValue) -> Result<Series> {
        let series = match value {
            FieldValue::Bool(v) => Series::new(name.into(), [*v].as_slice()),
            FieldValue::Char(v) => {
                let val = *v as u8;
                UInt8Chunked::from_slice(name.into(), &[val]).into_series()
            }
            FieldValue::Int8(v) => Series::new(name.into(), [*v].as_slice()),
            FieldValue::Int16(v) => Series::new(name.into(), [*v].as_slice()),
            FieldValue::Int32(v) => Series::new(name.into(), [*v].as_slice()),
            FieldValue::Int64(v) => Series::new(name.into(), [*v].as_slice()),
            FieldValue::UInt8(v) => UInt8Chunked::from_slice(name.into(), &[*v]).into_series(),
            FieldValue::UInt16(v) => UInt16Chunked::from_slice(name.into(), &[*v]).into_series(),
            FieldValue::UInt32(v) => Series::new(name.into(), [*v].as_slice()),
            FieldValue::UInt64(v) => Series::new(name.into(), [*v].as_slice()),
            FieldValue::Float32(v) => Series::new(name.into(), [*v].as_slice()),
            FieldValue::Float64(v) => Series::new(name.into(), [*v].as_slice()),
            FieldValue::Array(values) => self.array_to_series(name, values)?,
            FieldValue::Struct(nested) => self.value_to_series(nested)?.with_name(name.into()),
        };

        Ok(series)
    }

    /// Convert an array of FieldValues to a Polars Series with Array dtype.
    fn array_to_series(&self, name: &str, values: &[FieldValue]) -> Result<Series> {
        if values.is_empty() {
            return Err(WpilogError::ParseError("Empty array".to_string()));
        }

        // Determine the element type from the first value
        let elem_series = match &values[0] {
            FieldValue::Bool(_) => {
                let vals: Vec<bool> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::Bool(b) => *b,
                        _ => panic!("Inconsistent array types"),
                    })
                    .collect();
                Series::new("".into(), vals.as_slice())
            }
            FieldValue::Int8(_) => {
                let vals: Vec<i8> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::Int8(i) => *i,
                        _ => panic!("Inconsistent array types"),
                    })
                    .collect();
                Series::new("".into(), vals.as_slice())
            }
            FieldValue::Int16(_) => {
                let vals: Vec<i16> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::Int16(i) => *i,
                        _ => panic!("Inconsistent array types"),
                    })
                    .collect();
                Series::new("".into(), vals.as_slice())
            }
            FieldValue::Int32(_) => {
                let vals: Vec<i32> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::Int32(i) => *i,
                        _ => panic!("Inconsistent array types"),
                    })
                    .collect();
                Series::new("".into(), vals.as_slice())
            }
            FieldValue::Int64(_) => {
                let vals: Vec<i64> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::Int64(i) => *i,
                        _ => panic!("Inconsistent array types"),
                    })
                    .collect();
                Series::new("".into(), vals.as_slice())
            }
            FieldValue::UInt8(_) => {
                let vals: Vec<u8> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::UInt8(i) => *i,
                        _ => panic!("Inconsistent array types"),
                    })
                    .collect();
                UInt8Chunked::from_slice("".into(), &vals).into_series()
            }
            FieldValue::UInt16(_) => {
                let vals: Vec<u16> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::UInt16(i) => *i,
                        _ => panic!("Inconsistent array types"),
                    })
                    .collect();
                UInt16Chunked::from_slice("".into(), &vals).into_series()
            }
            FieldValue::UInt32(_) => {
                let vals: Vec<u32> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::UInt32(i) => *i,
                        _ => panic!("Inconsistent array types"),
                    })
                    .collect();
                Series::new("".into(), vals.as_slice())
            }
            FieldValue::UInt64(_) => {
                let vals: Vec<u64> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::UInt64(i) => *i,
                        _ => panic!("Inconsistent array types"),
                    })
                    .collect();
                Series::new("".into(), vals.as_slice())
            }
            FieldValue::Float32(_) => {
                let vals: Vec<f32> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::Float32(f) => *f,
                        _ => panic!("Inconsistent array types"),
                    })
                    .collect();
                Series::new("".into(), vals.as_slice())
            }
            FieldValue::Float64(_) => {
                let vals: Vec<f64> = values
                    .iter()
                    .map(|v| match v {
                        FieldValue::Float64(f) => *f,
                        _ => panic!("Inconsistent array types"),
                    })
                    .collect();
                Series::new("".into(), vals.as_slice())
            }
            FieldValue::Struct(_) => {
                return Err(WpilogError::ParseError(
                    "Arrays of structs not yet supported in Polars conversion".to_string(),
                ));
            }
            _ => {
                return Err(WpilogError::ParseError(
                    "Unsupported array element type".to_string(),
                ));
            }
        };

        // Convert to a List (single row containing the list)
        // Wrap the series in a list
        let list_series = Series::new(name.into(), &[elem_series]);
        Ok(list_series)
    }

    /// Convert multiple struct values to a Polars Series (multiple rows).
    pub fn values_to_series(&self, struct_name: &str, values: &[StructValue]) -> Result<Series> {
        if values.is_empty() {
            // Return an empty struct series
            let dtype = self.schema_to_dtype(struct_name)?;
            return Ok(Series::new_empty(PlSmallStr::from(""), &dtype));
        }

        let schema = self.registry.get(struct_name).ok_or_else(|| {
            WpilogError::SchemaError(format!("Struct '{}' not found in registry", struct_name))
        })?;

        // Collect all values for each field
        let mut field_series_vec = Vec::new();

        for field in &schema.fields {
            let field_name = match field {
                StructField::Standard(f) => &f.name,
                StructField::BitField(f) => &f.name,
            };

            // Collect values for this field from all struct values
            let mut series_vec = Vec::new();
            for value in values {
                let field_value = value.fields.get(field_name).ok_or_else(|| {
                    WpilogError::ParseError(format!(
                        "Field '{}' not found in struct value",
                        field_name
                    ))
                })?;

                let series = self.field_value_to_series(field_name, field_value)?;
                series_vec.push(series);
            }

            // Concatenate all series for this field
            let concatenated = if series_vec.len() == 1 {
                series_vec.into_iter().next().unwrap()
            } else {
                // Concatenate series vertically using append
                let mut base = series_vec[0].clone();
                for s in &series_vec[1..] {
                    base.append(s)?;
                }
                base
            };

            field_series_vec.push(concatenated);
        }

        // Create a StructChunked from the field series
        // The second parameter should be the length of the series (number of rows), not number of fields
        let len = field_series_vec.first().map(|s| s.len()).unwrap_or(0);
        let series_refs: Vec<&Series> = field_series_vec.iter().collect();
        let struct_chunked =
            StructChunked::from_series(PlSmallStr::from(""), len, series_refs.into_iter())?;

        Ok(struct_chunked.into_series())
    }

    /// Convert a slice of optional struct values to a Polars Series.
    /// This preserves null values for sparse data.
    pub fn optional_values_to_series(
        &self,
        struct_name: &str,
        values: &[Option<StructValue>],
    ) -> Result<Series> {
        if values.is_empty() {
            // Return an empty struct series
            let dtype = self.schema_to_dtype(struct_name)?;
            return Ok(Series::new_empty(PlSmallStr::from(""), &dtype));
        }

        let schema = self.registry.get(struct_name).ok_or_else(|| {
            WpilogError::SchemaError(format!("Struct '{}' not found in registry", struct_name))
        })?;

        // Collect all values for each field, preserving nulls
        let mut field_series_vec = Vec::new();

        for field in &schema.fields {
            let field_name = match field {
                StructField::Standard(f) => &f.name,
                StructField::BitField(f) => &f.name,
            };

            // Collect values for this field from all struct values, with nulls
            let mut series_vec = Vec::new();
            for opt_value in values {
                let series = match opt_value {
                    Some(value) => {
                        let field_value = value.fields.get(field_name).ok_or_else(|| {
                            WpilogError::ParseError(format!(
                                "Field '{}' not found in struct value",
                                field_name
                            ))
                        })?;
                        self.field_value_to_series(field_name, field_value)?
                    }
                    None => {
                        // Create a null value for this field
                        self.create_null_field_series(field_name, field)?
                    }
                };
                series_vec.push(series);
            }

            // Concatenate all series for this field
            let concatenated = if series_vec.len() == 1 {
                series_vec.into_iter().next().unwrap()
            } else {
                // Concatenate series vertically using append
                let mut base = series_vec[0].clone();
                for s in &series_vec[1..] {
                    base.append(s)?;
                }
                base
            };

            field_series_vec.push(concatenated);
        }

        // Create a StructChunked from the field series
        let len = field_series_vec.first().map(|s| s.len()).unwrap_or(0);
        let series_refs: Vec<&Series> = field_series_vec.iter().collect();
        let struct_chunked =
            StructChunked::from_series(PlSmallStr::from(""), len, series_refs.into_iter())?;

        Ok(struct_chunked.into_series())
    }

    /// Create a null value series for a field
    fn create_null_field_series(&self, name: &str, field: &StructField) -> Result<Series> {
        match field {
            StructField::Standard(std_field) => {
                let dtype = self.field_type_to_dtype(&std_field.field_type)?;
                Ok(Series::full_null(name.into(), 1, &dtype))
            }
            StructField::BitField(_) => {
                // Bit-fields are represented as Int64
                Ok(Series::full_null(name.into(), 1, &DataType::Int64))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::deserializer::StructDeserializer;
    use super::*;
    use byteorder::{ByteOrder, LittleEndian};

    #[test]
    fn test_schema_to_dtype() {
        let mut registry = StructRegistry::new();
        registry
            .register("Translation2d".to_string(), "double x; double y")
            .unwrap();

        let converter = PolarsConverter::new(&registry);
        let dtype = converter.schema_to_dtype("Translation2d").unwrap();

        match dtype {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name().as_str(), "x");
                assert_eq!(fields[0].dtype(), &DataType::Float64);
                assert_eq!(fields[1].name().as_str(), "y");
                assert_eq!(fields[1].dtype(), &DataType::Float64);
            }
            _ => panic!("Expected Struct type"),
        }
    }

    #[test]
    fn test_value_to_series() {
        let mut registry = StructRegistry::new();
        registry
            .register("Translation2d".to_string(), "double x; double y")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);
        let converter = PolarsConverter::new(&registry);

        // Create binary data: x=1.5, y=2.5
        let mut data = vec![0u8; 16];
        LittleEndian::write_f64(&mut data[0..8], 1.5);
        LittleEndian::write_f64(&mut data[8..16], 2.5);

        let value = deserializer.deserialize("Translation2d", &data).unwrap();
        let series = converter.value_to_series(&value).unwrap();

        assert_eq!(series.len(), 1);
        assert_eq!(
            series.dtype(),
            &DataType::Struct(vec![
                Field::new("x".into(), DataType::Float64),
                Field::new("y".into(), DataType::Float64),
            ])
        );
    }

    #[test]
    fn test_values_to_series() {
        let mut registry = StructRegistry::new();
        registry
            .register("Translation2d".to_string(), "double x; double y")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);
        let converter = PolarsConverter::new(&registry);

        // Create two struct values
        let mut data1 = vec![0u8; 16];
        LittleEndian::write_f64(&mut data1[0..8], 1.0);
        LittleEndian::write_f64(&mut data1[8..16], 2.0);
        let value1 = deserializer.deserialize("Translation2d", &data1).unwrap();

        let mut data2 = vec![0u8; 16];
        LittleEndian::write_f64(&mut data2[0..8], 3.0);
        LittleEndian::write_f64(&mut data2[8..16], 4.0);
        let value2 = deserializer.deserialize("Translation2d", &data2).unwrap();

        let series = converter
            .values_to_series("Translation2d", &[value1, value2])
            .unwrap();

        assert_eq!(series.len(), 2);
    }

    #[test]
    fn test_optional_values_to_series() {
        // Test converting optional struct values (for sparse data)
        let mut registry = StructRegistry::new();
        registry
            .register("Point".to_string(), "double x; double y")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);
        let converter = PolarsConverter::new(&registry);

        // Create struct values with some None values
        let mut data1 = vec![0u8; 16];
        LittleEndian::write_f64(&mut data1[0..8], 1.0);
        LittleEndian::write_f64(&mut data1[8..16], 2.0);
        let value1 = deserializer.deserialize("Point", &data1).unwrap();

        let mut data2 = vec![0u8; 16];
        LittleEndian::write_f64(&mut data2[0..8], 3.0);
        LittleEndian::write_f64(&mut data2[8..16], 4.0);
        let value2 = deserializer.deserialize("Point", &data2).unwrap();

        // Create optional values: Some, None, Some
        let optional_values = vec![Some(value1), None, Some(value2)];

        let series = converter
            .optional_values_to_series("Point", &optional_values)
            .unwrap();

        assert_eq!(series.len(), 3);

        // For structs, Polars represents None by having null values in the fields
        // Get the struct fields using field_by_name
        let struct_chunked = series.struct_().unwrap();

        // Check the x field
        let x_field = struct_chunked.field_by_name("x").unwrap();
        let x_null = x_field.is_null();
        assert!(!x_null.get(0).unwrap()); // First x should not be null
        assert!(x_null.get(1).unwrap()); // Second x should be null (from None value)
        assert!(!x_null.get(2).unwrap()); // Third x should not be null

        // Check the y field
        let y_field = struct_chunked.field_by_name("y").unwrap();
        let y_null = y_field.is_null();
        assert!(!y_null.get(0).unwrap()); // First y should not be null
        assert!(y_null.get(1).unwrap()); // Second y should be null (from None value)
        assert!(!y_null.get(2).unwrap()); // Third y should not be null
    }

    #[test]
    fn test_struct_array_to_list_series() {
        // Test converting struct arrays to Polars List(Struct) series
        let mut registry = StructRegistry::new();
        registry
            .register("Velocity".to_string(), "double vx; double vy")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);
        let converter = PolarsConverter::new(&registry);

        // Create two arrays of structs
        // Array 1: [(1.0, 2.0), (3.0, 4.0)]
        let mut data1_1 = vec![0u8; 16];
        LittleEndian::write_f64(&mut data1_1[0..8], 1.0);
        LittleEndian::write_f64(&mut data1_1[8..16], 2.0);
        let value1_1 = deserializer.deserialize("Velocity", &data1_1).unwrap();

        let mut data1_2 = vec![0u8; 16];
        LittleEndian::write_f64(&mut data1_2[0..8], 3.0);
        LittleEndian::write_f64(&mut data1_2[8..16], 4.0);
        let value1_2 = deserializer.deserialize("Velocity", &data1_2).unwrap();

        let array1 = vec![value1_1, value1_2];

        // Array 2: [(5.0, 6.0)]
        let mut data2_1 = vec![0u8; 16];
        LittleEndian::write_f64(&mut data2_1[0..8], 5.0);
        LittleEndian::write_f64(&mut data2_1[8..16], 6.0);
        let value2_1 = deserializer.deserialize("Velocity", &data2_1).unwrap();

        let array2 = vec![value2_1];

        // Convert each array to a series
        let series1 = converter.values_to_series("Velocity", &array1).unwrap();
        let series2 = converter.values_to_series("Velocity", &array2).unwrap();

        // Create a List series from these
        let list_series = Series::new("velocities".into(), vec![series1, series2]);

        assert_eq!(list_series.len(), 2);

        // Verify the dtype is List(Struct)
        match list_series.dtype() {
            DataType::List(inner) => {
                match inner.as_ref() {
                    DataType::Struct(_) => {} // Success
                    _ => panic!("Expected List(Struct), got List({})", inner),
                }
            }
            _ => panic!("Expected List type, got {}", list_series.dtype()),
        }
    }

    #[test]
    fn test_empty_struct_array() {
        // Test converting empty struct arrays
        let mut registry = StructRegistry::new();
        registry
            .register("Point".to_string(), "double x; double y")
            .unwrap();

        let converter = PolarsConverter::new(&registry);

        // Create empty struct array and verify the dtype
        let struct_dtype = converter.schema_to_dtype("Point").unwrap();
        let empty_series = Series::new_empty("".into(), &struct_dtype);

        assert_eq!(empty_series.len(), 0);

        // Verify the dtype is correct
        match empty_series.dtype() {
            DataType::Struct(_) => {} // Success
            _ => panic!("Expected Struct type, got {}", empty_series.dtype()),
        }
    }

    #[test]
    fn test_nested_struct_in_array() {
        // Test struct arrays where each element contains nested structs
        let mut registry = StructRegistry::new();

        // Register nested struct
        registry
            .register("Vector2d".to_string(), "double x; double y")
            .unwrap();

        // Register struct with nested field
        registry
            .register("Pose".to_string(), "Vector2d position; double rotation")
            .unwrap();

        let deserializer = StructDeserializer::new(&registry);
        let converter = PolarsConverter::new(&registry);

        // Create struct array with nested structs
        let mut data1 = vec![0u8; 24];
        LittleEndian::write_f64(&mut data1[0..8], 1.0); // position.x
        LittleEndian::write_f64(&mut data1[8..16], 2.0); // position.y
        LittleEndian::write_f64(&mut data1[16..24], 0.5); // rotation
        let value1 = deserializer.deserialize("Pose", &data1).unwrap();

        let mut data2 = vec![0u8; 24];
        LittleEndian::write_f64(&mut data2[0..8], 3.0); // position.x
        LittleEndian::write_f64(&mut data2[8..16], 4.0); // position.y
        LittleEndian::write_f64(&mut data2[16..24], 1.5); // rotation
        let value2 = deserializer.deserialize("Pose", &data2).unwrap();

        let series = converter
            .values_to_series("Pose", &[value1, value2])
            .unwrap();

        assert_eq!(series.len(), 2);

        // Verify the dtype contains nested struct
        match series.dtype() {
            DataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name().as_str(), "position");
                assert_eq!(fields[1].name().as_str(), "rotation");

                // Check that position is a nested struct
                match fields[0].dtype() {
                    DataType::Struct(nested_fields) => {
                        assert_eq!(nested_fields.len(), 2);
                        assert_eq!(nested_fields[0].name().as_str(), "x");
                        assert_eq!(nested_fields[1].name().as_str(), "y");
                    }
                    _ => panic!("Expected nested Struct for position field"),
                }
            }
            _ => panic!("Expected Struct type"),
        }
    }
}
