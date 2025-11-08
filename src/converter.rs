//! Main conversion logic from WPILog to Polars DataFrame.
//!
//! This module implements the two-pass algorithm:
//! 1. First pass: Infer schema from START control records and build struct registry
//! 2. Second pass: Accumulate data into column builders

use crate::builders::DataFrameBuilder;
use crate::datalog::{DataLogReader, DataLogRecord};
use crate::error::{Result, WpilogError};
use crate::schema::WpilogSchema;
use crate::struct_support::{StructDeserializer, StructRegistry};
use crate::types::{PolarsDataType, PolarsValue};
use polars::prelude::*;

/// Converts WPILog binary data to a Polars DataFrame.
pub struct WpilogConverter;

impl WpilogConverter {
    /// Converts WPILog data from a byte slice to a Polars DataFrame.
    pub fn from_bytes(data: &[u8]) -> Result<DataFrame> {
        let reader = DataLogReader::new(data);

        if !reader.is_valid() {
            return Err(WpilogError::InvalidFormat(
                "Invalid WPILog file header".to_string(),
            ));
        }

        // First pass: build struct registry and infer schema
        let (registry, schema) = Self::build_registry_and_schema(&reader)?;

        // Second pass: accumulate data
        Self::accumulate_data(reader, &schema, registry)
    }

    /// First pass: builds struct registry from structschema entries and infers schema.
    /// Optimized to use a single loop by processing both struct schemas and main schema columns simultaneously.
    fn build_registry_and_schema(reader: &DataLogReader) -> Result<(StructRegistry, WpilogSchema)> {
        let mut registry = StructRegistry::new();
        let mut schema = WpilogSchema::new();
        let mut schema_entries = std::collections::HashMap::new();
        let mut schema_defs = std::collections::HashMap::new();
        let mut finished_entries = std::collections::HashSet::new();

        // Single pass: collect struct schema definitions AND infer main schema simultaneously
        for record_result in reader.records()? {
            let record = record_result?;

            if record.is_start() {
                let start_data = record.get_start_data()?;

                // Check if this is a struct schema definition entry
                if start_data.type_name == "structschema" {
                    // Store the mapping from entry ID to schema name
                    // Format: "/.schema/struct:StructName" -> "StructName"
                    let simple_name =
                        if let Some(stripped) = start_data.name.strip_prefix("/.schema/struct:") {
                            stripped.to_string()
                        } else {
                            start_data.name.clone()
                        };
                    schema_entries.insert(start_data.entry, simple_name);
                } else {
                    // This is a regular data column - add to schema (unless already finished)
                    if !finished_entries.contains(&start_data.entry) {
                        let dtype = PolarsDataType::from_wpilog_type(&start_data.type_name)?;
                        let column = crate::schema::ColumnInfo {
                            entry_id: start_data.entry,
                            name: start_data.name,
                            dtype,
                            nullable: true,
                            metadata: start_data.metadata,
                        };
                        schema.add_column(column);
                    }
                }
            } else if record.is_finish() {
                // Track finished entries (needed by both processes)
                let entry_id = record.get_finish_entry()?;
                finished_entries.insert(entry_id);
            } else if !record.is_control() && schema_entries.contains_key(&record.entry) {
                // This is struct schema data - collect it immediately
                let struct_name = schema_entries.get(&record.entry).unwrap().clone();
                let schema_text = record.get_string();
                schema_defs.insert(struct_name, schema_text);
            }
        }

        // Validate that we found at least one column
        if schema.num_columns() == 0 {
            return Err(WpilogError::SchemaError(
                "No columns found in WPILog file".to_string(),
            ));
        }

        // Register structs with dependency resolution (retry until all are registered or no progress)
        let mut registered = std::collections::HashSet::new();

        // Loop until all structs are registered or we make no progress in an iteration
        while registered.len() < schema_defs.len() {
            let start_count = registered.len();

            for (struct_name, schema_text) in &schema_defs {
                if registered.contains(struct_name) {
                    continue;
                }

                // Try to register this struct (ignore errors, will retry in next iteration)
                if registry.register(struct_name.clone(), schema_text).is_ok() {
                    registered.insert(struct_name.clone());
                }
            }

            // If we made no progress in this iteration, break to avoid infinite loop
            if registered.len() == start_count {
                break;
            }
        }

        // Report any structs that couldn't be registered
        if registered.len() < schema_defs.len() {
            for (struct_name, _) in &schema_defs {
                if !registered.contains(struct_name) {
                    eprintln!(
                        "Warning: Failed to register struct '{}' - possible missing dependency",
                        struct_name
                    );
                }
            }
        }

        Ok((registry, schema))
    }

    /// Second pass: accumulates data into a DataFrame.
    fn accumulate_data(
        reader: DataLogReader,
        schema: &WpilogSchema,
        registry: StructRegistry,
    ) -> Result<DataFrame> {
        // Create deserializer for struct data (using reference to avoid any cloning)
        let deserializer = StructDeserializer::new(&registry);

        // Estimate capacity (rough approximation)
        let estimated_records = reader.data.len() / 25;

        // Build column names and types
        let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<PolarsDataType> =
            schema.columns().iter().map(|c| c.dtype.clone()).collect();

        // Create builder with registry reference (no cloning needed)
        let mut builder = DataFrameBuilder::new(column_names, column_types, estimated_records)
            .with_registry(&registry);

        // Track which columns have been updated for the current timestamp
        let mut current_timestamp: Option<i64> = None;
        let mut current_values: Vec<Option<PolarsValue>> = vec![None; schema.num_columns()];
        let mut finished_entries = std::collections::HashSet::new();

        for record_result in reader.records()? {
            let record = record_result?;

            // Skip control records (they were processed in schema inference)
            if record.is_control() {
                if record.is_finish() {
                    let entry_id = record.get_finish_entry()?;
                    finished_entries.insert(entry_id);
                }
                continue;
            }

            // Skip records for finished entries
            if finished_entries.contains(&record.entry) {
                continue;
            }

            // Get column info for this entry
            // Skip entries that aren't in the schema (e.g., structschema entries)
            let column_info = match schema.get_column_by_entry(record.entry) {
                Some(info) => info,
                None => continue, // Skip this entry
            };

            // Find the column index
            let column_index = schema
                .columns()
                .iter()
                .position(|c| c.entry_id == record.entry)
                .unwrap();

            // If this is a new timestamp, flush the previous row
            if let Some(ts) = current_timestamp {
                if ts != record.timestamp as i64 {
                    builder.push_row(ts, &current_values);
                    current_values = vec![None; schema.num_columns()];
                    current_timestamp = Some(record.timestamp as i64);
                }
            } else {
                current_timestamp = Some(record.timestamp as i64);
            }

            // Parse the record value based on its type
            let value = Self::parse_record_value(&record, &column_info.dtype, &deserializer)?;
            current_values[column_index] = Some(value);
        }

        // Flush the last row
        if let Some(ts) = current_timestamp {
            builder.push_row(ts, &current_values);
        }

        // Build the DataFrame
        builder.build()
    }

    /// Parses a data record value based on its type.
    fn parse_record_value(
        record: &DataLogRecord,
        dtype: &PolarsDataType,
        deserializer: &StructDeserializer,
    ) -> Result<PolarsValue> {
        match dtype {
            PolarsDataType::Float64 => Ok(PolarsValue::Float64(record.get_double()?)),
            PolarsDataType::Float32 => Ok(PolarsValue::Float32(record.get_float()?)),
            PolarsDataType::Int64 => Ok(PolarsValue::Int64(record.get_integer()?)),
            PolarsDataType::Boolean => Ok(PolarsValue::Boolean(record.get_boolean()?)),
            PolarsDataType::String => Ok(PolarsValue::String(record.get_string())),
            PolarsDataType::BooleanArray => {
                Ok(PolarsValue::BooleanArray(record.get_boolean_array()))
            }
            PolarsDataType::Int64Array => Ok(PolarsValue::Int64Array(record.get_integer_array()?)),
            PolarsDataType::Float32Array => {
                Ok(PolarsValue::Float32Array(record.get_float_array()?))
            }
            PolarsDataType::Float64Array => {
                Ok(PolarsValue::Float64Array(record.get_double_array()?))
            }
            PolarsDataType::StringArray => Ok(PolarsValue::StringArray(record.get_string_array()?)),
            PolarsDataType::Struct(struct_name) => {
                // Deserialize struct data using the registry
                let struct_value = deserializer.deserialize(struct_name, &record.data)?;
                Ok(PolarsValue::Struct(struct_value))
            }
            PolarsDataType::StructArray(struct_name) => {
                // Deserialize struct array data using the registry
                let struct_schema = deserializer.registry().get(struct_name).ok_or_else(|| {
                    WpilogError::SchemaError(format!(
                        "Struct '{}' not found in registry for array deserialization",
                        struct_name
                    ))
                })?;

                let struct_size = struct_schema.total_size;
                if record.data.len() % struct_size != 0 {
                    return Err(WpilogError::ParseError(format!(
                        "Invalid struct array size: {} is not a multiple of struct size {}",
                        record.data.len(),
                        struct_size
                    )));
                }

                // Deserialize each struct in the array
                let num_structs = record.data.len() / struct_size;
                let mut struct_values = Vec::with_capacity(num_structs);

                for i in 0..num_structs {
                    let start = i * struct_size;
                    let end = start + struct_size;
                    let struct_data = &record.data[start..end];
                    let struct_value = deserializer.deserialize(struct_name, struct_data)?;
                    struct_values.push(struct_value);
                }

                Ok(PolarsValue::StructArray(struct_values))
            }
        }
    }
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
        // Header byte: entry_len=1, size_len=1, timestamp_len=1
        data.push(0x00); // Header byte
        data.push(0x00); // Entry ID = 0 (control)
        data.push(26); // Payload size
        data.push(0x01); // Timestamp = 1

        // Start control payload
        data.push(0x00); // Control type = Start
        data.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // Entry ID = 1
        data.extend_from_slice(&[0x04, 0x00, 0x00, 0x00]); // Name length = 4
        data.extend_from_slice(b"test"); // Name = "test"
        data.extend_from_slice(&[0x05, 0x00, 0x00, 0x00]); // Type length = 5
        data.extend_from_slice(b"int64"); // Type = "int64"
        data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Metadata length = 0

        // Data record for entry ID 1 with value 42
        data.push(0x00); // Header byte
        data.push(0x01); // Entry ID = 1
        data.push(0x08); // Payload size = 8
        data.push(0x02); // Timestamp = 2
        data.extend_from_slice(&[0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // Value = 42

        data
    }

    #[test]
    fn test_converter_basic() {
        let data = create_test_wpilog();
        let df = WpilogConverter::from_bytes(&data).unwrap();

        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 2); // timestamp + test

        let col_names = df.get_column_names();
        assert!(col_names.iter().any(|s| s.as_str() == "timestamp"));
        assert!(col_names.iter().any(|s| s.as_str() == "test"));
    }
}
