//! Main conversion logic from WPILog to Polars DataFrame.
//!
//! This module implements the two-pass algorithm:
//! 1. First pass: Infer schema from START control records
//! 2. Second pass: Accumulate data into column builders

use crate::builders::DataFrameBuilder;
use crate::datalog::{DataLogReader, DataLogRecord};
use crate::error::{Result, WpilogError};
use crate::schema::WpilogSchema;
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

        // First pass: infer schema
        let schema = WpilogSchema::infer_from_records(reader.records()?)?;

        // Second pass: accumulate data
        Self::accumulate_data(reader, &schema)
    }

    /// Second pass: accumulates data into a DataFrame.
    fn accumulate_data(reader: DataLogReader, schema: &WpilogSchema) -> Result<DataFrame> {
        // Estimate capacity (rough approximation)
        let estimated_records = reader.data.len() / 25;

        // Build column names and types
        let column_names: Vec<String> = schema.columns().iter().map(|c| c.name.clone()).collect();
        let column_types: Vec<PolarsDataType> =
            schema.columns().iter().map(|c| c.dtype.clone()).collect();

        // Create builder
        let mut builder = DataFrameBuilder::new(column_names, column_types, estimated_records);

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
            let column_info = schema.get_column_by_entry(record.entry).ok_or_else(|| {
                WpilogError::InvalidEntry(format!("Entry ID {} not found in schema", record.entry))
            })?;

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
            let value = Self::parse_record_value(&record, &column_info.dtype)?;
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
    fn parse_record_value(record: &DataLogRecord, dtype: &PolarsDataType) -> Result<PolarsValue> {
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
