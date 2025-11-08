//! Schema inference and storage for WPILog files.
//!
//! This module provides:
//! - `ColumnInfo`: metadata about a single column
//! - `WpilogSchema`: collection of column information
//! - Schema inference from START control records

use crate::datalog::DataLogIterator;
use crate::error::{Result, WpilogError};
use crate::types::PolarsDataType;
use polars::prelude::*;
use std::collections::HashMap;

/// Information about a single column in the schema.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub entry_id: u32,
    pub name: String,
    pub dtype: PolarsDataType,
    pub nullable: bool,
    pub metadata: String,
}

/// Schema for a WPILog file, containing information about all columns.
#[derive(Debug, Clone)]
pub struct WpilogSchema {
    columns: Vec<ColumnInfo>,
    entry_to_index: HashMap<u32, usize>,
}

impl WpilogSchema {
    /// Creates a new empty schema.
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            entry_to_index: HashMap::new(),
        }
    }

    /// Adds a column to the schema.
    pub fn add_column(&mut self, column: ColumnInfo) {
        let index = self.columns.len();
        self.entry_to_index.insert(column.entry_id, index);
        self.columns.push(column);
    }

    /// Gets column information by entry ID.
    pub fn get_column_by_entry(&self, entry_id: u32) -> Option<&ColumnInfo> {
        self.entry_to_index
            .get(&entry_id)
            .and_then(|&idx| self.columns.get(idx))
    }

    /// Gets all columns in the schema.
    pub fn columns(&self) -> &[ColumnInfo] {
        &self.columns
    }

    /// Gets the number of columns (excluding timestamp).
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Converts to a Polars Schema.
    /// The resulting schema includes a "timestamp" column followed by all entry columns.
    pub fn to_polars_schema(&self) -> Schema {
        let mut fields = Vec::with_capacity(self.columns.len() + 1);

        // Timestamp column is always first
        fields.push(Field::new("timestamp".into(), DataType::Int64));

        // Add all entry columns
        for col in &self.columns {
            fields.push(Field::new(
                col.name.as_str().into(),
                col.dtype.to_polars_dtype(),
            ));
        }

        Schema::from_iter(fields)
    }

    /// Infers schema from a WPILog file by reading all START control records.
    pub fn infer_from_records(mut records: DataLogIterator) -> Result<Self> {
        let mut schema = Self::new();
        let mut finished_entries = std::collections::HashSet::new();

        for record_result in records.by_ref() {
            let record = record_result?;

            if record.is_start() {
                let start_data = record.get_start_data()?;

                // Skip if this entry was already finished and is being reused
                if finished_entries.contains(&start_data.entry) {
                    continue;
                }

                // Skip structschema entries - they are metadata, not data columns
                if start_data.type_name == "structschema" {
                    continue;
                }

                let dtype = PolarsDataType::from_wpilog_type(&start_data.type_name)?;

                let column = ColumnInfo {
                    entry_id: start_data.entry,
                    name: start_data.name,
                    dtype,
                    nullable: true, // All columns are nullable for sparse data
                    metadata: start_data.metadata,
                };

                schema.add_column(column);
            } else if record.is_finish() {
                let entry_id = record.get_finish_entry()?;
                finished_entries.insert(entry_id);
            }
        }

        if schema.num_columns() == 0 {
            return Err(WpilogError::SchemaError(
                "No columns found in WPILog file".to_string(),
            ));
        }

        Ok(schema)
    }
}

impl Default for WpilogSchema {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let mut schema = WpilogSchema::new();

        schema.add_column(ColumnInfo {
            entry_id: 1,
            name: "test".to_string(),
            dtype: PolarsDataType::Float64,
            nullable: true,
            metadata: String::new(),
        });

        assert_eq!(schema.num_columns(), 1);
        assert!(schema.get_column_by_entry(1).is_some());
        assert!(schema.get_column_by_entry(2).is_none());
    }

    #[test]
    fn test_to_polars_schema() {
        let mut schema = WpilogSchema::new();

        schema.add_column(ColumnInfo {
            entry_id: 1,
            name: "value".to_string(),
            dtype: PolarsDataType::Float64,
            nullable: true,
            metadata: String::new(),
        });

        let polars_schema = schema.to_polars_schema();
        assert_eq!(polars_schema.len(), 2); // timestamp + value
        assert!(polars_schema.get("timestamp").is_some());
        assert!(polars_schema.get("value").is_some());
    }
}
