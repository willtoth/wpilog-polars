//! Column builders for constructing Polars Series with sparse data support.
//!
//! This module provides builders that handle:
//! - Sparse data (columns not updated at every timestamp)
//! - Null-filling for missing values
//! - Pre-allocation for performance
//! - Proper Polars List serialization for array types

use crate::error::{Result, WpilogError};
use crate::struct_support::{PolarsConverter, StructRegistry};
use crate::types::{PolarsDataType, PolarsValue};
use polars::prelude::*;

/// A builder for a single column with sparse data support.
pub struct ColumnBuilder {
    name: String,
    dtype: PolarsDataType,
    values: Vec<Option<PolarsValue>>,
}

impl ColumnBuilder {
    /// Creates a new ColumnBuilder with pre-allocated capacity.
    pub fn new(name: String, dtype: PolarsDataType, capacity: usize) -> Self {
        Self {
            name,
            dtype,
            values: Vec::with_capacity(capacity),
        }
    }

    /// Adds a value to the builder.
    pub fn push(&mut self, value: Option<PolarsValue>) {
        self.values.push(value);
    }

    /// Adds a null value to the builder.
    pub fn push_null(&mut self) {
        self.values.push(None);
    }

    /// Returns the number of values in the builder.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Builds a Polars Series from the accumulated values.
    pub fn build(self, registry: Option<&StructRegistry>) -> Result<Series> {
        match self.dtype {
            PolarsDataType::Float64 => {
                let values: Vec<Option<f64>> = self
                    .values
                    .into_iter()
                    .map(|opt| match opt {
                        Some(PolarsValue::Float64(v)) => Some(v),
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(self.name.as_str().into(), values))
            }
            PolarsDataType::Float32 => {
                let values: Vec<Option<f32>> = self
                    .values
                    .into_iter()
                    .map(|opt| match opt {
                        Some(PolarsValue::Float32(v)) => Some(v),
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(self.name.as_str().into(), values))
            }
            PolarsDataType::Int64 => {
                let values: Vec<Option<i64>> = self
                    .values
                    .into_iter()
                    .map(|opt| match opt {
                        Some(PolarsValue::Int64(v)) => Some(v),
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(self.name.as_str().into(), values))
            }
            PolarsDataType::Boolean => {
                let values: Vec<Option<bool>> = self
                    .values
                    .into_iter()
                    .map(|opt| match opt {
                        Some(PolarsValue::Boolean(v)) => Some(v),
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(self.name.as_str().into(), values))
            }
            PolarsDataType::String => {
                let values: Vec<Option<String>> = self
                    .values
                    .into_iter()
                    .map(|opt| match opt {
                        Some(PolarsValue::String(v)) => Some(v),
                        _ => None,
                    })
                    .collect();
                Ok(Series::new(self.name.as_str().into(), values))
            }
            PolarsDataType::BooleanArray => {
                let values: Vec<Option<Series>> = self
                    .values
                    .into_iter()
                    .map(|opt| match opt {
                        Some(PolarsValue::BooleanArray(v)) => Some(Series::new("".into(), v)),
                        _ => None,
                    })
                    .collect();
                let list_series = Series::new(self.name.as_str().into(), values);
                Ok(list_series)
            }
            PolarsDataType::Int64Array => {
                let values: Vec<Option<Series>> = self
                    .values
                    .into_iter()
                    .map(|opt| match opt {
                        Some(PolarsValue::Int64Array(v)) => Some(Series::new("".into(), v)),
                        _ => None,
                    })
                    .collect();
                let list_series = Series::new(self.name.as_str().into(), values);
                Ok(list_series)
            }
            PolarsDataType::Float32Array => {
                let values: Vec<Option<Series>> = self
                    .values
                    .into_iter()
                    .map(|opt| match opt {
                        Some(PolarsValue::Float32Array(v)) => Some(Series::new("".into(), v)),
                        _ => None,
                    })
                    .collect();
                let list_series = Series::new(self.name.as_str().into(), values);
                Ok(list_series)
            }
            PolarsDataType::Float64Array => {
                let values: Vec<Option<Series>> = self
                    .values
                    .into_iter()
                    .map(|opt| match opt {
                        Some(PolarsValue::Float64Array(v)) => Some(Series::new("".into(), v)),
                        _ => None,
                    })
                    .collect();
                let list_series = Series::new(self.name.as_str().into(), values);
                Ok(list_series)
            }
            PolarsDataType::StringArray => {
                let values: Vec<Option<Series>> = self
                    .values
                    .into_iter()
                    .map(|opt| match opt {
                        Some(PolarsValue::StringArray(v)) => Some(Series::new("".into(), v)),
                        _ => None,
                    })
                    .collect();
                let list_series = Series::new(self.name.as_str().into(), values);
                Ok(list_series)
            }
            PolarsDataType::Struct(ref struct_name) => {
                // Convert struct values to Polars structs
                if let Some(reg) = registry {
                    let converter = PolarsConverter::new(reg);

                    // Collect struct values, preserving None for sparse data
                    let struct_values: Vec<Option<crate::struct_support::StructValue>> = self
                        .values
                        .into_iter()
                        .map(|opt| match opt {
                            Some(PolarsValue::Struct(sv)) => Some(sv),
                            _ => None,
                        })
                        .collect();

                    if struct_values.is_empty() {
                        // Return empty struct series
                        let dtype = converter.schema_to_dtype(struct_name)?;
                        return Ok(Series::new_empty(self.name.as_str().into(), &dtype));
                    }

                    // Convert all struct values to a single series, preserving nulls
                    let series =
                        converter.optional_values_to_series(struct_name, &struct_values)?;
                    Ok(series.with_name(self.name.as_str().into()))
                } else {
                    // Fallback: convert to hex strings if no registry available
                    Err(WpilogError::SchemaError(
                        "Struct registry required for struct columns".to_string(),
                    ))
                }
            }
            PolarsDataType::StructArray(ref struct_name) => {
                // Convert struct array values to Polars List(Struct)
                if let Some(reg) = registry {
                    let converter = PolarsConverter::new(reg);

                    // Collect struct array values, preserving None for sparse data
                    let struct_array_values: Vec<Option<Vec<crate::struct_support::StructValue>>> =
                        self.values
                            .into_iter()
                            .map(|opt| match opt {
                                Some(PolarsValue::StructArray(svs)) => Some(svs),
                                _ => None,
                            })
                            .collect();

                    if struct_array_values.is_empty() {
                        // Return empty list series
                        let struct_dtype = converter.schema_to_dtype(struct_name)?;
                        let list_dtype = DataType::List(Box::new(struct_dtype));
                        return Ok(Series::new_empty(self.name.as_str().into(), &list_dtype));
                    }

                    // Convert to List(Struct) series
                    let mut list_series_vec = Vec::new();
                    for opt_structs in struct_array_values {
                        let series = match opt_structs {
                            Some(structs) if !structs.is_empty() => {
                                // Convert array of structs to a series
                                converter.values_to_series(struct_name, &structs)?
                            }
                            _ => {
                                // Empty or null array
                                let struct_dtype = converter.schema_to_dtype(struct_name)?;
                                Series::new_empty("".into(), &struct_dtype)
                            }
                        };
                        list_series_vec.push(series);
                    }

                    // Create a List series from the struct series
                    let list_series = Series::new(self.name.as_str().into(), list_series_vec);
                    Ok(list_series)
                } else {
                    // Fallback: convert to hex strings if no registry available
                    Err(WpilogError::SchemaError(
                        "Struct registry required for struct array columns".to_string(),
                    ))
                }
            }
        }
    }
}

/// A collection of column builders for constructing a DataFrame.
pub struct DataFrameBuilder<'a> {
    timestamp: Vec<i64>,
    columns: Vec<ColumnBuilder>,
    registry: Option<&'a StructRegistry>,
}

impl<'a> DataFrameBuilder<'a> {
    /// Creates a new DataFrameBuilder with pre-allocated capacity.
    /// Uses ~25 bytes per record as an estimate.
    pub fn new(
        column_names: Vec<String>,
        column_types: Vec<PolarsDataType>,
        capacity: usize,
    ) -> Self {
        let columns = column_names
            .into_iter()
            .zip(column_types.into_iter())
            .map(|(name, dtype)| ColumnBuilder::new(name, dtype, capacity))
            .collect();

        Self {
            timestamp: Vec::with_capacity(capacity),
            columns,
            registry: None,
        }
    }

    /// Sets the struct registry for this builder.
    pub fn with_registry(mut self, registry: &'a StructRegistry) -> Self {
        self.registry = Some(registry);
        self
    }

    /// Adds a row to the builder.
    /// Values is a sparse map from column index to value.
    pub fn push_row(&mut self, timestamp: i64, values: &[Option<PolarsValue>]) {
        self.timestamp.push(timestamp);

        for (i, builder) in self.columns.iter_mut().enumerate() {
            if let Some(value) = values.get(i) {
                builder.push(value.clone());
            } else {
                builder.push_null();
            }
        }
    }

    /// Builds a Polars DataFrame from the accumulated data.
    pub fn build(self) -> Result<DataFrame> {
        let mut columns = Vec::with_capacity(self.columns.len() + 1);

        // Add timestamp column first
        columns.push(Series::new("timestamp".into(), self.timestamp).into());

        // Add all other columns
        for builder in self.columns {
            columns.push(builder.build(self.registry)?.into());
        }

        DataFrame::new(columns).map_err(|e| WpilogError::PolarsError(e))
    }

    /// Returns the number of rows currently in the builder.
    pub fn len(&self) -> usize {
        self.timestamp.len()
    }

    /// Returns true if the builder is empty.
    pub fn is_empty(&self) -> bool {
        self.timestamp.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_builder_float64() {
        let mut builder = ColumnBuilder::new("test".to_string(), PolarsDataType::Float64, 10);

        builder.push(Some(PolarsValue::Float64(1.0)));
        builder.push(None);
        builder.push(Some(PolarsValue::Float64(3.0)));

        let series = builder.build(None).unwrap();
        assert_eq!(series.len(), 3);
        assert_eq!(series.name(), "test");
    }

    #[test]
    fn test_column_builder_int64_array() {
        let mut builder = ColumnBuilder::new("test".to_string(), PolarsDataType::Int64Array, 10);

        builder.push(Some(PolarsValue::Int64Array(vec![1, 2, 3])));
        builder.push(None);
        builder.push(Some(PolarsValue::Int64Array(vec![4, 5])));

        let series = builder.build(None).unwrap();
        assert_eq!(series.len(), 3);
    }

    #[test]
    fn test_dataframe_builder() {
        let mut builder = DataFrameBuilder::new(
            vec!["col1".to_string(), "col2".to_string()],
            vec![PolarsDataType::Float64, PolarsDataType::Int64],
            10,
        );

        builder.push_row(
            1000,
            &[
                Some(PolarsValue::Float64(1.5)),
                Some(PolarsValue::Int64(42)),
            ],
        );

        builder.push_row(2000, &[None, Some(PolarsValue::Int64(43))]);

        let df = builder.build().unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.width(), 3); // timestamp + 2 columns
    }
}
