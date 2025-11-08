//! WPILib struct support for parsing packed binary structures.
//!
//! This module implements the WPILib struct serialization specification,
//! allowing binary struct data to be deserialized into Polars Struct types.

pub mod deserializer;
pub mod parser;
pub mod polars_converter;
pub mod registry;
pub mod types;

pub use deserializer::{FieldValue, StructDeserializer, StructValue};
pub use parser::SchemaParser;
pub use polars_converter::PolarsConverter;
pub use registry::StructRegistry;
pub use types::*;
