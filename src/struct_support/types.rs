//! Core types for WPILib struct support.

use std::collections::HashMap;

/// A complete struct schema definition.
#[derive(Debug, Clone)]
pub struct StructSchema {
    pub name: String,
    pub fields: Vec<StructField>,
    pub total_size: usize,
}

/// A field in a struct (either standard or bit-field).
#[derive(Debug, Clone)]
pub enum StructField {
    Standard(StandardField),
    BitField(BitFieldDecl),
}

/// A standard field declaration.
#[derive(Debug, Clone)]
pub struct StandardField {
    pub name: String,
    pub field_type: FieldType,
    pub offset: usize,
    pub size: usize,
    pub enum_spec: Option<EnumSpec>,
}

/// A bit-field declaration.
#[derive(Debug, Clone)]
pub struct BitFieldDecl {
    pub name: String,
    pub int_type: IntegerType,
    pub bit_width: usize,
    pub storage_offset: usize,
    pub bit_offset: usize,
    pub spans_units: bool,
    pub enum_spec: Option<EnumSpec>,
}

/// Field data type.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldType {
    Bool,
    Char,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Array {
        elem_type: Box<FieldType>,
        length: usize,
    },
    Struct(String), // Reference to another struct by name
}

/// Integer type for bit-fields.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum IntegerType {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
}

impl IntegerType {
    /// Get the size in bytes of this integer type.
    pub fn size(self) -> usize {
        match self {
            IntegerType::Bool | IntegerType::Int8 | IntegerType::UInt8 => 1,
            IntegerType::Int16 | IntegerType::UInt16 => 2,
            IntegerType::Int32 | IntegerType::UInt32 => 4,
            IntegerType::Int64 | IntegerType::UInt64 => 8,
        }
    }

    /// Get the maximum bit width for this type.
    pub fn max_bits(self) -> u8 {
        match self {
            IntegerType::Bool => 1,
            IntegerType::Int8 | IntegerType::UInt8 => 8,
            IntegerType::Int16 | IntegerType::UInt16 => 16,
            IntegerType::Int32 | IntegerType::UInt32 => 32,
            IntegerType::Int64 | IntegerType::UInt64 => 64,
        }
    }
}

/// Enum specification for integer fields.
#[derive(Debug, Clone)]
pub struct EnumSpec {
    pub values: HashMap<i64, String>, // value -> name mapping
}

impl FieldType {
    /// Get the size in bytes of this field type (non-recursive).
    pub fn primitive_size(&self) -> Option<usize> {
        match self {
            FieldType::Bool | FieldType::Char | FieldType::Int8 | FieldType::UInt8 => Some(1),
            FieldType::Int16 | FieldType::UInt16 => Some(2),
            FieldType::Int32 | FieldType::UInt32 | FieldType::Float32 => Some(4),
            FieldType::Int64 | FieldType::UInt64 | FieldType::Float64 => Some(8),
            _ => None,
        }
    }
}
