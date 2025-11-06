//! WPILog binary format parser.
//!
//! This module provides low-level parsing of WPILog binary data, including:
//! - File header validation
//! - Record iteration
//! - Control record (Start, Finish, Set Metadata) parsing
//! - Data record type extraction (boolean, int64, float, double, string, arrays)

use crate::error::{Result, WpilogError};
use byteorder::{LittleEndian, ReadBytesExt};
use std::io::Cursor;

const CONTROL_START: u8 = 0;
const CONTROL_FINISH: u8 = 1;
const CONTROL_SET_METADATA: u8 = 2;

/// Data contained in a start control record.
#[derive(Debug, Clone)]
pub struct StartRecordData {
    pub entry: u32,
    pub name: String,
    pub type_name: String,
    pub metadata: String,
}

/// Data contained in a set metadata control record.
#[derive(Debug, Clone)]
pub struct MetadataRecordData {
    pub entry: u32,
    pub metadata: String,
}

/// A record in the data log.
#[derive(Debug, Clone)]
pub struct DataLogRecord {
    pub entry: u32,
    pub timestamp: u64,
    pub data: Vec<u8>,
}

impl DataLogRecord {
    /// Returns true if the record is a control record (entry ID 0).
    pub fn is_control(&self) -> bool {
        self.entry == 0
    }

    fn get_control_type(&self) -> Option<u8> {
        self.data.first().copied()
    }

    /// Returns true if the record is a start control record.
    pub fn is_start(&self) -> bool {
        self.entry == 0 && self.data.len() >= 17 && self.get_control_type() == Some(CONTROL_START)
    }

    /// Returns true if the record is a finish control record.
    pub fn is_finish(&self) -> bool {
        self.entry == 0 && self.data.len() == 5 && self.get_control_type() == Some(CONTROL_FINISH)
    }

    /// Returns true if the record is a set metadata control record.
    pub fn is_set_metadata(&self) -> bool {
        self.entry == 0
            && self.data.len() >= 9
            && self.get_control_type() == Some(CONTROL_SET_METADATA)
    }

    /// Decodes a start control record.
    pub fn get_start_data(&self) -> Result<StartRecordData> {
        if !self.is_start() {
            return Err(WpilogError::ParseError("Not a start record".to_string()));
        }

        let mut cursor = Cursor::new(&self.data);
        cursor.set_position(1); // Skip control type

        let entry = cursor.read_u32::<LittleEndian>()?;
        let (name, pos) = read_inner_string(&self.data, cursor.position() as usize)?;
        let (type_name, pos) = read_inner_string(&self.data, pos)?;
        let (metadata, _) = read_inner_string(&self.data, pos)?;

        Ok(StartRecordData {
            entry,
            name,
            type_name,
            metadata,
        })
    }

    /// Decodes a finish control record.
    pub fn get_finish_entry(&self) -> Result<u32> {
        if !self.is_finish() {
            return Err(WpilogError::ParseError("Not a finish record".to_string()));
        }

        let mut cursor = Cursor::new(&self.data[1..5]);
        Ok(cursor.read_u32::<LittleEndian>()?)
    }

    /// Decodes a set metadata control record.
    pub fn get_set_metadata_data(&self) -> Result<MetadataRecordData> {
        if !self.is_set_metadata() {
            return Err(WpilogError::ParseError(
                "Not a set metadata record".to_string(),
            ));
        }

        let mut cursor = Cursor::new(&self.data[1..5]);
        let entry = cursor.read_u32::<LittleEndian>()?;
        let (metadata, _) = read_inner_string(&self.data, 5)?;

        Ok(MetadataRecordData { entry, metadata })
    }

    /// Decodes a boolean data record.
    pub fn get_boolean(&self) -> Result<bool> {
        if self.data.len() != 1 {
            return Err(WpilogError::ParseError(format!(
                "Invalid boolean size: expected 1 byte, got {}",
                self.data.len()
            )));
        }
        Ok(self.data[0] != 0)
    }

    /// Decodes an integer (int64) data record.
    pub fn get_integer(&self) -> Result<i64> {
        if self.data.len() != 8 {
            return Err(WpilogError::ParseError(format!(
                "Invalid integer size: expected 8 bytes, got {}",
                self.data.len()
            )));
        }
        let mut cursor = Cursor::new(&self.data);
        Ok(cursor.read_i64::<LittleEndian>()?)
    }

    /// Decodes a float data record.
    pub fn get_float(&self) -> Result<f32> {
        if self.data.len() != 4 {
            return Err(WpilogError::ParseError(format!(
                "Invalid float size: expected 4 bytes, got {}",
                self.data.len()
            )));
        }
        let mut cursor = Cursor::new(&self.data);
        Ok(cursor.read_f32::<LittleEndian>()?)
    }

    /// Decodes a double data record.
    pub fn get_double(&self) -> Result<f64> {
        if self.data.len() != 8 {
            return Err(WpilogError::ParseError(format!(
                "Invalid double size: expected 8 bytes, got {}",
                self.data.len()
            )));
        }
        let mut cursor = Cursor::new(&self.data);
        Ok(cursor.read_f64::<LittleEndian>()?)
    }

    /// Decodes a string data record.
    /// Uses lossy UTF-8 conversion as a fallback for binary data marked as strings.
    pub fn get_string(&self) -> String {
        match String::from_utf8(self.data.clone()) {
            Ok(s) => s,
            Err(_) => {
                // Fallback: use lossy UTF-8 conversion for binary data marked as strings
                // This handles cases where the WPILog schema declares a field as string
                // but it actually contains binary/msgpack data
                String::from_utf8_lossy(&self.data).to_string()
            }
        }
    }

    /// Decodes msgpack data.
    pub fn get_msgpack(&self) -> Result<rmpv::Value> {
        rmpv::decode::read_value(&mut Cursor::new(&self.data))
            .map_err(|e| WpilogError::ParseError(format!("MsgPack decode error: {}", e)))
    }

    /// Decodes a boolean array data record.
    pub fn get_boolean_array(&self) -> Vec<bool> {
        self.data.iter().map(|&x| x != 0).collect()
    }

    /// Decodes an integer array data record.
    pub fn get_integer_array(&self) -> Result<Vec<i64>> {
        if self.data.len() % 8 != 0 {
            return Err(WpilogError::ParseError(format!(
                "Invalid integer array size: {} is not a multiple of 8",
                self.data.len()
            )));
        }
        let mut result = Vec::with_capacity(self.data.len() / 8);
        let mut cursor = Cursor::new(&self.data);
        while cursor.position() < self.data.len() as u64 {
            result.push(cursor.read_i64::<LittleEndian>()?);
        }
        Ok(result)
    }

    /// Decodes a float array data record.
    pub fn get_float_array(&self) -> Result<Vec<f32>> {
        if self.data.len() % 4 != 0 {
            return Err(WpilogError::ParseError(format!(
                "Invalid float array size: {} is not a multiple of 4",
                self.data.len()
            )));
        }
        let mut result = Vec::with_capacity(self.data.len() / 4);
        let mut cursor = Cursor::new(&self.data);
        while cursor.position() < self.data.len() as u64 {
            result.push(cursor.read_f32::<LittleEndian>()?);
        }
        Ok(result)
    }

    /// Decodes a double array data record.
    pub fn get_double_array(&self) -> Result<Vec<f64>> {
        if self.data.len() % 8 != 0 {
            return Err(WpilogError::ParseError(format!(
                "Invalid double array size: {} is not a multiple of 8",
                self.data.len()
            )));
        }
        let mut result = Vec::with_capacity(self.data.len() / 8);
        let mut cursor = Cursor::new(&self.data);
        while cursor.position() < self.data.len() as u64 {
            result.push(cursor.read_f64::<LittleEndian>()?);
        }
        Ok(result)
    }

    /// Decodes a string array data record.
    pub fn get_string_array(&self) -> Result<Vec<String>> {
        let mut cursor = Cursor::new(&self.data);
        let size = cursor.read_u32::<LittleEndian>()? as usize;

        if size > (self.data.len() - 4) / 4 {
            return Err(WpilogError::ParseError(format!(
                "Invalid string array size: {}",
                size
            )));
        }

        let mut result = Vec::with_capacity(size);
        let mut pos = 4;

        for _ in 0..size {
            let (s, new_pos) = read_inner_string(&self.data, pos)?;
            result.push(s);
            pos = new_pos;
        }

        Ok(result)
    }
}

/// Reads a length-prefixed string from within a buffer.
fn read_inner_string(data: &[u8], pos: usize) -> Result<(String, usize)> {
    if pos + 4 > data.len() {
        return Err(WpilogError::ParseError(
            "Invalid string size position".to_string(),
        ));
    }

    let mut cursor = Cursor::new(&data[pos..pos + 4]);
    let size = cursor.read_u32::<LittleEndian>()? as usize;
    let end = pos + 4 + size;

    if end > data.len() {
        return Err(WpilogError::ParseError(format!(
            "Invalid string size: {}",
            size
        )));
    }

    let s = match String::from_utf8(data[pos + 4..end].to_vec()) {
        Ok(s) => s,
        Err(_) => {
            // Fallback: use lossy UTF-8 conversion for binary data marked as strings
            String::from_utf8_lossy(&data[pos + 4..end]).to_string()
        }
    };

    Ok((s, end))
}

/// WPILog file reader.
pub struct DataLogReader<'a> {
    pub(crate) data: &'a [u8],
}

impl<'a> DataLogReader<'a> {
    /// Creates a new DataLogReader from a byte slice.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// Returns true if the data appears to be a valid WPILog file.
    pub fn is_valid(&self) -> bool {
        self.data.len() >= 12 && &self.data[0..6] == b"WPILOG" && self.get_version() >= 0x0100
    }

    /// Gets the WPILog version number.
    pub fn get_version(&self) -> u16 {
        if self.data.len() < 12 {
            return 0;
        }
        let mut cursor = Cursor::new(&self.data[6..8]);
        cursor.read_u16::<LittleEndian>().unwrap_or(0)
    }

    /// Gets the extra header string.
    pub fn get_extra_header(&self) -> String {
        if self.data.len() < 12 {
            return String::new();
        }

        let mut cursor = Cursor::new(&self.data[8..12]);
        let size = cursor.read_u32::<LittleEndian>().unwrap_or(0) as usize;

        if 12 + size > self.data.len() {
            return String::new();
        }

        String::from_utf8(self.data[12..12 + size].to_vec()).unwrap_or_default()
    }

    /// Returns an iterator over all records in the log.
    pub fn records(&self) -> Result<DataLogIterator<'a>> {
        if !self.is_valid() {
            return Err(WpilogError::InvalidFormat(
                "Not a valid WPILOG file".to_string(),
            ));
        }

        let mut cursor = Cursor::new(&self.data[8..12]);
        let extra_header_size = cursor.read_u32::<LittleEndian>()? as usize;
        let start_pos = 12 + extra_header_size;

        Ok(DataLogIterator {
            data: self.data,
            pos: start_pos,
        })
    }
}

/// Iterator over WPILog records.
pub struct DataLogIterator<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for DataLogIterator<'a> {
    type Item = Result<DataLogRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.data.len() < self.pos + 4 {
            return None;
        }

        let header_byte = self.data[self.pos];
        let entry_len = ((header_byte & 0x3) + 1) as usize;
        let size_len = (((header_byte >> 2) & 0x3) + 1) as usize;
        let timestamp_len = (((header_byte >> 4) & 0x7) + 1) as usize;
        let header_len = 1 + entry_len + size_len + timestamp_len;

        if self.data.len() < self.pos + header_len {
            return None;
        }

        let entry = read_varint(&self.data[self.pos + 1..], entry_len);
        let size = read_varint(&self.data[self.pos + 1 + entry_len..], size_len) as usize;
        let timestamp = read_varint(
            &self.data[self.pos + 1 + entry_len + size_len..],
            timestamp_len,
        );

        if self.data.len() < self.pos + header_len + size {
            return None;
        }

        let data = self.data[self.pos + header_len..self.pos + header_len + size].to_vec();

        let record = DataLogRecord {
            entry: entry as u32,
            timestamp,
            data,
        };

        self.pos += header_len + size;

        Some(Ok(record))
    }
}

/// Reads a variable-length integer from a byte slice.
fn read_varint(data: &[u8], len: usize) -> u64 {
    let mut val = 0u64;
    for i in 0..len {
        val |= (data[i] as u64) << (i * 8);
    }
    val
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_varint() {
        let data = vec![0x01, 0x00, 0x00, 0x00];
        assert_eq!(read_varint(&data, 1), 1);
        assert_eq!(read_varint(&data, 4), 1);

        let data = vec![0xff, 0x00, 0x00, 0x00];
        assert_eq!(read_varint(&data, 1), 255);

        let data = vec![0x00, 0x01, 0x00, 0x00];
        assert_eq!(read_varint(&data, 2), 256);
    }

    #[test]
    fn test_header_validation() {
        let valid_header = b"WPILOG\x00\x01\x00\x00\x00\x00";
        let reader = DataLogReader::new(valid_header);
        assert!(reader.is_valid());
        assert_eq!(reader.get_version(), 0x0100);

        let invalid_magic = b"NOTLOG\x00\x01\x00\x00\x00\x00";
        let reader = DataLogReader::new(invalid_magic);
        assert!(!reader.is_valid());
    }
}
