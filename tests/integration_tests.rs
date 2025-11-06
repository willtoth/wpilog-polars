use polars::prelude::*;
use wpilog_polars::{infer_schema, WpilogParser};

/// Helper function to create a complete WPILog file with multiple data types
fn create_comprehensive_wpilog() -> Vec<u8> {
    let mut data = Vec::new();

    // Header: "WPILOG" + version 0x0100 + extra header length 0
    data.extend_from_slice(b"WPILOG");
    data.extend_from_slice(&[0x00, 0x01]); // Version 1.0
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Extra header length = 0

    // Entry 1: double type (1+4+4+5+4+6+4=28)
    data.push(0x00); // Header byte
    data.push(0x00); // Entry ID = 0 (control)
    data.push(28); // Payload size
    data.push(0x01); // Timestamp = 1
    data.push(0x00); // Control type = Start
    data.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // Entry ID = 1
    data.extend_from_slice(&[0x05, 0x00, 0x00, 0x00]); // Name length = 5
    data.extend_from_slice(b"speed"); // Name
    data.extend_from_slice(&[0x06, 0x00, 0x00, 0x00]); // Type length = 6
    data.extend_from_slice(b"double"); // Type
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Metadata length = 0

    // Entry 2: int64 type (1+4+4+5+4+5+4=27)
    data.push(0x00); // Header byte
    data.push(0x00); // Entry ID = 0 (control)
    data.push(27); // Payload size
    data.push(0x01); // Timestamp = 1
    data.push(0x00); // Control type = Start
    data.extend_from_slice(&[0x02, 0x00, 0x00, 0x00]); // Entry ID = 2
    data.extend_from_slice(&[0x05, 0x00, 0x00, 0x00]); // Name length = 5
    data.extend_from_slice(b"count"); // Name
    data.extend_from_slice(&[0x05, 0x00, 0x00, 0x00]); // Type length = 5
    data.extend_from_slice(b"int64"); // Type
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Metadata length = 0

    // Entry 3: boolean type (1+4+4+6+4+7+4=30)
    data.push(0x00); // Header byte
    data.push(0x00); // Entry ID = 0 (control)
    data.push(30); // Payload size
    data.push(0x01); // Timestamp = 1
    data.push(0x00); // Control type = Start
    data.extend_from_slice(&[0x03, 0x00, 0x00, 0x00]); // Entry ID = 3
    data.extend_from_slice(&[0x06, 0x00, 0x00, 0x00]); // Name length = 6
    data.extend_from_slice(b"active"); // Name
    data.extend_from_slice(&[0x07, 0x00, 0x00, 0x00]); // Type length = 7
    data.extend_from_slice(b"boolean"); // Type
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Metadata length = 0

    // Data record 1: timestamp 100, speed=1.5
    data.push(0x00); // Header byte
    data.push(0x01); // Entry ID = 1
    data.push(0x08); // Payload size = 8
    data.push(0x64); // Timestamp = 100
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f]); // 1.5 as f64

    // Data record 2: timestamp 100, count=42
    data.push(0x00); // Header byte
    data.push(0x02); // Entry ID = 2
    data.push(0x08); // Payload size = 8
    data.push(0x64); // Timestamp = 100
    data.extend_from_slice(&[0x2a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // 42 as i64

    // Data record 3: timestamp 200, speed=2.5
    data.push(0x00); // Header byte
    data.push(0x01); // Entry ID = 1
    data.push(0x08); // Payload size = 8
    data.push(0xc8); // Timestamp = 200 (0xc8)
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x40]); // 2.5 as f64

    // Data record 4: timestamp 200, active=true
    data.push(0x00); // Header byte
    data.push(0x03); // Entry ID = 3
    data.push(0x01); // Payload size = 1
    data.push(0xc8); // Timestamp = 200
    data.push(0x01); // true

    data
}

#[test]
fn test_parse_comprehensive_wpilog() {
    let data = create_comprehensive_wpilog();
    let df = WpilogParser::from_bytes(data).expect("Failed to parse WPILog");

    // Check dimensions
    assert_eq!(df.height(), 2); // 2 unique timestamps
    assert_eq!(df.width(), 4); // timestamp + 3 columns

    // Check column names
    let col_names = df.get_column_names();
    assert!(col_names.iter().any(|s| s.as_str() == "timestamp"));
    assert!(col_names.iter().any(|s| s.as_str() == "speed"));
    assert!(col_names.iter().any(|s| s.as_str() == "count"));
    assert!(col_names.iter().any(|s| s.as_str() == "active"));
}

#[test]
fn test_sparse_data_handling() {
    let data = create_comprehensive_wpilog();
    let df = WpilogParser::from_bytes(data).expect("Failed to parse WPILog");

    // First row should have speed and count, but active should be null
    // Second row should have speed and active, but count should be null

    let speed_col = df.column("speed").unwrap();
    assert_eq!(speed_col.len(), 2);
    let speed_null = speed_col.is_null();
    assert!(!speed_null.get(0).unwrap());
    assert!(!speed_null.get(1).unwrap());

    let count_col = df.column("count").unwrap();
    assert_eq!(count_col.len(), 2);
    let count_null = count_col.is_null();
    assert!(!count_null.get(0).unwrap());
    assert!(count_null.get(1).unwrap()); // Should be null in second row

    let active_col = df.column("active").unwrap();
    assert_eq!(active_col.len(), 2);
    let active_null = active_col.is_null();
    assert!(active_null.get(0).unwrap()); // Should be null in first row
    assert!(!active_null.get(1).unwrap());
}

#[test]
fn test_infer_schema_comprehensive() {
    let data = create_comprehensive_wpilog();
    let schema = infer_schema(&data).expect("Failed to infer schema");

    assert_eq!(schema.len(), 4); // timestamp + 3 columns

    // Check types
    assert_eq!(schema.get("timestamp"), Some(&DataType::Int64));
    assert_eq!(schema.get("speed"), Some(&DataType::Float64));
    assert_eq!(schema.get("count"), Some(&DataType::Int64));
    assert_eq!(schema.get("active"), Some(&DataType::Boolean));
}

#[test]
fn test_array_types() {
    let mut data = Vec::new();

    // Header
    data.extend_from_slice(b"WPILOG");
    data.extend_from_slice(&[0x00, 0x01]); // Version 1.0
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Extra header length = 0

    // Entry 1: int64[] type (1+4+4+6+4+7+4=30)
    data.push(0x00); // Header byte
    data.push(0x00); // Entry ID = 0 (control)
    data.push(30); // Payload size
    data.push(0x01); // Timestamp = 1
    data.push(0x00); // Control type = Start
    data.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // Entry ID = 1
    data.extend_from_slice(&[0x06, 0x00, 0x00, 0x00]); // Name length = 6
    data.extend_from_slice(b"values"); // Name
    data.extend_from_slice(&[0x07, 0x00, 0x00, 0x00]); // Type length = 7
    data.extend_from_slice(b"int64[]"); // Type
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Metadata length = 0

    // Data record: timestamp 100, values=[1, 2, 3]
    data.push(0x00); // Header byte
    data.push(0x01); // Entry ID = 1
    data.push(0x18); // Payload size = 24 (3 * 8 bytes)
    data.push(0x64); // Timestamp = 100
    data.extend_from_slice(&[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // 1
    data.extend_from_slice(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // 2
    data.extend_from_slice(&[0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]); // 3

    let df = WpilogParser::from_bytes(data).expect("Failed to parse WPILog");

    assert_eq!(df.height(), 1);
    assert_eq!(df.width(), 2); // timestamp + values

    let values_col = df.column("values").unwrap();
    assert_eq!(
        values_col.dtype(),
        &DataType::List(Box::new(DataType::Int64))
    );
}

#[test]
fn test_invalid_header() {
    let data = b"NOTLOG\x00\x01\x00\x00\x00\x00";
    let result = WpilogParser::from_bytes(data.to_vec());
    assert!(result.is_err());
}

#[test]
fn test_empty_file() {
    let data = b"";
    let result = WpilogParser::from_bytes(data.to_vec());
    assert!(result.is_err());
}
