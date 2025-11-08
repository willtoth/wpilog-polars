//! Integration tests using real WPILog files
//!
//! These tests validate the parser against real-world data files.
//! Test files are stored in `testlog/` (git-ignored).
//!
//! Run with: cargo test --test integration_tests

use polars::prelude::*;
use wpilog_polars::WpilogParser;

mod fixtures;

// ============================================================================
// Real-World Integration Tests
// ============================================================================

/// Smoke test that runs automatically if test files are available
///
/// This test provides quick validation that real WPILog files can be parsed
/// successfully. It runs as part of the normal test suite if files are present.
#[test]
fn test_real_files_smoke_test() {
    use fixtures::test_files;

    if !fixtures::has_test_files() {
        println!("\n⚠️  No test files available - skipping integration tests");
        println!("   Place .wpilog files in testlog/ to enable");
        println!("   See tests/README.md for details\n");
        return;
    }

    println!("\n🔥 Running smoke test on real WPILog files...\n");

    for test_file in test_files::all() {
        if !test_file.exists() {
            continue;
        }

        println!("Testing: {}", test_file.filename);

        let data = test_file.read().expect("Failed to read file");
        let df = WpilogParser::from_bytes(data).expect("Failed to parse");

        assert!(df.height() > 0, "DataFrame should have rows");
        assert!(df.width() > 0, "DataFrame should have columns");

        println!("  ✅ {} rows × {} columns\n", df.height(), df.width());
    }
}

/// Comprehensive integration test for real robotics log file
///
/// Validates parser against a 49MB real-world WPILog file with:
/// - Struct arrays (e.g., SwerveModuleState[])
/// - Nested structs (e.g., ChassisSpeeds with Rotation2d)
/// - Sparse data across 391 columns
/// - 17,577 timestamped rows
///
/// Run with: cargo test test_real_wpilog_comprehensive -- --ignored --nocapture
#[test]
#[ignore] // Only run when explicitly requested
fn test_real_wpilog_comprehensive() {
    use fixtures::test_files;

    let test_file = test_files::AKIT_LOG;

    if !test_file.exists() {
        test_file.skip_if_missing();
        return;
    }

    println!("\n🔍 Comprehensive test: {}", test_file.filename);
    println!("   Description: {}", test_file.description);

    // Read and parse
    let data = test_file.read().expect("Failed to read test file");
    let data_len = data.len();
    println!("   File size: {:.2} MB", data_len as f64 / 1_000_000.0);

    println!("   Parsing...");
    let start = std::time::Instant::now();
    let df = WpilogParser::from_bytes(data).expect("Failed to parse WPILog file");
    let duration = start.elapsed();

    println!("   ✅ Parsed in {:.2?}", duration);

    // Validate dimensions
    let rows = df.height();
    let cols = df.width();
    println!("   Dimensions: {} rows × {} columns", rows, cols);

    if let Some(expected_rows) = test_file.expected_rows {
        assert_eq!(rows, expected_rows, "Row count mismatch");
    }
    if let Some(expected_cols) = test_file.expected_cols {
        assert_eq!(cols, expected_cols, "Column count mismatch");
    }

    if let Some(min_rows) = test_file.min_rows {
        assert!(
            rows >= min_rows,
            "Row count {} below minimum {}",
            rows,
            min_rows
        );
    }
    if let Some(min_cols) = test_file.min_cols {
        assert!(
            cols >= min_cols,
            "Column count {} below minimum {}",
            cols,
            min_cols
        );
    }

    // Validate timestamp column exists
    assert!(
        df.get_column_names()
            .iter()
            .any(|s| s.as_str() == "timestamp"),
        "Missing timestamp column"
    );

    // Check for struct array columns (List(Struct) dtype)
    let mut struct_array_count = 0;
    let col_names = df.get_column_names();
    for name in col_names.iter() {
        let col = df.column(name.as_str()).unwrap();
        if matches!(col.dtype(), DataType::List(inner) if matches!(inner.as_ref(), DataType::Struct(_)))
        {
            struct_array_count += 1;
            println!("   Found struct array: {}", name);
        }
    }

    println!("   Total struct arrays: {}", struct_array_count);
    assert!(
        struct_array_count > 0,
        "Expected at least one struct array column"
    );

    // Check for all-null columns (potential data loss)
    let empty_cols: Vec<_> = col_names
        .iter()
        .filter(|name| {
            let col = df.column(name.as_str()).unwrap();
            col.is_null().all()
        })
        .collect();

    if !empty_cols.is_empty() {
        println!(
            "   ⚠️  Warning: {} columns are entirely null",
            empty_cols.len()
        );
    }

    // Test Parquet export
    println!("   Testing Parquet export...");
    let export_path = "testlog/test_output.parquet";
    let mut file = std::fs::File::create(export_path).expect("Failed to create output file");
    polars::prelude::ParquetWriter::new(&mut file)
        .finish(&mut df.clone())
        .expect("Failed to write Parquet");

    let export_size = std::fs::metadata(export_path).unwrap().len();
    let compression_ratio = (1.0 - (export_size as f64 / data_len as f64)) * 100.0;
    println!(
        "   ✅ Exported to Parquet: {:.2} MB ({:.1}% compression)",
        export_size as f64 / 1_000_000.0,
        compression_ratio
    );

    // Clean up
    std::fs::remove_file(export_path).ok();

    println!("   ✅ All validations passed!\n");
}

/// Print test data availability and status
///
/// Run with: cargo test print_test_data_status -- --ignored --nocapture
#[test]
#[ignore]
fn print_test_data_status() {
    fixtures::print_test_data_status();
}
