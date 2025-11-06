# wpilog-polars

High-performance conversion of WPILog binary data directly to Polars DataFrames.

## Overview

`wpilog-polars` is a Rust library that converts WPILog files (binary datalog files from WPILib robotics) directly to Polars DataFrames with zero intermediate formats. It's designed for maximum performance when analyzing robot telemetry data.

## Features

- **Direct conversion** to Polars DataFrames - no intermediate CSV or JSON
- **All WPILog data types supported** - scalars (boolean, int64, float, double, string) and arrays
- **Automatic sparse data handling** - null-fills missing values at each timestamp
- **Memory-mapped file I/O** - uses `mmap` for maximum read performance
- **UTF-8 fallback** - gracefully handles binary data marked as strings
- **Type-safe** - comprehensive error handling with detailed error messages

## Installation

### As a Library

Add this to your `Cargo.toml`:

```toml
[dependencies]
wpilog-polars = "0.1.0"
```

### As a CLI Tool

```bash
cargo install --path .
```

Or run directly:

```bash
cargo run -- <command> [args]
```

## CLI Usage

The `wpilog-polars` CLI provides several commands for working with WPILog files:

### Commands

- `parse` - Parse and display/export WPILog data
- `schema` - Show schema information
- `info` - Show file statistics and metadata
- `convert` - Convert WPILog to CSV or Parquet

### Parse Command

Display or export WPILog data with filtering and column selection:

```bash
# Display data in terminal
wpilog-polars parse robot.wpilog

# Show only first 10 rows
wpilog-polars parse robot.wpilog --head 10

# Select specific columns
wpilog-polars parse robot.wpilog --columns timestamp,speed,position

# Filter data
wpilog-polars parse robot.wpilog --filter "speed=0.5"

# Export to CSV
wpilog-polars parse robot.wpilog --format csv --output data.csv

# Export to Parquet
wpilog-polars parse robot.wpilog --format parquet --output data.parquet
```

### Schema Command

Inspect the schema without loading all data:

```bash
# Show column names and types
wpilog-polars schema robot.wpilog

# Show detailed type information
wpilog-polars schema robot.wpilog --verbose
```

### Info Command

Display file statistics and metadata:

```bash
wpilog-polars info robot.wpilog
```

Output includes:
- File size
- Number of rows and columns
- Time range and sampling rate
- Null percentage for each column

### Convert Command

Convert WPILog to other formats:

```bash
# Convert to CSV
wpilog-polars convert robot.wpilog output.csv

# Convert to Parquet with compression
wpilog-polars convert robot.wpilog output.parquet --compression zstd

# Specify format explicitly
wpilog-polars convert robot.wpilog output.file --format parquet
```

Supported compressions: `uncompressed`, `snappy`, `gzip`, `lz4`, `zstd`

## Library Usage

### Basic Example

```rust
use wpilog_polars::WpilogParser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse from file (uses memory mapping for performance)
    let df = WpilogParser::from_file("robot.wpilog")?;

    println!("Parsed {} rows and {} columns", df.height(), df.width());
    println!("{}", df);

    Ok(())
}
```

### Parse from bytes

```rust
use wpilog_polars::WpilogParser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read("robot.wpilog")?;
    let df = WpilogParser::from_bytes(data)?;

    println!("{}", df);

    Ok(())
}
```

### Schema Inference

You can infer the schema without parsing all the data:

```rust
use wpilog_polars::infer_schema;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data = std::fs::read("robot.wpilog")?;
    let schema = infer_schema(&data)?;

    for (name, dtype) in schema.iter() {
        println!("{}: {:?}", name, dtype);
    }

    Ok(())
}
```

## Data Types

The library supports all WPILog data types:

| WPILog Type | Polars Type |
|-------------|-------------|
| `double` | `Float64` |
| `float` | `Float32` |
| `int64` | `Int64` |
| `boolean` | `Boolean` |
| `string` | `String` |
| `raw` | `String` (binary data) |
| `double[]` | `List(Float64)` |
| `float[]` | `List(Float32)` |
| `int64[]` | `List(Int64)` |
| `boolean[]` | `List(Boolean)` |
| `string[]` | `List(String)` |
| `msgpack` | `String` (serialized) |
| `struct` | `String` (serialized) |

## DataFrame Structure

The resulting DataFrame has the following structure:

- First column: `timestamp` (Int64) - microseconds since start
- Subsequent columns: one per WPILog entry, named according to the entry name

### Sparse Data

WPILog files contain sparse data where not every column is updated at every timestamp. This library automatically handles this by:

1. Identifying unique timestamps across all entries
2. Creating rows for each unique timestamp
3. Filling missing values with nulls for columns not updated at that timestamp

## Performance

The library is designed for high performance:

- **Two-pass algorithm**: Schema inference, then data accumulation
- **Pre-allocated builders**: Capacity estimation (~25 bytes per record)
- **Memory-mapped I/O**: `from_file()` uses `mmap` for fastest reading
- **Zero-copy where possible**: Minimal data copying during parsing
- **Direct columnar format**: No row-to-column conversion needed

### Benchmarks

Run benchmarks with:

```bash
cargo bench
```

Expected performance on modern hardware:
- Small files (100 records): < 1ms
- Medium files (10K records): < 10ms
- Large files (100K records): < 100ms
- Very large files (500K records): < 500ms

## Architecture

The library consists of several modules:

- **`datalog`**: Low-level WPILog binary format parsing
- **`types`**: Type system mapping between WPILog and Polars
- **`schema`**: Schema inference from START control records
- **`builders`**: Column builders with sparse data support
- **`converter`**: Main conversion logic (two-pass algorithm)
- **`error`**: Comprehensive error types

### Two-Pass Algorithm

1. **First pass**: Read all START control records to infer schema
2. **Second pass**: Accumulate data into pre-allocated column builders

This approach provides optimal performance by:
- Knowing all column names and types upfront
- Pre-allocating memory for builders
- Processing data records only once

## Error Handling

The library uses the `WpilogError` enum for all errors:

- `InvalidFormat` - Invalid WPILog header or version
- `ParseError` - Data parsing error (corrupt data, wrong type, etc.)
- `SchemaError` - Schema inference or validation error
- `InvalidEntry` - Entry not found or invalid entry ID
- `PolarsError` - Error from Polars during DataFrame construction
- `Io` - I/O error reading files

## Testing

Run tests with:

```bash
cargo test
```

The test suite includes:
- Unit tests for all modules
- Integration tests with realistic WPILog data
- Tests for sparse data handling
- Tests for all data types including arrays

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## References

- [WPILog Format Specification](docs/wpilog.adoc)
- [Polars DataFrame Documentation](https://docs.rs/polars/)
- [WPILib](https://github.com/wpilibsuite)
