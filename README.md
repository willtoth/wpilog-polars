# wpilog-polars

High-performance parser for WPILog robot telemetry files. Converts WPILog binary data to Polars DataFrames for analysis.

## Installation

```bash
cargo install --path .
```

Or run directly:

```bash
cargo run -- <command> [args]
```

## Quick Start

```bash
# View file information
wpilog-polars info robot.wpilog

# Display data in terminal
wpilog-polars parse robot.wpilog --head 10

# Convert to CSV
wpilog-polars convert robot.wpilog output.csv

# Convert to Parquet
wpilog-polars convert robot.wpilog output.parquet
```

## Commands

### `info` - Show File Statistics

Display file size, row/column count, time range, and null percentages:

```bash
wpilog-polars info robot.wpilog
```

### `schema` - View Column Information

Show column names and data types:

```bash
# Basic schema
wpilog-polars schema robot.wpilog

# Detailed types
wpilog-polars schema robot.wpilog --verbose
```

### `parse` - Display or Export Data

View and filter data with options:

```bash
# Show first 10 rows
wpilog-polars parse robot.wpilog --head 10

# Show last 5 rows
wpilog-polars parse robot.wpilog --tail 5

# Select specific columns
wpilog-polars parse robot.wpilog --columns timestamp,speed,position

# Filter data
wpilog-polars parse robot.wpilog --filter "speed=0.5"

# Export to CSV
wpilog-polars parse robot.wpilog --format csv --output data.csv

# Export to Parquet
wpilog-polars parse robot.wpilog --format parquet --output data.parquet
```

### `convert` - Convert to Other Formats

Convert WPILog files to CSV or Parquet:

```bash
# Auto-detect format from extension
wpilog-polars convert robot.wpilog output.csv

# Parquet with compression
wpilog-polars convert robot.wpilog output.parquet --compression zstd

# Available compressions: uncompressed, snappy, gzip, lz4, zstd
```

## Using as a Library

Add to your `Cargo.toml`:

```toml
[dependencies]
wpilog-polars = "0.1.0"
```

### Example

```rust
use wpilog_polars::WpilogParser;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse from file (uses memory-mapped I/O)
    let df = WpilogParser::from_file("robot.wpilog")?;

    println!("Loaded {} rows and {} columns", df.height(), df.width());
    println!("{}", df);

    // Work with the Polars DataFrame
    // - Filter, select, aggregate, etc.
    // - Export to CSV, Parquet, Arrow, etc.

    Ok(())
}
```

### Parse from Bytes

```rust
let data = std::fs::read("robot.wpilog")?;
let df = WpilogParser::from_bytes(&data)?;
```

### Infer Schema Only

```rust
use wpilog_polars::infer_schema;

let data = std::fs::read("robot.wpilog")?;
let schema = infer_schema(&data)?;

for (name, dtype) in schema.iter() {
    println!("{}: {:?}", name, dtype);
}
```

## Supported Data Types

| WPILog Type | Polars Type |
|-------------|-------------|
| `boolean` | `Boolean` |
| `int64` | `Int64` |
| `float` | `Float32` |
| `double` | `Float64` |
| `string` | `String` |
| `boolean[]` | `List(Boolean)` |
| `int64[]` | `List(Int64)` |
| `float[]` | `List(Float32)` |
| `double[]` | `List(Float64)` |
| `string[]` | `List(String)` |
| `struct:*` | `String` (hex-encoded)* |

\* Struct support is in progress. Currently stored as hex strings.

## DataFrame Structure

The resulting DataFrame contains:
- **`timestamp`** column (Int64, in microseconds)
- One column per WPILog entry with the entry's name

### Sparse Data Handling

WPILog files contain sparse data where columns update at different rates. The parser:
1. Creates one row per unique timestamp
2. Fills missing values with `null` for columns not updated at that timestamp
3. Preserves the last known value is NOT done - each row shows only what was logged

## License

MIT

## References

- [WPILog Documentation](https://github.com/wpilibsuite/allwpilib/blob/main/wpiutil/doc/datalog.adoc)
- [Polars Documentation](https://docs.rs/polars/)
- [WPILib](https://github.com/wpilibsuite)
