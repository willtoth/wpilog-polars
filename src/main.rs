use anyhow::{Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use polars::prelude::*;
use std::path::PathBuf;
use std::time::Instant;
use wpilog_polars::WpilogParser;

/// High-performance WPILog to Polars DataFrame converter
#[derive(Parser)]
#[command(name = "wpilog-polars")]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Parse a WPILog file and display or export the data
    Parse {
        /// Input WPILog file path
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Output format
        #[arg(short, long, value_enum, default_value = "display")]
        format: OutputFormat,

        /// Output file path (required for csv and parquet formats)
        #[arg(short, long, value_name = "FILE")]
        output: Option<PathBuf>,

        /// Columns to select (comma-separated, default: all)
        #[arg(short, long, value_delimiter = ',')]
        columns: Option<Vec<String>>,

        /// Display only first N rows (default: all)
        #[arg(short = 'n', long)]
        head: Option<usize>,

        /// Display only last N rows
        #[arg(short = 't', long)]
        tail: Option<usize>,

        /// Filter rows where column matches value (format: column=value)
        #[arg(long, value_name = "FILTER")]
        filter: Option<Vec<String>>,
    },

    /// Show schema information for a WPILog file
    Schema {
        /// Input WPILog file path
        #[arg(value_name = "FILE")]
        input: PathBuf,

        /// Show detailed type information
        #[arg(short, long)]
        verbose: bool,
    },

    /// Show general information about a WPILog file
    Info {
        /// Input WPILog file path
        #[arg(value_name = "FILE")]
        input: PathBuf,
    },

    /// Convert WPILog to another format
    Convert {
        /// Input WPILog file path
        #[arg(value_name = "INPUT")]
        input: PathBuf,

        /// Output file path
        #[arg(value_name = "OUTPUT")]
        output: PathBuf,

        /// Output format (auto-detected from extension if not specified)
        #[arg(short, long, value_enum)]
        format: Option<ConvertFormat>,

        /// Compression for Parquet files
        #[arg(long, value_enum, default_value = "snappy")]
        compression: Compression,
    },
}

#[derive(Debug, Clone, ValueEnum)]
enum OutputFormat {
    /// Display in terminal (pretty table)
    Display,
    /// Export to CSV
    Csv,
    /// Export to Parquet
    Parquet,
}

#[derive(Debug, Clone, ValueEnum)]
enum ConvertFormat {
    Csv,
    Parquet,
}

#[derive(Debug, Clone, ValueEnum)]
enum Compression {
    Uncompressed,
    Snappy,
    Gzip,
    Lz4,
    Zstd,
}

fn to_parquet_compression(c: Compression) -> ParquetCompression {
    match c {
        Compression::Uncompressed => ParquetCompression::Uncompressed,
        Compression::Snappy => ParquetCompression::Snappy,
        Compression::Gzip => ParquetCompression::Gzip(None),
        Compression::Lz4 => ParquetCompression::Lz4Raw,
        Compression::Zstd => ParquetCompression::Zstd(None),
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Parse {
            input,
            format,
            output,
            columns,
            head,
            tail,
            filter,
        } => parse_command(input, format, output, columns, head, tail, filter)?,

        Commands::Schema { input, verbose } => schema_command(input, verbose)?,

        Commands::Info { input } => info_command(input)?,

        Commands::Convert {
            input,
            output,
            format,
            compression,
        } => convert_command(input, output, format, compression)?,
    }

    Ok(())
}

fn parse_command(
    input: PathBuf,
    format: OutputFormat,
    output: Option<PathBuf>,
    columns: Option<Vec<String>>,
    head: Option<usize>,
    tail: Option<usize>,
    filter: Option<Vec<String>>,
) -> Result<()> {
    // Parse the WPILog file
    println!("Parsing {}...", input.display());
    let parse_start = Instant::now();
    let mut df = WpilogParser::from_file(&input)
        .with_context(|| format!("Failed to parse WPILog file: {}", input.display()))?;
    let parse_duration = parse_start.elapsed();

    println!("Loaded {} rows and {} columns ({:.3}s)", df.height(), df.width(), parse_duration.as_secs_f64());

    // Apply column selection
    if let Some(cols) = columns {
        let mut selected = cols.clone();
        // Always include timestamp if not already present
        if !selected.contains(&"timestamp".to_string()) {
            selected.insert(0, "timestamp".to_string());
        }
        df = df.select(&selected)?;
        println!("Selected {} columns", df.width());
    }

    // Apply filters
    if let Some(filters) = filter {
        for filter_expr in filters {
            let parts: Vec<&str> = filter_expr.split('=').collect();
            if parts.len() != 2 {
                anyhow::bail!(
                    "Invalid filter format '{}'. Expected 'column=value'",
                    filter_expr
                );
            }
            let (col_name, value) = (parts[0].trim(), parts[1].trim());

            // Try to parse the value and apply filter
            df = apply_filter(df, col_name, value)?;
            println!("Applied filter: {} = {}", col_name, value);
        }
        println!("After filtering: {} rows", df.height());
    }

    // Apply head/tail
    if let Some(n) = head {
        df = df.head(Some(n));
        println!("Showing first {} rows", n);
    } else if let Some(n) = tail {
        df = df.tail(Some(n));
        println!("Showing last {} rows", n);
    }

    // Output based on format
    match format {
        OutputFormat::Display => {
            println!("\n{}", df);
        }
        OutputFormat::Csv => {
            let output_path = output.context("Output path required for CSV format")?;
            let mut file = std::fs::File::create(&output_path)?;
            CsvWriter::new(&mut file).finish(&mut df)?;
            println!("Exported to CSV: {}", output_path.display());
        }
        OutputFormat::Parquet => {
            let output_path = output.context("Output path required for Parquet format")?;
            let file = std::fs::File::create(&output_path)?;
            let save_start = Instant::now();
            ParquetWriter::new(file).finish(&mut df)?;
            let save_duration = save_start.elapsed();
            println!("Exported to Parquet: {} ({:.3}s)", output_path.display(), save_duration.as_secs_f64());
        }
    }

    Ok(())
}

fn apply_filter(df: DataFrame, col_name: &str, value: &str) -> Result<DataFrame> {
    // Use lazy API for filtering
    let lazy_df = df.lazy();

    // Build filter expression
    let filter_expr = match value.parse::<i64>() {
        Ok(val) => col(col_name).eq(lit(val)),
        Err(_) => match value.parse::<f64>() {
            Ok(val) => col(col_name).eq(lit(val)),
            Err(_) => match value.parse::<bool>() {
                Ok(val) => col(col_name).eq(lit(val)),
                Err(_) => col(col_name).eq(lit(value)),
            },
        },
    };

    let result = lazy_df.filter(filter_expr).collect()?;
    Ok(result)
}

fn schema_command(input: PathBuf, verbose: bool) -> Result<()> {
    use wpilog_polars::datalog::DataLogReader;
    use wpilog_polars::schema::WpilogSchema;

    println!("Reading schema from {}...", input.display());

    let data = std::fs::read(&input)?;
    let reader = DataLogReader::new(&data);
    let records = reader.records()?;

    // Infer WpilogSchema (not Polars schema) to get original type info
    let wpilog_schema = WpilogSchema::infer_from_records(records)
        .with_context(|| format!("Failed to infer schema from: {}", input.display()))?;

    println!("\nSchema for {}:", input.display());
    println!("{} columns found\n", wpilog_schema.num_columns() + 1); // +1 for timestamp

    // Always show timestamp first
    println!("  {:30} {}", "timestamp", "Int64");

    if verbose {
        for col in wpilog_schema.columns() {
            println!("  {:30} {:?}", col.name, col.dtype);
        }
    } else {
        for col in wpilog_schema.columns() {
            // Format the type nicely, showing struct names
            let type_str = match &col.dtype {
                wpilog_polars::types::PolarsDataType::Struct(name) => {
                    format!("Struct({})", name)
                }
                other => format!("{:?}", other.to_polars_dtype()),
            };
            println!("  {:30} {}", col.name, type_str);
        }
    }

    Ok(())
}

fn info_command(input: PathBuf) -> Result<()> {
    println!("Analyzing {}...\n", input.display());

    // Get file size
    let metadata = std::fs::metadata(&input)?;
    let file_size = metadata.len();

    // Parse the file
    let df = WpilogParser::from_file(&input)
        .with_context(|| format!("Failed to parse WPILog file: {}", input.display()))?;

    println!("File Information:");
    println!("  Path:           {}", input.display());
    println!(
        "  Size:           {} bytes ({:.2} MB)",
        file_size,
        file_size as f64 / 1_048_576.0
    );
    println!("\nData Information:");
    println!("  Rows:           {}", df.height());
    println!("  Columns:        {}", df.width());

    // Get timestamp range
    if let Ok(ts_col) = df.column("timestamp") {
        if let Ok(ts) = ts_col.i64() {
            if let (Some(min), Some(max)) = (ts.min(), ts.max()) {
                let duration_us = max - min;
                let duration_s = duration_us as f64 / 1_000_000.0;
                println!(
                    "  Time range:     {:.3}s ({} to {} μs)",
                    duration_s, min, max
                );

                if df.height() > 1 {
                    let avg_interval = duration_us as f64 / (df.height() - 1) as f64;
                    println!(
                        "  Avg interval:   {:.2} μs ({:.2} Hz)",
                        avg_interval,
                        1_000_000.0 / avg_interval
                    );
                }
            }
        }
    }

    println!("\nColumns:");
    for name in df.get_column_names() {
        if let Ok(col) = df.column(name.as_str()) {
            let null_count = col.is_null().sum().unwrap_or(0);
            let null_pct = (null_count as f64 / df.height() as f64) * 100.0;
            println!(
                "  {:30} {:15?} (nulls: {:.1}%)",
                name,
                col.dtype(),
                null_pct
            );
        }
    }

    Ok(())
}

fn convert_command(
    input: PathBuf,
    output: PathBuf,
    format: Option<ConvertFormat>,
    compression: Compression,
) -> Result<()> {
    println!("Converting {} to {}...", input.display(), output.display());

    // Determine output format
    let out_format = if let Some(f) = format {
        f
    } else {
        // Auto-detect from extension
        match output.extension().and_then(|s| s.to_str()) {
            Some("csv") => ConvertFormat::Csv,
            Some("parquet") => ConvertFormat::Parquet,
            _ => anyhow::bail!(
                "Cannot determine output format from extension. Use --format to specify."
            ),
        }
    };

    // Parse the WPILog file
    let parse_start = Instant::now();
    let mut df = WpilogParser::from_file(&input)
        .with_context(|| format!("Failed to parse WPILog file: {}", input.display()))?;
    let parse_duration = parse_start.elapsed();

    println!("Loaded {} rows and {} columns ({:.3}s)", df.height(), df.width(), parse_duration.as_secs_f64());

    // Write to output format
    match out_format {
        ConvertFormat::Csv => {
            let mut file = std::fs::File::create(&output)?;
            CsvWriter::new(&mut file).finish(&mut df)?;
            println!("Successfully converted to CSV: {}", output.display());
        }
        ConvertFormat::Parquet => {
            let file = std::fs::File::create(&output)?;
            let save_start = Instant::now();
            ParquetWriter::new(file)
                .with_compression(to_parquet_compression(compression))
                .finish(&mut df)?;
            let save_duration = save_start.elapsed();
            println!("Successfully converted to Parquet: {} ({:.3}s)", output.display(), save_duration.as_secs_f64());
        }
    }

    Ok(())
}
