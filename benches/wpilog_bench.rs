use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use pprof::criterion::Output;
use wpilog_polars::WpilogParser;

/// Helper function to create a WPILog file with specified number of records
fn create_wpilog_with_records(num_records: usize) -> Vec<u8> {
    let mut data = Vec::new();

    // Header
    data.extend_from_slice(b"WPILOG");
    data.extend_from_slice(&[0x00, 0x01]); // Version 1.0
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Extra header length = 0

    // Entry 1: double type
    data.push(0x00); // Header byte
    data.push(0x00); // Entry ID = 0 (control)
    data.push(27); // Payload size
    data.push(0x01); // Timestamp = 1
    data.push(0x00); // Control type = Start
    data.extend_from_slice(&[0x01, 0x00, 0x00, 0x00]); // Entry ID = 1
    data.extend_from_slice(&[0x05, 0x00, 0x00, 0x00]); // Name length = 5
    data.extend_from_slice(b"speed"); // Name
    data.extend_from_slice(&[0x06, 0x00, 0x00, 0x00]); // Type length = 6
    data.extend_from_slice(b"double"); // Type
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Metadata length = 0

    // Entry 2: int64 type
    data.push(0x00); // Header byte
    data.push(0x00); // Entry ID = 0 (control)
    data.push(26); // Payload size
    data.push(0x01); // Timestamp = 1
    data.push(0x00); // Control type = Start
    data.extend_from_slice(&[0x02, 0x00, 0x00, 0x00]); // Entry ID = 2
    data.extend_from_slice(&[0x05, 0x00, 0x00, 0x00]); // Name length = 5
    data.extend_from_slice(b"count"); // Name
    data.extend_from_slice(&[0x05, 0x00, 0x00, 0x00]); // Type length = 5
    data.extend_from_slice(b"int64"); // Type
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Metadata length = 0

    // Entry 3: boolean type
    data.push(0x00); // Header byte
    data.push(0x00); // Entry ID = 0 (control)
    data.push(28); // Payload size
    data.push(0x01); // Timestamp = 1
    data.push(0x00); // Control type = Start
    data.extend_from_slice(&[0x03, 0x00, 0x00, 0x00]); // Entry ID = 3
    data.extend_from_slice(&[0x06, 0x00, 0x00, 0x00]); // Name length = 6
    data.extend_from_slice(b"active"); // Name
    data.extend_from_slice(&[0x07, 0x00, 0x00, 0x00]); // Type length = 7
    data.extend_from_slice(b"boolean"); // Type
    data.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // Metadata length = 0

    // Add data records
    for i in 0..num_records {
        let timestamp = (i * 20) as u8; // 20ms between records

        // Entry 1 data
        data.push(0x00); // Header byte
        data.push(0x01); // Entry ID = 1
        data.push(0x08); // Payload size = 8
        data.push(timestamp);
        let value = (i as f64) * 1.5;
        data.extend_from_slice(&value.to_le_bytes());

        // Entry 2 data (every other record)
        if i % 2 == 0 {
            data.push(0x00); // Header byte
            data.push(0x02); // Entry ID = 2
            data.push(0x08); // Payload size = 8
            data.push(timestamp);
            data.extend_from_slice(&(i as i64).to_le_bytes());
        }

        // Entry 3 data (every third record)
        if i % 3 == 0 {
            data.push(0x00); // Header byte
            data.push(0x03); // Entry ID = 3
            data.push(0x01); // Payload size = 1
            data.push(timestamp);
            data.push(if i % 6 == 0 { 1 } else { 0 });
        }
    }

    data
}

fn benchmark_parse_small(c: &mut Criterion) {
    let data = create_wpilog_with_records(100);
    let size = data.len();

    let mut group = c.benchmark_group("parse_small");
    group.throughput(Throughput::Bytes(size as u64));

    group.bench_function(BenchmarkId::new("100_records", size), |b| {
        b.iter(|| {
            let df = WpilogParser::from_bytes(black_box(data.clone())).unwrap();
            black_box(df);
        });
    });

    group.finish();
}

fn benchmark_parse_medium(c: &mut Criterion) {
    let data = create_wpilog_with_records(10_000);
    let size = data.len();

    let mut group = c.benchmark_group("parse_medium");
    group.throughput(Throughput::Bytes(size as u64));

    group.bench_function(BenchmarkId::new("10k_records", size), |b| {
        b.iter(|| {
            let df = WpilogParser::from_bytes(black_box(data.clone())).unwrap();
            black_box(df);
        });
    });

    group.finish();
}

fn benchmark_parse_large(c: &mut Criterion) {
    let data = create_wpilog_with_records(100_000);
    let size = data.len();

    let mut group = c.benchmark_group("parse_large");
    group.throughput(Throughput::Bytes(size as u64));
    group.sample_size(10); // Reduce sample size for large benchmark

    group.bench_function(BenchmarkId::new("100k_records", size), |b| {
        b.iter(|| {
            let df = WpilogParser::from_bytes(black_box(data.clone())).unwrap();
            black_box(df);
        });
    });

    group.finish();
}

fn benchmark_parse_very_large(c: &mut Criterion) {
    let data = create_wpilog_with_records(500_000);
    let size = data.len();

    let mut group = c.benchmark_group("parse_very_large");
    group.throughput(Throughput::Bytes(size as u64));
    group.sample_size(10); // Reduce sample size for very large benchmark

    group.bench_function(BenchmarkId::new("500k_records", size), |b| {
        b.iter(|| {
            let df = WpilogParser::from_bytes(black_box(data.clone())).unwrap();
            black_box(df);
        });
    });

    group.finish();
}

fn benchmark_schema_inference(c: &mut Criterion) {
    let data = create_wpilog_with_records(100_000);

    c.bench_function("schema_inference_100k", |b| {
        b.iter(|| {
            let schema = wpilog_polars::infer_schema(black_box(&data)).unwrap();
            black_box(schema);
        });
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default().with_profiler(pprof::criterion::PProfProfiler::new(100, Output::Flamegraph(None)));
    targets =
        benchmark_parse_small,
        benchmark_parse_medium,
        benchmark_parse_large,
        benchmark_parse_very_large,
        benchmark_schema_inference
}
criterion_main!(benches);
