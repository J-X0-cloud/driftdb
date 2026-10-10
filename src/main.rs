//! `driftdb` command line interface.

use std::io::{self, BufRead, BufReader, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::{Args, Parser, Subcommand, ValueEnum};

use driftdb::downsample::run_rule;
use driftdb::query::time::{format_rfc3339, parse_duration};
use driftdb::server::{serve, ServerConfig};
use driftdb::storage::segment::Segment;
use driftdb::{Db, DownsampleRule, Error, Options, Precision};

#[derive(Debug, Parser)]
#[command(
    name = "driftdb",
    version,
    about = "Embedded time-series storage engine"
)]
struct Cli {
    /// Database directory.
    #[arg(
        long,
        short = 'd',
        global = true,
        env = "DRIFTDB_DATA_DIR",
        default_value = "driftdb-data"
    )]
    data_dir: PathBuf,

    #[command(flatten)]
    engine: EngineArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Args)]
struct EngineArgs {
    /// Drop data older than this duration (e.g. 30d, 12h).
    #[arg(long, global = true, env = "DRIFTDB_RETENTION", value_parser = duration_arg)]
    retention: Option<i64>,

    /// Flush the memtable once it holds roughly this many MiB.
    #[arg(long, global = true, env = "DRIFTDB_MEMTABLE_MB", default_value_t = 16)]
    memtable_mb: usize,

    /// Do not fsync the WAL after each write (faster, loses the last
    /// writes on power failure).
    #[arg(long, global = true, env = "DRIFTDB_NO_SYNC")]
    no_sync: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the HTTP server.
    Serve {
        /// Interface to bind.
        #[arg(long, env = "DRIFTDB_BIND", default_value = "0.0.0.0")]
        bind: IpAddr,
        /// Port to listen on.
        #[arg(long, env = "PORT", default_value_t = 8086)]
        port: u16,
        /// Background maintenance period; 0s disables it.
        #[arg(long, default_value = "10s", value_parser = duration_arg)]
        maintenance_interval: i64,
        /// Downsampling rule `source:interval:aggs[:target[:lag]]`; repeatable.
        #[arg(long = "downsample", env = "DRIFTDB_DOWNSAMPLE", value_delimiter = ';')]
        downsample: Vec<DownsampleRule>,
    },
    /// Write line protocol from a file or stdin.
    Write {
        /// Input file; `-` or omitted reads stdin.
        file: Option<PathBuf>,
        /// Timestamp precision of the input.
        #[arg(long, short, default_value = "ns")]
        precision: Precision,
    },
    /// Run a query.
    Query {
        /// Query text.
        query: String,
        /// Output format.
        #[arg(long, short, value_enum, default_value_t = Format::Table)]
        format: Format,
        /// Print raw nanosecond timestamps instead of RFC 3339.
        #[arg(long)]
        epoch: bool,
    },
    /// Interactive query shell reading statements from stdin.
    Shell,
    /// Flush the memtable to a segment.
    Flush,
    /// Compact segments.
    Compact {
        /// Merge every segment into one.
        #[arg(long)]
        full: bool,
    },
    /// Apply the retention policy now.
    Retention,
    /// Run downsampling rules once.
    Downsample {
        /// Rules `source:interval:aggs[:target[:lag]]`.
        #[arg(required = true)]
        rules: Vec<DownsampleRule>,
    },
    /// Print engine statistics as JSON.
    Stats,
    /// Describe the contents of one segment file.
    Inspect {
        /// Segment file path.
        segment: PathBuf,
    },
    /// Print synthetic line protocol for demos and load tests.
    Generate {
        /// Number of hosts.
        #[arg(long, default_value_t = 10)]
        hosts: usize,
        /// Points per host.
        #[arg(long, default_value_t = 1_000)]
        points: usize,
        /// Spacing between points.
        #[arg(long, default_value = "10s", value_parser = duration_arg)]
        interval: i64,
        /// Timestamp of the first point (nanoseconds); defaults so the
        /// last point lands near the current time.
        #[arg(long)]
        start: Option<i64>,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Format {
    Table,
    Json,
}

/// `println!` that reports a closed stdout (for example `| head`) as an
/// I/O error instead of panicking.
macro_rules! outln {
    ($($arg:tt)*) => {
        writeln!(io::stdout(), $($arg)*)?
    };
}

/// `print!` counterpart of [`outln!`].
macro_rules! out {
    ($($arg:tt)*) => {
        write!(io::stdout(), $($arg)*)?
    };
}

fn duration_arg(s: &str) -> Result<i64, String> {
    parse_duration(s).ok_or_else(|| format!("invalid duration {s:?} (examples: 10s, 5m, 30d)"))
}

impl EngineArgs {
    fn options(&self) -> Options {
        Options {
            memtable_bytes: self.memtable_mb.max(1) << 20,
            sync_wal: !self.no_sync,
            retention: self.retention,
            ..Options::default()
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let default_level = if matches!(cli.command, Command::Serve { .. }) {
        "info"
    } else {
        "warn"
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level)),
        )
        .with_writer(io::stderr)
        .init();

    match run(cli) {
        Ok(()) => ExitCode::SUCCESS,
        // The reader went away (e.g. `driftdb query ... | head`); not an error.
        Err(Error::Io(e)) if e.kind() == io::ErrorKind::BrokenPipe => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(cli: Cli) -> driftdb::Result<()> {
    let opts = cli.engine.options();
    match cli.command {
        Command::Serve {
            bind,
            port,
            maintenance_interval,
            downsample,
        } => {
            let db = Arc::new(Db::open(&cli.data_dir, opts)?);
            let config = ServerConfig {
                addr: SocketAddr::new(bind, port),
                maintenance_interval: (maintenance_interval > 0)
                    .then(|| Duration::from_nanos(maintenance_interval as u64)),
                rules: downsample,
            };
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            runtime.block_on(serve(db, config))
        }
        Command::Write { file, precision } => {
            let db = Db::open(&cli.data_dir, opts)?;
            let started = Instant::now();
            let (written, expired) = match file.as_deref() {
                None => write_stream(&db, io::stdin().lock(), precision)?,
                Some(p) if p == Path::new("-") => write_stream(&db, io::stdin().lock(), precision)?,
                Some(p) => {
                    let f = std::fs::File::open(p).map_err(|e| Error::io_at(p, e))?;
                    write_stream(&db, BufReader::new(f), precision)?
                }
            };
            db.close()?;
            let secs = started.elapsed().as_secs_f64();
            eprintln!(
                "wrote {written} points in {secs:.2}s ({:.0} points/s){}",
                written as f64 / secs.max(1e-9),
                if expired > 0 {
                    format!(", skipped {expired} outside retention")
                } else {
                    String::new()
                }
            );
            Ok(())
        }
        Command::Query {
            query,
            format,
            epoch,
        } => {
            let db = Db::open(&cli.data_dir, opts)?;
            let result = db.query(&query)?;
            print_result(&result, format, epoch)
        }
        Command::Shell => shell(Db::open(&cli.data_dir, opts)?),
        Command::Flush => {
            let db = Db::open(&cli.data_dir, opts)?;
            match db.flush()? {
                Some(meta) => outln!("flushed {} points into segment {}", meta.points, meta.id),
                None => outln!("memtable was empty"),
            }
            Ok(())
        }
        Command::Compact { full } => {
            let db = Db::open(&cli.data_dir, opts)?;
            db.flush()?;
            let reports = if full {
                db.compact_all()?.into_iter().collect()
            } else {
                db.compact()?
            };
            if reports.is_empty() {
                outln!("nothing to compact");
            }
            for r in reports {
                outln!(
                    "merged {:?} -> {} (level {}): {} -> {} points, {} -> {} bytes",
                    r.inputs,
                    r.output
                        .map_or_else(|| "nothing".into(), |id| format!("segment {id}")),
                    r.level,
                    r.points_in,
                    r.points_out,
                    r.bytes_in,
                    r.bytes_out
                );
            }
            Ok(())
        }
        Command::Retention => {
            if opts.retention.is_none() {
                return Err(Error::InvalidArgument(
                    "pass --retention to apply a policy".into(),
                ));
            }
            let db = Db::open(&cli.data_dir, opts)?;
            outln!("removed {} segment(s)", db.enforce_retention()?);
            Ok(())
        }
        Command::Downsample { rules } => {
            let db = Db::open(&cli.data_dir, opts)?;
            for rule in &rules {
                let r = run_rule(&db, rule)?;
                outln!(
                    "{rule}: {} windows, {} points, watermark {}",
                    r.windows,
                    r.points,
                    r.watermark.map_or_else(|| "unset".into(), format_rfc3339)
                );
            }
            db.close()
        }
        Command::Stats => {
            let db = Db::open(&cli.data_dir, opts)?;
            let stats = db.stats()?;
            outln!(
                "{}",
                serde_json::to_string_pretty(&stats)
                    .map_err(|e| Error::InvalidArgument(e.to_string()))?
            );
            Ok(())
        }
        Command::Inspect { segment } => inspect(&segment),
        Command::Generate {
            hosts,
            points,
            interval,
            start,
        } => generate(hosts, points, interval, start),
    }
}

/// Stream line protocol into the database in bounded batches.
fn write_stream(
    db: &Db,
    input: impl BufRead,
    precision: Precision,
) -> driftdb::Result<(usize, usize)> {
    const BATCH_LINES: usize = 20_000;
    let (mut written, mut expired) = (0, 0);
    let mut batch = String::new();
    let mut batch_start_line = 1;
    let mut lines_in_batch = 0;
    let mut flush_batch = |batch: &mut String, first_line: usize| -> driftdb::Result<()> {
        let summary = db.write_lines(batch, precision).map_err(|e| match e {
            Error::LineProtocol { line, message } => Error::LineProtocol {
                line: line + first_line - 1,
                message,
            },
            other => other,
        })?;
        written += summary.written;
        expired += summary.expired;
        batch.clear();
        Ok(())
    };
    for line in input.lines() {
        let line = line?;
        batch.push_str(&line);
        batch.push('\n');
        lines_in_batch += 1;
        if lines_in_batch == BATCH_LINES {
            flush_batch(&mut batch, batch_start_line)?;
            batch_start_line += lines_in_batch;
            lines_in_batch = 0;
        }
    }
    if !batch.is_empty() {
        flush_batch(&mut batch, batch_start_line)?;
    }
    Ok((written, expired))
}

fn print_result(result: &driftdb::QueryResult, format: Format, epoch: bool) -> driftdb::Result<()> {
    match format {
        Format::Table => {
            if result.series.is_empty() {
                outln!("(no results)");
            } else {
                out!("{}", result.to_table(!epoch));
            }
        }
        Format::Json => outln!(
            "{}",
            serde_json::to_string_pretty(result)
                .map_err(|e| Error::InvalidArgument(e.to_string()))?
        ),
    }
    Ok(())
}

fn shell(db: Db) -> driftdb::Result<()> {
    let stdin = io::stdin();
    let interactive = std::io::IsTerminal::is_terminal(&stdin);
    let mut statement = String::new();
    let mut input = stdin.lock();
    loop {
        if interactive {
            out!(
                "{}",
                if statement.is_empty() {
                    "driftdb> "
                } else {
                    "      -> "
                }
            );
            io::stdout().flush()?;
        }
        let mut line = String::new();
        if input.read_line(&mut line)? == 0 {
            break;
        }
        let trimmed = line.trim();
        if statement.is_empty() && matches!(trimmed, "exit" | "quit" | "\\q") {
            break;
        }
        statement.push_str(&line);
        // Statements end with ';' (or a blank line when interactive).
        if !(trimmed.ends_with(';') || (interactive && trimmed.is_empty())) {
            continue;
        }
        let text = std::mem::take(&mut statement);
        if text.trim().trim_end_matches(';').trim().is_empty() {
            continue;
        }
        let started = Instant::now();
        match db.query(&text) {
            Ok(result) => {
                print_result(&result, Format::Table, false)?;
                if interactive {
                    outln!("({:.1} ms)", started.elapsed().as_secs_f64() * 1e3);
                }
            }
            Err(e) => eprintln!("error: {e}"),
        }
    }
    if !statement.trim().is_empty() {
        match db.query(&statement) {
            Ok(result) => print_result(&result, Format::Table, false)?,
            Err(e) => eprintln!("error: {e}"),
        }
    }
    Ok(())
}

fn inspect(path: &Path) -> driftdb::Result<()> {
    let id = path
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(driftdb::storage::manifest::parse_segment_file_name)
        .unwrap_or(0);
    let seg = Segment::open(path, id)?;
    let stats = seg.stats();
    let mut blocks = 0usize;
    let mut block_bytes = 0u64;
    let mut per_series: Vec<(u64, u64, usize, u64)> = Vec::new();
    for sid in seg.series_ids() {
        let handles = seg.blocks(sid);
        let pts: u64 = handles.iter().map(|h| u64::from(h.count)).sum();
        let bytes: u64 = handles.iter().map(|h| u64::from(h.len)).sum();
        // Decode every block so checksum errors surface here.
        for h in handles {
            seg.read_block(h)?;
        }
        blocks += handles.len();
        block_bytes += bytes;
        per_series.push((sid, pts, handles.len(), bytes));
    }
    outln!("segment       {}", path.display());
    outln!("file bytes    {}", stats.bytes);
    outln!("series        {}", stats.series);
    outln!("points        {}", stats.points);
    outln!("blocks        {blocks}");
    if stats.points > 0 {
        outln!(
            "time range    {} .. {}",
            format_rfc3339(stats.min_ts),
            format_rfc3339(stats.max_ts)
        );
        outln!(
            "compression   {:.2} bytes/point in blocks ({:.1}x vs 16 raw bytes)",
            block_bytes as f64 / stats.points as f64,
            16.0 * stats.points as f64 / block_bytes.max(1) as f64
        );
    }
    per_series.sort_by(|a, b| b.1.cmp(&a.1));
    outln!(
        "\n{:>10}  {:>10}  {:>6}  {:>10}",
        "series",
        "points",
        "blocks",
        "bytes"
    );
    for (sid, pts, nblocks, bytes) in per_series.iter().take(20) {
        outln!("{sid:>10}  {pts:>10}  {nblocks:>6}  {bytes:>10}");
    }
    if per_series.len() > 20 {
        outln!("... {} more series", per_series.len() - 20);
    }
    Ok(())
}

/// Deterministic pseudo-random CPU-like data (xorshift; no extra deps).
fn generate(hosts: usize, points: usize, interval: i64, start: Option<i64>) -> driftdb::Result<()> {
    let start = start.unwrap_or_else(|| {
        let span = interval.saturating_mul(points as i64);
        driftdb::model::now_nanos().saturating_sub(span)
    });
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        (state >> 11) as f64 / (1u64 << 53) as f64
    };
    let stdout = io::stdout();
    let mut out = io::BufWriter::new(stdout.lock());
    let regions = ["us-east", "us-west", "eu-central", "ap-south"];
    let mut levels: Vec<f64> = (0..hosts).map(|_| 20.0 + next() * 40.0).collect();
    for i in 0..points {
        let ts = start.saturating_add(interval.saturating_mul(i as i64));
        for (h, level) in levels.iter_mut().enumerate() {
            *level = (*level + (next() - 0.5) * 4.0).clamp(0.0, 100.0);
            let user = (*level * 100.0).round() / 100.0;
            let system = ((*level * 0.3 + next() * 2.0) * 100.0).round() / 100.0;
            writeln!(
                out,
                "cpu,host=host-{h:03},region={} usage_user={user},usage_system={system} {ts}",
                regions[h % regions.len()]
            )?;
        }
    }
    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_definition_is_consistent() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn write_stream_reports_global_line_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(
            dir.path(),
            Options {
                sync_wal: false,
                ..Options::default()
            },
        )
        .unwrap();
        let input = "m v=1 1\nm v=2 2\n";
        assert_eq!(
            write_stream(&db, input.as_bytes(), Precision::Nanoseconds).unwrap(),
            (2, 0)
        );
        let mut body = String::new();
        for i in 0..20_005 {
            body.push_str(&format!("m v={i} {i}\n"));
        }
        body.push_str("m v=broken\n");
        match write_stream(&db, body.as_bytes(), Precision::Nanoseconds).unwrap_err() {
            Error::LineProtocol { line, .. } => assert_eq!(line, 20_006),
            other => panic!("{other}"),
        }
    }

    #[test]
    fn durations_parse_for_flags() {
        assert_eq!(duration_arg("30d"), Ok(30 * 86_400 * 1_000_000_000));
        assert!(duration_arg("thirty").is_err());
    }
}
