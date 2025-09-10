use anyhow::Result;
use clap::Parser;
use sorter_core::{SortConfig, compact_with_sort, validate_global_order};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser, Debug)]
#[command(
    name = "deltasort",
    version,
    about = "Compaction + global sort for Delta tables (delta-rs)"
)]
struct Args {
    /// Table URI, e.g. s3://bucket/table or /path/to/table
    #[arg(long)]
    table: String,

    /// Comma-separated list of columns to sort by (lexicographic order)
    #[arg(long, value_delimiter = ',')]
    sort_columns: Vec<String>,

    /// Target file size in bytes (e.g., 268435456 for 256MB)
    #[arg(long)]
    target_file_size_bytes: Option<usize>,

    /// Optional predicate to limit rewrite (future use)
    #[arg(long)]
    predicate: Option<String>,

    /// Concurrency for IO and sorting
    #[arg(long, default_value_t = 8)]
    concurrency: usize,

    /// Plan only; do not commit
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Validate only: check global ordering and exit
    #[arg(long, default_value_t = false)]
    validate_only: bool,

    /// Perform full-table overwrite sorted by sort key (strict global ordering)
    #[arg(long, default_value_t = false)]
    repartition_by_sort_key: bool,

    /// Log level: off,error,warn,info,debug,trace (overrides RUST_LOG)
    #[arg(long)]
    log_level: Option<String>,

    /// Null sort behavior: first or last
    #[arg(long, value_parser = ["first", "last"], default_value = "first")]
    nulls: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let filter = if let Some(level) = args.log_level.as_deref() {
        EnvFilter::new(level)
    } else {
        EnvFilter::from_default_env()
    };
    fmt().with_env_filter(filter).init();
    let cfg = SortConfig {
        sort_columns: args.sort_columns,
        target_file_size_bytes: args.target_file_size_bytes,
        predicate: args.predicate,
        concurrency: args.concurrency,
        dry_run: args.dry_run,
        repartition_by_sort_key: args.repartition_by_sort_key,
        nulls_first: args.nulls == "first",
    };
    if args.validate_only {
        let report = validate_global_order(&args.table, &cfg.sort_columns, cfg.nulls_first).await?;
        println!(
            "Validated files: {} | boundary_violations: {}",
            report.checked_files, report.boundary_violations
        );
        if !report.details_sample.is_empty() {
            println!("Sample:");
            for line in &report.details_sample {
                println!("- {}", line);
            }
        }
        if report.boundary_violations > 0 {
            anyhow::bail!("Ordering violations found: {}", report.boundary_violations);
        }
        Ok(())
    } else {
        let dry = cfg.dry_run;
        let strict = cfg.repartition_by_sort_key;
        match compact_with_sort(&args.table, cfg).await {
            Ok(()) => {
                if dry {
                    println!("Dry run completed: planned rewrites; see logs for details.");
                } else if strict {
                    println!("Success: full-table sorted overwrite completed.");
                } else {
                    println!("Success: partition-aware compaction + sort completed.");
                }
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}
