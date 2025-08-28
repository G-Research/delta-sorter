use anyhow::{anyhow, Context, Result};
use deltalake::kernel::Action;
use deltalake::kernel::scalars::ScalarExt;
use deltalake::protocol::DeltaOperation;
use deltalake::writer::DeltaWriter;
use deltalake::{DeltaTable, DeltaTableError};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::path::Path as FsPath;
use std::sync::Arc;
use tracing::{debug, info, warn};
use datafusion::prelude::ParquetReadOptions;
use deltalake::arrow::array::{Array, ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray, StringArray, BooleanArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray};
use deltalake::arrow::datatypes::{DataType, TimeUnit};

#[derive(Clone, Debug)]
enum SortVal {
    Null,
    Bool(bool),
    Int(i128),
    Float(f64),
    Str(String),
    Ts(i128),
    Other(String),
}

/// Compare two sort values with NULL handling.
///
/// Assumes values originate from the same column across rows/files and thus
/// have the same logical type (except for NULL). If mismatched non-NULL types
/// are encountered, the comparison falls back to a stable but arbitrary
/// ordering and triggers a debug assertion.
fn cmp_sort_val_with_nulls(a: &SortVal, b: &SortVal, nulls_first: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (SortVal::Null, SortVal::Null) => Equal,
        (SortVal::Null, _) => if nulls_first { Less } else { Greater },
        (_, SortVal::Null) => if nulls_first { Greater } else { Less },
        (SortVal::Bool(x), SortVal::Bool(y)) => x.cmp(y),
        (SortVal::Int(x), SortVal::Int(y)) => x.cmp(y),
        (SortVal::Float(x), SortVal::Float(y)) => x.partial_cmp(y).unwrap_or(Equal),
        (SortVal::Ts(x), SortVal::Ts(y)) => x.cmp(y),
        (SortVal::Str(x), SortVal::Str(y)) => x.cmp(y),
        // Mismatched non-NULL types: should not occur with a consistent schema.
        // Use a stable but arbitrary ordering and flag in debug builds.
        (ax, by) => {
            debug_assert!(
                std::mem::discriminant(ax) == std::mem::discriminant(by),
                "mismatched sort value types: {:?} vs {:?}",
                ax,
                by
            );
            format!("{:?}", ax).cmp(&format!("{:?}", by))
        },
    }
}

fn cmp_tuple_with_nulls(a: &[SortVal], b: &[SortVal], nulls_first: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    for (va, vb) in a.iter().zip(b.iter()) {
        let ord = cmp_sort_val_with_nulls(va, vb, nulls_first);
        if ord != Equal { return ord; }
    }
    a.len().cmp(&b.len())
}

/// Configuration for sorting and compaction behavior.
///
/// - `sort_columns`: Columns used for lexicographic ordering.
/// - `target_file_size_bytes`: Advisory parquet file size; writer may exceed.
/// - `predicate`: Reserved for future filtering support.
/// - `concurrency`: Max concurrent partition rewrites.
/// - `dry_run`: If true, plans/validates only without committing changes.
/// - `repartition_by_sort_key`: If true, perform strict full-table sorted overwrite.
/// - `nulls_first`: Controls NULLS FIRST/LAST behavior in ordering.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SortConfig {
    pub sort_columns: Vec<String>,
    pub target_file_size_bytes: Option<usize>,
    pub predicate: Option<String>,
    pub concurrency: usize,
    pub dry_run: bool,
    pub repartition_by_sort_key: bool,
    pub nulls_first: bool,
}

impl Default for SortConfig {
    fn default() -> Self {
        Self {
            sort_columns: vec![],
            target_file_size_bytes: None,
            predicate: None,
            concurrency: 8,
            dry_run: false,
            repartition_by_sort_key: false,
            nulls_first: true,
        }
    }
}

/// A plan describing which groups (partitions) to rewrite.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RewritePlan {
    pub table_uri: String,
    pub groups: Vec<RewriteGroup>,
}

/// A single rewrite group, typically a partition, with input files and size estimates.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RewriteGroup {
    pub partition: Option<Vec<(String, String)>>,
    pub input_files: Vec<String>,
    pub estimated_rows: usize,
    pub estimated_bytes: usize,
}

/// Summary of ordering validation across table files.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ValidationReport {
    pub checked_files: usize,
    pub boundary_violations: usize,
    pub details_sample: Vec<String>,
}

/// Per-partition metrics emitted after a rewrite commit.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PartitionMetrics {
    pub partition: Option<Vec<(String, String)>>,
    pub files_in: usize,
    pub files_out: usize,
    pub bytes_in: i64,
    pub bytes_out: i64,
    pub duration_ms: u128,
}

/// Compact small files and enforce ordering.
///
/// Default mode rewrites only partitions that fail ordering validation.
/// If `cfg.repartition_by_sort_key` is true, performs a full-table sorted overwrite.
pub async fn compact_with_sort(table_uri: &str, cfg: SortConfig) -> Result<()> {
    info!(table_uri, "starting compaction with global sort");

    if cfg.repartition_by_sort_key {
        warn!(
            "repartition-by-sort-key enabled: performing full-table overwrite sorted by {:?}",
            cfg.sort_columns
        );
        if cfg.dry_run {
            info!("dry-run: would execute full-table sorted overwrite");
            return Ok(());
        }
        return commit_sorted_overwrite(table_uri, &cfg).await;
    } else {
        let plan = match plan_rewrites(table_uri, &cfg).await {
            Ok(p) => p,
            Err(e) => {
                if cfg.dry_run {
                    // In dry-run mode, tolerate planning failures (e.g., missing table in scaffold)
                    warn!(error=?e, "dry-run: planning failed; skipping execution");
                    return Ok(());
                } else {
                    return Err(e);
                }
            }
        };
        if cfg.dry_run {
            info!(groups = plan.groups.len(), "dry-run: planned groups");
            debug!(?plan, "rewrite plan");
            return Ok(());
        }

        
        let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(cfg.concurrency.max(1)));
        let mut handles = futures::stream::FuturesUnordered::new();
        let start_all = std::time::Instant::now();
        let mut total_files_in: usize = 0;
        let mut total_files_out: usize = 0;
        let mut total_bytes_in: i64 = 0;
        let mut total_bytes_out: i64 = 0;
        let mut partitions_processed: usize = 0;
        for group in plan.groups {
            let permit = sem.clone().acquire_owned().await.unwrap();
            let g = group.clone();
            let table_uri = table_uri.to_string();
            let cfg = cfg.clone();
            handles.push(tokio::spawn(async move {
                let _p = permit;
                let res = rewrite_partition_tx(&table_uri, &g, &cfg).await;
                if let Err(ref e) = res {
                    warn!(error=?e, partition=?g.partition, "partition rewrite failed");
                }
                res
            }));
        }
        while let Some(res) = handles.next().await {
            let metrics = res??;
            total_files_in += metrics.files_in;
            total_files_out += metrics.files_out;
            total_bytes_in += metrics.bytes_in;
            total_bytes_out += metrics.bytes_out;
            partitions_processed += 1;
        }
        let elapsed_ms = start_all.elapsed().as_millis();
        info!(partitions_processed, total_files_in, total_files_out, total_bytes_in, total_bytes_out, elapsed_ms, "rewrite run summary");
    }

    info!("compaction with sort completed");
    Ok(())
}

#[deprecated(note = "renamed to compact_with_sort; behavior unchanged")]
pub async fn compact_with_global_sort(table_uri: &str, cfg: SortConfig) -> Result<()> {
    compact_with_sort(table_uri, cfg).await
}

/// Build a partition-aware rewrite plan by validating ordering per partition.
pub async fn plan_rewrites(table_uri: &str, cfg: &SortConfig) -> Result<RewritePlan> {
    let table = deltalake::open_table(table_uri)
        .await
        .with_context(|| format!("open_table({table_uri})"))?;

    if let Ok(schema) = table.get_schema() {
        let field_names: std::collections::HashSet<&str> =
            schema.fields().map(|f| f.name.as_str()).collect();
        for col in &cfg.sort_columns {
            if !field_names.contains(col.as_str()) {
                warn!(column = col, "sort column not found in schema");
            }
        }
    }

    use std::collections::BTreeMap;
    let mut by_partition: BTreeMap<String, RewriteGroup> = BTreeMap::new();
    for add in table
        .get_active_add_actions_by_partitions(&[])?
        .collect::<Result<Vec<_>, _>>()?
    {
        let pvals = add.partition_values().unwrap_or_default();
        let mut parts_vec: Vec<(String, String)> = pvals
            .iter()
            .map(|(k, v)| (k.to_string(), v.serialize()))
            .collect();
        parts_vec.sort_by(|a, b| a.0.cmp(&b.0));
        let key = if parts_vec.is_empty() {
            "__nopart__".to_string()
        } else {
            parts_vec
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("/")
        };

        let entry = by_partition.entry(key).or_insert_with(|| RewriteGroup {
            partition: if parts_vec.is_empty() { None } else { Some(parts_vec.clone()) },
            input_files: Vec::new(),
            estimated_rows: 0,
            estimated_bytes: 0,
        });
        entry.input_files.push(add.path().to_string());
        let sz = add.size();
        entry.estimated_bytes = entry.estimated_bytes.saturating_add(sz as usize);
    }

    let mut groups = Vec::new();
    for g in by_partition.into_values() {
        match partition_is_sorted(table_uri, &g.partition, &cfg.sort_columns).await {
            Ok(true) => {
                debug!(?g.partition, "partition already sorted; skipping");
            }
            Ok(false) => groups.push(g),
            Err(e) => {
                warn!(error=?e, "validator failed; including partition for rewrite");
                groups.push(g);
            }
        }
    }

    groups.sort_by(|a, b| b.estimated_bytes.cmp(&a.estimated_bytes));

    Ok(RewritePlan {
        table_uri: table_uri.to_string(),
        groups,
    })
}

/// Rewrite a single partition: read rows, sort by `cfg.sort_columns`, and overwrite.
pub async fn rewrite_partition_overwrite(
    table_uri: &str,
    group: &RewriteGroup,
    cfg: &SortConfig,
) -> Result<()> {
    use datafusion::prelude::{col, lit, Expr, SessionContext};
    use deltalake::operations::DeltaOps;

    let table = deltalake::open_table(table_uri)
        .await
        .with_context(|| format!("open_table({table_uri}) for partition rewrite"))?;

    let ctx = SessionContext::new();
    ctx.register_table("t", std::sync::Arc::new(table.clone()))
        .context("register delta table in DataFusion (partition)")?;
    let mut df = ctx.table("t").await.context("open DF table t (partition)")?;

    if let Some(parts) = &group.partition {
        let mut pred: Option<Expr> = None;
        for (k, v) in parts {
            let e = col(k).eq(lit(v.clone()));
            pred = Some(match pred { Some(p) => p.and(e), None => e });
        }
        if let Some(p) = pred { df = df.filter(p)?; }
    }

    let sort_exprs = cfg
        .sort_columns
        .iter()
        .map(|c| col(c).sort(true, true))
        .collect::<Vec<_>>();
    if !sort_exprs.is_empty() {
        df = df.sort(sort_exprs)?;
    }

    let batches = df.collect().await?;
    let _updated = DeltaOps(table)
        .write(batches)
        .with_save_mode(deltalake::protocol::SaveMode::Overwrite)
        .await?;
    Ok(())
}

/// Execute a rewrite plan by reading, sorting, and writing new files (no commit).
///
/// Returns the adds and removes to be committed by the caller.
pub async fn execute_rewrites(
    plan: &RewritePlan,
    cfg: &SortConfig,
) -> Result<(Vec<deltalake::kernel::Add>, Vec<deltalake::kernel::Remove>)> {
    use datafusion::prelude::{col, SessionContext};
    use deltalake::writer::RecordBatchWriter;
    use futures::StreamExt;

    if plan.groups.is_empty() {
        return Ok((vec![], vec![]));
    }

    // Open table and prepare DataFusion context
    let table = deltalake::open_table(&plan.table_uri)
        .await
        .with_context(|| format!("open_table({})", &plan.table_uri))?;
    let ctx = SessionContext::new();
    ctx.register_table("t", std::sync::Arc::new(table.clone()))
        .context("register delta table in DataFusion")?;

    // Build sorting expressions from user columns
    let sort_exprs = cfg
        .sort_columns
        .iter()
        .map(|c| col(c).sort(true, true))
        .collect::<Vec<_>>();

    let df = ctx.table("t").await.context("open DF table t")?;
    let df = if sort_exprs.is_empty() {
        df
    } else {
        df.sort(sort_exprs).context("apply sort")?
    };

    let mut stream = df.execute_stream().await.context("execute DF stream")?;

    let mut writer = RecordBatchWriter::for_table(&table)
        .map_err(|e| anyhow!(e))?;

    while let Some(batch) = stream.next().await.transpose().context("next batch")? {
        writer.write(batch).await.map_err(|e| anyhow!(e))?;
    }

    let mut removes: Vec<deltalake::kernel::Remove> = Vec::new();
    for add in table
        .get_active_add_actions_by_partitions(&[])?
        .collect::<Result<Vec<_>, _>>()?
    {
        let remove = add.remove_action(false);
        removes.push(remove);
    }

    let mut adds: Vec<deltalake::kernel::Add> = writer.flush().await.map_err(|e| anyhow!(e))?;
    // Mark adds as data_change = false to match rewrite semantics
    for a in &mut adds {
        a.data_change = false;
    }

    Ok((adds, removes))
}

/// Perform a strict global sort and atomically replace table contents.
pub async fn commit_sorted_overwrite(table_uri: &str, cfg: &SortConfig) -> Result<()> {
    use datafusion::prelude::{col, SessionContext};
    use deltalake::kernel::{Action, Remove};
    use deltalake::protocol::{DeltaOperation, SaveMode};
    use deltalake::operations::transaction::CommitBuilder;

    let mut table = deltalake::open_table(table_uri)
        .await
        .with_context(|| format!("open_table({table_uri}) for overwrite"))?;

    let start = std::time::Instant::now();
    let ctx = SessionContext::new();
    ctx.register_table("t", std::sync::Arc::new(table.clone()))
        .context("register delta table in DataFusion (overwrite)")?;
    let mut sql = String::from("SELECT * FROM t");
    if !cfg.sort_columns.is_empty() {
        let nulls = if cfg.nulls_first { "NULLS FIRST" } else { "NULLS LAST" };
        let order = cfg
            .sort_columns
            .iter()
            .map(|c| format!("\"{}\" {}", c, nulls))
            .collect::<Vec<_>>()
            .join(",");
        sql.push_str(&format!(" ORDER BY {}", order));
    }
    let df = ctx.sql(&sql).await?;

    let plan = df.create_physical_plan().await?;

    let mut removes: Vec<Remove> = Vec::new();
    let mut bytes_in: i64 = 0;
    for add in table
        .get_active_add_actions_by_partitions(&[])?
        .collect::<Result<Vec<_>, _>>()?
    {
        bytes_in += add.size();
        removes.push(add.remove_action(false));
    }

    let files_in = removes.len();
    let files_out = 0usize;
    let bytes_out: i64 = 0;

    let _updated = deltalake::operations::write::WriteBuilder::new(table.log_store().clone(), table.state)
        .with_input_execution_plan(plan)
        .with_input_session_state(ctx.state())
        .with_save_mode(SaveMode::Overwrite)
        .await?;
    let duration_ms = start.elapsed().as_millis();
    info!(files_in, bytes_in, duration_ms, "full-table sorted overwrite committed");
    Ok(())
}

/// Validate global ordering by checking inter-file boundaries and per-file monotonicity.
pub async fn validate_global_order(table_uri: &str, sort_columns: &[String], nulls_first: bool) -> Result<ValidationReport> {
    let table = deltalake::open_table(table_uri).await?;
    let uris: Vec<String> = table.get_file_uris()?.collect();
    let ctx = datafusion::prelude::SessionContext::new();
    let mut entries = Vec::new();
    let mut violations = 0usize;
    let mut details = Vec::new();
    for uri in uris {
        if let Some((mins, maxs)) = minmax_for_uri(&ctx, &uri, sort_columns).await? {
            entries.push((uri.clone(), mins, maxs));
        }
        if !file_is_monotonic(&ctx, &uri, sort_columns, nulls_first).await? {
            violations += 1;
            details.push(format!("monotonicity violation within file: {}", uri));
        }
    }
    let checked = entries.len();
    let (boundary_violations, mut boundary_details) = count_boundary_violations(entries, nulls_first);
    violations += boundary_violations;
    details.append(&mut boundary_details);
    details.truncate(20);
    Ok(ValidationReport { checked_files: checked, boundary_violations: violations, details_sample: details })
}

async fn file_is_monotonic(
    ctx: &datafusion::prelude::SessionContext,
    uri: &str,
    cols: &[String],
    nulls_first: bool,
) -> Result<bool> {
    if cols.is_empty() { return Ok(true); }
    let df = ctx.read_parquet(vec![uri.to_string()], ParquetReadOptions::default()).await?;
    let batches = df.collect().await?;
    if batches.is_empty() { return Ok(true); }
    let schema = batches[0].schema();
    let mut indices: Vec<usize> = Vec::with_capacity(cols.len());
    for c in cols {
        let idx = schema.index_of(c)?;
        indices.push(idx);
    }
    let mut prev: Option<Vec<SortVal>> = None;
    for batch in batches {
        let rows = batch.num_rows();
        for r in 0..rows {
            let mut t = Vec::with_capacity(indices.len());
            for &i in &indices {
                t.push(arrow_value_to_sortval(batch.column(i).clone(), r));
            }
            if let Some(p) = &prev {
                if cmp_tuple_with_nulls(&t, p, nulls_first).is_lt() { return Ok(false); }
            }
            prev = Some(t);
        }
    }
    Ok(true)
}

async fn partition_is_sorted(
    table_uri: &str,
    partition: &Option<Vec<(String, String)>>,
    sort_columns: &[String],
) -> Result<bool> {
    let table = deltalake::open_table(table_uri).await?;
    // Build candidate file URIs
    let mut uris: Vec<String> = table.get_file_uris()?.collect();
    if let Some(parts) = partition {
        uris.retain(|u| parts.iter().all(|(k, v)| u.contains(&format!("{k}={v}"))));
    }
    let ctx = datafusion::prelude::SessionContext::new();
    let mut entries: Vec<(String, Vec<SortVal>, Vec<SortVal>)> = Vec::new();
    for uri in uris {
        if let Some((mins, maxs)) = minmax_for_uri(&ctx, &uri, sort_columns).await? {
            entries.push((uri, mins, maxs));
        }
    }
    if entries.is_empty() { return Ok(true); }
    let (violations, _details) = count_boundary_violations(entries, true);
    Ok(violations == 0)
}

fn count_boundary_violations(
    mut entries: Vec<(String, Vec<SortVal>, Vec<SortVal>)>,
    nulls_first: bool,
) -> (usize, Vec<String>) {
    // Sort files by min tuple to define sequence
    entries.sort_by(|a, b| cmp_tuple_with_nulls(&a.1, &b.1, nulls_first));
    let mut violations = 0usize;
    let mut details = Vec::new();
    for win in entries.windows(2) {
        let (path_a, _min_a, max_a) = (&win[0].0, &win[0].1, &win[0].2);
        let (path_b, min_b, _max_b) = (&win[1].0, &win[1].1, &win[1].2);
        if cmp_tuple_with_nulls(max_a, min_b, nulls_first).is_gt() {
            violations += 1;
            details.push(format!("boundary violation: {} max > {} min", path_a, path_b));
        }
    }
    (violations, details)
}

// stats-based extraction removed in favor of per-file sampling

async fn minmax_for_uri(
    ctx: &datafusion::prelude::SessionContext,
    uri: &str,
    cols: &[String],
) -> Result<Option<(Vec<SortVal>, Vec<SortVal>)>> {
    if cols.is_empty() { return Ok(None); }
    let df = ctx.read_parquet(vec![uri.to_string()], ParquetReadOptions::default()).await?;
    let batches = df.collect().await?;
    if batches.is_empty() { return Ok(None); }
    let schema = batches[0].schema();
    let mut indices: Vec<usize> = Vec::with_capacity(cols.len());
    for c in cols {
        let idx = schema.index_of(c)?;
        indices.push(idx);
    }
    let mut min_tuple: Option<Vec<SortVal>> = None;
    let mut max_tuple: Option<Vec<SortVal>> = None;
    for batch in batches {
        let rows = batch.num_rows();
        for r in 0..rows {
            let mut t = Vec::with_capacity(indices.len());
            for &i in &indices {
                t.push(arrow_value_to_sortval(batch.column(i).clone(), r));
            }
            match &min_tuple {
                None => min_tuple = Some(t.clone()),
                Some(current) => { if cmp_tuple(&t, current).is_lt() { min_tuple = Some(t.clone()); } }
            }
            match &max_tuple {
                None => max_tuple = Some(t.clone()),
                Some(current) => { if cmp_tuple(&t, current).is_gt() { max_tuple = Some(t); } }
            }
        }
    }
    match (min_tuple, max_tuple) {
        (Some(mins), Some(maxs)) => Ok(Some((mins, maxs))),
        _ => Ok(None),
    }
}

fn arrow_value_to_sortval(arr: ArrayRef, idx: usize) -> SortVal {
    match arr.data_type() {
        DataType::Int32 => {
            let a = arr.as_any().downcast_ref::<Int32Array>().unwrap();
            if a.is_null(idx) { SortVal::Null } else { SortVal::Int(a.value(idx) as i128) }
        }
        DataType::Int64 => {
            let a = arr.as_any().downcast_ref::<Int64Array>().unwrap();
            if a.is_null(idx) { SortVal::Null } else { SortVal::Int(a.value(idx) as i128) }
        }
        DataType::Float32 => {
            let a = arr.as_any().downcast_ref::<Float32Array>().unwrap();
            if a.is_null(idx) { SortVal::Null } else { SortVal::Float(a.value(idx) as f64) }
        }
        DataType::Float64 => {
            let a = arr.as_any().downcast_ref::<Float64Array>().unwrap();
            if a.is_null(idx) { SortVal::Null } else { SortVal::Float(a.value(idx)) }
        }
        DataType::Utf8 => {
            let a = arr.as_any().downcast_ref::<StringArray>().unwrap();
            if a.is_null(idx) { SortVal::Null } else { SortVal::Str(a.value(idx).to_string()) }
        }
        DataType::LargeUtf8 => {
            let a = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
            if a.is_null(idx) { SortVal::Null } else { SortVal::Str(a.value(idx).to_string()) }
        }
        DataType::Boolean => {
            let a = arr.as_any().downcast_ref::<BooleanArray>().unwrap();
            if a.is_null(idx) { SortVal::Null } else { SortVal::Bool(a.value(idx)) }
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let a = arr.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
            if a.is_null(idx) { SortVal::Null } else { SortVal::Ts(a.value(idx) as i128) }
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let a = arr.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
            if a.is_null(idx) { SortVal::Null } else { SortVal::Ts(a.value(idx) as i128) }
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let a = arr.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
            if a.is_null(idx) { SortVal::Null } else { SortVal::Ts(a.value(idx) as i128) }
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let a = arr.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
            if a.is_null(idx) { SortVal::Null } else { SortVal::Ts(a.value(idx) as i128) }
        }
        _ => SortVal::Other(format!("{arr:?}")),
    }
}


fn cmp_tuple(a: &[SortVal], b: &[SortVal]) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    for (va, vb) in a.iter().zip(b.iter()) {
        let ord = cmp_sort_val_with_nulls(va, vb, true);
        if ord != Equal { return ord; }
    }
    a.len().cmp(&b.len())
}

pub async fn rewrite_partition_tx(
    table_uri: &str,
    group: &RewriteGroup,
    cfg: &SortConfig,
) -> Result<PartitionMetrics> {
    use datafusion::prelude::{col, lit, Expr, SessionContext};
    use deltalake::kernel::{Action, Remove};
    use deltalake::protocol::{DeltaOperation, SaveMode};
    use deltalake::operations::transaction::CommitBuilder;

    let mut table = deltalake::open_table(table_uri)
        .await
        .with_context(|| format!("open_table({table_uri}) for partition rewrite (tx)"))?;

    let start = std::time::Instant::now();
    let ctx = SessionContext::new();
    ctx.register_table("t", std::sync::Arc::new(table.clone()))
        .context("register delta table in DataFusion (partition)")?;
    let mut sql = String::from("SELECT * FROM t");
    if let Some(parts) = &group.partition {
        sql.push_str(" WHERE ");
        sql.push_str(&build_partition_predicate_sql_typed(&table, parts));
    }
    if !cfg.sort_columns.is_empty() {
        let nulls = if cfg.nulls_first { "NULLS FIRST" } else { "NULLS LAST" };
        let order = cfg
            .sort_columns
            .iter()
            .map(|c| format!("\"{}\" {}", c, nulls))
            .collect::<Vec<_>>()
            .join(",");
        sql.push_str(&format!(" ORDER BY {}", order));
    }
    let df = ctx.sql(&sql).await?;

    let mut stream = df.execute_stream().await?;
    let mut rb_writer = deltalake::writer::RecordBatchWriter::for_table(&table)?;
    let mut all_adds: Vec<deltalake::kernel::Add> = Vec::new();
    let target = cfg.target_file_size_bytes.unwrap_or(0);
    while let Some(batch) = stream.next().await.transpose()? {
        rb_writer.write(batch).await?;
        if target > 0 && rb_writer.buffer_len() >= target {
            let mut adds = rb_writer.flush().await?;
            for a in &mut adds { a.data_change = false; }
            all_adds.extend(adds);
        }
    }
    let mut adds = rb_writer.flush().await?;
    for a in &mut adds { a.data_change = false; }
    all_adds.extend(adds);

    let mut removes: Vec<Remove> = Vec::new();
    let mut bytes_in: i64 = 0;
    for add in table
        .get_active_add_actions_by_partitions(&[])?
        .collect::<Result<Vec<_>, _>>()?
    {
        let pvals = add.partition_values().unwrap_or_default();
        let mut parts_vec: Vec<(String, String)> = pvals
            .iter()
            .map(|(k, v)| (k.to_string(), v.serialize()))
            .collect();
        parts_vec.sort_by(|a, b| a.0.cmp(&b.0));
        if &Some(parts_vec) == &group.partition {
            bytes_in += add.size();
            removes.push(add.remove_action(false));
        }
    }

    let files_in = removes.len();
    let files_out = all_adds.len();
    let bytes_out: i64 = all_adds.iter().map(|a| a.size).sum();

    let mut actions: Vec<Action> = Vec::with_capacity(files_in + files_out);
    actions.extend(removes.into_iter().map(Action::Remove));
    actions.extend(all_adds.into_iter().map(Action::Add));

    let predicate_str = group.partition.as_ref().map(|parts| build_partition_predicate_sql_typed(&table, parts));
    let operation = DeltaOperation::Write { mode: SaveMode::Overwrite, partition_by: None, predicate: predicate_str };

    let version = CommitBuilder::default()
        .with_actions(actions)
        .build(Some(table.snapshot().unwrap()), table.log_store().clone(), operation)
        .await?
        .version();
    table.update().await?;
    let duration_ms = start.elapsed().as_millis();
    info!(?group.partition, files_in, files_out, bytes_in, bytes_out, duration_ms, version, "partition rewrite committed");
    Ok(PartitionMetrics { partition: group.partition.clone(), files_in, files_out, bytes_in, bytes_out, duration_ms })
}

fn build_partition_predicate_sql(parts: &[(String, String)]) -> String {
    let mut exprs = Vec::new();
    for (k, v) in parts {
        let vv = v.trim_matches('"');
        if vv.eq_ignore_ascii_case("null") {
            exprs.push(format!("\"{}\" IS NULL", k));
        } else {
            let esc = vv.replace("'", "''");
            exprs.push(format!("\"{}\" = '{}'", k, esc));
        }
    }
    exprs.join(" AND ")
}

fn build_partition_predicate_sql_typed(table: &DeltaTable, parts: &[(String, String)]) -> String {
    use deltalake::kernel::DataType as KDT;
    let schema = match table.get_schema() { Ok(s) => s, Err(_) => return build_partition_predicate_sql(parts) };
    let mut type_map = std::collections::HashMap::new();
    for f in schema.fields() { type_map.insert(f.name.clone(), f.data_type().clone()); }
    build_partition_predicate_sql_from_types(&type_map, parts)
}

fn build_partition_predicate_sql_from_types(
    type_map: &std::collections::HashMap<String, deltalake::kernel::DataType>,
    parts: &[(String, String)],
) -> String {
    use deltalake::kernel::{DataType as KDT, PrimitiveType as KPT};
    let mut exprs = Vec::new();
    for (k, raw) in parts {
        let val = raw.trim_matches('"');
        if val.eq_ignore_ascii_case("null") {
            exprs.push(format!("\"{}\" IS NULL", k));
            continue;
        }
        let dt = type_map.get(k);
        let push_num = |key: &str, v: &str, exprs: &mut Vec<String>| {
            exprs.push(format!("\"{}\" = {}", key, v));
        };
        match dt {
            Some(KDT::Primitive(KPT::Byte))
            | Some(KDT::Primitive(KPT::Short))
            | Some(KDT::Primitive(KPT::Integer))
            | Some(KDT::Primitive(KPT::Long)) => {
                if val.parse::<i128>().is_ok() { push_num(k, val, &mut exprs); }
                else { exprs.push(format!("\"{}\" = '{}'", k, val.replace("'", "''"))); }
            }
            Some(KDT::Primitive(KPT::Float)) | Some(KDT::Primitive(KPT::Double)) => {
                if val.parse::<f64>().is_ok() { push_num(k, val, &mut exprs); }
                else { exprs.push(format!("\"{}\" = '{}'", k, val.replace("'", "''"))); }
            }
            Some(KDT::Primitive(KPT::Boolean)) => {
                let low = val.to_ascii_lowercase();
                if low == "true" || low == "false" {
                    exprs.push(format!("\"{}\" = {}", k, low.to_ascii_uppercase()));
                } else {
                    exprs.push(format!("\"{}\" = '{}'", k, val.replace("'", "''")));
                }
            }
            Some(KDT::Primitive(KPT::Decimal(_, _))) => {
                if val.chars().all(|c| c.is_ascii_digit() || c == '.' || c == '-' || c == '+') {
                    push_num(k, val, &mut exprs);
                } else {
                    exprs.push(format!("\"{}\" = '{}'", k, val.replace("'", "''")));
                }
            }
            _ => {
                let esc = val.replace("'", "''");
                exprs.push(format!("\"{}\" = '{}'", k, esc));
            }
        }
    }
    exprs.join(" AND ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::kernel::{DataType as KDT, PrimitiveType as KPT};
    use std::collections::HashMap;

    #[test]
    fn predicate_typing_numbers_strings_null() {
        let mut tm: HashMap<String, KDT> = HashMap::new();
        tm.insert("id".to_string(), KDT::Primitive(KPT::Integer));
        tm.insert("active".to_string(), KDT::Primitive(KPT::Boolean));
        tm.insert("amount".to_string(), KDT::Primitive(KPT::Decimal(10, 2)));
        tm.insert("country".to_string(), KDT::Primitive(KPT::String));
        tm.insert("region".to_string(), KDT::Primitive(KPT::String));

        let parts = vec![
            ("id".to_string(), "42".to_string()),
            ("active".to_string(), "true".to_string()),
            ("amount".to_string(), "1234.50".to_string()),
            ("country".to_string(), "US".to_string()),
            ("region".to_string(), "NULL".to_string()),
        ];
        let pred = build_partition_predicate_sql_from_types(&tm, &parts);
        assert_eq!(pred, "\"id\" = 42 AND \"active\" = TRUE AND \"amount\" = 1234.50 AND \"country\" = 'US' AND \"region\" IS NULL");
    }

    #[test]
    fn predicate_typing_unknown_type_quotes_string() {
        let tm: HashMap<String, KDT> = HashMap::new();
        let parts = vec![("code".to_string(), "001".to_string())];
        let pred = build_partition_predicate_sql_from_types(&tm, &parts);
        assert_eq!(pred, "\"code\" = '001'");
    }
}
