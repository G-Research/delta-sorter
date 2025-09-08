use once_cell::sync::OnceCell;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use sorter_core::{compact_with_sort, validate_global_order, SortConfig};

static RUNTIME: OnceCell<tokio::runtime::Runtime> = OnceCell::new();

fn rt() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("deltasort-rt")
            .build()
            .expect("tokio runtime")
    })
}

#[pyfunction]
#[pyo3(signature = (table_uri, sort_columns, target_file_size_bytes=None, predicate=None, concurrency=None, dry_run=None, repartition_by_sort_key=None, nulls=None))]
fn compact(
    table_uri: String,
    sort_columns: Vec<String>,
    target_file_size_bytes: Option<usize>,
    predicate: Option<String>,
    concurrency: Option<usize>,
    dry_run: Option<bool>,
    repartition_by_sort_key: Option<bool>,
    nulls: Option<String>,
) -> PyResult<()> {
    let cfg = SortConfig {
        sort_columns,
        target_file_size_bytes,
        predicate,
        concurrency: concurrency.unwrap_or(8),
        dry_run: dry_run.unwrap_or(false),
        repartition_by_sort_key: repartition_by_sort_key.unwrap_or(false),
        nulls_first: match nulls.as_deref() {
            Some("last") => false,
            _ => true,
        },
    };
    rt().block_on(compact_with_sort(&table_uri, cfg))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))
}

#[pyfunction]
#[pyo3(signature = (table_uri, sort_columns, nulls=None))]
fn validate(
    table_uri: String,
    sort_columns: Vec<String>,
    nulls: Option<String>,
) -> PyResult<Py<PyDict>> {
    let nulls_first = match nulls.as_deref() {
        Some("last") => false,
        _ => true,
    };
    let report = rt()
        .block_on(validate_global_order(
            &table_uri,
            &sort_columns,
            nulls_first,
        ))
        .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    Python::attach(|py| {
        let d = pyo3::types::PyDict::new(py);
        d.set_item("checked_files", report.checked_files)?;
        d.set_item("boundary_violations", report.boundary_violations)?;
        d.set_item("details_sample", report.details_sample)?;
        Ok(d.unbind())
    })
}

#[pymodule]
fn deltasort(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(compact, m)?)?;
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    Ok(())
}
