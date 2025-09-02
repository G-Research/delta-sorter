use sorter_core::{compact_with_sort, SortConfig};

#[tokio::test]
async fn dry_run_executes() {
    let cfg = SortConfig {
        sort_columns: vec!["objectId".into(), "dateTime".into()],
        dry_run: true,
        ..Default::default()
    };
    // Using a non-existing table URI is fine for dry-run in current scaffold
    let res = compact_with_sort("/tmp/nonexistent", cfg).await;
    assert!(res.is_ok());
}
