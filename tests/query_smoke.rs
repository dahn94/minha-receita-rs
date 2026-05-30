mod common;

use minha_receita_rs::query::{DataContext, SearchParams};
use tempfile::TempDir;

#[tokio::test]
async fn opens_and_counts() {
    let td = TempDir::new().unwrap();
    common::write_tiny_companies(td.path()).await;
    let ctx = DataContext::open(td.path()).await.unwrap();
    let count = ctx.row_count().await.unwrap();
    assert_eq!(count, 2);
}

#[tokio::test]
async fn lookup_returns_matching_row() {
    let td = TempDir::new().unwrap();
    common::write_tiny_companies(td.path()).await;
    let ctx = DataContext::open(td.path()).await.unwrap();
    let batches = ctx.lookup("00000000000191").await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn lookup_normalizes_punctuation() {
    let td = TempDir::new().unwrap();
    common::write_tiny_companies(td.path()).await;
    let ctx = DataContext::open(td.path()).await.unwrap();
    let batches = ctx.lookup("00.000.000/0001-91").await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn search_by_uf_returns_filtered() {
    let td = TempDir::new().unwrap();
    common::write_tiny_companies(td.path()).await;
    let ctx = DataContext::open(td.path()).await.unwrap();
    let p = SearchParams {
        uf: Some("SP".into()),
        limit: 10,
        ..Default::default()
    };
    let batches = ctx.search(&p).await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn search_limit_caps_rows() {
    let td = TempDir::new().unwrap();
    common::write_tiny_companies(td.path()).await;
    let ctx = DataContext::open(td.path()).await.unwrap();
    let p = SearchParams {
        limit: 1,
        ..Default::default()
    };
    let batches = ctx.search(&p).await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn search_all_returns_everything() {
    let td = TempDir::new().unwrap();
    common::write_tiny_companies(td.path()).await;
    let ctx = DataContext::open(td.path()).await.unwrap();
    let p = SearchParams {
        all: true,
        limit: 1, // ignored when all = true
        ..Default::default()
    };
    let batches = ctx.search(&p).await.unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
}

#[tokio::test]
async fn sql_raw_passes_through() {
    let td = TempDir::new().unwrap();
    common::write_tiny_companies(td.path()).await;
    let ctx = DataContext::open(td.path()).await.unwrap();
    let batches = ctx
        .sql("SELECT uf, COUNT(*) AS n FROM companies GROUP BY uf")
        .await
        .unwrap();
    let total: i64 = batches
        .iter()
        .map(|b| {
            b.column_by_name("n")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .unwrap()
                .iter()
                .flatten()
                .sum::<i64>()
        })
        .sum();
    assert_eq!(total, 2);
}
