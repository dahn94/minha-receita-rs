mod common;

use minha_receita_rs::query::DataContext;
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
