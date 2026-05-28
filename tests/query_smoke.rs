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
