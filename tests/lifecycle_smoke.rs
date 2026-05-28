use std::path::PathBuf;
use tempfile::TempDir;
use minha_receita_rs::lifecycle;

#[tokio::test]
async fn init_writes_period_file() {
    let td = TempDir::new().unwrap();
    let zip_src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/2026-01.zip");
    let ibge = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/tabmun.csv");

    lifecycle::init_from_local(td.path(), &zip_src, &ibge, "2026-01".parse().unwrap()).await.unwrap();
    let p = std::fs::read_to_string(td.path().join(".period")).unwrap();
    assert_eq!(p.trim(), "2026-01");
    assert!(td.path().join("companies").exists());
}
