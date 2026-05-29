use minha_receita_rs::lifecycle;
use minha_receita_rs::schema::Period;
use std::path::PathBuf;
use tempfile::TempDir;

#[tokio::test]
async fn init_writes_period_file() {
    let td = TempDir::new().unwrap();
    let zip_src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/2026-01.zip");
    let ibge = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata/tabmun.csv");

    lifecycle::init_from_local(td.path(), &zip_src, &ibge, "2026-01".parse().unwrap())
        .await
        .unwrap();
    let p = std::fs::read_to_string(td.path().join(".period")).unwrap();
    assert_eq!(p.trim(), "2026-01");
    assert!(td.path().join("companies").exists());
}

#[test]
fn read_period_file_works() {
    let td = TempDir::new().unwrap();
    std::fs::write(td.path().join(".period"), "2026-04").unwrap();
    let p = lifecycle::read_local_period(td.path()).unwrap();
    assert_eq!(p, "2026-04".parse::<Period>().unwrap());
}

#[test]
fn missing_period_file_errors() {
    let td = TempDir::new().unwrap();
    assert!(lifecycle::read_local_period(td.path()).is_err());
}
