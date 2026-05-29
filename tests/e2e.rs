use std::path::PathBuf;

use assert_cmd::Command;
use predicates::prelude::PredicateBooleanExt;
use predicates::str::contains;
use tempfile::TempDir;

fn testdata() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("testdata")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn full_pipeline_lookup_search_sql() {
    let td = TempDir::new().unwrap();

    minha_receita_rs::lifecycle::init_from_local(
        td.path(),
        &testdata().join("2026-01.zip"),
        &testdata().join("tabmun.csv"),
        "2026-01".parse().unwrap(),
    )
    .await
    .unwrap();

    Command::cargo_bin("minha-receita-rs")
        .unwrap()
        .args([
            "sql",
            "SELECT COUNT(*) AS n FROM companies",
            "--data",
            td.path().to_str().unwrap(),
            "--format",
            "csv",
        ])
        .assert()
        .success()
        .stdout(contains("n").and(contains("1")));

    Command::cargo_bin("minha-receita-rs")
        .unwrap()
        .args([
            "search",
            "--limit=1",
            "--data",
            td.path().to_str().unwrap(),
            "--format",
            "json",
        ])
        .assert()
        .success();
}
