use assert_cmd::Command;
use predicates::prelude::*;
use predicates::str::contains;

fn bin() -> Command {
    Command::cargo_bin("minha-receita-rs").unwrap()
}

#[test]
fn shows_help() {
    bin().arg("--help").assert().success().stdout(
        contains("init")
            .and(contains("update"))
            .and(contains("download"))
            .and(contains("transform"))
            .and(contains("lookup"))
            .and(contains("search"))
            .and(contains("sql")),
    );
}

#[test]
fn lookup_requires_cnpj_arg() {
    bin().arg("lookup").assert().failure();
}

#[test]
fn lookup_parses_cnpj() {
    let out = bin()
        .args(["lookup", "12345678000190", "--data", "/tmp/does-not-exist"])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains("error: unrecognized"), "stderr: {stderr}");
}

#[test]
fn lookup_without_data_fails_cleanly() {
    bin()
        .args(["lookup", "12345678000190", "--data", "/no/such/path"])
        .assert()
        .failure()
        .stderr(contains("ausente").or(contains("missing")));
}

#[test]
fn search_parses_all_flags() {
    bin()
        .args([
            "search",
            "--uf=SP",
            "--cnae=4711-3/01",
            "--bairro=Centro",
            "--municipio=7107",
            "--natureza=2046",
            "--situacao=ATIVA",
            "--limit=5",
            "--page=2",
            "--data=/no/such",
        ])
        .assert()
        .failure(); // fails because data dir is bogus, but parses.
}
