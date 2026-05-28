use assert_cmd::Command;
use predicates::str::contains;
use predicates::prelude::*;

fn bin() -> Command {
    Command::cargo_bin("minha-receita-rs").unwrap()
}

#[test]
fn shows_help() {
    bin().arg("--help").assert().success()
        .stdout(contains("init").and(contains("update"))
            .and(contains("download")).and(contains("transform"))
            .and(contains("lookup")).and(contains("search")).and(contains("sql")));
}

#[test]
fn lookup_requires_cnpj_arg() {
    bin().arg("lookup").assert().failure();
}

#[test]
fn lookup_parses_cnpj() {
    let out = bin().args(["lookup", "12345678000190", "--data", "/tmp/does-not-exist"]).output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(!stderr.contains("error: unrecognized"), "stderr: {stderr}");
}
