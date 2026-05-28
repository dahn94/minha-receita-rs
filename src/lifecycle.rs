use std::path::{Path, PathBuf};

use crate::schema::Period;
use crate::{Error, Result};

pub fn default_root() -> PathBuf {
    if let Ok(env) = std::env::var("MR_DATA") {
        return PathBuf::from(env);
    }
    let base = dirs::data_dir().unwrap_or_else(|| PathBuf::from("."));
    base.join("minha-receita-rs")
}

/// Testable variant: assumes the outer zip is already on disk and tabmun.csv too.
pub async fn init_from_local(
    root: &Path,
    outer_zip: &Path,
    ibge_csv: &Path,
    period: Period,
) -> Result<()> {
    let companies = root.join("companies");
    if companies.exists() {
        return Err(Error::MissingData(format!(
            "{} já existe — use `update` em vez de `init`",
            companies.display()
        )));
    }
    std::fs::create_dir_all(root)?;
    let zips = root.join("zips");
    std::fs::create_dir_all(&zips)?;

    // Extract outer zip into a temp dir, copy inner per-source zips into `zips/`.
    let tmp = tempfile::TempDir::new()?;
    crate::transform::extract_zip_to_dir(outer_zip, tmp.path())?;
    let inner_dir = tmp.path().join(period.to_string());
    let source = if inner_dir.exists() { inner_dir } else { tmp.path().to_path_buf() };
    for entry in std::fs::read_dir(&source)? {
        let p = entry?.path();
        if p.extension().and_then(|s| s.to_str()) == Some("zip") {
            std::fs::copy(&p, zips.join(p.file_name().unwrap()))?;
        }
    }
    crate::transform::run(&zips, ibge_csv, root).await?;
    std::fs::write(root.join(".period"), period.to_string())?;
    Ok(())
}

pub async fn init(root: &Path, period_override: Option<String>, concurrency: usize) -> Result<()> {
    use crate::download::{
        RECEITA_BASE_URL, discover_and_download, fetch_ibge_url, download_file,
    };
    let client = reqwest::Client::builder()
        .user_agent("minha-receita-rs/0.1")
        .build()?;
    let period = period_override.map(|s| s.parse::<Period>()).transpose()?;
    let zips = root.join("zips");
    let actual_period = discover_and_download(&client, RECEITA_BASE_URL, period, &zips, concurrency).await?;
    let ibge_url = fetch_ibge_url(&client).await?;
    let ibge_csv = zips.join("tabmun.csv");
    download_file(&client, &ibge_url, &ibge_csv).await?;
    crate::transform::run(&zips, &ibge_csv, root).await?;
    std::fs::write(root.join(".period"), actual_period.to_string())?;
    Ok(())
}
