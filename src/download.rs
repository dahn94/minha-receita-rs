use std::path::Path;

use futures::StreamExt;
use scraper::{Html, Selector};
use serde::Deserialize;
use tokio::io::AsyncWriteExt;

use crate::schema::Period;
use crate::{Error, Result};

pub async fn download_file(client: &reqwest::Client, url: &str, dest: &Path) -> Result<()> {
    // Resume / skip se já existir com tamanho conhecido.
    if dest.exists() {
        let local_len = std::fs::metadata(dest)?.len();
        let head = client.head(url).send().await?;
        if let Some(remote_len) = head
            .headers()
            .get(reqwest::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
        {
            if local_len == remote_len {
                tracing::info!(file=%dest.display(), "already complete, skipping");
                return Ok(());
            }
        }
    }
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let resp = client.get(url).send().await?;
    let status = resp.status();
    if !status.is_success() {
        return Err(Error::Http { url: url.into(), status: status.as_u16() });
    }
    let mut file = tokio::fs::File::create(dest).await?;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
    }
    file.flush().await?;
    Ok(())
}

#[derive(Deserialize)]
struct CkanPkg { result: CkanResult }
#[derive(Deserialize)]
struct CkanResult { resources: Vec<CkanResource> }
#[derive(Deserialize)]
struct CkanResource { url: String }

pub fn parse_ibge_ckan(json: &str) -> Result<String> {
    let pkg: CkanPkg = serde_json::from_str(json)?;
    pkg.result.resources.into_iter()
        .find(|r| r.url.to_lowercase().ends_with(".csv"))
        .map(|r| r.url)
        .ok_or_else(|| Error::MissingData("no CSV resource in CKAN response".into()))
}

pub fn parse_period_listing(html: &str) -> Vec<Period> {
    let doc = Html::parse_document(html);
    let sel = Selector::parse("a").unwrap();
    doc.select(&sel)
        .filter_map(|a| {
            let href = a.value().attr("href")?.trim_end_matches('/');
            href.parse::<Period>().ok()
        })
        .collect()
}

pub fn parse_file_listing(html: &str) -> Vec<String> {
    let doc = Html::parse_document(html);
    let sel = Selector::parse("a").unwrap();
    doc.select(&sel)
        .filter_map(|a| a.value().attr("href").map(|s| s.to_string()))
        .filter(|s| s.ends_with(".zip"))
        .collect()
}

pub fn latest_period(html: &str) -> Result<Period> {
    parse_period_listing(html)
        .into_iter()
        .max()
        .ok_or_else(|| Error::MissingData("no period in listing".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
<html><body>
<a href="2024-12/">2024-12/</a>
<a href="2025-01/">2025-01/</a>
<a href="2026-04/">2026-04/</a>
<a href="readme.txt">readme.txt</a>
</body></html>
"#;

    #[test]
    fn parses_yyyy_mm_anchors() {
        let mut periods = parse_period_listing(SAMPLE);
        periods.sort();
        let strs: Vec<String> = periods.iter().map(|p| p.to_string()).collect();
        assert_eq!(strs, vec!["2024-12", "2025-01", "2026-04"]);
    }

    #[test]
    fn ignores_non_period_anchors() {
        let html = r#"<a href="foo">x</a><a href="2026-05/">2026-05/</a>"#;
        let periods = parse_period_listing(html);
        assert_eq!(periods.len(), 1);
        assert_eq!(periods[0].to_string(), "2026-05");
    }

    const FILES_SAMPLE: &str = r#"
<html><body>
<a href="Empresas0.zip">Empresas0.zip</a>
<a href="Estabelecimentos0.zip">Estabelecimentos0.zip</a>
<a href="Socios0.zip">Socios0.zip</a>
<a href="Cnaes.zip">Cnaes.zip</a>
<a href="../">../</a>
</body></html>
"#;

    #[test]
    fn parses_zip_anchors() {
        let names = parse_file_listing(FILES_SAMPLE);
        assert!(names.iter().any(|n| n == "Empresas0.zip"));
        assert!(names.iter().any(|n| n == "Cnaes.zip"));
        assert!(!names.iter().any(|n| n == "../"));
    }

    const CKAN_SAMPLE: &str = r#"
{
  "success": true,
  "result": {
    "resources": [
      {"url": "https://example.com/other.pdf", "format": "pdf"},
      {"url": "https://example.com/tabmun.csv", "format": "CSV"}
    ]
  }
}
"#;

    #[test]
    fn extracts_csv_resource_url() {
        let url = parse_ibge_ckan(CKAN_SAMPLE).unwrap();
        assert_eq!(url, "https://example.com/tabmun.csv");
    }

    #[tokio::test]
    async fn downloads_a_file() {
        let mut server = mockito::Server::new_async().await;
        let m = server.mock("GET", "/file.zip")
            .with_status(200)
            .with_body("hello-bytes")
            .with_header("content-length", "11")
            .create_async().await;

        let td = tempfile::TempDir::new().unwrap();
        let dest = td.path().join("file.zip");
        let client = reqwest::Client::new();
        download_file(&client, &format!("{}/file.zip", server.url()), &dest).await.unwrap();

        let got = std::fs::read_to_string(&dest).unwrap();
        assert_eq!(got, "hello-bytes");
        m.assert_async().await;
    }

    #[tokio::test]
    async fn skips_when_size_matches() {
        let td = tempfile::TempDir::new().unwrap();
        let dest = td.path().join("file.zip");
        std::fs::write(&dest, "hello-bytes").unwrap();

        let mut server = mockito::Server::new_async().await;
        // Espera-se nenhum GET — só HEAD com content-length.
        let m_head = server.mock("HEAD", "/file.zip")
            .with_status(200)
            .with_header("content-length", "11")
            .create_async().await;

        let client = reqwest::Client::new();
        download_file(&client, &format!("{}/file.zip", server.url()), &dest).await.unwrap();
        m_head.assert_async().await;
    }
}
