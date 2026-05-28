use std::path::Path;

use futures::stream::{self, StreamExt};
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

pub async fn download_all(
    client: &reqwest::Client,
    urls: &[String],
    dest_dir: &Path,
    concurrency: usize,
) -> Result<()> {
    std::fs::create_dir_all(dest_dir)?;
    let results: Vec<Result<()>> = stream::iter(urls.iter().cloned())
        .map(|url| {
            let client = client.clone();
            let dest = dest_dir.join(url.rsplit('/').next().unwrap_or("file.bin"));
            async move { download_file(&client, &url, &dest).await }
        })
        .buffer_unordered(concurrency.max(1))
        .collect()
        .await;
    for r in results {
        r?;
    }
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

pub async fn discover_and_download(
    client: &reqwest::Client,
    base_url: &str,
    period_override: Option<Period>,
    dest_dir: &Path,
    concurrency: usize,
) -> Result<Period> {
    let period = if let Some(p) = period_override {
        p
    } else {
        let html = client.get(base_url).send().await?.error_for_status()?.text().await?;
        latest_period(&html)?
    };
    let period_url = format!("{}{}/", base_url.trim_end_matches('/'), &format!("/{}", period));
    let html = client.get(&period_url).send().await?.error_for_status()?.text().await?;
    let names = parse_file_listing(&html);
    let urls: Vec<String> = names.iter().map(|n| format!("{period_url}{n}")).collect();
    download_all(client, &urls, dest_dir, concurrency).await?;
    Ok(period)
}

pub async fn fetch_latest_period(client: &reqwest::Client, base_url: &str) -> Result<Period> {
    let html = client.get(base_url).send().await?.error_for_status()?.text().await?;
    latest_period(&html)
}

pub async fn fetch_ibge_url(client: &reqwest::Client) -> Result<String> {
    const CKAN: &str = "https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id=abb968cb-3710-4f85-89cf-875c91b9c7f6";
    let body = client.get(CKAN).send().await?.error_for_status()?.text().await?;
    parse_ibge_ckan(&body)
}

pub const RECEITA_BASE_URL: &str =
    "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/";

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

    #[tokio::test]
    async fn discover_and_download_full_flow() {
        let mut server = mockito::Server::new_async().await;
        // Listing raiz
        let _root = server.mock("GET", "/dados_abertos_cnpj/")
            .with_body(r#"<a href="2026-04/">2026-04/</a>"#)
            .create_async().await;
        let _period_dir = server.mock("GET", "/dados_abertos_cnpj/2026-04/")
            .with_body(r#"<a href="Cnaes.zip">Cnaes.zip</a>"#)
            .create_async().await;
        let _file = server.mock("GET", "/dados_abertos_cnpj/2026-04/Cnaes.zip")
            .with_status(200).with_body("XX").with_header("content-length", "2")
            .create_async().await;

        let td = tempfile::TempDir::new().unwrap();
        let client = reqwest::Client::new();
        let base = format!("{}/dados_abertos_cnpj/", server.url());
        let period = discover_and_download(&client, &base, None, td.path(), 2).await.unwrap();

        assert_eq!(period.to_string(), "2026-04");
        assert_eq!(std::fs::read_to_string(td.path().join("Cnaes.zip")).unwrap(), "XX");
    }

    #[tokio::test]
    async fn parallel_orchestrator_downloads_all() {
        let mut server = mockito::Server::new_async().await;
        let m1 = server.mock("GET", "/a.zip").with_status(200).with_body("aaa")
            .with_header("content-length", "3").create_async().await;
        let m2 = server.mock("GET", "/b.zip").with_status(200).with_body("bbbb")
            .with_header("content-length", "4").create_async().await;

        let td = tempfile::TempDir::new().unwrap();
        let client = reqwest::Client::new();
        let urls = vec![format!("{}/a.zip", server.url()), format!("{}/b.zip", server.url())];
        download_all(&client, &urls, td.path(), 2).await.unwrap();

        assert_eq!(std::fs::read_to_string(td.path().join("a.zip")).unwrap(), "aaa");
        assert_eq!(std::fs::read_to_string(td.path().join("b.zip")).unwrap(), "bbbb");
        m1.assert_async().await;
        m2.assert_async().await;
    }
}
