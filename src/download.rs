use std::path::Path;

use futures::stream::{self, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;

use crate::schema::Period;
use crate::{Error, Result};

/// Receita Federal Nextcloud share. The public share is read-only WebDAV;
/// authenticate with the share token as username and empty password.
pub const RECEITA_WEBDAV_URL: &str =
    "https://arquivos.receitafederal.gov.br/public.php/webdav/";
pub const RECEITA_SHARE_TOKEN: &str = "YggdBLfdninEJX9";

pub async fn download_file(client: &reqwest::Client, url: &str, dest: &Path) -> Result<()> {
    download_file_with_progress(client, url, dest, None, None).await
}

fn pending_style() -> ProgressStyle {
    ProgressStyle::with_template("{prefix:25!} {msg}").unwrap()
}

fn active_style() -> ProgressStyle {
    ProgressStyle::with_template(
        "{prefix:25!} [{bar:30.cyan/blue}] {bytes:>10}/{total_bytes:<10} {bytes_per_sec:>12} eta {eta:>4} {msg}",
    )
    .unwrap()
    .progress_chars("=> ")
}

async fn download_file_with_progress(
    client: &reqwest::Client,
    url: &str,
    dest: &Path,
    auth_token: Option<&str>,
    pb: Option<&ProgressBar>,
) -> Result<()> {
    if let Some(pb) = pb {
        pb.set_style(active_style());
        pb.set_message("");
    }
    let head_req = client.head(url);
    let head_req = match auth_token {
        Some(tok) => head_req.basic_auth(tok, Some("")),
        None => head_req,
    };
    let head = head_req.send().await?;
    let remote_len = head
        .headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok());

    if dest.exists()
        && let Some(rl) = remote_len
        && std::fs::metadata(dest)?.len() == rl
    {
        if let Some(pb) = pb {
            pb.set_length(rl);
            pb.set_position(rl);
            pb.finish_with_message("já completo");
        } else {
            tracing::info!(file=%dest.display(), "already complete, skipping");
        }
        return Ok(());
    }

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let get_req = client.get(url);
    let get_req = match auth_token {
        Some(tok) => get_req.basic_auth(tok, Some("")),
        None => get_req,
    };
    let resp = get_req.send().await?;
    let status = resp.status();
    if !status.is_success() {
        return Err(Error::Http {
            url: url.into(),
            status: status.as_u16(),
        });
    }
    if let (Some(pb), Some(rl)) = (pb, remote_len) {
        pb.set_length(rl);
    }
    let mut file = tokio::fs::File::create(dest).await?;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        if let Some(pb) = pb {
            pb.inc(chunk.len() as u64);
        }
    }
    file.flush().await?;
    if let Some(pb) = pb {
        pb.finish_with_message("ok");
    }
    Ok(())
}

pub async fn download_all(
    client: &reqwest::Client,
    urls: &[String],
    dest_dir: &Path,
    concurrency: usize,
    auth_token: Option<&str>,
) -> Result<()> {
    std::fs::create_dir_all(dest_dir)?;

    let multi = MultiProgress::new();
    let bars: Vec<ProgressBar> = urls
        .iter()
        .map(|url| {
            let name = url.rsplit('/').next().unwrap_or("file.bin").to_string();
            let pb = multi.add(ProgressBar::new(0));
            pb.set_style(pending_style());
            pb.set_prefix(name);
            pb.set_message("aguardando");
            pb
        })
        .collect();

    let results: Vec<Result<()>> = stream::iter(urls.iter().zip(bars.iter()))
        .map(|(url, pb)| {
            let client = client.clone();
            let url = url.clone();
            let dest = dest_dir.join(url.rsplit('/').next().unwrap_or("file.bin"));
            async move {
                download_file_with_progress(&client, &url, &dest, auth_token, Some(pb)).await
            }
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
struct CkanPkg {
    result: CkanResult,
}
#[derive(Deserialize)]
struct CkanResult {
    resources: Vec<CkanResource>,
}
#[derive(Deserialize)]
struct CkanResource {
    url: String,
}

pub fn parse_ibge_ckan(json: &str) -> Result<String> {
    let pkg: CkanPkg = serde_json::from_str(json)?;
    pkg.result
        .resources
        .into_iter()
        .find(|r| r.url.to_lowercase().ends_with(".csv"))
        .map(|r| r.url)
        .ok_or_else(|| Error::MissingData("no CSV resource in CKAN response".into()))
}

/// Issues a WebDAV PROPFIND with Depth: 1 and returns the raw XML.
async fn propfind(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    depth: u8,
) -> Result<String> {
    let method = reqwest::Method::from_bytes(b"PROPFIND").expect("PROPFIND is a valid method name");
    let resp = client
        .request(method, url)
        .basic_auth(token, Some(""))
        .header("Depth", depth.to_string())
        .send()
        .await?;
    let status = resp.status();
    // Nextcloud answers PROPFIND with 207 Multi-Status.
    if !status.is_success() && status.as_u16() != 207 {
        return Err(Error::Http {
            url: url.into(),
            status: status.as_u16(),
        });
    }
    Ok(resp.text().await?)
}

/// Extract every `<*:href>` text node from a PROPFIND XML response.
fn extract_hrefs(xml: &str) -> Vec<String> {
    let mut reader = Reader::from_str(xml);
    reader.config_mut().trim_text(true);
    let mut hrefs = Vec::new();
    let mut buf = Vec::new();
    let mut in_href = false;
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(e)) => {
                let name = e.name();
                let bytes = name.as_ref();
                if bytes == b"href" || bytes.ends_with(b":href") {
                    in_href = true;
                }
            }
            Ok(Event::Text(e)) if in_href => {
                if let Ok(s) = e.unescape() {
                    hrefs.push(s.into_owned());
                }
            }
            Ok(Event::End(_)) => {
                in_href = false;
            }
            Ok(Event::Eof) => break,
            Err(_) => break,
            _ => {}
        }
        buf.clear();
    }
    hrefs
}

/// Extract `Period` directories from a PROPFIND response on the share root.
pub fn parse_propfind_periods(xml: &str) -> Vec<Period> {
    extract_hrefs(xml)
        .iter()
        .filter_map(|href| {
            let trimmed = href.trim_end_matches('/');
            let last = trimmed.rsplit('/').next()?;
            last.parse::<Period>().ok()
        })
        .collect()
}

/// Extract `.zip` filenames from a PROPFIND response on a period directory.
pub fn parse_propfind_files(xml: &str) -> Vec<String> {
    extract_hrefs(xml)
        .iter()
        .filter_map(|href| {
            let last = href.trim_end_matches('/').rsplit('/').next()?;
            Some(last.to_string())
        })
        .filter(|s| s.to_lowercase().ends_with(".zip"))
        .collect()
}

pub fn latest_propfind_period(xml: &str) -> Result<Period> {
    parse_propfind_periods(xml)
        .into_iter()
        .max()
        .ok_or_else(|| Error::MissingData("no period in PROPFIND response".into()))
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
        let xml = propfind(client, base_url, RECEITA_SHARE_TOKEN, 1).await?;
        latest_propfind_period(&xml)?
    };
    let period_url = format!("{}/{}/", base_url.trim_end_matches('/'), period);
    let xml = propfind(client, &period_url, RECEITA_SHARE_TOKEN, 1).await?;
    let names = parse_propfind_files(&xml);
    let urls: Vec<String> = names.iter().map(|n| format!("{period_url}{n}")).collect();
    download_all(client, &urls, dest_dir, concurrency, Some(RECEITA_SHARE_TOKEN)).await?;
    Ok(period)
}

pub async fn fetch_latest_period(client: &reqwest::Client, base_url: &str) -> Result<Period> {
    let xml = propfind(client, base_url, RECEITA_SHARE_TOKEN, 1).await?;
    latest_propfind_period(&xml)
}

pub async fn fetch_ibge_url(client: &reqwest::Client) -> Result<String> {
    const CKAN: &str = "https://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id=abb968cb-3710-4f85-89cf-875c91b9c7f6";
    let body = client
        .get(CKAN)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    parse_ibge_ckan(&body)
}

/// Back-compat alias.
pub const RECEITA_BASE_URL: &str = RECEITA_WEBDAV_URL;

#[cfg(test)]
mod tests {
    use super::*;

    const PROPFIND_PERIODS: &str = r#"<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
  <d:response><d:href>/public.php/webdav/</d:href></d:response>
  <d:response><d:href>/public.php/webdav/2024-12/</d:href></d:response>
  <d:response><d:href>/public.php/webdav/2025-01/</d:href></d:response>
  <d:response><d:href>/public.php/webdav/2026-04/</d:href></d:response>
  <d:response><d:href>/public.php/webdav/cnpj.tar.gz</d:href></d:response>
</d:multistatus>"#;

    #[test]
    fn parses_period_directories_from_propfind() {
        let mut periods = parse_propfind_periods(PROPFIND_PERIODS);
        periods.sort();
        let strs: Vec<String> = periods.iter().map(|p| p.to_string()).collect();
        assert_eq!(strs, vec!["2024-12", "2025-01", "2026-04"]);
    }

    #[test]
    fn latest_period_picks_max() {
        let p = latest_propfind_period(PROPFIND_PERIODS).unwrap();
        assert_eq!(p.to_string(), "2026-04");
    }

    const PROPFIND_FILES: &str = r#"<?xml version="1.0"?>
<d:multistatus xmlns:d="DAV:">
  <d:response><d:href>/public.php/webdav/2026-04/</d:href></d:response>
  <d:response><d:href>/public.php/webdav/2026-04/Empresas0.zip</d:href></d:response>
  <d:response><d:href>/public.php/webdav/2026-04/Estabelecimentos0.zip</d:href></d:response>
  <d:response><d:href>/public.php/webdav/2026-04/Cnaes.zip</d:href></d:response>
  <d:response><d:href>/public.php/webdav/2026-04/README.txt</d:href></d:response>
</d:multistatus>"#;

    #[test]
    fn parses_zip_files_from_propfind() {
        let names = parse_propfind_files(PROPFIND_FILES);
        assert!(names.iter().any(|n| n == "Empresas0.zip"));
        assert!(names.iter().any(|n| n == "Cnaes.zip"));
        assert!(!names.iter().any(|n| n == "README.txt"));
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
        let m = server
            .mock("GET", "/file.zip")
            .with_status(200)
            .with_body("hello-bytes")
            .with_header("content-length", "11")
            .create_async()
            .await;

        let td = tempfile::TempDir::new().unwrap();
        let dest = td.path().join("file.zip");
        let client = reqwest::Client::new();
        download_file(&client, &format!("{}/file.zip", server.url()), &dest)
            .await
            .unwrap();

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
        let m_head = server
            .mock("HEAD", "/file.zip")
            .with_status(200)
            .with_header("content-length", "11")
            .create_async()
            .await;

        let client = reqwest::Client::new();
        download_file(&client, &format!("{}/file.zip", server.url()), &dest)
            .await
            .unwrap();
        m_head.assert_async().await;
    }

    #[tokio::test]
    async fn parallel_orchestrator_downloads_all() {
        let mut server = mockito::Server::new_async().await;
        let m1 = server
            .mock("GET", "/a.zip")
            .with_status(200)
            .with_body("aaa")
            .with_header("content-length", "3")
            .create_async()
            .await;
        let m2 = server
            .mock("GET", "/b.zip")
            .with_status(200)
            .with_body("bbbb")
            .with_header("content-length", "4")
            .create_async()
            .await;

        let td = tempfile::TempDir::new().unwrap();
        let client = reqwest::Client::new();
        let urls = vec![
            format!("{}/a.zip", server.url()),
            format!("{}/b.zip", server.url()),
        ];
        download_all(&client, &urls, td.path(), 2, None).await.unwrap();

        assert_eq!(
            std::fs::read_to_string(td.path().join("a.zip")).unwrap(),
            "aaa"
        );
        assert_eq!(
            std::fs::read_to_string(td.path().join("b.zip")).unwrap(),
            "bbbb"
        );
        m1.assert_async().await;
        m2.assert_async().await;
    }
}
