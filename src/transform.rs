use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use crate::Result;

pub fn extract_zip_to_dir(zip_path: &Path, out_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(out_dir)?;
    let file = File::open(zip_path)?;
    let mut archive = zip::ZipArchive::new(file)?;
    for i in 0..archive.len() {
        let mut entry = archive.by_index(i)?;
        let name = entry.name().to_string();
        if name.ends_with('/') {
            continue;
        }
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes)?;
        let (decoded, _, had_errors) = encoding_rs::WINDOWS_1252.decode(&bytes);
        if had_errors {
            tracing::warn!(file=%name, "cp1252 decoding produced replacement chars");
        }
        let dest = out_dir.join(&name);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut f = File::create(&dest)?;
        f.write_all(decoded.as_bytes())?;
    }
    Ok(())
}

pub fn extract_all(zip_dir: &Path, out_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(out_dir)?;
    for entry in std::fs::read_dir(zip_dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.extension().and_then(|s| s.to_str()) == Some("zip") {
            extract_zip_to_dir(&p, out_dir)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn write_test_zip(path: &Path, inner_name: &str, contents_cp1252: &[u8]) {
        let f = std::fs::File::create(path).unwrap();
        let mut zw = zip::ZipWriter::new(f);
        zw.start_file(inner_name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zw.write_all(contents_cp1252).unwrap();
        zw.finish().unwrap();
    }

    #[test]
    fn extracts_and_converts_cp1252() {
        let td = tempfile::TempDir::new().unwrap();
        let zip_path = td.path().join("Empresas0.zip");
        // 0xE7 = ç em CP1252
        write_test_zip(&zip_path, "Empresas0", &[b'a', 0xE7, b'o']);

        let out = td.path().join("out");
        extract_zip_to_dir(&zip_path, &out).unwrap();

        let extracted = std::fs::read_to_string(out.join("Empresas0")).unwrap();
        assert_eq!(extracted, "aço");
    }

    #[test]
    fn extract_all_walks_directory() {
        let td = tempfile::TempDir::new().unwrap();
        let zips = td.path().join("zips");
        std::fs::create_dir_all(&zips).unwrap();
        write_test_zip(&zips.join("a.zip"), "a.csv", b"hello");
        write_test_zip(&zips.join("b.zip"), "b.csv", b"world");

        let out = td.path().join("ext");
        extract_all(&zips, &out).unwrap();

        assert!(out.join("a.csv").exists());
        assert!(out.join("b.csv").exists());
    }
}
