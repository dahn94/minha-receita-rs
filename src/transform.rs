use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

use crate::Result;

pub fn classify(name: &str) -> Option<&'static str> {
    let upper = name.to_uppercase();
    let stem: &str = std::path::Path::new(&upper)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or(&upper);
    let kinds = [
        ("empresas", &["EMPRECSV", "EMPRESAS"][..]),
        ("estabelecimentos", &["ESTABELE", "ESTABELECIMENTOS"][..]),
        ("socios", &["SOCIOCSV", "SOCIOS"][..]),
        ("simples", &["SIMPLES.CSV", "SIMPLES"][..]),
        ("cnaes", &["CNAECSV", "CNAES"][..]),
        ("motivos", &["MOTICSV", "MOTIVOS"][..]),
        ("municipios", &["MUNICCSV", "MUNICIPIOS"][..]),
        ("naturezas", &["NATJUCSV", "NATUREZAS"][..]),
        ("paises", &["PAISCSV", "PAISES"][..]),
        ("qualificacoes", &["QUALSCSV", "QUALIFICACOES"][..]),
    ];
    for (kind, patterns) in &kinds {
        for p in *patterns {
            if stem.contains(p) {
                return Some(kind);
            }
        }
    }
    None
}

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

    #[test]
    fn classifies_real_receita_names() {
        assert_eq!(classify("K3241.K03200Y0.D10110.EMPRECSV"), Some("empresas"));
        assert_eq!(classify("K3241.K03200Y0.D10110.ESTABELE"), Some("estabelecimentos"));
        assert_eq!(classify("K3241.K03200Y0.D10110.SOCIOCSV"), Some("socios"));
        assert_eq!(classify("F.K03200$Z.D10110.CNAECSV"), Some("cnaes"));
        assert_eq!(classify("F.K03200$Z.D10110.MUNICCSV"), Some("municipios"));
        assert_eq!(classify("F.K03200$Z.D10110.NATJUCSV"), Some("naturezas"));
        assert_eq!(classify("F.K03200$Z.D10110.PAISCSV"), Some("paises"));
        assert_eq!(classify("F.K03200$Z.D10110.MOTICSV"), Some("motivos"));
        assert_eq!(classify("F.K03200$Z.D10110.QUALSCSV"), Some("qualificacoes"));
        assert_eq!(classify("D10110.SIMPLES.CSV.D10110"), Some("simples"));
        assert_eq!(classify("ignore.txt"), None);
    }

    #[test]
    fn classifies_test_friendly_names() {
        assert_eq!(classify("Empresas0"), Some("empresas"));
        assert_eq!(classify("Estabelecimentos0"), Some("estabelecimentos"));
        assert_eq!(classify("Socios0"), Some("socios"));
        assert_eq!(classify("Cnaes"), Some("cnaes"));
        assert_eq!(classify("Municipios"), Some("municipios"));
        assert_eq!(classify("Naturezas"), Some("naturezas"));
        assert_eq!(classify("Paises"), Some("paises"));
        assert_eq!(classify("Motivos"), Some("motivos"));
        assert_eq!(classify("Qualificacoes"), Some("qualificacoes"));
        assert_eq!(classify("Simples"), Some("simples"));
    }
}
