use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::datasource::MemTable;
use datafusion::prelude::*;

use crate::Result;

pub fn raw_schema(kind: &str) -> Option<SchemaRef> {
    let f = |n: &str, t: DataType| Field::new(n, t, true);
    let utf = |n: &str| f(n, DataType::Utf8);

    let schema = match kind {
        "empresas" => Schema::new(vec![
            utf("cnpj_basico"),
            utf("razao_social"),
            utf("natureza_juridica"),
            utf("qualificacao_responsavel"),
            utf("capital_social_raw"),
            utf("porte"),
            utf("ente_federativo_responsavel"),
        ]),
        "estabelecimentos" => Schema::new(vec![
            utf("cnpj_basico"),
            utf("cnpj_ordem"),
            utf("cnpj_dv"),
            utf("identificador_matriz_filial"),
            utf("nome_fantasia"),
            utf("situacao_cadastral_codigo"),
            utf("data_situacao_cadastral_raw"),
            utf("motivo_situacao_cadastral"),
            utf("nome_cidade_exterior"),
            utf("codigo_pais"),
            utf("data_inicio_atividade_raw"),
            utf("cnae_fiscal_principal"),
            utf("cnae_fiscal_secundaria"),
            utf("tipo_logradouro"),
            utf("logradouro"),
            utf("numero"),
            utf("complemento"),
            utf("bairro"),
            utf("cep"),
            utf("uf"),
            utf("codigo_municipio"),
            utf("ddd1"),
            utf("telefone1"),
            utf("ddd2"),
            utf("telefone2"),
            utf("ddd_fax"),
            utf("fax"),
            utf("email"),
            utf("situacao_especial"),
            utf("data_situacao_especial_raw"),
        ]),
        "socios" => Schema::new(vec![
            utf("cnpj_basico"),
            utf("identificador_de_socio"),
            utf("nome_socio"),
            utf("cnpj_cpf_do_socio"),
            utf("codigo_qualificacao_socio"),
            utf("data_entrada_sociedade_raw"),
            utf("codigo_pais"),
            utf("cpf_representante_legal"),
            utf("nome_representante_legal"),
            utf("codigo_qualificacao_representante_legal"),
            utf("codigo_faixa_etaria"),
        ]),
        "simples" => Schema::new(vec![
            utf("cnpj_basico"),
            utf("opcao_pelo_simples_raw"),
            utf("data_opcao_pelo_simples_raw"),
            utf("data_exclusao_do_simples_raw"),
            utf("opcao_pelo_mei_raw"),
            utf("data_opcao_pelo_mei_raw"),
            utf("data_exclusao_do_mei_raw"),
        ]),
        "cnaes" | "motivos" | "naturezas" | "paises" | "qualificacoes" => Schema::new(vec![
            utf("codigo"),
            utf("descricao"),
        ]),
        "municipios" => Schema::new(vec![
            utf("codigo"),
            utf("descricao"),
        ]),
        _ => return None,
    };
    Some(Arc::new(schema))
}

pub async fn register_sources(ctx: &SessionContext, staging: &Path) -> Result<()> {
    for kind in [
        "empresas", "estabelecimentos", "socios", "simples",
        "cnaes", "motivos", "naturezas", "paises", "qualificacoes", "municipios",
    ] {
        let dir = staging.join(kind);
        if !dir.exists() {
            continue;
        }
        let schema = raw_schema(kind).expect("known kind");
        let fmt = CsvFormat::default()
            .with_has_header(false)
            .with_delimiter(b';');
        let opts = ListingOptions::new(Arc::new(fmt))
            .with_file_extension(".csv");
        ctx.register_listing_table(kind, dir.to_str().unwrap(), opts, Some(schema), None)
            .await?;
    }
    Ok(())
}

pub async fn register_ibge(ctx: &SessionContext, csv_path: &Path) -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("codigo_tom", DataType::Utf8, true),
        Field::new("nome", DataType::Utf8, true),
        Field::new("uf", DataType::Utf8, true),
        Field::new("codigo_ibge", DataType::Utf8, true),
    ]));
    let file_name = csv_path.file_name().unwrap().to_str().unwrap();
    let tmp = tempfile::TempDir::new()?;
    std::fs::copy(csv_path, tmp.path().join(file_name))?;
    let df = ctx
        .read_csv(
            tmp.path().to_str().unwrap(),
            CsvReadOptions::new()
                .has_header(true)
                .delimiter(b';')
                .schema(schema.as_ref()),
        )
        .await?;
    let batches = df.collect().await?;
    let mem = MemTable::try_new(schema, vec![batches])?;
    ctx.register_table("ibge", Arc::new(mem))?;
    Ok(())
}

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

pub fn organize_by_kind(extracted: &Path, staging: &Path) -> Result<()> {
    for entry in std::fs::read_dir(extracted)? {
        let entry = entry?;
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if let Some(kind) = classify(name) {
            let dest_dir = staging.join(kind);
            std::fs::create_dir_all(&dest_dir)?;
            let dest = dest_dir.join(format!("{}.csv", name));
            std::fs::copy(&p, &dest)?;
        }
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
    fn raw_schemas_have_expected_columns() {
        let s = raw_schema("empresas").unwrap();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names[0], "cnpj_basico");
        assert_eq!(names[1], "razao_social");

        let s = raw_schema("estabelecimentos").unwrap();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names[0], "cnpj_basico");
        assert!(names.contains(&"uf"));
    }

    #[test]
    fn organize_by_kind_moves_files() {
        let td = tempfile::TempDir::new().unwrap();
        let extracted = td.path().join("ext");
        std::fs::create_dir_all(&extracted).unwrap();
        std::fs::write(extracted.join("Empresas0"), "a;b").unwrap();
        std::fs::write(extracted.join("Empresas1"), "c;d").unwrap();
        std::fs::write(extracted.join("Cnaes"), "e;f").unwrap();
        std::fs::write(extracted.join("ignore.txt"), "x").unwrap();

        let staging = td.path().join("staging");
        organize_by_kind(&extracted, &staging).unwrap();

        assert!(staging.join("empresas").join("Empresas0.csv").exists());
        assert!(staging.join("empresas").join("Empresas1.csv").exists());
        assert!(staging.join("cnaes").join("Cnaes.csv").exists());
        assert!(!staging.join("ignore.txt").exists());
    }

    #[tokio::test]
    async fn register_sources_makes_them_queryable() {
        let td = tempfile::TempDir::new().unwrap();
        let staging = td.path().join("staging").join("cnaes");
        std::fs::create_dir_all(&staging).unwrap();
        std::fs::write(staging.join("Cnaes.csv"), "0111-3/01;Cultivo de arroz\n0111-3/02;Cultivo de milho\n").unwrap();

        let ctx = datafusion::prelude::SessionContext::new();
        register_sources(&ctx, td.path().join("staging").as_path()).await.unwrap();
        let df = ctx.sql("SELECT COUNT(*) as n FROM cnaes").await.unwrap();
        let b = df.collect().await.unwrap();
        let n = b[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0);
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn register_ibge_provides_mapping() {
        let td = tempfile::TempDir::new().unwrap();
        let csv = td.path().join("tabmun.csv");
        std::fs::write(&csv, "codigo_tom;nome;uf;codigo_ibge\n7107;SAO PAULO;SP;3550308\n").unwrap();

        let ctx = datafusion::prelude::SessionContext::new();
        register_ibge(&ctx, &csv).await.unwrap();
        let df = ctx.sql("SELECT codigo_ibge FROM ibge WHERE codigo_tom = '7107'").await.unwrap();
        let b = df.collect().await.unwrap();
        let v = b[0].column(0).as_any().downcast_ref::<arrow::array::StringArray>().unwrap().value(0);
        assert_eq!(v, "3550308");
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
