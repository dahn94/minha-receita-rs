use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::listing::ListingOptions;
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
        "cnaes" | "motivos" | "naturezas" | "paises" | "qualificacoes" => {
            Schema::new(vec![utf("codigo"), utf("descricao")])
        }
        "municipios" => Schema::new(vec![utf("codigo"), utf("descricao")]),
        _ => return None,
    };
    Some(Arc::new(schema))
}

pub async fn register_sources(ctx: &SessionContext, staging: &Path) -> Result<()> {
    for kind in [
        "empresas",
        "estabelecimentos",
        "socios",
        "simples",
        "cnaes",
        "motivos",
        "naturezas",
        "paises",
        "qualificacoes",
        "municipios",
    ] {
        let dir = staging.join(kind);
        if !dir.exists() {
            continue;
        }
        let schema = raw_schema(kind).expect("known kind");
        let fmt = CsvFormat::default()
            .with_has_header(false)
            .with_delimiter(b';');
        let opts = ListingOptions::new(Arc::new(fmt)).with_file_extension(".csv");
        ctx.register_listing_table(kind, dir.to_str().unwrap(), opts, Some(schema), None)
            .await?;
    }
    Ok(())
}

pub async fn register_ibge(ctx: &SessionContext, csv_path: &Path) -> Result<()> {
    let raw_schema = Arc::new(Schema::new(vec![
        Field::new("codigo_tom", DataType::Utf8, true),
        Field::new("cnpj", DataType::Utf8, true),
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
                .has_header(false)
                .delimiter(b';')
                .schema(raw_schema.as_ref()),
        )
        .await?;
    let projected = df.select(vec![
        col("codigo_tom"),
        trim(vec![col("nome")]).alias("nome"),
        trim(vec![col("uf")]).alias("uf"),
        trim(vec![col("codigo_ibge")]).alias("codigo_ibge"),
    ])?;
    let final_schema = Arc::new(Schema::new(vec![
        Field::new("codigo_tom", DataType::Utf8, true),
        Field::new("nome", DataType::Utf8, true),
        Field::new("uf", DataType::Utf8, true),
        Field::new("codigo_ibge", DataType::Utf8, true),
    ]));
    let batches = projected.collect().await?;
    let mem = MemTable::try_new(final_schema, vec![batches])?;
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
        let dest = out_dir.join(&name);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut f = File::create(&dest)?;
        // Receita outer zips wrap binary inner zips; only CP1252-decode text payloads.
        let is_binary = name.to_lowercase().ends_with(".zip");
        if is_binary {
            f.write_all(&bytes)?;
        } else {
            let (decoded, _, had_errors) = encoding_rs::WINDOWS_1252.decode(&bytes);
            if had_errors {
                tracing::warn!(file=%name, "cp1252 decoding produced replacement chars");
            }
            f.write_all(decoded.as_bytes())?;
        }
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

/// Single SQL that joins all raw tables and produces one row per estabelecimento
/// with `qsa` and `cnaes_secundarios` as arrays of structs.
///
/// Deviations from the original plan (DataFusion 44 compatibility):
/// - Date parsing uses `to_date(s, '%Y%m%d')` (chrono strftime), wrapped in a
///   CASE that returns NULL for empty / "00000000" inputs. DF 44 has no
///   `try_to_date` and a literal `'yyyyMMdd'` is rejected by `to_date`.
/// - The `flatten()` around `string_to_array(...)` was dropped — the function
///   already returns a 1-D list.
/// - The `cnaes_secundarios` CTE no longer uses correlated
///   `FROM r, UNNEST(r.codigos) u(code)` (DF 44 plans that as an outer
///   reference and bails out at physical planning). Instead `unnest(...)`
///   is used as a scalar projection inside a single CTE, which DataFusion
///   rewrites into a logical Unnest node it can execute.
/// - Boolean coercions (`= 'S'`) became full CASE expressions to keep NULLs
///   distinct from `false`.
/// - `array_remove(make_array(...), NULL)` replaces the original
///   `array[..]` literal syntax (DF 44 prefers `make_array`).
pub const CONSOLIDATION_SQL: &str = r#"
WITH
socios_agg AS (
    SELECT
        s.cnpj_basico,
        array_agg(named_struct(
            'identificador_de_socio', try_cast(s.identificador_de_socio AS INT),
            'nome_socio', s.nome_socio,
            'cnpj_cpf_do_socio', s.cnpj_cpf_do_socio,
            'codigo_qualificacao_socio', try_cast(s.codigo_qualificacao_socio AS INT),
            'qualificacao_socio', qs.descricao,
            'data_entrada_sociedade', CASE
                WHEN s.data_entrada_sociedade_raw IS NULL
                  OR length(s.data_entrada_sociedade_raw) = 0
                  OR s.data_entrada_sociedade_raw = '00000000'
                THEN NULL
                ELSE to_date(s.data_entrada_sociedade_raw, '%Y%m%d')
            END,
            'codigo_pais', s.codigo_pais,
            'pais', ps.descricao,
            'cpf_representante_legal', s.cpf_representante_legal,
            'nome_representante_legal', s.nome_representante_legal,
            'codigo_qualificacao_representante_legal', try_cast(s.codigo_qualificacao_representante_legal AS INT),
            'qualificacao_representante_legal', qrl.descricao,
            'codigo_faixa_etaria', try_cast(s.codigo_faixa_etaria AS INT)
        )) AS qsa
    FROM socios s
    LEFT JOIN qualificacoes qs ON qs.codigo = s.codigo_qualificacao_socio
    LEFT JOIN qualificacoes qrl ON qrl.codigo = s.codigo_qualificacao_representante_legal
    LEFT JOIN paises ps ON ps.codigo = s.codigo_pais
    GROUP BY s.cnpj_basico
),
cnaes_sec_exploded AS (
    SELECT
        concat(est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv) AS cnpj,
        unnest(string_to_array(est.cnae_fiscal_secundaria, ',')) AS code
    FROM estabelecimentos est
    WHERE est.cnae_fiscal_secundaria IS NOT NULL
      AND length(est.cnae_fiscal_secundaria) > 0
),
cnaes_sec_resolved AS (
    SELECT
        e.cnpj,
        array_agg(named_struct('codigo', c.codigo, 'descricao', c.descricao)) AS cnaes_secundarios
    FROM cnaes_sec_exploded e
    LEFT JOIN cnaes c ON c.codigo = e.code
    GROUP BY e.cnpj
)
SELECT
    concat(est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv) AS cnpj,
    est.cnpj_basico AS cnpj_raiz,
    emp.razao_social,
    est.nome_fantasia,
    CASE est.situacao_cadastral_codigo
        WHEN '01' THEN 'NULA'
        WHEN '02' THEN 'ATIVA'
        WHEN '03' THEN 'SUSPENSA'
        WHEN '04' THEN 'INAPTA'
        WHEN '08' THEN 'BAIXADA'
        ELSE est.situacao_cadastral_codigo
    END AS situacao_cadastral,
    CASE
        WHEN est.data_situacao_cadastral_raw IS NULL
          OR length(est.data_situacao_cadastral_raw) = 0
          OR est.data_situacao_cadastral_raw = '00000000'
        THEN NULL
        ELSE to_date(est.data_situacao_cadastral_raw, '%Y%m%d')
    END AS data_situacao_cadastral,
    named_struct('codigo', est.motivo_situacao_cadastral, 'descricao', mot.descricao) AS motivo_situacao_cadastral,
    CASE
        WHEN est.data_inicio_atividade_raw IS NULL
          OR length(est.data_inicio_atividade_raw) = 0
          OR est.data_inicio_atividade_raw = '00000000'
        THEN NULL
        ELSE to_date(est.data_inicio_atividade_raw, '%Y%m%d')
    END AS data_inicio_atividade,
    named_struct('codigo', est.cnae_fiscal_principal, 'descricao', cf.descricao) AS cnae_fiscal,
    csr.cnaes_secundarios,
    named_struct('codigo', emp.natureza_juridica, 'descricao', nat.descricao) AS natureza_juridica,
    named_struct('codigo', emp.qualificacao_responsavel, 'descricao', qr.descricao) AS qualificacao_responsavel,
    try_cast(replace(emp.capital_social_raw, ',', '.') AS DOUBLE) AS capital_social,
    named_struct(
        'codigo', emp.porte,
        'descricao', CASE emp.porte
            WHEN '01' THEN 'NAO INFORMADO'
            WHEN '03' THEN 'MICRO EMPRESA'
            WHEN '05' THEN 'EMPRESA DE PEQUENO PORTE'
            ELSE 'DEMAIS'
        END
    ) AS porte,
    emp.ente_federativo_responsavel,
    est.uf,
    named_struct(
        'codigo', est.codigo_municipio,
        'codigo_ibge', ibge.codigo_ibge,
        'descricao', mun.descricao
    ) AS municipio,
    named_struct('codigo', est.codigo_pais, 'descricao', pae.descricao) AS pais,
    named_struct(
        'tipo_logradouro', est.tipo_logradouro,
        'logradouro', est.logradouro,
        'numero', est.numero,
        'complemento', est.complemento,
        'bairro', est.bairro,
        'cep', est.cep
    ) AS endereco,
    est.email,
    array_remove(
        make_array(
            CASE WHEN est.telefone1 IS NOT NULL AND length(est.telefone1) > 0
                 THEN concat(coalesce(est.ddd1, ''), est.telefone1) END,
            CASE WHEN est.telefone2 IS NOT NULL AND length(est.telefone2) > 0
                 THEN concat(coalesce(est.ddd2, ''), est.telefone2) END
        ), NULL
    ) AS telefones,
    sa.qsa,
    CASE WHEN sim.opcao_pelo_simples_raw = 'S' THEN true
         WHEN sim.opcao_pelo_simples_raw = 'N' THEN false
         ELSE NULL END AS opcao_pelo_simples,
    CASE
        WHEN sim.data_opcao_pelo_simples_raw IS NULL
          OR length(sim.data_opcao_pelo_simples_raw) = 0
          OR sim.data_opcao_pelo_simples_raw = '00000000'
        THEN NULL
        ELSE to_date(sim.data_opcao_pelo_simples_raw, '%Y%m%d')
    END AS data_opcao_pelo_simples,
    CASE
        WHEN sim.data_exclusao_do_simples_raw IS NULL
          OR length(sim.data_exclusao_do_simples_raw) = 0
          OR sim.data_exclusao_do_simples_raw = '00000000'
        THEN NULL
        ELSE to_date(sim.data_exclusao_do_simples_raw, '%Y%m%d')
    END AS data_exclusao_do_simples,
    CASE WHEN sim.opcao_pelo_mei_raw = 'S' THEN true
         WHEN sim.opcao_pelo_mei_raw = 'N' THEN false
         ELSE NULL END AS opcao_pelo_mei,
    CASE
        WHEN sim.data_opcao_pelo_mei_raw IS NULL
          OR length(sim.data_opcao_pelo_mei_raw) = 0
          OR sim.data_opcao_pelo_mei_raw = '00000000'
        THEN NULL
        ELSE to_date(sim.data_opcao_pelo_mei_raw, '%Y%m%d')
    END AS data_opcao_pelo_mei,
    CASE
        WHEN sim.data_exclusao_do_mei_raw IS NULL
          OR length(sim.data_exclusao_do_mei_raw) = 0
          OR sim.data_exclusao_do_mei_raw = '00000000'
        THEN NULL
        ELSE to_date(sim.data_exclusao_do_mei_raw, '%Y%m%d')
    END AS data_exclusao_do_mei
FROM estabelecimentos est
LEFT JOIN empresas emp ON emp.cnpj_basico = est.cnpj_basico
LEFT JOIN cnaes cf ON cf.codigo = est.cnae_fiscal_principal
LEFT JOIN naturezas nat ON nat.codigo = emp.natureza_juridica
LEFT JOIN qualificacoes qr ON qr.codigo = emp.qualificacao_responsavel
LEFT JOIN motivos mot ON mot.codigo = est.motivo_situacao_cadastral
LEFT JOIN municipios mun ON mun.codigo = est.codigo_municipio
LEFT JOIN paises pae ON pae.codigo = est.codigo_pais
LEFT JOIN ibge ON ibge.codigo_tom = est.codigo_municipio
LEFT JOIN socios_agg sa ON sa.cnpj_basico = est.cnpj_basico
LEFT JOIN cnaes_sec_resolved csr ON csr.cnpj = concat(est.cnpj_basico, est.cnpj_ordem, est.cnpj_dv)
LEFT JOIN simples sim ON sim.cnpj_basico = est.cnpj_basico
"#;

pub async fn consolidate(ctx: &SessionContext) -> Result<datafusion::dataframe::DataFrame> {
    Ok(ctx.sql(CONSOLIDATION_SQL).await?)
}

/// Write the consolidated DataFrame as Parquet, partitioned by `uf`, under
/// `<out_dir>/companies/uf=<UF>/...`.
///
/// Deviations from the original spec snippet:
/// - DataFusion 44's `DataFrame::write_parquet` takes
///   `Option<TableParquetOptions>` (a config-driven struct), not
///   `Option<WriterProperties>`. We configure compression, statistics, and a
///   bloom filter on `cnpj` via that struct instead.
/// - The spec snippet sorted by `cnae_fiscal` and `endereco`, but both of
///   those are struct columns in the consolidated output. DF 44 does not
///   support sorting on struct columns at planning, so we drop `with_sort_by`
///   entirely. The partition-by alone satisfies the column-store layout
///   requirements; sorting was only a downstream-query optimization.
fn build_parquet_opts() -> datafusion::common::config::TableParquetOptions {
    use datafusion::common::config::{ParquetColumnOptions, TableParquetOptions};
    let mut props = TableParquetOptions::new();
    props.global.compression = Some("zstd(3)".to_string());
    props.global.statistics_enabled = Some("page".to_string());
    props.global.bloom_filter_on_write = true;
    let cnpj_opts = ParquetColumnOptions {
        bloom_filter_enabled: Some(true),
        ..Default::default()
    };
    props
        .column_specific_options
        .insert("cnpj".to_string(), cnpj_opts);
    props
}

pub async fn write_partitioned(df: datafusion::dataframe::DataFrame, out_dir: &Path) -> Result<()> {
    use datafusion::dataframe::DataFrameWriteOptions;
    use datafusion::prelude::*;

    let target = out_dir.join("companies");
    std::fs::create_dir_all(&target)?;

    // Two-pass write to avoid Arrow's i32 offset overflow when DataFusion's
    // partition-by writer accumulates large UFs (e.g. SP) into a single
    // per-partition builder. Pass 1 streams the consolidation result to a
    // single unpartitioned parquet (each batch ~8K rows, no cross-batch
    // accumulation). Pass 2 reads it back and writes one UF at a time, so
    // each file stream's offsets stay bounded by that UF's bytes.
    let staging_tmp = tempfile::TempDir::new()?;
    let staging = staging_tmp.path().join("staging");
    std::fs::create_dir_all(&staging)?;

    df.write_parquet(
        staging.to_str().unwrap(),
        DataFrameWriteOptions::new(),
        Some(build_parquet_opts()),
    )
    .await?;

    let ctx = SessionContext::new();
    ctx.register_parquet(
        "staging",
        staging.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;

    use arrow::array::Array;
    let uf_batches = ctx
        .sql(
            "SELECT DISTINCT CAST(uf AS VARCHAR) AS uf \
             FROM staging WHERE uf IS NOT NULL ORDER BY uf",
        )
        .await?
        .collect()
        .await?;
    let mut ufs: Vec<String> = Vec::new();
    for b in &uf_batches {
        let raw = b.column(0).clone();
        let col = arrow::compute::cast(&raw, &arrow::datatypes::DataType::Utf8)?;
        let col = col
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("cast to Utf8 yields StringArray");
        for i in 0..col.len() {
            if !col.is_null(i) {
                ufs.push(col.value(i).to_string());
            }
        }
    }

    for uf in &ufs {
        let escaped = uf.replace('\'', "''");
        let part_df = ctx
            .sql(&format!(
                "SELECT * EXCEPT(uf) FROM staging WHERE uf = '{}'",
                escaped
            ))
            .await?;
        let uf_dir = target.join(format!("uf={}", uf));
        std::fs::create_dir_all(&uf_dir)?;
        part_df
            .write_parquet(
                uf_dir.to_str().unwrap(),
                DataFrameWriteOptions::new(),
                Some(build_parquet_opts()),
            )
            .await?;
    }
    Ok(())
}

pub async fn run(zip_dir: &Path, ibge_csv: &Path, out_dir: &Path) -> Result<()> {
    let tmp = tempfile::TempDir::new()?;
    let ext = tmp.path().join("ext");
    let staging = tmp.path().join("staging");

    let spinner = indicatif::ProgressBar::new_spinner();
    spinner.set_style(
        indicatif::ProgressStyle::with_template("{spinner:.cyan} [{elapsed_precise}] {msg}")
            .unwrap(),
    );
    spinner.enable_steady_tick(std::time::Duration::from_millis(100));

    spinner.set_message(format!("Extraindo ZIPs de {}", zip_dir.display()));
    extract_all(zip_dir, &ext)?;
    spinner.set_message("Organizando arquivos por tipo");
    organize_by_kind(&ext, &staging)?;

    // Crank target_partitions way up: the consolidation does GROUP BY cnpj_basico
    // (`socios_agg` CTE) and produces a List<Struct> of qsa per group. With the
    // default ~num_cpus partitions, each hash bucket accumulates ~50M/N groups
    // of List<Struct> data, and Arrow's i32 list/string offsets overflow at
    // ~2 GB cumulative. 64 partitions keep each bucket well under 2 GB.
    let config = SessionConfig::new()
        .with_target_partitions(64)
        .with_batch_size(4096);
    let ctx = SessionContext::new_with_config(config);
    spinner.set_message("Registrando tabelas-fonte");
    register_sources(&ctx, &staging).await?;
    register_ibge(&ctx, ibge_csv).await?;

    spinner.set_message("Consolidando (planejando query)");
    let df = consolidate(&ctx).await?;
    spinner.set_message(format!("Escrevendo Parquet em {}", out_dir.display()));
    write_partitioned(df, out_dir).await?;
    spinner.finish_with_message("Transformação concluída");
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
        assert_eq!(
            classify("K3241.K03200Y0.D10110.ESTABELE"),
            Some("estabelecimentos")
        );
        assert_eq!(classify("K3241.K03200Y0.D10110.SOCIOCSV"), Some("socios"));
        assert_eq!(classify("F.K03200$Z.D10110.CNAECSV"), Some("cnaes"));
        assert_eq!(classify("F.K03200$Z.D10110.MUNICCSV"), Some("municipios"));
        assert_eq!(classify("F.K03200$Z.D10110.NATJUCSV"), Some("naturezas"));
        assert_eq!(classify("F.K03200$Z.D10110.PAISCSV"), Some("paises"));
        assert_eq!(classify("F.K03200$Z.D10110.MOTICSV"), Some("motivos"));
        assert_eq!(
            classify("F.K03200$Z.D10110.QUALSCSV"),
            Some("qualificacoes")
        );
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
        std::fs::write(
            staging.join("Cnaes.csv"),
            "0111-3/01;Cultivo de arroz\n0111-3/02;Cultivo de milho\n",
        )
        .unwrap();

        let ctx = datafusion::prelude::SessionContext::new();
        register_sources(&ctx, td.path().join("staging").as_path())
            .await
            .unwrap();
        let df = ctx.sql("SELECT COUNT(*) as n FROM cnaes").await.unwrap();
        let b = df.collect().await.unwrap();
        let n = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn register_ibge_provides_mapping() {
        let td = tempfile::TempDir::new().unwrap();
        let csv = td.path().join("tabmun.csv");
        // Real wire format: 5 columns, no header, with padded names.
        std::fs::write(
            &csv,
            "7107;26994533000120;SAO PAULO                                     ;SP;3550308\n",
        )
        .unwrap();

        let ctx = datafusion::prelude::SessionContext::new();
        register_ibge(&ctx, &csv).await.unwrap();
        let df = ctx
            .sql("SELECT codigo_ibge, nome FROM ibge WHERE codigo_tom = '7107'")
            .await
            .unwrap();
        let b = df.collect().await.unwrap();
        let ibge = b[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0);
        let nome = b[0]
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap()
            .value(0);
        assert_eq!(ibge, "3550308");
        assert_eq!(nome, "SAO PAULO");
    }

    #[tokio::test]
    async fn consolidates_testdata() {
        let td = tempfile::TempDir::new().unwrap();
        let zip = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join("2026-01.zip");
        let ext_outer = td.path().join("outer");
        extract_zip_to_dir(&zip, &ext_outer).unwrap();
        let inner_dir = if ext_outer.join("2026-01").exists() {
            ext_outer.join("2026-01")
        } else {
            ext_outer.clone()
        };
        let ext_inner = td.path().join("inner");
        for entry in std::fs::read_dir(&inner_dir).unwrap() {
            let p = entry.unwrap().path();
            if p.extension().and_then(|s| s.to_str()) == Some("zip") {
                extract_zip_to_dir(&p, &ext_inner).unwrap();
            }
        }
        let staging = td.path().join("staging");
        organize_by_kind(&ext_inner, &staging).unwrap();

        let ctx = datafusion::prelude::SessionContext::new();
        register_sources(&ctx, &staging).await.unwrap();
        let ibge_csv = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("testdata")
            .join("tabmun.csv");
        register_ibge(&ctx, &ibge_csv).await.unwrap();

        let df = consolidate(&ctx).await.unwrap();
        let batches = df.collect().await.unwrap();
        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert!(total > 0, "consolidation produced zero rows");

        // Verify expected columns exist.
        let schema = batches[0].schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        for expected in ["cnpj", "razao_social", "uf", "cnae_fiscal", "qsa"] {
            assert!(names.contains(&expected), "missing column: {expected}");
        }
    }

    #[tokio::test]
    async fn write_partitioned_creates_uf_dirs() {
        let ctx = datafusion::prelude::SessionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("cnpj", DataType::Utf8, false),
            Field::new("uf", DataType::Utf8, false),
        ]));
        let batch = arrow::array::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow::array::StringArray::from(vec!["a", "b", "c"])),
                Arc::new(arrow::array::StringArray::from(vec!["SP", "SP", "RJ"])),
            ],
        )
        .unwrap();
        let df = ctx.read_batch(batch).unwrap();

        let td = tempfile::TempDir::new().unwrap();
        write_partitioned(df, td.path()).await.unwrap();
        assert!(td.path().join("companies").join("uf=SP").exists());
        assert!(td.path().join("companies").join("uf=RJ").exists());
    }

    #[tokio::test]
    async fn run_end_to_end_on_testdata() {
        let td = tempfile::TempDir::new().unwrap();
        let testdata = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");

        // Pre-step: extract `2026-01.zip` to simulate a download — produces inner zips.
        let in_dir = td.path().join("zips");
        std::fs::create_dir_all(&in_dir).unwrap();
        let outer = td.path().join("outer");
        extract_zip_to_dir(&testdata.join("2026-01.zip"), &outer).unwrap();
        let inner_src = if outer.join("2026-01").exists() {
            outer.join("2026-01")
        } else {
            outer.clone()
        };
        for entry in std::fs::read_dir(&inner_src).unwrap() {
            let p = entry.unwrap().path();
            if p.extension().and_then(|s| s.to_str()) == Some("zip") {
                std::fs::copy(&p, in_dir.join(p.file_name().unwrap())).unwrap();
            }
        }

        let out = td.path().join("parquet");
        run(&in_dir, &testdata.join("tabmun.csv"), &out)
            .await
            .unwrap();

        assert!(out.join("companies").exists());
        let count = walkdir(&out.join("companies"))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("parquet"))
            .count();
        assert!(count > 0, "no parquet files written");
    }

    fn walkdir(p: &Path) -> impl Iterator<Item = std::path::PathBuf> + '_ {
        let mut stack = vec![p.to_path_buf()];
        std::iter::from_fn(move || {
            while let Some(d) = stack.pop() {
                let rd = std::fs::read_dir(&d).ok()?;
                for e in rd.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        stack.push(p);
                    } else {
                        return Some(p);
                    }
                }
            }
            None
        })
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
