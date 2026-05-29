use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::RecordBatch;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::prelude::*;

use crate::Result;

pub struct DataContext {
    pub ctx: SessionContext,
    pub root: PathBuf,
}

impl DataContext {
    pub async fn open(data_dir: impl AsRef<Path>) -> Result<Self> {
        let root = data_dir.as_ref().to_path_buf();
        let companies = root.join("companies");
        if !companies.exists() {
            return Err(crate::Error::MissingData(companies.display().to_string()));
        }
        let ctx = SessionContext::new();
        ctx.register_listing_table(
            "companies",
            companies.to_str().unwrap(),
            ListingOptions::new(Arc::new(ParquetFormat::default()))
                .with_file_extension(".parquet")
                .with_table_partition_cols(vec![(
                    "uf".to_string(),
                    arrow::datatypes::DataType::Utf8,
                )]),
            None,
            None,
        )
        .await?;
        Ok(Self { ctx, root })
    }

    pub async fn row_count(&self) -> Result<usize> {
        let df = self.ctx.sql("SELECT COUNT(*) AS n FROM companies").await?;
        let batches = df.collect().await?;
        let n = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap()
            .value(0);
        Ok(n as usize)
    }

    pub async fn lookup(&self, cnpj: &str) -> Result<Vec<RecordBatch>> {
        let normalized = normalize_cnpj(cnpj)?;
        let sql = format!(
            "SELECT * FROM companies WHERE cnpj = '{}'",
            sanitize(&normalized)
        );
        let df = self.ctx.sql(&sql).await?;
        Ok(df.collect().await?)
    }
}

pub fn normalize_cnpj(s: &str) -> Result<String> {
    let cleaned: String = s.chars().filter(|c| c.is_ascii_digit()).collect();
    if cleaned.len() != 14 {
        return Err(crate::Error::InvalidCnpj(s.to_string()));
    }
    Ok(cleaned)
}

fn sanitize(s: &str) -> String {
    s.replace('\'', "''")
}

/// CNAE codes are stored as 7 bare digits (e.g. `4711301`). Accept the
/// human-friendly punctuated form (`4711-3/01`) too by keeping only digits.
fn normalize_cnae(s: &str) -> String {
    s.chars().filter(|c| c.is_ascii_digit()).collect()
}

/// Build the `SELECT … WHERE … LIMIT … OFFSET …` for a search. Pure (no I/O)
/// so the clause shaping can be unit-tested without a dataset.
fn build_search_sql(p: &SearchParams) -> String {
    let mut where_clauses: Vec<String> = Vec::new();
    if let Some(v) = &p.uf {
        where_clauses.push(format!("uf = '{}'", sanitize(v)));
    }
    if let Some(v) = &p.cnae {
        where_clauses.push(format!("cnae_fiscal.codigo = '{}'", normalize_cnae(v)));
    }
    if let Some(v) = &p.bairro {
        // Receita stores bairro upper-cased; match case-insensitively.
        where_clauses.push(format!("upper(endereco.bairro) = upper('{}')", sanitize(v)));
    }
    if let Some(v) = &p.municipio {
        where_clauses.push(format!("municipio.codigo = '{}'", sanitize(v)));
    }
    if let Some(v) = &p.natureza {
        where_clauses.push(format!("natureza_juridica.codigo = '{}'", sanitize(v)));
    }
    if let Some(v) = &p.situacao {
        where_clauses.push(format!("situacao_cadastral = '{}'", sanitize(v)));
    }
    let where_sql = if where_clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", where_clauses.join(" AND "))
    };
    let limit = p.limit.clamp(1, 100);
    let offset = limit.saturating_mul(p.page.max(1).saturating_sub(1));
    format!("SELECT * FROM companies{where_sql} LIMIT {limit} OFFSET {offset}")
}

#[derive(Debug, Default, Clone)]
pub struct SearchParams {
    pub uf: Option<String>,
    pub cnae: Option<String>,
    pub bairro: Option<String>,
    pub municipio: Option<String>,
    pub natureza: Option<String>,
    pub situacao: Option<String>,
    pub limit: usize,
    pub page: usize,
}

impl DataContext {
    pub async fn search(&self, p: &SearchParams) -> Result<Vec<RecordBatch>> {
        let sql = build_search_sql(p);
        let df = self.ctx.sql(&sql).await?;
        Ok(df.collect().await?)
    }
}

impl DataContext {
    pub async fn sql(&self, query: &str) -> Result<Vec<RecordBatch>> {
        let df = self.ctx.sql(query).await?;
        Ok(df.collect().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cnae_accepts_punctuated_form() {
        let p = SearchParams {
            cnae: Some("4711-3/01".into()),
            limit: 10,
            page: 1,
            ..Default::default()
        };
        assert!(build_search_sql(&p).contains("cnae_fiscal.codigo = '4711301'"));
    }

    #[test]
    fn bairro_matches_case_insensitively() {
        let p = SearchParams {
            bairro: Some("Centro".into()),
            limit: 10,
            page: 1,
            ..Default::default()
        };
        assert!(build_search_sql(&p).contains("upper(endereco.bairro) = upper('Centro')"));
    }

    #[test]
    fn pagination_translates_to_offset() {
        let p = SearchParams {
            limit: 25,
            page: 3,
            ..Default::default()
        };
        assert!(build_search_sql(&p).contains("LIMIT 25 OFFSET 50"));
    }

    #[test]
    fn single_quotes_are_escaped() {
        let p = SearchParams {
            bairro: Some("D'OESTE".into()),
            limit: 10,
            page: 1,
            ..Default::default()
        };
        assert!(build_search_sql(&p).contains("upper('D''OESTE')"));
    }
}
