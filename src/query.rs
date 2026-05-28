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
        let mut where_clauses: Vec<String> = Vec::new();
        if let Some(v) = &p.uf {
            where_clauses.push(format!("uf = '{}'", sanitize(v)));
        }
        if let Some(v) = &p.cnae {
            where_clauses.push(format!("cnae_fiscal.codigo = '{}'", sanitize(v)));
        }
        if let Some(v) = &p.bairro {
            where_clauses.push(format!("endereco.bairro = '{}'", sanitize(v)));
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
        let limit = p.limit.max(1).min(100);
        let offset = limit.saturating_mul(p.page.max(1).saturating_sub(1));
        let sql = format!(
            "SELECT * FROM companies{} LIMIT {} OFFSET {}",
            where_sql, limit, offset
        );
        let df = self.ctx.sql(&sql).await?;
        Ok(df.collect().await?)
    }
}
