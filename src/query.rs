use std::path::{Path, PathBuf};
use std::sync::Arc;

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
            return Err(crate::Error::MissingData(
                companies.display().to_string(),
            ));
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
}
