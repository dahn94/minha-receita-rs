use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use minha_receita_rs::schema::companies_schema;

/// Escreve um parquet mínimo (1 row por UF) sob `<base>/companies/uf=<UF>/part-0.parquet`.
pub async fn write_tiny_companies(base: &Path) {
    use datafusion::dataframe::DataFrameWriteOptions;
    use datafusion::prelude::*;

    let ctx = SessionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("cnpj", DataType::Utf8, false),
        Field::new("uf", DataType::Utf8, false),
        Field::new("razao_social", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["00000000000191", "11111111000111"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["SP", "RJ"])),
            Arc::new(StringArray::from(vec!["ACME LTDA", "FOO SA"])),
        ],
    )
    .unwrap();
    let df = ctx.read_batch(batch).unwrap();
    let target = base.join("companies");
    std::fs::create_dir_all(&target).unwrap();
    df.write_parquet(
        target.to_str().unwrap(),
        DataFrameWriteOptions::new().with_partition_by(vec!["uf".to_string()]),
        None,
    )
    .await
    .unwrap();
    let _ = companies_schema();
}
