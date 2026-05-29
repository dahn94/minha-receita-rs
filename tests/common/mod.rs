use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use minha_receita_rs::schema::companies_schema;

/// Escreve um parquet mínimo (1 row por UF) sob `<base>/companies/uf=<UF>/part-0.parquet`.
/// Inclui as colunas (e structs aninhadas) que o `search` projeta, pra refletir
/// o schema real.
pub async fn write_tiny_companies(base: &Path) {
    use datafusion::dataframe::DataFrameWriteOptions;
    use datafusion::prelude::*;

    let ctx = SessionContext::new();

    let cnae_fields = Fields::from(vec![
        Field::new("codigo", DataType::Utf8, true),
        Field::new("descricao", DataType::Utf8, true),
    ]);
    let mun_fields = Fields::from(vec![
        Field::new("codigo", DataType::Utf8, true),
        Field::new("codigo_ibge", DataType::Utf8, true),
        Field::new("descricao", DataType::Utf8, true),
    ]);

    let schema = Arc::new(Schema::new(vec![
        Field::new("cnpj", DataType::Utf8, false),
        Field::new("razao_social", DataType::Utf8, true),
        Field::new("nome_fantasia", DataType::Utf8, true),
        Field::new("situacao_cadastral", DataType::Utf8, true),
        Field::new("cnae_fiscal", DataType::Struct(cnae_fields.clone()), true),
        Field::new("uf", DataType::Utf8, false),
        Field::new("municipio", DataType::Struct(mun_fields.clone()), true),
    ]));

    let cnae_fiscal = StructArray::new(
        cnae_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["6204000", "4711301"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["Consultoria em TI", "Comércio varejista"])) as ArrayRef,
        ],
        None,
    );
    let municipio = StructArray::new(
        mun_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["7107", "6001"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["3550308", "3304557"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["SAO PAULO", "RIO DE JANEIRO"])) as ArrayRef,
        ],
        None,
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["00000000000191", "11111111000111"])) as ArrayRef,
            Arc::new(StringArray::from(vec!["ACME LTDA", "FOO SA"])),
            Arc::new(StringArray::from(vec![Some("ACME"), None])),
            Arc::new(StringArray::from(vec!["ATIVA", "BAIXADA"])),
            Arc::new(cnae_fiscal),
            Arc::new(StringArray::from(vec!["SP", "RJ"])),
            Arc::new(municipio),
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
