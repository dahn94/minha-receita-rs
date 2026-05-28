use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};

fn codigo_descricao() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("codigo", DataType::Utf8, true),
        Field::new("descricao", DataType::Utf8, true),
    ]))
}

fn endereco() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("tipo_logradouro", DataType::Utf8, true),
        Field::new("logradouro", DataType::Utf8, true),
        Field::new("numero", DataType::Utf8, true),
        Field::new("complemento", DataType::Utf8, true),
        Field::new("bairro", DataType::Utf8, true),
        Field::new("cep", DataType::Utf8, true),
    ]))
}

fn municipio() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("codigo", DataType::Utf8, true),
        Field::new("codigo_ibge", DataType::Utf8, true),
        Field::new("descricao", DataType::Utf8, true),
    ]))
}

fn socio() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("identificador_de_socio", DataType::Int32, true),
        Field::new("nome_socio", DataType::Utf8, true),
        Field::new("cnpj_cpf_do_socio", DataType::Utf8, true),
        Field::new("codigo_qualificacao_socio", DataType::Int32, true),
        Field::new("qualificacao_socio", DataType::Utf8, true),
        Field::new("data_entrada_sociedade", DataType::Date32, true),
        Field::new("codigo_pais", DataType::Utf8, true),
        Field::new("pais", DataType::Utf8, true),
        Field::new("cpf_representante_legal", DataType::Utf8, true),
        Field::new("nome_representante_legal", DataType::Utf8, true),
        Field::new("codigo_qualificacao_representante_legal", DataType::Int32, true),
        Field::new("qualificacao_representante_legal", DataType::Utf8, true),
        Field::new("codigo_faixa_etaria", DataType::Int32, true),
        Field::new("faixa_etaria", DataType::Utf8, true),
    ]))
}

pub fn companies_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("cnpj", DataType::Utf8, false),
        Field::new("cnpj_raiz", DataType::Utf8, false),
        Field::new("razao_social", DataType::Utf8, true),
        Field::new("nome_fantasia", DataType::Utf8, true),
        Field::new("situacao_cadastral", DataType::Utf8, true),
        Field::new("data_situacao_cadastral", DataType::Date32, true),
        Field::new("motivo_situacao_cadastral", codigo_descricao(), true),
        Field::new("data_inicio_atividade", DataType::Date32, true),
        Field::new("cnae_fiscal", codigo_descricao(), true),
        Field::new(
            "cnaes_secundarios",
            DataType::List(Arc::new(Field::new("item", codigo_descricao(), true))),
            true,
        ),
        Field::new("natureza_juridica", codigo_descricao(), true),
        Field::new("qualificacao_responsavel", codigo_descricao(), true),
        Field::new("capital_social", DataType::Float64, true),
        Field::new("porte", codigo_descricao(), true),
        Field::new("ente_federativo_responsavel", DataType::Utf8, true),
        Field::new("uf", DataType::Utf8, false),
        Field::new("municipio", municipio(), true),
        Field::new("pais", codigo_descricao(), true),
        Field::new("endereco", endereco(), true),
        Field::new("email", DataType::Utf8, true),
        Field::new(
            "telefones",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "qsa",
            DataType::List(Arc::new(Field::new("item", socio(), true))),
            true,
        ),
        Field::new("opcao_pelo_simples", DataType::Boolean, true),
        Field::new("data_opcao_pelo_simples", DataType::Date32, true),
        Field::new("data_exclusao_do_simples", DataType::Date32, true),
        Field::new("opcao_pelo_mei", DataType::Boolean, true),
        Field::new("data_opcao_pelo_mei", DataType::Date32, true),
        Field::new("data_exclusao_do_mei", DataType::Date32, true),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn companies_schema_has_expected_top_level_fields() {
        let s = companies_schema();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        for expected in [
            "cnpj", "cnpj_raiz", "razao_social", "nome_fantasia",
            "situacao_cadastral", "data_situacao_cadastral",
            "motivo_situacao_cadastral", "data_inicio_atividade",
            "cnae_fiscal", "cnaes_secundarios", "natureza_juridica",
            "qualificacao_responsavel", "capital_social", "porte",
            "ente_federativo_responsavel", "uf", "municipio", "pais",
            "endereco", "email", "telefones", "qsa",
            "opcao_pelo_simples", "data_opcao_pelo_simples",
            "data_exclusao_do_simples", "opcao_pelo_mei",
            "data_opcao_pelo_mei", "data_exclusao_do_mei",
        ] {
            assert!(names.contains(&expected), "missing field: {expected}");
        }
    }

    #[test]
    fn cnae_fiscal_is_struct_with_codigo_descricao() {
        let s = companies_schema();
        let f = s.field_with_name("cnae_fiscal").unwrap();
        match f.data_type() {
            DataType::Struct(children) => {
                let names: Vec<&str> = children.iter().map(|f| f.name().as_str()).collect();
                assert_eq!(names, vec!["codigo", "descricao"]);
            }
            other => panic!("expected struct, got {other:?}"),
        }
    }

    #[test]
    fn qsa_is_list_of_struct() {
        let s = companies_schema();
        let f = s.field_with_name("qsa").unwrap();
        match f.data_type() {
            DataType::List(inner) => match inner.data_type() {
                DataType::Struct(_) => {}
                other => panic!("expected list of struct, got list of {other:?}"),
            },
            other => panic!("expected list, got {other:?}"),
        }
    }
}
