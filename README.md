# minha-receita-rs

CLI local em Rust pra consultar dados abertos do CNPJ da Receita Federal.
Reescrita parcial (somente uso local, sem servidor HTTP) baseada em
[minha-receita] (Go). Usa Apache DataFusion como engine de queries sobre
Parquet particionado por UF.

[minha-receita]: https://codeberg.org/cuducos/minha-receita

## Uso

Primeira vez (baixa + transforma):

```sh
minha-receita-rs init
```

Atualizar pra um novo período da Receita:

```sh
minha-receita-rs update
```

Consultas:

```sh
minha-receita-rs lookup 12345678000190
minha-receita-rs search --uf=SP --cnae=4711-3/01 --bairro=Centro --limit=10
minha-receita-rs sql "SELECT uf, COUNT(*) FROM companies GROUP BY uf"
```

Formato de saída:

```sh
minha-receita-rs sql "SELECT * FROM companies LIMIT 5" --format csv --output amostra.csv
minha-receita-rs lookup 12345678000190 --format json
```

## Variáveis de ambiente

- `MR_DATA` — diretório raiz do dado local (default: dir padrão de dados do SO).
- `RUST_LOG` — verbosidade (`info`, `debug`, etc.).

## Licença

MIT.
