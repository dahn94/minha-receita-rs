# minha-receita-rs

CLI local em Rust pra consultar os **dados abertos do CNPJ da Receita Federal**.
Baixa os ZIPs publicados pela Receita, converte tudo pra Parquet particionado por
UF, e expõe três modos de consulta: por CNPJ, por filtros estruturados e SQL bruto.

Roda 100% local — não há servidor HTTP, não há banco de dados. A engine é
[Apache DataFusion]. Baseado no projeto [minha-receita] (Go).

[Apache DataFusion]: https://datafusion.apache.org/
[minha-receita]: https://codeberg.org/cuducos/minha-receita

---

## 1. Instalar o Rust

Você só precisa do Rust uma vez. Caso já tenha `cargo` instalado, pule.

### Linux / macOS

```sh
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Aceite a instalação padrão (`1`). Depois reabra o terminal **ou** rode
`source "$HOME/.cargo/env"` pra usar o `cargo` na sessão atual.

No macOS, se o compilador reclamar de `xcrun`, instale as Command Line Tools:

```sh
xcode-select --install
```

No Linux, se faltar algo, instale o necessário pra TLS:

- **Debian/Ubuntu:** `sudo apt-get install -y build-essential pkg-config libssl-dev`
- **Fedora:** `sudo dnf install -y gcc openssl-devel`
- **Arch:** `sudo pacman -S --needed base-devel openssl`

### Windows

1. Baixe `rustup-init.exe` em <https://rustup.rs> e execute.
2. Quando perguntar, escolha `1` (default).
3. O instalador pode pedir pra instalar as **Visual Studio Build Tools**
   (componente "Desktop development with C++"). Aceite — é obrigatório.
4. Feche e reabra o PowerShell.

> Recomendado: use **PowerShell** ou **WSL2 Ubuntu**. Não foi testado com MSYS/MinGW.

Confirme que está tudo certo:

```sh
rustc --version
cargo --version
```

---

## 2. Compilar

Clone e compile em modo release (rápido pra rodar, demora 1–3 min na primeira vez):

```sh
git clone https://github.com/dahn94/minha-receita-rs.git
cd minha-receita-rs
cargo build --release
```

O binário fica em `target/release/minha-receita-rs` (no Windows,
`target\release\minha-receita-rs.exe`).

Se quiser que ele seja chamado de qualquer lugar como `minha-receita-rs`:

```sh
cargo install --path .
```

Isso copia o binário pra `~/.cargo/bin/` (já está no `PATH` se você instalou pelo
rustup). No Windows fica em `%USERPROFILE%\.cargo\bin\`.

> Daqui pra frente os exemplos chamam só `minha-receita-rs`. Se você **não**
> rodou `cargo install`, troque por `./target/release/minha-receita-rs`
> (ou `.\target\release\minha-receita-rs.exe` no Windows).

---

## 3. Primeira vez — baixar e transformar

```sh
minha-receita-rs init
```

O `init` faz três coisas:

1. **Descobre** o período mais recente publicado pela Receita.
2. **Baixa** os ~7 GB de ZIPs + a tabela do IBGE.
3. **Transforma** em Parquet particionado por UF (~20 GB extraídos
   temporariamente, depois ~18 GB no Parquet final).

A base é o **Brasil inteiro** — ~70 milhões de estabelecimentos, todas as UFs
(+ exterior). Reserve uns **45 GB livres** (pico, durante a transformação) e
bastante tempo na primeira vez. Você pode fixar um período específico:

```sh
minha-receita-rs init --period 2026-04
```

> O `--period` escolhe **qual versão mensal** (competência) baixar — cada uma é
> a base nacional **completa**, não um recorte daquele mês. As datas de abertura
> das empresas vão de 1891 até hoje.

Quando publicarem um novo período, atualize:

```sh
minha-receita-rs update
```

### Onde os dados ficam

Por padrão, **dentro do próprio projeto**, em `data/` na raiz do repositório (já
está no `.gitignore`, então nunca vai pro Git):

```
<seu-clone>/data/
├── zips/        # ZIPs da Receita + tabmun.csv (IBGE)
├── companies/   # Parquet particionado por UF (uf=AC/, uf=SP/, …)
└── .period      # competência baixada (ex.: 2026-04)
```

Esse caminho é fixado quando você compila (é o diretório do seu clone), então
**todos os comandos acham os dados sozinhos** — não importa de onde você rode o
binário, e você não precisa passar `--data`. Se mover ou apagar o clone depois
de compilar, é só recompilar (`cargo build --release`).

Pra usar outro caminho (ex.: um disco maior), exporte `MR_DATA` — vale pra todos
os comandos:

```sh
# Linux/macOS
export MR_DATA=/caminho/que/eu/quiser
# Windows PowerShell
$env:MR_DATA = "C:\caminho\que\eu\quiser"
```

Ou, pontualmente: `init --root /caminho` e as consultas com `--data /caminho`.

---

## 4. Consultar

### Lookup por CNPJ

```sh
minha-receita-rs lookup 33683111000280
minha-receita-rs lookup "33.683.111/0002-80"      # pontuação é aceita
minha-receita-rs lookup 33683111000280 --format json
```

### Busca filtrada

Filtros são combinados com `AND`. Resultados são paginados (`--limit`, `--page`).

```sh
minha-receita-rs search --uf SP --limit 10
minha-receita-rs search --uf RJ --cnae 4711-3/01 --bairro Centro
minha-receita-rs search --situacao ATIVA --municipio 7107 --limit 50 --page 2
```

O `search` mostra uma visão concisa pra caber na tela (cnpj, razão social, nome
fantasia, situação, município, UF e CNAE) — sem as colunas aninhadas. Pro
registro completo de uma empresa, use `lookup <cnpj>`.

Flags disponíveis: `--uf`, `--cnae`, `--bairro`, `--municipio`, `--natureza`,
`--situacao`, `--limit` (default 10, máx 100), `--page` (default 1).

Detalhes de formato: `--cnae` aceita os dois jeitos (`4711-3/01` ou `4711301`);
`--bairro` é case-insensitive (`Centro` = `CENTRO`); `--uf`, `--situacao`
(`ATIVA`/`BAIXADA`/…) e os códigos (`--municipio`, `--natureza`) batem exato.

### SQL bruto

Tudo é exposto como uma tabela chamada `companies`. Você pode usar qualquer SQL
suportado pelo DataFusion:

```sh
minha-receita-rs sql "SELECT uf, COUNT(*) FROM companies GROUP BY uf ORDER BY 2 DESC"

minha-receita-rs sql \
  "SELECT cnpj, razao_social FROM companies
   WHERE cnae_fiscal.codigo = '6204000' AND uf = 'DF' LIMIT 5"
```

---

## 5. Formatos de saída

Todos os comandos de consulta aceitam `--format` e `--output`:

```sh
# Padrão: tabela ASCII colorida no terminal
minha-receita-rs lookup 33683111000280

# CSV pra abrir no Excel/Sheets (só com colunas escalares)
minha-receita-rs sql "SELECT cnpj, razao_social, uf FROM companies LIMIT 100" \
  --format csv --output amostra.csv

# JSON Lines — recomendado pra dados completos (mantém structs aninhados)
minha-receita-rs lookup 33683111000280 --format json --output empresa.jsonl
```

> ⚠️ **CSV não suporta colunas struct** (`cnae_fiscal`, `qsa`, `endereco`, ...).
> Se o seu `SELECT` inclui essas colunas, use `--format json` ou
> `--format table`. A limitação vem do arrow csv writer, não do nosso código.

---

## 6. Variáveis de ambiente

| Variável        | Uso |
|---|---|
| `MR_DATA`       | Diretório raiz do dado local (sobrescreve o padrão `<clone>/data`). |
| `RUST_LOG`      | Verbosidade dos logs (`error`, `warn`, `info`, `debug`, `trace`). Padrão `info`. |
| `MR_MEMORY_GB`  | Força um perfil de memória durante `init`/`transform`. Por padrão a RAM total é detectada e usada pra escolher chunks por UF + paralelismo. Use se quiser limitar (ex.: `MR_MEMORY_GB=8` num laptop disputando RAM). |

Exemplos:

```sh
RUST_LOG=debug minha-receita-rs init               # Linux/macOS
$env:RUST_LOG="debug"; minha-receita-rs init       # Windows PowerShell
```

---

## 7. Problemas comuns

**"linker `link.exe` not found" (Windows)** — Instale o "Desktop development
with C++" pelo Visual Studio Installer.

**"openssl-sys: could not find OpenSSL" (Linux)** — Falta `libssl-dev` /
`openssl-devel` (veja seção 1).

**`init` falha no meio do download** — Rode de novo; downloads já completos são
detectados via `HEAD` + `Content-Length` e pulados.

**`init` reclama que `companies/` já existe** — Use `update` em vez de `init`,
ou apague o diretório antes (`rm -rf data/companies`, ou `$MR_DATA/companies`
se você configurou `MR_DATA`).

**Consulta diz que não acha o dado** — As consultas usam `<clone>/data` por
padrão. Se você moveu o clone depois de compilar, recompile (`cargo build
--release`) ou aponte com `--data /caminho` / `MR_DATA`.

---

## Licença

MIT.
