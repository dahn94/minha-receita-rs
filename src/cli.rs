use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::output::Format;

#[derive(Parser)]
#[command(
    name = "minha-receita-rs",
    version,
    about = "CNPJ Receita Federal — local CLI sobre DataFusion"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Primeira vez: baixa + transforma em um único comando.
    Init {
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long)]
        period: Option<String>,
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
    },
    /// Atualiza para o período mais recente, se houver.
    Update {
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
    },
    /// Baixa os ZIPs da Receita + tabela IBGE.
    Download {
        #[arg(long)]
        out: PathBuf,
        #[arg(long)]
        period: Option<String>,
        #[arg(long, default_value_t = 4)]
        concurrency: usize,
    },
    /// Transforma os CSVs descomprimidos em Parquet particionado.
    Transform {
        #[arg(long)]
        r#in: PathBuf,
        #[arg(long)]
        out: PathBuf,
    },
    /// Consulta um CNPJ.
    Lookup {
        cnpj: String,
        #[command(flatten)]
        query: QueryFlags,
    },
    /// Busca filtrada.
    Search {
        #[arg(long)]
        uf: Option<String>,
        #[arg(long)]
        cnae: Option<String>,
        #[arg(long)]
        bairro: Option<String>,
        #[arg(long)]
        municipio: Option<String>,
        #[arg(long)]
        natureza: Option<String>,
        #[arg(long)]
        situacao: Option<String>,
        #[arg(long, default_value_t = 10)]
        limit: usize,
        #[arg(long, default_value_t = 1)]
        page: usize,
        #[command(flatten)]
        query: QueryFlags,
    },
    /// Executa SQL bruto.
    Sql {
        query: String,
        #[command(flatten)]
        flags: QueryFlags,
    },
}

#[derive(clap::Args)]
pub struct QueryFlags {
    #[arg(long, env = "MR_DATA")]
    pub data: Option<PathBuf>,
    #[arg(long, value_enum, default_value_t = Format::Table)]
    pub format: Format,
    #[arg(long)]
    pub output: Option<PathBuf>,
}

pub async fn run(cli: Cli) -> anyhow::Result<()> {
    use crate::output;
    use crate::query::{DataContext, SearchParams};
    use std::io;

    match cli.command {
        Command::Init {
            root,
            period,
            concurrency,
        } => {
            let root = root.unwrap_or_else(crate::lifecycle::default_root);
            crate::lifecycle::init(&root, period, concurrency).await?;
            eprintln!("Init concluído em {}", root.display());
        }
        Command::Update { root, concurrency } => {
            let root = root.unwrap_or_else(crate::lifecycle::default_root);
            match crate::lifecycle::update(&root, concurrency).await? {
                crate::lifecycle::UpdateOutcome::UpToDate(p) => {
                    eprintln!("Já está na versão mais recente ({p})")
                }
                crate::lifecycle::UpdateOutcome::Updated { from, to } => {
                    eprintln!("Atualizado de {from} para {to}")
                }
            }
        }
        Command::Download {
            out,
            period,
            concurrency,
        } => {
            let client = reqwest::Client::builder()
                .user_agent("minha-receita-rs/0.1")
                .build()?;
            let period = period
                .map(|s| s.parse::<crate::schema::Period>())
                .transpose()?;
            let p = crate::download::discover_and_download(
                &client,
                crate::download::RECEITA_BASE_URL,
                period,
                &out,
                concurrency,
            )
            .await?;
            let url = crate::download::fetch_ibge_url(&client).await?;
            crate::download::download_file(&client, &url, &out.join("tabmun.csv")).await?;
            eprintln!("Baixado período {p} em {}", out.display());
        }
        Command::Transform { r#in, out } => {
            let ibge = r#in.join("tabmun.csv");
            if !ibge.exists() {
                anyhow::bail!("tabmun.csv não encontrado em {}", r#in.display());
            }
            crate::transform::run(&r#in, &ibge, &out).await?;
            eprintln!("Transformação concluída em {}", out.display());
        }
        Command::Lookup { cnpj, query } => {
            let data = query
                .data
                .ok_or_else(|| anyhow::anyhow!("--data ou MR_DATA não definido"))?;
            let ctx = DataContext::open(&data).await?;
            let batches = ctx.lookup(&cnpj).await?;
            let mut out: Box<dyn io::Write> = match query.output {
                Some(p) => Box::new(std::fs::File::create(p)?),
                None => Box::new(io::stdout()),
            };
            output::write(query.format, &batches, &mut *out)?;
        }
        Command::Search {
            uf,
            cnae,
            bairro,
            municipio,
            natureza,
            situacao,
            limit,
            page,
            query,
        } => {
            let data = query
                .data
                .ok_or_else(|| anyhow::anyhow!("--data ou MR_DATA não definido"))?;
            let ctx = DataContext::open(&data).await?;
            let params = SearchParams {
                uf,
                cnae,
                bairro,
                municipio,
                natureza,
                situacao,
                limit,
                page,
            };
            let batches = ctx.search(&params).await?;
            let mut out: Box<dyn io::Write> = match query.output {
                Some(p) => Box::new(std::fs::File::create(p)?),
                None => Box::new(io::stdout()),
            };
            output::write(query.format, &batches, &mut *out)?;
        }
        Command::Sql { query, flags } => {
            let data = flags
                .data
                .ok_or_else(|| anyhow::anyhow!("--data ou MR_DATA não definido"))?;
            let ctx = DataContext::open(&data).await?;
            let batches = ctx.sql(&query).await?;
            let mut out: Box<dyn io::Write> = match flags.output {
                Some(p) => Box::new(std::fs::File::create(p)?),
                None => Box::new(io::stdout()),
            };
            output::write(flags.format, &batches, &mut *out)?;
        }
    }
    Ok(())
}
