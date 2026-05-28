use std::io::{self, Write};
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::output::Format;

#[derive(Parser)]
#[command(name = "minha-receita-rs", version, about = "CNPJ Receita Federal — local CLI sobre DataFusion")]
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

pub async fn run(_cli: Cli) -> anyhow::Result<()> {
    // Implementado em tasks 7–25.
    eprintln!("not yet implemented");
    std::process::exit(2);
}

fn _open_output(path: Option<&PathBuf>) -> anyhow::Result<Box<dyn Write>> {
    Ok(match path {
        Some(p) => Box::new(std::fs::File::create(p)?),
        None => Box::new(io::stdout()),
    })
}
