use clap::Parser;

#[derive(Parser)]
#[command(name = "minha-receita-rs", version)]
pub struct Cli {}

pub async fn run(_cli: Cli) -> anyhow::Result<()> {
    Ok(())
}
