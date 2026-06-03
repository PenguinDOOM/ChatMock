use anyhow::Result;
use chatmock_rs::cli;
use chatmock_rs::config::Cli;
use chatmock_rs::server;
use clap::Parser;

#[tokio::main]
async fn main() -> Result<()> {
    server::init_tracing();
    let cli = Cli::parse();
    cli::run(cli).await?;
    Ok(())
}
