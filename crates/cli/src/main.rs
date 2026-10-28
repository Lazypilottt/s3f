//! s3f CLI — ultra-fast S3 ops from the command line.
//! Built by Lazypilottt

#![expect(
    clippy::multiple_crate_versions,
    reason = "transitive deps from aws-sigv4 pull duplicate versions"
)]
#![expect(clippy::print_stdout, reason = "CLI output is the whole point")]

mod cli;
mod commands;
mod fmt;

use clap::Parser as _;
use s3f::S3Client;

use crate::cli::{Cli, Command};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "tracing")]
    {
        use tracing_subscriber::EnvFilter;

        tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("s3f=info")),
            )
            .with_target(true)
            .with_timer(tracing_subscriber::fmt::time::uptime())
            .init();
    }

    let cli = Cli::parse();
    let config = cli::build_config(&cli);
    let client = S3Client::new(config).await?;

    match &cli.command {
        Command::Download(args) => commands::download::run(&client, args, cli.quiet).await?,
        Command::Ls(args) => commands::list::run(&client, args, cli.quiet).await?,
        Command::Upload(args) => commands::upload::run(&client, args, cli.quiet).await?,
    }

    Ok(())
}
