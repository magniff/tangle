use anyhow;
use clap::Parser;
use tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = tangle::settings::TangleArguments::parse();

    simplelog::TermLogger::init(
        arguments.log_level,
        simplelog::Config::default(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )?;

    tangle::server::run(
        tokio::net::TcpListener::bind(arguments.server_address).await?,
        tokio::signal::ctrl_c(),
    )
    .await
}
