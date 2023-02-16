use anyhow;
use clap::Parser;
use tokio;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let switch_arguments = tangle::settings::TangleArguments::parse();

    simplelog::TermLogger::init(
        switch_arguments.log_level,
        simplelog::Config::default(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )?;

    tangle::server::run(
        tokio::net::TcpListener::bind(switch_arguments.server_address).await?,
        tokio::signal::ctrl_c(),
    )
    .await
}
