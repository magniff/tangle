use clap::Parser;

fn main() -> anyhow::Result<()> {
    let arguments = tangle::settings::TangleArguments::parse();

    simplelog::TermLogger::init(
        arguments.log_level,
        simplelog::Config::default(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .thread_name("tangled-main")
        .build()
        .unwrap();

    runtime.block_on(async move {
        tangle::server::run(
            tokio::net::TcpListener::bind(arguments.server_address).await?,
            tokio::signal::ctrl_c(),
        )
        .await
    })
}
