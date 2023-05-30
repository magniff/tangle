use std::sync::Arc;

use clap::Parser;

fn main() -> anyhow::Result<()> {
    let arguments = Arc::new(tangle::settings::TangleArguments::parse());

    simplelog::TermLogger::init(
        arguments.log_level,
        simplelog::Config::default(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(arguments.worker_threads as usize)
        .enable_io()
        .build()
        .unwrap();

    runtime.block_on(tangle::server::run_tangled(
        arguments,
        tokio::signal::ctrl_c(),
    ))
}
