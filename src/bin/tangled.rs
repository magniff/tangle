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

    let mut runtime_builder = &mut tokio::runtime::Builder::new_multi_thread();
    if let Some(thread_pool_size) = arguments.worker_threads {
        runtime_builder = runtime_builder.worker_threads(thread_pool_size);
    };

    runtime_builder
        .enable_io()
        .build()
        .unwrap()
        .block_on(tangle::server::run_tangled(
            arguments,
            tokio::signal::ctrl_c(),
        ))
}
