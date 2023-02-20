use clap::Parser;
use std::net::SocketAddr;

#[derive(Parser, Debug)]
pub struct TangleArguments {
    #[clap(env, long, default_value = "info")]
    pub log_level: simplelog::LevelFilter,

    #[clap(env, long, default_value = "4", help = "Max worker threads count")]
    pub worker_threads: u8,

    #[clap(env, long, default_value = "16", help = "Max in-flight messages")]
    pub max_in_flight: u8,

    #[clap(
        env,
        long,
        default_value = "0.0.0.0:6000",
        help = "TCP socket to listen to"
    )]
    pub server_address: SocketAddr,
}
