use clap::Parser;
use std::net::SocketAddr;

#[derive(Parser, Debug)]
pub struct TangleArguments {
    #[clap(env, long, default_value = "info")]
    pub log_level: simplelog::LevelFilter,

    #[clap(
        env,
        long,
        default_value = "127.0.0.1:6000",
        help = "TCP socket to listen to"
    )]
    pub server_address: SocketAddr,
}
