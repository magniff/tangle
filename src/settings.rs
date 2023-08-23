use clap::Parser;
use std::net::SocketAddr;

#[derive(Parser, Debug)]
pub struct TangleArguments {
    #[clap(env, long, default_value = "debug")]
    pub log_level: simplelog::LevelFilter,

    #[clap(env, long, help = "Max worker threads count")]
    pub worker_threads: Option<usize>,

    #[clap(env, long, default_value = "2048", help = "Max in-flight messages")]
    pub max_in_flight: u32,

    #[clap(
        env,
        long,
        default_value = "4096",
        help = "The size of the socket io buffer"
    )]
    pub socket_buffer_size: usize,

    #[clap(
        env,
        long,
        default_value = "0.0.0.0:6000",
        help = "TCP socket to listen to"
    )]
    pub server_address: SocketAddr,
}
