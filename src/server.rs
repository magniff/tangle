use std::future::Future;
use std::sync::Arc;

use anyhow::Result;
use futures::future::try_join_all;
use tokio;
use tokio::io::{AsyncReadExt, BufReader, BufWriter};
use tokio::net::TcpListener;

use crate::client::Client;
use crate::protocol;

#[derive(Clone, Copy)]
struct ListenerSettings {
    io_buffer_size: usize,
}

async fn run_network_listener(
    listener: TcpListener,
    messages: tokio::sync::mpsc::UnboundedSender<crate::components::server::Message>,
    settings: ListenerSettings,
) {
    log::info!("Spinning up a new network listener task...");
    while let Ok((tcp_stream, address)) = listener.accept().await {
        let (stream_reader, stream_writer) = tcp_stream.into_split();

        let mut reader = BufReader::with_capacity(settings.io_buffer_size, stream_reader);
        let mut writer = BufWriter::with_capacity(settings.io_buffer_size, stream_writer);

        // Check the protocol version first, NSQ itself has support for the V2 only
        let mut magic_buffer = [0; 4];
        if let Err(message) = reader.read_exact(&mut magic_buffer).await {
            log::error!(
                "{address}: Unable to read the magic bytes from the socket, reason: {message}"
            );
            continue;
        }

        match magic_buffer.as_slice() {
            b"  V2" => {
                tokio::spawn(protocol::handler::run_socket_mainloop(
                    Client::from_reader_writer(address, reader, writer),
                    messages.clone(),
                ));
                log::info!("Client {address} is now connected using NSQ protocol V2");
            }
            wrong_magic => {
                log::error!("{address}: client is using the wrong magic number -- {wrong_magic:?}");
                let error_response_result = protocol::writer::write_error_frame(
                    &mut writer,
                    protocol::constants::E_BAD_PROTOCOL,
                )
                .await;
                if let Err(error_message) = error_response_result {
                    log::error!(
                        "{address}: unable to send the error message to the client, reason: {error_message}"
                    );
                };
                continue;
            }
        }
    }
}

pub async fn run_tangled<S>(config: Arc<crate::settings::TangleArguments>, stopper: S) -> Result<()>
where
    S: Future<Output = std::io::Result<()>>,
{
    let listener = tokio::net::TcpListener::bind(config.server_address).await?;
    let listener_settings = ListenerSettings {
        io_buffer_size: config.socket_buffer_size,
    };
    let (server_channel_sender, server_channel_receiver) =
        tokio::sync::mpsc::unbounded_channel::<crate::components::server::Message>();

    tokio::select! {
        _ = stopper => {
            log::info!("Ctrl+C signal: Will now stop the server...")
        }
        server_result = try_join_all(vec![
            tokio::spawn(run_network_listener(listener, server_channel_sender, listener_settings)),
            tokio::spawn(crate::components::server::run_server(config.clone(), server_channel_receiver)),
        ]) => {
            log::info!("Fatal: Server exited with this result: {server_result:?}");
        }
    };
    Ok(())
}
