use crate::core::router::RouterHandle;
use crate::core::server::ServerHandle;
use connection::messages::{ArchivedCliCommand, CliCommand, Command};
use rkyv::Archived;
use std::net::SocketAddr;
use tachyonix::Sender;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use crate::RouterCommand;

pub struct CliListener {
    server_handle: ServerHandle,
    router_handle: RouterHandle,
}

impl CliListener {
    pub fn new(server_handle: ServerHandle, router_handle: RouterHandle) -> CliListener {
        CliListener {
            server_handle,
            router_handle,
        }
    }

    pub async fn listen(self, cli_address: SocketAddr) {
        let cli_listener = TcpListener::bind(cli_address).await.unwrap();
        loop {
            match cli_listener.accept().await {
                Ok((stream, _)) => {
                    let server_channel = self.server_handle.command_channel.clone();
                    let router_channel = self.router_handle.command_channel.clone();
                    tokio::spawn(async move {
                        handle_connection(stream, server_channel, router_channel).await;
                    });
                }
                Err(_) => break,
            }
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    server_channel: Sender<Command>,
    router_channel: Sender<RouterCommand>,
) {
    match stream.read_u32().await {
        Ok(message_size) => {
            let mut vec = vec![0; message_size as usize];
            match stream.read_exact(&mut vec).await {
                Ok(_) => {
                    let command_archive: &Archived<CliCommand> =
                        rkyv::check_archived_root::<CliCommand>(&vec).unwrap();
                    match command_archive {
                        ArchivedCliCommand::Shutdown() => {
                            server_channel
                                .send(Command::Shutdown())
                                .await
                                .expect("Unable to send command to Server!");
                            router_channel
                                .send(RouterCommand::Shutdown())
                                .await
                                .expect("Unable to send command to Router!")
                        }
                    }
                }
                Err(e) => {
                    println!("{}", e)
                }
            }
        }
        Err(e) => {
            println!("{}", e)
        }
    }
}
