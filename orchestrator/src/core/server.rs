//!    # Server Operational Flow
//!    * Upon initiation with `serve()` function, a `TcpListener` is spawned at a given address and begins listening for connections.
//!
//!    * When a client connects, the Server creates a `Connection` and `ConnectionHandle`. The Connection
//!    is given a `Sender` to send requests to the Router as well as a `Sender` to send Commands.
//!
//!    * `Connection` sends a `ChannelSubscribe` command to the Router to add itself to the routing table.
//!
//!    * The `ChannelSubscribe` command contains a UUID to identify the subscription, and a channel for
//!    the Router to respond to any requests
//!
//!    * `Connection` then spawns a `ReadConnection` and `WriteConnection`.
//!
//!    * `ReadConnection` handles incoming requests from the Client and forwards them on to the Router.
//!
//!    * `WriteConnection` accepts responses from the Router and send them to the Client.

use std::net::SocketAddr;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use bytes::BytesMut;
use connection::messages::Command;
use dashmap::DashMap;
use tachyonix::{channel, Receiver, Sender};
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::core::messages::{
    ChannelSubscribe, ChannelUnsubscribe, RouterCommand, RouterRequestWrapper, ServerCommand,
};
use util::map_access_wrapper::{arc_map_insert, arc_map_remove};

pub struct ServerHandle {
    pub command_channel: Sender<ServerCommand>,
}

pub struct Server {
    command_channel: Receiver<ServerCommand>,
    address: SocketAddr,
    connections: Arc<DashMap<Uuid, ConnectionHandle>>,
    router_channel: Sender<RouterRequestWrapper>,
}

impl Server {
    pub fn new(
        address: SocketAddr,
        router_channel: Sender<RouterRequestWrapper>,
    ) -> (Server, ServerHandle) {
        let (tx, rx) = channel(100);
        let server = Server {
            command_channel: rx,
            address,
            connections: Arc::new(DashMap::new()),
            router_channel,
        };

        let server_handle = ServerHandle {
            command_channel: tx,
        };
        (server, server_handle)
    }

    pub async fn serve(
        mut self,
        router_command_channel: Sender<RouterCommand>,
        self_command_channel: Sender<ServerCommand>,
    ) -> io::Result<()> {
        let data_listener = TcpListener::bind(self.address).await.unwrap();
        let connections_ref = self.connections.clone();
        let listener = tokio::spawn(async move {
            loop {
                let send_to_router = self.router_channel.clone();
                let router_command_clone = router_command_channel.clone();
                match data_listener.accept().await {
                    Ok((stream, _)) => {
                        let (connection, connection_handle) = Connection::new();
                        let connection_command_channel = connection_handle.command_channel.clone();
                        let server_command_channel = self_command_channel.clone();

                        arc_map_insert(
                            connections_ref.clone(),
                            connection.channel_id,
                            connection_handle,
                        );
                        tokio::spawn(async move {
                            connection
                                .start(
                                    stream,
                                    send_to_router,
                                    router_command_clone,
                                    connection_command_channel,
                                    server_command_channel,
                                )
                                .await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        while let Ok(command) = self.command_channel.recv().await {
            match command {
                ServerCommand::Shutdown() => {
                    listener.abort();
                    // TODO: This has no way of knowing when each channel is closed. For graceful shutdown it should remain open until they are all done. Fix with refactor of handle where handle owns the task spawning the actor
                    for conn_handle in self.connections.iter() {
                        conn_handle
                            .command_channel
                            .send(Command::Shutdown())
                            .await
                            .expect("Unable to send!");
                    }
                    break;
                }
                ServerCommand::RemoveServerConnection(id) => {
                    arc_map_remove(self.connections.clone(), &id);
                }
            }
        }

        Ok(())
    }
}

pub struct ConnectionHandle {
    command_channel: Sender<Command>,
}

pub struct Connection {
    command_channel: Receiver<Command>,
    channel_id: Uuid,
}

impl Connection {
    pub fn new() -> (Connection, ConnectionHandle) {
        let (tx, rx) = channel(100);
        let connection = Connection {
            command_channel: rx,
            channel_id: Uuid::new_v4(),
        };

        let connection_handle = ConnectionHandle {
            command_channel: tx,
        };
        (connection, connection_handle)
    }

    pub async fn start(
        mut self,
        stream: TcpStream,
        send_to_router: Sender<RouterRequestWrapper>,
        router_command_channel: Sender<RouterCommand>,
        self_command_channel: Sender<Command>,
        server_command_channel: Sender<ServerCommand>,
    ) {
        let (read, write) = tokio::io::split(stream);
        let (read_half_queue, read_input_queue) = channel::<Command>(100);
        let (write_half_queue, write_input_queue) = channel::<Command>(100);

        // large channel size causes slowdown on first request but subsequent requests are unaffected
        let (router_sender, receive_from_router) = channel::<BytesMut>(20_000);

        let (oneshot_send, oneshot_receive) = oneshot::channel::<Option<Uuid>>();
        router_command_channel
            .send(RouterCommand::Subscribe(ChannelSubscribe {
                channel_id: self.channel_id,
                response_channel: router_sender,
                subscribe_acknowledge: oneshot_send,
            }))
            .await
            .expect("Unable to Subscribe!");

        // In most cases this should eval to None. Will only set a new id if there was a collision found in the Router channel_map. Has been encountered in testing.
        match oneshot_receive.await.expect("Unable to receive!") {
            None => {}
            Some(id) => self.channel_id = id,
        }

        let read_connection = ReadConnection {
            command_channel: read_input_queue,
        };

        let write_connection = WriteConnection {
            command_channel: write_input_queue,
        };

        let write_handle = tokio::spawn(async move {
            write_connection.write(BufWriter::new(write), receive_from_router).await;
        });

        let read_handle = tokio::spawn(async move {
            read_connection
                .read(self.channel_id, BufReader::new(read), send_to_router, self_command_channel)
                .await;
        });

        loop {
            match self.command_channel.recv().await {
                Ok(command) => {
                    match command {
                        Command::Shutdown() => {
                            // ReadConnection may be the one sending the Shutdown command, so check that it is open before trying to send
                            if !read_half_queue.is_closed() {
                                read_half_queue
                                    .send(Command::Shutdown())
                                    .await
                                    .expect("Unable to send!");
                            }
                            write_half_queue
                                .send(Command::Shutdown())
                                .await
                                .expect("Unable to send!");
                            // Wait until both have closed before closing the Connection object
                            while !read_handle.is_finished() || !write_handle.is_finished() {
                                sleep(Duration::from_millis(1))
                            }
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
        router_command_channel
            .send(RouterCommand::Unsubscribe(ChannelUnsubscribe {
                channel_id: self.channel_id,
            }))
            .await
            .expect("Unable to Subscribe!");

        server_command_channel
            .send(ServerCommand::RemoveServerConnection(self.channel_id))
            .await
            .expect("Unable to send!")
    }
}

/** Reads from connected client and relays the request on to the router **/
pub struct ReadConnection {
    command_channel: Receiver<Command>,
}

impl ReadConnection {
    async fn read(mut self, channel_id: Uuid, mut data_read_stream: BufReader<ReadHalf<TcpStream>>, router_channel: Sender<RouterRequestWrapper>, server_command_channel: Sender<Command>) {
        let read_task = tokio::spawn(async move {
            loop {
                match data_read_stream.read_u32().await {
                    Ok(message_size) => {
                        let mut buff = BytesMut::with_capacity(message_size as usize);
                        // TODO: will likely need a different read function here to ensure we get the whole buffer. Or repeat the read until we fill the buffer
                        match data_read_stream.read_buf(&mut buff).await {
                            Ok(_) => {
                                router_channel
                                    .send(RouterRequestWrapper {
                                        channel_id,
                                        body: buff,
                                    })
                                    .await
                                    .expect("Unable to send!");
                            }
                            Err(_) => break,
                        }
                    }
                    Err(_) => break,
                }
            }
            server_command_channel
                .send(Command::Shutdown())
                .await
                .expect("Cannot send!");
        });
        loop {
            match self.command_channel.recv().await {
                Ok(command) => {
                    match command {
                        Command::Shutdown() => {
                            read_task.abort();
                            break
                        }
                    }
                }
                Err(_) => break
            }
        }
    }
}

/** Writes back to the client the response it receives from the router **/
pub struct WriteConnection {
    command_channel: Receiver<Command>,
}

impl WriteConnection {
    pub async fn write(mut self, mut data_write_stream: BufWriter<WriteHalf<TcpStream>>, mut router_channel: Receiver<BytesMut>) {
        let write_task = tokio::spawn(async move {
            loop {
                match router_channel.recv().await {
                    Ok(mut buffer) => {
                        data_write_stream
                            .write_u32(buffer.len() as u32)
                            .await
                            .expect("Unable to write!");
                        data_write_stream
                            .write_buf(&mut buffer)
                            .await
                            .expect("Unable to write buffer!");
                        match data_write_stream.flush().await {
                            Ok(_) => {}
                            Err(_) => break,
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        loop {
            match self.command_channel.recv().await {
                Ok(command) => {
                    match command {
                        Command::Shutdown() => {
                            // TODO: This may cause loss of messages currently in flight
                            write_task.abort();
                            break
                        }
                    }
                }
                Err(_) => break
            }
        }

    }
}
