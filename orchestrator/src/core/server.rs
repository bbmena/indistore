use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use dashmap::DashMap;
use tachyonix::{channel, Receiver, Sender};
use tokio::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

use connection::messages::{ChannelSubscribe, Command, RouterRequestWrapper};

pub struct ServerHandle {
    command_channel: Sender<Command>,
}

pub struct Server {
    command_channel: Receiver<Command>,
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

    pub async fn serve(mut self, router_command_queue: Sender<Command>) -> io::Result<()> {
        let data_listener = TcpListener::bind(self.address).await.unwrap();
        let connections_ref = self.connections.clone();
        let listener = tokio::spawn(async move {
            loop {
                let r_c = self.router_channel.clone();
                let r_c_q = router_command_queue.clone();
                match data_listener.accept().await {
                    Ok((stream, _)) => {
                        let (connection, connection_handle) = Connection::new();
                        connections_ref.insert(connection.channel_id, connection_handle);
                        tokio::spawn(async move {
                            connection.start(stream, r_c, r_c_q).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        loop {
            match self.command_channel.recv().await {
                Ok(command) => match command {
                    Command::Shutdown() => {
                        listener.abort();
                        for conn_handle in self.connections.iter() {
                            conn_handle
                                .command_channel
                                .send(Command::Shutdown())
                                .await
                                .expect("");
                        }
                        break;
                    }
                    _ => {}
                },
                _ => {}
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

pub struct ReadConnection {
    data_read_stream: BufReader<ReadHalf<TcpStream>>,
    command_channel: Receiver<Command>,
    router_channel: Sender<RouterRequestWrapper>,
}

pub struct WriteConnection {
    data_write_stream: BufWriter<WriteHalf<TcpStream>>,
    command_channel: Receiver<Command>,
    router_channel: Receiver<BytesMut>,
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
        router_channel: Sender<RouterRequestWrapper>,
        router_command_queue: Sender<Command>,
    ) {
        let (read, write) = tokio::io::split(stream);
        let (read_half_queue, read_input_queue) = channel::<Command>(100);
        let (write_half_queue, write_input_queue) = channel::<Command>(100);

        let (router_sender, router_receiver) = channel::<BytesMut>(10000);

        router_command_queue
            .send(Command::Subscribe(ChannelSubscribe {
                channel_id: self.channel_id,
                response_channel: router_sender,
            }))
            .await
            .expect("Unable to Subscribe!");

        let read_connection = ReadConnection {
            data_read_stream: BufReader::new(read),
            command_channel: read_input_queue,
            router_channel,
        };

        let write_connection = WriteConnection {
            data_write_stream: BufWriter::new(write),
            command_channel: write_input_queue,
            router_channel: router_receiver,
        };

        tokio::spawn(async move {
            write_connection.write().await;
        });

        tokio::spawn(async move {
            read_connection.read(self.channel_id).await;
        });

        loop {
            match self.command_channel.recv().await {
                Ok(command) => {
                    match command {
                        Command::Shutdown() => {
                            read_half_queue
                                .send(Command::Shutdown())
                                .await
                                .expect("TODO: panic message");
                            write_half_queue
                                .send(Command::Shutdown())
                                .await
                                .expect("TODO: panic message");
                            // TODO wait until both have shutdown gracefully
                            break;
                        }
                        _ => {}
                    }
                }
                Err(_) => {}
            }
        }
    }
}

impl ReadConnection {
    async fn read(mut self, channel_id: Uuid) {
        loop {
            match self.data_read_stream.read_u32().await {
                Ok(message_size) => {
                    let mut buff = BytesMut::with_capacity(message_size as usize);
                    // TODO: will likely need a different read function here to ensure we get the whole buffer. Or repeat the read until we fill the buffer
                    match self.data_read_stream.read_buf(&mut buff).await {
                        Ok(_) => {
                            self.router_channel
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
    }
}

impl WriteConnection {
    pub async fn write(mut self) {
        loop {
            match self.router_channel.recv().await {
                Ok(mut buffer) => {
                    let _ = self.data_write_stream.write_u32(buffer.len() as u32);
                    self.data_write_stream
                        .write_buf(&mut buffer)
                        .await
                        .expect("Unable to write buffer!");
                    match self.data_write_stream.flush().await {
                        Ok(_) => {}
                        Err(_) => break,
                    }
                }
                Err(_) => break,
            }
        }
    }
}
