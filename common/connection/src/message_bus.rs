use bytes::BytesMut;
use dashmap::DashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tachyonix::{channel, Receiver, Sender};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::net::TcpStream;

use async_channel::Receiver as Async_Receiver;
use async_channel::Sender as Async_Sender;
use dashmap::mapref::one::Ref;
use util::map_access_wrapper::map_insert;

use crate::messages::{Command, MessageBusCommand};

/**
   UNINITIATED - Not yet connected
   HEALTHY - No issue
   UNHEALTHY - More than two missed health-check
**/
enum LaneHealth {
    UNINITIATED,
    HEALTHY,
    UNHEALTHY,
}

/**
    HEALTHY - All lanes in healthy state
    PARTIAL - One lane in unhealthy or uninitiated state
    UNHEALTHY - More than one lane in unhealthy or uninitiated state
**/
#[derive(Copy, Clone)]
pub enum MessageBusHealth {
    HEALTHY,
    PARTIAL,
    UNHEALTHY,
}

pub struct ReadConnection {
    data_read_stream: BufReader<ReadHalf<TcpStream>>,
    command_channel: Receiver<Command>,
}

impl ReadConnection {
    pub async fn read(mut self, output_channel: Sender<BytesMut>) {
        tokio::spawn(async move {
            loop {
                match self.data_read_stream.read_u32().await {
                    Ok(message_size) => {
                        let mut vec = vec![0; message_size as usize];
                        match self.data_read_stream.read_exact(&mut vec).await {
                            Ok(s) => {
                                // TODO find a way around a copy here
                                let buff = BytesMut::from(&vec[..]);
                                output_channel.send(buff).await.expect("Unable to send!")
                            }
                            Err(_) => break,
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }
}

pub struct WriteConnection {
    data_write_stream: BufWriter<WriteHalf<TcpStream>>,
    command_channel: Receiver<Command>,
}

impl WriteConnection {
    pub async fn write(
        mut self,
        input_channel: Async_Receiver<BytesMut>,
        self_address: SocketAddr,
        target_address: SocketAddr,
    ) {
        tokio::spawn(async move {
            loop {
                let mut buffer = input_channel.recv().await.expect("unable to receive!");
                self.data_write_stream
                    .write_u32(buffer.len() as u32)
                    .await
                    .expect("");

                self.data_write_stream
                    .write_buf(&mut buffer)
                    .await
                    .expect("Unable to write buffer!");
                match self.data_write_stream.flush().await {
                    Err(_) => break,
                    _ => {}
                }
            }
        });
        loop {
            match self.command_channel.recv().await {
                Ok(command) => match command {
                    Command::Shutdown() => {}
                },
                Err(_) => break,
            }
        }
    }
}

pub struct ConnectionLane {
    command_channel: Receiver<Command>,
    lane_health: LaneHealth,
    port: u16,
}

pub struct ConnectionLaneHandle {
    command_channel: Sender<Command>,
    port: u16,
}

impl ConnectionLane {
    pub fn new(port: u16) -> (ConnectionLane, ConnectionLaneHandle) {
        let (tx, rx) = channel(100);

        let connection_lane = ConnectionLane {
            command_channel: rx,
            lane_health: LaneHealth::HEALTHY,
            port,
        };
        let connection_lane_handle = ConnectionLaneHandle {
            command_channel: tx,
            port,
        };
        (connection_lane, connection_lane_handle)
    }

    pub async fn start(
        mut self,
        input_channel: Async_Receiver<BytesMut>,
        output_channel: Sender<BytesMut>,
        stream: TcpStream,
    ) {
        let local_address = stream.local_addr().expect("oh noes").clone();
        let target_address = stream.peer_addr().expect("oh noes").clone();
        println!("Starting new lane at {}", &local_address);
        let (read, write) = tokio::io::split(stream);

        let (read_half_channel, read_command_channel) = channel::<Command>(100);
        let (write_half_channel, write_command_channel) = channel::<Command>(100);

        let write = WriteConnection {
            data_write_stream: BufWriter::new(write),
            command_channel: write_command_channel,
        };

        let read = ReadConnection {
            data_read_stream: BufReader::new(read),
            command_channel: read_command_channel,
        };

        tokio::spawn(async move {
            write
                .write(input_channel, local_address, target_address)
                .await;
        });

        tokio::spawn(async move {
            read.read(output_channel).await;
        });

        loop {
            match self.command_channel.recv().await {
                Ok(command) => match command {
                    Command::Shutdown() => {
                        read_half_channel.send(Command::Shutdown()).await.expect("");
                        write_half_channel
                            .send(Command::Shutdown())
                            .await
                            .expect("Unable to send!");
                        break;
                    }
                },
                Err(_) => break,
            }
        }
    }
}

/**
   A messaging channel for orchestrator-node communication. Makes use of multiple (default: three) streams to avoid blocking of competing requests from multiple clients
**/
pub struct MessageBus {
    command_channel: Receiver<MessageBusCommand>,
    connections: DashMap<SocketAddr, ConnectionLaneHandle>,
    message_bus_health: MessageBusHealth,
}

pub struct MessageBusHandle {
    pub command_channel: Sender<MessageBusCommand>,
    pub send_to_bus: Sender<BytesMut>,
}

impl MessageBus {
    pub fn new(send_to_bus: Sender<BytesMut>) -> (MessageBus, MessageBusHandle) {
        let connections = DashMap::new();
        let (tx, rx) = channel::<MessageBusCommand>(100);
        let bus = MessageBus {
            command_channel: rx,
            connections,
            message_bus_health: MessageBusHealth::HEALTHY,
        };
        let handle = MessageBusHandle {
            command_channel: tx,
            send_to_bus,
        };
        (bus, handle)
    }

    pub async fn start(
        mut self,
        input_channel: Receiver<BytesMut>,
        output_channel: Sender<BytesMut>,
        work_queue: Async_Sender<BytesMut>,
        stealer: Async_Receiver<BytesMut>,
    ) {
        let (lane_sender, receive_from_lane) = channel(200_000);

        let (input_command, rx) = channel(100);
        let input = MessageBusInput {
            input_channel,
            worker: work_queue,
            command_channel: rx,
        };
        let (output_command, rx) = channel(100);
        let output = MessageBusOutput {
            output_channel,
            command_channel: rx,
        };

        tokio::spawn(async move {
            input.start().await;
        });

        tokio::spawn(async move {
            output.start(receive_from_lane).await;
        });

        loop {
            match self.command_channel.recv().await {
                Ok(command) => {
                    match command {
                        MessageBusCommand::Shutdown() => {
                            // TODO these commands will currently be ignored. Need to be handled by receivers
                            input_command
                                .send(Command::Shutdown())
                                .await
                                .expect("Unable to send!");
                            output_command
                                .send(Command::Shutdown())
                                .await
                                .expect("Unable to send!");
                        }
                        MessageBusCommand::AddConnection(connection) => {
                            let stealer_clone = stealer.clone();
                            let (lane, handle) = ConnectionLane::new(connection.address.port());
                            map_insert(self.connections.clone(), connection.address, handle);
                            let sender = lane_sender.clone();
                            tokio::spawn(async move {
                                lane.start(stealer_clone, sender, connection.stream).await
                            });
                        }
                    }
                }
                Err(_) => {}
            }
        }
    }

    pub fn health(&self) -> MessageBusHealth {
        self.message_bus_health.clone()
    }
}

pub struct MessageBusInput {
    input_channel: Receiver<BytesMut>,
    worker: Async_Sender<BytesMut>,
    command_channel: Receiver<Command>,
}

// TODO needs to listen to command channel as well
impl MessageBusInput {
    pub async fn start(mut self) {
        loop {
            match self.input_channel.recv().await {
                Ok(message) => self.worker.send(message).await.expect("unable to send!"),
                Err(_) => break,
            }
        }
    }
}

pub struct MessageBusOutput {
    output_channel: Sender<BytesMut>,
    command_channel: Receiver<Command>,
}

// TODO needs to listen to command channel as well
impl MessageBusOutput {
    pub async fn start(self, mut from_lanes: Receiver<BytesMut>) {
        loop {
            match from_lanes.recv().await {
                Ok(message) => {
                    self.output_channel
                        .send(message)
                        .await
                        .expect("Unable to send!");
                }
                Err(_) => break,
            }
        }
    }
}

#[inline]
pub fn retrieve_response_channel(
    channel_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    origination_address: IpAddr,
) -> Option<Sender<BytesMut>> {
    match channel_map.get(&origination_address) {
        None => None,
        Some(response_channel) => Some(response_channel.send_to_bus.clone()),
    }
}
