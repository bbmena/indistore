use bytes::BytesMut;
use dashmap::DashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;
use tachyonix::{channel, Receiver, Sender};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf, WriteHalf};
use tokio::net::TcpStream;

use async_channel::Receiver as Async_Receiver;
use async_channel::Sender as Async_Sender;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use util::map_access_wrapper::{map_insert, map_remove};

use crate::messages::{Command, ConnectionManagerCommand, MessageBusAddress, MessageBusCommand};

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
    pub async fn read(mut self, output_channel: Sender<BytesMut>, kill_switch: Arc<Notify>) {
        tokio::spawn(async move {
            loop {
                match self.data_read_stream.read_u32().await {
                    Ok(message_size) => {
                        let mut vec = vec![0; message_size as usize];
                        match self.data_read_stream.read_exact(&mut vec).await {
                            Ok(_) => {
                                // TODO find a way around a copy here
                                let buff = BytesMut::from(&vec[..]);
                                output_channel.send(buff).await.expect("Unable to send!")
                            }
                            Err(_) => break,
                        }
                    }
                    Err(_) => {
                        println!("Client disconnected");
                        kill_switch.notify_one();
                        break;
                    }
                }
            }
        });
        loop {
            match self.command_channel.recv().await {
                Ok(command) => match command {
                    Command::Shutdown() => break,
                },
                Err(_) => break,
            }
        }
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
        kill_switch: Arc<Notify>,
    ) {
        tokio::spawn(async move {
            while let Ok(mut buffer) = input_channel.recv().await {
                self.data_write_stream
                    .write_u32(buffer.len() as u32)
                    .await
                    .expect("");

                self.data_write_stream
                    .write_buf(&mut buffer)
                    .await
                    .expect("Unable to write buffer!");
                match self.data_write_stream.flush().await {
                    Err(_) => {
                        println!("Client disconnected");
                        kill_switch.notify_one();
                        break;
                    }
                    _ => {}
                }
            }
        });
        loop {
            match self.command_channel.recv().await {
                Ok(command) => match command {
                    Command::Shutdown() => break,
                },
                Err(_) => break,
            }
        }
    }
}

pub struct ConnectionLane {
    lane_health: LaneHealth,
    port: u16,
}

pub struct ConnectionLaneHandle {
    command_channel: Sender<Command>,
    port: u16,
}

impl ConnectionLaneHandle {
    pub fn new(
        address: SocketAddr,
        input_channel: Async_Receiver<BytesMut>,
        output_channel: Sender<BytesMut>,
        stream: TcpStream,
        kill_switch: Sender<MessageBusCommand>,
    ) -> ConnectionLaneHandle {
        let (tx, rx) = channel(100);

        let connection_lane = ConnectionLane {
            lane_health: LaneHealth::HEALTHY,
            port: address.port(),
        };

        tokio::spawn(async move {
            connection_lane
                .start(
                    address,
                    input_channel,
                    output_channel,
                    stream,
                    rx,
                    kill_switch,
                )
                .await
        });

        ConnectionLaneHandle {
            command_channel: tx,
            port: address.port(),
        }
    }
}

impl ConnectionLane {
    async fn start(
        mut self,
        self_address: SocketAddr,
        input_channel: Async_Receiver<BytesMut>,
        output_channel: Sender<BytesMut>,
        stream: TcpStream,
        mut command_channel: Receiver<Command>,
        kill_switch: Sender<MessageBusCommand>,
    ) {
        println!("Starting new lane at {}", &self_address);
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
        let reader_kill_switch = Arc::new(Notify::new());
        let writer_kill_switch = reader_kill_switch.clone();
        let kill_switch_receiver = reader_kill_switch.clone();

        let writer = tokio::spawn(async move {
            write.write(input_channel, writer_kill_switch).await;
        });

        let reader = tokio::spawn(async move {
            read.read(output_channel, reader_kill_switch).await;
        });

        let command_channel_task = tokio::spawn(async move {
            while let Ok(command) = command_channel.recv().await {
                match command {
                    Command::Shutdown() => {
                        read_half_channel.send(Command::Shutdown()).await.expect("");
                        write_half_channel
                            .send(Command::Shutdown())
                            .await
                            .expect("Unable to send!");
                        break;
                    }
                }
            }
        });

        // If kill switch is notified that means there was a disconnect from the client. No need to wait for tasks to end. Abort.
        kill_switch_receiver.notified().await;
        command_channel_task.abort();
        writer.abort();
        reader.abort();
        // Notify parent of shutdown
        kill_switch
            .send(MessageBusCommand::RemoveConnection(self_address))
            .await
            .expect("Unable to send shutdown!")
    }
}

pub struct MessageBusConnectionManager {
    command_channel: Receiver<MessageBusCommand>,
    connections: DashMap<SocketAddr, ConnectionLaneHandle>,
}

pub struct MessageBusConnectionManagerHandle {
    command_channel: Sender<MessageBusCommand>,
}

impl MessageBusConnectionManagerHandle {
    fn new(kill_switch: Arc<Notify>) -> MessageBusConnectionManagerHandle {
        let connections = DashMap::new();
        let (tx, rx) = channel::<MessageBusCommand>(100);

        let connection_manager = MessageBusConnectionManager {
            command_channel: rx,
            connections,
        };

        tokio::spawn(async move {
            connection_manager.start(kill_switch).await;
        });

        MessageBusConnectionManagerHandle {
            command_channel: tx,
        }
    }
}

impl MessageBusConnectionManager {
    async fn start(mut self, kill_switch: Arc<Notify>) {
        while let Ok(command) = self.command_channel.recv().await {
            // TODO will eventually want a query command as well
            match command {
                MessageBusCommand::Shutdown() => break,
                MessageBusCommand::AddHandle(address, handle) => {
                    map_insert(&self.connections, address, handle);
                }
                MessageBusCommand::RemoveConnection(connection) => {
                    map_remove(&self.connections, &connection);

                    // If all connections to a client have been closed, the client must have disconnected so we can close the bus
                    if self.connections.is_empty() {
                        kill_switch.notify_one();
                        break;
                    }
                }
                _ => {}
            }
        }
    }
}

/**
   A messaging channel for orchestrator-node communication. Makes use of multiple (default: three) streams to avoid blocking of competing requests from multiple clients
**/
pub struct MessageBus {
    command_channel: Receiver<MessageBusCommand>,
    message_bus_health: MessageBusHealth,
    address: SocketAddr,
}

pub struct MessageBusHandle {
    pub command_channel: Sender<MessageBusCommand>,
    pub send_to_bus: Sender<BytesMut>,
    pub message_bus_task: JoinHandle<()>,
}

impl MessageBusHandle {
    pub fn new(
        output_channel: Sender<BytesMut>,
        address: SocketAddr,
        connection_manager_sender: Sender<ConnectionManagerCommand>,
    ) -> MessageBusHandle {
        let (tx, rx) = channel::<MessageBusCommand>(100);
        let (send_to_bus, input_channel) = channel::<BytesMut>(200_000);

        let message_bus = MessageBus {
            command_channel: rx,
            message_bus_health: MessageBusHealth::HEALTHY,
            address,
        };

        let message_bus_task = tokio::spawn(async move {
            message_bus
                .start(input_channel, output_channel, connection_manager_sender)
                .await
        });

        MessageBusHandle {
            command_channel: tx,
            send_to_bus,
            message_bus_task,
        }
    }
}

impl MessageBus {
    async fn start(
        mut self,
        input_channel: Receiver<BytesMut>,
        output_channel: Sender<BytesMut>,
        connection_manager_sender: Sender<ConnectionManagerCommand>,
    ) {
        let (work_queue, stealer) = async_channel::bounded(200_000);
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

        let message_bus_connection_manager_notify = Arc::new(Notify::new());
        let kill_switch = message_bus_connection_manager_notify.clone();

        let connection_manager = MessageBusConnectionManagerHandle::new(kill_switch.clone());

        loop {
            tokio::select! {
                Ok(command) = self.command_channel.recv() => {
                    match command {
                        MessageBusCommand::Shutdown() => {
                            input_command
                                .send(Command::Shutdown())
                                .await
                                .expect("Unable to send!");
                            output_command
                                .send(Command::Shutdown())
                                .await
                                .expect("Unable to send!");
                            break;
                        }
                        MessageBusCommand::AddConnection(connection) => {
                            let stealer_clone = stealer.clone();
                            let sender = lane_sender.clone();
                            let address = connection.address.clone();
                            let handle = ConnectionLaneHandle::new(
                                address.clone(),
                                stealer_clone,
                                sender,
                                connection.stream,
                                connection_manager.command_channel.clone()
                            );

                            connection_manager.command_channel.send(MessageBusCommand::AddHandle(address, handle)).await.expect("Unable to send!")
                        }
                        _ => {}
                    }
                },
                _ = message_bus_connection_manager_notify.notified() => {
                    break
                }
            }
        }

        connection_manager_sender
            .send(ConnectionManagerCommand::RemoveConnection(
                MessageBusAddress {
                    address: self.address,
                },
            ))
            .await
            .expect("Unable to send!");
        println!("Closing Message Bus")
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

// Access to DashMap must be done from a synchronous function
#[inline]
pub fn retrieve_messagebus_sender(
    channel_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    address: IpAddr,
) -> Option<Sender<BytesMut>> {
    match channel_map.get(&address) {
        None => None,
        Some(entry) => Some(entry.send_to_bus.clone()),
    }
}

// Access to DashMap must be done from a synchronous function
#[inline]
pub fn retrieve_command_channel(
    node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    key: &IpAddr,
) -> Option<Sender<MessageBusCommand>> {
    match node_map.get(key) {
        None => None,
        Some(entry) => Some(entry.command_channel.clone()),
    }
}
