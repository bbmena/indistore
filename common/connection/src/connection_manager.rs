use crate::message_bus::{MessageBus, MessageBusHandle};
use crate::messages::{AddConnection, ConnectionNotification, MessageBusCommand, NodeManagerCommand};
use bytes::BytesMut;
use dashmap::DashMap;
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use dashmap::mapref::one::Ref;
use tachyonix::{channel, Receiver, Sender};
use tokio::net::{TcpListener, TcpStream};

pub struct ConnectionManager {
    command_channel: Receiver<NodeManagerCommand>,
    address: SocketAddr,
    node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    notifier: Sender<ConnectionNotification>,
}

pub struct ConnectionManagerHandle {
    pub command_channel: Sender<NodeManagerCommand>,
    pub notification_receiver: Receiver<ConnectionNotification>,
}

// Access to DashMap must be done from a synchronous function
fn node_map_insert(node_map: Arc<DashMap<IpAddr, MessageBusHandle>>, key: IpAddr, val: MessageBusHandle) {
    node_map.insert(key, val);
}

// Access to DashMap must be done from a synchronous function
fn node_map_contains_key(node_map: Arc<DashMap<IpAddr, MessageBusHandle>>, key: &IpAddr) -> bool {
    node_map.contains_key(key)
}

// Access to DashMap must be done from a synchronous function
fn fetch_command_channel(node_map: Arc<DashMap<IpAddr, MessageBusHandle>>, key: &IpAddr) -> Option<Sender<MessageBusCommand>> {
    match node_map.get(key) {
        None => None,
        Some(entry) => {
            Some(entry.command_channel.clone())
        }
    }
}

impl ConnectionManager {
    pub fn new(
        address: SocketAddr,
        node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    ) -> (ConnectionManager, ConnectionManagerHandle) {
        let (command_sender, command_receiver) = channel(100);
        let (notifier, notification_receiver) = channel(100);

        let node_listener = ConnectionManager {
            command_channel: command_receiver,
            address,
            node_map,
            notifier,
        };

        let node_listener_handle = ConnectionManagerHandle {
            command_channel: command_sender,
            notification_receiver,
        };
        (node_listener, node_listener_handle)
    }

    pub async fn start(mut self, output_channel: Sender<BytesMut>) {
        let data_listener = TcpListener::bind(self.address).await.unwrap();
        let node_map = self.node_map.clone();
        let output_clone = output_channel.clone();
        let listener = tokio::spawn(async move {
            loop {
                match data_listener.accept().await {
                    Ok((stream, address)) => {
                        println!("New connection request from {}", &address);
                        if !node_map_contains_key(node_map.clone(), &address.ip()) {
                            let (send_to_bus, input_channel) = channel::<BytesMut>(200_000);

                            let (message_bus, handle) = MessageBus::new(send_to_bus);

                            node_map_insert(node_map.clone(), address.ip(), handle);
                            let out = output_clone.clone();
                            tokio::spawn(async move {
                                start_new_bus(message_bus, input_channel, out.clone()).await;
                            });
                            self.notifier
                                .send(ConnectionNotification { address })
                                .await
                                .expect("Unable to send!");
                        }

                        match fetch_command_channel(node_map.clone(), &address.ip) {
                            None => { println!("Connection not found!") }
                            Some(command_channel) => {
                                command_channel
                                    .send(MessageBusCommand::AddConnection(AddConnection {
                                        address,
                                        stream,
                                    }))
                                    .await
                                    .expect("Unable to send AddConnection command!");
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        let out_put_clone = output_channel.clone();
        loop {
            match self.command_channel.recv().await {
                Ok(command) => {
                    match command {
                        NodeManagerCommand::Shutdown() => {
                            // TODO: graceful shutdown
                            listener.abort();
                            break;
                        }
                        NodeManagerCommand::Connect(connect) => {
                            println!("Attempting to connect to {}", &connect.address);
                            let stream = TcpStream::connect(connect.address)
                                .await
                                .expect("Unable to connect!");
                            if !node_map_contains_key(self.node_map.clone(), &connect.address.ip()) {
                                let (send_to_bus, input_channel) = channel::<BytesMut>(200_000);

                                let (message_bus, handle) = MessageBus::new(send_to_bus);

                                node_map_insert(self.node_map.clone(), address.ip(), handle);
                                let out = out_put_clone.clone();
                                tokio::spawn(async move {
                                    start_new_bus(message_bus, input_channel, out.clone()).await;
                                });
                            }

                            match fetch_command_channel(self.node_map.clone(), &address.ip) {
                                None => { println!("Connection not found!") }
                                Some(command_channel) => {
                                    command_channel
                                        .send(MessageBusCommand::AddConnection(AddConnection {
                                            address: connect.address,
                                            stream,
                                        }))
                                        .await
                                        .expect("Unable to send AddConnection command!");
                                }
                            }
                        }
                    }
                }
                Err(_) => break,
            }
        }
    }
}

async fn start_new_bus(
    message_bus: MessageBus,
    input_channel: Receiver<BytesMut>,
    output_channel: Sender<BytesMut>,
) {
    let (work_queue, stealer) = async_channel::bounded(200_000);

    message_bus
        .start(input_channel, output_channel, work_queue, stealer)
        .await;
}
