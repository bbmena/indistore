use crate::message_bus::{retrieve_command_channel, MessageBus, MessageBusHandle};
use crate::messages::{
    ConnectionManagerCommand, ConnectionNotification, MessageBusCommand, MessageBusConnection,
};
use bytes::BytesMut;
use dashmap::DashMap;
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use tachyonix::{channel, Receiver, Sender};
use tokio::net::{TcpListener, TcpStream};
use util::map_access_wrapper::{arc_map_contains_key, arc_map_insert, arc_map_remove};

pub struct ConnectionManager {
    command_channel: Receiver<ConnectionManagerCommand>,
    address: SocketAddr,
    node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    notifier: Sender<ConnectionNotification>,
}

pub struct ConnectionManagerHandle {
    pub command_channel: Sender<ConnectionManagerCommand>,
    pub notification_receiver: Receiver<ConnectionNotification>,
}

impl ConnectionManagerHandle {
    pub fn new(
        address: SocketAddr,
        node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
        output_channel: Sender<BytesMut>,
    ) -> ConnectionManagerHandle {
        let (command_sender, command_receiver) = channel(100);
        let (notifier, notification_receiver) = channel(100);

        let connection_manager = ConnectionManager {
            command_channel: command_receiver,
            address,
            node_map,
            notifier,
        };

        let sender_clone = command_sender.clone();
        tokio::spawn(async move { connection_manager.start(output_channel, sender_clone).await });

        ConnectionManagerHandle {
            command_channel: command_sender,
            notification_receiver,
        }
    }
}

impl ConnectionManager {
    pub async fn start(
        mut self,
        output_channel: Sender<BytesMut>,
        self_sender: Sender<ConnectionManagerCommand>,
    ) {
        let data_listener = TcpListener::bind(self.address).await.unwrap();
        let node_map = self.node_map.clone();
        let output_clone = output_channel.clone();
        let sender_clone = self_sender.clone();
        let listener = tokio::spawn(async move {
            loop {
                match data_listener.accept().await {
                    Ok((stream, address)) => {
                        println!("New connection request from {}", &address);
                        if !arc_map_contains_key(node_map.clone(), &address.ip()) {
                            let out = output_clone.clone();
                            let handle =
                                MessageBusHandle::new(out, address.clone(), sender_clone.clone());

                            arc_map_insert(node_map.clone(), address.ip(), handle);

                            self.notifier
                                .send(ConnectionNotification { address })
                                .await
                                .expect("Unable to send!");
                        }

                        match retrieve_command_channel(node_map.clone(), &address.ip()) {
                            None => {
                                println!("Connection not found!")
                            }
                            Some(command_channel) => {
                                command_channel
                                    .send(MessageBusCommand::AddConnection(MessageBusConnection {
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
                        ConnectionManagerCommand::Shutdown() => {
                            // TODO: graceful shutdown
                            listener.abort();
                            break;
                        }
                        ConnectionManagerCommand::Connect(connect) => {
                            println!("Attempting to connect to {}", &connect.address);
                            let stream = TcpStream::connect(connect.address)
                                .await
                                .expect("Unable to connect!");
                            if !arc_map_contains_key(self.node_map.clone(), &connect.address.ip()) {
                                let out = out_put_clone.clone();
                                let handle = MessageBusHandle::new(
                                    out,
                                    connect.address.clone(),
                                    self_sender.clone(),
                                );

                                arc_map_insert(self.node_map.clone(), self.address.ip(), handle);
                            }

                            match retrieve_command_channel(
                                self.node_map.clone(),
                                &self.address.ip(),
                            ) {
                                None => {
                                    println!("Connection not found!")
                                }
                                Some(command_channel) => {
                                    command_channel
                                        .send(MessageBusCommand::AddConnection(
                                            MessageBusConnection {
                                                address: connect.address,
                                                stream,
                                            },
                                        ))
                                        .await
                                        .expect("Unable to send AddConnection command!");
                                }
                            }
                        }
                        ConnectionManagerCommand::RemoveConnection(remove_connection) => {
                            match arc_map_remove(
                                self.node_map.clone(),
                                &remove_connection.address.ip(),
                            ) {
                                None => {}
                                Some((_, handle)) => {
                                    handle.message_bus_task.abort();
                                    println!(
                                        "Connection removed at address: {}",
                                        remove_connection.address
                                    )
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
