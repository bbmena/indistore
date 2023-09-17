use crate::message_bus::{retrieve_command_channel, MessageBus, MessageBusHandle};
use crate::messages::{
    AddConnection, ConnectionNotification, MessageBusCommand, NodeManagerCommand,
};
use bytes::BytesMut;
use dashmap::DashMap;
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
};
use tachyonix::{channel, Receiver, Sender};
use tokio::net::{TcpListener, TcpStream};
use util::map_access_wrapper::{arc_map_contains_key, arc_map_insert};

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
                        if !arc_map_contains_key(node_map.clone(), &address.ip()) {
                            let out = output_clone.clone();
                            let handle = MessageBusHandle::new(out);

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
                            if !arc_map_contains_key(self.node_map.clone(), &connect.address.ip()) {
                                let out = out_put_clone.clone();
                                let handle = MessageBusHandle::new(out);

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
