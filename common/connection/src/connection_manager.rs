use crate::message_bus::{MessageBus, MessageBusHandle};
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
use tokio::task::JoinHandle;

pub struct ConnectionManager {
    command_channel: Receiver<NodeManagerCommand>,
    address: IpAddr,
    node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    notifier: Sender<ConnectionNotification>,
    ports: Vec<u16>
}

pub struct ConnectionManagerHandle {
    pub command_channel: Sender<NodeManagerCommand>,
    pub notification_receiver: Receiver<ConnectionNotification>,
}

impl ConnectionManager {
    pub fn new(
        address: IpAddr,
        node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
        ports: Vec<u16>
    ) -> (ConnectionManager, ConnectionManagerHandle) {
        let (command_sender, command_receiver) = channel(100);
        let (notifier, notification_receiver) = channel(100);

        let node_listener = ConnectionManager {
            command_channel: command_receiver,
            address,
            node_map,
            notifier,
            ports,
        };

        let node_listener_handle = ConnectionManagerHandle {
            command_channel: command_sender,
            notification_receiver,
        };
        (node_listener, node_listener_handle)
    }

    pub async fn start(mut self, output_channel: Sender<BytesMut>) {
        let mut listeners = Vec::with_capacity(self.ports.len());
        for port in self.ports {
            let data_listener = TcpListener::bind(SocketAddr::new(self.address, port)).await.unwrap();
            let node_map = self.node_map.clone();
            let output_clone = output_channel.clone();
            let notifier = self.notifier.clone();
            let listener = spawn_listener(notifier, data_listener, node_map, output_clone);
            listeners.push(listener)
        }


        // let data_listener = TcpListener::bind(self.address).await.unwrap();
        // let node_map = self.node_map.clone();
        // let output_clone = output_channel.clone();

        // let notifier = self.notifier.clone();
        // let listener = spawn_listener(notifier, data_listener, node_map, output_clone);
        let out_put_clone = output_channel.clone();
        loop {
            match self.command_channel.recv().await {
                Ok(command) => {
                    match command {
                        NodeManagerCommand::Shutdown() => {
                            // TODO: graceful shutdown
                            // listener.abort();
                            listeners.iter().for_each(|listen| listen.abort());
                            break;
                        }
                        NodeManagerCommand::Connect(connect) => {
                            println!("Attempting to connect to {}", &connect.address);
                            let stream = TcpStream::connect(connect.address)
                                .await
                                .expect("Unable to connect!");
                            if !self.node_map.contains_key(&connect.address.ip()) {
                                let (send_to_bus, input_channel) = channel::<BytesMut>(200_000);

                                let (message_bus, handle) = MessageBus::new(send_to_bus);

                                self.node_map.insert(connect.address.ip(), handle);
                                let out = out_put_clone.clone();
                                tokio::spawn(async move {
                                    start_new_bus(message_bus, input_channel, out.clone()).await;
                                });
                            }

                            let handle = self
                                .node_map
                                .get(&connect.address.ip())
                                .expect("Not found!");
                            handle
                                .command_channel
                                .send(MessageBusCommand::AddConnection(AddConnection {
                                    address: connect.address,
                                    stream,
                                }))
                                .await
                                .expect("TODO: panic message");
                        }
                    }
                }
                Err(_) => break,
            }
        }
    }
}

fn spawn_listener(
    notifier: Sender<ConnectionNotification>,
    data_listener: TcpListener,
    node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    output_channel: Sender<BytesMut>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            match data_listener.accept().await {
                Ok((stream, address)) => {
                    println!("New connection request from {}", &address);
                    if !node_map.contains_key(&address.ip()) {
                        let (send_to_bus, input_channel) = channel::<BytesMut>(200_000);

                        let (message_bus, handle) = MessageBus::new(send_to_bus);

                        node_map.insert(address.ip(), handle);
                        let out = output_channel.clone();
                        tokio::spawn(async move {
                            start_new_bus(message_bus, input_channel, out.clone()).await;
                        });
                        notifier
                            .send(ConnectionNotification { address })
                            .await
                            .expect("Unable to send!");
                    }

                    let handle = node_map.get(&address.ip()).expect("Not found!");
                    handle
                        .command_channel
                        .send(MessageBusCommand::AddConnection(AddConnection {
                            address,
                            stream,
                        }))
                        .await
                        .expect("TODO: panic message");
                }
                Err(_) => break,
            }
        }
    })
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
