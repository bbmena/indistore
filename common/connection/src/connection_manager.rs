use crate::message_bus::{MessageBus, MessageBusHandle};
use crate::messages::{AddConnection, MessageBusCommand, NodeManagerCommand};
use bytes::BytesMut;
use crossbeam_deque::Worker;
use dashmap::DashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use tachyonix::{channel, Receiver, RecvError, Sender};
use tokio::net::{TcpListener, TcpStream};

pub struct ConnectionManager {
    command_channel: Receiver<NodeManagerCommand>,
    address: SocketAddr,
    node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
}

pub struct NodeManagerHandle {
    command_channel: Sender<NodeManagerCommand>,
}

impl ConnectionManager {
    pub fn new(
        address: SocketAddr,
        node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    ) -> (ConnectionManager, NodeManagerHandle) {
        let (tx, rx) = channel(100);

        let node_listener = ConnectionManager {
            command_channel: rx,
            address,
            node_map,
        };

        let node_listener_handle = NodeManagerHandle {
            command_channel: tx,
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
                        if !node_map.contains_key(&address.ip()) {
                            let (send_to_bus, input_channel) = channel::<BytesMut>(1000);

                            let (message_bus, handle) = MessageBus::new(send_to_bus);

                            node_map.insert(address.ip(), handle);
                            let out = output_clone.clone();
                            tokio::spawn(async move {
                                start_new_bus(message_bus, input_channel, out.clone()).await;
                            });
                        }

                        let handle = node_map.get(&address.ip()).expect("Not found!");
                        handle
                            .command_channel
                            .send(MessageBusCommand::AddConnection(AddConnection {
                                address,
                                stream,
                            }));
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
                            let stream = TcpStream::connect(connect.address)
                                .await
                                .expect("Unable to connect!");
                            if !self.node_map.contains_key(&connect.address.ip()) {
                                let (send_to_bus, input_channel) = channel::<BytesMut>(1000);

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
                                }));
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
    let work_queue = Worker::new_fifo();
    let stealer = work_queue.stealer();

    message_bus
        .start(input_channel, output_channel, work_queue, stealer)
        .await;
}
