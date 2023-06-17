use crate::core::hash_ring::HashRing;
use bytes::BytesMut;
use connection::message_bus::MessageBusHandle;
use connection::messages::{
    ArchivedMessage, ChannelSubscribe, Command, Message, RouterRequestWrapper,
};
use dashmap::DashMap;
use rkyv::string::ArchivedString;
use rkyv::Archived;
use std::net::IpAddr;
use std::sync::Arc;
use tachyonix::{Receiver, Sender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct Router {
    command_queue: Receiver<Command>,
    hash_ring: Arc<Mutex<HashRing>>,
    node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    channel_map: Arc<DashMap<ChannelId, Sender<BytesMut>>>,
    message_to_channel_map: Arc<DashMap<MessageId, ChannelId>>,
}

pub struct RouterHandle {
    command_queue: Sender<Command>,
}

pub struct RequestQueueProcessor {
    request_queue: Receiver<RouterRequestWrapper>,
    hash_ring: Arc<Mutex<HashRing>>,
    channel_map: Arc<DashMap<ChannelId, Sender<BytesMut>>>,
    message_to_channel_map: Arc<DashMap<MessageId, ChannelId>>,
    node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
}

pub struct ResponseQueueProcessor {
    response_queue: Receiver<BytesMut>,
    channel_map: Arc<DashMap<ChannelId, Sender<BytesMut>>>,
    message_to_channel_map: Arc<DashMap<MessageId, ChannelId>>,
}

impl Router {
    pub fn new(
        command_queue_sender: Sender<Command>,
        command_queue_receiver: Receiver<Command>,
        node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    ) -> (Router, RouterHandle) {
        let hash_ring = Arc::new(Mutex::new(HashRing::new()));
        let channel_map = Arc::new(DashMap::new());
        let message_to_channel_map = Arc::new(DashMap::new());

        let router = Router {
            command_queue: command_queue_receiver,
            hash_ring,
            node_map,
            channel_map,
            message_to_channel_map,
        };
        let router_handle = RouterHandle {
            command_queue: command_queue_sender,
        };
        (router, router_handle)
    }

    pub async fn route(
        mut self,
        request_queue: Receiver<RouterRequestWrapper>,
        response_queue: Receiver<BytesMut>,
    ) {
        let channel_map_ref = self.channel_map.clone();
        let message_to_channel_map_ref = self.message_to_channel_map.clone();
        let hash_ring_ref = self.hash_ring.clone();
        let node_map_ref = self.node_map.clone();

        let mut request_processor = RequestQueueProcessor {
            request_queue,
            hash_ring: hash_ring_ref,
            channel_map: channel_map_ref,
            message_to_channel_map: message_to_channel_map_ref,
            node_map: node_map_ref,
        };

        let channel_map_ref = self.channel_map.clone();
        let message_to_channel_map_ref = self.message_to_channel_map.clone();

        let mut response_processor = ResponseQueueProcessor {
            response_queue,
            channel_map: channel_map_ref,
            message_to_channel_map: message_to_channel_map_ref,
        };

        let request_processor_handle = tokio::spawn(async move {
            request_processor.process().await;
        });
        let response_processor_handle = tokio::spawn(async move {
            response_processor.process().await;
        });

        loop {
            match self.command_queue.recv().await {
                Ok(command) => match command {
                    Command::Shutdown() => {
                        self.shutdown(request_processor_handle, response_processor_handle)
                            .await;
                        break;
                    }
                    Command::Subscribe(sub) => {
                        self.add_subscriber(sub);
                    }
                },
                Err(_) => break,
            }
        }
    }

    async fn shutdown(
        &self,
        request_processor_handle: JoinHandle<()>,
        response_processor_handle: JoinHandle<()>,
    ) {
        // TODO graceful shutdown
        request_processor_handle.abort();
        response_processor_handle.abort();
    }

    fn add_subscriber(&self, sub: ChannelSubscribe) {
        self.channel_map
            .insert(ChannelId::new(sub.channel_id), sub.response_channel);
    }
}

impl RequestQueueProcessor {
    async fn process(&mut self) {
        loop {
            let message = self.request_queue.recv().await.expect("Unable to read!");
            let buff = message.body;
            let message_archive: &Archived<Message> =
                rkyv::check_archived_root::<Message>(&buff[..]).unwrap();
            let routing_info: Option<(&ArchivedString, &Uuid)> = match message_archive {
                ArchivedMessage::Get(request) => Some((&request.key, &request.id)),
                ArchivedMessage::Put(request) => Some((&request.key, &request.id)),
                ArchivedMessage::Delete(request) => Some((&request.key, &request.id)),
                _ => None,
            };
            match routing_info {
                Some(routing_info) => {
                    let request_id = routing_info.1.clone();
                    match self
                        .hash_ring
                        .lock()
                        .await
                        .find_key_owner(routing_info.0.as_str().into())
                    {
                        None => {
                            println!("Unable to find address!")
                        }
                        Some(addr) => {
                            {
                                let node_lane = self.node_map.get(&addr).expect("Node not found!");
                                node_lane
                                    .send_to_bus
                                    .send(buff)
                                    .await
                                    .expect("Unable to send!");
                            }
                            self.message_to_channel_map.insert(
                                MessageId::new(request_id),
                                ChannelId::new(message.channel_id),
                            );
                        }
                    }
                }
                None => {
                    println!("Invalid message type received")
                }
            }
        }
    }
}

impl ResponseQueueProcessor {
    async fn process(&mut self) {
        loop {
            let buff = self.response_queue.recv().await.expect("Unable to read!");
            let message_archive: &Archived<Message> =
                rkyv::check_archived_root::<Message>(&buff[..]).unwrap();
            let routing_info: Option<(&ArchivedString, &Uuid)> = match message_archive {
                ArchivedMessage::GetResponse(response) => Some((&response.key, &response.id)),
                ArchivedMessage::PutResponse(response) => Some((&response.key, &response.id)),
                _ => None,
            };
            match routing_info {
                Some(routing_info) => {
                    let response_id = routing_info.1;
                    let channel_id = self
                        .message_to_channel_map
                        .get(&MessageId::new(response_id.clone()))
                        .expect("Message ID not found!");
                    let sender = self
                        .channel_map
                        .get(&*channel_id)
                        .expect("Channel ID not found!");
                    sender.send(buff).await.expect("TODO: panic message");
                }
                None => {
                    println!("Invalid message type received")
                }
            }
        }
    }
}

// Simple wrapper for a UUID to define what is being identified
#[derive(Eq, PartialEq, Hash)]
struct ChannelId {
    id: Uuid,
}
impl ChannelId {
    pub fn new(id: Uuid) -> ChannelId {
        ChannelId { id }
    }
}
// Simple wrapper for a UUID to define what is being identified
#[derive(Eq, PartialEq, Hash)]
struct MessageId {
    id: Uuid,
}
impl MessageId {
    pub fn new(id: Uuid) -> MessageId {
        MessageId { id }
    }
}
