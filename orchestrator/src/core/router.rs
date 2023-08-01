use crate::core::hash_ring::HashRing;
use bytes::BytesMut;
use connection::connection_manager::ConnectionManagerHandle;
use connection::message_bus::{retrieve_response_channel, MessageBusHandle};
use connection::messages::{
    ArchivedRequest, ArchivedResponse, ChannelSubscribe, ChannelUnsubscribe, Request, Response,
    RouterCommand, RouterRequestWrapper,
};
use dashmap::DashMap;
use rkyv::string::ArchivedString;
use rkyv::Archived;
use std::net::IpAddr;
use std::sync::Arc;
use tachyonix::{channel, Receiver, Sender};
use tokio::task::JoinHandle;
use util::map_access_wrapper::arc_map_insert;
use uuid::Uuid;

pub struct Router {
    command_channel: Receiver<RouterCommand>,
    node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    channel_map: Arc<DashMap<ChannelId, Sender<BytesMut>>>,
    message_to_channel_map: Arc<DashMap<MessageId, ChannelId>>,
}

pub struct RouterHandle {
    pub command_channel: Sender<RouterCommand>,
}

pub struct RequestQueueProcessor {
    request_channel: Receiver<RouterRequestWrapper>,
    hash_ring: HashRing,
    channel_map: Arc<DashMap<ChannelId, Sender<BytesMut>>>,
    message_to_channel_map: Arc<DashMap<MessageId, ChannelId>>,
    node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    command_channel: Receiver<RouterCommand>,
}

pub struct ResponseQueueProcessor {
    response_channel: Receiver<BytesMut>,
    channel_map: Arc<DashMap<ChannelId, Sender<BytesMut>>>,
    message_to_channel_map: Arc<DashMap<MessageId, ChannelId>>,
}

impl Router {
    pub fn new(
        command_queue_sender: Sender<RouterCommand>,
        command_queue_receiver: Receiver<RouterCommand>,
        node_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    ) -> (Router, RouterHandle) {
        let channel_map = Arc::new(DashMap::new());
        let message_to_channel_map = Arc::new(DashMap::new());

        let router = Router {
            command_channel: command_queue_receiver,
            node_map,
            channel_map,
            message_to_channel_map,
        };
        let router_handle = RouterHandle {
            command_channel: command_queue_sender,
        };
        (router, router_handle)
    }

    pub async fn route(
        mut self,
        request_queue: Receiver<RouterRequestWrapper>,
        response_queue: Receiver<BytesMut>,
        mut connection_manager_handle: ConnectionManagerHandle,
    ) {
        let channel_map_ref = self.channel_map.clone();
        let message_to_channel_map_ref = self.message_to_channel_map.clone();
        let node_map_ref = self.node_map.clone();

        let (to_request_processor, request_processor_receiver) = channel(100);

        let mut request_processor = RequestQueueProcessor {
            request_channel: request_queue,
            hash_ring: HashRing::new(),
            channel_map: channel_map_ref,
            message_to_channel_map: message_to_channel_map_ref,
            node_map: node_map_ref,
            command_channel: request_processor_receiver,
        };

        let channel_map_ref = self.channel_map.clone();
        let message_to_channel_map_ref = self.message_to_channel_map.clone();

        let mut response_processor = ResponseQueueProcessor {
            response_channel: response_queue,
            channel_map: channel_map_ref,
            message_to_channel_map: message_to_channel_map_ref,
        };

        let request_processor_handle = tokio::spawn(async move {
            request_processor.process().await;
        });
        let response_processor_handle = tokio::spawn(async move {
            response_processor.process().await;
        });

        let command_channel = to_request_processor.clone();
        tokio::spawn(async move {
            loop {
                let notification = connection_manager_handle
                    .notification_receiver
                    .recv()
                    .await
                    .expect("Unable to receive!");
                command_channel
                    .send(RouterCommand::AddNode(notification.address.ip()))
                    .await
                    .expect("Unable to send AddNode request");
                println!("Node added to ring");
            }
        });

        loop {
            match self.command_channel.recv().await {
                Ok(command) => match command {
                    RouterCommand::Shutdown() => {
                        self.shutdown(request_processor_handle, response_processor_handle)
                            .await;
                        break;
                    }
                    RouterCommand::Subscribe(sub) => {
                        self.add_subscriber(sub);
                    }
                    RouterCommand::AddNode(address) => {
                        to_request_processor
                            .send(RouterCommand::AddNode(address))
                            .await
                            .expect("Unable to send AddNode request");
                    }
                    RouterCommand::Unsubscribe(unsub) => {
                        self.remove_subscriber(unsub);
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

    // TODO: Unsub message could fail to be sent in the case of a thread panic. Might be good to add a periodic job to purge stale subscriptions.
    fn remove_subscriber(&self, unsub: ChannelUnsubscribe) {
        self.channel_map.remove(&ChannelId::new(unsub.channel_id));
    }
}

impl RequestQueueProcessor {
    async fn process(&mut self) {
        loop {
            tokio::select! {
                command = self.command_channel.recv() => {
                    match command.expect("") {
                        RouterCommand::AddNode(address) => {
                            self.hash_ring.add_node(address)
                        }
                        _ => {}
                    }
                }

                message = self.request_channel.recv() => {
                    let message = message.expect("");
                    let buff = message.body;
                    let message_archive: &Archived<Request> =
                        rkyv::check_archived_root::<Request>(&buff[..]).unwrap();
                    let routing_info: Option<(&ArchivedString, &Uuid)> = match message_archive {
                        ArchivedRequest::Get(request) => Some((&request.key, &request.id)),
                        ArchivedRequest::Put(request) => Some((&request.key, &request.id)),
                        ArchivedRequest::Delete(request) => Some((&request.key, &request.id)),
                    };
                    match routing_info {
                        None => {
                            println!("Invalid message type received")
                        }
                        Some((key, req_id)) => {
                            let request_id = req_id.clone();
                            let key_owner = self
                                .hash_ring
                                .find_key_owner(key.as_str().into())
                                .clone();
                            match key_owner {
                                None => {
                                    println!("Unable to find address!")
                                }
                                Some(addr) => {
                                    match retrieve_response_channel(self.node_map.clone(), addr.clone()) {
                                        None => {
                                            println!("Response Channel for address {} not found", addr)
                                        }
                                        Some(response_channel) => {
                                            response_channel.send(buff).await.expect("Unable to send!");
                                        }
                                    }
                                    arc_map_insert(
                                        self.message_to_channel_map.clone(),
                                        MessageId::new(request_id),
                                        ChannelId::new(message.channel_id),
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

impl ResponseQueueProcessor {
    async fn process(&mut self) {
        loop {
            let buff = self.response_channel.recv().await.expect("Unable to read!");
            let message_archive: &Archived<Response> =
                rkyv::check_archived_root::<Response>(&buff[..]).unwrap(); // TODO this starts to run into errors when a high number of channels are open from one client
            let routing_info: Option<(&ArchivedString, &Uuid)> = match message_archive {
                ArchivedResponse::GetResponse(response) => Some((&response.key, &response.id)),
                ArchivedResponse::PutResponse(response) => Some((&response.key, &response.id)),
                ArchivedResponse::InvalidRequestResponse(response) => {
                    Some((&response.key, &response.id))
                }
                _ => None,
            };
            match routing_info {
                Some((_, response_id)) => {
                    match message_channel_lookup(
                        self.message_to_channel_map.clone(),
                        MessageId::new(response_id.clone()),
                    ) {
                        None => {
                            println!("Message ID not found!")
                        }
                        Some(channel_id) => {
                            match channel_map_lookup(self.channel_map.clone(), channel_id) {
                                None => {
                                    //TODO client is left waiting when this error occurs. Need to send an error back to the client
                                    println!("Channel ID not found!")
                                }
                                Some(sender) => {
                                    sender.send(buff).await.expect("Unable to send response!");
                                }
                            }
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

// Access to DashMap must be done from a synchronous function
fn message_channel_lookup(
    message_to_channel_map: Arc<DashMap<MessageId, ChannelId>>,
    message_id: MessageId,
) -> Option<ChannelId> {
    match message_to_channel_map.remove(&message_id) {
        None => None,
        Some((_, channel_id)) => Some(channel_id),
    }
}

// Access to DashMap must be done from a synchronous function
fn channel_map_lookup(
    channel_map: Arc<DashMap<ChannelId, Sender<BytesMut>>>,
    channel_id: ChannelId,
) -> Option<Sender<BytesMut>> {
    match channel_map.get(&channel_id) {
        None => None,
        Some(entry) => Some(entry.value().clone()),
    }
}

// Simple wrapper for a UUID to define what is being identified
#[derive(Eq, PartialEq, Hash, Clone)]
struct ChannelId {
    id: Uuid,
}
impl ChannelId {
    pub fn new(id: Uuid) -> ChannelId {
        ChannelId { id }
    }
}
// Simple wrapper for a UUID to define what is being identified
#[derive(Eq, PartialEq, Hash, Clone)]
struct MessageId {
    id: Uuid,
}
impl MessageId {
    pub fn new(id: Uuid) -> MessageId {
        MessageId { id }
    }
}
