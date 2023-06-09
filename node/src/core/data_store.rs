use bytes::BytesMut;
use connection::message_bus::{retrieve_response_channel, MessageBusHandle};
use connection::messages::{
    ArchivedDeleteRequest, ArchivedGetRequest, ArchivedPutRequest, ArchivedRequest, Command,
    GetResponse, InvalidResponse, PutResponse, Request, RequestOrigin, Response,
};
use dashmap::DashMap;
use rkyv::Archived;
use std::net::IpAddr;
use std::sync::Arc;
use tachyonix::{channel, Receiver, Sender};

pub struct DataStore {
    data: Arc<DashMap<String, BytesMut>>,
    command_channel: Receiver<Command>,
}

pub struct DataStoreHandle {
    command_channel: Sender<Command>,
}

impl DataStore {
    pub fn new() -> (DataStore, DataStoreHandle) {
        let (tx, rx) = channel(100);
        let data_store = DataStore {
            data: Arc::new(DashMap::new()),
            command_channel: rx,
        };

        let data_store_handle = DataStoreHandle {
            command_channel: tx,
        };

        (data_store, data_store_handle)
    }

    pub async fn start(
        mut self,
        request_channel: Receiver<BytesMut>,
        channel_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    ) {
        let (tx, rx) = channel(100);

        let request_handler = DataStoreRequestHandler {
            command_channel: rx,
        };
        let data = self.data.clone();
        tokio::spawn(async move {
            request_handler
                .start(request_channel, data, channel_map)
                .await;
        });
        println!("Ready to begin processing requests");
        loop {
            match self.command_channel.recv().await {
                Ok(command) => {
                    match command {
                        Command::Shutdown() => {
                            // TODO graceful shutdown
                            tx.send(Command::Shutdown()).await.expect("");
                            break;
                        }
                    }
                }
                Err(_) => break,
            }
        }
    }
}

pub struct DataStoreRequestHandler {
    command_channel: Receiver<Command>,
}

fn handle_get(
    request: &ArchivedGetRequest,
    data: Arc<DashMap<String, BytesMut>>,
) -> Option<Response> {
    match data.get(request.key.as_str()) {
        None => Some(Response::InvalidResponse(InvalidResponse {
            id: request.id,
            key: request.key.to_string(),
        })),
        Some(value) => Some(Response::GetResponse(GetResponse {
            id: request.id,
            key: request.key.to_string(),
            payload: Vec::from(value.value().clone()),
        })),
    }
}

fn handle_put(
    request: &ArchivedPutRequest,
    data: Arc<DashMap<String, BytesMut>>,
) -> Option<Response> {
    let bytes = BytesMut::from(&request.payload[..]);
    data.insert(request.key.to_string(), bytes);
    Some(Response::PutResponse(PutResponse {
        id: request.id,
        key: request.key.to_string(),
        success: true,
    }))
}

// Basically just an ack at this point
fn handle_delete(
    request: &ArchivedDeleteRequest,
    data: Arc<DashMap<String, BytesMut>>,
) -> Option<Response> {
    data.remove(&request.key.to_string());
    Some(Response::DeleteResponse())
}

// fn retrieve_response_channel(
//     channel_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
//     origination_address: IpAddr,
// ) -> Sender<BytesMut> {
//     let response_channel = channel_map
//         .get(&origination_address)
//         .expect("Channel not found");
//     response_channel.send_to_bus.clone()
// }

impl DataStoreRequestHandler {
    pub async fn start(
        mut self,
        mut request_channel: Receiver<BytesMut>,
        data: Arc<DashMap<String, BytesMut>>,
        channel_map: Arc<DashMap<IpAddr, MessageBusHandle>>,
    ) {
        tokio::spawn(async move {
            loop {
                let message = request_channel.recv().await.expect("Unable to receive!");
                let message_archive: &Archived<Request> =
                    rkyv::check_archived_root::<Request>(&message[..]).unwrap();

                let origination_address = message_archive.request_origin();

                // Any interaction with DashMap needs to be wrapped in a sync function. Async access can cause deadlock
                let response = match message_archive {
                    ArchivedRequest::Get(request) => handle_get(request, data.clone()),
                    ArchivedRequest::Put(request) => handle_put(request, data.clone()),
                    ArchivedRequest::Delete(request) => handle_delete(request, data.clone()),
                };

                match response {
                    None => {}
                    Some(resp) => {
                        let buff = rkyv::to_bytes::<_, 1024>(&resp).expect("Can't serialize!");
                        let bytes = BytesMut::from(&buff[..]);
                        // Any interaction with DashMap needs to be wrapped in a sync function. Async access can cause deadlock
                        let mut response_channel =
                            retrieve_response_channel(channel_map.clone(), origination_address);
                        response_channel.send(bytes).await.expect("Unable to send!");
                        drop(response_channel);
                    }
                }
            }
        });

        loop {
            match self.command_channel.recv().await {
                Ok(command) => {
                    match command {
                        // TODO
                        Command::Shutdown() => break,
                    }
                }
                Err(_) => break,
            }
        }
    }
}
