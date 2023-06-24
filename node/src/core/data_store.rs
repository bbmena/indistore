use bytes::BytesMut;
use connection::message_bus::MessageBusHandle;
use connection::messages::{
    ArchivedRequest, Command, GetResponse, InvalidResponse, PutResponse, Request, RequestOrigin,
    Response,
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

                let response = match message_archive {
                    ArchivedRequest::Get(request) => match data.get(request.key.as_str()) {
                        None => Some(Response::InvalidResponse(InvalidResponse {
                            id: request.id,
                            key: request.key.to_string(),
                        })),
                        Some(value) => Some(Response::GetResponse(GetResponse {
                            id: request.id,
                            key: request.key.to_string(),
                            payload: Vec::from(value.value().clone()),
                        })),
                    },
                    ArchivedRequest::Put(request) => {
                        let bytes = BytesMut::from(&request.payload[..]);
                        data.insert(request.key.to_string(), bytes);
                        Some(Response::PutResponse(PutResponse {
                            id: request.id,
                            key: request.key.to_string(),
                            success: true,
                        }))
                    }
                    ArchivedRequest::Delete(_) => Some(Response::DeleteResponse()),
                };

                match response {
                    None => {}
                    Some(resp) => {
                        let buff = rkyv::to_bytes::<_, 1024>(&resp).expect("Can't serialize!");
                        let bytes = BytesMut::from(&buff[..]);
                        let response_channel = channel_map
                            .get(&origination_address)
                            .expect("Channel not found");
                        response_channel
                            .send_to_bus
                            .send(bytes)
                            .await
                            .expect("Unable to send!");
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
