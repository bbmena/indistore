use bytes::BytesMut;
use rkyv::{with::CopyOptimize, Archive, Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use tachyonix::Sender;
use tokio::net::TcpStream;
use tokio::sync::oneshot::Sender as OneShotSender;
use uuid::Uuid;

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
// #[archive_attr(repr(C))]
pub struct GetRequest {
    pub id: Uuid,
    // Orchestrator IP, NOT client IP
    pub request_origin: IpAddr,
    pub key: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
// #[archive_attr(repr(C))]
pub struct PutRequest {
    pub id: Uuid,
    // Orchestrator IP, NOT client IP
    pub request_origin: IpAddr,
    pub key: String,
    #[with(CopyOptimize)]
    pub payload: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeleteRequest {
    pub id: Uuid,
    // Orchestrator IP, NOT client IP
    pub request_origin: IpAddr,
    pub key: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct GetResponse {
    pub id: Uuid,
    pub key: String,
    #[with(CopyOptimize)]
    pub payload: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct PutResponse {
    pub id: Uuid,
    pub key: String,
    pub success: bool,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct InvalidRequestResponse {
    pub id: Uuid,
    pub key: String,
}

pub trait RequestOrigin {
    fn request_origin(&self) -> IpAddr;
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub enum Request {
    Get(GetRequest),
    Put(PutRequest),
    Delete(DeleteRequest),
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub enum Response {
    GetResponse(GetResponse),
    PutResponse(PutResponse),
    DeleteResponse(),
    InvalidRequestResponse(InvalidRequestResponse),
}

impl RequestOrigin for Request {
    fn request_origin(&self) -> IpAddr {
        match self {
            Request::Get(get) => get.request_origin,
            Request::Put(put) => put.request_origin,
            Request::Delete(delete) => delete.request_origin,
        }
    }
}

impl RequestOrigin for ArchivedRequest {
    fn request_origin(&self) -> IpAddr {
        match self {
            ArchivedRequest::Get(get) => get
                .request_origin
                .deserialize(&mut rkyv::Infallible)
                .unwrap(),
            ArchivedRequest::Put(put) => put
                .request_origin
                .deserialize(&mut rkyv::Infallible)
                .unwrap(),
            ArchivedRequest::Delete(delete) => delete
                .request_origin
                .deserialize(&mut rkyv::Infallible)
                .unwrap(),
        }
    }
}

pub enum NodeManagerCommand {
    Shutdown(),
    Connect(Connect),
}

pub enum RouterCommand {
    Shutdown(),
    AddNode(IpAddr),
    Subscribe(ChannelSubscribe),
    Unsubscribe(ChannelUnsubscribe),
}

pub enum Command {
    Shutdown(),
}

pub enum MessageBusCommand {
    Shutdown(),
    AddConnection(AddConnection),
}

pub struct Connect {
    pub address: SocketAddr,
}

pub struct AddConnection {
    pub address: SocketAddr,
    pub stream: TcpStream,
}

pub struct OneShotMessage {
    pub id: Uuid,
    pub body: BytesMut,
    pub response_channel: OneShotSender<BytesMut>,
}

pub struct ChannelSubscribe {
    pub channel_id: Uuid,
    pub response_channel: Sender<BytesMut>,
}

pub struct ChannelUnsubscribe {
    pub channel_id: Uuid,
}

pub struct RouterRequestWrapper {
    pub channel_id: Uuid,
    pub body: BytesMut,
}

pub struct ConnectionNotification {
    pub address: SocketAddr,
}
