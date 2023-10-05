use crate::message_bus::{ConnectionLaneHandle, MessageBusHandle};
use rkyv::{with::CopyOptimize, Archive, Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use tokio::net::TcpStream;
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

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub enum CliCommand {
    Shutdown(),
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

pub enum ConnectionManagerCommand {
    Shutdown(),
    Connect(MessageBusAddress),
    RemoveConnection(MessageBusAddress),
}

pub enum Command {
    Shutdown(),
}

pub enum MessageBusCommand {
    Shutdown(),
    AddConnection(MessageBusConnection),
    RemoveConnection(SocketAddr),
    AddHandle(SocketAddr, ConnectionLaneHandle),
}

pub struct MessageBusAddress {
    pub address: SocketAddr,
}

pub struct MessageBusConnection {
    pub address: SocketAddr,
    pub stream: TcpStream,
}

pub struct ConnectionNotification {
    pub address: SocketAddr,
}
