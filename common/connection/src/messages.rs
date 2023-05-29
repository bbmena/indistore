use crate::message_bus::LaneCommunicator;
use bytes::BytesMut;
use rkyv::{Archive, Deserialize, Serialize};
use std::net::IpAddr;
use tachyonix::Sender;
use tokio::sync::oneshot::Sender as OneShotSender;
use uuid::Uuid;

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
// #[archive_attr(repr(C))]
pub struct GetRequest {
    pub id: Uuid,
    pub key: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
// #[archive_attr(repr(C))]
pub struct PutRequest {
    pub id: Uuid,
    pub key: String,
    pub payload: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct DeleteRequest {
    pub id: Uuid,
    pub key: String,
}

#[derive(Archive, Serialize, Deserialize, Debug, PartialEq)]
#[archive(compare(PartialEq), check_bytes)]
pub struct GetResponse {
    pub id: Uuid,
    pub key: String,
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
pub enum Message {
    Get(GetRequest),
    Put(PutRequest),
    Delete(DeleteRequest),
    GetResponse(GetResponse),
    PutResponse(PutResponse),
    DeleteResponse(),
}

pub enum Command {
    Shutdown(),
    AddNode(AddNode),
    Subscribe(ChannelSubscribe),
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

pub struct AddNode {
    pub address: IpAddr,
    pub handle: LaneCommunicator,
}

pub struct RouterRequestWrapper {
    pub channel_id: Uuid,
    pub body: BytesMut,
}
