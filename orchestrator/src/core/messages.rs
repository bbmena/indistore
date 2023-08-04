use bytes::BytesMut;
use std::net::IpAddr;
use tachyonix::Sender;
use uuid::Uuid;

use tokio::sync::oneshot::Sender as OneshotSender;

pub enum RouterCommand {
    Shutdown(),
    AddNode(IpAddr),
    Subscribe(ChannelSubscribe),
    Unsubscribe(ChannelUnsubscribe),
}

pub enum ServerCommand {
    Shutdown(),
    RemoveServerConnection(Uuid),
}

pub struct RouterRequestWrapper {
    pub channel_id: Uuid,
    pub body: BytesMut,
}

pub struct ChannelSubscribe {
    pub channel_id: Uuid,
    pub response_channel: Sender<BytesMut>,
    pub subscribe_acknowledge: OneshotSender<Option<Uuid>>,
}

pub struct ChannelUnsubscribe {
    pub channel_id: Uuid,
}
