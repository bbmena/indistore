pub mod core;

use crate::core::data_store::DataStore;
use bytes::BytesMut;
use connection::connection_manager::{ConnectionManager, ConnectionManagerHandle};
use connection::messages::{Connect, NodeManagerCommand};
use dashmap::DashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tachyonix::channel;
use tokio::io;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    let orchestrator_listener_address =
        IpAddr::from(Ipv4Addr::new(127, 0, 0, 1));

    let ports: Vec<u16> = vec![2337, 2338, 2339];
    let node_map = Arc::new(DashMap::new());
    let node_map_clone = node_map.clone();

    let (orchestrator_manager, handle) =
        ConnectionManager::new(orchestrator_listener_address, node_map, ports);

    // give `to_router_from_node` to the NodeManager to clone and send to each MessageBus
    // give from_node_to_router to the data store to answer requests
    let (to_data_store_from_node, from_node_to_data_store) = channel::<BytesMut>(200_000);

    tokio::spawn(async move {
        orchestrator_manager.start(to_data_store_from_node).await;
    });

    startup(&handle).await;

    let (data_store, data_store_handle) = DataStore::new();

    data_store
        .start(from_node_to_data_store, node_map_clone)
        .await;

    Ok(())
}

async fn startup(handle: &ConnectionManagerHandle) {
    let node_listener_address = SocketAddr::new(IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)), 1338);
    handle
        .command_channel
        .send(NodeManagerCommand::Connect(Connect {
            address: node_listener_address,
        }))
        .await
        .expect("");

    let node_listener_address = SocketAddr::new(IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)), 1339);
    handle
        .command_channel
        .send(NodeManagerCommand::Connect(Connect {
            address: node_listener_address,
        }))
        .await
        .expect("");

    let node_listener_address = SocketAddr::new(IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)), 1340);
    handle
        .command_channel
        .send(NodeManagerCommand::Connect(Connect {
            address: node_listener_address,
        }))
        .await
        .expect("");
}
