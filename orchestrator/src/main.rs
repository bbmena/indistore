pub mod core;

use bytes::BytesMut;
use connection::messages::{RouterCommand, RouterRequestWrapper};
use dashmap::DashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
// use tachyonix::channel;
use tokio::io;

extern crate rmp_serde as rmps;

use crate::core::router::Router;
use crate::core::server::Server;
use connection::connection_manager::ConnectionManager;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::channel;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    println!("Hello from orchestrator!");

    let data_address = SocketAddr::new(IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)), 1337);
    let node_listener_address = SocketAddr::new(IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)), 1338);
    let node_map = Arc::new(DashMap::new());

    // TODO ConnectionManager needs to be able to listen on multiple ports
    let (node_manager, node_manager_handle) =
        ConnectionManager::new(node_listener_address, node_map.clone());

    let (to_router, for_router) = channel::<RouterRequestWrapper>(200_000);
    let (command_queue_sender, command_queue_receiver) = channel::<RouterCommand>(1000);

    let (router, router_handle) = Router::new(
        command_queue_sender.clone(),
        command_queue_receiver,
        node_map.clone(),
    );

    // give `to_router_from_node` to the NodeManager to clone and send to each MessageBus
    let (to_router_from_node, from_node_to_router) = channel::<BytesMut>(200_000);

    tokio::spawn(async move {
        node_manager.start(to_router_from_node).await;
    });

    tokio::spawn(async move {
        router
            .route(for_router, from_node_to_router, node_manager_handle)
            .await;
    });

    let (server, server_handle) = Server::new(data_address, to_router);
    server
        .serve(command_queue_sender.clone())
        .await
        .expect("Unable to start server!");

    Ok(())
}

#[derive(Serialize, Deserialize, Debug)]
struct Something {
    key: String,
    val: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Complex {
    key: String,
    val: Something,
}
