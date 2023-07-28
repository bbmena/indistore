pub mod core;

use bytes::BytesMut;
use connection::messages::{RouterCommand, RouterRequestWrapper};
use dashmap::DashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tachyonix::channel;
use tokio::io::Result;
use tokio::net::TcpListener;

use crate::core::router::Router;
use crate::core::server::Server;
use connection::connection_manager::ConnectionManager;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    println!("Hello from orchestrator!");

    let cli_address = SocketAddr::new(IpAddr::from(Ipv4Addr::new(127, 0, 0, 1)), 1337);

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

    tokio::spawn(async move {
        server
            .serve(command_queue_sender.clone())
            .await
            .expect("Unable to start server!");
    });

    let cli_listener = TcpListener::bind(cli_address).await.unwrap();

    loop{
        match cli_listener.accept().await {
            Ok(stream) => {
                // TODO implement command receiver
            }
            Err(_) => { break }
        }
    }

    Ok(())
}
