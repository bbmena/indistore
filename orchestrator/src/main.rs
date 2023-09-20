mod cli_listener;
pub mod core;
mod settings;

use bytes::BytesMut;
use connection::connection_manager::{ConnectionManager, ConnectionManagerHandle};
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tachyonix::channel;
use tokio::io::Result;

use crate::cli_listener::CliListener;
use crate::core::messages::{RouterCommand, RouterRequestWrapper};
use crate::core::router::RouterHandle;
use crate::core::server::ServerHandle;
use crate::core::{router::Router, server::Server};
use crate::settings::Settings;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    println!("Starting Orchestrator");

    let settings = Settings::new().unwrap();

    let cli_listener_address =
        SocketAddr::new(settings.listener_address, settings.cli.listener_port);
    let client_listener_address =
        SocketAddr::new(settings.listener_address, settings.server.listener_port);
    let node_listener_address = SocketAddr::new(
        settings.listener_address,
        settings.node_manager.listener_port,
    );

    let node_map = Arc::new(DashMap::new());

    // give `to_router_from_node` to the NodeManager to clone and send to each MessageBus
    let (to_router_from_node, from_node_to_router) = channel::<BytesMut>(200_000);

    // TODO ConnectionManager needs to be able to listen on multiple ports
    let node_manager_handle =
        ConnectionManagerHandle::new(node_listener_address, node_map.clone(), to_router_from_node);

    let (to_router, for_router) = channel::<RouterRequestWrapper>(200_000);
    let (command_queue_sender, command_queue_receiver) = channel::<RouterCommand>(1000);

    let router_handle = RouterHandle::new(
        command_queue_sender.clone(),
        command_queue_receiver,
        node_map.clone(),
        for_router,
        from_node_to_router,
        node_manager_handle,
    );

    let server_handle = ServerHandle::new(
        client_listener_address,
        to_router,
        command_queue_sender.clone(),
    );

    let cli_listener = CliListener::new(server_handle, router_handle);
    cli_listener.listen(cli_listener_address).await;

    Ok(())
}
