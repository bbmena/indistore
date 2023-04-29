mod api;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::io;
use tokio::sync::mpsc;
use connection::message_bus;
use connection::message_bus::MessageBus;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
    println!("Hello from orchestrator!");
    let sa = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1337);
    let addresses: Vec<SocketAddr> = vec![sa];
    let (bus_to_lane, mut lane_from_bus) = mpsc::channel(32);
    let (lane_to_bus, mut bus_from_lane) = mpsc::channel(32);
    let mb = MessageBus::init(addresses, lane_from_bus, lane_to_bus );
    Ok(())
}
