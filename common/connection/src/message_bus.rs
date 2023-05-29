use bytes::BytesMut;
use std::collections::HashMap;
use std::net::SocketAddr;
use tachyonix::{channel, Receiver, Sender};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter,
    ReadHalf, WriteHalf,
};
use tokio::net::TcpStream;

use crate::messages::Message;

/**
   UNINITIATED - Not yet connected
   HEALTHY - No issue
   UNHEALTHY - More than two missed health-check
**/
enum LaneHealth {
    UNINITIATED,
    HEALTHY,
    UNHEALTHY,
}

/**
    HEALTHY - All lanes in healthy state
    PARTIAL - One lane in unhealthy or uninitiated state
    UNHEALTHY - More than one lane in unhealthy or uninitiated state
**/
#[derive(Copy, Clone)]
pub enum MessageBusHealth {
    HEALTHY,
    PARTIAL,
    UNHEALTHY,
}

pub struct ConnectionLane {
    data_read_stream: BufReader<ReadHalf<TcpStream>>,
    data_write_stream: BufWriter<WriteHalf<TcpStream>>,
    lane_health: LaneHealth,
    input_channel: Receiver<BytesMut>,
    output_channel: Sender<BytesMut>,
    port: u16,
}

impl ConnectionLane {
    pub fn new(
        stream: TcpStream,
        input_channel: Receiver<BytesMut>,
        output_channel: Sender<BytesMut>,
    ) -> ConnectionLane {
        let port = stream.local_addr().expect("No address!").port();
        let (read, write) = tokio::io::split(stream);
        // match write.poll_write() {
        //     Poll::Ready(_) => {}
        //     Poll::Pending => {}
        // };

        ConnectionLane {
            data_read_stream: BufReader::new(read),
            data_write_stream: BufWriter::new(write),
            lane_health: LaneHealth::HEALTHY,
            input_channel,
            output_channel,
            port,
        }
    }

    pub async fn start(self) {}
}

pub struct LaneCommunicator {
    pub write_to_lane: Sender<BytesMut>,
    read_from_lane: Receiver<BytesMut>,
    port: u16,
    lane_health: LaneHealth,
}

pub struct MessageBus {
    // mapped by port number
    connection_lanes: HashMap<u16, LaneCommunicator>,
    input_channel: Receiver<BytesMut>,
    output_channel: Sender<BytesMut>,
    message_bus_health: MessageBusHealth,
}

/**
   A messaging channel for inter-node communication. Makes use of multiple (three) streams to avoid blocking
**/
impl MessageBus {
    // To be used when trying to initiate a connection with a remote service
    pub async fn init(
        lane_addresses: Vec<SocketAddr>,
        input_channel: Receiver<BytesMut>,
        output_channel: Sender<BytesMut>,
    ) -> MessageBus {
        let mut connection_lanes: HashMap<u16, LaneCommunicator> = HashMap::new();
        let mut lanes_to_start: Vec<ConnectionLane> = Vec::with_capacity(lane_addresses.capacity());
        for addr in lane_addresses.into_iter() {
            match TcpStream::connect(addr).await {
                Ok(stream) => {
                    let (bus_to_lane, lane_from_bus) = channel(32);
                    let (lane_to_bus, bus_from_lane) = channel(32);
                    let connection_lane = ConnectionLane::new(stream, lane_from_bus, lane_to_bus);
                    let lane_communicator = LaneCommunicator {
                        write_to_lane: bus_to_lane,
                        read_from_lane: bus_from_lane,
                        port: addr.port(),
                        lane_health: LaneHealth::HEALTHY,
                    };
                    connection_lanes.insert(addr.port(), lane_communicator);
                    lanes_to_start.push(connection_lane);
                }
                Err(e) => {}
            }
        }

        for lane in lanes_to_start.into_iter() {
            tokio::spawn(async move { lane.start().await });
        }

        MessageBus {
            connection_lanes,
            input_channel,
            output_channel,
            message_bus_health: MessageBusHealth::HEALTHY,
        }
    }

    // To be used when accepting a connection from a remote service
    pub fn new(
        lanes_to_start: Vec<ConnectionLane>,
        connection_lane_comms: Vec<LaneCommunicator>,
        input_channel: Receiver<BytesMut>,
        output_channel: Sender<BytesMut>,
    ) -> MessageBus {
        let mut connection_lanes: HashMap<u16, LaneCommunicator> = HashMap::new();

        for lane_com in connection_lane_comms.into_iter() {
            connection_lanes.insert(lane_com.port, lane_com);
        }

        for lane in lanes_to_start.into_iter() {
            tokio::spawn(async move { lane.start().await });
        }

        MessageBus {
            connection_lanes,
            input_channel,
            output_channel,
            message_bus_health: MessageBusHealth::HEALTHY,
        }
    }

    pub fn start(&self) {}

    pub fn health(&self) -> MessageBusHealth {
        self.message_bus_health.clone()
    }
}
