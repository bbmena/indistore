use dashmap::DashMap;
use serde::Deserialize;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter, ReadHalf, Result, WriteHalf};

use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::sync::oneshot::Sender;
use uuid::Uuid;

use connection::messages::{
    ArchivedResponse, GetRequest, GetResponse, InvalidRequestResponse, PutRequest, PutResponse,
    Request, Response,
};
use connection::{deserialize, serialize};

use rkyv::Deserialize as des;
use rkyv::{AlignedVec, Archived};
use util::map_access_wrapper::{arc_map_insert, arc_map_remove};

async fn read_loop(
    mut data_read_stream: BufReader<ReadHalf<TcpStream>>,
    response_map: Arc<DashMap<Uuid, Sender<Response>>>,
) {
    loop {
        match data_read_stream.read_u32().await {
            Ok(message_size) => {
                let mut vec = vec![0; message_size as usize];
                match data_read_stream.read_exact(&mut vec).await {
                    Ok(_) => {
                        let message_archive: &Archived<Response> =
                            rkyv::check_archived_root::<Response>(&vec).unwrap();

                        match message_archive {
                            ArchivedResponse::GetResponse(get) => {
                                let resp: GetResponse =
                                    get.deserialize(&mut rkyv::Infallible).unwrap();
                                let (_, sender) = arc_map_remove(response_map.clone(), &resp.id)
                                    .expect("Key not found!");
                                sender
                                    .send(Response::GetResponse(resp))
                                    .expect("Unable to send response!");
                            }
                            ArchivedResponse::PutResponse(put) => {
                                let resp: PutResponse =
                                    put.deserialize(&mut rkyv::Infallible).unwrap();
                                let (_, sender) = arc_map_remove(response_map.clone(), &resp.id)
                                    .expect("Key not found!");
                                sender
                                    .send(Response::PutResponse(resp))
                                    .expect("Unable to send response!");
                            }
                            ArchivedResponse::InvalidRequestResponse(invalid) => {
                                let resp: InvalidRequestResponse =
                                    invalid.deserialize(&mut rkyv::Infallible).unwrap();
                                let (_, sender) = arc_map_remove(response_map.clone(), &resp.id)
                                    .expect("Key not found!");
                                sender
                                    .send(Response::InvalidRequestResponse(resp))
                                    .expect("Unable to send response!");
                            }
                            _ => (),
                        };
                    }
                    Err(_) => break,
                }
            }
            Err(e) => {
                println!("{}", e);
                break;
            }
        }
    }
}

pub struct Client {
    orchestrator_addresses: Vec<SocketAddr>,
    current_orchestrator: SocketAddr,
    data_write_stream: BufWriter<WriteHalf<TcpStream>>,
    response_map: Arc<DashMap<Uuid, Sender<Response>>>,
}

impl Client {
    pub async fn init(current_orchestrator: SocketAddr) -> Result<Client> {
        match TcpStream::connect(current_orchestrator).await {
            Ok(stream) => {
                let sock_ref = socket2::SockRef::from(&stream);
                let mut alive = socket2::TcpKeepalive::new();
                alive = alive.with_time(Duration::from_secs(20));
                alive = alive.with_interval(Duration::from_secs(20));

                sock_ref
                    .set_tcp_keepalive(&alive)
                    .expect("Can't keep alive");
                let (read, write) = tokio::io::split(stream);
                let response_map = Arc::new(DashMap::new());

                let clone = response_map.clone();
                tokio::spawn(async move {
                    read_loop(BufReader::new(read), clone).await;
                });

                Ok(Client {
                    orchestrator_addresses: vec![current_orchestrator],
                    current_orchestrator,
                    data_write_stream: BufWriter::new(write),
                    response_map,
                })
            }
            Err(e) => Err(e),
        }
    }

    pub async fn put<T>(&mut self, key: String, val: T) -> Result<bool>
    where
        T: serde::ser::Serialize,
    {
        let req_id = Uuid::new_v4();
        let payload = serialize(val);
        let request = Request::Put(PutRequest {
            id: req_id.clone(),
            request_origin: self.current_orchestrator.ip(),
            key,
            payload,
        });
        let buff = rkyv::to_bytes::<_, 2048>(&request).unwrap();
        let (sender, receiver) = oneshot::channel::<Response>();
        arc_map_insert(self.response_map.clone(), req_id, sender);

        self.send_request(buff).await;

        match receiver.await.expect("Receiver Error!") {
            Response::PutResponse(_) => Ok(true),
            _ => {
                println!("Unsupported response to request type");
                Err(Error::new(
                    ErrorKind::Unsupported,
                    "Unsupported response to request",
                ))
            }
        }
    }

    pub async fn get<T>(&mut self, key: String) -> Result<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let req_id = Uuid::new_v4();
        let request = Request::Get(GetRequest {
            id: req_id.clone(),
            request_origin: self.current_orchestrator.ip(),
            key,
        });
        let buff = rkyv::to_bytes::<_, 2048>(&request).expect("Can't serialize!");
        let (sender, receiver) = oneshot::channel::<Response>();
        arc_map_insert(self.response_map.clone(), req_id, sender);

        self.send_request(buff).await;

        match receiver.await.expect("Receiver Error!") {
            Response::GetResponse(resp) => Ok(deserialize(resp.payload)),
            Response::InvalidRequestResponse(resp) => Err(Error::new(
                ErrorKind::NotFound,
                format!("Value for key '{}' not found", resp.key),
            )),
            _ => {
                println!("Unsupported response type");
                Err(Error::new(
                    ErrorKind::Unsupported,
                    "Unsupported response to request type",
                ))
            }
        }
    }

    #[inline(always)]
    async fn send_request(&mut self, buff: AlignedVec) {
        self.data_write_stream
            .write_u32(buff.len() as u32)
            .await
            .expect("Unable to write length to data stream!");
        self.data_write_stream
            .write_all(&buff)
            .await
            .expect("Unable to write buffer to data stream!");
        self.data_write_stream
            .flush()
            .await
            .expect("Unable to flush data stream!");
    }
}
