use std::iter::Map;
use std::sync::Arc;
use axum::extract::State;
use axum::Router;
use axum::routing::get;
use tokio::io;
use tokio::sync::mpsc::{Receiver, Sender};

pub struct ISMessage {

}

// handler should submit a request and then poll the state for a response. might be good to use evmap instead
struct AppState {
    input_channel: Receiver<ISMessage>,
    output_channel: Sender<ISMessage>,
    response_map: Map<String, String>
}

impl AppState {

}

pub async fn serve(input_channel: Receiver<ISMessage>, output_channel: Sender<ISMessage>,) -> io::Result<()> {

    let shared_state = Arc::new(AppState { input_channel, output_channel });

    let router = Router::new()
        .route("/", get(handler))
        .with_state(shared_state);

    axum::Server::bind(&addr.parse().unwrap())
        .serve(router.into_make_service())
        .await
        .unwrap();

    Ok(())
}

async fn handler(
    State(state): State<Arc<AppState>>,
) {
    state.output_channel.send();
    state.input_channel.poll_recv()
}