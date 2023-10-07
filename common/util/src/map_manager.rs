use crate::map_access_wrapper::{map_insert, map_remove};
use dashmap::DashMap;
use std::hash::Hash;
use std::sync::Arc;
use tachyonix::{channel, Receiver, Sender};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

pub struct MapManager<K: Eq + PartialEq + Hash + Send, V: Send> {
    command_channel: Receiver<MapManagerCommand<K, V>>,
    map: DashMap<K, V>,
}

pub struct MapManagerHandle<K: Eq + PartialEq + Hash + Send, V: Send> {
    pub command_channel: Sender<MapManagerCommand<K, V>>,
    map_manager_task: JoinHandle<()>,
}

impl<K: Eq + PartialEq + Hash + Send + 'static, V: Send + 'static> MapManagerHandle<K, V> {
    pub fn new(kill_switch: Arc<Notify>) -> MapManagerHandle<K, V> {
        let map = DashMap::new();
        let (tx, rx) = channel::<MapManagerCommand<K, V>>(100);

        let map_manager = MapManager {
            command_channel: rx,
            map,
        };

        let map_manager_task = tokio::spawn(async move {
            map_manager.start(kill_switch).await;
        });

        MapManagerHandle {
            command_channel: tx,
            map_manager_task,
        }
    }

    // Sending a shutdown command will put the command in queue behind whatever other commands have already been sent
    // Use this to end the task more directly
    pub fn end_task(&self) {
        self.map_manager_task.abort()
    }
}

impl<K: Eq + PartialEq + Hash + Send, V: Send> MapManager<K, V> {
    async fn start(mut self, kill_switch: Arc<Notify>) {
        while let Ok(command) = self.command_channel.recv().await {
            match command {
                MapManagerCommand::Shutdown() => break,
                MapManagerCommand::AddValue(key, value) => {
                    map_insert(&self.map, key, value);
                }
                MapManagerCommand::RemoveValue(key) => {
                    map_remove(&self.map, &key);

                    // If all connections to a client have been closed, the client must have disconnected so we can close the bus
                    if self.map.is_empty() {
                        kill_switch.notify_one();
                        break;
                    }
                }
            }
        }
    }
}

pub enum MapManagerCommand<K: Eq + PartialEq + Hash + Send, V: Send> {
    Shutdown(),
    AddValue(K, V),
    RemoveValue(K),
}

pub struct MapManagerAddPayload<K: Eq + PartialEq + Hash + Send, V: Send> {
    key: K,
    value: V,
}
