use std::hash::Hash;
use std::sync::Arc;
use dashmap::DashMap;

// Access to DashMap must be done from a synchronous function
pub fn arc_map_insert<K, V>(map: Arc<DashMap<K, V>>, key: K, val: V) where K: Eq + PartialEq + Hash {
    map.insert(key, val);
}

// Access to DashMap must be done from a synchronous function
pub fn map_insert<K, V>(map: &DashMap<K, V>, key: K, val: V) where K: Eq + PartialEq + Hash {
    map.insert(key, val);
}