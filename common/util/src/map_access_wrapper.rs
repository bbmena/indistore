use dashmap::DashMap;
use std::hash::Hash;
use std::sync::Arc;

// Access to DashMap must be done from a synchronous function
pub fn map_insert<K, V>(map: &DashMap<K, V>, key: K, val: V)
where
    K: Eq + PartialEq + Hash,
{
    map.insert(key, val);
}

// Access to DashMap must be done from a synchronous function
pub fn arc_map_insert<K, V>(map: Arc<DashMap<K, V>>, key: K, val: V)
where
    K: Eq + PartialEq + Hash,
{
    map.insert(key, val);
}

// Access to DashMap must be done from a synchronous function
pub fn arc_map_contains_key<K, V>(map: Arc<DashMap<K, V>>, key: &K) -> bool
where
    K: Eq + PartialEq + Hash,
{
    map.contains_key(key)
}

// Access to DashMap must be done from a synchronous function
pub fn arc_map_remove<K, V>(map: Arc<DashMap<K, V>>, key: &K) -> Option<(K, V)>
where
    K: Eq + PartialEq + Hash,
{
    map.remove(key)
}
