use sha3::digest::Output;
use sha3::{Digest, Sha3_256};
use std::cmp::Ordering;
use std::net::IpAddr;
use std::sync::Mutex;

pub struct HashRing {
    nodes: Vec<RingNode>,
    hasher: Mutex<Sha3_256>,
}

impl HashRing {
    pub fn new() -> HashRing {
        let hasher = Mutex::new(Sha3_256::new());
        HashRing {
            nodes: Vec::new(),
            hasher,
        }
    }

    pub fn add_node(&mut self, address: IpAddr) {
        let hash = {
            let mut hasher = self
                .hasher
                .lock()
                .expect("Unable to acquire lock on Hasher!");
            hasher.update(address.to_string());
            hasher.finalize_reset()
        };
        let node = RingNode { hash, address };

        self.nodes.push(node);
        self.nodes.sort()
    }

    pub fn remove_node(&mut self, node: &RingNode) {
        match self.nodes.binary_search(node) {
            Ok(idx) => {
                self.nodes.remove(idx);
            }
            Err(_) => {
                println!("Node not found")
            }
        }
    }

    pub fn find_key_owner(&self, key: String) -> Option<IpAddr> {
        if self.nodes.is_empty() {
            return None;
        }

        let hash = {
            let mut hasher = self
                .hasher
                .lock()
                .expect("Unable to acquire lock on Hasher!");
            hasher.update(key);
            hasher.finalize_reset()
        };

        let index = match self.nodes.binary_search_by(|node| node.hash.cmp(&hash)) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        if index == self.nodes.len() {
            return Some(self.nodes[0].address.clone());
        }

        Some(self.nodes[index].address.clone())
    }
}

pub struct RingNode {
    hash: Output<Sha3_256>,
    address: IpAddr,
}

impl RingNode {
    pub fn new(address: IpAddr) -> RingNode {
        let mut hasher = Sha3_256::new();
        hasher.update(address.to_string());
        RingNode {
            hash: hasher.finalize(),
            address,
        }
    }
}

impl PartialEq for RingNode {
    fn eq(&self, other: &RingNode) -> bool {
        self.hash == other.hash
    }
}

impl Eq for RingNode {}

impl PartialOrd for RingNode {
    fn partial_cmp(&self, other: &RingNode) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RingNode {
    fn cmp(&self, other: &RingNode) -> Ordering {
        self.hash.cmp(&other.hash)
    }
}
