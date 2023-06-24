pub mod connection_manager;
pub mod message_bus;
pub mod messages;

use rmp_serde::Deserializer;
use serde::{Deserialize, Serialize};

pub fn serialize<T>(val: T) -> Vec<u8>
where
    T: serde::ser::Serialize,
{
    let mut buff = Vec::new();
    rmp_serde::encode::write_named(&mut buff, &val).expect("Unable to serialize!");
    buff
}

// pub fn deserialize<'de, T>(buff: &'de [u8]) -> T
// where
//     T: Deserialize<'de>,
// {
//     let mut de = Deserializer::from_read_ref(buff);
//     Deserialize::deserialize(&mut de).expect("Unable to deserialize!")
// }

pub fn deserialize<T>(buff: Vec<u8>) -> T
where
    T: for<'a> Deserialize<'a>,
{
    let mut de = Deserializer::from_read_ref(&buff);
    Deserialize::deserialize(&mut de).expect("Unable to deserialize!")
}
