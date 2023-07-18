use std::collections::HashMap;
use uuid::Uuid;
use rand;
use rand::Rng;
use std::fs;
use serde::{Deserialize, Serialize};
use rmp_serde::Deserializer;

#[derive(Serialize, Deserialize, Debug)]
pub struct Message<T>
    where T: serde::ser::Serialize
{
    id: String,
    val: T
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SmallSubObject {
    field_1: u32,
    field_2: String,
    field_3: String,
    field_4: u16
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SomeData {
    id: Uuid,
    name: String,
    phone: String,
    ssn: String,
    account_balance: u32,
    address: String,
    field_a: String,
    field_b: String,
    a_number: u32,
    sub_objects: Vec<SmallSubObject>,
    more_info: String,
    a_map: HashMap<String, String>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SomeDataWrapper {
    meta_field: String,
    another_field: String,
    a_number: u32,
    a_big_number: u64,
    array_of_data: Vec<SomeData>
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Something {
    pub(crate) key: String,
    pub(crate) val: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Complex {
    pub(crate) key: String,
    pub(crate) val: Something
}

pub fn create_small_object() -> SomeData {
    let mut sub_objects = Vec::new();

    for n in 0..4 {
        sub_objects.insert(n, SmallSubObject{
            field_1: (n * 5000) as u32,
            field_2: "a short string".to_string(),
            field_3: "a slightly longer, but still short string".to_string(),
            field_4: (n * 10) as u16
        })
    }

    let mut a_map = HashMap::new();
    for n in 0..10 {
        a_map.insert(n.to_string(), (n * rand::thread_rng().gen_range(0..100)).to_string());
    }

    SomeData{
        id: Uuid::new_v4(),
        name: "some name".to_string(),
        phone: "123-456-7890".to_string(),
        ssn: "546-25-8567".to_string(),
        account_balance: 1000,
        address: "123 Nowhere Street".to_string(),
        field_a: "some short string".to_string(),
        field_b: "some other string".to_string(),
        a_number: 12345678,
        sub_objects,
        more_info: "some more info here about the object".to_string(),
        a_map
    }
}

pub fn create_medium_object() -> SomeDataWrapper {
    let mut array_of_data = Vec::new();

    for n in 0..10 {
        array_of_data.insert(n, create_small_object());
    }

    SomeDataWrapper{
        meta_field: "This is so meta".to_string(),
        another_field: "Here is another field of data".to_string(),
        a_number: 5678,
        a_big_number: 12345678901234,
        array_of_data
    }
}

pub fn create_large_object() -> SomeDataWrapper {
    let mut array_of_data = Vec::new();

    for n in 0..100 {
        array_of_data.insert(n, create_small_object());
    }

    SomeDataWrapper{
        meta_field: "This is so meta".to_string(),
        another_field: "Here is another field of data".to_string(),
        a_number: 5678,
        a_big_number: 12345678901234,
        array_of_data
    }
}