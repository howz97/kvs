// #![deny(missing_docs)]
//! This is key-value store lib
pub mod my_engine;
pub mod protocol;
pub mod sled_engine;

use failure;
use std::error::Error;
use std::fmt;
use std::result;

pub type Result<T> = result::Result<T, failure::Error>;

pub trait KvsEngine {
    fn set(&mut self, key: String, val: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}

#[derive(Debug)]
pub enum MyErr {
    KeyNotFound,
    InvalidArg,
    ErrExtension,
}

impl fmt::Display for MyErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            MyErr::KeyNotFound => write!(f, "Key not found"),
            MyErr::InvalidArg => write!(f, "Invalid argument"),
            MyErr::ErrExtension => write!(f, "Unexpected file extension"),
        }
    }
}

impl Error for MyErr {}
