// #![deny(missing_docs)]
//! This is key-value store lib
pub mod client;
pub mod my_engine;
pub mod protocol;
pub mod server;
pub mod sled_engine;
pub mod thread_pool;

use failure;
use std::error::Error;
use std::fmt;
use std::result;

pub type Result<T> = result::Result<T, failure::Error>;

pub trait KvsEngine: Clone + Send + 'static {
    fn set(&self, key: String, value: String) -> Result<()>;

    fn get(&self, key: String) -> Result<Option<String>>;

    fn remove(&self, key: String) -> Result<()>;
}

#[derive(Debug)]
pub enum MyErr {
    KeyNotFound,
    InvalidArg,
    ErrExtension,
    WrongEngine,
    FileNotFound(u32),
}

impl fmt::Display for MyErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            MyErr::KeyNotFound => write!(f, "Key not found"),
            MyErr::InvalidArg => write!(f, "Invalid argument"),
            MyErr::ErrExtension => write!(f, "Unexpected file extension"),
            MyErr::WrongEngine => write!(f, "Wrong engine detected"),
            MyErr::FileNotFound(id) => write!(f, "File handle {} not found", id),
        }
    }
}

impl Error for MyErr {}
