// #![deny(missing_docs)]
//! This is key-value store lib
pub mod client;
pub mod engine;
pub mod protocol;
pub mod server;
pub mod thread_pool;

pub use engine::{KvStore, KvsEngine, SledKvsEngine};
use failure;
use std::error::Error;
use std::fmt;
use std::result;

pub type Result<T> = result::Result<T, failure::Error>;

#[derive(Debug)]
pub enum MyErr {
    KeyNotFound,
    ErrExtension,
    WrongEngine,
}

impl fmt::Display for MyErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            MyErr::KeyNotFound => write!(f, "Key not found"),
            MyErr::ErrExtension => write!(f, "Unexpected file extension"),
            MyErr::WrongEngine => write!(f, "Wrong engine detected"),
        }
    }
}

impl Error for MyErr {}
