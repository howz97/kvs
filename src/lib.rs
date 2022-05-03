// #![deny(missing_docs)]
//! This is key-value store lib
pub mod my_engine;
pub mod protocol;
pub mod sled_engine;

use failure::Error;
use std::result;

pub type Result<T> = result::Result<T, Error>;

pub trait KvsEngine {
    fn set(&mut self, key: String, val: String) -> Result<()>;
    fn get(&mut self, key: String) -> Result<Option<String>>;
    fn remove(&mut self, key: String) -> Result<()>;
}
