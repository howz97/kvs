pub mod my_engine;
pub mod sled_engine;
pub use my_engine::KvStore;
pub use sled_engine::SledKvsEngine;

use crate::Result;
use async_trait::async_trait;

#[async_trait]
pub trait KvsEngine: Clone + Send + 'static {
    async fn set(self, key: String, value: String) -> Result<()>;

    async fn get(self, key: String) -> Result<Option<String>>;

    async fn remove(self, key: String) -> Result<()>;
}
