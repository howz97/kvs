pub mod kvs_eng;
pub mod sled_eng;
pub use kvs_eng::KvStore;
pub use sled_eng::SledKvsEngine;

use crate::Result;
use async_trait::async_trait;

#[async_trait]
pub trait KvsEngine: Clone + Send + 'static {
    async fn set(self, key: String, value: String) -> Result<()>;

    async fn get(self, key: String) -> Result<Option<String>>;

    async fn remove(self, key: String) -> Result<()>;
}
