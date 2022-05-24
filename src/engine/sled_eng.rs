use crate::{KvsEngine, MyErr, Result};
use async_trait::async_trait;
use sled::{self, Db};
use std::path::PathBuf;

#[derive(Clone)]
pub struct SledKvsEngine {
    db: Db,
}

impl SledKvsEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let eng = SledKvsEngine {
            db: sled::open(path.into())?,
        };
        Ok(eng)
    }
}

#[async_trait]
impl KvsEngine for SledKvsEngine {
    async fn set(self, key: String, val: String) -> Result<()> {
        self.db.insert(key, val.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }
    async fn get(self, key: String) -> Result<Option<String>> {
        let opt_iv = self.db.get(key)?;
        if let Some(iv) = opt_iv {
            Ok(Some(String::from_utf8(iv.to_vec())?))
        } else {
            Ok(None)
        }
    }
    async fn remove(self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(MyErr::KeyNotFound)?;
        self.db.flush()?;
        Ok(())
    }
}
