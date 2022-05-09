use crate::{KvsEngine, MyErr, Result};
use sled::{self, Db};
use std::path::PathBuf;

#[derive(Clone)]
pub struct SledKvEngine {
    db: Db,
}

impl SledKvEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvEngine> {
        let eng = SledKvEngine {
            db: sled::open(path.into())?,
        };
        Ok(eng)
    }
}

impl KvsEngine for SledKvEngine {
    fn set(&self, key: String, val: String) -> Result<()> {
        self.db.insert(key, val.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }
    fn get(&self, key: String) -> Result<Option<String>> {
        let opt_iv = self.db.get(key)?;
        if let Some(iv) = opt_iv {
            Ok(Some(String::from_utf8(iv.to_vec())?))
        } else {
            Ok(None)
        }
    }
    fn remove(&self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(MyErr::KeyNotFound)?;
        self.db.flush()?;
        Ok(())
    }
}
