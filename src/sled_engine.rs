use crate::{KvsEngine, MyErr, Result};
use sled::{self, Db};
use std::path::PathBuf;

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
    fn set(&mut self, key: String, val: String) -> Result<()> {
        self.db.insert(key, val.as_bytes())?;
        Ok(())
    }
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let opt_iv = self.db.get(key)?;
        if let Some(iv) = opt_iv {
            Ok(Some(String::from_utf8(iv.to_vec())?))
        } else {
            Ok(None)
        }
    }
    fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(MyErr::KeyNotFound)?;
        Ok(())
    }
}
