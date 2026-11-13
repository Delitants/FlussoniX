pub mod catalog;
pub mod connection;
pub mod lease;
pub mod schema;

use catalog::HaCatalog;
use connection::HaPool;
use lease::HaLedger;
use std::{path::PathBuf, sync::Arc};

pub struct HaAuthority {
    pub pool: Arc<HaPool>,
    pub catalog: Arc<HaCatalog>,
    pub ledger: Arc<HaLedger>,
}

impl HaAuthority {
    pub fn new(pool: Arc<HaPool>, export_path: PathBuf) -> Self {
        Self {
            catalog: Arc::new(HaCatalog::new(pool.clone(), export_path)),
            ledger: Arc::new(HaLedger::new(pool.clone())),
            pool,
        }
    }
}
