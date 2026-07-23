pub mod firestore;

use crate::Result;
use chrono::{DateTime, Utc};

pub trait DataStore {
    fn obtain_machine_id(&self) -> impl Future<Output = Result<(u16, DateTime<Utc>)>>;
    fn extend_machine_id_lease(
        &self,
        machine_id: u16,
    ) -> impl Future<Output = Result<DateTime<Utc>>>;
}

pub use firestore::Firestore;
