pub mod firestore;

use crate::Result;
use chrono::{DateTime, Utc};

pub trait DataStore {
    type Revision;

    fn obtain_machine_id(
        &self,
    ) -> impl Future<Output = Result<(u16, DateTime<Utc>, Self::Revision)>>;
    fn extend_machine_id_lease(
        &self,
        machine_id: u16,
        revision: Self::Revision,
    ) -> impl Future<Output = Result<(DateTime<Utc>, Self::Revision)>>;
    fn release(
        &self,
        machine_id: u16,
        revision: Self::Revision,
    ) -> impl Future<Output = Result<()>>;
}

pub use firestore::Firestore;
