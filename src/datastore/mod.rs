pub mod firestore;

use crate::Result;
use chrono::{DateTime, Utc};

pub trait DataStore {
    type Revision;

    fn acquire_lease(&self) -> impl Future<Output = Result<(u16, DateTime<Utc>, Self::Revision)>>;
    fn renew_lease(
        &self,
        lease_id: u16,
        revision: &Self::Revision,
    ) -> impl Future<Output = Result<(DateTime<Utc>, Self::Revision)>>;
    fn release_lease(
        &self,
        lease_id: u16,
        revision: &Self::Revision,
    ) -> impl Future<Output = Result<()>>;
}

pub use firestore::Firestore;
