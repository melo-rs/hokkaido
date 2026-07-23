use crate::{
    Error, Result,
    datastore::{DataStore, Firestore},
    firestore::FirestoreDb as FirestoreDatabase,
    utils::utc_now,
};
use chrono::{DateTime, TimeDelta, Utc};
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct Lease {
    id: u16,
    lasts_until: DateTime<Utc>,
    revision: DateTime<Utc>,
    firestore: Firestore,
}

impl Lease {
    const HEARTBEAT_INTERVAL_SECONDS: u64 = 60;
    const LEASE_EXPIRY_THRESHOLD_SECONDS: i64 = 60;

    pub async fn new(firestore_database: FirestoreDatabase) -> Result<Self> {
        let firestore = Firestore::new(firestore_database);
        let (id, lasts_until, revision) = firestore.obtain_machine_id().await?;

        Ok(Self {
            id,
            lasts_until,
            firestore,
            revision,
        })
    }

    pub async fn maintain(&mut self, cancellation_token: CancellationToken) -> Result<()> {
        if self.lasts_until < utc_now() {
            return Err(Error::MachineIDLost);
        }

        let mut interval = time::interval(Duration::from_secs(Self::HEARTBEAT_INTERVAL_SECONDS));

        loop {
            tokio::select! {
              _ = cancellation_token.cancelled() => {
                return Ok(())
              }

              _ = interval.tick() => {
                match self.firestore.extend_machine_id_lease(self.id, self.revision).await {
                  Ok((lasts_until, revision)) => {
                    self.lasts_until = lasts_until;
                    self.revision = revision;
                  }
                  Err(error) => {
                    if self.lasts_until - utc_now() < TimeDelta::seconds(Self::LEASE_EXPIRY_THRESHOLD_SECONDS) {
                      return Err(error)
                    }
                  }
                }
              }
            }
        }
    }

    pub async fn release(self) -> Result<()> {
      self.firestore.release(self.id, self.revision).await
    }

    pub fn as_u16(&self) -> u16 {
        self.id
    }

    pub fn as_u64(&self) -> u64 {
        self.id as u64
    }
}
