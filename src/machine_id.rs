use crate::{
    Result,
    datastore::{DataStore, Firestore},
    firestore::FirestoreDb as FirestoreDatabase,
    utils::utc_now,
};
use chrono::{DateTime, TimeDelta, Utc};
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct MachineID {
    machine_id: u16,
    lease_until: DateTime<Utc>,
    firestore: Firestore,
}

impl MachineID {
    const HEARTBEAT_INTERVAL_SECONDS: u64 = 60;
    const LEASE_EXPIRY_THRESHOLD_SECONDS: i64 = 60;

    pub async fn obtain(firestore_database: FirestoreDatabase) -> Result<Self> {
        let firestore = Firestore::new(firestore_database);
        let (machine_id, lease_until) = firestore.obtain_machine_id().await?;

        Ok(Self {
            firestore,
            machine_id,
            lease_until,
        })
    }

    pub async fn maintain(&mut self, cancellation_token: CancellationToken) -> Result<()> {
        let mut interval = time::interval(Duration::from_secs(Self::HEARTBEAT_INTERVAL_SECONDS));

        loop {
            tokio::select! {
              _ = cancellation_token.cancelled() => {
                return Ok(())
              }

              _ = interval.tick() => {
                match self.firestore.extend_machine_id_lease(self.machine_id).await {
                  Ok(lease_until) => {
                    self.lease_until = lease_until
                  }
                  Err(error) => {
                    if self.lease_until - utc_now() < TimeDelta::seconds(Self::LEASE_EXPIRY_THRESHOLD_SECONDS) {
                      return Err(error)
                    }
                  }
                }
              }
            }
        }
    }

    pub fn as_u16(&self) -> u16 {
        self.machine_id
    }

    pub fn as_u64(&self) -> u64 {
        self.machine_id as u64
    }
}
