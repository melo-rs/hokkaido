use crate::{Error, Result, datastore::DataStore, utils::utc_now};
use chrono::{DateTime, TimeDelta, Utc};
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct Lease<D>
where
    D: DataStore,
{
    id: u16,
    lasts_until: DateTime<Utc>,
    revision: D::Revision,
    datastore: D,
}

impl<D> Lease<D>
where
    D: DataStore,
{
    const HEARTBEAT_INTERVAL_SECONDS: u64 = 60;
    const LEASE_EXPIRY_THRESHOLD_SECONDS: i64 = 60;

    pub async fn new(datastore: D) -> Result<Self> {
        let (id, lasts_until, revision) = datastore.obtain_machine_id().await?;

        Ok(Self {
            id,
            lasts_until,
            revision,
            datastore,
        })
    }

    pub async fn maintain(&mut self, cancellation_token: CancellationToken) -> Result<()> {
        if self.lasts_until < utc_now() {
            return Err(Error::LeaseLost);
        }

        let mut interval = time::interval(Duration::from_secs(Self::HEARTBEAT_INTERVAL_SECONDS));

        loop {
            tokio::select! {
              _ = cancellation_token.cancelled() => {
                return Ok(())
              }

              _ = interval.tick() => {
                match self.datastore.extend_machine_id_lease(self.id, &self.revision).await {
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
        let Self {
            id,
            revision,
            datastore,
            ..
        } = self;

        datastore.release(id, &revision).await
    }

    pub fn as_u16(&self) -> u16 {
        self.id
    }

    pub fn as_u64(&self) -> u64 {
        self.id as u64
    }
}
