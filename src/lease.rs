use crate::{Error, Result, datastore::DataStore, utils::utc_now};
use chrono::{DateTime, TimeDelta, Utc};
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct Lease<D>
where
    D: DataStore,
{
    lease_id: u16,
    expires_at: DateTime<Utc>,
    revision: D::Revision,
    datastore: D,
}

impl<D> Lease<D>
where
    D: DataStore,
{
    const LEASE_RENEWAL_INTERVAL: Duration = Duration::from_secs(60);
    const LEASE_RENEWAL_TIMEOUT: Duration = Duration::from_secs(30);

    const LEASE_EXPIRY_THRESHOLD_SECONDS: i64 = 60;

    pub async fn new(datastore: D) -> Result<Self> {
        let (lease_id, expires_at, revision) = datastore.acquire_lease().await?;

        Ok(Self {
            lease_id,
            expires_at,
            revision,
            datastore,
        })
    }

    pub async fn maintain(&mut self, cancellation_token: CancellationToken) -> Result<()> {
        if self.expires_at <= utc_now() {
            return Err(Error::LeaseLost);
        }

        let mut interval = time::interval(Self::LEASE_RENEWAL_INTERVAL);

        loop {
            tokio::select! {
              _ = cancellation_token.cancelled() => {
                return Ok(())
              }

              _ = interval.tick() => {
                let timeout_result = time::timeout(Self::LEASE_RENEWAL_TIMEOUT, self.datastore.renew_lease(self.lease_id, &self.revision)).await;

                let result = match timeout_result {
                  Ok(result) => result,
                  Err(_) => return Err(Error::LeaseRenewalTimeout),
                };

                match result {
                  Ok((expires_at, revision)) => {
                    self.expires_at = expires_at;
                    self.revision = revision;
                  }
                  Err(error) => {
                    if error.is_lease_lost_error() {
                      return Err(error)
                    }

                    if self.expires_at - utc_now() < TimeDelta::seconds(Self::LEASE_EXPIRY_THRESHOLD_SECONDS) {
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
            lease_id,
            revision,
            datastore,
            ..
        } = self;

        datastore.release_lease(lease_id, &revision).await
    }

    pub fn id(&self) -> u16 {
        self.lease_id
    }
}
