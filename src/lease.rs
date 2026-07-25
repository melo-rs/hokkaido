use crate::{Error, Result, datastore::DataStore, utils::utc_now};
use chrono::{DateTime, Utc};
use rand::random_range;
use tokio::time::{self, Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct Lease<D>
where
    D: DataStore,
{
    pub id: u16,

    lasts_until: DateTime<Utc>,
    revision: D::Revision,
    datastore: D,
}

impl<D> Lease<D>
where
    D: DataStore,
{
    const LEASE_RENEWAL_INTERVAL: Duration = Duration::from_secs(60);
    const LEASE_RENEWAL_TIMEOUT: Duration = Duration::from_secs(10);
    const LEASE_RENEWAL_SAFETY_MARGIN: Duration = Duration::from_secs(60);

    pub async fn new(datastore: D) -> Result<Self> {
        let (id, lasts_until, revision) = datastore.create_lease().await?;

        Ok(Self {
            id,
            lasts_until,
            revision,
            datastore,
        })
    }

    pub async fn maintain(&mut self, cancellation_token: CancellationToken) -> Result<()> {
        let initial_jitter = Duration::from_secs(random_range(0..5));

        time::sleep(initial_jitter).await;

        let mut interval = time::interval(Self::LEASE_RENEWAL_INTERVAL);
        let mut renewal_deadline = self.lasts_until - Self::LEASE_RENEWAL_SAFETY_MARGIN;

        loop {
            tokio::select! {
              _ = cancellation_token.cancelled() => {
                return Ok(())
              }

              _ = interval.tick() => {
                if utc_now() >= renewal_deadline {
                  return Err(Error::LeaseLost)
                }

                let timeout_result = time::timeout(Self::LEASE_RENEWAL_TIMEOUT, self.datastore.renew_lease(self.id, &self.revision)).await;

                let result = match timeout_result {
                  Ok(result) => result,
                  Err(_) => {
                    if utc_now() >= renewal_deadline {
                      return Err(Error::LeaseRenewalTimeout)
                    }

                    continue
                  },
                };

                match result {
                  Ok((lasts_until, revision)) => {
                    self.lasts_until = lasts_until;
                    self.revision = revision;

                    renewal_deadline = self.lasts_until - Self::LEASE_RENEWAL_SAFETY_MARGIN;
                  }
                  Err(error) => {
                    if error.is_lease_lost_error() {
                      return Err(error)
                    }

                    if utc_now() >= renewal_deadline {
                      return Err(error)
                    }
                  }
                }
              }
            }
        }
    }

    pub async fn revoke(self) -> Result<()> {
        let Self {
            id,
            revision,
            datastore,
            ..
        } = self;

        datastore.revoke_lease(id, &revision).await
    }
}
