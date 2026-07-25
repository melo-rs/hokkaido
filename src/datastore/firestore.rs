use crate::{Error, Result, datastore::DataStore, utils::utc_now};
use chrono::{DateTime, TimeDelta, Utc};
use firestore::{
    FirestoreDb as FirestoreDatabase, FirestoreDocument, FirestoreWritePrecondition,
    errors::FirestoreError, paths, timestamp_utils::from_timestamp,
};
use rand::random_range;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct Firestore {
    database: FirestoreDatabase,
}

impl Firestore {
    const LEASE_COLLECTION: &'static str = "leases";
    const LEASE_DURATION_SECONDS: i64 = 300;

    pub fn new(firestore_database: FirestoreDatabase) -> Self {
        Self {
            database: firestore_database,
        }
    }

    fn is_failed_precondition_error(error: &FirestoreError) -> bool {
        if let FirestoreError::DatabaseError(database_error) = error
            && database_error.public.code == "FailedPrecondition"
        {
            return true;
        }

        false
    }

    fn revision(document: &FirestoreDocument) -> Result<DateTime<Utc>> {
        let update_time = document
            .update_time
            .expect("Firestore is expected to always return document update times");

        Ok(from_timestamp(update_time)?)
    }
}

#[derive(Serialize, Deserialize)]
struct LeaseRecord {
    #[serde(with = "firestore::serialize_as_timestamp")]
    lease_until: DateTime<Utc>,
}

impl DataStore for Firestore {
    type Revision = DateTime<Utc>;

    async fn create_lease(&self) -> Result<(u16, DateTime<Utc>, Self::Revision)> {
        let mut slot_id = random_range(0..1024);
        let increment = random_range(0..512) * 2 + 1;

        let mut scanned = 0;

        while scanned < 1024 {
            let slot_id_str = slot_id.to_string();

            let document = self
                .database
                .fluent()
                .select()
                .by_id_in(Self::LEASE_COLLECTION)
                .one(&slot_id_str)
                .await?;

            let lease_until = utc_now() + TimeDelta::seconds(Self::LEASE_DURATION_SECONDS);

            match document {
                Some(document) => {
                    let mut fields =
                        FirestoreDatabase::deserialize_doc_to::<LeaseRecord>(&document)?;

                    if fields.lease_until >= utc_now() {
                        scanned += 1;
                        slot_id = (slot_id + increment) & 1023;

                        continue;
                    }

                    fields.lease_until = lease_until;

                    let update_time = Self::revision(&document)?;

                    let document = FirestoreDatabase::serialize_to_doc(
                        format!(
                            "{}/{}/{}",
                            self.database.get_documents_path(),
                            Self::LEASE_COLLECTION,
                            slot_id_str,
                        ),
                        &fields,
                    )?;

                    let result = self
                        .database
                        .fluent()
                        .update()
                        .in_col(Self::LEASE_COLLECTION)
                        .precondition(FirestoreWritePrecondition::UpdateTime(update_time))
                        .document(document)
                        .execute()
                        .await;

                    match result {
                        Ok(document) => {
                            return Ok((slot_id, lease_until, Self::revision(&document)?));
                        }
                        Err(firestore_error) => {
                            if Self::is_failed_precondition_error(&firestore_error) {
                                scanned += 1;
                                slot_id = (slot_id + increment) & 1023;

                                continue;
                            }

                            return Err(firestore_error.into());
                        }
                    }
                }
                _ => {
                    let document =
                        FirestoreDatabase::serialize_to_doc("", &LeaseRecord { lease_until })?;

                    let result = self
                        .database
                        .fluent()
                        .insert()
                        .into(Self::LEASE_COLLECTION)
                        .document_id(&slot_id_str)
                        .document(document)
                        .execute()
                        .await;

                    match result {
                        Ok(document) => {
                            return Ok((slot_id, lease_until, Self::revision(&document)?));
                        }
                        Err(firestore_error) => {
                            if let FirestoreError::DataConflictError(ref data_conflict_error) =
                                firestore_error
                                && data_conflict_error.public.code == "AlreadyExists"
                            {
                                scanned += 1;
                                slot_id = (slot_id + increment) & 1023;

                                continue;
                            }

                            return Err(firestore_error.into());
                        }
                    }
                }
            };
        }

        Err(Error::LeaseSlotsExhausted)
    }

    async fn renew_lease(
        &self,
        lease_id: u16,
        revision: &Self::Revision,
    ) -> Result<(DateTime<Utc>, Self::Revision)> {
        let lease = LeaseRecord {
            lease_until: utc_now() + TimeDelta::seconds(Self::LEASE_DURATION_SECONDS),
        };

        let document = FirestoreDatabase::serialize_to_doc(
            format!(
                "{}/{}/{}",
                self.database.get_documents_path(),
                Self::LEASE_COLLECTION,
                lease_id,
            ),
            &lease,
        )?;

        let result = self
            .database
            .fluent()
            .update()
            .fields(paths!(LeaseRecord::lease_until))
            .in_col(Self::LEASE_COLLECTION)
            .precondition(FirestoreWritePrecondition::UpdateTime(*revision))
            .document(document)
            .execute()
            .await;

        match result {
            Ok(document) => Ok((lease.lease_until, Self::revision(&document)?)),
            Err(firestore_error) => {
                if Self::is_failed_precondition_error(&firestore_error) {
                    return Err(Error::LeaseLost);
                }

                Err(firestore_error.into())
            }
        }
    }

    async fn revoke_lease(&self, lease_id: u16, revision: &Self::Revision) -> Result<()> {
        let lease = LeaseRecord {
            lease_until: DateTime::UNIX_EPOCH,
        };

        let result = self
            .database
            .fluent()
            .update()
            .fields(paths!(LeaseRecord::lease_until))
            .in_col(Self::LEASE_COLLECTION)
            .precondition(FirestoreWritePrecondition::UpdateTime(*revision))
            .document_id(lease_id.to_string())
            .object(&lease)
            .execute::<()>()
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(firestore_error) => {
                if Self::is_failed_precondition_error(&firestore_error) {
                    return Err(Error::LeaseLost);
                }

                Err(firestore_error.into())
            }
        }
    }
}
