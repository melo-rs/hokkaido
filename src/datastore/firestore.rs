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
    const MACHINE_COLLECTION: &'static str = "machines";
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
struct Machine {
    #[serde(with = "firestore::serialize_as_timestamp")]
    lease_until: DateTime<Utc>,
}

impl DataStore for Firestore {
    type Revision = DateTime<Utc>;

    async fn obtain_machine_id(&self) -> Result<(u16, DateTime<Utc>, Self::Revision)> {
        let mut machine_id = random_range(0..1024);
        let increment = random_range(0..512) * 2 + 1;

        let mut checked = 0;

        while checked < 1024 {
            let machine_id_str = machine_id.to_string();

            let document = self
                .database
                .fluent()
                .select()
                .by_id_in(Self::MACHINE_COLLECTION)
                .one(&machine_id_str)
                .await?;

            let lease_until = utc_now() + TimeDelta::seconds(Self::LEASE_DURATION_SECONDS);

            match document {
                Some(document) => {
                    let mut fields = FirestoreDatabase::deserialize_doc_to::<Machine>(&document)?;

                    if fields.lease_until >= utc_now() {
                        checked += 1;
                        machine_id = (machine_id + increment) & 1023;

                        continue;
                    }

                    fields.lease_until =
                        utc_now() + TimeDelta::seconds(Self::LEASE_DURATION_SECONDS);

                    let update_time = Self::revision(&document)?;

                    let document = FirestoreDatabase::serialize_to_doc(
                        format!(
                            "{}/{}/{}",
                            self.database.get_documents_path(),
                            Self::MACHINE_COLLECTION,
                            machine_id_str,
                        ),
                        &fields,
                    )?;

                    let result = self
                        .database
                        .fluent()
                        .update()
                        .in_col(Self::MACHINE_COLLECTION)
                        .precondition(FirestoreWritePrecondition::UpdateTime(update_time))
                        .document(document)
                        .execute()
                        .await;

                    match result {
                        Ok(document) => {
                            return Ok((machine_id, lease_until, Self::revision(&document)?));
                        }
                        Err(firestore_error) => {
                            if let FirestoreError::DatabaseError(ref db_error) = firestore_error
                                && db_error.public.code == "FailedPrecondition"
                            {
                                checked += 1;
                                machine_id = (machine_id + increment) & 1023;

                                continue;
                            }

                            return Err(firestore_error.into());
                        }
                    }
                }
                _ => {
                    let document =
                        FirestoreDatabase::serialize_to_doc("", &Machine { lease_until })?;

                    let result = self
                        .database
                        .fluent()
                        .insert()
                        .into(Self::MACHINE_COLLECTION)
                        .document_id(&machine_id_str)
                        .document(document)
                        .execute()
                        .await;

                    match result {
                        Ok(document) => {
                            return Ok((machine_id, lease_until, Self::revision(&document)?));
                        }
                        Err(firestore_error) => {
                            if let FirestoreError::DataConflictError(ref data_conflict_error) =
                                firestore_error
                                && data_conflict_error.public.code == "AlreadyExists"
                            {
                                checked += 1;
                                machine_id = (machine_id + increment) & 1023;

                                continue;
                            }

                            return Err(firestore_error.into());
                        }
                    }
                }
            };
        }

        Err(Error::MachineIDsExhausted)
    }

    async fn extend_machine_id_lease(
        &self,
        machine_id: u16,
        revision: &Self::Revision,
    ) -> Result<(DateTime<Utc>, Self::Revision)> {
        let machine = Machine {
            lease_until: utc_now() + TimeDelta::seconds(Self::LEASE_DURATION_SECONDS),
        };

        let document = FirestoreDatabase::serialize_to_doc(
            format!(
                "{}/{}/{}",
                self.database.get_documents_path(),
                Self::MACHINE_COLLECTION,
                machine_id,
            ),
            &machine,
        )?;

        let result = self
            .database
            .fluent()
            .update()
            .fields(paths!(Machine::lease_until))
            .in_col(Self::MACHINE_COLLECTION)
            .precondition(FirestoreWritePrecondition::UpdateTime(*revision))
            .document(document)
            .execute()
            .await;

        match result {
            Ok(document) => Ok((machine.lease_until, Self::revision(&document)?)),
            Err(firestore_error) => {
                if Self::is_failed_precondition_error(&firestore_error) {
                    return Err(Error::LeaseLost);
                }

                Err(firestore_error.into())
            }
        }
    }

    async fn release(&self, machine_id: u16, revision: &Self::Revision) -> Result<()> {
        let machine = Machine {
            lease_until: DateTime::UNIX_EPOCH,
        };

        let result = self
            .database
            .fluent()
            .update()
            .fields(paths!(Machine::lease_until))
            .in_col(Self::MACHINE_COLLECTION)
            .precondition(FirestoreWritePrecondition::UpdateTime(*revision))
            .document_id(machine_id.to_string())
            .object(&machine)
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
