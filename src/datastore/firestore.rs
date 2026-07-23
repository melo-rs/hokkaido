use crate::{Error, Result, datastore::DataStore, utils::utc_now};
use chrono::{DateTime, TimeDelta, Utc};
use firestore::{
    FirestoreDb as FirestoreDatabase, FirestoreWritePrecondition, errors::FirestoreError, paths,
    timestamp_utils::from_timestamp,
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
}

#[derive(Serialize, Deserialize)]
struct Machine {
    #[serde(with = "firestore::serialize_as_timestamp")]
    lease_until: DateTime<Utc>,
}

impl DataStore for Firestore {
    async fn obtain_machine_id(&self) -> Result<(u16, DateTime<Utc>)> {
        let mut machine_id = random_range(0..1024);
        let increment = random_range(0..512) * 2 + 1;

        let mut checked = 0;

        while checked < 1024 {
            let machine_id_str = machine_id.to_string();

            let (document, mut fields) = match self
                .database
                .fluent()
                .select()
                .by_id_in(Self::MACHINE_COLLECTION)
                .one(&machine_id_str)
                .await?
            {
                Some(document) => {
                    let fields = FirestoreDatabase::deserialize_doc_to::<Machine>(&document)?;
                    (document, fields)
                }
                _ => return Err(Error::MissedMachineIDRecord(machine_id_str)),
            };

            if fields.lease_until >= utc_now() {
                checked += 1;
                machine_id = (machine_id + increment) & 1023;

                continue;
            }

            let update_time = from_timestamp(
                document
                    .update_time
                    .expect("This field cannot be null in Firestore documents"),
            )?;

            fields.lease_until = utc_now() + TimeDelta::minutes(5);

            let result = self
                .database
                .fluent()
                .update()
                .fields(paths!(Machine::lease_until))
                .in_col(Self::MACHINE_COLLECTION)
                .precondition(FirestoreWritePrecondition::UpdateTime(update_time))
                .document_id(&machine_id_str)
                .object(&fields)
                .execute::<()>()
                .await;

            match result {
                Ok(_) => return Ok((machine_id, fields.lease_until)),
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

        Err(Error::MachineIDsExhausted)
    }

    async fn extend_machine_id_lease(&self, machine_id: u16) -> Result<DateTime<Utc>> {
        let machine = Machine {
            lease_until: utc_now() + TimeDelta::seconds(Self::LEASE_DURATION_SECONDS),
        };

        self.database
            .fluent()
            .update()
            .fields(paths!(Machine::lease_until))
            .in_col(Self::MACHINE_COLLECTION)
            .document_id(machine_id.to_string())
            .object(&machine)
            .execute::<()>()
            .await?;

        Ok(machine.lease_until)
    }
}
