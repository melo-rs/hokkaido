use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use std::{
    ops::Add,
    sync::atomic::{AtomicU16, Ordering},
};
use yuki_rs::firestore::{
    FirestoreDb as FirestoreDatabase, FirestoreDbOptions as FirestoreDatabaseOptions,
};

const GOOGLE_PROJECT_ID: &str = "sandbox";

static NEXT_DATABASE_ID: AtomicU16 = AtomicU16::new(0);

#[rstest::fixture]
pub async fn fresh_firestore() -> FirestoreDatabase {
    let database_id = NEXT_DATABASE_ID.fetch_add(1, Ordering::Relaxed);

    let options = FirestoreDatabaseOptions::new(GOOGLE_PROJECT_ID.to_owned())
        .with_database_id(database_id.to_string());

    FirestoreDatabase::with_options_token_source(
        options,
        gcloud_sdk::GCP_DEFAULT_SCOPES.clone(),
        gcloud_sdk::TokenSourceType::Default,
    )
    .await
    .expect("failed to create firestore database")
}

#[derive(Serialize, Deserialize)]
pub struct MachineDocument {
    lease_until: DateTime<Utc>,
}

impl Default for MachineDocument {
    fn default() -> Self {
        Self {
            lease_until: DateTime::UNIX_EPOCH.to_utc(),
        }
    }
}

impl MachineDocument {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_lease_until(mut self, lease_until: DateTime<Utc>) -> Self {
        self.lease_until = lease_until;
        self
    }
}

pub const MACHINE_COLLECTION: &str = "machines";

async fn seed_firestore(firestore: &FirestoreDatabase, machine_document: &MachineDocument) {
    let batch_writer = firestore
        .create_simple_batch_writer()
        .await
        .expect("failed to create batch writer");

    let mut current_batch = batch_writer.new_batch();

    for machine_id in 0..=1023 {
        firestore
            .fluent()
            .update()
            .in_col(MACHINE_COLLECTION)
            .document_id(machine_id.to_string())
            .object(machine_document)
            .add_to_batch(&mut current_batch)
            .expect("failed to add write to writer batch");
    }

    current_batch
        .write()
        .await
        .expect("failed to send write batch");
}

#[rstest::fixture]
pub async fn seeded_firestore() -> FirestoreDatabase {
    let firestore = fresh_firestore().await;
    let machine_document = MachineDocument::new();

    seed_firestore(&firestore, &machine_document).await;

    firestore
}

#[rstest::fixture]
pub async fn firestore_with_leased_machines() -> FirestoreDatabase {
    let firestore = fresh_firestore().await;

    let machine_document =
        MachineDocument::new().with_lease_until(Utc::now().add(TimeDelta::hours(1)));

    seed_firestore(&firestore, &machine_document).await;

    firestore
}
