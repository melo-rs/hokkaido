use fixtures::{firestore_with_leased_machines, fresh_firestore, seeded_firestore};
use std::{assert_matches, collections::HashSet};
use yuki_rs::{Error, firestore::FirestoreDb as FirestoreDatabase, machine_id::MachineID};

mod fixtures;

#[rstest::rstest]
#[tokio::test]
async fn obtain_succeeds(#[future(awt)] seeded_firestore: FirestoreDatabase) {
    let machine_id = MachineID::obtain(seeded_firestore)
        .await
        .expect("should succeed");

    assert!(machine_id.as_u16() < 1023);
}

#[rstest::rstest]
#[tokio::test]
async fn obtain_succeeds_with_maximum_concurrent_workers(
    #[future(awt)] seeded_firestore: FirestoreDatabase,
) {
    let futures = (0..1024).map(|_| async {
        MachineID::obtain(seeded_firestore.clone())
            .await
            .expect("worker should receive machine ID")
            .as_u16()
    });

    let results = futures::future::join_all(futures).await;

    let result_hashset: HashSet<u16> = results.into_iter().collect();
    let wished_hashset: HashSet<u16> = (0..1024).collect();

    assert_eq!(result_hashset, wished_hashset)
}

#[rstest::rstest]
#[tokio::test]
async fn obtain_fails_when_firestore_is_not_seeded(
    #[future(awt)] fresh_firestore: FirestoreDatabase,
) {
    let error = MachineID::obtain(fresh_firestore)
        .await
        .expect_err("should fail");

    assert_matches!(error, Error::MissedMachineIDRecord(_))
}

#[rstest::rstest]
#[tokio::test]
async fn obtain_fails_when_no_free_machine_id_exists(
    #[future(awt)] firestore_with_leased_machines: FirestoreDatabase,
) {
    let error = MachineID::obtain(firestore_with_leased_machines)
        .await
        .expect_err("should fail");

    assert_matches!(error, Error::MachineIDsExhausted)
}

#[rstest::rstest]
#[tokio::test]
async fn maintain_succeeds() {}
