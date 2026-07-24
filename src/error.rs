use firestore::errors::FirestoreError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("firestore client error")]
    Firestore(#[from] FirestoreError),

    #[error("there are no lease slots available")]
    LeaseSlotsExhausted,

    #[error("the lease was lost")]
    LeaseLost,
}

pub type Result<T> = std::result::Result<T, Error>;
