use firestore::errors::FirestoreError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("firestore client error")]
    Firestore(#[from] FirestoreError),

    #[error("there is no unused machine ID")]
    MachineIDsExhausted,

    #[error("The machine ID lease was lost")]
    LeaseLost,
}

pub type Result<T> = std::result::Result<T, Error>;
