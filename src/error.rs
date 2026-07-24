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

impl Error {
    pub fn is_lease_lost_error(&self) -> bool {
        matches!(self, Self::LeaseLost)
    }

    pub fn is_lease_slots_exhausted_error(&self) -> bool {
        matches!(self, Self::LeaseSlotsExhausted)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
