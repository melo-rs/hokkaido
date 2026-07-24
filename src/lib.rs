mod utils;

pub mod datastore;
pub mod error;
pub mod lease;

pub use datastore::Firestore;
pub use error::{Error, Result};
pub use lease::Lease;

pub use firestore;
