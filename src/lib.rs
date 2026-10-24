//! pgs3 is an S3-compatible object store with the bytes in Postgres.

mod http;
mod s3util;
mod store;

pub use http::router;
pub use store::Store;
