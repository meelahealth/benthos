use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::typemap::TypeMap;

#[derive(Debug)]
pub enum Error {
    Retry,
    Fail,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Error::Retry => "Retry",
            Error::Fail => "Fail",
        })
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Clone)]
pub struct WorkRequest {
    pub id: String,
    pub action: String,
    pub data: serde_json::Value,
    pub attempts: usize,
    pub created_at: DateTime<Utc>,
    pub last_attempted_at: Option<DateTime<Utc>>,
    pub not_before: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait Task {
    async fn run(&self, data: &TypeMap, request: WorkRequest) -> Result<(), Error>;
}
