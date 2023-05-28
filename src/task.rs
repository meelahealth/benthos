use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::typemap::TypeMap;

pub enum Error {}

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
