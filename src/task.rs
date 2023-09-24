use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{broker::NewWorkRequest, typemap::TypeMap};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkRequest {
    pub id: String,
    pub action: String,
    pub data: serde_json::Value,
    pub attempts: usize,
    pub created_at: DateTime<Utc>,
    pub last_attempted_at: Option<DateTime<Utc>>,
    pub not_before: Option<DateTime<Utc>>,
    pub succeeded_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
}

impl WorkRequest {
    pub fn new(id: String, wr: NewWorkRequest) -> Self {
        Self {
            id,
            action: wr.action,
            data: wr.data,
            attempts: 0,
            created_at: Utc::now(),
            last_attempted_at: None,
            not_before: wr.not_before,
            succeeded_at: None,
            failed_at: None,
            started_at: None,
        }
    }

    pub fn and_started_at(&self, started_at: DateTime<Utc>) -> WorkRequest {
        let mut x = self.clone();
        x.started_at = Some(started_at);
        x
    }
}

#[async_trait]
pub trait Task {
    /// The name of the task.
    fn id(&self) -> &'static str;

    /// The task runner.
    async fn run(&self, data: &TypeMap, request: WorkRequest) -> Result<(), Error>;

    /// The repetition interval for the task.
    fn repetition_rule(&self) -> Option<cron::Schedule> {
        None
    }

    /// Generates the data for the pending task to be run.
    async fn generate_data(&self, _data: &TypeMap) -> serde_json::Value {
        serde_json::json!({})
    }
}
