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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum State {
    Pending,
    Started,
    Attempted,
    Failed,
    Expired,
    Succeeded,
}

impl State {
    pub fn is_final(&self) -> bool {
        match self {
            Self::Failed | Self::Succeeded | Self::Expired => true,
            _ => false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkRequest {
    pub id: String,
    pub action: String,
    pub state: State,
    pub data: bson::Bson,
    pub attempts: usize,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl WorkRequest {
    pub fn new(id: String, wr: NewWorkRequest) -> Self {
        let now = Utc::now();
        Self {
            id,
            action: wr.action,
            data: wr.data,
            state: State::Pending,
            attempts: 0,
            scheduled_at: wr.scheduled_at,
            expires_at: wr.expires_at,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn and_started_at(&self, started_at: DateTime<Utc>) -> WorkRequest {
        let mut x = self.clone();
        x.state = State::Started;
        x.updated_at = started_at;
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
    async fn generate_data(&self, _data: &TypeMap) -> bson::Bson {
        bson::Bson::Document(bson::Document::new())
    }
}
