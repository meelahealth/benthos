use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{broker::NewWorkRequest, task::WorkRequest, TypeMap};

#[async_trait]
pub trait Backend {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Shared data for all tasks.
    fn data(&self) -> Arc<TypeMap>;

    /// Returns a list of work request identifiers that are ready to be processed.
    async fn poll(&self) -> Result<Vec<String>, Self::Error>;

    /// Returns a work request for the given identifier.
    async fn work_request_with_id<S: AsRef<str> + Send>(
        &self,
        id: S,
    ) -> Result<Option<WorkRequest>, Self::Error>;

    /// Returns a set of work requests for the given identifiers.
    async fn work_request_with_ids<S: AsRef<str> + Send + Sync>(
        &self,
        id: &[S],
    ) -> Result<Vec<WorkRequest>, Self::Error>;

    /// Marks an attempt was made.
    async fn mark_attempted(&self, id: &str) -> Result<(), Self::Error>;

    /// Marks a work item as succeeded. Will increment attempts by one.
    async fn mark_succeeded(&self, id: &str) -> Result<(), Self::Error>;

    /// Marks a work item as failed. Will increment attempts by one.
    async fn mark_failed(&self, id: &str) -> Result<(), Self::Error>;

    /// Queues a new work request. Returns the identifier.
    async fn add_work_request(&self, work_request: NewWorkRequest) -> Result<String, Self::Error>;

    /// Check if work request with given action and date already exists.
    async fn has_work_request(
        &self,
        action: &str,
        next_date: DateTime<Utc>,
    ) -> Result<bool, Self::Error>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statistics {
    pub processed: usize,
    pub failed: usize,
    pub busy: usize,
    pub enqueued: usize,
    pub retries: usize,
    pub scheduled: usize,
    pub dead: usize,
}

#[derive(Debug, Clone, Default)]
pub struct WorkRequestFilter {
    pub before: Option<DateTime<Utc>>,
    pub after: Option<DateTime<Utc>>,
}

#[async_trait]
pub trait BackendManager: Backend {
    /// Returns statistics about the queue.
    async fn statistics(&self) -> Result<Statistics, Self::Error>;

    /// Returns work requests within the provided filter.
    async fn work_requests(
        &self,
        filter: WorkRequestFilter,
    ) -> Result<Vec<WorkRequest>, Self::Error>;
}
