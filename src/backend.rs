use async_trait::async_trait;

use crate::{broker::NewWorkRequest, task::WorkRequest};

#[async_trait]
pub trait Backend {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns a list of work request identifiers that are ready to be processed.
    async fn poll(&self) -> Result<Vec<String>, Self::Error>;

    /// Returns a work request for the given identifier.
    async fn work_request_with_id(&self, id: &str) -> Result<WorkRequest, Self::Error>;

    /// Marks an attempt was made.
    async fn mark_attempted(&self, id: &str) -> Result<(), Self::Error>;

    /// Marks a work item as succeeded. Will increment attempts by one.
    async fn mark_succeeded(&self, id: &str) -> Result<(), Self::Error>;

    /// Queues a new work request.
    async fn add_work_request(&self, work_request: NewWorkRequest) -> Result<(), Self::Error>;
}
