use std::{
    collections::HashSet,
    fmt::{Debug, Display},
    sync::{atomic::AtomicU64, Arc},
};

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use tokio::sync::RwLock;

use crate::{
    backend::{Backend, BackendManager, Statistics, WorkRequestFilter},
    broker::NewWorkRequest,
    task::WorkRequest,
    TypeMap,
};

#[derive(Debug)]
pub enum Error {}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Error")
    }
}

pub struct MemoryEngine {
    next_id: AtomicU64,
    work_requests: RwLock<Vec<WorkRequest>>,
    data: Arc<TypeMap>,
}

impl MemoryEngine {
    pub fn new(data: TypeMap) -> Self {
        Self {
            next_id: AtomicU64::new(0),
            work_requests: RwLock::new(Vec::new()),
            data: Arc::new(data),
        }
    }
}

#[async_trait]
impl Backend for MemoryEngine {
    type Error = Error;

    /// Shared data for all tasks.
    fn data(&self) -> Arc<TypeMap> {
        self.data.clone()
    }

    /// Returns a list of work request identifiers that are ready to be processed.
    async fn poll(&self) -> Result<Vec<String>, Self::Error> {
        tracing::trace!("Polling");
        tracing::trace!("{:#?}", self.work_requests.read().await);

        let now = chrono::Utc::now();
        let results = self
            .work_requests
            .read()
            .await
            .iter()
            .filter(|x| x.failed_at.is_none() && x.succeeded_at.is_none())
            .filter(|x| match x.not_before {
                Some(v) => v <= now,
                None => true,
            })
            .map(|x| x.id.clone())
            .collect::<Vec<_>>();

        tracing::trace!("Polled: {}", results.len());
        Ok(results)
    }

    /// Returns a work request for the given identifier.
    async fn work_request_with_id<S: AsRef<str> + Send>(
        &self,
        id: S,
    ) -> Result<Option<WorkRequest>, Self::Error> {
        let id = id.as_ref();
        Ok(self
            .work_requests
            .read()
            .await
            .iter()
            .find(|x| x.id == id)
            .cloned())
    }

    /// Returns a set of work requests for the given identifiers.
    async fn work_request_with_ids<S: AsRef<str> + Send + Sync>(
        &self,
        ids: &[S],
    ) -> Result<Vec<WorkRequest>, Self::Error> {
        let ids = ids.iter().map(|x| x.as_ref()).collect::<HashSet<_>>();
        Ok(self
            .work_requests
            .read()
            .await
            .iter()
            .filter(|x| ids.contains(&*x.id))
            .cloned()
            .collect())
    }

    /// Marks an attempt was made.
    async fn mark_attempted(&self, id: &str) -> Result<(), Self::Error> {
        tracing::trace!(id = id, "Marking attempted");
        let mut work_requests = self.work_requests.write().await;
        if let Some(x) = work_requests.iter_mut().find(|x| x.id == id) {
            x.attempts += 1;
            x.last_attempted_at = Some(chrono::Utc::now());
        }
        Ok(())
    }

    /// Marks a work item as succeeded. Will increment attempts by one.
    async fn mark_succeeded(&self, id: &str) -> Result<(), Self::Error> {
        tracing::trace!(id = id, "Marking succeeded");
        let mut work_requests = self.work_requests.write().await;
        if let Some(x) = work_requests.iter_mut().find(|x| x.id == id) {
            x.attempts += 1;
            x.last_attempted_at = Some(chrono::Utc::now());
            x.succeeded_at = Some(chrono::Utc::now());
        }
        Ok(())
    }

    /// Marks a work item as failed. Will increment attempts by one.
    async fn mark_failed(&self, id: &str) -> Result<(), Self::Error> {
        tracing::trace!(id = id, "Marking failed");
        let mut work_requests = self.work_requests.write().await;
        if let Some(x) = work_requests.iter_mut().find(|x| x.id == id) {
            x.attempts += 1;
            x.last_attempted_at = Some(chrono::Utc::now());
            x.failed_at = Some(chrono::Utc::now());
        }
        Ok(())
    }

    /// Queues a new work request.
    async fn add_work_request(&self, work_request: NewWorkRequest) -> Result<String, Self::Error> {
        let mut work_requests = self.work_requests.write().await;
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        work_requests.push(WorkRequest::new(format!("id-{id}"), work_request));
        Ok(format!("id-{id}"))
    }

    /// Check if work request with given action and date already exists.
    async fn has_work_request<Tz: TimeZone>(
        &self,
        action: &str,
        next_date: DateTime<Tz>,
    ) -> Result<bool, Self::Error>
    where
        Tz: TimeZone + Send + Sync + Display + Debug + 'static,
        Tz::Offset: Send + Sync + Display + Debug + 'static,
    {
        Ok(self
            .work_requests
            .read()
            .await
            .iter()
            .any(|x| x.action == action && x.not_before == Some(next_date.with_timezone(&Utc))))
    }
}

#[async_trait]
impl BackendManager for MemoryEngine {
    /// Returns statistics about the queue.
    async fn statistics(&self) -> Result<Statistics, Self::Error> {
        Ok(Statistics {
            processed: 0,
            failed: 0,
            busy: 0,
            enqueued: 0,
            retries: 0,
            scheduled: 0,
            dead: 0,
        })
    }

    /// Returns work requests within the provided filter.
    async fn work_requests(
        &self,
        _filter: WorkRequestFilter,
    ) -> Result<Vec<WorkRequest>, Self::Error> {
        Ok(vec![])
    }
}
