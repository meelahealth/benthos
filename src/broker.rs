use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::{
    backend::{Backend, BackendManager, Statistics, WorkRequestFilter},
    task::{Error, Task, WorkRequest},
};

struct ActiveTask {
    request: WorkRequest,
    _handle: tokio::task::JoinHandle<()>,
}

pub struct Monitor<B> {
    backend: Arc<B>,
}

impl<B: BackendManager> Monitor<B> {
    pub async fn statistics(&self) -> Result<Statistics, B::Error> {
        self.backend.statistics().await
    }

    pub async fn work_requests(
        &self,
        filter: WorkRequestFilter,
    ) -> Result<Vec<WorkRequest>, B::Error> {
        // This doesn't currently take into account active tasks.
        self.backend.work_requests(filter).await
    }
}

pub struct Broker<B, Tz: TimeZone> {
    backend: Arc<B>,
    timezone: Tz,
    poll_interval: u64,
    task_timeout_secs: u64,
    active_tasks: Arc<tokio::sync::RwLock<HashMap<String, ActiveTask>>>,
    tasks: Arc<HashMap<String, Arc<dyn Task + Send + Sync>>>,
    parallelism_semaphore: Option<Arc<tokio::sync::Semaphore>>,
}

impl<B, Tz: TimeZone + Copy> Clone for Broker<B, Tz> {
    fn clone(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            timezone: self.timezone,
            poll_interval: self.poll_interval,
            task_timeout_secs: self.task_timeout_secs,
            active_tasks: Arc::clone(&self.active_tasks),
            tasks: Arc::clone(&self.tasks),
            parallelism_semaphore: self.parallelism_semaphore.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewWorkRequest {
    pub action: String,
    pub data: serde_json::Value,
    pub scheduled_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl<B: BackendManager, Tz: TimeZone> Broker<B, Tz> {
    pub fn monitor(&self) -> Monitor<B> {
        Monitor {
            backend: self.backend.clone(),
        }
    }
}

pub struct Options<Tz: TimeZone> {
    pub poll_interval: u64,
    pub task_timeout_secs: u64,
    pub max_parallel_tasks: Option<usize>,
    pub timezone: Tz,
}

impl<B, Tz> Broker<B, Tz>
where
    B: Backend + Send + Sync + 'static,
    Tz: TimeZone + Copy + Send + Sync + Display + Debug + 'static,
    Tz::Offset: Send + Sync + Display + Debug + 'static,
{
    pub fn new(
        backend: Arc<B>,
        options: &Options<Tz>,
        handlers: &[Arc<dyn Task + Send + Sync>],
    ) -> Self {
        let max_parallel_tasks = options.max_parallel_tasks.unwrap_or(0);

        Self {
            backend,
            timezone: options.timezone,
            poll_interval: options.poll_interval,
            task_timeout_secs: options.task_timeout_secs,
            active_tasks: Default::default(),
            tasks: Arc::new(
                handlers
                    .iter()
                    .map(|t| (t.id().to_string(), t.clone()))
                    .collect(),
            ),
            parallelism_semaphore: if max_parallel_tasks > 0 {
                Some(Arc::new(tokio::sync::Semaphore::new(max_parallel_tasks)))
            } else {
                None
            },
        }
    }

    pub async fn add_work(&self, work: NewWorkRequest) -> Result<String, B::Error> {
        self.backend.add_work_request(work).await
    }

    pub async fn active_tasks(&self) -> Vec<WorkRequest> {
        let mut active_tasks: Vec<WorkRequest> = {
            let x = self.active_tasks.read().await;
            x.values().map(|task| task.request.clone()).collect()
        };

        active_tasks.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));
        active_tasks
    }

    /// Process one iteration of the worker loop.
    ///
    /// This polls the backend for ready work, processes repeating tasks,
    /// and executes all available work requests. Returns the number of
    /// work requests that were started.
    ///
    /// This is primarily useful for testing, where you can call `tick()`
    /// to execute tasks synchronously without starting the background worker.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // In a test:
    /// let broker = Broker::new(...);
    ///
    /// // Queue some work
    /// broker.add_work(work_request).await?;
    ///
    /// // Process it immediately
    /// let count = broker.tick().await?;
    /// assert_eq!(count, 1);
    /// ```
    pub async fn tick(&self) -> Result<usize, B::Error> {
        let data = self.backend.data().await;

        // Handle repeating tasks
        self.poll_repeating_tasks(&data).await?;

        // Poll for ready work
        tracing::trace!("Polling for work requests");
        let new_ids = self.backend.poll().await?;
        tracing::trace!(count = new_ids.len(), "Found work requests");

        let mut started_count = 0;

        for id in new_ids {
            // Skip if already active
            {
                let active_tasks = self.active_tasks.read().await;
                if active_tasks.contains_key(&id) {
                    continue;
                }
            }

            // Start processing this work request
            let was_started = self.process_work_request(id, &data).await?;

            if was_started {
                started_count += 1;
            }
        }

        Ok(started_count)
    }

    /// Poll for repeating tasks and enqueue them if needed.
    ///
    /// This checks all tasks with `repetition_rule()` and creates work requests
    /// for any that are due but don't already have a queued job.
    pub async fn poll_repeating_tasks(&self, data: &Arc<crate::TypeMap>) -> Result<(), B::Error> {
        tracing::trace!("Polling repeating tasks");

        // Build schedule for each repeating task
        let mut repeating_tasks = self
            .tasks
            .values()
            .filter_map(|x| {
                x.repetition_rule()
                    .map(|r| (x.id(), r.upcoming_owned(self.timezone).peekable()))
            })
            .collect::<HashMap<_, _>>();

        let repeating_work_requests = repeating_tasks
            .iter_mut()
            .filter_map(|(id, sched)| {
                let now = Utc::now();
                // Skip past times
                while sched.peek().map(|t| t < &now).unwrap_or(false) {
                    sched.next();
                }

                if let Some(dt) = sched.peek() {
                    return Some((*id, dt));
                }

                None
            })
            .collect::<Vec<_>>();

        tracing::trace!(
            "{:?}",
            repeating_work_requests
                .iter()
                .map(|(id, x)| (id, x))
                .collect::<Vec<_>>()
        );

        for (id, next_date) in repeating_work_requests {
            let has_job = match self.backend.has_work_request(id, next_date.clone()).await {
                Ok(v) => {
                    if v {
                        tracing::trace!(id, "Already has job with id: {}", id);
                    } else {
                        tracing::trace!(id, "Creating new job for id: {}", id);
                    }
                    v
                }
                Err(e) => {
                    tracing::error!(id, error=%e, "Error while polling for repeating work request");
                    continue;
                }
            };

            if !has_job {
                let task = self.tasks.get(id).unwrap();

                let work_request = self
                    .backend
                    .add_work_request(NewWorkRequest {
                        action: task.id().to_string(),
                        data: task.generate_data(&data).await,
                        scheduled_at: Some(next_date.with_timezone(&Utc)),
                        expires_at: Some(
                            next_date.with_timezone(&Utc) + chrono::Duration::seconds(30),
                        ),
                    })
                    .await;

                match work_request {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::error!(id=task.id(), action=task.id().to_string(), error=%e, "Failed to add work request");
                        continue;
                    }
                }
            }
        }

        Ok(())
    }

    /// Process a single work request by ID.
    ///
    /// This fetches the work request, validates it, and spawns a task to execute it.
    /// Returns `true` if the work request was started, `false` if it was skipped
    /// (e.g., already expired, no handler found, etc.).
    ///
    /// This is public to allow fine-grained control in tests.
    pub async fn process_work_request(
        &self,
        id: String,
        data: &Arc<crate::TypeMap>,
    ) -> Result<bool, B::Error> {
        let work_request = match self.backend.work_request_with_id(&id).await {
            Ok(Some(v)) => v,
            Ok(None) => {
                tracing::error!(id, "No work request found");
                return Ok(false);
            }
            Err(e) => {
                tracing::error!(id, error=%e, "An error occurred while polling for work request");
                return Err(e);
            }
        };

        let action = work_request.action.clone();

        let Some(handler) = self.tasks.get(&work_request.action).map(Arc::clone) else {
            tracing::error!(id, action, "No handler found");
            match self.backend.mark_failed(&id).await {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(id, error=%e, "Failed to mark work request as failed");
                }
            }
            return Ok(false);
        };

        // Check expiration
        if let Some(expires_at) = work_request.expires_at {
            if expires_at < Utc::now() {
                tracing::trace!(id, action, "Work request has expired");

                let mut timeout = 1u64;

                loop {
                    match self.backend.mark_expired(&id).await {
                        Ok(_) => {
                            break;
                        }
                        Err(e) => {
                            tracing::error!(id, action, error=%e, "Failed to mark expired; retrying in {} seconds", timeout);
                            tokio::time::sleep(tokio::time::Duration::from_secs(timeout)).await;
                            timeout *= 2;
                        }
                    }
                }
                return Ok(false);
            }
        }

        // Spawn task execution
        let (tx, rx) = tokio::sync::oneshot::channel();

        let active_work_request = work_request.and_started_at(Utc::now());
        let action = active_work_request.action.clone();

        let handle = tokio::task::spawn(Self::execute_work_request(
            id.clone(),
            action.clone(),
            work_request,
            handler,
            rx,
            data.clone(),
            self.backend.clone(),
            self.parallelism_semaphore.clone(),
            self.task_timeout_secs,
            self.active_tasks.clone(),
        ));

        let task = ActiveTask {
            _handle: handle,
            request: active_work_request,
        };

        self.active_tasks.write().await.insert(id.clone(), task);
        tx.send(()).unwrap();

        Ok(true)
    }

    /// Execute a work request (spawned as a separate task).
    async fn execute_work_request(
        id: String,
        action: String,
        work_request: WorkRequest,
        handler: Arc<dyn Task + Send + Sync>,
        rx: tokio::sync::oneshot::Receiver<()>,
        data: Arc<crate::TypeMap>,
        backend: Arc<B>,
        parallelism_semaphore: Option<Arc<tokio::sync::Semaphore>>,
        task_timeout_secs: u64,
        active_tasks_lock: Arc<tokio::sync::RwLock<HashMap<String, ActiveTask>>>,
    ) {
        let _permit = if let Some(sem) = parallelism_semaphore.as_ref() {
            let s = match sem.acquire().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(id, action, error=%e, "Failed to acquire semaphore");
                    return;
                }
            };
            Some(s)
        } else {
            None
        };

        rx.await.unwrap();

        tracing::debug!(id, action, "Starting task");
        let result = tokio::select! {
            result = handler.run(&data, work_request) => {
                result
            },
            _ = sleep(Duration::from_secs(task_timeout_secs)) => {
                tracing::error!(id, action, "Task timed out");

                let mut timeout = 1u64;
                loop {
                    match backend.mark_attempted(&id).await {
                        Ok(_) => {
                            break;
                        }
                        Err(e) => {
                            tracing::error!(id, action, error=%e, "Failed to mark attempted; retrying in {} seconds", timeout);
                            tokio::time::sleep(
                                tokio::time::Duration::from_secs(timeout),
                            )
                            .await;
                            timeout *= 2;
                        }
                    }
                }
                active_tasks_lock.write().await.remove(&id);
                return;
            }
        };

        let mut timeout = 1u64;
        match result {
            Ok(_) => {
                tracing::debug!(id, action, "Finished task");
                loop {
                    match backend.mark_succeeded(&id).await {
                        Ok(_) => {
                            break;
                        }
                        Err(e) => {
                            tracing::error!(id, action, error=%e, "Failed to mark succeeded; retrying in {} seconds", timeout);
                            tokio::time::sleep(tokio::time::Duration::from_secs(
                                timeout,
                            ))
                            .await;
                            timeout *= 2;
                        }
                    }
                }
            }
            Err(e) => match e {
                Error::Retry => {
                    tracing::error!(id, action, error=%e, "Task being retried");
                    loop {
                        match backend.mark_attempted(&id).await {
                            Ok(_) => {
                                break;
                            }
                            Err(e) => {
                                tracing::error!(id, action, error=%e, "Failed to mark attempted; retrying in {} seconds", timeout);
                                tokio::time::sleep(
                                    tokio::time::Duration::from_secs(timeout),
                                )
                                .await;
                                timeout *= 2;
                            }
                        }
                    }
                }
                Error::Fail => {
                    tracing::error!(id, action, error=%e, "Task failed");
                    loop {
                        match backend.mark_failed(&id).await {
                            Ok(_) => {
                                break;
                            }
                            Err(e) => {
                                tracing::error!(id, action, error=%e, "Failed to mark failed; retrying in {} seconds", timeout);
                                tokio::time::sleep(
                                    tokio::time::Duration::from_secs(timeout),
                                )
                                .await;
                                timeout *= 2;
                            }
                        }
                    }
                }
            },
        }
        active_tasks_lock.write().await.remove(&id);
    }

    async fn _start(broker: Arc<Self>, poll_interval: u64) {
        tracing::info!("starting broker worker loop");

        loop {
            match broker.tick().await {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(error=%e, "Failed to tick broker");
                }
            }

            tracing::trace!("Sleeping for poll interval ({poll_interval}s)");
            sleep(Duration::from_secs(poll_interval)).await;
        }
    }

    pub fn start_workers(&self) -> tokio::task::JoinHandle<()> {
        let broker = Arc::new(Clone::clone(self));
        let poll_interval = self.poll_interval;
        tokio::spawn(Self::_start(broker, poll_interval))
    }
}
