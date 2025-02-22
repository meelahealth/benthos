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

#[derive(Clone)]
pub struct Broker<B, Tz: TimeZone> {
    backend: Arc<B>,
    timezone: Tz,
    poll_interval: u64,
    task_timeout_secs: u64,
    active_tasks: Arc<tokio::sync::RwLock<HashMap<String, ActiveTask>>>,
    tasks: Arc<HashMap<String, Arc<dyn Task + Send + Sync>>>,
    parallelism_semaphore: Option<Arc<tokio::sync::Semaphore>>,
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

    async fn _start(
        backend: Arc<B>,
        parallelism_semaphore: Option<Arc<tokio::sync::Semaphore>>,
        active_tasks_lock: Arc<tokio::sync::RwLock<HashMap<String, ActiveTask>>>,
        handlers: Arc<HashMap<String, Arc<dyn Task + Send + Sync>>>,
        poll_interval: u64,
        task_timeout_secs: u64,
        tz: Tz,
    ) {
        let data = backend.data().await;
        tracing::info!("starting broker worker loop");

        let mut repeating_tasks = handlers
            .values()
            .filter_map(|x| {
                x.repetition_rule()
                    .map(|r| (x.id(), r.upcoming_owned(tz).peekable()))
            })
            .collect::<HashMap<_, _>>();

        loop {
            tracing::trace!("polling repeating tasks");
            let repeating_work_requests = repeating_tasks
                .iter_mut()
                .filter_map(|(id, sched)| {
                    let now = Utc::now();
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
                let has_job = match backend.has_work_request(id, next_date.clone()).await {
                    Ok(v) => {
                        if v {
                            tracing::trace!("Already has job with id: {}", id);
                        } else {
                            tracing::trace!("Creating new job for id: {}", id);
                        }
                        v
                    }
                    Err(e) => {
                        tracing::error!(error=%e, id=id, "Error while polling for repeating work request");
                        continue;
                    }
                };

                if !has_job {
                    let task = handlers.get(id).unwrap();

                    let work_request = backend
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
                            tracing::error!(error=%e, "Failed to add work request");
                            continue;
                        }
                    }
                }
            }

            tracing::trace!("Polling ids");
            let new_ids = match backend.poll().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(error=%e, "Failed to poll broker backend");
                    sleep(Duration::from_secs(poll_interval)).await;
                    continue;
                }
            };

            tracing::trace!(count = new_ids.len(), "New ids");
            for id in new_ids {
                let active_tasks = active_tasks_lock.read().await;
                if active_tasks.contains_key(&id) {
                    continue;
                }

                let work_request = match backend.work_request_with_id(&id).await {
                    Ok(Some(v)) => v,
                    Ok(None) => {
                        tracing::error!(id = id, "No work request found");
                        continue;
                    }
                    Err(e) => {
                        tracing::error!(error=%e, id=id, "An error occurred while polling for work request");
                        continue;
                    }
                };

                let Some(handler) = handlers.get(&work_request.action).map(Arc::clone) else {
                    tracing::error!(action = work_request.action, "No handler found");
                    match backend.mark_failed(&id).await {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!(error=%e, id=id, "Failed to mark work request as failed");
                        }
                    }
                    continue;
                };
                drop(active_tasks);

                if let Some(expires_at) = work_request.expires_at {
                    if expires_at < Utc::now() {
                        tracing::trace!(action = work_request.action, "Work request has expired");

                        let mut timeout = 1u64;
                        loop {
                            match backend.mark_expired(&id).await {
                                Ok(_) => {
                                    break;
                                }
                                Err(e) => {
                                    tracing::error!(error=%e, id=id, "Failed to mark expired; retrying in {} seconds", timeout);
                                    tokio::time::sleep(tokio::time::Duration::from_secs(timeout))
                                        .await;
                                    timeout *= 2;
                                }
                            }
                        }
                        continue;
                    }
                }

                let (tx, rx) = tokio::sync::oneshot::channel();

                let task = {
                    let active_tasks_lock = Arc::clone(&active_tasks_lock);
                    let data = Arc::clone(&data);
                    let id = id.clone();
                    let backend = backend.clone();
                    let sem = parallelism_semaphore.clone();

                    let active_work_request = work_request.and_started_at(Utc::now());
                    let handle = tokio::task::spawn(async move {
                        let _permit = if let Some(sem) = sem.as_ref() {
                            let s = match sem.acquire().await {
                                Ok(v) => v,
                                Err(e) => {
                                    tracing::error!(error=%e, "Failed to acquire semaphore");
                                    return;
                                }
                            };
                            Some(s)
                        } else {
                            None
                        };

                        rx.await.unwrap();
                        let action = work_request.action.clone();

                        tracing::debug!(id = %id, action = %action, "Starting task");
                        let result = tokio::select! {
                            result = handler.run(&data, work_request) => {
                                result
                            },
                            _ = sleep(Duration::from_secs(task_timeout_secs)) => {
                                tracing::error!(id = %id, "Task timed out");

                                let mut timeout = 1u64;
                                loop {
                                    match backend.mark_attempted(&id).await {
                                        Ok(_) => {
                                            break;
                                        }
                                        Err(e) => {
                                            tracing::error!(error=%e, id=id, "Failed to mark attempted; retrying in {} seconds", timeout);
                                            tokio::time::sleep(
                                                tokio::time::Duration::from_secs(timeout),
                                            )
                                            .await;
                                            timeout *= 2;
                                        }
                                    }
                                }
                                return;
                            }
                        };

                        let mut timeout = 1u64;
                        match result {
                            Ok(_) => {
                                tracing::debug!(id = %id, action = %action, "Finished task");
                                loop {
                                    match backend.mark_succeeded(&id).await {
                                        Ok(_) => {
                                            break;
                                        }
                                        Err(e) => {
                                            tracing::error!(error=%e, id=id, "Failed to mark succeeded; retrying in {} seconds", timeout);
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
                                    tracing::error!(id=id, action=action, error=%e, "Task being retried");
                                    loop {
                                        match backend.mark_attempted(&id).await {
                                            Ok(_) => {
                                                break;
                                            }
                                            Err(e) => {
                                                tracing::error!(error=%e, id=id, "Failed to mark attempted; retrying in {} seconds", timeout);
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
                                    tracing::error!(id=id, action=action, error=%e, "Task failed");
                                    loop {
                                        match backend.mark_failed(&id).await {
                                            Ok(_) => {
                                                break;
                                            }
                                            Err(e) => {
                                                tracing::error!(error=%e, id=id, "Failed to mark failed; retrying in {} seconds", timeout);
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
                    });
                    ActiveTask {
                        _handle: handle,
                        request: active_work_request,
                    }
                };

                active_tasks_lock.write().await.insert(id.clone(), task);
                tx.send(()).unwrap();
            }

            tracing::trace!("Sleeping for poll interval ({poll_interval}s)");
            sleep(Duration::from_secs(poll_interval)).await;
        }
    }

    pub fn start_workers(&self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(Self::_start(
            self.backend.clone(),
            self.parallelism_semaphore.clone(),
            self.active_tasks.clone(),
            self.tasks.clone(),
            self.poll_interval,
            self.task_timeout_secs,
            self.timezone,
        ))
    }
}
