use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;

use crate::{
    backend::{Backend, BackendManager, Statistics, WorkRequestFilter},
    task::{Error, Task, WorkRequest},
    typemap::TypeMap,
};

struct ActiveTask {
    request: WorkRequest,
    _handle: tokio::task::JoinHandle<()>,
}

pub struct Monitor<B> {
    backend: Arc<B>,
    active_tasks: Arc<tokio::sync::RwLock<HashMap<String, ActiveTask>>>,
}

impl<B: BackendManager> Monitor<B> {
    pub async fn statistics(&self) -> Result<Statistics, B::Error> {
        self.backend.statistics().await
    }

    pub async fn active_tasks(&self) -> Vec<WorkRequest> {
        let mut active_tasks: Vec<WorkRequest> = {
            let x = self.active_tasks.read().await;
            x.values().map(|task| task.request.clone()).collect()
        };

        active_tasks.sort_by(|a, b| a.started_at.cmp(&b.started_at));
        active_tasks
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
pub struct Broker<B> {
    backend: Arc<B>,
    poll_interval: u64,
    data: Arc<TypeMap>,
    active_tasks: Arc<tokio::sync::RwLock<HashMap<String, ActiveTask>>>,
    handlers: Arc<HashMap<String, Arc<dyn Task + Send + Sync>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewWorkRequest {
    pub action: String,
    pub data: serde_json::Value,
    pub not_before: Option<DateTime<Utc>>,
}

impl<B: BackendManager> Broker<B> {
    pub fn monitor(&self) -> Monitor<B> {
        Monitor {
            backend: self.backend.clone(),
            active_tasks: self.active_tasks.clone(),
        }
    }
}

impl<B: Backend + Send + Sync + 'static> Broker<B> {
    pub fn new(
        backend: Arc<B>,
        poll_interval: u64,
        data: Arc<TypeMap>,
        handlers: &[Arc<dyn Task + Send + Sync>],
    ) -> Self {
        Self {
            backend,
            poll_interval,
            data,
            active_tasks: Default::default(),
            handlers: Arc::new(
                handlers
                    .iter()
                    .map(|t| (t.id().to_string(), t.clone()))
                    .collect(),
            ),
        }
    }

    pub async fn add_work(&self, work: NewWorkRequest) -> Result<(), B::Error> {
        self.backend.add_work_request(work).await
    }

    async fn _start(
        backend: Arc<B>,
        data: Arc<TypeMap>,
        active_tasks_lock: Arc<tokio::sync::RwLock<HashMap<String, ActiveTask>>>,
        handlers: Arc<HashMap<String, Arc<dyn Task + Send + Sync>>>,
        poll_interval: u64,
    ) {
        tracing::info!("starting broker worker loop");
        loop {
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
                    Ok(v) => v,
                    Err(e) => {
                        tracing::error!(error=%e, id=id, "No work request found for id");
                        continue;
                    }
                };

                let Some(handler) = handlers.get(&work_request.action).map(Arc::clone) else {
                    tracing::error!(action=work_request.action, "No handler found");
                    continue;
                };

                drop(active_tasks);
                let (tx, rx) = tokio::sync::oneshot::channel();

                let task = {
                    let active_tasks_lock = Arc::clone(&active_tasks_lock);
                    let data = Arc::clone(&data);
                    let id = id.clone();
                    let backend = backend.clone();

                    let active_work_request = work_request.and_started_at(Utc::now());
                    let handle = tokio::task::spawn(async move {
                        rx.await.unwrap();
                        let action = work_request.action.clone();

                        tracing::debug!(id = %id, "Starting task");
                        let result = handler.run(&data, work_request).await;
                        let mut timeout = 1u64;
                        match result {
                            Ok(_) => {
                                tracing::debug!(id = %id, "Finished task");
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

            sleep(Duration::from_secs(poll_interval)).await;
        }
    }

    pub fn start_workers(&self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(Self::_start(
            self.backend.clone(),
            self.data.clone(),
            self.active_tasks.clone(),
            self.handlers.clone(),
            self.poll_interval,
        ))
    }
}
