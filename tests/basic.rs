use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use benthos::{
    backend::Backend,
    broker::{Broker, NewWorkRequest},
    task::{Task, WorkRequest},
    TypeMap,
};
use chrono::Utc;

struct TestBackend {
    ids: Mutex<Vec<String>>,
}

struct Error;

impl std::fmt::Display for Error {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

impl std::error::Error for Error {}

#[async_trait]
impl Backend for TestBackend {
    type Error = Error;

    /// Returns a list of work request identifiers that are ready to be processed.
    async fn poll(&self) -> Result<Vec<String>, Self::Error> {
        println!("Polling");
        let mut oh = self.ids.lock().unwrap();
        let mut out = vec![];
        std::mem::swap(&mut out, &mut oh);
        Ok(out)
    }

    /// Returns a work request for the given identifier.
    async fn work_request_with_id(&self, id: &str) -> Result<WorkRequest, Self::Error> {
        Ok(WorkRequest {
            id: id.to_string(),
            action: "test".to_string(),
            data: serde_json::json!({ "test": true }),
            attempts: 0,
            created_at: Utc::now(),
            last_attempted_at: None,
            not_before: None,
        })
    }

    async fn mark_attempted(&self, id: &str) -> Result<(), Self::Error> {
        println!("Attempted: {id}");
        Ok(())
    }

    async fn mark_succeeded(&self, id: &str) -> Result<(), Self::Error> {
        println!("Succeeded: {id}");
        Ok(())
    }

    async fn mark_failed(&self, id: &str) -> Result<(), Self::Error> {
        println!("Failed: {id}");
        Ok(())
    }

    /// Queues a new work request.
    async fn add_work_request(&self, _work_request: NewWorkRequest) -> Result<(), Self::Error> {
        self.ids.lock().unwrap().push("iddd".to_string());
        Ok(())
    }
}

struct TestHandler;

#[async_trait]
impl Task for TestHandler {
    fn id(&self) -> &'static str {
        "test"
    }

    async fn run(&self, _data: &TypeMap, request: WorkRequest) -> Result<(), benthos::task::Error> {
        println!("{:?}", request);
        Ok(())
    }
}

#[tokio::test]
async fn smoke() {
    let backend = TestBackend {
        ids: Mutex::new(vec!["1".to_string(), "2".to_string()]),
    };
    let broker = Broker::new(
        Arc::new(backend),
        1,
        Default::default(),
        &[Arc::new(TestHandler) as _],
    );

    let task = broker.start_workers();
    broker
        .add_work(NewWorkRequest {
            action: "lol".to_string(),
            data: Default::default(),
            not_before: Some(Utc::now()),
        })
        .await
        .unwrap();
    broker
        .add_work(NewWorkRequest {
            action: "lol".to_string(),
            data: Default::default(),
            not_before: Some(Utc::now()),
        })
        .await
        .unwrap();
    broker
        .add_work(NewWorkRequest {
            action: "lol".to_string(),
            data: Default::default(),
            not_before: Some(Utc::now()),
        })
        .await
        .unwrap();

    task.await.unwrap();
}
