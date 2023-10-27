use std::sync::{Arc, Weak};

use async_trait::async_trait;
use benthos::{
    broker::{self, Broker, NewWorkRequest},
    engine::memory::MemoryEngine,
    task::{Task, WorkRequest},
    TypeMap,
};
use chrono::Utc;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    run().await
}

async fn run() {
    // This is how you can pass the broker to tasks themselves.
    let broker = Arc::new_cyclic(|weak| {
        let mut data = TypeMap::new();
        data.insert(weak.to_owned());

        Broker::new(
            Arc::new(MemoryEngine::new(data)),
            &broker::Options {
                poll_interval: 10,
                task_timeout_secs: 60,
                max_parallel_tasks: Some(5),
                timezone: Utc,
            },
            &[
                Arc::new(PeriodicPrintingTask) as _,
                Arc::new(PrintTask) as _,
            ],
        )
    });

    let handle = broker.start_workers();

    broker
        .add_work(NewWorkRequest {
            action: "print_task".into(),
            data: bson::Bson::String("test string".into()),
            scheduled_at: Some(Utc::now() + chrono::Duration::seconds(2)),
            expires_at: None,
        })
        .await
        .unwrap();

    handle.await.unwrap();
}

struct PrintTask;

#[async_trait]
impl Task for PrintTask {
    /// The name of the task.
    fn id(&self) -> &'static str {
        "print_task"
    }

    /// The task runner.
    async fn run(&self, data: &TypeMap, request: WorkRequest) -> Result<(), benthos::task::Error> {
        println!("Printed: {}", request.data.as_str().unwrap());

        let broker = data
            .get::<Weak<Broker<MemoryEngine, Utc>>>()
            .unwrap()
            .upgrade()
            .unwrap();
        println!("{:#?}", broker.active_tasks().await);

        Ok(())
    }
}

struct PeriodicPrintingTask;

#[async_trait]
impl Task for PeriodicPrintingTask {
    /// The name of the task.
    fn id(&self) -> &'static str {
        "periodic_printing_task"
    }

    /// The task runner.
    async fn run(&self, data: &TypeMap, request: WorkRequest) -> Result<(), benthos::task::Error> {
        println!("{:?}", request);
        Ok(())
    }

    /// The repetition interval for the task.
    fn repetition_rule(&self) -> Option<cron::Schedule> {
        Some("0,20,40 * * * * * *".parse().unwrap())
    }

    /// Generates the data for the pending task to be run.
    async fn generate_data(&self, data: &TypeMap) -> bson::Bson {
        bson::Bson::Document(bson::Document::new())
    }
}
