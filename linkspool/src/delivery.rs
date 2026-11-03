//! Apalis delivery worker service.

use std::{sync::Arc, time::Duration};

use apalis::prelude::{BackendExt, BoxDynError, WorkerBuilder};
use apalis_sqlite::{Config as ApalisConfig, SqliteStorage};
use linkspool_core::{Core, DELIVERY_QUEUE, DeliveryJob};
use rivium::{Kind, Service};

use crate::internal;

/// Build the durable delivery worker service.
///
/// The worker consumes the same queue that [`linkspool_core::Store::submit`]
/// writes to. Each job carries only a message identifier and attempt number;
/// the authoritative payload stays in the message database.
pub fn service(core: Arc<Core>) -> Box<dyn Service> {
    rivium::service("delivery", Kind::Background, move |context| async move {
        let pool = core.open().await.map_err(internal)?.pool().clone();
        let concurrency = core.config().worker_concurrency();

        let config = ApalisConfig::default()
            .queue(DELIVERY_QUEUE)
            .batch_size(concurrency);
        let storage = SqliteStorage::<DeliveryJob>::new(&pool)
            .with_config(config)
            .poll_with_interval(Duration::from_millis(500));

        let handler = {
            let core = core.clone();
            move |job: DeliveryJob| {
                let core = core.clone();
                async move {
                    match core.deliver(job).await {
                        Ok(()) => Ok(()),
                        Err(error) => Err(BoxDynError::from(error)),
                    }
                }
            }
        };
        let worker_name = format!("linkspool-delivery-{}", std::process::id());
        let worker = WorkerBuilder::new(worker_name.as_str())
            .backend(storage)
            .build(handler);

        context.ready();
        tracing::info!(concurrency, worker = %worker_name, "delivery worker started");

        worker
            .run_with_ctx(move |worker| {
                let context = context.clone();
                async move {
                    context.stopped().await;
                    worker.stop()?;
                    Ok(())
                }
            })
            .await
            .map_err(internal)
    })
}
