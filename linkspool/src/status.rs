//! Periodic status snapshot for hosts without a query interface.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use linkspool_core::{Core, StatusConfig, now_millis};
use rivium::{Kind, Service};
use serde::Serialize;
use tokio::time::MissedTickBehavior;

use crate::internal;

/// JSON document written for the embedded Android UI.
#[derive(Debug, Serialize)]
struct Snapshot<'a> {
    version: &'a str,
    destination: &'a str,
    updated_at: i64,
    total: i64,
    counts: BTreeMap<String, i64>,
}

/// Build the status snapshot service.
pub fn service(core: Arc<Core>, config: StatusConfig) -> Box<dyn Service> {
    rivium::service("status", Kind::Background, move |context| async move {
        core.open().await.map_err(internal)?;
        context.ready();
        tracing::info!(path = %config.path.display(), "status service started");

        let mut ticker = tokio::time::interval(Duration::from_millis(config.interval_ms));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = context.stopped() => return Ok(()),
                _ = ticker.tick() => {
                    if let Err(error) = write_snapshot(&core, &config).await {
                        tracing::error!(%error, "status snapshot failed");
                    }
                }
            }
        }
    })
}

async fn write_snapshot(core: &Core, config: &StatusConfig) -> Result<(), rivium::Error> {
    let counts = core.counts().await.map_err(internal)?;
    let snapshot = Snapshot {
        version: env!("CARGO_PKG_VERSION"),
        destination: &core.config().queue.destination,
        updated_at: now_millis(),
        total: counts.values().sum(),
        counts,
    };
    let encoded = serde_json::to_vec_pretty(&snapshot).map_err(internal)?;

    if let Some(parent) = config.path.parent()
        && !parent.as_os_str().is_empty()
    {
        tokio::fs::create_dir_all(parent).await.map_err(internal)?;
    }
    let temporary = config.path.with_extension("json.tmp");
    tokio::fs::write(&temporary, encoded)
        .await
        .map_err(internal)?;
    tokio::fs::rename(&temporary, &config.path)
        .await
        .map_err(internal)?;
    Ok(())
}
