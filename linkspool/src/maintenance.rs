//! Periodic maintenance service.

use std::{sync::Arc, time::Duration};

use linkspool_core::{Core, MaintenanceConfig, MaintenanceReport};
use rivium::{Kind, Service};
use tokio::time::MissedTickBehavior;

use crate::internal;

/// Build the maintenance service.
///
/// The first pass runs immediately so a restarted host recovers interrupted
/// deliveries before the worker begins consuming jobs.
pub fn service(core: Arc<Core>, config: MaintenanceConfig) -> Box<dyn Service> {
    rivium::service(
        "maintenance",
        Kind::Background,
        move |context| async move {
            core.open().await.map_err(internal)?;
            context.ready();

            let mut ticker = tokio::time::interval(Duration::from_millis(config.interval_ms));
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = context.stopped() => return Ok(()),
                    _ = ticker.tick() => {
                        match core.maintain().await {
                            Ok(report) => report_if_active(report),
                            Err(error) => tracing::error!(%error, "maintenance pass failed"),
                        }
                    }
                }
            }
        },
    )
}

fn report_if_active(report: MaintenanceReport) {
    if report == MaintenanceReport::default() {
        return;
    }
    tracing::debug!(
        recovered = report.recovered,
        expired = report.expired,
        purged = report.purged,
        jobs_purged = report.jobs_purged,
        "maintenance pass complete"
    );
}
