//! File-drop ingestion for hosts that cannot call the core directly.
//!
//! Producers write a complete file into the inbox directory and the host
//! deletes it only after the message is durable. A crash between ingestion and
//! deletion is safe because the file name is used as the idempotency key.

use std::{path::Path, sync::Arc, time::Duration};

use linkspool_core::{Core, CoreError, InboxConfig, NewMessage};
use rivium::{Kind, Service};
use tokio::time::MissedTickBehavior;

use crate::internal;

/// Suffix used to park files that can never be ingested.
const REJECTED_SUFFIX: &str = ".rejected";
/// Suffix used by producers for in-progress atomic writes.
const TEMP_SUFFIX: &str = ".tmp";

/// Build the inbox ingestion service.
pub fn service(core: Arc<Core>, config: InboxConfig) -> Box<dyn Service> {
    rivium::service("inbox", Kind::Background, move |context| async move {
        core.open().await.map_err(internal)?;
        tokio::fs::create_dir_all(&config.path)
            .await
            .map_err(internal)?;
        context.ready();
        tracing::info!(path = %config.path.display(), "inbox service started");

        let mut ticker = tokio::time::interval(Duration::from_millis(config.poll_interval_ms));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = context.stopped() => return Ok(()),
                _ = ticker.tick() => {
                    if let Err(error) = scan(&core, &config).await {
                        tracing::error!(%error, "inbox scan failed");
                    }
                }
            }
        }
    })
}

async fn scan(core: &Core, config: &InboxConfig) -> Result<(), CoreError> {
    let mut entries = tokio::fs::read_dir(&config.path).await?;
    let mut names = Vec::new();
    while let Some(entry) = entries.next_entry().await? {
        let file_type = entry.file_type().await?;
        if !file_type.is_file() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            names.push(name.to_owned());
        }
    }
    names.sort();

    for name in names {
        if name.starts_with('.') || name.ends_with(TEMP_SUFFIX) || name.ends_with(REJECTED_SUFFIX) {
            continue;
        }
        ingest(core, config, &name).await?;
    }
    Ok(())
}

async fn ingest(core: &Core, config: &InboxConfig, name: &str) -> Result<(), CoreError> {
    let path = config.path.join(name);
    let Some(key) = Path::new(name).file_stem().and_then(|stem| stem.to_str()) else {
        reject(&path, "file name has no usable stem").await?;
        return Ok(());
    };
    if key.is_empty() || key.len() > 256 {
        reject(&path, "file name must contain 1 to 256 bytes").await?;
        return Ok(());
    }

    let bytes = tokio::fs::read(&path).await?;
    if bytes.len() > config.max_file_bytes {
        reject(&path, "file exceeds inbox.max_file_bytes").await?;
        return Ok(());
    }
    let Ok(text) = String::from_utf8(bytes) else {
        reject(&path, "file is not valid UTF-8").await?;
        return Ok(());
    };

    let new = NewMessage {
        producer: config.producer.clone(),
        destination: core.config().queue.destination.clone(),
        idempotency_key: Some(key.to_owned()),
        payload: serde_json::json!({ "text": text }),
        expires_at: None,
    };

    match core.submit(new).await {
        Ok(submission) => {
            tokio::fs::remove_file(&path).await?;
            tracing::info!(
                message_id = %submission.message.id,
                duplicate = submission.duplicate,
                file = name,
                "inbox message accepted"
            );
        }
        Err(CoreError::QueueFull) => {
            tracing::debug!(file = name, "inbox message deferred; queue is full");
        }
        Err(error @ (CoreError::Invalid(_) | CoreError::PayloadTooLarge { .. })) => {
            tracing::warn!(file = name, %error, "inbox message rejected");
            reject(&path, &error.to_string()).await?;
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

async fn reject(path: &Path, reason: &str) -> Result<(), CoreError> {
    let mut rejected = path.as_os_str().to_owned();
    rejected.push(REJECTED_SUFFIX);
    tokio::fs::rename(path, Path::new(&rejected)).await?;
    tracing::warn!(path = %path.display(), reason, "inbox file parked as rejected");
    Ok(())
}
