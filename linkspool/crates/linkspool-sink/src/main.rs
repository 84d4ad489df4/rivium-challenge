//! `linkspool-sink` command-line receiver.

use std::{net::SocketAddr, path::PathBuf, process::ExitCode, sync::Arc};

use clap::Parser;
use linkspool_sink::{Sink, SinkError, serve};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "linkspool-sink",
    version,
    about = "Durable TCP receiver for LinkSpool"
)]
struct Cli {
    /// Address to listen on.
    #[arg(long, default_value = "127.0.0.1:9000")]
    listen: SocketAddr,

    /// Receiver-side SQLite database.
    #[arg(long, default_value = "linkspool-sink.db")]
    database: PathBuf,

    /// Drop the first acknowledgement for every newly recorded message.
    ///
    /// A sender that times out retransmits the same message; the retransmission
    /// is recorded as a duplicate and acknowledged normally.
    #[arg(long)]
    drop_ack: bool,

    /// Maximum accepted frame payload size in bytes.
    #[arg(long, default_value_t = linkspool_protocol::DEFAULT_MAX_FRAME_BYTES)]
    max_frame_bytes: usize,
}

#[tokio::main]
async fn main() -> ExitCode {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(%error, "sink failed");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), SinkError> {
    let sink = Arc::new(Sink::open(&cli.database, cli.drop_ack).await?);
    let listener = TcpListener::bind(cli.listen).await?;
    let totals = sink.totals().await?;

    tracing::info!(
        address = %cli.listen,
        database = %cli.database.display(),
        drop_ack = cli.drop_ack,
        received = totals.received,
        duplicates = totals.duplicates,
        "sink listening"
    );

    tokio::select! {
        result = serve(listener, sink.clone(), cli.max_frame_bytes) => result?,
        _ = tokio::signal::ctrl_c() => tracing::info!("shutdown requested"),
    }

    let totals = sink.totals().await?;
    tracing::info!(
        received = totals.received,
        duplicates = totals.duplicates,
        dropped = totals.dropped,
        "sink stopped"
    );
    Ok(())
}
