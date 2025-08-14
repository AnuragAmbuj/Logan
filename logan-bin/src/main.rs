use anyhow::Result;
use clap::Parser;
use dashmap::DashMap;
use logan_protocol::messages::CreatableTopic;
use logan_server::server::Server;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info, Level};

/// Kafka-compatible message broker
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Address to bind the server to
    #[arg(short, long, default_value = "0.0.0.0:9092")]
    bind: SocketAddr,

    /// Maximum number of concurrent connections
    #[arg(long, default_value = "1024")]
    max_connections: usize,

    /// Log level (trace, debug, info, warn, error)
    #[arg(short, long, default_value = "info")]
    log_level: Level,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command line arguments
    let args = Args::parse();

    // Initialize logging
    init_logging(args.log_level);

    info!("Starting Logan Kafka broker...");
    info!("Binding to {}", args.bind);
    info!("Max connections: {}", args.max_connections);

    // Create and start the server
    let topics = Arc::new(DashMap::<String, CreatableTopic>::new());
    let listener = TcpListener::bind(args.bind).await?;
    let (server, _shutdown_sender) = Server::new(listener, args.max_connections, topics.clone());

    let server_handle = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            error!("Server error: {}", e);
        }
    });

    server_handle.await?;

    info!("Server shutting down");

    Ok(())
}

fn init_logging(level: Level) {
    use tracing_subscriber::fmt;
    use tracing_subscriber::prelude::*;

    // Configure the formatter
    let formatting_layer = fmt::layer()
        .with_ansi(true)
        .with_level(true)
        .with_timer(fmt::time::UtcTime::rfc_3339())
        .with_writer(std::io::stderr);

    // Set up the subscriber with the specified log level
    let filter_layer = tracing_subscriber::filter::LevelFilter::from_level(level);

    tracing_subscriber::registry()
        .with(formatting_layer)
        .with(filter_layer)
        .init();
}
