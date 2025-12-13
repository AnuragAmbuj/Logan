//! Server implementation for the Logan Kafka broker

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::dispatch::dispatch;
use crate::error::ServerError;
use crate::offset_manager::OffsetManager;
use crate::shard::ShardManager;
use anyhow::Result;
use dashmap::DashMap;
use logan_protocol::messages::CreatableTopic;
use std::sync::Arc;

/// Main server type for the Logan Kafka broker
#[derive(Debug)]
#[allow(dead_code)]
pub struct Server {
    /// The TCP listener
    listener: TcpListener,
    /// The maximum number of concurrent connections
    max_connections: usize,
    /// Channel for shutdown signal
    shutdown_tx: broadcast::Sender<()>,
    /// Shared topic state
    topics: Arc<DashMap<String, CreatableTopic>>,
    /// Shard Manager (Core-Local Partitioning)
    /// Shard Manager (Core-Local Partitioning)
    shard_manager: Arc<ShardManager>,
    /// Offset Manager
    offset_manager: Arc<OffsetManager>,
}

impl Server {
    /// Create a new server instance
    pub fn new(
        listener: TcpListener,
        max_connections: usize,
        topics: Arc<DashMap<String, CreatableTopic>>,
        shard_manager: Arc<ShardManager>,
        offset_manager: Arc<OffsetManager>,
    ) -> (Self, broadcast::Sender<()>) {
        let (shutdown_tx, _shutdown_rx) = broadcast::channel(1);

        (
            Self {
                listener,
                max_connections,
                topics,
                shutdown_tx: shutdown_tx.clone(),
                shard_manager,
                offset_manager,
            },
            shutdown_tx,
        )
    }

    /// Get the local address of the server
    pub fn local_addr(&self) -> SocketAddr {
        self.listener.local_addr().unwrap()
    }

    /// Run the server
    pub async fn run(self) -> Result<(), ServerError> {
        info!("Server listening on {}", self.local_addr());

        let mut connection_count = 0;
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Cleaner Task is TODO for ShardManager (shards should manage their own retention)
        // For now, we omit the global cleaner task as it relied on LogManager::cleanup().
        // Shards should self-clean or accept a Clean command.

        loop {
            tokio::select! {
                result = self.listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            if connection_count >= self.max_connections {
                                warn!("Max connections reached, rejecting new connection from {}", addr);
                                continue;
                            }
                            connection_count += 1;
                            info!("Accepted connection #{}: {}", connection_count, addr);
                            if let Err(e) = stream.set_nodelay(true) {
                                warn!("Failed to set TCP_NODELAY: {}", e);
                            }

                            let topics = Arc::clone(&self.topics);
                            let shard_manager = Arc::clone(&self.shard_manager);
                            let offset_manager = Arc::clone(&self.offset_manager);
                            tokio::spawn(async move {
                                handle_connection(stream, addr, topics, shard_manager, offset_manager).await;
                            });
                        }
                        Err(e) => {
                            error!("Error accepting connection: {}", e);
                        }
                    }
                }
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received, shutting down...");
                    break;
                }
            }
        }

        Ok(())
    }
}

async fn handle_connection(
    mut stream: tokio::net::TcpStream,
    addr: SocketAddr,
    topics: Arc<DashMap<String, CreatableTopic>>,
    shard_manager: Arc<ShardManager>,
    offset_manager: Arc<OffsetManager>,
) {
    loop {
        match dispatch(
            &mut stream,
            topics.clone(),
            shard_manager.clone(),
            offset_manager.clone(),
        )
        .await
        {
            Ok(true) => {
                // Keep connection open
            }
            Ok(false) => {
                info!("Closing connection with {}", addr);
                break;
            }
            Err(e) => {
                error!("Error processing request from {}: {}", addr, e);
                break;
            }
        }
    }
}
