//! Server implementation for the Logan Kafka broker

use std::net::SocketAddr;

use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::dispatch::dispatch;
use crate::error::ServerError;
use logan_protocol::messages::CreatableTopic;
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use anyhow::Result;

/// In-memory message store for each topic and partition
pub type TopicMessageStore = DashMap<String, DashMap<i32, Mutex<Vec<u8>>>>;

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
    /// In-memory message store
    messages: Arc<TopicMessageStore>,
}

impl Server {
    /// Create a new server instance
    pub fn new(
        listener: TcpListener,
        max_connections: usize,
        topics: Arc<DashMap<String, CreatableTopic>>,
    ) -> (Self, broadcast::Sender<()>) {
        let (shutdown_tx, _shutdown_rx) = broadcast::channel(1);

        (
            Self {
                listener,
                max_connections,
                topics,
                shutdown_tx: shutdown_tx.clone(),
                messages: Arc::new(DashMap::new()),
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

                            let topics = Arc::clone(&self.topics);
                            let messages = Arc::clone(&self.messages);
                            tokio::spawn(async move {
                                handle_connection(stream, addr, topics, messages).await;
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
    messages: Arc<TopicMessageStore>,
) {
    loop {
        match dispatch(&mut stream, topics.clone(), messages.clone()).await {
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
