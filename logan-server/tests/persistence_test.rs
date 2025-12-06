use anyhow::Result;
use dashmap::DashMap;
use logan_client::Client;
use logan_protocol::messages::CreatableTopic;
use logan_server::server::Server;
use logan_storage::LogManager;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;

async fn start_server(
    log_dir: std::path::PathBuf,
) -> Result<(
    std::net::SocketAddr,
    broadcast::Sender<()>,
    tokio::task::JoinHandle<()>,
)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let local_addr = listener.local_addr()?;
    let topics = Arc::new(DashMap::<String, CreatableTopic>::new());
    let log_manager = Arc::new(LogManager::new(log_dir)?);
    let (server, shutdown_tx) = Server::new(listener, 100, topics, log_manager);

    let handle = tokio::spawn(async move {
        if let Err(e) = server.run().await {
            eprintln!("Server error: {}", e);
        }
    });

    Ok((local_addr, shutdown_tx, handle))
}

#[tokio::test]
async fn test_persistence() -> Result<()> {
    // 1. Setup temp dir
    let temp_dir = tempfile::tempdir()?;
    let log_dir = temp_dir.path().to_path_buf();

    // 2. Start server
    let (addr, shutdown_tx, server_handle) = start_server(log_dir.clone()).await?;
    let addr_str = addr.to_string();

    // 3. Produce message
    let mut client = Client::connect(addr).await?;
    client.create_topics(vec!["test-topic".to_string()]).await?;
    client.produce("test-topic", 0, b"hello world").await?;

    // 4. Shutdown server
    shutdown_tx.send(())?;
    server_handle.await?;

    // 5. Restart server
    let (addr_new, shutdown_tx_new, server_handle_new) = start_server(log_dir).await?;

    // 6. Fetch message
    let mut client_new = Client::connect(addr_new).await?;
    let response = client_new.fetch("test-topic", 0).await?;

    // 7. Verify
    let topic_response = &response.responses.0[0];
    let partition_data = &topic_response.partitions.0[0];
    let records = partition_data
        .records
        .0
        .as_ref()
        .expect("Should have records");
    assert_eq!(records.as_ref(), b"hello world");

    // Cleanup
    shutdown_tx_new.send(())?;
    server_handle_new.await?;

    Ok(())
}
