use anyhow::Result;
use dashmap::DashMap;
use logan_client::Client;
use logan_protocol::messages::CreatableTopic;
use logan_server::offset_manager::OffsetManager;
use logan_server::server::Server;
use logan_server::shard::ShardManager;
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

    // Initialize ShardManager
    let config = logan_storage::config::LogConfig::default();
    let _num_shards = 2; // Use 2 shards for testing
    let shard_manager = Arc::new(ShardManager::new(log_dir.clone(), config, 2).await?);
    let offset_manager = Arc::new(OffsetManager::new());

    let (server, shutdown_tx) = Server::new(listener, 100, topics, shard_manager, offset_manager);

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
    // addr_str unused

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

#[tokio::test]
async fn test_delete_topics() -> Result<()> {
    // 1. Setup temp dir
    let temp_dir = tempfile::tempdir()?;
    let log_dir = temp_dir.path().to_path_buf();
    let (addr, shutdown_tx, server_handle) = start_server(log_dir).await?;

    let topic_name = "topic-to-delete";

    // 2. Create Client & Topic
    let mut client = Client::connect(addr).await?;
    client.create_topics(vec![topic_name.to_string()]).await?;

    // 3. Produce Data
    client.produce(topic_name, 0, b"data to delete").await?;

    // 4. Delete Topic
    let del_resp = client.delete_topics(vec![topic_name.to_string()]).await?;

    // Validate deletion response
    // Expecting error_code 0 (None)
    assert_eq!(del_resp.topic_errors.0.len(), 1);
    assert_eq!(del_resp.topic_errors.0[0].topic.0, topic_name);
    assert_eq!(del_resp.topic_errors.0[0].error_code as i16, 0);

    // 5. Verify Fetch returns empty (Shard handles missing log)
    // When log is missing, Shard currently returns empty record set (code 0).
    // Or if handling Metadata correctly, we might check Metadata first.

    let fetch_resp = client.fetch(topic_name, 0).await?;
    // Expectations:
    // If topic is truly deleted from memory, Shard recreation might happen on Fetch if we are not careful?
    // Current Shard implementation: handle_fetch -> get_or_load_log.
    // get_or_load_log checks dir existence. If we deleted dir, it returns "Log not found" error or empty?
    // Shard code: if subdir exists { ... } else { Err("Log not found") }
    // handle_fetch catches Err and returns Ok(LogReadResult::Data(Vec::new()))

    // So distinctively, we should get NO records.
    let topic_resp = &fetch_resp.responses.0[0];
    let part_resp = &topic_resp.partitions.0[0];

    if let Some(records) = &part_resp.records.0 {
        assert!(records.is_empty(), "Records should be empty after deletion");
    } else {
        // NullableBytes can be None, effectively empty
    }

    // 6. Verify Metadata is gone
    let meta_resp = client.send_metadata_request().await?;
    let found = meta_resp.topics.0.iter().any(|t| t.name.0 == topic_name);
    assert!(!found, "Topic should be absent from metadata");

    // Cleanup
    shutdown_tx.send(())?;
    server_handle.await?;

    Ok(())
}
