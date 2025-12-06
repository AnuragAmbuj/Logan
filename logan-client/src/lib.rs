use anyhow::Result;
use bytes::{Bytes, BytesMut};
use logan_protocol::{
    RequestHeader, ResponseHeader,
    api_keys::ApiKey,
    codec::{Decodable, Encodable},
    messages::*,
    primitives::{KafkaArray, KafkaBool, KafkaBytes, KafkaString, NullableString},
};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::info;

#[derive(Debug)]
pub struct Client {
    stream: TcpStream,
    correlation_id: i32,
}

impl Client {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        info!("Connected to server at {}", addr);
        Ok(Self {
            stream,
            correlation_id: 0,
        })
    }

    pub async fn create_topics(&mut self, topics: Vec<String>) -> Result<CreateTopicsResponse> {
        info!("Sending CreateTopics request...");

        let creatable_topics = topics
            .into_iter()
            .map(|name| CreatableTopic {
                name: KafkaString(name),
                num_partitions: 1,
                replication_factor: 1,
                assignments: KafkaArray(vec![]),
                configs: KafkaArray(vec![]),
            })
            .collect();

        let request = CreateTopicsRequest {
            topics: KafkaArray(creatable_topics),
            timeout_ms: 5000,
        };

        self.send_request(ApiKey::CreateTopics, 0, request).await
    }

    pub async fn delete_topics(&mut self, topics: Vec<String>) -> Result<DeleteTopicsResponse> {
        info!("Sending DeleteTopics request...");

        let request = DeleteTopicsRequest {
            topics: KafkaArray(topics.into_iter().map(KafkaString).collect()),
            timeout_ms: 5000,
        };

        self.send_request(ApiKey::DeleteTopics, 0, request).await
    }

    pub async fn send_api_versions_request(&mut self) -> Result<ApiVersionsResponse> {
        info!("Sending ApiVersions request...");
        self.send_request(ApiKey::ApiVersions, 0, ApiVersionsRequest)
            .await
    }

    pub async fn send_metadata_request(&mut self) -> Result<MetadataResponse> {
        info!("Sending Metadata request...");

        let request = MetadataRequest {
            topics: KafkaArray(vec![]),
            allow_auto_topic_creation: KafkaBool(true),
        };

        self.send_request(ApiKey::Metadata, 0, request).await
    }

    pub async fn produce(
        &mut self,
        topic: &str,
        partition: i32,
        records: &[u8],
    ) -> Result<ProduceResponse> {
        let request = ProduceRequest {
            acks: -1,
            timeout_ms: 5000,
            topic_data: KafkaArray(vec![TopicProduceData {
                name: KafkaString(topic.to_string()),
                partition_data: KafkaArray(vec![PartitionProduceData {
                    index: partition,
                    record_set: KafkaBytes(Bytes::from(records.to_vec())),
                }]),
            }]),
        };

        self.send_request(ApiKey::Produce, 0, request).await
    }

    pub async fn fetch(&mut self, topic: &str, partition: i32) -> Result<FetchResponse> {
        let request = FetchRequest {
            topics: KafkaArray(vec![FetchTopic {
                topic: KafkaString(topic.to_string()),
                partitions: KafkaArray(vec![FetchPartition {
                    partition_index: partition,
                    fetch_offset: 0,
                    max_bytes: 1024,
                }]),
            }]),
        };

        let response: FetchResponse = self.send_request(ApiKey::Fetch, 0, request).await?;
        Ok(response)
    }

    async fn send_request<R: Encodable, T: Decodable + std::fmt::Debug>(
        &mut self,
        api_key: ApiKey,
        api_version: i16,
        request: R,
    ) -> Result<T> {
        let correlation_id = self.correlation_id;
        self.correlation_id += 1;

        let header = RequestHeader {
            api_key,
            api_version,
            correlation_id,
            client_id: NullableString(Some("logan-client".to_string())),
        };

        let mut buf = BytesMut::new();
        header.encode(&mut buf)?;
        request.encode(&mut buf)?;

        let mut size_buf = BytesMut::with_capacity(4);
        (buf.len() as i32).encode(&mut size_buf)?;

        self.stream.write_all(&size_buf).await?;
        self.stream.write_all(&buf).await?;

        let mut response_size_buf = [0u8; 4];
        self.stream.read_exact(&mut response_size_buf).await?;
        let response_size = i32::decode(&mut &response_size_buf[..])?;

        let mut response_buf = vec![0; response_size as usize];
        self.stream.read_exact(&mut response_buf).await?;
        let mut response_buf = Bytes::from(response_buf);

        let _ = ResponseHeader::decode(&mut response_buf)?;
        let response = T::decode(&mut response_buf)?;

        info!("Received response: {:?}", response);
        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use logan_server::server::Server;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use tokio::net::TcpListener;

    async fn start_test_server() -> Result<SocketAddr> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let topics = Arc::new(DashMap::new());
        let (server, _shutdown_tx) = Server::new(listener, 10, topics);

        tokio::spawn(async move {
            server.run().await.unwrap();
        });

        Ok(addr)
    }

    #[tokio::test]
    async fn test_api_versions() -> Result<()> {
        // 1. Start a server
        let addr = start_test_server().await?;

        // 2. Create a client
        let mut client = Client::connect(addr).await?;

        // 3. Send a request
        let request = ApiVersionsRequest {};
        let response: ApiVersionsResponse =
            client.send_request(ApiKey::ApiVersions, 0, request).await?;

        assert_eq!(response.error_code, 0);
        assert!(!response.api_versions.0.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_create_topic() -> Result<()> {
        // 1. Start a server
        let addr = start_test_server().await?;

        // 2. Create a client
        let mut client = Client::connect(addr).await?;

        // 3. Send a request
        let topic_name = "test-topic";
        let create_response = client.create_topics(vec![topic_name.to_string()]).await?;

        assert_eq!(create_response.topic_errors.0.len(), 1);
        let topic_response = &create_response.topic_errors.0[0];
        assert_eq!(topic_response.topic.0, topic_name);
        assert_eq!(
            topic_response.error_code as i16,
            logan_protocol::error_codes::ErrorCode::None as i16
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_produce() -> Result<()> {
        // 1. Start a server
        let addr = start_test_server().await?;

        // 2. Create a client
        let mut client = Client::connect(addr).await?;

        // 3. Create a topic first
        let topic_name = "produce-test-topic";
        let create_response = client.create_topics(vec![topic_name.to_string()]).await?;
        assert_eq!(
            create_response.topic_errors.0[0].error_code as i16,
            logan_protocol::error_codes::ErrorCode::None as i16
        );

        // 4. Produce a message to the topic
        let response = client.produce(topic_name, 0, b"hello world").await?;

        // 5. Assert the response
        assert_eq!(response.responses.0.len(), 1);
        let topic_response = &response.responses.0[0];
        assert_eq!(topic_response.name.0, topic_name);
        assert_eq!(topic_response.partition_responses.0.len(), 1);
        let partition_response = &topic_response.partition_responses.0[0];
        assert_eq!(
            partition_response.error_code as i16,
            logan_protocol::error_codes::ErrorCode::None as i16
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_fetch() -> Result<()> {
        // 1. Start a server
        let addr = start_test_server().await?;

        // 2. Create a client
        let mut client = Client::connect(addr).await?;

        // 3. Create a topic
        let topic_name = "fetch-test-topic";
        let _ = client.create_topics(vec![topic_name.to_string()]).await?;

        // 4. Produce a message
        let message_payload = b"hello fetch!";
        let _ = client.produce(topic_name, 0, message_payload).await?;

        // 5. Fetch the message
        let fetch_response = client.fetch(topic_name, 0).await?;

        // 6. Assert the response
        assert_eq!(fetch_response.responses.0.len(), 1);
        let topic_response = &fetch_response.responses.0[0];
        assert_eq!(topic_response.topic.0, topic_name);
        assert_eq!(topic_response.partitions.0.len(), 1);
        let partition_data = &topic_response.partitions.0[0];
        assert_eq!(
            partition_data.error_code,
            logan_protocol::error_codes::ErrorCode::None as i16
        );
        let records = partition_data.records.0.as_ref().unwrap();
        assert_eq!(records.as_ref(), message_payload);

        Ok(())
    }
}
