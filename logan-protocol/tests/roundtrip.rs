use anyhow::Result;
use bytes::{Bytes, BytesMut};
use logan_protocol::api_keys::ApiKey;
use logan_protocol::codec::{Decodable, Encodable};
use logan_protocol::messages::*;
use logan_protocol::primitives::*;
use logan_protocol::{RequestHeader, ResponseHeader};

#[test]
fn test_metadata_request_roundtrip_v0() -> Result<()> {
    // V0 request: [topics array] + (no allow_auto_topic_creation)
    // Actually our Rust struct for MetadataRequest is "upgraded" to include all fields usually,
    // but we use custom Decode impl to handle versions.
    // However, our current codebase hardcodes a specific version logic in `impl Decodable`.
    // Let's verify what the current implementation supports.

    // Construct a simple request
    let req = MetadataRequest {
        topics: KafkaArray(vec![KafkaString("test-topic".to_string())]),
    };

    let mut buf = BytesMut::new();
    req.encode(&mut buf)?;

    let mut decode_buf = buf.freeze();
    let decoded = MetadataRequest::decode(&mut decode_buf)?;

    assert_eq!(decoded.topics.0.len(), 1);
    assert_eq!(decoded.topics.0[0].0, "test-topic");
    Ok(())
}

#[test]
fn test_metadata_response_roundtrip() -> Result<()> {
    let mut brokers = Vec::new();
    brokers.push(BrokerV0 {
        node_id: 1,
        host: KafkaString("localhost".to_string()),
        port: 9092,
    });

    let resp = MetadataResponse {
        brokers: KafkaArray(brokers),
        topics: KafkaArray(vec![]),
    };

    let mut buf = BytesMut::new();
    resp.encode(&mut buf)?;

    let mut decode_buf = buf.freeze();
    let decoded = MetadataResponse::decode(&mut decode_buf)?;

    assert_eq!(decoded.brokers.0.len(), 1);
    assert_eq!(decoded.brokers.0[0].host.0, "localhost");
    Ok(())
}

#[test]
fn test_produce_request_roundtrip() -> Result<()> {
    // Create a deep nested structure for ProduceRequest
    let data = b"hello world";
    let partition_data = PartitionProduceData {
        index: 0,
        record_set: KafkaBytes(Bytes::from(data.to_vec())),
    };

    let topic_data = TopicProduceData {
        name: KafkaString("my-topic".to_string()),
        partition_data: KafkaArray(vec![partition_data]),
    };

    let req = ProduceRequest {
        acks: 1,
        timeout_ms: 1000,
        topic_data: KafkaArray(vec![topic_data]),
    };

    let mut buf = BytesMut::new();
    req.encode(&mut buf)?;

    let mut decode_buf = buf.freeze();
    let decoded = ProduceRequest::decode(&mut decode_buf)?;

    assert_eq!(decoded.acks, 1);
    assert_eq!(decoded.topic_data.0.len(), 1);
    assert_eq!(decoded.topic_data.0[0].name.0, "my-topic");
    Ok(())
}

#[test]
fn test_api_versions_request_roundtrip() -> Result<()> {
    let req = ApiVersionsRequest {};
    let mut buf = BytesMut::new();
    req.encode(&mut buf)?;

    let mut decode_buf = buf.freeze();
    let _decoded = ApiVersionsRequest::decode(&mut decode_buf)?;
    Ok(())
}

#[test]
fn test_header_roundtrip() -> Result<()> {
    let header = RequestHeader {
        api_key: ApiKey::Metadata,
        api_version: 0,
        correlation_id: 12345,
        client_id: NullableString(Some("test-client".to_string())),
    };

    let mut buf = BytesMut::new();
    header.encode(&mut buf)?;

    let mut decode_buf = buf.freeze();
    let decoded = RequestHeader::decode(&mut decode_buf)?;

    assert_eq!(decoded.api_key, ApiKey::Metadata);
    assert_eq!(decoded.correlation_id, 12345);
    assert_eq!(decoded.client_id.0.unwrap(), "test-client");
    Ok(())
}
