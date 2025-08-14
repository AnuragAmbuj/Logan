//! Kafka protocol message types

use bytes::{Buf, BufMut};

use super::primitives::{Bytes, KafkaArray, KafkaBool, KafkaString, NullableString};
use super::{ApiKey, Decodable, Encodable};
use crate::protocol::error_codes::ErrorCode;
use crate::ServerError;

/// A Kafka protocol request header
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct RequestHeader {
    /// The API key of this request
    pub api_key: ApiKey,
    /// The API version of this request
    pub api_version: i16,
    /// The correlation ID of this request
    pub correlation_id: i32,
    /// The client ID string
    pub client_id: NullableString,
}

impl Decodable for RequestHeader {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let api_key = ApiKey::decode(buf)?;
        let api_version = i16::decode(buf)?;
        let correlation_id = i32::decode(buf)?;
        // The client_id is a nullable string. The version of the request
        // determines if it's present. For now we will assume a version that includes it.
        let client_id = NullableString::decode(buf)?;

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
        })
    }
}

impl Encodable for RequestHeader {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.api_key.encode(buf)?;
        self.api_version.encode(buf)?;
        self.correlation_id.encode(buf)?;
        self.client_id.encode(buf)?;
        Ok(())
    }
}

/// A Kafka protocol response header
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ResponseHeader {
    /// The correlation ID of this response
    pub correlation_id: i32,
}

impl Encodable for ResponseHeader {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.correlation_id.encode(buf)
    }
}

/// Metadata Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct MetadataRequest {
    /// The topics to fetch metadata for
    pub topics: KafkaArray<KafkaString>,
    /// Whether to auto-create topics
    pub allow_auto_topic_creation: KafkaBool,
}

impl Decodable for MetadataRequest {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let topics = KafkaArray::<KafkaString>::decode(buf)?;
        let allow_auto_topic_creation = KafkaBool::decode(buf)?;
        Ok(Self { topics, allow_auto_topic_creation })
    }
}

impl Encodable for MetadataRequest {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.topics.encode(buf)?;
        self.allow_auto_topic_creation.encode(buf)
    }
}

/// Metadata Response (v0+)
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct MetadataResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// Each broker in the response.
    pub brokers: KafkaArray<Broker>,
    /// The cluster ID that responding broker belongs to.
    pub cluster_id: NullableString,
    /// The ID of the controller broker.
    pub controller_id: i32,
    /// Each topic in the response.
    pub topics: KafkaArray<TopicMetadata>,
}

impl Decodable for MetadataResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let throttle_time_ms = i32::decode(buf)?;
        let brokers = KafkaArray::<Broker>::decode(buf)?;
        let cluster_id = NullableString::decode(buf)?;
        let controller_id = i32::decode(buf)?;
        let topics = KafkaArray::<TopicMetadata>::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            brokers,
            cluster_id,
            controller_id,
            topics,
        })
    }
}

impl Encodable for MetadataResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.throttle_time_ms.encode(buf)?;
        self.brokers.encode(buf)?;
        self.cluster_id.encode(buf)?;
        self.controller_id.encode(buf)?;
        self.topics.encode(buf)
    }
}

/// Broker information
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct Broker {
    /// The broker ID.
    pub node_id: i32,
    /// The broker hostname.
    pub host: KafkaString,
    /// The broker port.
    pub port: i32,
    /// The rack of the broker, or None if it has not been assigned to a rack.
    pub rack: NullableString,
}

impl Decodable for Broker {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let node_id = i32::decode(buf)?;
        let host = KafkaString::decode(buf)?;
        let port = i32::decode(buf)?;
        let rack = NullableString::decode(buf)?;
        Ok(Self {
            node_id,
            host,
            port,
            rack,
        })
    }
}

impl Encodable for Broker {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.node_id.encode(buf)?;
        self.host.encode(buf)?;
        self.port.encode(buf)?;
        self.rack.encode(buf)
    }
}

/// Topic metadata
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct TopicMetadata {
    /// The topic error, or None if there was no error.
    pub error_code: i16,
    /// The topic name.
    pub name: KafkaString,
    /// True if the topic is internal.
    pub is_internal: KafkaBool,
    /// Each partition in the topic.
    pub partitions: KafkaArray<PartitionMetadata>,
}

impl Decodable for TopicMetadata {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let error_code = i16::decode(buf)?;
        let name = KafkaString::decode(buf)?;
        let is_internal = KafkaBool::decode(buf)?;
        let partitions = KafkaArray::<PartitionMetadata>::decode(buf)?;
        Ok(Self {
            error_code,
            name,
            is_internal,
            partitions,
        })
    }
}

impl Encodable for TopicMetadata {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.error_code.encode(buf)?;
        self.name.encode(buf)?;
        self.is_internal.encode(buf)?;
        self.partitions.encode(buf)
    }
}

/// Partition metadata
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct PartitionMetadata {
    /// The partition error, or None if there was no error.
    pub error_code: i16,
    /// The partition index.
    pub partition_index: i32,
    /// The ID of the leader broker.
    pub leader_id: i32,
    /// The leader epoch of this partition.
    pub leader_epoch: i32,
    /// The set of all nodes that host this partition.
    pub replica_nodes: KafkaArray<i32>,
    /// The set of nodes that are in sync with the leader for this partition.
    pub isr_nodes: KafkaArray<i32>,
    /// The set of nodes that are allowed to serve the partition.
    pub offline_replicas: KafkaArray<i32>,
}

impl Decodable for PartitionMetadata {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let error_code = i16::decode(buf)?;
        let partition_index = i32::decode(buf)?;
        let leader_id = i32::decode(buf)?;
        let leader_epoch = i32::decode(buf)?;
        let replica_nodes = KafkaArray::<i32>::decode(buf)?;
        let isr_nodes = KafkaArray::<i32>::decode(buf)?;
        let offline_replicas = KafkaArray::<i32>::decode(buf)?;
        Ok(Self {
            error_code,
            partition_index,
            leader_id,
            leader_epoch,
            replica_nodes,
            isr_nodes,
            offline_replicas,
        })
    }
}

impl Encodable for PartitionMetadata {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.error_code.encode(buf)?;
        self.partition_index.encode(buf)?;
        self.leader_id.encode(buf)?;
        self.leader_epoch.encode(buf)?;
        self.replica_nodes.encode(buf)?;
        self.isr_nodes.encode(buf)?;
        self.offline_replicas.encode(buf)
    }
}

/// ApiVersions Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct ApiVersionsRequest;

impl Decodable for ApiVersionsRequest {
    fn decode(_buf: &mut impl Buf) -> Result<Self, ServerError> {
        Ok(Self)
    }
}

impl Encodable for ApiVersionsRequest {
    fn encode(&self, _buf: &mut impl BufMut) -> Result<(), ServerError> {
        Ok(())
    }
}

/// ApiVersions Response (v0+)
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct ApiVersionsResponse {
    /// The error code, or 0 if there was no error.
    pub error_code: i16,
    /// The API versions supported by the broker.
    pub api_versions: KafkaArray<ApiVersionResponse>,
}

impl Decodable for ApiVersionsResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let error_code = i16::decode(buf)?;
        let api_versions = KafkaArray::<ApiVersionResponse>::decode(buf)?;
        Ok(Self {
            error_code,
            api_versions,
        })
    }
}

impl Encodable for ApiVersionsResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.error_code.encode(buf)?;
        self.api_versions.encode(buf)
    }
}

/// Part of the ApiVersionsResponse, representing a single API key and its supported versions.
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct ApiVersionResponse {
    /// The API key.
    pub api_key: ApiKey,
    /// The minimum supported version.
    pub min_version: i16,
    /// The maximum supported version.
    pub max_version: i16,
}

impl Decodable for ApiVersionResponse {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let api_key = ApiKey::decode(buf)?;
        let min_version = i16::decode(buf)?;
        let max_version = i16::decode(buf)?;
        Ok(Self {
            api_key,
            min_version,
            max_version,
        })
    }
}

impl Encodable for ApiVersionResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.api_key.encode(buf)?;
        self.min_version.encode(buf)?;
        self.max_version.encode(buf)
    }
}

/// Fetch Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct FetchRequest {
    /// The replica ID indicates the node ID of the replica initiating this request. Normal clients should set this to -1.
    pub replica_id: i32,
    /// The maximum time in milliseconds to wait for the response.
    pub max_wait_ms: i32,
    /// The minimum bytes to accumulate in the response.
    pub min_bytes: i32,
    /// The maximum bytes to fetch.
    pub max_bytes: i32,
    /// This setting controls the visibility of transactional records.
    pub isolation_level: i8,
    /// The fetch session ID.
    pub session_id: i32,
    /// The fetch session epoch.
    pub session_epoch: i32,
    /// The topics to fetch.
    pub topics: Vec<FetchableTopic>,
    /// The forgotten topics to remove from the fetch session.
    pub forgotten_topics: Vec<ForgottenTopic>,
}

/// Fetchable topic
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct FetchableTopic {
    /// The topic name.
    pub name: String,
    /// The partitions to fetch.
    pub fetch_partitions: Vec<FetchPartition>,
}

/// Fetch partition
#[derive(Debug)]
#[allow(dead_code)]
pub struct FetchPartition {
    /// The partition index.
    pub partition: i32,
    /// The message offset.
    pub fetch_offset: i64,
    /// The log start offset.
    pub log_start_offset: i64,
    /// The maximum bytes to fetch.
    pub max_bytes: i32,
}

/// Forgotten topic
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ForgottenTopic {
    /// The topic name.
    pub name: String,
    /// The partitions to remove from the fetch session.
    pub partitions: Vec<i32>,
}

/// Fetch Response (v0+)
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct FetchResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The response error code.
    pub error_code: ErrorCode,
    /// The session ID.
    pub session_id: i32,
    /// The response topics.
    pub responses: KafkaArray<FetchableTopicResponse>,
}

impl Encodable for FetchResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.throttle_time_ms.encode(buf)?;
        self.error_code.encode(buf)?;
        self.session_id.encode(buf)?;
        self.responses.encode(buf)?;
        Ok(())
    }
}

/// Fetchable topic response
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct FetchableTopicResponse {
    /// The topic name.
    pub topic: KafkaString,
    /// The topic partitions.
    pub partitions: KafkaArray<PartitionData>,
}

impl Encodable for FetchableTopicResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.topic.encode(buf)?;
        self.partitions.encode(buf)?;
        Ok(())
    }
}

/// Partition data
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct PartitionData {
    /// The partition index.
    pub partition_index: i32,
    /// The partition error code.
    pub error_code: ErrorCode,
    /// The high watermark of the partition.
    pub high_watermark: i64,
    /// The last stable offset of the partition.
    pub last_stable_offset: i64,
    /// The log start offset.
    pub log_start_offset: i64,
    /// The aborted transactions.
    pub aborted_transactions: KafkaArray<AbortedTransaction>,
    /// The replica ID.
    pub replica_id: i32,
    /// The records.
    pub records: Bytes,
}

impl Encodable for PartitionData {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.partition_index.encode(buf)?;
        self.error_code.encode(buf)?;
        self.high_watermark.encode(buf)?;
        self.last_stable_offset.encode(buf)?;
        self.log_start_offset.encode(buf)?;
        self.aborted_transactions.encode(buf)?;
        self.replica_id.encode(buf)?;
        self.records.encode(buf)?;
        Ok(())
    }
}

/// Aborted transaction
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct AbortedTransaction {
    /// The producer ID.
    pub producer_id: i64,
    /// The first offset in the aborted transaction.
    pub first_offset: i64,
}

impl Encodable for AbortedTransaction {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.producer_id.encode(buf)?;
        self.first_offset.encode(buf)?;
        Ok(())
    }
}

/// Fetch topic
#[derive(Debug)]
#[allow(dead_code)]
pub struct FetchTopic {
    /// The topic name.
    pub topic: KafkaString,
    /// The partitions to fetch.
    pub partitions: KafkaArray<FetchPartition>,
}

/// ListOffsets Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ListOffsetsRequest {
    /// The replica ID indicates the node ID of the replica initiating this request. Normal clients should set this to -1.
    pub replica_id: i32,
    /// The isolation level.
    pub isolation_level: i8,
    /// The topics to list offsets for.
    pub topics: KafkaArray<ListOffsetsTopic>,
}

impl Decodable for ListOffsetsRequest {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let replica_id = i32::decode(buf)?;
        let isolation_level = i8::decode(buf)?;
        let topics = KafkaArray::<ListOffsetsTopic>::decode(buf)?;
        Ok(Self { replica_id, isolation_level, topics })
    }
}

/// Offset topic
#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct ListOffsetsTopic {
    /// The topic name.
    pub name: KafkaString,
    /// The partitions to list offsets for.
    pub partitions: KafkaArray<OffsetPartition>,
}

impl Decodable for ListOffsetsTopic {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let name = KafkaString::decode(buf)?;
        let partitions = KafkaArray::<OffsetPartition>::decode(buf)?;
        Ok(Self { name, partitions })
    }
}

/// Offset partition
#[derive(Debug)]
#[allow(dead_code)]
pub struct OffsetPartition {
    /// The partition index.
    pub partition_index: i32,
    /// The timestamp to find an offset for.
    pub timestamp: i64,
}

impl Decodable for OffsetPartition {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let partition_index = i32::decode(buf)?;
        let timestamp = i64::decode(buf)?;
        Ok(Self { partition_index, timestamp })
    }
}

/// ListOffsets Response (v0+)
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct ListOffsetsResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The response topics.
    pub topics: KafkaArray<OffsetTopicResponse>,
}

impl Encodable for ListOffsetsResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.throttle_time_ms.encode(buf)?;
        self.topics.encode(buf)
    }
}

/// Offset topic response
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct OffsetTopicResponse {
    /// The topic name.
    pub name: KafkaString,
    /// The topic partitions.
    pub partitions: KafkaArray<OffsetPartitionResponse>,
}

impl Encodable for OffsetTopicResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.name.encode(buf)?;
        self.partitions.encode(buf)
    }
}

/// Offset partition response
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct OffsetPartitionResponse {
    /// The partition index.
    pub partition_index: i32,
    /// The partition error code.
    pub error_code: i16,
    /// The timestamp associated with the returned offset.
    pub timestamp: i64,
    /// The offset found.
    pub offset: i64,
    /// The leader epoch.
    pub leader_epoch: i32,
}

impl Encodable for OffsetPartitionResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.partition_index.encode(buf)?;
        self.error_code.encode(buf)?;
        self.timestamp.encode(buf)?;
        self.offset.encode(buf)?;
        self.leader_epoch.encode(buf)
    }
}

/// FindCoordinator Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct FindCoordinatorRequest {
    /// The coordinator key.
    pub key: KafkaString,
    /// The coordinator key type.
    pub key_type: i8,
}

/// FindCoordinator Response (v0+)
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct FindCoordinatorResponse {
    /// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
    pub throttle_time_ms: i32,
    /// The response error code.
    pub error_code: ErrorCode,
    /// The response error message, or None if there was no error.
    pub error_message: NullableString,
    /// The node ID.
    pub node_id: i32,
    /// The host name.
    pub host: KafkaString,
    /// The port.
    pub port: i32,
}

impl Encodable for FindCoordinatorResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.throttle_time_ms.encode(buf)?;
        self.error_code.encode(buf)?;
        self.error_message.encode(buf)?;
        self.node_id.encode(buf)?;
        self.host.encode(buf)?;
        self.port.encode(buf)?;
        Ok(())
    }
}

/// CreateTopics Request (v0+)
#[derive(Debug)]
#[allow(dead_code)]
pub struct CreateTopicsRequest {
    pub topics: KafkaArray<CreatableTopic>,
    pub timeout_ms: i32,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct CreatableTopic {
    pub name: KafkaString,
    pub num_partitions: i32,
    pub replication_factor: i16,
    pub assignments: KafkaArray<ReplicaAssignment>,
    pub configs: KafkaArray<ConfigEntry>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ReplicaAssignment {
    pub partition_index: i32,
    pub broker_ids: KafkaArray<i32>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ConfigEntry {
    pub config_name: KafkaString,
    pub config_value: NullableString,
}

impl Decodable for CreateTopicsRequest {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let topics = KafkaArray::<CreatableTopic>::decode(buf)?;
        let timeout_ms = i32::decode(buf)?;
        Ok(Self { topics, timeout_ms })
    }
}

impl Decodable for CreatableTopic {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let name = KafkaString::decode(buf)?;
        let num_partitions = i32::decode(buf)?;
        let replication_factor = i16::decode(buf)?;
        let assignments = KafkaArray::<ReplicaAssignment>::decode(buf)?;
        let configs = KafkaArray::<ConfigEntry>::decode(buf)?;
        Ok(Self {
            name,
            num_partitions,
            replication_factor,
            assignments,
            configs,
        })
    }
}

impl Decodable for ReplicaAssignment {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let partition_index = i32::decode(buf)?;
        let broker_ids = KafkaArray::<i32>::decode(buf)?;
        Ok(Self { partition_index, broker_ids })
    }
}

impl Decodable for ConfigEntry {
    fn decode(buf: &mut impl Buf) -> Result<Self, ServerError> {
        let config_name = KafkaString::decode(buf)?;
        let config_value = NullableString::decode(buf)?;
        Ok(Self { config_name, config_value })
    }
}

/// CreateTopics Response (v0+)
#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct CreateTopicsResponse {
    pub topic_errors: KafkaArray<TopicError>,
}

#[derive(Debug, Default)]
#[allow(dead_code)]
pub struct TopicError {
    pub topic: KafkaString,
    pub error_code: i16,
}

impl Encodable for CreateTopicsResponse {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.topic_errors.encode(buf)
    }
}

impl Encodable for TopicError {
    fn encode(&self, buf: &mut impl BufMut) -> Result<(), ServerError> {
        self.topic.encode(buf)?;
        self.error_code.encode(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_code_conversion() {
        assert_eq!(logan_common::ErrorCode::try_from(0), Ok(logan_common::ErrorCode::None));
        assert_eq!(logan_common::ErrorCode::try_from(1), Ok(logan_common::ErrorCode::OffsetOutOfRange));
        assert_eq!(logan_common::ErrorCode::try_from(2), Ok(logan_common::ErrorCode::CorruptMessage));
        assert_eq!(logan_common::ErrorCode::try_from(3), Ok(logan_common::ErrorCode::UnknownTopicOrPartition));
        assert_eq!(logan_common::ErrorCode::try_from(4), Ok(logan_common::ErrorCode::InvalidFetchSize));
        assert_eq!(logan_common::ErrorCode::try_from(5), Ok(logan_common::ErrorCode::LeaderNotAvailable));
        assert!(logan_common::ErrorCode::try_from(999).is_err());
    }
}
